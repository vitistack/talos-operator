/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"bufio"
	"bytes"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/onsi/ginkgo/v2"
)

const (
	prometheusOperatorVersion = "v0.77.1"
	prometheusOperatorURL     = "https://github.com/prometheus-operator/prometheus-operator/" +
		"releases/download/%s/bundle.yaml"

	certmanagerVersion = "v1.16.3"
	certmanagerURLTmpl = "https://github.com/cert-manager/cert-manager/releases/download/%s/cert-manager.yaml"
)

func warnError(err error) {
	_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "warning: %v\n", err)
}

// Run executes the provided command within this context
func Run(cmd *exec.Cmd) (string, error) {
	dir, _ := GetProjectDir()
	cmd.Dir = dir

	if err := os.Chdir(cmd.Dir); err != nil {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "chdir dir: %s\n", err)
	}

	cmd.Env = append(os.Environ(), "GO111MODULE=on")
	command := strings.Join(cmd.Args, " ")
	_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "running: %s\n", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("%s failed with error: (%w) %s", command, err, string(output))
	}

	return string(output), nil
}

// InstallPrometheusOperator installs the prometheus Operator to be used to export the enabled metrics.
func InstallPrometheusOperator() error {
	bundleURL := fmt.Sprintf(prometheusOperatorURL, prometheusOperatorVersion)
	if err := validateReleaseURL(bundleURL, []string{
		"/prometheus-operator/prometheus-operator/releases/download/",
	}); err != nil {
		return err
	}
	// #nosec G204 - test helper executes kubectl with a pinned, validated HTTPS URL
	cmd := exec.Command("kubectl", "create", "-f", bundleURL)
	_, err := Run(cmd)
	return err
}

// UninstallPrometheusOperator uninstalls the prometheus
func UninstallPrometheusOperator() {
	bundleURL := fmt.Sprintf(prometheusOperatorURL, prometheusOperatorVersion)
	if err := validateReleaseURL(bundleURL, []string{
		"/prometheus-operator/prometheus-operator/releases/download/",
	}); err != nil {
		warnError(err)
		return
	}
	// #nosec G204 - test helper executes kubectl with a pinned, validated HTTPS URL
	cmd := exec.Command("kubectl", "delete", "-f", bundleURL)
	if _, err := Run(cmd); err != nil {
		warnError(err)
	}
}

// IsPrometheusCRDsInstalled checks if any Prometheus CRDs are installed
// by verifying the existence of key CRDs related to Prometheus.
func IsPrometheusCRDsInstalled() bool {
	// List of common Prometheus CRDs
	prometheusCRDs := []string{
		"prometheuses.monitoring.coreos.com",
		"prometheusrules.monitoring.coreos.com",
		"prometheusagents.monitoring.coreos.com",
	}

	cmd := exec.Command("kubectl", "get", "crds", "-o", "custom-columns=NAME:.metadata.name")
	output, err := Run(cmd)
	if err != nil {
		return false
	}
	crdList := GetNonEmptyLines(output)
	for _, crd := range prometheusCRDs {
		for _, line := range crdList {
			if strings.Contains(line, crd) {
				return true
			}
		}
	}

	return false
}

// UninstallCertManager uninstalls the cert manager
func UninstallCertManager() {
	cmURL := fmt.Sprintf(certmanagerURLTmpl, certmanagerVersion)
	if err := validateReleaseURL(cmURL, []string{
		"/cert-manager/cert-manager/releases/download/",
	}); err != nil {
		warnError(err)
		return
	}
	// #nosec G204 - test helper executes kubectl with a pinned, validated HTTPS URL
	cmd := exec.Command("kubectl", "delete", "-f", cmURL)
	if _, err := Run(cmd); err != nil {
		warnError(err)
	}
}

// InstallCertManager installs the cert manager bundle.
func InstallCertManager() error {
	cmURL := fmt.Sprintf(certmanagerURLTmpl, certmanagerVersion)
	if err := validateReleaseURL(cmURL, []string{
		"/cert-manager/cert-manager/releases/download/",
	}); err != nil {
		return err
	}
	// #nosec G204 - test helper executes kubectl with a pinned, validated HTTPS URL
	cmd := exec.Command("kubectl", "apply", "-f", cmURL)
	if _, err := Run(cmd); err != nil {
		return err
	}
	// Wait for cert-manager-webhook to be ready, which can take time if cert-manager
	// was re-installed after uninstalling on a cluster.
	// #nosec G204 - safe constant arguments to kubectl wait
	cmd = exec.Command("kubectl", "wait", "deployment.apps/cert-manager-webhook",
		"--for", "condition=Available",
		"--namespace", "cert-manager",
		"--timeout", "5m",
	)

	_, err := Run(cmd)
	return err
}

// IsCertManagerCRDsInstalled checks if any Cert Manager CRDs are installed
// by verifying the existence of key CRDs related to Cert Manager.
func IsCertManagerCRDsInstalled() bool {
	// List of common Cert Manager CRDs
	certManagerCRDs := []string{
		"certificates.cert-manager.io",
		"issuers.cert-manager.io",
		"clusterissuers.cert-manager.io",
		"certificaterequests.cert-manager.io",
		"orders.acme.cert-manager.io",
		"challenges.acme.cert-manager.io",
	}

	// Execute the kubectl command to get all CRDs
	cmd := exec.Command("kubectl", "get", "crds")
	output, err := Run(cmd)
	if err != nil {
		return false
	}

	// Check if any of the Cert Manager CRDs are present
	crdList := GetNonEmptyLines(output)
	for _, crd := range certManagerCRDs {
		for _, line := range crdList {
			if strings.Contains(line, crd) {
				return true
			}
		}
	}

	return false
}

// LoadImageToKindClusterWithName loads a local docker image to the kind cluster
func LoadImageToKindClusterWithName(name string) error {
	cluster := "kind"
	if v, ok := os.LookupEnv("KIND_CLUSTER"); ok {
		// validate cluster name to avoid command injection via env
		if isSafeClusterName(v) {
			cluster = v
		} else {
			warnError(fmt.Errorf("invalid KIND_CLUSTER name; using default 'kind'"))
		}
	}
	kindOptions := []string{"load", "docker-image", name, "--name", cluster}
	// #nosec G204 - test helper executes 'kind' with validated/safe parameters
	cmd := exec.Command("kind", kindOptions...)
	_, err := Run(cmd)
	return err
}

// GetNonEmptyLines converts given command output string into individual objects
// according to line breakers, and ignores the empty elements in it.
func GetNonEmptyLines(output string) []string {
	var res []string
	elements := strings.Split(output, "\n")
	for _, element := range elements {
		if element != "" {
			res = append(res, element)
		}
	}

	return res
}

// GetProjectDir will return the directory where the project is
func GetProjectDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return wd, err
	}
	wd = strings.ReplaceAll(wd, "/test/e2e", "")
	return wd, nil
}

// UncommentCode searches for target in the file and remove the comment prefix
// of the target content. The target content may span multiple lines.
func UncommentCode(filename, target, prefix string) error {
	// ensure the file path is within the project directory
	projectDir, pErr := GetProjectDir()
	if pErr != nil {
		return pErr
	}
	absPath, aErr := filepath.Abs(filename)
	if aErr != nil {
		return aErr
	}
	cleanPath := filepath.Clean(absPath)
	// Prevent path traversal or writing outside the repo directory
	if !strings.HasPrefix(cleanPath, projectDir+string(filepath.Separator)) && cleanPath != projectDir {
		return fmt.Errorf("unsafe path: %s", filename)
	}
	// #nosec G304 - file path validated against project root
	content, err := os.ReadFile(cleanPath)
	if err != nil {
		return err
	}
	strContent := string(content)

	idx := strings.Index(strContent, target)
	if idx < 0 {
		return fmt.Errorf("unable to find the code %s to be uncomment", target)
	}

	out := new(bytes.Buffer)
	_, err = out.Write(content[:idx])
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(bytes.NewBufferString(target))
	if !scanner.Scan() {
		return nil
	}
	for {
		_, err := out.WriteString(strings.TrimPrefix(scanner.Text(), prefix))
		if err != nil {
			return err
		}
		// Avoid writing a newline in case the previous line was the last in target.
		if !scanner.Scan() {
			break
		}
		if _, err := out.WriteString("\n"); err != nil {
			return err
		}
	}

	_, err = out.Write(content[idx+len(target):])
	if err != nil {
		return err
	}
	// #nosec G304 - file path already validated above
	return os.WriteFile(cleanPath, out.Bytes(), 0o600)
}

// validateReleaseURL ensures the URL is HTTPS, points to github.com, and matches an allowed path prefix.
func validateReleaseURL(raw string, allowedPrefixes []string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("invalid scheme: %s", u.Scheme)
	}
	if !strings.EqualFold(u.Host, "github.com") {
		return fmt.Errorf("unexpected host: %s", u.Host)
	}
	for _, p := range allowedPrefixes {
		if strings.HasPrefix(u.Path, p) {
			return nil
		}
	}
	return fmt.Errorf("unexpected path: %s", u.Path)
}

var clusterNameRe = regexp.MustCompile(`^[a-z0-9-]+$`)

func isSafeClusterName(s string) bool {
	if len(s) == 0 || len(s) > 63 {
		return false
	}
	return clusterNameRe.MatchString(s)
}
