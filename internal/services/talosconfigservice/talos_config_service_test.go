package talosconfigservice

import (
	"testing"

	yaml "gopkg.in/yaml.v3"
)

func TestApplyTenantOverridesMergesNestedValues(t *testing.T) {
	service := NewTalosConfigService()

	base := []byte(`machine:
  install:
    disk: /dev/vda
  network:
    hostname: default-host
`)

	overrides := map[string]any{
		"machine": map[string]any{
			"install": map[string]any{
				"disk": "/dev/custom",
			},
			"network": map[string]any{
				"hostname":    "tenant-host",
				"nameservers": []any{"10.0.0.2"},
			},
		},
	}

	out, err := service.applyTenantOverrides(base, overrides)
	if err != nil {
		t.Fatalf("applyTenantOverrides returned error: %v", err)
	}

	var actual map[string]any
	if err := yaml.Unmarshal(out, &actual); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}

	machine, ok := actual["machine"].(map[string]any)
	if !ok {
		t.Fatalf("machine section missing after merge")
	}

	install, ok := machine["install"].(map[string]any)
	if !ok {
		t.Fatalf("install section missing after merge")
	}

	if disk := install["disk"]; disk != "/dev/custom" {
		t.Fatalf("expected disk to be overridden, got %v", disk)
	}

	network, ok := machine["network"].(map[string]any)
	if !ok {
		t.Fatalf("network section missing after merge")
	}

	if hostname := network["hostname"]; hostname != "tenant-host" {
		t.Fatalf("expected hostname override, got %v", hostname)
	}

	nameservers, ok := network["nameservers"].([]any)
	if !ok || len(nameservers) != 1 || nameservers[0] != "10.0.0.2" {
		t.Fatalf("expected nameservers to be replaced, got %v", network["nameservers"])
	}
}

func TestApplyTenantOverridesNoOverrides(t *testing.T) {
	service := NewTalosConfigService()
	base := []byte("machine:\n  install:\n    disk: /dev/vda\n")

	out, err := service.applyTenantOverrides(base, nil)
	if err != nil {
		t.Fatalf("applyTenantOverrides returned error: %v", err)
	}

	if string(out) != string(base) {
		t.Fatalf("expected base config to remain unchanged when overrides are nil")
	}
}

func TestMergeRoleTemplateWithOverridesCreatesCopy(t *testing.T) {
	service := NewTalosConfigService()
	role := []byte("machine:\n  install:\n    disk: /dev/vda\n")
	overrides := map[string]any{
		"machine": map[string]any{
			"install": map[string]any{
				"disk": "/dev/custom",
			},
		},
	}

	merged, err := service.MergeRoleTemplateWithOverrides(role, overrides)
	if err != nil {
		t.Fatalf("MergeRoleTemplateWithOverrides returned error: %v", err)
	}

	if string(merged) == string(role) {
		t.Fatalf("expected merged template to differ from original role")
	}
	if string(role) != "machine:\n  install:\n    disk: /dev/vda\n" {
		t.Fatalf("expected original role to remain untouched, got %q", string(role))
	}
}

func TestMergeRoleTemplateWithOverridesSkipsWhenNoOverrides(t *testing.T) {
	service := NewTalosConfigService()
	merged, err := service.MergeRoleTemplateWithOverrides([]byte("machine: {}"), nil)
	if err != nil {
		t.Fatalf("expected nil error when overrides absent: %v", err)
	}
	if merged != nil {
		t.Fatalf("expected nil merged output without overrides")
	}
}

func TestApplyTenantOverridesPreservesAdmissionControlConfig(t *testing.T) {
	service := NewTalosConfigService()
	base := []byte("cluster:\n" +
		"  apiServer:\n" +
		"    admissionControl:\n" +
		"      - name: PodSecurity\n" +
		"        configuration:\n" +
		"          apiVersion: pod-security.admission.config.k8s.io/v1alpha1\n" +
		"          kind: PodSecurityConfiguration\n" +
		"          defaults:\n" +
		"            enforce: baseline\n" +
		"          exemptions:\n" +
		"            namespaces:\n" +
		"              - kube-system\n")

	overrides := map[string]any{
		"cluster": map[string]any{
			"apiServer": map[string]any{
				"admissionControl": []any{
					map[string]any{
						"name": "PodSecurity",
						"configuration": map[string]any{
							"exemptions": map[string]any{
								"namespaces": []any{"falco"},
							},
						},
					},
				},
			},
		},
	}

	merged, err := service.applyTenantOverrides(base, overrides)
	if err != nil {
		t.Fatalf("applyTenantOverrides returned error: %v", err)
	}

	var actual map[string]any
	if err := yaml.Unmarshal(merged, &actual); err != nil {
		t.Fatalf("failed to unmarshal merged config: %v", err)
	}

	clusterSection := actual["cluster"].(map[string]any)
	apiServer := clusterSection["apiServer"].(map[string]any)
	controls := apiServer["admissionControl"].([]any)
	if len(controls) != 1 {
		t.Fatalf("expected single admission control entry, got %d", len(controls))
	}
	entry := controls[0].(map[string]any)
	config := entry["configuration"].(map[string]any)
	if config["kind"] != "PodSecurityConfiguration" {
		t.Fatalf("expected kind to be preserved, got %v", config["kind"])
	}
	if config["apiVersion"] != "pod-security.admission.config.k8s.io/v1alpha1" {
		t.Fatalf("expected apiVersion to be preserved, got %v", config["apiVersion"])
	}
	defaults, ok := config["defaults"].(map[string]any)
	if !ok {
		t.Fatalf("expected defaults map to remain present")
	}
	if defaults["enforce"] != "baseline" {
		t.Fatalf("expected defaults.enforce to remain baseline, got %v", defaults["enforce"])
	}
	exemptions := config["exemptions"].(map[string]any)
	namespaces := exemptions["namespaces"].([]any)
	if len(namespaces) != 1 || namespaces[0] != "falco" {
		t.Fatalf("expected namespaces to be overridden, got %v", namespaces)
	}
}
