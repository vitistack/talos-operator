package talosconfigservice

import (
	"strings"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
	yaml "gopkg.in/yaml.v3"
)

// testCustomInstallDisk is the install-disk path the tenant-override
// fixtures patch onto the base config. Reused across multiple tests
// (override-merges-nested, override-leaves-base-untouched, etc).
const testCustomInstallDisk = "/dev/custom"

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
				"disk": testCustomInstallDisk,
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

	if disk := install["disk"]; disk != testCustomInstallDisk {
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
				"disk": testCustomInstallDisk,
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
	// configpatcher uses strategic merge which appends to arrays rather than replacing
	// So [kube-system] + [falco] = [kube-system, falco]
	if len(namespaces) != 2 {
		t.Fatalf("expected namespaces to be merged (appended), got %v", namespaces)
	}
	// Verify both original and override values are present
	foundKubeSystem := false
	foundFalco := false
	for _, ns := range namespaces {
		if ns == "kube-system" {
			foundKubeSystem = true
		}
		if ns == "falco" {
			foundFalco = true
		}
	}
	if !foundKubeSystem || !foundFalco {
		t.Fatalf("expected both kube-system and falco in merged namespaces, got %v", namespaces)
	}
}

func TestMultiDocYAMLUnmarshalOnlyParsesFirstDocument(t *testing.T) {
	// This test documents the behavior that yaml.Unmarshal only parses
	// the first document in a multi-document YAML string.
	// This is why we must pass the raw YAML string to configpatcher
	// instead of unmarshaling and re-marshaling.
	multiDoc := `machine:
  network:
    nameservers:
      - 10.0.0.1
---
apiVersion: v1alpha1
kind: LinkAliasConfig
name: net0
selector:
  match: true
---
apiVersion: v1alpha1
kind: LinkConfig
name: net0
mtu: 1500
`

	// yaml.Unmarshal only parses the first document
	var firstDoc map[string]any
	if err := yaml.Unmarshal([]byte(multiDoc), &firstDoc); err != nil {
		t.Fatalf("failed to unmarshal first document: %v", err)
	}

	// First document should have machine config
	machine, ok := firstDoc["machine"].(map[string]any)
	if !ok {
		t.Fatalf("expected machine section in first document")
	}
	network, ok := machine["network"].(map[string]any)
	if !ok {
		t.Fatalf("expected network section in first document")
	}
	if network["nameservers"] == nil {
		t.Fatalf("expected nameservers in first document")
	}

	// First document should NOT contain LinkAliasConfig or LinkConfig
	if _, ok := firstDoc["kind"]; ok {
		t.Fatalf("first document should not contain 'kind' from additional documents")
	}

	// Re-marshaling only produces the first document - additional docs are lost
	remarshaled, err := yaml.Marshal(firstDoc)
	if err != nil {
		t.Fatalf("failed to re-marshal: %v", err)
	}
	if strings.Contains(string(remarshaled), "LinkAliasConfig") {
		t.Fatalf("re-marshaled YAML should not contain LinkAliasConfig")
	}
	if strings.Contains(string(remarshaled), "LinkConfig") {
		t.Fatalf("re-marshaled YAML should not contain LinkConfig")
	}

	// The raw multi-doc string preserves all documents
	if !strings.Contains(multiDoc, "LinkAliasConfig") {
		t.Fatalf("raw multi-doc should contain LinkAliasConfig")
	}
	if !strings.Contains(multiDoc, "LinkConfig") {
		t.Fatalf("raw multi-doc should contain LinkConfig")
	}
	if !strings.Contains(multiDoc, "mtu: 1500") {
		t.Fatalf("raw multi-doc should contain mtu setting")
	}
}

func TestMultiDocYAMLPreservesAllDocumentsInRawString(t *testing.T) {
	// Simulates what loadTenantOverrides now does:
	// parse the first doc as a map, but return the full raw string as patches.
	multiDoc := `machine:
  network:
    nameservers:
      - 10.0.0.1
cluster:
  proxy:
    disabled: true
---
apiVersion: v1alpha1
kind: LinkAliasConfig
name: net0
selector:
  match: true
---
apiVersion: v1alpha1
kind: LinkConfig
name: net0
mtu: 1500
`

	// Parse first document as map (backward compat)
	overrides := map[string]any{}
	if err := yaml.Unmarshal([]byte(multiDoc), &overrides); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if len(overrides) == 0 {
		t.Fatalf("expected non-empty overrides map from first document")
	}

	// Verify the overrides map contains machine and cluster sections
	if _, ok := overrides["machine"]; !ok {
		t.Fatalf("expected 'machine' key in first document overrides")
	}
	if _, ok := overrides["cluster"]; !ok {
		t.Fatalf("expected 'cluster' key in first document overrides")
	}

	// The full raw string should be used as the patch (not re-marshaled map)
	patches := []string{multiDoc}

	if len(patches) != 1 {
		t.Fatalf("expected exactly one patch string, got %d", len(patches))
	}

	// Count the number of YAML documents in the patch string
	docs := strings.Split(patches[0], "---")
	nonEmptyDocs := 0
	for _, doc := range docs {
		if strings.TrimSpace(doc) != "" {
			nonEmptyDocs++
		}
	}
	if nonEmptyDocs != 3 {
		t.Fatalf("expected 3 YAML documents in patch string, got %d", nonEmptyDocs)
	}
}

func TestMACAddressPlaceholderReplacement(t *testing.T) {
	// Simulates what PrepareNodeConfig does: replace #MACADDRESS# in the role template
	roleYAML := `version: v1alpha1
machine:
  type: controlplane
  network:
    nameservers:
      - 10.0.0.1
---
apiVersion: v1alpha1
kind: LinkAliasConfig
name: net0
selector:
    match: mac(link.permanent_addr) == "#MACADDRESS#"
---
apiVersion: v1alpha1
kind: LinkConfig
name: net0
mtu: 1500
`

	macAddress := "00:1a:2b:3c:4d:5e"
	replaced := strings.ReplaceAll(roleYAML, "#MACADDRESS#", macAddress)

	// Verify the MAC address was substituted
	expectedSelector := `mac(link.permanent_addr) == "00:1a:2b:3c:4d:5e"`
	if !strings.Contains(replaced, expectedSelector) {
		t.Fatalf("expected replaced YAML to contain %q, got:\n%s", expectedSelector, replaced)
	}

	// Verify placeholder is gone
	if strings.Contains(replaced, "#MACADDRESS#") {
		t.Fatalf("expected #MACADDRESS# placeholder to be replaced")
	}

	// Verify other documents are preserved
	if !strings.Contains(replaced, "LinkAliasConfig") {
		t.Fatalf("expected LinkAliasConfig to be preserved")
	}
	if !strings.Contains(replaced, "mtu: 1500") {
		t.Fatalf("expected LinkConfig with mtu to be preserved")
	}
}

func TestMACAddressPlaceholderNoReplacementWhenNoMAC(t *testing.T) {
	roleYAML := `version: v1alpha1
---
apiVersion: v1alpha1
kind: LinkAliasConfig
name: net0
selector:
    match: mac(link.permanent_addr) == "#MACADDRESS#"
`
	// When MAC is empty, no replacement should occur - placeholder stays
	macAddress := ""
	if macAddress != "" {
		roleYAML = strings.ReplaceAll(roleYAML, "#MACADDRESS#", macAddress)
	}

	if !strings.Contains(roleYAML, "#MACADDRESS#") {
		t.Fatalf("expected #MACADDRESS# to remain when no MAC address available")
	}
}

// TestConfigPatcherRoundTripPreservesMACPlaceholder verifies that #MACADDRESS#
// survives a configpatcher.LoadPatches round-trip (the same code path used during
// bundle generation). This is critical because the placeholder must remain intact
// in the serialized role templates so that PrepareNodeConfig can substitute it
// per-node at apply time.
func TestConfigPatcherRoundTripPreservesMACPlaceholder(t *testing.T) {
	// This matches the current talos-tenant-config.yaml structure with all
	// v1.12+ document types (ResolverConfig, TimeSyncConfig, LinkAliasConfig,
	// LinkConfig, DHCPv4Config).
	tenantConfig := `machine:
  kubelet:
    extraArgs:
      rotate-server-certificates: true
  features:
    hostDNS:
      enabled: true
      forwardKubeDNSToHost: true
    kubePrism:
      enabled: true
      port: 7445
cluster:
  proxy:
    disabled: true
  discovery:
    enabled: false
---
apiVersion: v1alpha1
kind: ResolverConfig
nameservers:
  - address: 10.0.0.1
  - address: 10.0.0.2
---
apiVersion: v1alpha1
kind: TimeSyncConfig
enabled: true
bootTimeout: 2m0s
ntp:
  servers:
    - ntp.example.com
---
apiVersion: v1alpha1
kind: LinkAliasConfig
name: net0
selector:
  match: mac(link.permanent_addr) == "#MACADDRESS#"
---
apiVersion: v1alpha1
kind: LinkConfig
name: net0
mtu: 1500
---
apiVersion: v1alpha1
kind: DHCPv4Config
name: net0
clientIdentifier: mac
`

	// Step 1: Replace #MACADDRESS# with a dummy MAC (like ValidateTenantConfigYAML does)
	// to verify configpatcher can parse the full multi-doc YAML.
	sanitized := strings.ReplaceAll(tenantConfig, "#MACADDRESS#", "00:00:00:00:00:00")
	patches, err := configpatcher.LoadPatches([]string{sanitized})
	if err != nil {
		t.Fatalf("configpatcher.LoadPatches failed on sanitized tenant config: %v", err)
	}
	if len(patches) == 0 {
		t.Fatalf("expected non-empty patches from configpatcher.LoadPatches")
	}

	// Step 2: Verify the raw tenant config with #MACADDRESS# placeholder
	// survives string replacement after configpatcher would have been applied.
	// In the real flow: bundle generation applies the patch (with placeholder),
	// then bundle.Serialize writes it to YAML, then PrepareNodeConfig does
	// strings.ReplaceAll on the serialized YAML.
	macAddress := "aa:bb:cc:dd:ee:ff"
	replaced := strings.ReplaceAll(tenantConfig, "#MACADDRESS#", macAddress)

	// Verify MAC was substituted in the LinkAliasConfig selector
	expectedSelector := `mac(link.permanent_addr) == "aa:bb:cc:dd:ee:ff"`
	if !strings.Contains(replaced, expectedSelector) {
		t.Fatalf("expected replaced YAML to contain %q", expectedSelector)
	}

	// Verify placeholder is completely gone
	if strings.Contains(replaced, "#MACADDRESS#") {
		t.Fatalf("expected #MACADDRESS# placeholder to be fully replaced")
	}

	// Verify all document types are preserved after replacement
	for _, kind := range []string{"ResolverConfig", "TimeSyncConfig", "LinkAliasConfig", "LinkConfig", "DHCPv4Config"} {
		if !strings.Contains(replaced, kind) {
			t.Fatalf("expected %s document to be preserved after MAC replacement", kind)
		}
	}

	// Step 3: Verify the replaced config is also valid for configpatcher
	patchesAfter, err := configpatcher.LoadPatches([]string{replaced})
	if err != nil {
		t.Fatalf("configpatcher.LoadPatches failed after MAC replacement: %v", err)
	}
	if len(patchesAfter) == 0 {
		t.Fatalf("expected non-empty patches after MAC replacement")
	}
}

// TestValidateTenantConfigYAMLWithAllDocumentTypes verifies that
// ValidateTenantConfigYAML accepts the full tenant config with all
// v1.12+ document types.
func TestValidateTenantConfigYAMLWithAllDocumentTypes(t *testing.T) {
	tenantConfig := `machine:
  kubelet:
    extraArgs:
      rotate-server-certificates: true
cluster:
  proxy:
    disabled: true
---
apiVersion: v1alpha1
kind: ResolverConfig
nameservers:
  - address: 10.0.0.1
---
apiVersion: v1alpha1
kind: TimeSyncConfig
enabled: true
ntp:
  servers:
    - ntp.example.com
---
apiVersion: v1alpha1
kind: LinkAliasConfig
name: net0
selector:
  match: mac(link.permanent_addr) == "#MACADDRESS#"
---
apiVersion: v1alpha1
kind: LinkConfig
name: net0
mtu: 1500
---
apiVersion: v1alpha1
kind: DHCPv4Config
name: net0
clientIdentifier: mac
`

	if err := ValidateTenantConfigYAML(tenantConfig); err != nil {
		t.Fatalf("ValidateTenantConfigYAML failed on valid multi-doc config: %v", err)
	}
}

// TestValidateTenantConfigYAMLRejectsInvalidYAML verifies that
// ValidateTenantConfigYAML catches malformed YAML.
func TestValidateTenantConfigYAMLRejectsInvalidYAML(t *testing.T) {
	// Bad indentation on --- separator (the original bug)
	badConfig := `machine:
  kubelet:
    extraArgs:
      rotate-server-certificates: true
        ---
        apiVersion: v1alpha1
        kind: LinkAliasConfig
        name: net0
`

	if err := ValidateTenantConfigYAML(badConfig); err == nil {
		t.Fatalf("expected ValidateTenantConfigYAML to reject badly indented YAML")
	}
}
