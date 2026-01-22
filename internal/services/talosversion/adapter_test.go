package talosversion

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/vitistack/talos-operator/pkg/consts"
)

func TestGetTalosVersionAdapter_V1_11(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    string
	}{
		{"v1.11.0", "v1.11.0", "1.11.x"},
		{"v1.11.6", "v1.11.6", "1.11.x"},
		{"1.11.0", "1.11.0", "1.11.x"},
		{"1.11.6", "1.11.6", "1.11.x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := GetTalosVersionAdapter(tt.version)
			if got := adapter.Version(); got != tt.want {
				t.Errorf("GetTalosVersionAdapter(%q).Version() = %q, want %q", tt.version, got, tt.want)
			}
		})
	}
}

func TestGetTalosVersionAdapter_V1_12(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    string
	}{
		{"v1.12.0", "v1.12.0", "1.12.x"},
		{"v1.12.1", "v1.12.1", "1.12.x"},
		{"v1.12.2", "v1.12.2", "1.12.x"},
		{"1.12.0", "1.12.0", "1.12.x"},
		{"1.12.2", "1.12.2", "1.12.x"},
		{"1.12.5", "1.12.5", "1.12.x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := GetTalosVersionAdapter(tt.version)
			if got := adapter.Version(); got != tt.want {
				t.Errorf("GetTalosVersionAdapter(%q).Version() = %q, want %q", tt.version, got, tt.want)
			}
		})
	}
}

func TestGetTalosVersionAdapter_FutureVersions(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    string
	}{
		{"v1.13.0", "v1.13.0", "1.13.x"},
		{"v1.14.0", "v1.14.0", "1.13.x"}, // Future versions use latest known adapter
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := GetTalosVersionAdapter(tt.version)
			if got := adapter.Version(); got != tt.want {
				t.Errorf("GetTalosVersionAdapter(%q).Version() = %q, want %q", tt.version, got, tt.want)
			}
		})
	}
}

func TestGetTalosVersionAdapter_InvalidVersion(t *testing.T) {
	// Invalid versions should fall back to 1.12.x
	adapter := GetTalosVersionAdapter("invalid")
	if got := adapter.Version(); got != "1.12.x" {
		t.Errorf("GetTalosVersionAdapter(invalid).Version() = %q, want %q", got, "1.12.x")
	}
}

func TestGetCurrentTalosVersionAdapter(t *testing.T) {
	// Set the Talos version via viper
	viper.Set(consts.TALOS_VERSION, "v1.11.6")
	defer viper.Set(consts.TALOS_VERSION, "")

	adapter := GetCurrentTalosVersionAdapter()
	if got := adapter.Version(); got != "1.11.x" {
		t.Errorf("GetCurrentTalosVersionAdapter().Version() = %q, want %q", got, "1.11.x")
	}
}

func TestBuildHostnamePatch_V1_11(t *testing.T) {
	adapter := NewV1_11Adapter()
	patch := adapter.BuildHostnamePatch("my-node")

	expected := `machine:
  network:
    hostname: my-node`

	if patch != expected {
		t.Errorf("BuildHostnamePatch() = %q, want %q", patch, expected)
	}
}

func TestBuildHostnamePatch_V1_12(t *testing.T) {
	adapter := NewV1_12Adapter()
	patch := adapter.BuildHostnamePatch("my-node")

	expected := `apiVersion: v1alpha1
kind: HostnameConfig
hostname: my-node
auto: "off"`

	if patch != expected {
		t.Errorf("BuildHostnamePatch() = %q, want %q", patch, expected)
	}
}

func TestBuildResolverPatch_V1_11(t *testing.T) {
	adapter := NewV1_11Adapter()
	patch := adapter.BuildResolverPatch([]string{"8.8.8.8", "8.8.4.4"})

	if !strings.Contains(patch, "machine:") {
		t.Error("v1.11.x resolver patch should contain 'machine:'")
	}
	if !strings.Contains(patch, "network:") {
		t.Error("v1.11.x resolver patch should contain 'network:'")
	}
	if !strings.Contains(patch, "nameservers:") {
		t.Error("v1.11.x resolver patch should contain 'nameservers:'")
	}
	if !strings.Contains(patch, "8.8.8.8") {
		t.Error("v1.11.x resolver patch should contain nameserver")
	}
}

func TestBuildResolverPatch_V1_12(t *testing.T) {
	adapter := NewV1_12Adapter()
	patch := adapter.BuildResolverPatch([]string{"8.8.8.8", "8.8.4.4"})

	if !strings.Contains(patch, "apiVersion: v1alpha1") {
		t.Error("v1.12.x resolver patch should contain 'apiVersion: v1alpha1'")
	}
	if !strings.Contains(patch, "kind: ResolverConfig") {
		t.Error("v1.12.x resolver patch should contain 'kind: ResolverConfig'")
	}
	if !strings.Contains(patch, "dnsServers:") {
		t.Error("v1.12.x resolver patch should contain 'dnsServers:'")
	}
	if !strings.Contains(patch, "8.8.8.8") {
		t.Error("v1.12.x resolver patch should contain nameserver")
	}
}

func TestBuildTimePatch_V1_11(t *testing.T) {
	adapter := NewV1_11Adapter()
	patch := adapter.BuildTimePatch([]string{"ntp.example.com"}, false, "2m0s")

	if !strings.Contains(patch, "machine:") {
		t.Error("v1.11.x time patch should contain 'machine:'")
	}
	if !strings.Contains(patch, "time:") {
		t.Error("v1.11.x time patch should contain 'time:'")
	}
	if !strings.Contains(patch, "disabled: false") {
		t.Error("v1.11.x time patch should contain 'disabled: false'")
	}
	if !strings.Contains(patch, "ntp.example.com") {
		t.Error("v1.11.x time patch should contain NTP server")
	}
}

func TestBuildTimePatch_V1_12(t *testing.T) {
	adapter := NewV1_12Adapter()
	patch := adapter.BuildTimePatch([]string{"ntp.example.com"}, false, "2m0s")

	if !strings.Contains(patch, "apiVersion: v1alpha1") {
		t.Error("v1.12.x time patch should contain 'apiVersion: v1alpha1'")
	}
	if !strings.Contains(patch, "kind: TimeConfig") {
		t.Error("v1.12.x time patch should contain 'kind: TimeConfig'")
	}
	if !strings.Contains(patch, "disabled: false") {
		t.Error("v1.12.x time patch should contain 'disabled: false'")
	}
	if !strings.Contains(patch, "ntp.example.com") {
		t.Error("v1.12.x time patch should contain NTP server")
	}
}

func TestBuildInstallDiskPatch(t *testing.T) {
	// Install disk patch is the same for all versions
	tests := []struct {
		name    string
		adapter TalosVersionAdapter
	}{
		{"v1.11.x", NewV1_11Adapter()},
		{"v1.12.x", NewV1_12Adapter()},
		{"v1.13.x", NewV1_13Adapter()},
	}

	expected := `machine:
  install:
    disk: /dev/sda`

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patch := tt.adapter.BuildInstallDiskPatch("/dev/sda")
			if patch != expected {
				t.Errorf("BuildInstallDiskPatch() = %q, want %q", patch, expected)
			}
		})
	}
}

func TestDefaultKubernetesVersion(t *testing.T) {
	tests := []struct {
		name    string
		adapter TalosVersionAdapter
		want    string
	}{
		{"v1.11.x", NewV1_11Adapter(), "1.34.1"},
		{"v1.12.x", NewV1_12Adapter(), "1.35.0"},
		{"v1.13.x", NewV1_13Adapter(), "1.36.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.adapter.DefaultKubernetesVersion(); got != tt.want {
				t.Errorf("DefaultKubernetesVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDefaultEtcdVersion(t *testing.T) {
	tests := []struct {
		name    string
		adapter TalosVersionAdapter
		want    string
	}{
		{"v1.11.x", NewV1_11Adapter(), "3.5.17"},
		{"v1.12.x", NewV1_12Adapter(), "3.6.6"},
		{"v1.13.x", NewV1_13Adapter(), "3.6.6"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.adapter.DefaultEtcdVersion(); got != tt.want {
				t.Errorf("DefaultEtcdVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEtcdImageRegistry(t *testing.T) {
	tests := []struct {
		name    string
		adapter TalosVersionAdapter
		want    string
	}{
		{"v1.11.x", NewV1_11Adapter(), "gcr.io/etcd-development/etcd"},
		{"v1.12.x", NewV1_12Adapter(), "registry.k8s.io/etcd"},
		{"v1.13.x", NewV1_13Adapter(), "registry.k8s.io/etcd"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.adapter.EtcdImageRegistry(); got != tt.want {
				t.Errorf("EtcdImageRegistry() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGrubUseUKICmdlineDefault(t *testing.T) {
	tests := []struct {
		name    string
		adapter TalosVersionAdapter
		want    bool
	}{
		{"v1.11.x", NewV1_11Adapter(), false},
		{"v1.12.x", NewV1_12Adapter(), true},
		{"v1.13.x", NewV1_13Adapter(), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.adapter.GrubUseUKICmdlineDefault(); got != tt.want {
				t.Errorf("GrubUseUKICmdlineDefault() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSupportsMultiDocConfig(t *testing.T) {
	tests := []struct {
		name    string
		adapter TalosVersionAdapter
		want    bool
	}{
		{"v1.11.x", NewV1_11Adapter(), false},
		{"v1.12.x", NewV1_12Adapter(), true},
		{"v1.13.x", NewV1_13Adapter(), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.adapter.SupportsMultiDocConfig(); got != tt.want {
				t.Errorf("SupportsMultiDocConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSupportsHostnameConfigDocument(t *testing.T) {
	tests := []struct {
		name    string
		adapter TalosVersionAdapter
		want    bool
	}{
		{"v1.11.x", NewV1_11Adapter(), false},
		{"v1.12.x", NewV1_12Adapter(), true},
		{"v1.13.x", NewV1_13Adapter(), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.adapter.SupportsHostnameConfigDocument(); got != tt.want {
				t.Errorf("SupportsHostnameConfigDocument() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestListSupportedVersions(t *testing.T) {
	versions := ListSupportedVersions()
	if len(versions) < 3 {
		t.Errorf("ListSupportedVersions() returned %d versions, expected at least 3", len(versions))
	}

	// Check that expected versions are present
	expected := []string{"1.11.x", "1.12.x", "1.13.x"}
	for _, v := range expected {
		found := false
		for _, got := range versions {
			if got == v {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("ListSupportedVersions() missing version %q", v)
		}
	}
}
