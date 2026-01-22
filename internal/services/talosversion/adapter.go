package talosversion

import (
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	"github.com/vitistack/talos-operator/pkg/consts"
)

// TalosVersionAdapter provides version-specific configuration generation for different Talos versions.
// Implement this interface to add support for new Talos versions with different config formats.
type TalosVersionAdapter interface {
	// Version returns the Talos version range this adapter handles (e.g., "1.11.x", "1.12.x")
	Version() string

	// BuildHostnamePatch creates a hostname configuration patch for this Talos version
	BuildHostnamePatch(hostname string) string

	// BuildResolverPatch creates a nameservers/DNS resolver configuration patch
	// In v1.11.x this is under machine.network.nameservers
	// In v1.12.x+ this uses the ResolverConfig document
	BuildResolverPatch(nameservers []string) string

	// BuildInstallDiskPatch creates an install disk configuration patch
	BuildInstallDiskPatch(disk string) string

	// BuildVMInstallImagePatch creates a VM install image patch (for install.image)
	BuildVMInstallImagePatch(installImage string) string

	// BuildTimePatch creates a time/NTP configuration patch
	// In v1.11.x this is under machine.time
	// In v1.12.x+ this uses the TimeConfig document
	BuildTimePatch(servers []string, disabled bool, bootTimeout string) string

	// SupportsMultiDocConfig returns true if this version uses multi-doc configuration
	// (HostnameConfig, ResolverConfig, TimeConfig, etc.)
	SupportsMultiDocConfig() bool

	// SupportsHostnameConfigDocument returns true if this version uses the HostnameConfig document.
	//
	// Deprecated: Use SupportsMultiDocConfig instead.
	SupportsHostnameConfigDocument() bool

	// DefaultKubernetesVersion returns the recommended default Kubernetes version for this Talos version
	DefaultKubernetesVersion() string

	// DefaultEtcdVersion returns the default etcd version for this Talos version
	DefaultEtcdVersion() string

	// EtcdImageRegistry returns the etcd image registry for this Talos version
	// v1.11.x uses gcr.io/etcd-development/etcd
	// v1.12.x+ uses registry.k8s.io/etcd
	EtcdImageRegistry() string

	// GrubUseUKICmdlineDefault returns the default value for grubUseUKICmdline
	// This changed from false to true in v1.12.x
	GrubUseUKICmdlineDefault() bool
}

// GetTalosVersionAdapter returns the appropriate adapter for the given Talos version string.
// Version can be in format "v1.11.6", "1.11.6", "v1.12.2", etc.
func GetTalosVersionAdapter(version string) TalosVersionAdapter {
	// Strip leading 'v' if present
	cleanVersion := strings.TrimPrefix(version, "v")

	// Parse the version
	v, err := semver.NewVersion(cleanVersion)
	if err != nil {
		vlog.Warn("Failed to parse Talos version \"%s\", using latest adapter (1.12.x): %v", version, err)
		return NewV1_12Adapter()
	}

	// Check major.minor version and return appropriate adapter
	minor := v.Minor()

	switch {
	case minor <= 11:
		vlog.Info("Using Talos v1.11.x adapter for version %s", cleanVersion)
		return NewV1_11Adapter()
	case minor == 12:
		vlog.Info("Using Talos v1.12.x adapter for version %s", cleanVersion)
		return NewV1_12Adapter()
	default:
		// For v1.13+ use the latest known adapter
		vlog.Info("Using Talos v1.13.x adapter for version %s (future version)", cleanVersion)
		return NewV1_13Adapter()
	}
}

// GetCurrentTalosVersionAdapter returns the adapter for the currently configured Talos version.
func GetCurrentTalosVersionAdapter() TalosVersionAdapter {
	talosVersion := viper.GetString(consts.TALOS_VERSION)
	return GetTalosVersionAdapter(talosVersion)
}

// ListSupportedVersions returns a list of supported Talos version ranges.
func ListSupportedVersions() []string {
	return []string{"1.11.x", "1.12.x", "1.13.x"}
}
