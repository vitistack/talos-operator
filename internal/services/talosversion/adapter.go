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

	// MaxKubernetesMinor returns the highest Kubernetes minor version this Talos
	// release supports, as (major, minor). Any patch of that minor is supported,
	// which is why this is not derived from DefaultKubernetesVersion: the default
	// is one specific patch, not a ceiling.
	MaxKubernetesMinor() (major, minor uint64)

	// DefaultEtcdVersion returns the default etcd version for this Talos version
	DefaultEtcdVersion() string

	// EtcdImageRegistry returns the etcd image registry for this Talos version
	// v1.11.x uses gcr.io/etcd-development/etcd
	// v1.12.x+ uses registry.k8s.io/etcd
	EtcdImageRegistry() string

	// GrubUseUKICmdlineDefault returns the default value for grubUseUKICmdline
	// This changed from false to true in v1.12.x
	GrubUseUKICmdlineDefault() bool

	// BuildVIPPatch creates a VIP configuration patch for control plane HA.
	// In v1.11.x this uses machine.network.interfaces[].vip.ip
	// In v1.12.x+ this uses the Layer2VIPConfig document
	BuildVIPPatch(vipIP, link string) string

	// BuildLinkAliasConfigPatch creates a LinkAliasConfig document that maps a
	// stable alias name (e.g. "net0") to a physical interface via a CEL selector.
	// The selector uses #MACADDRESS# as a placeholder that is replaced per-node
	// with the actual MAC address during PrepareNodeConfig.
	// In v1.11.x this returns "" (multi-doc not supported).
	// In v1.12.x+ this returns a LinkAliasConfig document.
	BuildLinkAliasConfigPatch(name string) string
}

// GetTalosVersionAdapter returns the appropriate adapter for the given Talos version string.
// Version can be in format "v1.11.6", "1.11.6", "v1.12.2", etc.
//
// An unrecognised or unparseable version still yields a usable adapter. Callers
// that must distinguish "this adapter describes the version" from "this adapter
// is a stand-in" — anything enforcing a rule rather than generating config —
// should use GetTalosVersionAdapterFor instead.
func GetTalosVersionAdapter(version string) TalosVersionAdapter {
	adapter, _ := GetTalosVersionAdapterFor(version)
	return adapter
}

// GetTalosVersionAdapterFor resolves an adapter and reports whether it actually
// covers the requested version. exact is false when the version cannot be
// parsed, or when its minor sits outside the range the switch below knows
// about — a future Talos release, or one older than the oldest adapter. In
// those cases the returned adapter is a best-effort stand-in whose values
// describe a different release, so enforcing anything against it would be
// guesswork.
func GetTalosVersionAdapterFor(version string) (adapter TalosVersionAdapter, exact bool) {
	// Strip leading 'v' if present
	cleanVersion := strings.TrimPrefix(version, "v")

	// Parse the version
	v, err := semver.NewVersion(cleanVersion)
	if err != nil {
		vlog.Warnf("Failed to parse Talos version %q, using latest adapter (1.12.x): %v", version, err)
		return NewV1_12Adapter(), false
	}

	// Check major.minor version and return appropriate adapter
	minor := v.Minor()

	// Every adapter describes a 1.x release, so a different major is one none
	// of them can speak for — a 2.12 is not a 1.12. Adapter selection stays
	// exactly as it was; only the claim that the adapter describes the version
	// is withheld, which is what makes callers fail open.
	knownMajor := v.Major() == 1
	if !knownMajor {
		vlog.Warnf("Talos version %s has an unrecognised major; adapter values may not apply", cleanVersion)
	}

	switch {
	case minor < 11:
		vlog.Warnf("Talos version %s predates the oldest known adapter (v1.11.x); "+
			"using it as an approximation", cleanVersion)
		return NewV1_11Adapter(), false
	case minor == 11:
		vlog.Warnf("Talos v1.11.x is deprecated and support will be removed in a future release; "+
			"please migrate to v1.12.x or later (version %s)", cleanVersion)
		return NewV1_11Adapter(), knownMajor
	case minor == 12:
		vlog.Infof("Using Talos v1.12.x adapter for version %s", cleanVersion)
		return NewV1_12Adapter(), knownMajor
	case minor == 13:
		vlog.Infof("Using Talos v1.13.x adapter for version %s", cleanVersion)
		return NewV1_13Adapter(), knownMajor
	default:
		// For v1.14+ use the latest known adapter. Talos 1.14 is released and
		// supports Kubernetes up to 1.37, so this branch is currently reached
		// in practice and a v1_14 adapter is the correct fix. Until then
		// exact=false keeps callers from judging it against v1.13's ceiling.
		vlog.Infof("Using Talos v1.13.x adapter for version %s (newer than the newest adapter)", cleanVersion)
		return NewV1_13Adapter(), false
	}
}

// GetCurrentTalosVersionAdapter returns the adapter for the currently configured Talos version.
func GetCurrentTalosVersionAdapter() TalosVersionAdapter {
	talosVersion := viper.GetString(consts.TALOS_VERSION)
	return GetTalosVersionAdapter(talosVersion)
}

// ListSupportedVersions returns a list of supported Talos version ranges.
func ListSupportedVersions() []string {
	return []string{versionV1_11, versionV1_12, versionV1_13}
}
