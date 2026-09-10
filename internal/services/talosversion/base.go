package talosversion

import (
	"fmt"
	"strings"
)

// adapterConfig holds version-specific configuration values
type adapterConfig struct {
	version           string
	kubernetesVersion string
	// maxK8sMajor/maxK8sMinor are the highest Kubernetes version this Talos
	// release supports, taken from the published support matrix at
	// https://docs.siderolabs.com/talos/<version>/getting-started/support-matrix
	// (verified 2026-09-10):
	//
	//	Talos 1.11 -> 1.34, 1.33, 1.32, 1.31, 1.30, 1.29
	//	Talos 1.12 -> 1.35, 1.34, 1.33, 1.32, 1.31, 1.30
	//	Talos 1.13 -> 1.36, 1.35, 1.34, 1.33, 1.32, 1.31
	//
	// Unlike kubernetesVersion these are enforced, so keep them sourced from
	// the matrix rather than inferred. The matrix also defines a lower bound
	// per release, which is deliberately not modelled here — see
	// SupportsKubernetesVersion for why.
	maxK8sMajor    uint64
	maxK8sMinor    uint64
	etcdVersion    string
	etcdRegistry   string
	multiDoc       bool
	grubUKICmdline bool
}

// baseAdapter provides shared implementation for all version adapters
type baseAdapter struct {
	config adapterConfig
}

func (a *baseAdapter) Version() string {
	return a.config.version
}

func (a *baseAdapter) SupportsMultiDocConfig() bool {
	return a.config.multiDoc
}

func (a *baseAdapter) SupportsHostnameConfigDocument() bool {
	return a.config.multiDoc
}

func (a *baseAdapter) DefaultKubernetesVersion() string {
	return a.config.kubernetesVersion
}

// MaxKubernetesMinor returns the highest Kubernetes minor this Talos release
// supports. Distinct from DefaultKubernetesVersion, which is only the version
// used when none is specified: a cluster may legitimately run any patch of the
// ceiling minor, above or below that default.
func (a *baseAdapter) MaxKubernetesMinor() (major, minor uint64) {
	return a.config.maxK8sMajor, a.config.maxK8sMinor
}

func (a *baseAdapter) DefaultEtcdVersion() string {
	return a.config.etcdVersion
}

func (a *baseAdapter) EtcdImageRegistry() string {
	return a.config.etcdRegistry
}

func (a *baseAdapter) GrubUseUKICmdlineDefault() bool {
	return a.config.grubUKICmdline
}

func (a *baseAdapter) BuildInstallDiskPatch(disk string) string {
	return fmt.Sprintf(`machine:
  install:
    disk: %s`, disk)
}

func (a *baseAdapter) BuildVIPPatch(vipIP, link string) string {
	// v1.11.x: VIP is configured inline on a network interface
	return fmt.Sprintf(`machine:
  network:
    interfaces:
      - interface: %s
        vip:
          ip: %s`, link, vipIP)
}

func (a *baseAdapter) BuildLinkAliasConfigPatch(name string) string {
	// v1.11.x: multi-doc config not supported
	return ""
}

func (a *baseAdapter) BuildVMInstallImagePatch(installImage string) string {
	return fmt.Sprintf(`machine:
  install:
    image: %s`, installImage)
}

// multiDocAdapter handles Talos v1.12.x+ configuration (multi-doc format)
type multiDocAdapter struct {
	baseAdapter
}

func (a *multiDocAdapter) BuildLinkAliasConfigPatch(name string) string {
	// v1.12.x+: LinkAliasConfig maps a stable alias to a physical interface via MAC
	return fmt.Sprintf(`apiVersion: v1alpha1
kind: LinkAliasConfig
name: %s
selector:
  match: mac(link.permanent_addr) == "#MACADDRESS#"`, name)
}

func (a *multiDocAdapter) BuildVIPPatch(vipIP, link string) string {
	// v1.12.x+: VIP uses the Layer2VIPConfig document
	return fmt.Sprintf(`apiVersion: v1alpha1
kind: Layer2VIPConfig
name: %s
link: %s`, vipIP, link)
}

func (a *multiDocAdapter) BuildHostnamePatch(hostname string) string {
	// auto must be "off" when setting an explicit hostname
	return fmt.Sprintf(`apiVersion: v1alpha1
kind: HostnameConfig
hostname: %s
auto: "off"`, hostname)
}

func (a *multiDocAdapter) BuildResolverPatch(nameservers []string) string {
	if len(nameservers) == 0 {
		return ""
	}
	var sb strings.Builder
	_, _ = sb.WriteString("apiVersion: v1alpha1\n")
	_, _ = sb.WriteString("kind: ResolverConfig\n")
	_, _ = sb.WriteString("dnsServers:\n")
	for _, ns := range nameservers {
		_, _ = fmt.Fprintf(&sb, "  - %s\n", ns)
	}
	return strings.TrimSuffix(sb.String(), "\n")
}

func (a *multiDocAdapter) BuildTimePatch(servers []string, disabled bool, bootTimeout string) string {
	var sb strings.Builder
	_, _ = sb.WriteString("apiVersion: v1alpha1\n")
	_, _ = sb.WriteString("kind: TimeConfig\n")
	_, _ = fmt.Fprintf(&sb, "disabled: %t\n", disabled)
	if len(servers) > 0 {
		_, _ = sb.WriteString("servers:\n")
		for _, s := range servers {
			_, _ = fmt.Fprintf(&sb, "  - %s\n", s)
		}
	}
	if bootTimeout != "" {
		_, _ = fmt.Fprintf(&sb, "bootTimeout: %s\n", bootTimeout)
	}
	return strings.TrimSuffix(sb.String(), "\n")
}
