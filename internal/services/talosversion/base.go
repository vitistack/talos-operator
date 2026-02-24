package talosversion

import (
	"fmt"
	"strings"
)

// adapterConfig holds version-specific configuration values
type adapterConfig struct {
	version           string
	kubernetesVersion string
	etcdVersion       string
	etcdRegistry      string
	multiDoc          bool
	grubUKICmdline    bool
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

func (a *baseAdapter) BuildVMInstallImagePatch(installImage string) string {
	return fmt.Sprintf(`machine:
  install:
    image: %s`, installImage)
}

// multiDocAdapter handles Talos v1.12.x+ configuration (multi-doc format)
type multiDocAdapter struct {
	baseAdapter
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
