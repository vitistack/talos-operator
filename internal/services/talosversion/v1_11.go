//nolint:dupl // Each version adapter intentionally has similar structure
package talosversion

import (
	"fmt"
	"strings"
)

// V1_11Adapter handles Talos v1.11.x configuration (legacy format)
type V1_11Adapter struct {
	baseAdapter
}

// NewV1_11Adapter creates a new v1.11.x adapter
func NewV1_11Adapter() *V1_11Adapter {
	return &V1_11Adapter{
		baseAdapter: baseAdapter{
			config: adapterConfig{
				version:           "1.11.x",
				kubernetesVersion: "1.34.1",
				etcdVersion:       "3.5.17",
				etcdRegistry:      "gcr.io/etcd-development/etcd",
				multiDoc:          false,
				grubUKICmdline:    false,
			},
		},
	}
}

func (a *V1_11Adapter) BuildHostnamePatch(hostname string) string {
	return fmt.Sprintf(`machine:
  network:
    hostname: %s`, hostname)
}

func (a *V1_11Adapter) BuildResolverPatch(nameservers []string) string {
	if len(nameservers) == 0 {
		return ""
	}
	var sb strings.Builder
	_, _ = sb.WriteString("machine:\n")
	_, _ = sb.WriteString("  network:\n")
	_, _ = sb.WriteString("    nameservers:\n")
	for _, ns := range nameservers {
		_, _ = sb.WriteString(fmt.Sprintf("      - %s\n", ns))
	}
	return strings.TrimSuffix(sb.String(), "\n")
}

func (a *V1_11Adapter) BuildTimePatch(servers []string, disabled bool, bootTimeout string) string {
	var sb strings.Builder
	_, _ = sb.WriteString("machine:\n")
	_, _ = sb.WriteString("  time:\n")
	_, _ = sb.WriteString(fmt.Sprintf("    disabled: %t\n", disabled))
	if len(servers) > 0 {
		_, _ = sb.WriteString("    servers:\n")
		for _, s := range servers {
			_, _ = sb.WriteString(fmt.Sprintf("      - %s\n", s))
		}
	}
	if bootTimeout != "" {
		_, _ = sb.WriteString(fmt.Sprintf("    bootTimeout: %s\n", bootTimeout))
	}
	return strings.TrimSuffix(sb.String(), "\n")
}
