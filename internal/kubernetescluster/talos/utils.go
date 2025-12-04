package talos

import (
	"net"
	"strings"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
)

// Time duration constants for better readability
const (
	secondDuration = time.Second
	minuteDuration = time.Minute
)

// nodeGroupConfig holds configuration for applying configs to a group of machines
type nodeGroupConfig struct {
	stageNum      int
	nodeType      string
	phase         string
	conditionName string
	flagName      string
	stopOnError   bool
	waitForReboot bool // whether to wait for node to reboot after applying config
}

// newNodeConfigContext holds configuration context for adding new nodes
type newNodeConfigContext struct {
	clientConfig    *clientconfig.Config
	tenantOverrides map[string]any
	endpointIP      string
}

// filterNonControlPlanes returns machines that are not control planes
func filterNonControlPlanes(machines []*vitistackv1alpha1.Machine) []*vitistackv1alpha1.Machine {
	var result []*vitistackv1alpha1.Machine
	for _, m := range machines {
		role := m.Labels[vitistackv1alpha1.NodeRoleAnnotation]
		if role != controlPlaneRole {
			result = append(result, m)
		}
	}
	return result
}

// capitalizeFirst capitalizes the first letter of a string
func capitalizeFirst(s string) string {
	if len(s) == 0 {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// extractIPv4Addresses extracts the first IPv4 address from each machine
func extractIPv4Addresses(machines []*vitistackv1alpha1.Machine) []string {
	var ips []string
	for _, m := range machines {
		for _, ipAddr := range m.Status.PublicIPAddresses {
			ip := net.ParseIP(ipAddr)
			if ip != nil && ip.To4() != nil {
				ips = append(ips, ipAddr)
				break
			}
		}
	}
	return ips
}

// getFirstIPv4 returns the first IPv4 address from a machine's public IPs
func getFirstIPv4(m *vitistackv1alpha1.Machine) string {
	for _, ipAddr := range m.Status.PublicIPAddresses {
		parsedIP := net.ParseIP(ipAddr)
		if parsedIP != nil && parsedIP.To4() != nil {
			return ipAddr
		}
	}
	return ""
}

// stringSlicesEqual checks if two string slices are equal
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
