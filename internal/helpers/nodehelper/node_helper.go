package nodehelper

import (
	"net"
	"strings"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
)

const controlPlaneRole = "control-plane"

// FilterNonControlPlanes returns machines that are not control planes
func FilterNonControlPlanes(machines []*vitistackv1alpha1.Machine) []*vitistackv1alpha1.Machine {
	var result []*vitistackv1alpha1.Machine
	for _, m := range machines {
		role := m.Labels[vitistackv1alpha1.NodeRoleAnnotation]
		if role != controlPlaneRole {
			result = append(result, m)
		}
	}
	return result
}

// FilterControlPlanes returns machines that are control planes
func FilterControlPlanes(machines []*vitistackv1alpha1.Machine) []*vitistackv1alpha1.Machine {
	var result []*vitistackv1alpha1.Machine
	for _, m := range machines {
		role := m.Labels[vitistackv1alpha1.NodeRoleAnnotation]
		if role == controlPlaneRole {
			result = append(result, m)
		}
	}
	return result
}

// CapitalizeFirst capitalizes the first letter of a string
func CapitalizeFirst(s string) string {
	if len(s) == 0 {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// ExtractIPv4Addresses extracts the first IPv4 address from each machine
func ExtractIPv4Addresses(machines []*vitistackv1alpha1.Machine) []string {
	var ips []string
	for _, m := range machines {
		ip := GetFirstIPv4(m)
		if ip != "" {
			ips = append(ips, ip)
		}
	}
	return ips
}

// GetFirstIPv4 returns the first IPv4 address from a machine's public IPs
func GetFirstIPv4(m *vitistackv1alpha1.Machine) string {
	for _, ipAddr := range m.Status.PublicIPAddresses {
		parsedIP := net.ParseIP(ipAddr)
		if parsedIP != nil && parsedIP.To4() != nil {
			return ipAddr
		}
	}
	return ""
}

// GetControlPlaneIPv4s extracts IPv4 addresses from control plane machines
func GetControlPlaneIPv4s(machines []*vitistackv1alpha1.Machine) []string {
	controlPlanes := FilterControlPlanes(machines)
	return ExtractIPv4Addresses(controlPlanes)
}

// IsControlPlane checks if a machine is a control plane
func IsControlPlane(m *vitistackv1alpha1.Machine) bool {
	return m.Labels[vitistackv1alpha1.NodeRoleAnnotation] == controlPlaneRole
}

// HasIPv4Address checks if a machine has at least one IPv4 address
func HasIPv4Address(m *vitistackv1alpha1.Machine) bool {
	return GetFirstIPv4(m) != ""
}

// AllMachinesHaveIPv4 checks if all machines have IPv4 addresses
func AllMachinesHaveIPv4(machines []*vitistackv1alpha1.Machine) bool {
	for _, m := range machines {
		if !HasIPv4Address(m) {
			return false
		}
	}
	return true
}

// FindMachineByName finds a machine by name in a list
func FindMachineByName(machines []*vitistackv1alpha1.Machine, name string) *vitistackv1alpha1.Machine {
	for _, m := range machines {
		if m.Name == name {
			return m
		}
	}
	return nil
}

// MachineNames returns the names of all machines
func MachineNames(machines []*vitistackv1alpha1.Machine) []string {
	names := make([]string, len(machines))
	for i, m := range machines {
		names[i] = m.Name
	}
	return names
}

// SplitFirstAndRest returns the first machine and the rest separately
func SplitFirstAndRest(machines []*vitistackv1alpha1.Machine) (*vitistackv1alpha1.Machine, []*vitistackv1alpha1.Machine) {
	if len(machines) == 0 {
		return nil, nil
	}
	return machines[0], machines[1:]
}
