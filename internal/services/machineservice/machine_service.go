package machineservice

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// Machine phases
	MachinePhaseRunning = "Running"

	// Default polling settings
	DefaultMachineCheckInterval = 10 * time.Second
	DefaultMachineTimeout       = 2 * time.Minute
)

// MachineService handles machine-related operations
type MachineService struct {
	client.Client
}

// NewMachineService creates a new instance of MachineService
func NewMachineService(c client.Client) *MachineService {
	return &MachineService{
		Client: c,
	}
}

// GetClusterMachines retrieves all machines associated with the cluster
func (s *MachineService) GetClusterMachines(ctx context.Context,
	cluster *vitistackcrdsv1alpha1.KubernetesCluster) ([]*vitistackcrdsv1alpha1.Machine, error) {
	machineList := &vitistackcrdsv1alpha1.MachineList{}
	listOpts := []client.ListOption{
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"cluster.vitistack.io/cluster-id": cluster.Spec.Cluster.ClusterId},
	}

	if err := s.List(ctx, machineList, listOpts...); err != nil {
		return nil, fmt.Errorf("failed to list machines: %w", err)
	}

	machines := make([]*vitistackcrdsv1alpha1.Machine, len(machineList.Items))
	for i := range machineList.Items {
		machines[i] = &machineList.Items[i]
	}

	return machines, nil
}

// WaitForMachinesReady waits for all machines to be in running state with IP addresses
func (s *MachineService) WaitForMachinesReady(ctx context.Context,
	machines []*vitistackcrdsv1alpha1.Machine) ([]*vitistackcrdsv1alpha1.Machine, error) {
	timeout := time.After(DefaultMachineTimeout)
	ticker := time.NewTicker(DefaultMachineCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			return nil, fmt.Errorf("timeout waiting for machines to be ready")
		case <-ticker.C:
			readyMachines, allReady := s.checkMachinesReady(ctx, machines)
			if allReady {
				vlog.Info(fmt.Sprintf("All machines are ready: count=%d", len(readyMachines)))
				return readyMachines, nil
			}
			vlog.Info(fmt.Sprintf("Waiting for machines to be ready: ready=%d total=%d", len(readyMachines), len(machines)))
		}
	}
}

// checkMachinesReady checks if all machines are in running state with IP addresses
func (s *MachineService) checkMachinesReady(ctx context.Context,
	machines []*vitistackcrdsv1alpha1.Machine) ([]*vitistackcrdsv1alpha1.Machine, bool) {
	var readyMachines []*vitistackcrdsv1alpha1.Machine

	for _, machine := range machines {
		updatedMachine := &vitistackcrdsv1alpha1.Machine{}
		if err := s.Get(ctx, types.NamespacedName{
			Name:      machine.Name,
			Namespace: machine.Namespace,
		}, updatedMachine); err != nil {
			continue
		}

		if s.IsMachineReady(updatedMachine) {
			readyMachines = append(readyMachines, updatedMachine)
		}
	}

	return readyMachines, len(readyMachines) == len(machines)
}

// IsMachineReady checks if a machine is ready for Talos cluster creation
func (s *MachineService) IsMachineReady(m *vitistackcrdsv1alpha1.Machine) bool {
	if m.Status.Phase != MachinePhaseRunning {
		return false
	}

	hasNetwork := false
	hasDisk := false

	for i := range m.Status.Disks {
		disk := &m.Status.Disks[i]
		if disk.PVCName != "" && disk.Device != "" {
			hasDisk = true
			break
		}
	}

	for i := range m.Status.NetworkInterfaces {
		iface := &m.Status.NetworkInterfaces[i]
		if iface.MACAddress != "" && (len(iface.IPAddresses) > 0 || len(iface.IPv6Addresses) > 0) {
			hasNetwork = true
			break
		}
	}

	return hasNetwork && hasDisk
}

// FilterMachinesByRole filters machines by their role
func (s *MachineService) FilterMachinesByRole(
	machines []*vitistackcrdsv1alpha1.Machine,
	role string) []*vitistackcrdsv1alpha1.Machine {
	var filtered []*vitistackcrdsv1alpha1.Machine
	for _, machine := range machines {
		if machine.Labels["cluster.vitistack.io/role"] == role {
			filtered = append(filtered, machine)
		}
	}
	return filtered
}

// GetIPv4Address returns the first IPv4 address from a machine
func (s *MachineService) GetIPv4Address(m *vitistackcrdsv1alpha1.Machine) string {
	if len(m.Status.NetworkInterfaces) == 0 {
		return ""
	}

	for _, ipAddr := range m.Status.NetworkInterfaces[0].IPAddresses {
		ip := net.ParseIP(ipAddr)
		if ip != nil && ip.To4() != nil {
			return ipAddr
		}
	}
	return ""
}

// GetControlPlaneIPs collects IPv4 addresses from control plane machines
func (s *MachineService) GetControlPlaneIPs(machines []*vitistackcrdsv1alpha1.Machine) []string {
	var ips []string
	for _, m := range machines {
		if ip := s.GetIPv4Address(m); ip != "" {
			ips = append(ips, ip)
		}
	}
	return ips
}
