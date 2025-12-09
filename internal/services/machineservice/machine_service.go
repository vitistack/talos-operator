package machineservice

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// Machine phases
	MachinePhaseRunning = "Running"

	// Default polling settings
	DefaultMachineCheckInterval = 10 * time.Second
	DefaultMachineTimeout       = 30 * time.Second
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
	cluster *vitistackv1alpha1.KubernetesCluster) ([]*vitistackv1alpha1.Machine, error) {
	machineList := &vitistackv1alpha1.MachineList{}
	listOpts := []client.ListOption{
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{vitistackv1alpha1.ClusterIdAnnotation: cluster.Spec.Cluster.ClusterId},
	}

	if err := s.List(ctx, machineList, listOpts...); err != nil {
		return nil, fmt.Errorf("failed to list machines: %w", err)
	}

	machines := make([]*vitistackv1alpha1.Machine, len(machineList.Items))
	for i := range machineList.Items {
		machines[i] = &machineList.Items[i]
	}

	return machines, nil
}

// WaitForMachinesReady waits for all machines to be in running state with IP addresses
func (s *MachineService) WaitForMachinesReady(ctx context.Context,
	machines []*vitistackv1alpha1.Machine) ([]*vitistackv1alpha1.Machine, error) {
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
	machines []*vitistackv1alpha1.Machine) ([]*vitistackv1alpha1.Machine, bool) {
	var readyMachines []*vitistackv1alpha1.Machine

	for _, machine := range machines {
		updatedMachine := &vitistackv1alpha1.Machine{}
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
func (s *MachineService) IsMachineReady(m *vitistackv1alpha1.Machine) bool {
	if m.Status.Phase != MachinePhaseRunning {
		return false
	}

	hasNetwork := len(m.Status.PublicIPAddresses) > 0
	hasDisk := s.isDiskReady(m)

	return hasNetwork && hasDisk
}

// isDiskReady checks if the machine has a ready disk based on its provider
func (s *MachineService) isDiskReady(m *vitistackv1alpha1.Machine) bool {
	for i := range m.Status.Disks {
		disk := &m.Status.Disks[i]
		if s.isDiskReadyForProvider(disk, m.Status.Provider) {
			return true
		}
	}
	return false
}

// isDiskReadyForProvider checks if a disk is ready based on provider-specific criteria
func (s *MachineService) isDiskReadyForProvider(disk *vitistackv1alpha1.MachineStatusDisk, provider vitistackv1alpha1.MachineProviderType) bool {
	// Device is always required - it's the path Talos needs for installation
	if disk.Device == "" {
		return false
	}

	// Provider-specific checks
	if s.isKubernetesBasedProvider(provider) {
		// KubeVirt and similar Kubernetes-based providers require PVC
		return disk.PVCName != ""
	}

	// For non-Kubernetes providers (Proxmox, VMware, bare-metal, etc.)
	// the device path is sufficient
	return true
}

// isKubernetesBasedProvider checks if the provider is Kubernetes-based (uses PVCs for storage)
func (s *MachineService) isKubernetesBasedProvider(provider vitistackv1alpha1.MachineProviderType) bool {
	switch provider {
	case vitistackv1alpha1.MachineProviderTypeKubevirt:
		return true
	default:
		return false
	}
}

// FilterMachinesByRole filters machines by their role and sorts them by name
// to ensure consistent ordering (e.g., ctp0 before ctp1, ctp2)
func (s *MachineService) FilterMachinesByRole(
	machines []*vitistackv1alpha1.Machine,
	role string) []*vitistackv1alpha1.Machine {
	var filtered []*vitistackv1alpha1.Machine
	for _, machine := range machines {
		if machine.Labels[vitistackv1alpha1.NodeRoleAnnotation] == role {
			filtered = append(filtered, machine)
		}
	}
	// Sort by name to ensure consistent ordering
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Name < filtered[j].Name
	})
	return filtered
}
