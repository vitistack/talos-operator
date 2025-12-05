package machine

import (
	"context"
	"fmt"
	"strings"

	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/machineclassservice"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type MachineManager struct {
	client.Client
	Scheme *runtime.Scheme
}

// NewMachineManager creates a new instance of MachineManager
func NewMachineManager(c client.Client, scheme *runtime.Scheme) *MachineManager {
	return &MachineManager{
		Client: c,
		Scheme: scheme,
	}
}

// ReconcileMachines creates or updates machine manifests based on the KubernetesCluster spec.
// Returns a list of excess machines that should be deleted (scale-down scenario).
func (m *MachineManager) ReconcileMachines(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	// Extract node information from cluster spec
	desiredMachines, err := m.GenerateMachinesFromCluster(cluster)
	if err != nil {
		return fmt.Errorf("failed to generate machines from cluster spec: %w", err)
	}

	// Build a set of desired machine names
	desiredNames := make(map[string]bool)
	for _, machine := range desiredMachines {
		desiredNames[machine.Name] = true
	}

	// Apply desired machines to Kubernetes
	for _, machine := range desiredMachines {
		if err := m.applyMachine(ctx, machine, cluster); err != nil {
			return fmt.Errorf("failed to apply machine %s: %w", machine.Name, err)
		}
	}

	return nil
}

// GetExcessMachines returns machines that exist but are not in the desired state (for scale-down).
// Separates control planes and workers for proper deletion ordering.
// Also detects workers belonging to nodepools that no longer exist.
func (m *MachineManager) GetExcessMachines(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (
	excessControlPlanes []*vitistackv1alpha1.Machine,
	excessWorkers []*vitistackv1alpha1.Machine,
	err error,
) {
	// Generate desired machines from cluster spec
	desiredMachines, err := m.GenerateMachinesFromCluster(cluster)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate machines from cluster spec: %w", err)
	}

	// Build a set of desired machine names
	desiredNames := make(map[string]bool)
	for _, machine := range desiredMachines {
		desiredNames[machine.Name] = true
	}

	// Build a set of desired nodepool names
	desiredNodePools := make(map[string]bool)
	for i := range cluster.Spec.Topology.Workers.NodePools {
		desiredNodePools[cluster.Spec.Topology.Workers.NodePools[i].Name] = true
	}

	// List all current machines for this cluster
	currentMachines, err := m.ListClusterMachines(ctx, cluster)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list cluster machines: %w", err)
	}

	// Find excess machines (exist but not desired, or belong to deleted nodepools)
	for i := range currentMachines {
		machine := &currentMachines[i]

		// Skip machines that are already being deleted (have a DeletionTimestamp)
		if machine.DeletionTimestamp != nil {
			continue
		}

		role := machine.Labels[vitistackv1alpha1.NodeRoleAnnotation]

		if role == "control-plane" {
			// Control plane: check by name
			if !desiredNames[machine.Name] {
				excessControlPlanes = append(excessControlPlanes, machine)
			}
		} else {
			// Worker: check by name OR by nodepool existence
			isExcess := false

			// Check if the machine name is not in desired list
			if !desiredNames[machine.Name] {
				isExcess = true
			}

			// Check if the worker belongs to a nodepool that no longer exists
			if nodepool, ok := machine.Annotations[vitistackv1alpha1.NodePoolAnnotation]; ok && nodepool != "" {
				if !desiredNodePools[nodepool] {
					isExcess = true
				}
			}

			if isExcess {
				excessWorkers = append(excessWorkers, machine)
			}
		}
	}

	return excessControlPlanes, excessWorkers, nil
}

// ListClusterMachines returns all machines belonging to a cluster
func (m *MachineManager) ListClusterMachines(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) ([]vitistackv1alpha1.Machine, error) {
	machineList := &vitistackv1alpha1.MachineList{}
	listOpts := []client.ListOption{
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{vitistackv1alpha1.ClusterIdAnnotation: cluster.Spec.Cluster.ClusterId},
	}

	if err := m.List(ctx, machineList, listOpts...); err != nil {
		return nil, fmt.Errorf("failed to list machines: %w", err)
	}

	return machineList.Items, nil
}

// DeleteMachine deletes a single machine resource
func (m *MachineManager) DeleteMachine(ctx context.Context, machine *vitistackv1alpha1.Machine) error {
	vlog.Info("Deleting machine resource: " + machine.Name)
	if err := m.Delete(ctx, machine); err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete machine %s: %w", machine.Name, err)
		}
	}
	return nil
}

// GenerateMachinesFromCluster extracts node information and creates Machine specs
func (m *MachineManager) GenerateMachinesFromCluster(cluster *vitistackv1alpha1.KubernetesCluster) ([]*vitistackv1alpha1.Machine, error) {
	var machines []*vitistackv1alpha1.Machine

	// Extract basic information from cluster
	clusterId := cluster.Spec.Cluster.ClusterId
	namespace := cluster.Namespace

	// Validate machine classes before generating machines
	if err := m.validateMachineClasses(context.Background(), cluster); err != nil {
		return nil, fmt.Errorf("machine class validation failed: %w", err)
	}

	// Create control plane machines
	controlPlaneMachines := m.generateControlPlaneMachines(cluster, clusterId, namespace)
	machines = append(machines, controlPlaneMachines...)

	// Create worker machines
	workerMachines := m.generateWorkerMachines(cluster, clusterId, namespace)
	machines = append(machines, workerMachines...)

	return machines, nil
}

// generateControlPlaneMachines creates control plane Machine objects from cluster spec
func (m *MachineManager) generateControlPlaneMachines(cluster *vitistackv1alpha1.KubernetesCluster, clusterId, namespace string) []*vitistackv1alpha1.Machine {
	var machines []*vitistackv1alpha1.Machine

	// Create control plane nodes (assuming at least 1)
	controlPlaneReplicas := 1
	if cluster.Spec.Topology.ControlPlane.Replicas > 0 {
		controlPlaneReplicas = cluster.Spec.Topology.ControlPlane.Replicas
	}

	// Convert control plane storage to machine disks
	controlPlaneDisks := convertStorageToDisks(cluster.Spec.Topology.ControlPlane.Storage)

	// Get control plane machine class (default to "large" if not specified)
	controlPlaneMachineClass := cluster.Spec.Topology.ControlPlane.MachineClass
	if controlPlaneMachineClass == "" {
		controlPlaneMachineClass = "large"
	}

	for i := 0; i < controlPlaneReplicas; i++ {
		machine := &vitistackv1alpha1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-ctp%d", clusterId, i),
				Namespace: namespace,
				Labels: map[string]string{
					vitistackv1alpha1.ClusterIdAnnotation: clusterId,
					vitistackv1alpha1.NodeRoleAnnotation:  "control-plane",
				},
			},
			Spec: vitistackv1alpha1.MachineSpec{
				Name:         fmt.Sprintf("%s-ctp%d", clusterId, i),
				MachineClass: controlPlaneMachineClass,
				Provider:     vitistackv1alpha1.MachineProviderType(cluster.Spec.Topology.ControlPlane.Provider.String()),
				Disks:        controlPlaneDisks,
				Tags: map[string]string{
					"cluster": clusterId,
					"role":    "control-plane",
				},
			},
		}
		machines = append(machines, machine)
	}

	return machines
}

// generateWorkerMachines creates worker Machine objects from cluster spec
func (m *MachineManager) generateWorkerMachines(cluster *vitistackv1alpha1.KubernetesCluster, clusterId, namespace string) []*vitistackv1alpha1.Machine {
	var machines []*vitistackv1alpha1.Machine

	// Create worker nodes based on node pools if available
	if len(cluster.Spec.Topology.Workers.NodePools) > 0 {
		workerIndex := 0
		for idx := range cluster.Spec.Topology.Workers.NodePools {
			nodePool := cluster.Spec.Topology.Workers.NodePools[idx]
			workerDisks := convertStorageToDisks(nodePool.Storage)
			machineClass := nodePool.MachineClass
			if machineClass == "" {
				machineClass = "medium"
			}

			for i := 0; i < nodePool.Replicas; i++ {
				machine := &vitistackv1alpha1.Machine{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("%s-wrk%d", clusterId, workerIndex),
						Namespace: namespace,
						Labels: map[string]string{
							vitistackv1alpha1.ClusterIdAnnotation: clusterId,
							vitistackv1alpha1.NodeRoleAnnotation:  "worker",
						},
						Annotations: map[string]string{
							vitistackv1alpha1.NodePoolAnnotation: nodePool.Name,
						},
					},
					Spec: vitistackv1alpha1.MachineSpec{
						Name:         fmt.Sprintf("%s-wrk%d", clusterId, workerIndex),
						MachineClass: machineClass,
						Provider:     vitistackv1alpha1.MachineProviderType(nodePool.Provider.String()),
						Disks:        workerDisks,
						Tags: map[string]string{
							"cluster":  clusterId,
							"role":     "worker",
							"nodepool": nodePool.Name,
						},
					},
				}
				machines = append(machines, machine)
				workerIndex++
			}
		}
	} else {
		// Create default worker node if no node pools are specified
		machine := &vitistackv1alpha1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-wrk0", clusterId),
				Namespace: namespace,
				Labels: map[string]string{
					vitistackv1alpha1.ClusterIdAnnotation: clusterId,
					vitistackv1alpha1.NodeRoleAnnotation:  "worker",
				},
			},
			Spec: vitistackv1alpha1.MachineSpec{
				Name:         fmt.Sprintf("%s-wrk0", clusterId),
				MachineClass: "medium",
				Tags: map[string]string{
					"cluster": clusterId,
					"role":    "worker",
				},
			},
		}
		machines = append(machines, machine)
	}

	return machines
}

// convertStorageToDisks converts KubernetesClusterStorage to MachineSpecDisk
func convertStorageToDisks(storage []vitistackv1alpha1.KubernetesClusterStorage) []vitistackv1alpha1.MachineSpecDisk {
	if len(storage) == 0 {
		return nil
	}

	disks := make([]vitistackv1alpha1.MachineSpecDisk, 0, len(storage))
	for i, s := range storage {
		disk := vitistackv1alpha1.MachineSpecDisk{
			Name: fmt.Sprintf("disk-%d", i),
			Type: s.Class,
		}

		// Parse size string (e.g., "20Gi") to GB
		if s.Size != "" {
			disk.SizeGB = parseSizeToGB(s.Size)
		}

		// First disk is the boot disk
		if i == 0 {
			disk.Boot = true
		}

		disks = append(disks, disk)
	}

	return disks
}

// sizeMultiplier defines the conversion multiplier to GB for each size suffix
var sizeMultipliers = map[string]int64{
	"Gi": 1,    // GiB to GB (approximate)
	"G":  1,    // GB
	"Mi": 0,    // MiB to GB (divide by 1024, handled specially)
	"M":  0,    // MB to GB (divide by 1024, handled specially)
	"Ti": 1024, // TiB to GB
	"T":  1024, // TB to GB
}

// parseSizeToGB converts a size string (e.g., "20Gi", "100G", "50") to GB as int64
func parseSizeToGB(size string) int64 {
	if size == "" {
		return 0
	}
	size = strings.TrimSpace(size)

	// Try known suffixes
	for suffix, multiplier := range sizeMultipliers {
		var value int64
		if n, err := fmt.Sscanf(size, "%d"+suffix, &value); err == nil && n == 1 {
			if multiplier == 0 {
				return value / 1024 // For Mi and M suffixes
			}
			return value * multiplier
		}
	}

	// Fallback: try to parse as plain integer (assume GB)
	var value int64
	if _, err := fmt.Sscanf(size, "%d", &value); err == nil {
		return value
	}
	return 0
}

// applyMachine creates or updates a Machine resource in Kubernetes
func (m *MachineManager) applyMachine(ctx context.Context, machine *vitistackv1alpha1.Machine, cluster *vitistackv1alpha1.KubernetesCluster) error {
	// Set owner reference
	if err := controllerutil.SetControllerReference(cluster, machine, m.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}

	// Check if machine already exists
	existingMachine := &vitistackv1alpha1.Machine{}
	err := m.Get(ctx, types.NamespacedName{Name: machine.Name, Namespace: machine.Namespace}, existingMachine)

	if err != nil {
		if errors.IsNotFound(err) {
			// Machine doesn't exist, create it
			vlog.Info("Creating machine: " + machine.Name)
			if err := m.Create(ctx, machine); err != nil {
				return fmt.Errorf("failed to create machine: %w", err)
			}
		} else {
			return fmt.Errorf("failed to get machine: %w", err)
		}
	} else {
		// Machine exists, update it if needed using strategic merge patch to avoid conflicts
		patch := client.MergeFrom(existingMachine.DeepCopy())
		existingMachine.Spec = machine.Spec
		existingMachine.Labels = machine.Labels
		existingMachine.Annotations = machine.Annotations
		if err := m.Patch(ctx, existingMachine, patch); err != nil {
			// If patch fails due to conflict, it's likely because status was updated
			// Log as debug and skip - the spec should be reconciled on next iteration
			if errors.IsConflict(err) {
				vlog.Debug("Machine update conflict (object modified), will retry on next reconcile: machine=" + machine.Name)
				return nil
			}
			return fmt.Errorf("failed to patch machine: %w", err)
		}
	}

	return nil
}

// CleanupMachines deletes all machines associated with a cluster
func (m *MachineManager) CleanupMachines(ctx context.Context, clusterId, namespace string) error {
	// List all machines with the cluster label
	machineList := &vitistackv1alpha1.MachineList{}
	listOpts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels{vitistackv1alpha1.ClusterIdAnnotation: clusterId},
	}

	if err := m.List(ctx, machineList, listOpts...); err != nil {
		return fmt.Errorf("failed to list machines: %w", err)
	}

	// Delete each machine
	for i := range machineList.Items {
		machine := &machineList.Items[i]
		vlog.Info("Deleting machine: " + machine.Name)
		if err := m.Delete(ctx, machine); err != nil {
			if !errors.IsNotFound(err) {
				return fmt.Errorf("failed to delete machine %s: %w", machine.Name, err)
			}
		}
	}

	vlog.Info(fmt.Sprintf("Successfully cleaned up machines: cluster=%s machineCount=%d", clusterId, len(machineList.Items)))
	return nil
}

// validateMachineClasses validates that all machine classes referenced in the cluster spec exist and are enabled
func (m *MachineManager) validateMachineClasses(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	// Collect all unique machine classes from the cluster spec
	machineClasses := make(map[string]bool)

	// Control plane machine class
	cpMachineClass := cluster.Spec.Topology.ControlPlane.MachineClass
	if cpMachineClass == "" {
		cpMachineClass = "large" // default
	}
	machineClasses[cpMachineClass] = true

	// Worker node pool machine classes
	if len(cluster.Spec.Topology.Workers.NodePools) > 0 {
		for i := range cluster.Spec.Topology.Workers.NodePools {
			nodePool := cluster.Spec.Topology.Workers.NodePools[i]
			mc := nodePool.MachineClass
			if mc == "" {
				mc = "medium" // default
			}
			machineClasses[mc] = true
		}
	} else {
		// Default worker machine class
		machineClasses["medium"] = true
	}

	// Validate each unique machine class
	for machineClassName := range machineClasses {
		if err := machineclassservice.ValidateMachineClass(ctx, machineClassName); err != nil {
			return fmt.Errorf("invalid machineClass %q: %w", machineClassName, err)
		}
		vlog.Debug(fmt.Sprintf("Validated machineClass: %s", machineClassName))
	}

	return nil
}
