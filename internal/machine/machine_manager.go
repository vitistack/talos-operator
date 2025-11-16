package machine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/pkg/consts"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/yaml"
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

// ReconcileMachines creates or updates machine manifests based on the KubernetesCluster spec
func (m *MachineManager) ReconcileMachines(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster) error {
	// Extract node information from cluster spec
	machines, err := m.GenerateMachinesFromCluster(cluster)
	if err != nil {
		return fmt.Errorf("failed to generate machines from cluster spec: %w", err)
	}

	persistMachineManifests := viper.GetBool(consts.PERSIST_MACHINE_MANIFESTS)
	// Save machines to files
	if persistMachineManifests {
		if err := m.SaveMachinesToFiles(machines, cluster.Spec.Cluster.ClusterId); err != nil {
			vlog.Error("Failed to save machines to files", err)
			// Don't fail reconciliation if file save fails, but log the error
		}
	}

	// Apply machines to Kubernetes
	for _, machine := range machines {
		if err := m.applyMachine(ctx, machine, cluster); err != nil {
			return fmt.Errorf("failed to apply machine %s: %w", machine.Name, err)
		}
	}

	return nil
}

// GenerateMachinesFromCluster extracts node information and creates Machine specs
func (m *MachineManager) GenerateMachinesFromCluster(cluster *vitistackcrdsv1alpha1.KubernetesCluster) ([]*vitistackcrdsv1alpha1.Machine, error) {
	var machines []*vitistackcrdsv1alpha1.Machine

	// Extract basic information from cluster
	clusterId := cluster.Spec.Cluster.ClusterId
	namespace := cluster.Namespace

	// Create control plane nodes (assuming at least 1)
	controlPlaneReplicas := 1
	if cluster.Spec.Topology.ControlPlane.Replicas > 0 {
		controlPlaneReplicas = cluster.Spec.Topology.ControlPlane.Replicas
	}

	for i := 0; i < controlPlaneReplicas; i++ {
		machine := &vitistackcrdsv1alpha1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-ctp%d", clusterId, i),
				Namespace: namespace,
				Labels: map[string]string{
					"cluster.vitistack.io/cluster-id": clusterId,
					"cluster.vitistack.io/role":       "control-plane",
				},
			},
			Spec: vitistackcrdsv1alpha1.MachineSpec{
				Name: fmt.Sprintf("%s-ctp%d", clusterId, i),
				// We'll set basic required fields from the machine CRD spec
				InstanceType: "large", // Default value, can be overridden from cluster spec
				Provider:     cluster.Spec.Topology.ControlPlane.Provider,
				Tags: map[string]string{
					"cluster": clusterId,
					"role":    "control-plane",
				},
			},
		}
		machines = append(machines, machine)
	}

	// Create worker nodes based on node pools if available
	if len(cluster.Spec.Topology.Workers.NodePools) > 0 {
		workerIndex := 0
		for idx := range cluster.Spec.Topology.Workers.NodePools {
			nodePool := cluster.Spec.Topology.Workers.NodePools[idx]
			for i := 0; i < nodePool.Replicas; i++ {
				virtualMachine := &vitistackcrdsv1alpha1.Machine{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("%s-wrk%d", clusterId, workerIndex),
						Namespace: namespace,
						Labels: map[string]string{
							"cluster.vitistack.io/cluster-id": clusterId,
							"cluster.vitistack.io/role":       "worker",
						},
						Annotations: map[string]string{
							"cluster.vitistack.io/nodepool": nodePool.Name,
						},
					},
					Spec: vitistackcrdsv1alpha1.MachineSpec{
						Name:         fmt.Sprintf("%s-wrk%d", clusterId, workerIndex),
						InstanceType: nodePool.MachineClass,
						Provider:     nodePool.Provider,
						Tags: map[string]string{
							"cluster":  clusterId,
							"role":     "worker",
							"nodepool": nodePool.Name,
						},
					},
				}
				machines = append(machines, virtualMachine)
				workerIndex++
			}
		}
	} else {
		// Create default worker nodes if no node pools are specified
		for i := 0; i < 1; i++ {
			machine := &vitistackcrdsv1alpha1.Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-wrk%d", clusterId, i),
					Namespace: namespace,
					Labels: map[string]string{
						"cluster.vitistack.io/cluster-id": clusterId,
						"cluster.vitistack.io/role":       "worker",
					},
				},
				Spec: vitistackcrdsv1alpha1.MachineSpec{
					Name:         fmt.Sprintf("%s-wrk%d", clusterId, i),
					InstanceType: "medium",
					Tags: map[string]string{
						"cluster": clusterId,
						"role":    "worker",
					},
				},
			}
			machines = append(machines, machine)
		}
	}

	return machines, nil
}

// SaveMachinesToFiles saves machine manifests as YAML files in the configured path
func (m *MachineManager) SaveMachinesToFiles(machines []*vitistackcrdsv1alpha1.Machine, clusterId string) error {
	baseDir := viper.GetString(consts.MACHINE_MANIFESTS_PATH)
	clusterDir := filepath.Join(baseDir, clusterId)

	// Create cluster-specific directory
	if err := os.MkdirAll(clusterDir, 0750); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", clusterDir, err)
	}

	for _, machine := range machines {
		// Convert machine to YAML
		machineYAML, err := yaml.Marshal(machine)
		if err != nil {
			return fmt.Errorf("failed to marshal machine %s to YAML: %w", machine.Name, err)
		}

		// Save to file
		filename := filepath.Join(clusterDir, fmt.Sprintf("%s.yaml", machine.Name))
		if err := os.WriteFile(filename, machineYAML, 0600); err != nil {
			return fmt.Errorf("failed to write machine file %s: %w", filename, err)
		}
	}

	return nil
}

// applyMachine creates or updates a Machine resource in Kubernetes
func (m *MachineManager) applyMachine(ctx context.Context, machine *vitistackcrdsv1alpha1.Machine, cluster *vitistackcrdsv1alpha1.KubernetesCluster) error {
	// Set owner reference
	if err := controllerutil.SetControllerReference(cluster, machine, m.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}

	// Check if machine already exists
	existingMachine := &vitistackcrdsv1alpha1.Machine{}
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
	machineList := &vitistackcrdsv1alpha1.MachineList{}
	listOpts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels{"cluster.vitistack.io/cluster-id": clusterId},
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

// CleanupMachineFiles removes machine manifest files for a cluster
func (m *MachineManager) CleanupMachineFiles(clusterId string) error {
	baseDir := viper.GetString(consts.MACHINE_MANIFESTS_PATH)
	clusterDir := filepath.Join(baseDir, clusterId)

	if err := os.RemoveAll(clusterDir); err != nil {
		return fmt.Errorf("failed to remove cluster directory %s: %w", clusterDir, err)
	}

	return nil
}
