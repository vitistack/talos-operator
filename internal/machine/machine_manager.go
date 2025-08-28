package machine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
	vitistackcrdsv1alpha1 "github.com/vitistack/crds/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/pkg/consts"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
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
	log := ctrl.LoggerFrom(ctx)

	// Extract node information from cluster spec
	machines, err := m.GenerateMachinesFromCluster(cluster)
	if err != nil {
		return fmt.Errorf("failed to generate machines from cluster spec: %w", err)
	}

	persistMachineManifests := viper.GetBool(consts.PERSIST_MACHINE_MANIFESTS)
	// Save machines to files
	if persistMachineManifests {
		if err := m.SaveMachinesToFiles(machines, cluster.Name); err != nil {
			log.Error(err, "Failed to save machines to files")
			// Don't fail reconciliation if file save fails, but log the error
		}
	}

	// Apply machines to Kubernetes
	for _, machine := range machines {
		if err := m.applyMachine(ctx, machine, cluster); err != nil {
			return fmt.Errorf("failed to apply machine %s: %w", machine.Name, err)
		}
	}

	log.Info("Successfully reconciled machines", "cluster", cluster.Name, "machineCount", len(machines))
	return nil
}

// GenerateMachinesFromCluster extracts node information and creates Machine specs
func (m *MachineManager) GenerateMachinesFromCluster(cluster *vitistackcrdsv1alpha1.KubernetesCluster) ([]*vitistackcrdsv1alpha1.Machine, error) {
	var machines []*vitistackcrdsv1alpha1.Machine

	// Extract basic information from cluster
	clusterName := cluster.Name
	namespace := cluster.Namespace

	// Create control plane nodes (assuming at least 1)
	controlPlaneReplicas := 1
	if cluster.Spec.Topology.ControlPlane.Replicas > 0 {
		controlPlaneReplicas = cluster.Spec.Topology.ControlPlane.Replicas
	}

	for i := 0; i < controlPlaneReplicas; i++ {
		machine := &vitistackcrdsv1alpha1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-control-plane-%d", clusterName, i),
				Namespace: namespace,
				Labels: map[string]string{
					"cluster.vitistack.io/cluster-name": clusterName,
					"cluster.vitistack.io/role":         "control-plane",
				},
			},
			Spec: vitistackcrdsv1alpha1.MachineSpec{
				Name: fmt.Sprintf("%s-control-plane-%d", clusterName, i),
				// We'll set basic required fields from the machine CRD spec
				InstanceType: "large", // Default value, can be overridden from cluster spec
				Tags: map[string]string{
					"cluster": clusterName,
					"role":    "control-plane",
				},
			},
		}
		machines = append(machines, machine)
	}

	// Create worker nodes based on node pools if available
	if len(cluster.Spec.Topology.Workers.NodePools) > 0 {
		for idx := range cluster.Spec.Topology.Workers.NodePools {
			nodePool := cluster.Spec.Topology.Workers.NodePools[idx]
			for i := 0; i < nodePool.Replicas; i++ {
				virtualMachine := &vitistackcrdsv1alpha1.Machine{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("%s-worker-%s-%d", clusterName, nodePool.Name, i),
						Namespace: namespace,
						Labels: map[string]string{
							"cluster.vitistack.io/cluster-name": clusterName,
							"cluster.vitistack.io/role":         "worker",
							"cluster.vitistack.io/nodepool":     nodePool.Name,
						},
					},
					Spec: vitistackcrdsv1alpha1.MachineSpec{
						Name:         fmt.Sprintf("%s-worker-%s-%d", clusterName, nodePool.Name, i),
						InstanceType: nodePool.MachineClass,
						Tags: map[string]string{
							"cluster":  clusterName,
							"role":     "worker",
							"nodepool": nodePool.Name,
						},
					},
				}
				machines = append(machines, virtualMachine)
			}
		}
	} else {
		// Create default worker nodes if no node pools are specified
		for i := 0; i < 1; i++ {
			machine := &vitistackcrdsv1alpha1.Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-worker-%d", clusterName, i),
					Namespace: namespace,
					Labels: map[string]string{
						"cluster.vitistack.io/cluster-name": clusterName,
						"cluster.vitistack.io/role":         "worker",
					},
				},
				Spec: vitistackcrdsv1alpha1.MachineSpec{
					Name:         fmt.Sprintf("%s-worker-%d", clusterName, i),
					InstanceType: "medium",
					Tags: map[string]string{
						"cluster": clusterName,
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
func (m *MachineManager) SaveMachinesToFiles(machines []*vitistackcrdsv1alpha1.Machine, clusterName string) error {
	baseDir := viper.GetString(consts.MACHINE_MANIFESTS_PATH)
	clusterDir := filepath.Join(baseDir, clusterName)

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
	log := ctrl.LoggerFrom(ctx)

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
			log.Info("Creating machine", "machine", machine.Name)
			if err := m.Create(ctx, machine); err != nil {
				return fmt.Errorf("failed to create machine: %w", err)
			}
		} else {
			return fmt.Errorf("failed to get machine: %w", err)
		}
	} else {
		// Machine exists, update it if needed
		log.Info("Updating machine", "machine", machine.Name)
		existingMachine.Spec = machine.Spec
		existingMachine.Labels = machine.Labels
		if err := m.Update(ctx, existingMachine); err != nil {
			return fmt.Errorf("failed to update machine: %w", err)
		}
	}

	return nil
}

// CleanupMachines deletes all machines associated with a cluster
func (m *MachineManager) CleanupMachines(ctx context.Context, clusterName, namespace string) error {
	log := ctrl.LoggerFrom(ctx)

	// List all machines with the cluster label
	machineList := &vitistackcrdsv1alpha1.MachineList{}
	listOpts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels{"cluster.vitistack.io/cluster-name": clusterName},
	}

	if err := m.List(ctx, machineList, listOpts...); err != nil {
		return fmt.Errorf("failed to list machines: %w", err)
	}

	// Delete each machine
	for i := range machineList.Items {
		machine := &machineList.Items[i]
		log.Info("Deleting machine", "machine", machine.Name)
		if err := m.Delete(ctx, machine); err != nil {
			if !errors.IsNotFound(err) {
				return fmt.Errorf("failed to delete machine %s: %w", machine.Name, err)
			}
		}
	}

	log.Info("Successfully cleaned up machines", "cluster", clusterName, "machineCount", len(machineList.Items))
	return nil
}

// CleanupMachineFiles removes machine manifest files for a cluster
func (m *MachineManager) CleanupMachineFiles(clusterName string) error {
	baseDir := viper.GetString(consts.MACHINE_MANIFESTS_PATH)
	clusterDir := filepath.Join(baseDir, clusterName)

	if err := os.RemoveAll(clusterDir); err != nil {
		return fmt.Errorf("failed to remove cluster directory %s: %w", clusterDir, err)
	}

	return nil
}
