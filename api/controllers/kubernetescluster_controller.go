package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

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

// MachineReconciler reconciles a Machine object
type KubernetesClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	KubernetesClusterFinalizer = "kubernetescluster.vitistack.io/finalizer"
)

// +kubebuilder:rbac:groups=vitistack.io,resources=kubernetesclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=vitistack.io,resources=kubernetesclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=vitistack.io,resources=kubernetesclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=vitistack.io,resources=machines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=vitistack.io,resources=machines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines/status,verbs=get

func (r *KubernetesClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	// Fetch the KubernetesCluster instance
	var kubernetesCluster vitistackcrdsv1alpha1.KubernetesCluster
	if err := r.Get(ctx, req.NamespacedName, &kubernetesCluster); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// Resource was deleted - clean up any associated machines
			log.Info("KubernetesCluster resource deleted - cleaning up machines", "name", req.Name, "namespace", req.Namespace)
			return r.cleanupMachines(ctx, req.Name, req.Namespace)
		}
		log.Error(err, "unable to fetch KubernetesCluster")
		return ctrl.Result{}, err
	}

	log.Info("Reconciling KubernetesCluster", "name", kubernetesCluster.Name, "namespace", kubernetesCluster.Namespace)

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(&kubernetesCluster, KubernetesClusterFinalizer) {
		controllerutil.AddFinalizer(&kubernetesCluster, KubernetesClusterFinalizer)
		if err := r.Update(ctx, &kubernetesCluster); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Handle deletion
	if kubernetesCluster.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, &kubernetesCluster)
	}

	// Reconcile machines based on cluster spec
	if err := r.reconcileMachines(ctx, &kubernetesCluster); err != nil {
		log.Error(err, "Failed to reconcile machines")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: time.Minute * 5}, nil
}

// reconcileMachines creates or updates machine manifests based on the KubernetesCluster spec
func (r *KubernetesClusterReconciler) reconcileMachines(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster) error {
	log := ctrl.LoggerFrom(ctx)

	// Extract node information from cluster spec
	machines, err := r.GenerateMachinesFromCluster(cluster)
	if err != nil {
		return fmt.Errorf("failed to generate machines from cluster spec: %w", err)
	}

	persistMachineManifests := viper.GetBool(consts.PERSIST_MACHINE_MANIFESTS)
	// Save machines to files
	if persistMachineManifests {
		if err := r.SaveMachinesToFiles(machines, cluster.Name); err != nil {
			log.Error(err, "Failed to save machines to files")
			// Don't fail reconciliation if file save fails, but log the error
		}
	}

	// Apply machines to Kubernetes
	for _, machine := range machines {
		if err := r.applyMachine(ctx, machine, cluster); err != nil {
			return fmt.Errorf("failed to apply machine %s: %w", machine.Name, err)
		}
	}

	log.Info("Successfully reconciled machines", "cluster", cluster.Name, "machineCount", len(machines))
	return nil
}

// GenerateMachinesFromCluster extracts node information and creates Machine specs
func (r *KubernetesClusterReconciler) GenerateMachinesFromCluster(cluster *vitistackcrdsv1alpha1.KubernetesCluster) ([]*vitistackcrdsv1alpha1.Machine, error) {
	var machines []*vitistackcrdsv1alpha1.Machine

	// Extract basic information from cluster
	clusterName := cluster.Name
	namespace := cluster.Namespace

	// We'll create a simple example with basic node structure
	// This will be refined once we understand the exact field structure

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
				InstanceType: "standard", // Default value, can be overridden from cluster spec
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
		for _, nodePool := range cluster.Spec.Topology.Workers.NodePools {
			for i := 0; i < nodePool.Replicas; i++ {
				machine := &vitistackcrdsv1alpha1.Machine{
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
				machines = append(machines, machine)
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
					InstanceType: "standard",
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

// SaveMachinesToFiles saves machine manifests as YAML files in hack/results/
func (r *KubernetesClusterReconciler) SaveMachinesToFiles(machines []*vitistackcrdsv1alpha1.Machine, clusterName string) error {
	baseDir := viper.GetString(consts.MACHINE_MANIFESTS_PATH)
	clusterDir := filepath.Join(baseDir, clusterName)

	// Create cluster-specific directory
	if err := os.MkdirAll(clusterDir, 0755); err != nil {
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
		if err := os.WriteFile(filename, machineYAML, 0644); err != nil {
			return fmt.Errorf("failed to write machine file %s: %w", filename, err)
		}
	}

	return nil
}

// applyMachine creates or updates a Machine resource in Kubernetes
func (r *KubernetesClusterReconciler) applyMachine(ctx context.Context, machine *vitistackcrdsv1alpha1.Machine, cluster *vitistackcrdsv1alpha1.KubernetesCluster) error {
	log := ctrl.LoggerFrom(ctx)

	// Set owner reference
	if err := controllerutil.SetControllerReference(cluster, machine, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}

	// Check if machine already exists
	existingMachine := &vitistackcrdsv1alpha1.Machine{}
	err := r.Get(ctx, types.NamespacedName{Name: machine.Name, Namespace: machine.Namespace}, existingMachine)

	if err != nil {
		if errors.IsNotFound(err) {
			// Machine doesn't exist, create it
			log.Info("Creating machine", "machine", machine.Name)
			if err := r.Create(ctx, machine); err != nil {
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
		if err := r.Update(ctx, existingMachine); err != nil {
			return fmt.Errorf("failed to update machine: %w", err)
		}
	}

	return nil
}

// handleDeletion handles cleanup when a KubernetesCluster is being deleted
func (r *KubernetesClusterReconciler) handleDeletion(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	log.Info("Handling deletion of KubernetesCluster", "cluster", cluster.Name)

	// Clean up machines
	if _, err := r.cleanupMachines(ctx, cluster.Name, cluster.Namespace); err != nil {
		log.Error(err, "Failed to cleanup machines during deletion")
		return ctrl.Result{}, err
	}

	// Clean up files
	clusterDir := filepath.Join("hack/results", cluster.Name)
	if err := os.RemoveAll(clusterDir); err != nil {
		log.Error(err, "Failed to remove cluster directory", "directory", clusterDir)
		// Don't fail the deletion if file cleanup fails
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(cluster, KubernetesClusterFinalizer)
	if err := r.Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("Successfully deleted KubernetesCluster", "cluster", cluster.Name)
	return ctrl.Result{}, nil
}

// cleanupMachines deletes all machines associated with a cluster
func (r *KubernetesClusterReconciler) cleanupMachines(ctx context.Context, clusterName, namespace string) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	// List all machines with the cluster label
	machineList := &vitistackcrdsv1alpha1.MachineList{}
	listOpts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels{"cluster.vitistack.io/cluster-name": clusterName},
	}

	if err := r.List(ctx, machineList, listOpts...); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list machines: %w", err)
	}

	// Delete each machine
	for _, machine := range machineList.Items {
		log.Info("Deleting machine", "machine", machine.Name)
		if err := r.Delete(ctx, &machine); err != nil {
			if !errors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("failed to delete machine %s: %w", machine.Name, err)
			}
		}
	}

	log.Info("Successfully cleaned up machines", "cluster", clusterName, "machineCount", len(machineList.Items))
	return ctrl.Result{}, nil
}

func (r *KubernetesClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&vitistackcrdsv1alpha1.KubernetesCluster{}).
		Owns(&vitistackcrdsv1alpha1.Machine{}).
		Complete(r)
}
