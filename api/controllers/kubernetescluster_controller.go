package controllers

import (
	"context"
	"time"

	vitistackcrdsv1alpha1 "github.com/vitistack/crds/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/talos"
	"github.com/vitistack/talos-operator/internal/machine"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// MachineReconciler reconciles a Machine object
type KubernetesClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	TalosManager   *talos.TalosManager     // Manager for Talos clusters
	MachineManager *machine.MachineManager // Manager for Machine resources
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
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete

func (r *KubernetesClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	// Fetch the KubernetesCluster instance
	var kubernetesCluster vitistackcrdsv1alpha1.KubernetesCluster
	if err := r.Get(ctx, req.NamespacedName, &kubernetesCluster); err != nil {
		// If not found, nothing to do. Finalizer-based deletion will have handled cleanup.
		return ctrl.Result{}, client.IgnoreNotFound(err)
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
	if err := r.MachineManager.ReconcileMachines(ctx, &kubernetesCluster); err != nil {
		log.Error(err, "Failed to reconcile machines")
		return ctrl.Result{}, err
	}

	// Reconcile Talos cluster after machines are created
	if err := r.TalosManager.ReconcileTalosCluster(ctx, &kubernetesCluster); err != nil {
		log.Error(err, "Failed to reconcile Talos cluster")
		// Requeue for a later retry without surfacing an error
		return ctrl.Result{RequeueAfter: time.Minute * 2}, nil
	}

	return ctrl.Result{RequeueAfter: time.Minute * 5}, nil
}

// NewKubernetesClusterReconciler creates a new KubernetesClusterReconciler with initialized managers
func NewKubernetesClusterReconciler(c client.Client, scheme *runtime.Scheme) *KubernetesClusterReconciler {
	statusManager := status.NewManager(c)
	return &KubernetesClusterReconciler{
		Client: c,
		Scheme: scheme,

		TalosManager:   talos.NewTalosManager(c, statusManager),
		MachineManager: machine.NewMachineManager(c, scheme),
	}
}

func (r *KubernetesClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&vitistackcrdsv1alpha1.KubernetesCluster{}).
		Owns(&vitistackcrdsv1alpha1.Machine{}).
		Complete(r)
}

// handleDeletion handles cleanup when a KubernetesCluster is being deleted
func (r *KubernetesClusterReconciler) handleDeletion(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	log.Info("Handling deletion of KubernetesCluster", "cluster", cluster.Name)

	// Clean up machines
	if err := r.MachineManager.CleanupMachines(ctx, cluster.Name, cluster.Namespace); err != nil {
		log.Error(err, "Failed to cleanup machines during deletion")
		return ctrl.Result{}, err
	}

	// Clean up files
	if err := r.MachineManager.CleanupMachineFiles(cluster.Name); err != nil {
		log.Error(err, "Failed to remove cluster directory")
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
