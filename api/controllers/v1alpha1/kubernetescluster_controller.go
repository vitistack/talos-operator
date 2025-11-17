package v1alpha1

import (
	"context"
	"time"

	"github.com/NorskHelsenett/ror/pkg/kubernetes/providers/providermodels"
	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/talos"
	"github.com/vitistack/talos-operator/internal/machine"
	"github.com/vitistack/talos-operator/pkg/consts"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	StatusManager  *status.StatusManager   // Manager for status updates
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
	// Fetch the KubernetesCluster
	kubernetesCluster := &vitistackcrdsv1alpha1.KubernetesCluster{}
	if err := r.Get(ctx, req.NamespacedName, kubernetesCluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion first and only perform cleanup when our finalizer is present
	if kubernetesCluster.GetDeletionTimestamp() != nil {
		return r.handleDeletion(ctx, kubernetesCluster)
	}

	// Only reconcile clusters with spec.cluster.provider == "talos"
	if !r.isTalosProvider(kubernetesCluster) {
		vlog.Info("Skipping KubernetesCluster: unsupported provider (need 'talos'): cluster=" + kubernetesCluster.GetName())
		return ctrl.Result{}, nil
	}

	// Add finalizer if not present (only for talos-managed clusters)
	if requeue, err := r.ensureFinalizer(ctx, kubernetesCluster); err != nil {
		vlog.Error("Failed to add finalizer", err)
		return ctrl.Result{}, err
	} else if requeue {
		return ctrl.Result{Requeue: true}, nil
	}

	// Reconcile machines based on cluster spec
	if err := r.MachineManager.ReconcileMachines(ctx, kubernetesCluster); err != nil {
		vlog.Error("Failed to reconcile machines", err)
		return ctrl.Result{}, err
	}

	// Reconcile Talos cluster after machines are created
	if err := r.TalosManager.ReconcileTalosCluster(ctx, kubernetesCluster); err != nil {
		vlog.Error("Failed to reconcile Talos cluster", err)
		// Requeue for a later retry without surfacing an error
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Update KubernetesCluster status (skip if being deleted)
	if kubernetesCluster.GetDeletionTimestamp() == nil {
		if err := r.StatusManager.UpdateKubernetesClusterStatus(ctx, kubernetesCluster); err != nil {
			vlog.Error("Failed to update KubernetesCluster status", err)
		}
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// isTalosProvider returns true if spec.data.provider == "talos"
// TODO: Fix this - need to determine how to access spec.data.provider from typed struct
func (r *KubernetesClusterReconciler) isTalosProvider(kc *vitistackcrdsv1alpha1.KubernetesCluster) bool {
	return kc.Spec.Cluster.Provider == providermodels.ProviderTypeTalos.String()
}

// ensureFinalizer adds the finalizer if not present. Returns requeue=true when an update was made.
func (r *KubernetesClusterReconciler) ensureFinalizer(ctx context.Context, kc *vitistackcrdsv1alpha1.KubernetesCluster) (bool, error) {
	if controllerutil.ContainsFinalizer(kc, KubernetesClusterFinalizer) {
		return false, nil
	}
	controllerutil.AddFinalizer(kc, KubernetesClusterFinalizer)
	if err := r.Update(ctx, kc); err != nil {
		return false, err
	}
	return true, nil
}

// handleDeletion performs cleanup and removes the finalizer when present
func (r *KubernetesClusterReconciler) handleDeletion(ctx context.Context, kc *vitistackcrdsv1alpha1.KubernetesCluster) (ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(kc, KubernetesClusterFinalizer) {
		// Delete consolidated Talos Secret FIRST (before finalizer removal)
		// This ensures the secret is deleted even if finalizer removal fails
		// Note: Secret name uses cluster.Spec.Cluster.ClusterId
		secretName := viper.GetString(consts.SECRET_PREFIX) + kc.Spec.Cluster.ClusterId
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: kc.GetNamespace()}}
		if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			vlog.Error("Failed to delete Talos secret: "+secretName, err)
			return ctrl.Result{}, err
		}
		vlog.Info("Deleted Talos secret: " + secretName)

		// Clean up machines managed by this operator
		if err := r.MachineManager.CleanupMachines(ctx, kc.Spec.Cluster.ClusterId, kc.GetNamespace()); err != nil {
			vlog.Error("Failed to cleanup machines during deletion", err)
			return ctrl.Result{}, err
		}
		// Clean up files
		if err := r.MachineManager.CleanupMachineFiles(kc.Spec.Cluster.ClusterId); err != nil {
			vlog.Error("Failed to remove cluster directory", err)
			// don't fail deletion on file errors
		}
		// Remove finalizer
		controllerutil.RemoveFinalizer(kc, KubernetesClusterFinalizer)
		if err := r.Update(ctx, kc); err != nil {
			return ctrl.Result{}, err
		}
		vlog.Info("Successfully deleted KubernetesCluster: cluster=" + kc.GetName())
	}
	// If no finalizer, nothing to do for us
	return ctrl.Result{}, nil
}

// NewKubernetesClusterReconciler creates a new KubernetesClusterReconciler with initialized managers
func NewKubernetesClusterReconciler(c client.Client, scheme *runtime.Scheme) *KubernetesClusterReconciler {
	statusManager := status.NewManager(c)
	return &KubernetesClusterReconciler{
		Client: c,
		Scheme: scheme,

		TalosManager:   talos.NewTalosManager(c, statusManager),
		MachineManager: machine.NewMachineManager(c, scheme),
		StatusManager:  statusManager,
	}
}

func (r *KubernetesClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&vitistackcrdsv1alpha1.KubernetesCluster{}).
		Owns(&vitistackcrdsv1alpha1.Machine{}).
		Complete(r)
}
