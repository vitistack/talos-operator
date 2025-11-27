package v1alpha1

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/talos"
	"github.com/vitistack/talos-operator/internal/machine"
	"github.com/vitistack/talos-operator/internal/services/secretservice"
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

	SecretService *secretservice.SecretService
}

const (
	KubernetesClusterFinalizer = "kubernetescluster.vitistack.io/finalizer"
)

// +kubebuilder:rbac:groups=vitistack.io,resources=kubernetesclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=vitistack.io,resources=kubernetesclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=vitistack.io,resources=kubernetesclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=vitistack.io,resources=machines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=vitistack.io,resources=machines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=vitistack.io,resources=controlplanevirtualsharedips,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=vitistack.io,resources=controlplanevirtualsharedips/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines/status,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
func (r *KubernetesClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Fetch the KubernetesCluster
	kubernetesCluster := &vitistackv1alpha1.KubernetesCluster{}
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
		vlog.Warn("Failed to reconcile Talos cluster ", err)
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
func (r *KubernetesClusterReconciler) isTalosProvider(kc *vitistackv1alpha1.KubernetesCluster) bool {
	return kc.Spec.Cluster.Provider == vitistackv1alpha1.KubernetesProviderTypeTalos
}

// ensureFinalizer adds the finalizer if not present. Returns requeue=true when an update was made.
func (r *KubernetesClusterReconciler) ensureFinalizer(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster) (bool, error) {
	if controllerutil.ContainsFinalizer(kc, KubernetesClusterFinalizer) {
		return false, nil
	}

	// Retry logic to handle concurrent modifications
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Get fresh copy if retrying
		if attempt > 0 {
			freshKC := &vitistackv1alpha1.KubernetesCluster{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(kc), freshKC); err != nil {
				return false, err
			}
			kc = freshKC
			if controllerutil.ContainsFinalizer(kc, KubernetesClusterFinalizer) {
				return false, nil // Another reconcile added it
			}
		}

		controllerutil.AddFinalizer(kc, KubernetesClusterFinalizer)
		err := r.Update(ctx, kc)
		if err == nil {
			return true, nil
		}

		if apierrors.IsConflict(err) && attempt < maxRetries-1 {
			vlog.Warn("Conflict adding finalizer, retrying...")
			continue
		}
		return false, err
	}
	return false, fmt.Errorf("failed to add finalizer after %d retries", maxRetries)
}

// handleDeletion performs cleanup and removes the finalizer when present
func (r *KubernetesClusterReconciler) handleDeletion(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(kc, KubernetesClusterFinalizer) {
		return ctrl.Result{}, nil
	}

	if err := r.performCleanup(ctx, kc); err != nil {
		return ctrl.Result{}, err
	}

	return r.removeFinalizer(ctx, kc)
}

// performCleanup handles secret, machine, VIP, and file cleanup
func (r *KubernetesClusterReconciler) performCleanup(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster) error {
	// Delete ControlPlaneVirtualSharedIP
	vipName := kc.Spec.Cluster.ClusterId
	vip := &vitistackv1alpha1.ControlPlaneVirtualSharedIP{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vipName,
			Namespace: kc.GetNamespace(),
		},
	}
	if err := r.Delete(ctx, vip); err != nil && !apierrors.IsNotFound(err) {
		vlog.Error("Failed to delete ControlPlaneVirtualSharedIP: "+vipName, err)
		return err
	}
	vlog.Info("Deleted ControlPlaneVirtualSharedIP: " + vipName)

	// Delete Talos Secret
	secretName := viper.GetString(consts.SECRET_PREFIX) + kc.Spec.Cluster.ClusterId
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: kc.GetNamespace()}}
	if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
		vlog.Error("Failed to delete Talos secret: "+secretName, err)
		return err
	}
	vlog.Info("Deleted Talos secret: " + secretName)

	// Clean up machines
	if err := r.MachineManager.CleanupMachines(ctx, kc.Spec.Cluster.ClusterId, kc.GetNamespace()); err != nil {
		vlog.Error("Failed to cleanup machines during deletion", err)
		return err
	}

	return nil
}

// removeFinalizer removes the finalizer with retry logic
func (r *KubernetesClusterReconciler) removeFinalizer(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster) (ctrl.Result, error) {
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			freshKC, err := r.getFreshCluster(ctx, kc)
			if err != nil {
				return ctrl.Result{}, err
			}
			if freshKC == nil {
				// Already deleted or finalizer removed
				return ctrl.Result{}, nil
			}
			kc = freshKC
		}

		controllerutil.RemoveFinalizer(kc, KubernetesClusterFinalizer)
		err := r.Update(ctx, kc)
		if err == nil {
			vlog.Info("Successfully deleted KubernetesCluster: cluster=" + kc.GetName())
			return ctrl.Result{}, nil
		}

		if apierrors.IsConflict(err) && attempt < maxRetries-1 {
			vlog.Warn("Conflict removing finalizer, retrying...")
			continue
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, fmt.Errorf("failed to remove finalizer after %d retries", maxRetries)
}

// getFreshCluster retrieves a fresh copy of the cluster resource
func (r *KubernetesClusterReconciler) getFreshCluster(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster) (*vitistackv1alpha1.KubernetesCluster, error) {
	freshKC := &vitistackv1alpha1.KubernetesCluster{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(kc), freshKC); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if !controllerutil.ContainsFinalizer(freshKC, KubernetesClusterFinalizer) {
		return nil, nil
	}
	return freshKC, nil
}

// NewKubernetesClusterReconciler creates a new KubernetesClusterReconciler with initialized managers
func NewKubernetesClusterReconciler(c client.Client, scheme *runtime.Scheme) *KubernetesClusterReconciler {
	secretService := secretservice.NewSecretService(c)
	statusManager := status.NewManager(c, secretService)
	return &KubernetesClusterReconciler{
		Client:         c,
		Scheme:         scheme,
		SecretService:  secretService,
		TalosManager:   talos.NewTalosManager(c, statusManager),
		MachineManager: machine.NewMachineManager(c, scheme),
		StatusManager:  statusManager,
	}
}

func (r *KubernetesClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&vitistackv1alpha1.KubernetesCluster{}).
		Owns(&vitistackv1alpha1.Machine{}).
		Complete(r)
}
