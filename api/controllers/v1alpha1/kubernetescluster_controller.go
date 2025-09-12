package v1alpha1

import (
	"context"
	"time"

	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackcrdsv1alpha1 "github.com/vitistack/crds/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/talos"
	"github.com/vitistack/talos-operator/internal/machine"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	// Fetch the KubernetesCluster as unstructured to avoid decoding Quantity in status
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: "vitistack.io", Version: "v1alpha1", Kind: "KubernetesCluster"})
	if err := r.Get(ctx, req.NamespacedName, u); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion first and only perform cleanup when our finalizer is present
	if u.GetDeletionTimestamp() != nil {
		return r.handleDeletion(ctx, u)
	}

	// Only reconcile clusters with spec.data.provider == "talos"
	if !r.isTalosProvider(u) {
		vlog.Info("Skipping KubernetesCluster: unsupported provider (need 'talos'): cluster=" + u.GetName())
		return ctrl.Result{}, nil
	}

	// Add finalizer if not present (only for talos-managed clusters)
	if requeue, err := r.ensureFinalizer(ctx, u); err != nil {
		vlog.Error("Failed to add finalizer", err)
		return ctrl.Result{}, err
	} else if requeue {
		return ctrl.Result{Requeue: true}, nil
	}

	// Reconcile machines based on cluster spec
	// Build typed object with Spec only for downstream managers
	kubernetesCluster, err := buildTypedClusterFromUnstructured(u)
	if err != nil {
		vlog.Error("Failed to convert spec to typed KubernetesCluster.Spec", err)
		return ctrl.Result{}, err
	}

	if err := r.MachineManager.ReconcileMachines(ctx, kubernetesCluster); err != nil {
		vlog.Error("Failed to reconcile machines", err)
		return ctrl.Result{}, err
	}

	// Reconcile Talos cluster after machines are created
	if err := r.TalosManager.ReconcileTalosCluster(ctx, kubernetesCluster); err != nil {
		vlog.Error("Failed to reconcile Talos cluster", err)
		// Requeue for a later retry without surfacing an error
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	// Update KubernetesCluster status
	if err := r.StatusManager.UpdateKubernetesClusterStatus(ctx, kubernetesCluster); err != nil {
		vlog.Error("Failed to update KubernetesCluster status", err)
	}

	return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
}

// isTalosProvider returns true if spec.data.provider == "talos"
func (r *KubernetesClusterReconciler) isTalosProvider(u *unstructured.Unstructured) bool {
	provider, _, _ := unstructured.NestedString(u.Object, "spec", "data", "provider")
	return provider == "talos"
}

// ensureFinalizer adds the finalizer if not present. Returns requeue=true when an update was made.
func (r *KubernetesClusterReconciler) ensureFinalizer(ctx context.Context, u *unstructured.Unstructured) (bool, error) {
	if controllerutil.ContainsFinalizer(u, KubernetesClusterFinalizer) {
		return false, nil
	}
	controllerutil.AddFinalizer(u, KubernetesClusterFinalizer)
	if err := r.Update(ctx, u); err != nil {
		return false, err
	}
	return true, nil
}

// handleDeletion performs cleanup and removes the finalizer when present
func (r *KubernetesClusterReconciler) handleDeletion(ctx context.Context, u *unstructured.Unstructured) (ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(u, KubernetesClusterFinalizer) {
		// Clean up machines managed by this operator
		if err := r.MachineManager.CleanupMachines(ctx, u.GetName(), u.GetNamespace()); err != nil {
			vlog.Error("Failed to cleanup machines during deletion", err)
			return ctrl.Result{}, err
		}
		// Clean up files
		if err := r.MachineManager.CleanupMachineFiles(u.GetName()); err != nil {
			vlog.Error("Failed to remove cluster directory", err)
			// don't fail deletion on file errors
		}
		// Delete consolidated Talos Secret (k8s-<cluster>)
		secretName := "k8s-" + u.GetName()
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: u.GetNamespace()}}
		if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			vlog.Error("Failed to delete Talos secret: "+secretName, err)
			return ctrl.Result{}, err
		}
		// Remove finalizer
		controllerutil.RemoveFinalizer(u, KubernetesClusterFinalizer)
		if err := r.Update(ctx, u); err != nil {
			return ctrl.Result{}, err
		}
		vlog.Info("Successfully deleted KubernetesCluster: cluster=" + u.GetName())
	}
	// If no finalizer, nothing to do for us
	return ctrl.Result{}, nil
}

// buildTypedClusterFromUnstructured constructs a typed KubernetesCluster with only metadata and spec
func buildTypedClusterFromUnstructured(u *unstructured.Unstructured) (*vitistackcrdsv1alpha1.KubernetesCluster, error) {
	var kc vitistackcrdsv1alpha1.KubernetesCluster
	kc.ObjectMeta = metav1.ObjectMeta{Name: u.GetName(), Namespace: u.GetNamespace()}
	if specMap, found, _ := unstructured.NestedMap(u.Object, "spec"); found {
		// Convert spec map into typed Spec; ignore status entirely
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(specMap, &kc.Spec); err != nil {
			return nil, err
		}
	}
	return &kc, nil
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
	// Watch the KubernetesCluster as unstructured to avoid decoding rortypes.Quantity in status
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: "vitistack.io", Version: "v1alpha1", Kind: "KubernetesCluster"})
	return ctrl.NewControllerManagedBy(mgr).
		For(u).
		Owns(&vitistackcrdsv1alpha1.Machine{}).
		Complete(r)
}
