package controllers

import (
	"context"
	"time"

	"github.com/NorskHelsenett/ror/pkg/rlog"
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

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(u, KubernetesClusterFinalizer) {
		controllerutil.AddFinalizer(u, KubernetesClusterFinalizer)
		if err := r.Update(ctx, u); err != nil {
			rlog.Error("Failed to add finalizer", err)
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Handle deletion
	if u.GetDeletionTimestamp() != nil {
		// Handle deletion inline using unstructured
		// Clean up machines
		if err := r.MachineManager.CleanupMachines(ctx, u.GetName(), u.GetNamespace()); err != nil {
			rlog.Error("Failed to cleanup machines during deletion", err)
			return ctrl.Result{}, err
		}
		// Clean up files
		if err := r.MachineManager.CleanupMachineFiles(u.GetName()); err != nil {
			rlog.Error("Failed to remove cluster directory", err)
			// don't fail deletion on file errors
		}
		// Delete consolidated Talos Secret (k8s-<cluster>)
		secretName := "k8s-" + u.GetName()
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: u.GetNamespace()}}
		if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			rlog.Error("Failed to delete Talos secret: "+secretName, err)
			return ctrl.Result{}, err
		}
		// Remove finalizer
		controllerutil.RemoveFinalizer(u, KubernetesClusterFinalizer)
		if err := r.Update(ctx, u); err != nil {
			return ctrl.Result{}, err
		}
		rlog.Info("Successfully deleted KubernetesCluster: cluster=" + u.GetName())
		return ctrl.Result{}, nil
	}

	// Reconcile machines based on cluster spec
	// Build typed object with Spec only for downstream managers
	var kubernetesCluster vitistackcrdsv1alpha1.KubernetesCluster
	kubernetesCluster.ObjectMeta = metav1.ObjectMeta{Name: u.GetName(), Namespace: u.GetNamespace()}
	if specMap, found, _ := unstructured.NestedMap(u.Object, "spec"); found {
		// Convert spec map into typed Spec; ignore status entirely
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(specMap, &kubernetesCluster.Spec); err != nil {
			rlog.Error("Failed to convert spec to typed KubernetesCluster.Spec", err)
			return ctrl.Result{}, err
		}
	}

	if err := r.MachineManager.ReconcileMachines(ctx, &kubernetesCluster); err != nil {
		rlog.Error("Failed to reconcile machines", err)
		return ctrl.Result{}, err
	}

	// Reconcile Talos cluster after machines are created
	if err := r.TalosManager.ReconcileTalosCluster(ctx, &kubernetesCluster); err != nil {
		rlog.Error("Failed to reconcile Talos cluster", err)
		// Requeue for a later retry without surfacing an error
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	// Update KubernetesCluster status
	if err := r.StatusManager.UpdateKubernetesClusterStatus(ctx, &kubernetesCluster); err != nil {
		rlog.Error("Failed to update KubernetesCluster status", err)
	}

	return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
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
