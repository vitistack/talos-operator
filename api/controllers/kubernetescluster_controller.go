package controllers

import (
	"context"

	//vitistackv1alpha1 "github.com/vitistack/talos-operator/api/v1alpha1"
	vitistackcrdsv1alpha1 "github.com/vitistack/crds/pkg/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines/status,verbs=get

func (r *KubernetesClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	// Fetch the KubernetesCluster instance
	var kubernetesCluster vitistackcrdsv1alpha1.KubernetesCluster
	if err := r.Get(ctx, req.NamespacedName, &kubernetesCluster); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// Resource was deleted
			log.Info("KubernetesCluster resource deleted")
			return ctrl.Result{}, nil
		}
		log.Error(err, "unable to fetch KubernetesCluster")
		return ctrl.Result{}, err
	}

	log.Info("Reconciling KubernetesCluster", "name", kubernetesCluster.Name, "namespace", kubernetesCluster.Namespace)

	// TODO: Implement your reconciliation logic here
	// Example: Create/Update/Delete resources based on the KubernetesCluster spec

	return ctrl.Result{}, nil
}

func (r *KubernetesClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&vitistackcrdsv1alpha1.KubernetesCluster{}).
		//Owns(&kubevirtv1.VirtualMachine{}).
		Complete(r)
}
