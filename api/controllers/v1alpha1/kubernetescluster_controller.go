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
	"github.com/vitistack/talos-operator/internal/services/machineservice"
	"github.com/vitistack/talos-operator/internal/services/secretservice"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	"github.com/vitistack/talos-operator/internal/services/upgradeservice"
	"github.com/vitistack/talos-operator/internal/services/validationservice"
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

	TalosManager     *talos.TalosManager                  // Manager for Talos clusters
	MachineManager   *machine.MachineManager              // Manager for Machine resources
	StatusManager    *status.StatusManager                // Manager for status updates
	ValidatorService *validationservice.ValidationService // Service for validating resources
	UpgradeService   *upgradeservice.UpgradeService       // Service for managing upgrades

	SecretService *secretservice.SecretService
}

const (
	KubernetesClusterFinalizer               = "kubernetescluster.vitistack.io/finalizer"
	ControllerRequeueDelay     time.Duration = 5 * time.Second
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

	// Validate the cluster spec (especially control plane replicas for etcd quorum)
	if err := r.ValidatorService.ValidateKubernetesCluster(kubernetesCluster); err != nil {
		vlog.Error("KubernetesCluster validation failed", err)
		_ = r.StatusManager.SetPhase(ctx, kubernetesCluster, status.PhaseValidationError)
		_ = r.StatusManager.SetCondition(ctx, kubernetesCluster, "Valid", "False", "ValidationFailed", err.Error())
		// Don't requeue - user needs to fix the spec
		return ctrl.Result{}, nil
	}
	// Clear validation error - reset phase if it was ValidationError, and set Valid condition
	_ = r.StatusManager.ClearValidationError(ctx, kubernetesCluster)
	_ = r.StatusManager.SetCondition(ctx, kubernetesCluster, "Valid", "True", "ValidationPassed", "Cluster spec is valid")

	// Add finalizer if not present (only for talos-managed clusters)
	if requeue, err := r.ensureFinalizer(ctx, kubernetesCluster); err != nil {
		vlog.Error("Failed to add finalizer", err)
		return ctrl.Result{}, err
	} else if requeue {
		return ctrl.Result{Requeue: true}, nil
	}

	// Handle scale-down before machine reconciliation
	// This ensures nodes are properly removed from etcd/VIP before Machine CRDs are deleted
	if result, handled := r.handleScaleDown(ctx, kubernetesCluster); handled {
		return result, nil
	}

	// Reconcile machines based on cluster spec (creates/updates desired machines)
	if err := r.MachineManager.ReconcileMachines(ctx, kubernetesCluster); err != nil {
		vlog.Error("Failed to reconcile machines", err)
		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, nil
	}

	// Reconcile Talos cluster after machines are created
	if err := r.TalosManager.ReconcileTalosCluster(ctx, kubernetesCluster); err != nil {
		// Check if this is a RequeueError (non-blocking wait signal)
		if requeueErr, ok := talosclientservice.IsRequeueError(err); ok {
			vlog.Info("Talos cluster reconcile needs requeue: " + requeueErr.Reason)
			return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, nil
		}
		vlog.Warn("Failed to reconcile Talos cluster ", err)
		// Requeue for a later retry without surfacing an error
		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, nil
	}

	// Handle upgrade requests (only for ready clusters)
	if result, handled := r.handleUpgrades(ctx, kubernetesCluster); handled {
		return result, nil
	}

	// Update KubernetesCluster status (skip if being deleted)
	if kubernetesCluster.GetDeletionTimestamp() == nil {
		if err := r.StatusManager.UpdateKubernetesClusterStatus(ctx, kubernetesCluster); err != nil {
			vlog.Error("Failed to update KubernetesCluster status", err)
		}
	}

	return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, nil
}

// handleUpgrades checks for and processes upgrade requests via annotations.
// Returns (result, true) if an upgrade was handled/in-progress, (_, false) if no upgrade action needed.
func (r *KubernetesClusterReconciler) handleUpgrades(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (ctrl.Result, bool) {
	// Only handle upgrades for clusters that are ready
	if cluster.Status.Phase != status.PhaseReady &&
		cluster.Status.Phase != status.PhaseUpgradingTalos &&
		cluster.Status.Phase != status.PhaseUpgradingKubernetes {
		return ctrl.Result{}, false
	}

	state := r.UpgradeService.GetUpgradeState(cluster)

	// Handle ongoing Talos upgrade
	if state.TalosStatus == consts.UpgradeStatusInProgress {
		vlog.Info(fmt.Sprintf("Talos upgrade in progress: cluster=%s progress=%s", cluster.Name, state.TalosProgress))
		// TODO: Check node upgrade status and continue rolling upgrade
		return ctrl.Result{RequeueAfter: 30 * time.Second}, true
	}

	// Handle ongoing Kubernetes upgrade
	if state.KubernetesStatus == consts.UpgradeStatusInProgress {
		vlog.Info(fmt.Sprintf("Kubernetes upgrade in progress: cluster=%s", cluster.Name))
		return ctrl.Result{RequeueAfter: 30 * time.Second}, true
	}

	// Check for new Talos upgrade request
	if r.UpgradeService.IsTalosUpgradeRequested(cluster) {
		return r.handleTalosUpgradeRequest(ctx, cluster, state)
	}

	// Check for new Kubernetes upgrade request (only if no Talos upgrade pending)
	if r.UpgradeService.IsKubernetesUpgradeRequested(cluster) {
		return r.handleKubernetesUpgradeRequest(ctx, cluster, state)
	}

	return ctrl.Result{}, false
}

// handleTalosUpgradeRequest processes a Talos upgrade request
func (r *KubernetesClusterReconciler) handleTalosUpgradeRequest(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, state *upgradeservice.UpgradeState) (ctrl.Result, bool) {
	vlog.Info(fmt.Sprintf("Processing Talos upgrade request: cluster=%s current=%s target=%s",
		cluster.Name, state.TalosCurrent, state.TalosTarget))

	// Validate upgrade target
	if err := r.UpgradeService.ValidateUpgradeTarget(state.TalosCurrent, state.TalosTarget); err != nil {
		vlog.Error(fmt.Sprintf("Invalid Talos upgrade target: %v", err), err)
		_ = r.UpgradeService.FailTalosUpgrade(ctx, cluster, err.Error())
		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, true
	}

	// Get all machines for this cluster
	machines, err := r.MachineManager.ListClusterMachines(ctx, cluster)
	if err != nil {
		vlog.Error("Failed to get cluster machines for upgrade", err)
		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, true
	}

	// Start the upgrade
	if err := r.UpgradeService.StartTalosUpgrade(ctx, cluster, len(machines)); err != nil {
		vlog.Error("Failed to start Talos upgrade", err)
		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, true
	}

	vlog.Info(fmt.Sprintf("Talos upgrade started: cluster=%s nodes=%d", cluster.Name, len(machines)))

	// The actual upgrade execution will happen on subsequent reconcile loops
	// as the orchestrator processes nodes one by one
	return ctrl.Result{RequeueAfter: 10 * time.Second}, true
}

// handleKubernetesUpgradeRequest processes a Kubernetes upgrade request
func (r *KubernetesClusterReconciler) handleKubernetesUpgradeRequest(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, state *upgradeservice.UpgradeState) (ctrl.Result, bool) {
	vlog.Info(fmt.Sprintf("Processing Kubernetes upgrade request: cluster=%s current=%s target=%s",
		cluster.Name, state.KubernetesCurrent, state.KubernetesTarget))

	// Check if Kubernetes upgrade should be blocked
	if blocked, reason := r.UpgradeService.ShouldBlockKubernetesUpgrade(cluster); blocked {
		vlog.Info(fmt.Sprintf("Kubernetes upgrade blocked: cluster=%s reason=%s", cluster.Name, reason))
		_ = r.UpgradeService.BlockKubernetesUpgrade(ctx, cluster, reason)
		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, true
	}

	// Validate upgrade target
	if err := r.UpgradeService.ValidateUpgradeTarget(state.KubernetesCurrent, state.KubernetesTarget); err != nil {
		vlog.Error(fmt.Sprintf("Invalid Kubernetes upgrade target: %v", err), err)
		_ = r.UpgradeService.FailKubernetesUpgrade(ctx, cluster, err.Error())
		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, true
	}

	// Start the upgrade
	if err := r.UpgradeService.StartKubernetesUpgrade(ctx, cluster); err != nil {
		vlog.Error("Failed to start Kubernetes upgrade", err)
		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, true
	}

	vlog.Info(fmt.Sprintf("Kubernetes upgrade started: cluster=%s", cluster.Name))

	// The actual upgrade execution will happen via the TalosManager
	return ctrl.Result{RequeueAfter: 30 * time.Second}, true
}

// handleScaleDown handles machine scale-down operations.
// Returns (result, true) if scale-down was needed and handled, (_, false) if no scale-down was needed.
func (r *KubernetesClusterReconciler) handleScaleDown(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (ctrl.Result, bool) {
	excessCPs, excessWorkers, err := r.MachineManager.GetExcessMachines(ctx, cluster)
	if err != nil {
		vlog.Error("Failed to get excess machines", err)
		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, true
	}

	// Handle control plane scale-down (one at a time for safety)
	if len(excessCPs) > 0 {
		cpToRemove := excessCPs[len(excessCPs)-1] // Remove highest numbered first
		vlog.Info(fmt.Sprintf("Scale-down: removing control plane %s (1 of %d excess)", cpToRemove.Name, len(excessCPs)))

		if _, err := r.TalosManager.DeleteControlPlaneNode(ctx, cluster, cpToRemove.Name); err != nil {
			vlog.Error(fmt.Sprintf("Failed to delete control plane node %s from Talos cluster", cpToRemove.Name), err)
			return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, true
		}

		if err := r.MachineManager.DeleteMachine(ctx, cpToRemove); err != nil {
			vlog.Error(fmt.Sprintf("Failed to delete machine CRD %s", cpToRemove.Name), err)
			return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, true
		}

		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, true
	}

	// Handle worker scale-down (one at a time for safety)
	if len(excessWorkers) > 0 {
		workerToRemove := excessWorkers[len(excessWorkers)-1] // Remove highest numbered first
		vlog.Info(fmt.Sprintf("Scale-down: removing worker %s (1 of %d excess)", workerToRemove.Name, len(excessWorkers)))

		if _, err := r.TalosManager.DeleteWorkerNode(ctx, cluster, workerToRemove.Name); err != nil {
			vlog.Error(fmt.Sprintf("Failed to delete worker node %s from Talos cluster", workerToRemove.Name), err)
			return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, true
		}

		if err := r.MachineManager.DeleteMachine(ctx, workerToRemove); err != nil {
			vlog.Error(fmt.Sprintf("Failed to delete machine CRD %s", workerToRemove.Name), err)
			return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, true
		}

		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, true
	}

	return ctrl.Result{}, false
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
		vlog.Error("Failed to perform cleanup, will retry", err)
		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, nil
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

		// Resource was already deleted - this is fine during deletion
		if apierrors.IsNotFound(err) {
			vlog.Info("KubernetesCluster already deleted: cluster=" + kc.GetName())
			return ctrl.Result{}, nil
		}

		if apierrors.IsConflict(err) && attempt < maxRetries-1 {
			vlog.Warn("Conflict removing finalizer, retrying...")
			continue
		}
		vlog.Error("Failed to remove finalizer, will retry", err)
		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, nil
	}
	vlog.Warn(fmt.Sprintf("Failed to remove finalizer after %d retries, will retry later", maxRetries))
	return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, nil
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
	clientService := talosclientservice.NewTalosClientService()
	machineSvc := machineservice.NewMachineService(c)
	return &KubernetesClusterReconciler{
		Client:           c,
		Scheme:           scheme,
		SecretService:    secretService,
		TalosManager:     talos.NewTalosManager(c, statusManager),
		MachineManager:   machine.NewMachineManager(c, scheme),
		StatusManager:    statusManager,
		ValidatorService: validationservice.NewValidationService(),
		UpgradeService:   upgradeservice.NewUpgradeService(c, statusManager, clientService, machineSvc),
	}
}

func (r *KubernetesClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&vitistackv1alpha1.KubernetesCluster{}).
		Owns(&vitistackv1alpha1.Machine{}).
		Complete(r)
}
