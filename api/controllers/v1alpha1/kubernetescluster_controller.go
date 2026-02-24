package v1alpha1

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/helpers/nodehelper"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/talos"
	"github.com/vitistack/talos-operator/internal/machine"
	"github.com/vitistack/talos-operator/internal/services/machineservice"
	"github.com/vitistack/talos-operator/internal/services/secretservice"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	"github.com/vitistack/talos-operator/internal/services/talosconfigservice"
	"github.com/vitistack/talos-operator/internal/services/talosstateservice"
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

	TalosManager        *talos.TalosManager                  // Manager for Talos clusters
	MachineManager      *machine.MachineManager              // Manager for Machine resources
	StatusManager       *status.StatusManager                // Manager for status updates
	ValidatorService    *validationservice.ValidationService // Service for validating resources
	UpgradeService      *upgradeservice.UpgradeService       // Service for managing upgrades
	UpgradeOrchestrator *upgradeservice.UpgradeOrchestrator  // Orchestrator for executing upgrades (legacy)
	UpgradeController   *upgradeservice.UpgradeController    // New upgrade controller with state management

	SecretService *secretservice.SecretService
}

const (
	KubernetesClusterFinalizer               = "kubernetescluster.vitistack.io/finalizer"
	ControllerRequeueDelay     time.Duration = 5 * time.Second
	controlPlaneRole                         = "control-plane"
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

	// Initialize upgrade annotations for ready clusters (detects current versions)
	if kubernetesCluster.Status.Phase == status.PhaseReady {
		r.initializeUpgradeAnnotations(ctx, kubernetesCluster)
		// Check for available upgrades (compares cluster version with operator's configured version)
		_ = r.UpgradeService.CheckForAvailableUpgrades(ctx, kubernetesCluster)
	}

	return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, nil
}

// handleUpgrades checks for and processes upgrade requests via annotations.
// Returns (result, true) if an upgrade was handled/in-progress, (_, false) if no upgrade action needed.
func (r *KubernetesClusterReconciler) handleUpgrades(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (ctrl.Result, bool) {
	// Only handle upgrades for clusters that are ready or in upgrade-related states
	if cluster.Status.Phase != status.PhaseReady &&
		cluster.Status.Phase != status.PhaseUpgradingTalos &&
		cluster.Status.Phase != status.PhaseUpgradingKubernetes &&
		cluster.Status.Phase != status.PhaseUpgradeFailed {
		return ctrl.Result{}, false
	}

	// Get Talos client config
	clientConfig, err := r.TalosManager.GetTalosClientConfig(ctx, cluster)
	if err != nil {
		vlog.Warn(fmt.Sprintf("Failed to get Talos client config for upgrade: %v", err))
		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, false
	}

	// Get machines for this cluster
	machines, err := r.MachineManager.ListClusterMachines(ctx, cluster)
	if err != nil {
		vlog.Warn(fmt.Sprintf("Failed to list machines for upgrade: %v", err))
		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, false
	}

	// Use the new upgrade controller
	requeueAfter, handled, err := r.UpgradeController.HandleUpgrade(ctx, cluster, clientConfig, machines)
	if err != nil {
		vlog.Error(fmt.Sprintf("Upgrade error: %v", err), err)
		return ctrl.Result{RequeueAfter: ControllerRequeueDelay}, true
	}

	if handled {
		return ctrl.Result{RequeueAfter: requeueAfter}, true
	}

	return ctrl.Result{}, false
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
	for attempt := range maxRetries {
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
	for attempt := range maxRetries {
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
	stateService := talosstateservice.NewTalosStateService(secretService)
	statusManager := status.NewManager(c, secretService, stateService)
	clientService := talosclientservice.NewTalosClientService()
	machineSvc := machineservice.NewMachineService(c)
	talosManager := talos.NewTalosManager(c, statusManager)
	configService := talosconfigservice.NewTalosConfigService()
	upgradeService := upgradeservice.NewUpgradeService(c, statusManager, clientService, machineSvc, talosManager.GetStateService(), configService)
	upgradeController := upgradeservice.NewUpgradeController(c, secretService, statusManager, clientService, upgradeService)
	return &KubernetesClusterReconciler{
		Client:              c,
		Scheme:              scheme,
		SecretService:       secretService,
		TalosManager:        talosManager,
		MachineManager:      machine.NewMachineManager(c, scheme),
		StatusManager:       statusManager,
		ValidatorService:    validationservice.NewValidationService(),
		UpgradeService:      upgradeService,
		UpgradeOrchestrator: upgradeservice.NewUpgradeOrchestrator(upgradeService, clientService, talosManager),
		UpgradeController:   upgradeController,
	}
}

func (r *KubernetesClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&vitistackv1alpha1.KubernetesCluster{}).
		Owns(&vitistackv1alpha1.Machine{}).
		Complete(r)
}

// getFirstControlPlaneIP returns the IP of the first control plane machine
func (r *KubernetesClusterReconciler) getFirstControlPlaneIP(machines []vitistackv1alpha1.Machine) string {
	for i := range machines {
		m := &machines[i]
		if m.Labels[vitistackv1alpha1.NodeRoleAnnotation] == controlPlaneRole {
			ip := nodehelper.GetFirstIPv4(m)
			if ip != "" {
				return ip
			}
		}
	}
	return ""
}

// initializeUpgradeAnnotations detects and sets the current Talos and Kubernetes versions
// on the cluster annotations when the cluster is ready
func (r *KubernetesClusterReconciler) initializeUpgradeAnnotations(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) {
	state := r.UpgradeService.GetUpgradeState(cluster)

	// Skip if versions are already set
	if state.TalosCurrent != "" && state.KubernetesCurrent != "" {
		return
	}

	// Get Talos client config
	clientConfig, err := r.TalosManager.GetTalosClientConfig(ctx, cluster)
	if err != nil {
		vlog.Warn(fmt.Sprintf("Failed to get Talos config for version detection: %v", err))
		return
	}

	// Get machines to find a control plane IP
	machines, err := r.MachineManager.ListClusterMachines(ctx, cluster)
	if err != nil {
		vlog.Warn(fmt.Sprintf("Failed to list machines for version detection: %v", err))
		return
	}

	controlPlaneIP := r.getFirstControlPlaneIP(machines)
	if controlPlaneIP == "" {
		vlog.Warn("No control plane IP available for version detection")
		return
	}

	// Get Talos version from cluster
	talosVersion := ""
	if state.TalosCurrent == "" {
		c, err := r.TalosManager.GetClientService().CreateTalosClient(ctx, false, clientConfig, []string{controlPlaneIP})
		if err == nil {
			defer func() { _ = c.Close() }()
			talosVersion, _ = r.TalosManager.GetClientService().GetTalosVersion(ctx, c, controlPlaneIP)
		}
	}

	// Get Kubernetes version from cluster spec (this is what was configured)
	k8sVersion := ""
	if state.KubernetesCurrent == "" {
		// Use the version from the cluster spec (topology.version)
		k8sVersion = cluster.Spec.Topology.Version
	}

	// Initialize the upgrade annotations
	// Re-fetch the cluster to avoid conflict with status updates that may have occurred
	if talosVersion != "" || k8sVersion != "" {
		freshCluster := &vitistackv1alpha1.KubernetesCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), freshCluster); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to re-fetch cluster for annotation update: %v", err))
			return
		}

		if err := r.UpgradeService.InitializeCurrentVersions(ctx, freshCluster, talosVersion, k8sVersion); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to initialize upgrade annotations (will retry): %v", err))
		} else {
			vlog.Info(fmt.Sprintf("Initialized upgrade annotations: cluster=%s talos=%s k8s=%s",
				cluster.Name, talosVersion, k8sVersion))
		}
	}
}
