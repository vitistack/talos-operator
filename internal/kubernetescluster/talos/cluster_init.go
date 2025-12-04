package talos

import (
	"context"
	"fmt"
	"net"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/secretservice"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	yaml "gopkg.in/yaml.v3"
)

// initializeTalosCluster handles the staged initialization of a Talos cluster.
// Stages:
//  1. Apply config to first control plane
//     2a. Wait for first control plane API
//     2b. Bootstrap the cluster
//     2c. Retrieve kubeconfig
//  3. Apply config to remaining control planes
//  4. Apply config to workers
//  5. Mark cluster as ready
//
// nolint:gocognit,gocyclo,funlen // flow is linear and will be refactored later
func initializeTalosCluster(ctx context.Context, t *TalosManager, cluster *vitistackv1alpha1.KubernetesCluster) error {
	// Ensure the consolidated Secret exists very early - right after validation
	if err := t.ensureTalosSecretExists(ctx, cluster); err != nil {
		return fmt.Errorf("failed to ensure talos secret exists: %w", err)
	}

	// Short-circuit: if the cluster is already initialized per persisted secret flags, skip init
	if flags, err := t.getTalosSecretFlags(ctx, cluster); err == nil {
		if flags.ControlPlaneApplied && flags.WorkerApplied && flags.Bootstrapped && flags.ClusterAccess {
			// Reconcile removed nodes first (scale-down), then new nodes (scale-up)
			if err := t.reconcileRemovedNodes(ctx, cluster); err != nil {
				vlog.Warn(fmt.Sprintf("Error during node removal reconciliation: %v", err))
				// Continue with new node reconciliation even if removal has issues
			}

			// Cleanup orphaned K8s nodes (NotReady + SchedulingDisabled with no Machine CRD)
			if err := t.CleanupOrphanedK8sNodes(ctx, cluster); err != nil {
				vlog.Warn(fmt.Sprintf("Error during orphaned node cleanup: %v", err))
				// Continue even if cleanup has issues
			}

			return t.reconcileNewNodes(ctx, cluster)
		}
	}

	// Prepare machines and endpoints
	prepResult, err := t.prepareClusterInitialization(ctx, cluster)
	if err != nil {
		return err
	}
	if prepResult == nil {
		return nil // No machines found, skipped reconciliation
	}

	// Generate or load Talos configuration
	clientConfig, err := t.ensureTalosConfiguration(ctx, cluster, prepResult)
	if err != nil {
		return err
	}

	// Verify role templates exist before attempting to apply
	secretCheck, errCheck := t.secretService.GetTalosSecret(ctx, cluster)
	if errCheck != nil {
		return fmt.Errorf("failed to verify secret before config application: %w", errCheck)
	}
	if len(secretCheck.Data["controlplane.yaml"]) == 0 || len(secretCheck.Data["worker.yaml"]) == 0 {
		return fmt.Errorf("role templates not present in secret yet, cannot apply config (this should not happen)")
	}

	// Use insecure connections only until Talos API is confirmed ready with certificates
	flags, _ := t.getTalosSecretFlags(ctx, cluster)
	insecure := !flags.TalosAPIReady

	if len(prepResult.controlPlanes) == 0 {
		return fmt.Errorf("no control planes found, cannot proceed with cluster initialization")
	}

	firstControlPlane := prepResult.controlPlanes[0]
	remainingControlPlanes := prepResult.controlPlanes[1:]

	// Stage 1: Apply config to first control plane
	if err := t.stageApplyFirstControlPlane(ctx, cluster, clientConfig, firstControlPlane, insecure, prepResult); err != nil {
		return err
	}

	// Stage 2a: Wait for first control plane API
	if err := t.stageWaitFirstControlPlaneAPI(ctx, cluster, firstControlPlane); err != nil {
		return err
	}

	// Stage 2b: Bootstrap the cluster
	if err := t.stageBootstrapCluster(ctx, cluster, clientConfig, firstControlPlane); err != nil {
		return err
	}

	// Stage 2c: Retrieve kubeconfig
	if err := t.stageRetrieveKubeconfig(ctx, cluster, clientConfig, prepResult); err != nil {
		return err
	}

	// Stage 3: Apply config to remaining control planes
	if err := t.stageApplyRemainingControlPlanes(ctx, cluster, clientConfig, remainingControlPlanes, prepResult); err != nil {
		return err
	}

	// Check if all control planes Talos APIs are ready before configuring workers
	if err := t.stageWaitAllControlPlanesReady(ctx, cluster, prepResult.controlPlanes); err != nil {
		return err
	}

	// Check if Kubernetes API server is ready before configuring workers
	if err := t.stageWaitKubernetesAPIReady(ctx, cluster, prepResult.endpointIPs[0]); err != nil {
		return err
	}

	// Stage 4: Apply config to workers
	if err := t.stageApplyWorkers(ctx, cluster, clientConfig, prepResult.workers, prepResult); err != nil {
		return err
	}

	// Stage 5: Mark cluster as ready
	return t.stageFinalizeCluster(ctx, cluster, prepResult)
}

// clusterInitPrep holds prepared data for cluster initialization
type clusterInitPrep struct {
	machines        []*vitistackv1alpha1.Machine
	controlPlanes   []*vitistackv1alpha1.Machine
	workers         []*vitistackv1alpha1.Machine
	controlPlaneIPs []string
	endpointIPs     []string
	tenantOverrides map[string]any
	tenantPatches   []string
}

// prepareClusterInitialization prepares all required data for cluster initialization
func (t *TalosManager) prepareClusterInitialization(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*clusterInitPrep, error) {
	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster machines: %w", err)
	}

	if len(machines) == 0 {
		vlog.Info("No machines found for cluster, skipping Talos reconciliation: cluster=" + cluster.Name)
		_ = t.statusManager.SetPhase(ctx, cluster, "Pending")
		_ = t.statusManager.SetCondition(ctx, cluster, "MachinesDiscovered", "False", "NoMachines", "No machines found yet for this cluster")
		return nil, nil
	}

	// Wait for all machines to be in running state
	_ = t.statusManager.SetCondition(ctx, cluster, "MachinesReady", "False", "Waiting", "Waiting for machines to be running with IP addresses")
	readyMachines, err := t.machineService.WaitForMachinesReady(ctx, machines)
	if err != nil {
		_ = t.statusManager.SetCondition(ctx, cluster, "MachinesReady", "False", "Timeout", err.Error())
		return nil, fmt.Errorf("failed waiting for machines to be ready: %w", err)
	}
	_ = t.statusManager.SetCondition(ctx, cluster, "MachinesReady", "True", "Ready", "All machines are running and have IP addresses")

	// Collect control planes and workers
	controlPlanes := t.machineService.FilterMachinesByRole(readyMachines, controlPlaneRole)
	workers := filterNonControlPlanes(readyMachines)

	// Validate and collect control plane IPs
	controlPlaneIPs, err := t.collectControlPlaneIPs(ctx, cluster, controlPlanes)
	if err != nil {
		return nil, err
	}

	// Validate worker IPs
	if err := t.validateWorkerIPs(ctx, cluster, workers); err != nil {
		return nil, err
	}

	// Determine endpoint IPs
	endpointIPs, err := t.determineControlPlaneEndpoints(ctx, cluster, controlPlaneIPs)
	if err != nil {
		return nil, fmt.Errorf("failed to determine control plane endpoints: %w", err)
	}

	_ = t.statusManager.SetPhase(ctx, cluster, "ControlPlaneIPsPending")
	_ = t.statusManager.SetCondition(ctx, cluster, "", "True", "Generated", "Talos client and role configs generated")

	// Load tenant overrides
	tenantOverrides, err := t.loadTenantOverrides(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to load tenant overrides: %w", err)
	}

	// Convert tenant overrides to YAML patches
	var tenantPatches []string
	if len(tenantOverrides) > 0 {
		tenantYAML, err := yaml.Marshal(tenantOverrides)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal tenant overrides: %w", err)
		}
		tenantPatches = []string{string(tenantYAML)}
	}

	return &clusterInitPrep{
		machines:        readyMachines,
		controlPlanes:   controlPlanes,
		workers:         workers,
		controlPlaneIPs: controlPlaneIPs,
		endpointIPs:     endpointIPs,
		tenantOverrides: tenantOverrides,
		tenantPatches:   tenantPatches,
	}, nil
}

// collectControlPlaneIPs collects and validates control plane IPv4 addresses
func (t *TalosManager) collectControlPlaneIPs(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, controlPlanes []*vitistackv1alpha1.Machine) ([]string, error) {
	var controlPlaneIPs []string
	for _, m := range controlPlanes {
		ipv4Found := false
		if len(m.Status.PublicIPAddresses) > 0 {
			for i := range m.Status.PublicIPAddresses {
				ipAddr := m.Status.PublicIPAddresses[i]
				ip := net.ParseIP(ipAddr)
				if ip != nil && ip.To4() != nil {
					controlPlaneIPs = append(controlPlaneIPs, ipAddr)
					ipv4Found = true
					break
				}
			}
		}
		if !ipv4Found {
			_ = t.statusManager.SetCondition(ctx, cluster, "ControlPlaneIPsReady", "False", "NotReady", "Some control-plane nodes are missing IPv4 addresses")
			return nil, fmt.Errorf("control planes not ready quite yet, missing IPv4 addresses before applying configuration")
		}
	}
	return controlPlaneIPs, nil
}

// validateWorkerIPs validates that all workers have IPv4 addresses
func (t *TalosManager) validateWorkerIPs(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, workers []*vitistackv1alpha1.Machine) error {
	for _, m := range workers {
		ipv4Found := false
		if len(m.Status.PublicIPAddresses) > 0 {
			for i := range m.Status.PublicIPAddresses {
				ipAddr := m.Status.PublicIPAddresses[i]
				ip := net.ParseIP(ipAddr)
				if ip != nil && ip.To4() != nil {
					ipv4Found = true
					break
				}
			}
		}
		if !ipv4Found {
			_ = t.statusManager.SetCondition(ctx, cluster, "WorkerIPsReady", "False", "NotReady", "Some worker nodes are missing IPv4 addresses")
			return fmt.Errorf("workers not ready quite yet, missing IPv4 addresses before applying configuration")
		}
	}
	return nil
}

// stageApplyFirstControlPlane handles Stage 1: Apply config to first control plane
func (t *TalosManager) stageApplyFirstControlPlane(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	firstControlPlane *vitistackv1alpha1.Machine,
	insecure bool,
	prep *clusterInitPrep,
) error {
	flags, _ := t.getTalosSecretFlags(ctx, cluster)
	if flags.FirstControlPlaneApplied {
		vlog.Info("Stage 1 already complete: First control plane config already applied, skipping: cluster=" + cluster.Name)
		return nil
	}

	vlog.Info(fmt.Sprintf("Stage 1: Applying config to first control plane: node=%s cluster=%s", firstControlPlane.Name, cluster.Name))
	_ = t.statusManager.SetPhase(ctx, cluster, "ConfiguringFirstControlPlane")
	_ = t.statusManager.SetCondition(ctx, cluster, "FirstControlPlaneConfigApplied", "False", "Applying", "Applying config to first control plane")

	if err := t.applyPerNodeConfiguration(ctx, cluster, clientConfig, []*vitistackv1alpha1.Machine{firstControlPlane}, insecure, prep.tenantOverrides, prep.endpointIPs[0]); err != nil {
		_ = t.statusManager.SetCondition(ctx, cluster, "FirstControlPlaneConfigApplied", "False", "ApplyError", err.Error())
		return fmt.Errorf("failed to apply config to first control plane: %w", err)
	}

	if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"first_controlplane_applied": true}); err != nil {
		vlog.Error("Failed to set first_controlplane_applied flag in secret", err)
		return fmt.Errorf("failed to persist first_controlplane_applied flag: %w", err)
	}
	if err := t.addConfiguredNode(ctx, cluster, firstControlPlane.Name); err != nil {
		vlog.Error("Failed to add first control plane to configured nodes", err)
	}

	vlog.Info("Stage 1 complete: First control plane config applied: node=" + firstControlPlane.Name)
	_ = t.statusManager.SetCondition(ctx, cluster, "FirstControlPlaneConfigApplied", "True", "Applied", "Config applied to first control plane")
	return nil
}

// stageWaitFirstControlPlaneAPI handles Stage 2a: Wait for first control plane API
func (t *TalosManager) stageWaitFirstControlPlaneAPI(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	firstControlPlane *vitistackv1alpha1.Machine,
) error {
	flags, _ := t.getTalosSecretFlags(ctx, cluster)
	if flags.FirstControlPlaneReady {
		vlog.Info("Stage 2a already complete: First control plane already ready, skipping: cluster=" + cluster.Name)
		return nil
	}

	vlog.Info(fmt.Sprintf("Stage 2a: Checking if first control plane Talos API is ready: node=%s cluster=%s", firstControlPlane.Name, cluster.Name))
	_ = t.statusManager.SetPhase(ctx, cluster, "WaitingForFirstControlPlaneAPI")
	_ = t.statusManager.SetCondition(ctx, cluster, "FirstControlPlaneTalosAPIReady", "False", "Waiting", "Waiting for Talos API on first control plane")

	if !t.clientService.AreTalosAPIsReady([]*vitistackv1alpha1.Machine{firstControlPlane}) {
		return talosclientservice.NewRequeueError("first control plane Talos API not ready yet", 10*time.Second)
	}

	if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"first_controlplane_ready": true}); err != nil {
		vlog.Error("Failed to set first_controlplane_ready flag in secret", err)
		return fmt.Errorf("failed to persist first_controlplane_ready flag: %w", err)
	}

	vlog.Info("Stage 2a complete: First control plane Talos API is ready: node=" + firstControlPlane.Name)
	_ = t.statusManager.SetCondition(ctx, cluster, "FirstControlPlaneTalosAPIReady", "True", "Ready", "First control plane Talos API is ready")
	return nil
}

// stageBootstrapCluster handles Stage 2b: Bootstrap the cluster
func (t *TalosManager) stageBootstrapCluster(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	firstControlPlane *vitistackv1alpha1.Machine,
) error {
	flags, _ := t.getTalosSecretFlags(ctx, cluster)
	if flags.Bootstrapped {
		vlog.Info("Stage 2b already complete: Cluster already bootstrapped, skipping: cluster=" + cluster.Name)
		return nil
	}

	vlog.Info(fmt.Sprintf("Stage 2b: Bootstrapping cluster via first control plane: node=%s cluster=%s", firstControlPlane.Name, cluster.Name))
	_ = t.statusManager.SetPhase(ctx, cluster, "Bootstrapping")
	_ = t.statusManager.SetCondition(ctx, cluster, "Bootstrapped", "False", "Bootstrapping", "Bootstrapping Talos cluster")

	var firstCPIP string
	for _, ipAddr := range firstControlPlane.Status.PublicIPAddresses {
		ip := net.ParseIP(ipAddr)
		if ip != nil && ip.To4() != nil {
			firstCPIP = ipAddr
			break
		}
	}

	bootstrapClient, err := t.clientService.CreateTalosClient(ctx, false, clientConfig, []string{firstCPIP})
	if err != nil {
		return fmt.Errorf("failed to create Talos client for bootstrap: %w", err)
	}
	if err := t.clientService.BootstrapTalosControlPlaneWithRetry(ctx, bootstrapClient, firstCPIP, 5*time.Minute, 10*time.Second); err != nil {
		_ = t.statusManager.SetCondition(ctx, cluster, "Bootstrapped", "False", "BootstrapError", err.Error())
		return fmt.Errorf("failed to bootstrap Talos cluster: %w", err)
	}

	if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"bootstrapped": true}); err != nil {
		vlog.Error("Failed to set bootstrapped flag in secret", err)
		return fmt.Errorf("failed to persist bootstrapped flag: %w", err)
	}

	vlog.Info("Stage 2b complete: Cluster bootstrapped via first control plane: node=" + firstControlPlane.Name)
	_ = t.statusManager.SetPhase(ctx, cluster, "Bootstrapped")
	_ = t.statusManager.SetCondition(ctx, cluster, "Bootstrapped", "True", "Done", "Talos cluster bootstrapped")
	return nil
}

// stageRetrieveKubeconfig handles Stage 2c: Retrieve kubeconfig
func (t *TalosManager) stageRetrieveKubeconfig(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	prep *clusterInitPrep,
) error {
	flags, _ := t.getTalosSecretFlags(ctx, cluster)
	_, hasKubeconfig, _ := t.getTalosSecretState(ctx, cluster)

	if flags.ClusterAccess {
		vlog.Info("Stage 2c already complete: Kubeconfig already available, skipping: cluster=" + cluster.Name)
		return nil
	}

	if len(prep.controlPlaneIPs) == 0 {
		return nil
	}

	vlog.Info(fmt.Sprintf("Stage 2c: Retrieving kubeconfig after bootstrap: cluster=%s", cluster.Name))
	_ = t.statusManager.SetPhase(ctx, cluster, "RetrievingKubeconfig")
	_ = t.statusManager.SetCondition(ctx, cluster, "KubeconfigAvailable", "False", "Retrieving", "Retrieving kubeconfig from bootstrapped cluster")

	kubeconfigBytes, err := t.clientService.GetKubeconfigWithRetry(ctx, clientConfig, prep.controlPlaneIPs[0], prep.endpointIPs[0], 5*time.Minute, 10*time.Second)
	if err != nil {
		_ = t.statusManager.SetCondition(ctx, cluster, "KubeconfigAvailable", "False", "RetrieveError", err.Error())
		return fmt.Errorf("failed to get kubeconfig: %w", err)
	}

	if err := t.upsertTalosClusterConfigSecret(ctx, cluster, clientConfig, kubeconfigBytes); err != nil {
		_ = t.statusManager.SetCondition(ctx, cluster, "KubeconfigAvailable", "False", "PersistError", err.Error())
		return fmt.Errorf("failed to update Talos config secret with kubeconfig: %w", err)
	}
	if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"cluster_access": true}); err != nil {
		vlog.Error("Failed to set cluster_access flag in secret", err)
		return fmt.Errorf("failed to persist cluster_access flag: %w", err)
	}

	vlog.Info("Stage 2c complete: Kubeconfig retrieved and stored: cluster=" + cluster.Name)
	_ = t.statusManager.SetCondition(ctx, cluster, "KubeconfigAvailable", "True", "Persisted", "Kubeconfig stored in Secret")

	if !hasKubeconfig {
		vlog.Info("Kubeconfig stored in Secret: secret=" + secretservice.GetSecretName(cluster))
	}
	return nil
}

// stageApplyRemainingControlPlanes handles Stage 3: Apply config to remaining control planes
func (t *TalosManager) stageApplyRemainingControlPlanes(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	remainingControlPlanes []*vitistackv1alpha1.Machine,
	prep *clusterInitPrep,
) error {
	flags, _ := t.getTalosSecretFlags(ctx, cluster)
	if flags.ControlPlaneApplied {
		vlog.Info("Stage 3 already complete: All control plane configs already applied, skipping: cluster=" + cluster.Name)
		return nil
	}

	if err := t.applyConfigToMachineGroup(ctx, cluster, clientConfig, remainingControlPlanes, true, prep.tenantOverrides, prep.endpointIPs[0], &nodeGroupConfig{
		stageNum:      3,
		nodeType:      "control plane",
		phase:         "ConfiguringRemainingControlPlanes",
		conditionName: "ControlPlaneConfigApplied",
		flagName:      "controlplane_applied",
		stopOnError:   true,
		waitForReboot: true,
	}); err != nil {
		return err
	}

	if len(remainingControlPlanes) > 0 {
		if err := t.updateVIPPoolMembers(ctx, cluster); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to update VIP pool members after adding control planes: %v", err))
		}
	}
	return nil
}

// stageWaitAllControlPlanesReady checks if all control plane Talos APIs are ready
func (t *TalosManager) stageWaitAllControlPlanesReady(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	controlPlanes []*vitistackv1alpha1.Machine,
) error {
	flags, _ := t.getTalosSecretFlags(ctx, cluster)
	if flags.TalosAPIReady {
		return nil
	}

	vlog.Info(fmt.Sprintf("Checking if all control plane Talos APIs are ready: count=%d cluster=%s", len(controlPlanes), cluster.Name))
	_ = t.statusManager.SetCondition(ctx, cluster, "TalosAPIReady", "False", "Waiting", "Waiting for Talos API on all control planes")

	if !t.clientService.AreTalosAPIsReady(controlPlanes) {
		return talosclientservice.NewRequeueError("control planes Talos APIs not all ready yet", 10*time.Second)
	}

	_ = t.statusManager.SetCondition(ctx, cluster, "TalosAPIReady", "True", "Ready", "Talos API reachable on all control planes")
	if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"talos_api_ready": true}); err != nil {
		vlog.Error("Failed to set talos_api_ready flag in secret", err)
	}
	return nil
}

// stageWaitKubernetesAPIReady checks if Kubernetes API server is ready
func (t *TalosManager) stageWaitKubernetesAPIReady(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	endpointIP string,
) error {
	flags, _ := t.getTalosSecretFlags(ctx, cluster)
	if flags.KubernetesAPIReady {
		return nil
	}

	vlog.Info(fmt.Sprintf("Checking if Kubernetes API server is ready: endpoint=%s cluster=%s", endpointIP, cluster.Name))
	_ = t.statusManager.SetCondition(ctx, cluster, "KubernetesAPIReady", "False", "Waiting", "Waiting for Kubernetes API server to be ready")

	if !t.clientService.IsKubernetesAPIReady(endpointIP) {
		return talosclientservice.NewRequeueError("kubernetes API server not ready yet", 5*time.Second)
	}

	_ = t.statusManager.SetCondition(ctx, cluster, "KubernetesAPIReady", "True", "Ready", "Kubernetes API server is ready")
	if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"kubernetes_api_ready": true}); err != nil {
		vlog.Error("Failed to set kubernetes_api_ready flag in secret", err)
	}
	return nil
}

// stageApplyWorkers handles Stage 4: Apply config to workers
func (t *TalosManager) stageApplyWorkers(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	workers []*vitistackv1alpha1.Machine,
	prep *clusterInitPrep,
) error {
	flags, _ := t.getTalosSecretFlags(ctx, cluster)
	if flags.WorkerApplied {
		vlog.Info("Stage 4 already complete: Worker configs already applied, skipping: cluster=" + cluster.Name)
		return nil
	}

	if err := t.applyConfigToMachineGroup(ctx, cluster, clientConfig, workers, true, prep.tenantOverrides, prep.endpointIPs[0], &nodeGroupConfig{
		stageNum:      4,
		nodeType:      "worker",
		phase:         "ConfiguringWorkers",
		conditionName: "WorkerConfigApplied",
		flagName:      "worker_applied",
		stopOnError:   true,
		waitForReboot: true,
	}); err != nil {
		return err
	}
	return nil
}

// stageFinalizeCluster handles Stage 5: Mark cluster as ready
func (t *TalosManager) stageFinalizeCluster(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	prep *clusterInitPrep,
) error {
	_ = t.statusManager.SetPhase(ctx, cluster, "ConfigApplied")
	_ = t.statusManager.SetCondition(ctx, cluster, "ConfigApplied", "True", "Applied", "Talos configs applied to all nodes")

	allMachines := make([]*vitistackv1alpha1.Machine, 0, len(prep.controlPlanes)+len(prep.workers))
	allMachines = append(allMachines, prep.controlPlanes...)
	allMachines = append(allMachines, prep.workers...)
	vlog.Info(fmt.Sprintf("Stage 5: All %d nodes configured, marking cluster as ready: cluster=%s", len(allMachines), cluster.Name))

	if err := t.setSecretTimestamp(ctx, cluster, "ready_at"); err != nil {
		vlog.Error("Failed to set ready_at timestamp in secret", err)
	} else {
		vlog.Info("Secret timestamp updated: ready_at, cluster=" + cluster.Name)
	}

	_ = t.statusManager.SetPhase(ctx, cluster, "Ready")
	_ = t.statusManager.SetCondition(ctx, cluster, "NodesReady", "True", "Ready", fmt.Sprintf("All %d nodes are configured and ready", len(allMachines)))
	vlog.Info("Stage 5 complete: Cluster is ready: cluster=" + cluster.Name)

	return nil
}
