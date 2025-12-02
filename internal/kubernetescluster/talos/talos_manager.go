package talos

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"net"

	"github.com/vitistack/common/pkg/loggers/vlog"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/spf13/viper"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/internal/services/machineservice"
	"github.com/vitistack/talos-operator/internal/services/secretservice"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	"github.com/vitistack/talos-operator/internal/services/talosconfigservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	yaml "gopkg.in/yaml.v3"
)

const trueStr = "true"
const falseStr = "false"
const controlPlaneRole = "control-plane"

type TalosManager struct {
	client.Client
	statusManager  *status.StatusManager
	secretService  *secretservice.SecretService
	configService  *talosconfigservice.TalosConfigService
	clientService  *talosclientservice.TalosClientService
	machineService *machineservice.MachineService
}

// NewTalosManager creates a new instance of TalosManager
func NewTalosManager(c client.Client, statusManager *status.StatusManager) *TalosManager {
	return &TalosManager{
		Client:         c,
		statusManager:  statusManager,
		secretService:  secretservice.NewSecretService(c),
		configService:  talosconfigservice.NewTalosConfigService(),
		clientService:  talosclientservice.NewTalosClientService(),
		machineService: machineservice.NewMachineService(c),
	}
}

// MachineInfo holds information about a machine needed for Talos cluster creation
type MachineInfo struct {
	Name    string
	Role    string // "control-plane" or "worker"
	IP      string
	Machine *vitistackv1alpha1.Machine
}

// ReconcileTalosCluster waits for machines to be ready and creates a Talos cluster
// nolint:gocyclo // Reconcile flow is linear; refactor will be done separately.
func (t *TalosManager) ReconcileTalosCluster(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	err := initializeTalosCluster(ctx, t, cluster)
	// Always update status based on current persisted flags/conditions
	if sErr := t.statusManager.UpdateKubernetesClusterStatus(ctx, cluster); sErr != nil {
		vlog.Error("failed to update Kubernetes cluster status", sErr)
	}
	return err
}

// nolint:gocognit,gocyclo,funlen // flow is linear and will be refactored later
func initializeTalosCluster(ctx context.Context, t *TalosManager, cluster *vitistackv1alpha1.KubernetesCluster) error {
	// Ensure the consolidated Secret exists very early - right after validation
	// This provides state persistence from the very beginning of the lifecycle
	if err := t.ensureTalosSecretExists(ctx, cluster); err != nil {
		return fmt.Errorf("failed to ensure talos secret exists: %w", err)
	}

	// Short-circuit: if the cluster is already initialized per persisted secret flags, skip init
	if flags, err := t.getTalosSecretFlags(ctx, cluster); err == nil {
		if flags.ControlPlaneApplied && flags.WorkerApplied && flags.Bootstrapped && flags.ClusterAccess {
			// Check for new machines that need to be configured
			return t.reconcileNewNodes(ctx, cluster)
		}
	}
	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get cluster machines: %w", err)
	}

	if len(machines) == 0 {
		vlog.Info("No machines found for cluster, skipping Talos reconciliation: cluster=" + cluster.Name)
		_ = t.statusManager.SetPhase(ctx, cluster, "Pending")
		_ = t.statusManager.SetCondition(ctx, cluster, "MachinesDiscovered", "False", "NoMachines", "No machines found yet for this cluster")
		return nil
	}

	// Wait for all machines to be in running state
	// _ = t.statusManager.SetPhase(ctx, cluster, "WaitingForMachines")
	_ = t.statusManager.SetCondition(ctx, cluster, "MachinesReady", "False", "Waiting", "Waiting for machines to be running with IP addresses")
	readyMachines, err := t.machineService.WaitForMachinesReady(ctx, machines)
	if err != nil {
		_ = t.statusManager.SetCondition(ctx, cluster, "MachinesReady", "False", "Timeout", err.Error())
		return fmt.Errorf("failed waiting for machines to be ready: %w", err)
	}
	_ = t.statusManager.SetCondition(ctx, cluster, "MachinesReady", "True", "Ready", "All machines are running and have IP addresses")

	// collect control plane and worker IPs from ready machines
	controlPlanes := t.machineService.FilterMachinesByRole(readyMachines, controlPlaneRole)

	// get other nodes except control plane
	workers := filterNonControlPlanes(readyMachines)

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
			return fmt.Errorf("control planes not ready quite yet, missing IPv4 addresses before applying configuration")
		}
	}

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

	// Determine endpoint mode and get control plane endpoints accordingly
	endpointIPs, err := t.determineControlPlaneEndpoints(ctx, cluster, controlPlaneIPs)
	if err != nil {
		return fmt.Errorf("failed to determine control plane endpoints: %w", err)
	}

	_ = t.statusManager.SetPhase(ctx, cluster, "ControlPlaneIPsPending")
	_ = t.statusManager.SetCondition(ctx, cluster, "", "True", "Generated", "Talos client and role configs generated")

	tenantOverrides, err := t.loadTenantOverrides(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to load tenant overrides: %w", err)
	}

	// Convert tenant overrides to YAML patches for the new bundle approach
	var tenantPatches []string
	if len(tenantOverrides) > 0 {
		tenantYAML, err := yaml.Marshal(tenantOverrides)
		if err != nil {
			return fmt.Errorf("failed to marshal tenant overrides: %w", err)
		}
		tenantPatches = []string{string(tenantYAML)}
	}

	// Prefer existing artifacts from Secret; generate only if missing
	clientConfig, fromSecret, err := t.loadTalosArtifacts(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to load talos artifacts: %w", err)
	}
	if !fromSecret {
		// Generate Talos configuration using the new bundle approach (like talosctl)
		configBundle, err := t.configService.GenerateTalosConfigBundle(cluster, endpointIPs, tenantPatches, nil, nil)
		if err != nil {
			return fmt.Errorf("failed to generate Talos config bundle: %w", err)
		}

		cpYAML, err := t.configService.SerializeControlPlaneConfig(configBundle.Bundle)
		if err != nil {
			return fmt.Errorf("failed to serialize control plane config: %w", err)
		}

		wYAML, err := t.configService.SerializeWorkerConfig(configBundle.Bundle)
		if err != nil {
			return fmt.Errorf("failed to serialize worker config: %w", err)
		}

		// Serialize the secrets bundle for persistence
		secretsBundleYAML, err := t.configService.SerializeSecretsBundle(configBundle.SecretsBundle)
		if err != nil {
			return fmt.Errorf("failed to serialize secrets bundle: %w", err)
		}

		vlog.Info(fmt.Sprintf("Generated Talos config bundle: cluster=%s hasConfig=%t", cluster.Name, configBundle.ClientConfig != nil))
		// Update status phase: config generated
		_ = t.statusManager.SetPhase(ctx, cluster, "ConfigGenerated")
		_ = t.statusManager.SetCondition(ctx, cluster, "ConfigGenerated", "True", "Generated", "Talos client and role configs generated")
		// Persist initial Talos configs into a Secret (with secrets bundle, without kubeconfig yet)
		if err := t.upsertTalosClusterConfigSecretWithRoleYAML(ctx, cluster, configBundle.ClientConfig, cpYAML, wYAML, secretsBundleYAML, nil /* kubeconfig */); err != nil {
			return fmt.Errorf("failed to persist Talos config secret: %w", err)
		}
		clientConfig = configBundle.ClientConfig
	} else {
		vlog.Info("Loaded Talos artifacts from Secret: cluster=" + cluster.Name)
	}

	// Use insecure connections only until Talos API is confirmed ready with certificates
	flags, _ := t.getTalosSecretFlags(ctx, cluster)
	insecure := !flags.TalosAPIReady // Use secure connections once API is ready with certs

	// Verify role templates exist before attempting to apply
	secretCheck, errCheck := t.secretService.GetTalosSecret(ctx, cluster)
	if errCheck != nil {
		return fmt.Errorf("failed to verify secret before config application: %w", errCheck)
	}
	if len(secretCheck.Data["controlplane.yaml"]) == 0 || len(secretCheck.Data["worker.yaml"]) == 0 {
		return fmt.Errorf("role templates not present in secret yet, cannot apply config (this should not happen)")
	}

	// ==========================================
	// STAGED CONFIGURATION APPROACH
	// ==========================================
	// Stage 1: Apply config to first control plane only
	// Stage 2: Wait for first control plane API and bootstrap
	// Stage 3: Apply config to remaining control planes
	// Stage 4: Apply config to workers
	// ==========================================

	if len(controlPlanes) == 0 {
		return fmt.Errorf("no control planes found, cannot proceed with cluster initialization")
	}

	firstControlPlane := controlPlanes[0]
	remainingControlPlanes := controlPlanes[1:]

	// Stage 1: Apply config to first control plane
	if !flags.FirstControlPlaneApplied {
		vlog.Info(fmt.Sprintf("Stage 1: Applying config to first control plane: node=%s cluster=%s", firstControlPlane.Name, cluster.Name))
		_ = t.statusManager.SetPhase(ctx, cluster, "ConfiguringFirstControlPlane")
		_ = t.statusManager.SetCondition(ctx, cluster, "FirstControlPlaneConfigApplied", "False", "Applying", "Applying config to first control plane")

		if err := t.applyPerNodeConfiguration(ctx, cluster, clientConfig, []*vitistackv1alpha1.Machine{firstControlPlane}, insecure, tenantOverrides, endpointIPs[0]); err != nil {
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
	} else {
		vlog.Info("Stage 1 already complete: First control plane config already applied, skipping: cluster=" + cluster.Name)
	}

	// Stage 2: Wait for first control plane API and bootstrap
	// Re-fetch flags after potential update
	flags, _ = t.getTalosSecretFlags(ctx, cluster)

	if !flags.FirstControlPlaneReady {
		vlog.Info(fmt.Sprintf("Stage 2a: Checking if first control plane Talos API is ready: node=%s cluster=%s", firstControlPlane.Name, cluster.Name))
		_ = t.statusManager.SetPhase(ctx, cluster, "WaitingForFirstControlPlaneAPI")
		_ = t.statusManager.SetCondition(ctx, cluster, "FirstControlPlaneTalosAPIReady", "False", "Waiting", "Waiting for Talos API on first control plane")

		// Non-blocking check - if not ready, return RequeueError to let controller retry later
		if !t.clientService.AreTalosAPIsReady([]*vitistackv1alpha1.Machine{firstControlPlane}) {
			return talosclientservice.NewRequeueError("first control plane Talos API not ready yet", 10*time.Second)
		}

		if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"first_controlplane_ready": true}); err != nil {
			vlog.Error("Failed to set first_controlplane_ready flag in secret", err)
			return fmt.Errorf("failed to persist first_controlplane_ready flag: %w", err)
		}

		vlog.Info("Stage 2a complete: First control plane Talos API is ready: node=" + firstControlPlane.Name)
		_ = t.statusManager.SetCondition(ctx, cluster, "FirstControlPlaneTalosAPIReady", "True", "Ready", "First control plane Talos API is ready")
	} else {
		vlog.Info("Stage 2a already complete: First control plane already ready, skipping: cluster=" + cluster.Name)
	}

	// Stage 2b: Bootstrap the first control plane
	// Re-fetch flags after potential update
	flags, _ = t.getTalosSecretFlags(ctx, cluster)

	if !flags.Bootstrapped {
		vlog.Info(fmt.Sprintf("Stage 2b: Bootstrapping cluster via first control plane: node=%s cluster=%s", firstControlPlane.Name, cluster.Name))
		_ = t.statusManager.SetPhase(ctx, cluster, "Bootstrapping")
		_ = t.statusManager.SetCondition(ctx, cluster, "Bootstrapped", "False", "Bootstrapping", "Bootstrapping Talos cluster")

		// Get first control plane IP
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
	} else {
		vlog.Info("Stage 2b already complete: Cluster already bootstrapped, skipping: cluster=" + cluster.Name)
	}

	// Stage 2c: Get kubeconfig right after bootstrap (makes cluster accessible early)
	// Re-fetch flags after potential update
	flags, _ = t.getTalosSecretFlags(ctx, cluster)
	_, hasKubeconfig, _ := t.getTalosSecretState(ctx, cluster)

	if !flags.ClusterAccess && len(controlPlaneIPs) > 0 {
		vlog.Info(fmt.Sprintf("Stage 2c: Retrieving kubeconfig after bootstrap: cluster=%s", cluster.Name))
		_ = t.statusManager.SetPhase(ctx, cluster, "RetrievingKubeconfig")
		_ = t.statusManager.SetCondition(ctx, cluster, "KubeconfigAvailable", "False", "Retrieving", "Retrieving kubeconfig from bootstrapped cluster")

		// Use first control plane IP for kubeconfig retrieval
		kubeconfigBytes, err := t.clientService.GetKubeconfigWithRetry(ctx, clientConfig, controlPlaneIPs[0], endpointIPs[0], 5*time.Minute, 10*time.Second)
		if err != nil {
			_ = t.statusManager.SetCondition(ctx, cluster, "KubeconfigAvailable", "False", "RetrieveError", err.Error())
			return fmt.Errorf("failed to get kubeconfig: %w", err)
		}

		// Update consolidated Talos secret with kubeconfig
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
	} else if flags.ClusterAccess {
		vlog.Info("Stage 2c already complete: Kubeconfig already available, skipping: cluster=" + cluster.Name)
	}

	// Stage 3: Apply config to remaining control planes (after bootstrap)
	// Re-fetch flags after potential update
	flags, _ = t.getTalosSecretFlags(ctx, cluster)

	if !flags.ControlPlaneApplied {
		// Remaining control planes need insecure mode for initial configuration since they don't have the cluster CA yet
		if err := t.applyConfigToMachineGroup(ctx, cluster, clientConfig, remainingControlPlanes, true, tenantOverrides, endpointIPs[0], &nodeGroupConfig{
			stageNum:      3,
			nodeType:      "control plane",
			phase:         "ConfiguringRemainingControlPlanes",
			conditionName: "ControlPlaneConfigApplied",
			flagName:      "controlplane_applied",
			stopOnError:   true,
			waitForReboot: true, // Wait for each control plane to reboot before continuing
		}); err != nil {
			return err
		}

		// Update VIP pool members after adding new control planes
		if len(remainingControlPlanes) > 0 {
			if err := t.updateVIPPoolMembers(ctx, cluster); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to update VIP pool members after adding control planes: %v", err))
			}
		}
	} else {
		vlog.Info("Stage 3 already complete: All control plane configs already applied, skipping: cluster=" + cluster.Name)
	}

	// Re-fetch flags after potential update
	flags, _ = t.getTalosSecretFlags(ctx, cluster)

	// Check if all control planes Talos APIs are ready before configuring workers
	if !flags.TalosAPIReady {
		vlog.Info(fmt.Sprintf("Checking if all control plane Talos APIs are ready: count=%d cluster=%s", len(controlPlanes), cluster.Name))
		_ = t.statusManager.SetCondition(ctx, cluster, "TalosAPIReady", "False", "Waiting", "Waiting for Talos API on all control planes")

		// Non-blocking check - if not ready, return RequeueError to let controller retry later
		if !t.clientService.AreTalosAPIsReady(controlPlanes) {
			return talosclientservice.NewRequeueError("control planes Talos APIs not all ready yet", 10*time.Second)
		}

		_ = t.statusManager.SetCondition(ctx, cluster, "TalosAPIReady", "True", "Ready", "Talos API reachable on all control planes")
		if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"talos_api_ready": true}); err != nil {
			vlog.Error("Failed to set talos_api_ready flag in secret", err)
		}
	}

	// Re-fetch flags after potential update
	flags, _ = t.getTalosSecretFlags(ctx, cluster)

	// Check if Kubernetes API server is ready before configuring workers
	// This ensures workers can successfully connect to the control plane
	if !flags.KubernetesAPIReady {
		vlog.Info(fmt.Sprintf("Checking if Kubernetes API server is ready: endpoint=%s cluster=%s", endpointIPs[0], cluster.Name))
		_ = t.statusManager.SetCondition(ctx, cluster, "KubernetesAPIReady", "False", "Waiting", "Waiting for Kubernetes API server to be ready")

		// Non-blocking check - if not ready, return RequeueError to let controller retry later
		if !t.clientService.IsKubernetesAPIReady(endpointIPs[0]) {
			return talosclientservice.NewRequeueError("kubernetes API server not ready yet", 5*time.Second)
		}

		_ = t.statusManager.SetCondition(ctx, cluster, "KubernetesAPIReady", "True", "Ready", "Kubernetes API server is ready")
		if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"kubernetes_api_ready": true}); err != nil {
			vlog.Error("Failed to set kubernetes_api_ready flag in secret", err)
		}
	}

	// Stage 4: Apply config to workers (after control planes are ready)
	// Re-fetch flags after potential update
	flags, _ = t.getTalosSecretFlags(ctx, cluster)

	if !flags.WorkerApplied {
		// Workers always need insecure mode for initial configuration since they don't have the cluster CA yet
		if err := t.applyConfigToMachineGroup(ctx, cluster, clientConfig, workers, true, tenantOverrides, endpointIPs[0], &nodeGroupConfig{
			stageNum:      4,
			nodeType:      "worker",
			phase:         "ConfiguringWorkers",
			conditionName: "WorkerConfigApplied",
			flagName:      "worker_applied",
			stopOnError:   true,
			waitForReboot: true, // Wait for each worker to reboot before continuing
		}); err != nil {
			return err
		}
	} else {
		vlog.Info("Stage 4 already complete: Worker configs already applied, skipping: cluster=" + cluster.Name)
	}

	// Update status phase: configs applied
	_ = t.statusManager.SetPhase(ctx, cluster, "ConfigApplied")
	_ = t.statusManager.SetCondition(ctx, cluster, "ConfigApplied", "True", "Applied", "Talos configs applied to all nodes")

	// Stage 5: Mark cluster as ready
	// All nodes are configured, kubeconfig was already retrieved after bootstrap (Stage 2c)
	allMachines := make([]*vitistackv1alpha1.Machine, 0, len(controlPlanes)+len(workers))
	allMachines = append(allMachines, controlPlanes...)
	allMachines = append(allMachines, workers...)
	vlog.Info(fmt.Sprintf("Stage 5: All %d nodes configured, marking cluster as ready: cluster=%s", len(allMachines), cluster.Name))

	// Mark cluster as ready with timestamp
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

func (m *TalosManager) findNetworkNamespace(ctx context.Context, namespace string) (*vitistackv1alpha1.NetworkNamespace, error) {
	networkNamespaceList := &vitistackv1alpha1.NetworkNamespaceList{}
	if err := m.List(ctx, networkNamespaceList, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("failed to list NetworkNamespaces from supervisor cluster: %w", err)
	}

	if len(networkNamespaceList.Items) == 0 {
		vlog.Info("No NetworkNamespaces found in supervisor cluster",
			"namespace", namespace)
		return nil, nil
	}

	// Use the first NetworkNamespace found
	networkNamespace := &networkNamespaceList.Items[0]
	vlog.Info("Using first available NetworkNamespace",
		"namespace", namespace,
		"networkNamespaceName", networkNamespace.Name)

	return networkNamespace, nil
}

// ensureTalosSecretExists creates the consolidated Secret if it does not exist yet with default flags
func (t *TalosManager) ensureTalosSecretExists(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	_, err := t.secretService.GetTalosSecret(ctx, cluster)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	// create minimal secret with default flags and lifecycle timestamps
	now := time.Now().UTC().Format(time.RFC3339)
	data := map[string][]byte{
		"bootstrapped":               []byte(falseStr),
		"talosconfig_present":        []byte(falseStr),
		"controlplane_yaml_present":  []byte(falseStr),
		"worker_yaml_present":        []byte(falseStr),
		"kubeconfig_present":         []byte(falseStr),
		"talos_api_ready":            []byte(falseStr),
		"controlplane_applied":       []byte(falseStr),
		"worker_applied":             []byte(falseStr),
		"cluster_access":             []byte(falseStr),
		"first_controlplane_applied": []byte(falseStr),
		"first_controlplane_ready":   []byte(falseStr),
		"configured_nodes":           []byte(""), // comma-separated list of node names that have been configured
		"created_at":                 []byte(now),
	}
	err = t.secretService.CreateTalosSecret(ctx, cluster, data)
	if err == nil {
		vlog.Info("Secret created with initial status flags, cluster=" + cluster.Name)
	}
	return err
}

// getTalosSecretState returns persisted state flags from the cluster's consolidated Secret.
func (t *TalosManager) getTalosSecretState(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (bootstrapped bool, hasKubeconfig bool, err error) {
	secret, e := t.secretService.GetTalosSecret(ctx, cluster)
	if e != nil {
		return false, false, e
	}
	if secret.Data == nil {
		return false, false, nil
	}
	if b, ok := secret.Data["bootstrapped"]; ok && string(b) == trueStr {
		bootstrapped = true
	}
	if k, ok := secret.Data["kube.config"]; ok && len(k) > 0 {
		hasKubeconfig = true
	}
	return bootstrapped, hasKubeconfig, nil
}

// loadTalosArtifacts attempts to read talosconfig (and ensures role templates exist) from the consolidated Secret.
// Returns fromSecret=true when talosconfig and both role templates are present.
func (t *TalosManager) loadTalosArtifacts(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*clientconfig.Config, bool, error) {
	secret, err := t.secretService.GetTalosSecret(ctx, cluster)
	if err != nil {
		return nil, false, nil
	}
	if secret.Data == nil {
		return nil, false, nil
	}
	var cfg *clientconfig.Config
	if b, ok := secret.Data["talosconfig"]; ok && len(b) > 0 {
		// talosclient config is YAML; unmarshal back
		c := &clientconfig.Config{}
		if err := yaml.Unmarshal(b, c); err == nil {
			cfg = c
		} else {
			return nil, false, fmt.Errorf("failed to unmarshal talosconfig from secret: %w", err)
		}
	}

	cp := secret.Data["controlplane.yaml"]
	w := secret.Data["worker.yaml"]
	if cfg != nil && len(cp) > 0 && len(w) > 0 {
		return cfg, true, nil
	}
	return nil, false, nil
}

// talosSecretFlags captures persisted boolean flags stored in the consolidated Secret
type talosSecretFlags struct {
	ControlPlaneApplied      bool
	WorkerApplied            bool
	Bootstrapped             bool
	ClusterAccess            bool
	TalosAPIReady            bool // tracks when Talos API accepts secure (non-insecure) connections
	KubernetesAPIReady       bool // tracks when Kubernetes API server is reachable
	FirstControlPlaneApplied bool // tracks if the first control plane has config applied
	FirstControlPlaneReady   bool // tracks if the first control plane is ready (API reachable)
}

// getTalosSecretFlags reads boolean flags from the consolidated Secret
func (t *TalosManager) getTalosSecretFlags(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (talosSecretFlags, error) {
	secret, err := t.secretService.GetTalosSecret(ctx, cluster)
	if err != nil {
		return talosSecretFlags{}, err
	}
	return parseSecretFlags(secret.Data), nil
}

// parseSecretFlags extracts boolean flags from secret data
func parseSecretFlags(data map[string][]byte) talosSecretFlags {
	flags := talosSecretFlags{}
	if data == nil {
		return flags
	}

	flags.ControlPlaneApplied = isFlagTrue(data, "controlplane_applied")
	flags.WorkerApplied = isFlagTrue(data, "worker_applied")
	flags.Bootstrapped = isFlagTrue(data, "bootstrapped")
	flags.ClusterAccess = parseClusterAccessFlag(data)
	flags.TalosAPIReady = isFlagTrue(data, "talos_api_ready")
	flags.KubernetesAPIReady = isFlagTrue(data, "kubernetes_api_ready")
	flags.FirstControlPlaneApplied = isFlagTrue(data, "first_controlplane_applied")
	flags.FirstControlPlaneReady = isFlagTrue(data, "first_controlplane_ready")

	return flags
}

// isFlagTrue checks if a flag in secret data is set to "true"
func isFlagTrue(data map[string][]byte, key string) bool {
	if b, ok := data[key]; ok && string(b) == trueStr {
		return true
	}
	return false
}

// parseClusterAccessFlag determines cluster access from multiple possible flags
func parseClusterAccessFlag(data map[string][]byte) bool {
	if isFlagTrue(data, "cluster_access") {
		return true
	}
	if k, ok := data["kube.config"]; ok && len(k) > 0 {
		return true
	}
	if isFlagTrue(data, "kubeconfig_present") {
		return true
	}
	return false
} // setSecretTimestamp sets a timestamp field in the consolidated Secret
func (t *TalosManager) setSecretTimestamp(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, key string) error {
	// Retry logic to handle concurrent modifications
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := t.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		secret.Data[key] = []byte(time.Now().UTC().Format(time.RFC3339))

		err = t.secretService.UpdateTalosSecret(ctx, secret)
		if err == nil {
			return nil
		}

		// If conflict error, retry with fresh data
		if apierrors.IsConflict(err) {
			if attempt < maxRetries-1 {
				time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1))
				continue
			}
		}
		return err
	}
	return fmt.Errorf("failed to update secret timestamp after %d retries", maxRetries)
}

// setTalosSecretFlags sets provided boolean flags in the consolidated Secret
func (t *TalosManager) setTalosSecretFlags(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, updates map[string]bool) error {
	// Retry logic to handle concurrent modifications
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := t.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		for k, v := range updates {
			if v {
				secret.Data[k] = []byte(trueStr)
			} else {
				secret.Data[k] = []byte(falseStr)
			}
		}

		err = t.secretService.UpdateTalosSecret(ctx, secret)
		if err == nil {
			return nil
		}

		// If conflict error, retry with fresh data
		if apierrors.IsConflict(err) {
			if attempt < maxRetries-1 {
				vlog.Warn(fmt.Sprintf("Secret conflict on attempt %d, retrying: %v", attempt+1, err))
				time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1)) // exponential backoff
				continue
			}
		}
		return err
	}
	return fmt.Errorf("failed to update secret flags after %d retries", maxRetries)
}

func (t *TalosManager) applyPerNodeConfiguration(ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	machines []*vitistackv1alpha1.Machine,
	insecure bool,
	tenantOverrides map[string]any,
	endpointIP string) error {
	// Load role templates from persistent Secret
	secret, err := t.secretService.GetTalosSecret(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get talos secret %s: %w", secretservice.GetSecretName(cluster), err)
	}
	cpTemplate := secret.Data["controlplane.yaml"]
	wTemplate := secret.Data["worker.yaml"]

	for _, m := range machines {
		if len(m.Status.NetworkInterfaces) == 0 || len(m.Status.PublicIPAddresses) == 0 {
			continue
		}

		// Find first IPv4 address
		var ip string
		for _, ipAddr := range m.Status.PublicIPAddresses {
			parsedIP := net.ParseIP(ipAddr)
			if parsedIP != nil && parsedIP.To4() != nil {
				ip = ipAddr
				break
			}
		}
		if ip == "" {
			continue // Skip if no IPv4 address found
		}

		// Choose role template
		var roleYAML []byte
		if m.Labels[vitistackv1alpha1.NodeRoleAnnotation] == "control-plane" {
			roleYAML = cpTemplate
		} else {
			roleYAML = wTemplate
		}
		if len(roleYAML) == 0 {
			return fmt.Errorf("missing role template for node %s (role=%s) in secret %s", m.Name, m.Labels[vitistackv1alpha1.NodeRoleAnnotation], secretservice.GetSecretName(cluster))
		}

		// Select install disk and prepare configuration
		installDisk := t.configService.SelectInstallDisk(m)
		nodeConfig, err := t.configService.PrepareNodeConfig(cluster, roleYAML, installDisk, m, tenantOverrides)
		if err != nil {
			return fmt.Errorf("failed to prepare config for node %s: %w", m.Name, err)
		}

		// add endpoint IPs to node config
		patched, err := t.configService.PatchNodeConfigWithEndpointIPs(nodeConfig, endpointIP)
		if err != nil {
			return fmt.Errorf("failed to patch config for node %s with endpoint IPs: %w", m.Name, err)
		}
		nodeConfig = patched

		// Apply configuration to node
		if err := t.clientService.ApplyConfigToNode(ctx, insecure, clientConfig, ip, nodeConfig); err != nil {
			return fmt.Errorf("failed to apply config to node %s: %w", ip, err)
		}
	}
	return nil
}

func (t *TalosManager) loadTenantOverrides(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (map[string]any, error) {
	name := strings.TrimSpace(viper.GetString(consts.TENANT_CONFIGMAP_NAME))
	if name == "" {
		return nil, nil
	}

	namespace := strings.TrimSpace(viper.GetString(consts.TENANT_CONFIGMAP_NAMESPACE))
	if namespace == "" {
		namespace = cluster.Namespace
	}

	dataKey := strings.TrimSpace(viper.GetString(consts.TENANT_CONFIGMAP_DATA_KEY))
	if dataKey == "" {
		dataKey = "config.yaml"
	}

	cm := &corev1.ConfigMap{}
	if err := t.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, cm); err != nil {
		if apierrors.IsNotFound(err) {
			vlog.Info(fmt.Sprintf("Tenant overrides ConfigMap %s/%s not found, skipping overrides", namespace, name))
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read tenant overrides ConfigMap %s/%s: %w", namespace, name, err)
	}

	raw, ok := cm.Data[dataKey]
	if !ok || strings.TrimSpace(raw) == "" {
		vlog.Warn(fmt.Sprintf("Tenant overrides ConfigMap %s/%s missing data key %s, skipping overrides", namespace, name, dataKey))
		return nil, nil
	}

	replaced := strings.ReplaceAll(raw, "#CLUSTERID#", cluster.Spec.Cluster.ClusterId)
	overrides := map[string]any{}
	if err := yaml.Unmarshal([]byte(replaced), &overrides); err != nil {
		return nil, fmt.Errorf("failed to parse tenant overrides from ConfigMap %s/%s key %s: %w", namespace, name, dataKey, err)
	}

	return overrides, nil
}

// determineControlPlaneEndpoints determines the control plane endpoints based on the configured endpoint mode.
// Supported modes:
// - "none": Use control plane IPs directly (no load balancing)
// - "networkconfiguration": Use ControlPlaneVirtualSharedIP from NetworkNamespace (default)
// - "talosvip": Use Talos built-in VIP (requires additional Talos configuration)
// - "custom": Use user-provided endpoint addresses from CUSTOM_ENDPOINT environment variable
func (t *TalosManager) determineControlPlaneEndpoints(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, controlPlaneIPs []string) ([]string, error) {
	endpointMode := consts.EndpointMode(viper.GetString(consts.ENDPOINT_MODE))

	// Validate endpoint mode
	if !consts.IsValidEndpointMode(string(endpointMode)) {
		vlog.Warn(fmt.Sprintf("Invalid endpoint mode '%s', falling back to default '%s'", endpointMode, consts.DefaultEndpointMode))
		endpointMode = consts.DefaultEndpointMode
	}

	vlog.Info(fmt.Sprintf("Using endpoint mode: %s for cluster %s", endpointMode, cluster.Name))

	switch endpointMode {
	case consts.EndpointModeNone:
		return t.getEndpointsNone(controlPlaneIPs)

	case consts.EndpointModeNetworkConfiguration:
		return t.getEndpointsNetworkConfiguration(ctx, cluster, controlPlaneIPs)

	case consts.EndpointModeTalosVIP:
		return t.getEndpointsTalosVIP(ctx, cluster, controlPlaneIPs)

	case consts.EndpointModeCustom:
		return t.getEndpointsCustom()

	default:
		// This shouldn't happen due to validation above, but handle it gracefully
		return t.getEndpointsNetworkConfiguration(ctx, cluster, controlPlaneIPs)
	}
}

// getEndpointsNone returns control plane IPs directly without any load balancing.
// This is the simplest mode but provides no HA for the control plane endpoint.
func (t *TalosManager) getEndpointsNone(controlPlaneIPs []string) ([]string, error) {
	if len(controlPlaneIPs) == 0 {
		return nil, fmt.Errorf("no control plane IPs available for endpoint mode 'none'")
	}
	vlog.Info(fmt.Sprintf("Endpoint mode 'none': using control plane IP directly: %s", controlPlaneIPs[0]))
	// Return first control plane IP as endpoint
	return []string{controlPlaneIPs[0]}, nil
}

// getEndpointsNetworkConfiguration uses the NetworkNamespace's ControlPlaneVirtualSharedIP
// to obtain load balancer IPs. This is the default and recommended mode.
func (t *TalosManager) getEndpointsNetworkConfiguration(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, controlPlaneIPs []string) ([]string, error) {
	networkNamespace, err := t.findNetworkNamespace(ctx, cluster.GetNamespace())
	if err != nil {
		return nil, fmt.Errorf("failed to find network namespace: %w", err)
	}

	if networkNamespace == nil {
		vlog.Warn("No NetworkNamespace found, falling back to direct control plane IPs")
		return []string{controlPlaneIPs[0]}, nil
	}

	// Create or get ControlPlaneVirtualSharedIP for load balancing
	// Initially only use the first control plane IP - more will be added after they're configured
	firstControlPlaneIP := []string{controlPlaneIPs[0]}
	vipEndpoints, err := t.ensureControlPlaneVIPs(ctx, networkNamespace, cluster, firstControlPlaneIP)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure control plane VIP: %w", err)
	}

	vlog.Info(fmt.Sprintf("Endpoint mode 'networkconfiguration': using VIP endpoints: %v", vipEndpoints))
	return vipEndpoints, nil
}

// getEndpointsTalosVIP returns endpoints for Talos built-in VIP mode.
// In this mode, the operator uses the control plane IPs directly, but Talos
// handles VIP failover internally through its machine config.
// Note: The actual VIP configuration should be added to tenant config overrides.
func (t *TalosManager) getEndpointsTalosVIP(_ context.Context, _ *vitistackv1alpha1.KubernetesCluster, controlPlaneIPs []string) ([]string, error) {
	if len(controlPlaneIPs) == 0 {
		return nil, fmt.Errorf("no control plane IPs available for endpoint mode 'talosvip'")
	}

	// For Talos VIP mode, we need to check if a custom VIP is configured in tenant overrides
	// The VIP address should be configured in the Talos machine config via tenant overrides
	// For now, use the first control plane IP as the endpoint
	// The actual VIP will be handled by Talos' internal VIP mechanism
	vlog.Info(fmt.Sprintf("Endpoint mode 'talosvip': using first control plane IP: %s (Talos VIP should be configured in machine config)", controlPlaneIPs[0]))
	vlog.Warn("Talos VIP mode requires additional configuration in tenant overrides to set the VIP address")

	// Return first control plane IP - the actual VIP address should be in the Talos config
	return []string{controlPlaneIPs[0]}, nil
}

// getEndpointsCustom returns user-provided endpoint addresses from the CUSTOM_ENDPOINT environment variable.
// This allows users to specify their own load balancer or VIP addresses.
func (t *TalosManager) getEndpointsCustom() ([]string, error) {
	customEndpoint := strings.TrimSpace(viper.GetString(consts.CUSTOM_ENDPOINT))
	if customEndpoint == "" {
		return nil, fmt.Errorf("endpoint mode 'custom' requires CUSTOM_ENDPOINT environment variable to be set")
	}

	// Parse comma-separated endpoints
	endpoints := strings.Split(customEndpoint, ",")
	var validEndpoints []string
	for _, ep := range endpoints {
		ep = strings.TrimSpace(ep)
		if ep != "" {
			validEndpoints = append(validEndpoints, ep)
		}
	}

	if len(validEndpoints) == 0 {
		return nil, fmt.Errorf("no valid endpoints found in CUSTOM_ENDPOINT: %s", customEndpoint)
	}

	vlog.Info(fmt.Sprintf("Endpoint mode 'custom': using custom endpoints: %v", validEndpoints))
	return validEndpoints, nil
}

// ensureControlPlaneVIPs creates or updates a ControlPlaneVirtualSharedIP resource and waits for LoadBalancerIps
func (t *TalosManager) ensureControlPlaneVIPs(ctx context.Context, networkNamespace *vitistackv1alpha1.NetworkNamespace, cluster *vitistackv1alpha1.KubernetesCluster, controlPlaneIPs []string) ([]string, error) {
	vipName := cluster.Spec.Cluster.ClusterId

	vip := &vitistackv1alpha1.ControlPlaneVirtualSharedIP{}
	err := t.Get(ctx, types.NamespacedName{Name: vipName, Namespace: cluster.Namespace}, vip)

	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get ControlPlaneVirtualSharedIP: %w", err)
		}

		// Create new VIP
		vip = &vitistackv1alpha1.ControlPlaneVirtualSharedIP{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vipName,
				Namespace: cluster.Namespace,
				Labels: map[string]string{
					vitistackv1alpha1.ClusterIdAnnotation: cluster.Spec.Cluster.ClusterId,
					vitistackv1alpha1.NodeRoleAnnotation:  "control-plane",
				},
			},
			Spec: vitistackv1alpha1.ControlPlaneVirtualSharedIPSpec{
				DatacenterIdentifier:       networkNamespace.Spec.DatacenterIdentifier,
				ClusterIdentifier:          cluster.Spec.Cluster.ClusterId,
				SupervisorIdentifier:       networkNamespace.Spec.SupervisorIdentifier,
				Provider:                   cluster.Spec.Cluster.Provider.String(),
				Method:                     "first-alive",
				PoolMembers:                controlPlaneIPs,
				Environment:                cluster.Spec.Cluster.Environment,
				NetworkNamespaceIdentifier: networkNamespace.Name,
			},
		}

		if err := t.Create(ctx, vip); err != nil {
			return nil, fmt.Errorf("failed to create ControlPlaneVirtualSharedIP: %w", err)
		}
		vlog.Info(fmt.Sprintf("Created ControlPlaneVirtualSharedIP: %s/%s", vip.Namespace, vip.Name))
	} else if !stringSlicesEqual(vip.Spec.PoolMembers, controlPlaneIPs) {
		// Update existing VIP if pool members changed
		vip.Spec.PoolMembers = controlPlaneIPs
		if err := t.Update(ctx, vip); err != nil {
			return nil, fmt.Errorf("failed to update ControlPlaneVirtualSharedIP: %w", err)
		}
		vlog.Info(fmt.Sprintf("Updated ControlPlaneVirtualSharedIP pool members: %s/%s", vip.Namespace, vip.Name))
	}

	// Wait for LoadBalancerIps to be populated
	endpoints, err := t.waitForVIPLoadBalancerIP(ctx, cluster, vipName, 15*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed waiting for VIP LoadBalancerIps: %w", err)
	}

	vlog.Info(fmt.Sprintf("Using VIP endpoint for cluster %s: %v", cluster.Name, endpoints))
	return endpoints, nil
}

// waitForVIPLoadBalancerIP waits for the ControlPlaneVirtualSharedIP status to have LoadBalancerIps populated
// It handles cluster deletion, VIP errors, and provides proper error context
func (t *TalosManager) waitForVIPLoadBalancerIP(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, vipName string, timeout time.Duration) ([]string, error) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled while waiting for VIP LoadBalancerIps: %w", ctx.Err())
		case <-ticker.C:
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("timeout waiting for VIP LoadBalancerIps after %v", timeout)
			}

			// Check if cluster is being deleted
			if err := t.checkClusterDeletion(ctx, cluster); err != nil {
				return nil, err
			}

			// Check VIP status and get LoadBalancerIps
			loadBalancerIps, shouldContinue, err := t.checkVIPStatus(ctx, cluster.Namespace, vipName)
			if err != nil {
				return nil, err
			}
			if shouldContinue {
				continue
			}
			if loadBalancerIps != nil {
				return loadBalancerIps, nil
			}
		}
	}
}

// checkClusterDeletion checks if the cluster is being deleted or has been removed
func (t *TalosManager) checkClusterDeletion(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	clusterCheck := &vitistackv1alpha1.KubernetesCluster{}
	if err := t.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, clusterCheck); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("cluster %s/%s was deleted while waiting for VIP", cluster.Namespace, cluster.Name)
		}
		vlog.Warn(fmt.Sprintf("Failed to check cluster status: %v", err))
		// Don't return error on transient errors, let retry continue
		return nil
	}
	if clusterCheck.GetDeletionTimestamp() != nil {
		return fmt.Errorf("cluster %s/%s is being deleted, cancelling VIP wait", cluster.Namespace, cluster.Name)
	}
	return nil
}

// checkVIPStatus checks VIP status and returns LoadBalancerIps if ready
// Returns (loadBalancerIps, shouldContinue, error)
func (t *TalosManager) checkVIPStatus(ctx context.Context, namespace, vipName string) ([]string, bool, error) {
	vip := &vitistackv1alpha1.ControlPlaneVirtualSharedIP{}
	if err := t.Get(ctx, types.NamespacedName{Name: vipName, Namespace: namespace}, vip); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, fmt.Errorf("VIP %s/%s was deleted or not found", namespace, vipName)
		}
		vlog.Warn(fmt.Sprintf("Failed to get VIP status: %v", err))
		// Continue on transient errors
		return nil, true, nil
	}

	// Check for VIP error conditions
	if vip.Status.Phase == "Failed" || vip.Status.Status == "Failed" {
		msg := vip.Status.Message
		if msg == "" {
			msg = "VIP creation failed without details"
		}
		return nil, false, fmt.Errorf("VIP %s/%s failed: %s", namespace, vipName, msg)
	}

	// Check if LoadBalancerIps are populated
	if len(vip.Status.LoadBalancerIps) > 0 {
		vlog.Info(fmt.Sprintf("VIP %s/%s ready with LoadBalancerIps: %v", namespace, vipName, vip.Status.LoadBalancerIps))
		return vip.Status.LoadBalancerIps, false, nil
	}

	// Log current status for debugging
	statusInfo := fmt.Sprintf("phase=%s, status=%s", vip.Status.Phase, vip.Status.Status)
	if vip.Status.Message != "" {
		statusInfo += fmt.Sprintf(", message=%s", vip.Status.Message)
	}
	vlog.Info(fmt.Sprintf("Waiting for VIP %s/%s to have LoadBalancerIps populated... [%s]", namespace, vipName, statusInfo))
	return nil, true, nil
}

// stringSlicesEqual checks if two string slices are equal
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// getConfiguredNodes returns the list of node names that have been configured
func (t *TalosManager) getConfiguredNodes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) ([]string, error) {
	secret, err := t.secretService.GetTalosSecret(ctx, cluster)
	if err != nil {
		return nil, err
	}
	if secret.Data == nil {
		return nil, nil
	}
	nodesStr := string(secret.Data["configured_nodes"])
	if nodesStr == "" {
		return nil, nil
	}
	return strings.Split(nodesStr, ","), nil
}

// isNodeConfigured checks if a node has already been configured
func (t *TalosManager) isNodeConfigured(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) bool {
	nodes, err := t.getConfiguredNodes(ctx, cluster)
	if err != nil {
		return false
	}
	for _, n := range nodes {
		if n == nodeName {
			return true
		}
	}
	return false
}

// addConfiguredNode adds a node name to the list of configured nodes in the secret
func (t *TalosManager) addConfiguredNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := t.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}

		existingNodes := string(secret.Data["configured_nodes"])
		var nodes []string
		if existingNodes != "" {
			nodes = strings.Split(existingNodes, ",")
		}

		// Check if already exists
		for _, n := range nodes {
			if n == nodeName {
				return nil // Already configured
			}
		}

		nodes = append(nodes, nodeName)
		secret.Data["configured_nodes"] = []byte(strings.Join(nodes, ","))

		err = t.secretService.UpdateTalosSecret(ctx, secret)
		if err == nil {
			return nil
		}

		if apierrors.IsConflict(err) && attempt < maxRetries-1 {
			time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1))
			continue
		}
		return err
	}
	return fmt.Errorf("failed to add configured node after %d retries", maxRetries)
}

// reconcileNewNodes handles adding new nodes to an existing cluster
// This is called when the cluster is already initialized but new machines have been added
func (t *TalosManager) reconcileNewNodes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	newMachines, err := t.findUnconfiguredMachines(ctx, cluster)
	if err != nil {
		return err
	}
	if len(newMachines) == 0 {
		return nil
	}

	vlog.Info(fmt.Sprintf("Found %d new machines to configure for cluster %s", len(newMachines), cluster.Name))

	// Wait for new machines to be ready
	readyMachines, err := t.machineService.WaitForMachinesReady(ctx, newMachines)
	if err != nil {
		return fmt.Errorf("failed waiting for new machines to be ready: %w", err)
	}

	// Load configuration context
	configCtx, err := t.loadNewNodeConfigContext(ctx, cluster)
	if err != nil {
		return err
	}

	// Separate and configure new control planes and workers
	newControlPlanes := t.machineService.FilterMachinesByRole(readyMachines, controlPlaneRole)
	newWorkers := filterNonControlPlanes(readyMachines)

	// Configure new control planes first
	if err := t.configureNewControlPlanes(ctx, cluster, configCtx, newControlPlanes); err != nil {
		return err
	}

	// Configure new workers
	return t.configureNewWorkers(ctx, cluster, configCtx, newWorkers)
}

// updateVIPPoolMembers updates the VIP with current control plane IPs
func (t *TalosManager) updateVIPPoolMembers(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	// Only update VIP pool members if using networkconfiguration endpoint mode
	endpointMode := consts.EndpointMode(viper.GetString(consts.ENDPOINT_MODE))
	if endpointMode != consts.EndpointModeNetworkConfiguration {
		vlog.Info(fmt.Sprintf("Skipping VIP pool member update: endpoint mode is '%s', not 'networkconfiguration'", endpointMode))
		return nil
	}

	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return err
	}

	controlPlanes := t.machineService.FilterMachinesByRole(machines, controlPlaneRole)
	controlPlaneIPs := extractIPv4Addresses(controlPlanes)

	vipName := cluster.Spec.Cluster.ClusterId
	vip := &vitistackv1alpha1.ControlPlaneVirtualSharedIP{}
	if err := t.Get(ctx, types.NamespacedName{Name: vipName, Namespace: cluster.Namespace}, vip); err != nil {
		if apierrors.IsNotFound(err) {
			vlog.Warn(fmt.Sprintf("VIP %s not found, skipping pool member update", vipName))
			return nil
		}
		return err
	}

	if !stringSlicesEqual(vip.Spec.PoolMembers, controlPlaneIPs) {
		vip.Spec.PoolMembers = controlPlaneIPs
		if err := t.Update(ctx, vip); err != nil {
			return fmt.Errorf("failed to update VIP pool members: %w", err)
		}
		vlog.Info(fmt.Sprintf("Updated VIP pool members: vip=%s members=%v", vipName, controlPlaneIPs))
	}

	return nil
}

// nodeGroupConfig holds configuration for applying configs to a group of machines
type nodeGroupConfig struct {
	stageNum      int
	nodeType      string
	phase         string
	conditionName string
	flagName      string
	stopOnError   bool
	waitForReboot bool // whether to wait for node to reboot after applying config
}

// applyConfigToMachineGroup applies configuration to a group of machines (control planes or workers)
func (t *TalosManager) applyConfigToMachineGroup(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	machines []*vitistackv1alpha1.Machine,
	insecure bool,
	tenantOverrides map[string]any,
	endpointIP string,
	cfg *nodeGroupConfig,
) error {
	if len(machines) > 0 {
		vlog.Info(fmt.Sprintf("Stage %d: Applying config to %ss: count=%d cluster=%s", cfg.stageNum, cfg.nodeType, len(machines), cluster.Name))
		_ = t.statusManager.SetPhase(ctx, cluster, cfg.phase)
		_ = t.statusManager.SetCondition(ctx, cluster, cfg.conditionName, "False", "Applying", fmt.Sprintf("Applying config to %ss", cfg.nodeType))

		for _, m := range machines {
			if t.isNodeConfigured(ctx, cluster, m.Name) {
				vlog.Info(fmt.Sprintf("%s already configured, skipping: node=%s", capitalizeFirst(cfg.nodeType), m.Name))
				continue
			}

			// Get node IP for logging and waiting
			nodeIP := getFirstIPv4(m)
			if nodeIP == "" {
				vlog.Warn(fmt.Sprintf("No IPv4 address found for %s %s, skipping", cfg.nodeType, m.Name))
				continue
			}

			vlog.Info(fmt.Sprintf("Applying config to %s: node=%s ip=%s", cfg.nodeType, m.Name, nodeIP))
			if err := t.applyPerNodeConfiguration(ctx, cluster, clientConfig, []*vitistackv1alpha1.Machine{m}, insecure, tenantOverrides, endpointIP); err != nil {
				_ = t.statusManager.SetCondition(ctx, cluster, cfg.conditionName, "False", "ApplyError", fmt.Sprintf("Failed on node %s: %s", m.Name, err.Error()))
				if cfg.stopOnError {
					return fmt.Errorf("failed to apply config to %s %s: %w", cfg.nodeType, m.Name, err)
				}
				vlog.Error(fmt.Sprintf("Failed to apply config to %s %s", cfg.nodeType, m.Name), err)
				continue
			}

			// Wait for node to reboot and come back up if configured
			// This is important for maintenance mode nodes that install and reboot after receiving config
			if cfg.waitForReboot && insecure {
				vlog.Info(fmt.Sprintf("Waiting for %s to reboot after config apply: node=%s ip=%s", cfg.nodeType, m.Name, nodeIP))
				if err := t.clientService.WaitForNodeRebootAfterApply(nodeIP, 10*time.Second, 5*time.Minute, 10*time.Second); err != nil {
					vlog.Warn(fmt.Sprintf("Warning: timeout waiting for %s %s to reboot, continuing anyway: %v", cfg.nodeType, m.Name, err))
					// Don't fail here - the node might still come up, and the controller will handle it
				}
			}

			if err := t.addConfiguredNode(ctx, cluster, m.Name); err != nil {
				vlog.Error(fmt.Sprintf("Failed to add %s %s to configured nodes", cfg.nodeType, m.Name), err)
			}
		}
		vlog.Info(fmt.Sprintf("Stage %d complete: Config applied to all %ss", cfg.stageNum, cfg.nodeType))
	} else {
		vlog.Info(fmt.Sprintf("Stage %d: No %ss to configure", cfg.stageNum, cfg.nodeType))
	}

	if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{cfg.flagName: true}); err != nil {
		vlog.Error(fmt.Sprintf("Failed to set %s flag in secret", cfg.flagName), err)
		return fmt.Errorf("failed to persist %s flag: %w", cfg.flagName, err)
	}
	_ = t.statusManager.SetCondition(ctx, cluster, cfg.conditionName, "True", "Applied", fmt.Sprintf("Talos config applied to all %ss", cfg.nodeType))
	return nil
}

// filterNonControlPlanes returns machines that are not control planes
func filterNonControlPlanes(machines []*vitistackv1alpha1.Machine) []*vitistackv1alpha1.Machine {
	var result []*vitistackv1alpha1.Machine
	for _, m := range machines {
		role := m.Labels[vitistackv1alpha1.NodeRoleAnnotation]
		if role != controlPlaneRole {
			result = append(result, m)
		}
	}
	return result
}

// capitalizeFirst capitalizes the first letter of a string
func capitalizeFirst(s string) string {
	if len(s) == 0 {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// extractIPv4Addresses extracts the first IPv4 address from each machine
func extractIPv4Addresses(machines []*vitistackv1alpha1.Machine) []string {
	var ips []string
	for _, m := range machines {
		for _, ipAddr := range m.Status.PublicIPAddresses {
			ip := net.ParseIP(ipAddr)
			if ip != nil && ip.To4() != nil {
				ips = append(ips, ipAddr)
				break
			}
		}
	}
	return ips
}

// getFirstIPv4 returns the first IPv4 address from a machine's public IPs
func getFirstIPv4(m *vitistackv1alpha1.Machine) string {
	for _, ipAddr := range m.Status.PublicIPAddresses {
		parsedIP := net.ParseIP(ipAddr)
		if parsedIP != nil && parsedIP.To4() != nil {
			return ipAddr
		}
	}
	return ""
}

// newNodeConfigContext holds configuration context for adding new nodes
type newNodeConfigContext struct {
	clientConfig    *clientconfig.Config
	tenantOverrides map[string]any
	endpointIP      string
}

// findUnconfiguredMachines finds machines that haven't been configured yet
func (t *TalosManager) findUnconfiguredMachines(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) ([]*vitistackv1alpha1.Machine, error) {
	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster machines: %w", err)
	}

	if len(machines) == 0 {
		return nil, nil
	}

	configuredNodes, err := t.getConfiguredNodes(ctx, cluster)
	if err != nil {
		vlog.Warn(fmt.Sprintf("Failed to get configured nodes: %v", err))
		configuredNodes = []string{}
	}
	configuredSet := make(map[string]bool)
	for _, n := range configuredNodes {
		configuredSet[n] = true
	}

	var newMachines []*vitistackv1alpha1.Machine
	for _, m := range machines {
		if !configuredSet[m.Name] {
			newMachines = append(newMachines, m)
		}
	}
	return newMachines, nil
}

// loadNewNodeConfigContext loads configuration context needed for adding new nodes
func (t *TalosManager) loadNewNodeConfigContext(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*newNodeConfigContext, error) {
	clientConfig, _, err := t.loadTalosArtifacts(ctx, cluster)
	if err != nil || clientConfig == nil {
		return nil, fmt.Errorf("failed to load talos artifacts for new node configuration: %w", err)
	}

	tenantOverrides, err := t.loadTenantOverrides(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to load tenant overrides: %w", err)
	}

	// Determine endpoint IP based on endpoint mode
	endpointIP, err := t.getEndpointIPForNewNodes(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get endpoint IP for new nodes: %w", err)
	}

	return &newNodeConfigContext{
		clientConfig:    clientConfig,
		tenantOverrides: tenantOverrides,
		endpointIP:      endpointIP,
	}, nil
}

// getEndpointIPForNewNodes returns the endpoint IP to use when configuring new nodes.
// It respects the configured endpoint mode.
func (t *TalosManager) getEndpointIPForNewNodes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (string, error) {
	endpointMode := consts.EndpointMode(viper.GetString(consts.ENDPOINT_MODE))

	switch endpointMode {
	case consts.EndpointModeNone, consts.EndpointModeTalosVIP:
		// For 'none' and 'talosvip' modes, get the first control plane IP
		machines, err := t.machineService.GetClusterMachines(ctx, cluster)
		if err != nil {
			return "", fmt.Errorf("failed to get cluster machines: %w", err)
		}
		controlPlanes := t.machineService.FilterMachinesByRole(machines, controlPlaneRole)
		controlPlaneIPs := extractIPv4Addresses(controlPlanes)
		if len(controlPlaneIPs) == 0 {
			return "", fmt.Errorf("no control plane IPs found")
		}
		return controlPlaneIPs[0], nil

	case consts.EndpointModeCustom:
		// For 'custom' mode, use the configured custom endpoint
		customEndpoint := strings.TrimSpace(viper.GetString(consts.CUSTOM_ENDPOINT))
		if customEndpoint == "" {
			return "", fmt.Errorf("CUSTOM_ENDPOINT not set for endpoint mode 'custom'")
		}
		// Return first endpoint if comma-separated
		endpoints := strings.Split(customEndpoint, ",")
		return strings.TrimSpace(endpoints[0]), nil

	case consts.EndpointModeNetworkConfiguration:
		fallthrough
	default:
		// For 'networkconfiguration' mode, get from VIP
		vipName := cluster.Spec.Cluster.ClusterId
		vip := &vitistackv1alpha1.ControlPlaneVirtualSharedIP{}
		if err := t.Get(ctx, types.NamespacedName{Name: vipName, Namespace: cluster.Namespace}, vip); err != nil {
			return "", fmt.Errorf("failed to get VIP for new node configuration: %w", err)
		}
		if len(vip.Status.LoadBalancerIps) == 0 {
			return "", fmt.Errorf("VIP has no LoadBalancerIps, cannot configure new nodes")
		}
		return vip.Status.LoadBalancerIps[0], nil
	}
}

// configureNewControlPlanes configures new control plane nodes
func (t *TalosManager) configureNewControlPlanes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, configCtx *newNodeConfigContext, nodes []*vitistackv1alpha1.Machine) error {
	if len(nodes) == 0 {
		return nil
	}

	vlog.Info(fmt.Sprintf("Configuring %d new control planes for cluster %s", len(nodes), cluster.Name))
	_ = t.statusManager.SetCondition(ctx, cluster, "NewControlPlanesConfiguring", "True", "Configuring", fmt.Sprintf("Configuring %d new control planes", len(nodes)))

	for _, node := range nodes {
		if err := t.configureNewNode(ctx, cluster, configCtx, node, "control plane"); err != nil {
			vlog.Error(fmt.Sprintf("Failed to configure new control plane %s", node.Name), err)
			continue
		}
	}

	if err := t.updateVIPPoolMembers(ctx, cluster); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to update VIP pool members: %v", err))
	}

	_ = t.statusManager.SetCondition(ctx, cluster, "NewControlPlanesConfiguring", "False", "Configured", "New control planes configured")
	return nil
}

// configureNewWorkers configures new worker nodes
func (t *TalosManager) configureNewWorkers(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, configCtx *newNodeConfigContext, nodes []*vitistackv1alpha1.Machine) error {
	if len(nodes) == 0 {
		return nil
	}

	vlog.Info(fmt.Sprintf("Configuring %d new workers for cluster %s", len(nodes), cluster.Name))
	_ = t.statusManager.SetCondition(ctx, cluster, "NewWorkersConfiguring", "True", "Configuring", fmt.Sprintf("Configuring %d new workers", len(nodes)))

	for _, node := range nodes {
		if err := t.configureNewNode(ctx, cluster, configCtx, node, "worker"); err != nil {
			vlog.Error(fmt.Sprintf("Failed to configure new worker %s", node.Name), err)
			continue
		}
	}

	_ = t.statusManager.SetCondition(ctx, cluster, "NewWorkersConfiguring", "False", "Configured", "New workers configured")
	return nil
}

// configureNewNode configures a single new node
func (t *TalosManager) configureNewNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, configCtx *newNodeConfigContext, node *vitistackv1alpha1.Machine, nodeType string) error {
	ip := getFirstIPv4(node)
	if ip == "" {
		vlog.Warn(fmt.Sprintf("New %s %s missing IPv4 address, skipping", nodeType, node.Name))
		return nil
	}

	vlog.Info(fmt.Sprintf("Applying config to new %s: node=%s ip=%s", nodeType, node.Name, ip))
	// Use insecure mode for new nodes - they don't have the cluster CA yet
	if err := t.applyPerNodeConfiguration(ctx, cluster, configCtx.clientConfig, []*vitistackv1alpha1.Machine{node}, true, configCtx.tenantOverrides, configCtx.endpointIP); err != nil {
		return fmt.Errorf("failed to apply config: %w", err)
	}
	if err := t.addConfiguredNode(ctx, cluster, node.Name); err != nil {
		vlog.Error(fmt.Sprintf("Failed to add new %s %s to configured nodes", nodeType, node.Name), err)
	}
	vlog.Info(fmt.Sprintf("New %s configured successfully: node=%s", nodeType, node.Name))
	return nil
}

// mergeBootstrappedFlag ensures we preserve the bootstrapped flag from an existing Secret,
// and defaults it to false if not present.
func mergeBootstrappedFlag(existing *corev1.Secret, data map[string][]byte) {
	if existing != nil && existing.Data != nil {
		if v, ok := existing.Data["bootstrapped"]; ok {
			data["bootstrapped"] = v
		}
	}
	if _, ok := data["bootstrapped"]; !ok {
		data["bootstrapped"] = []byte("false")
	}
}

// computePresence inspects both new data and existing secret to determine presence flags.
func computePresence(existing *corev1.Secret, data map[string][]byte) (hasTalos, hasCP, hasW, hasK bool) {
	// helper to check key in new data or existing secret
	present := func(key string) bool {
		if len(data[key]) > 0 {
			return true
		}
		if existing != nil && existing.Data != nil && len(existing.Data[key]) > 0 {
			return true
		}
		return false
	}
	hasTalos = present("talosconfig")
	hasCP = present("controlplane.yaml")
	hasW = present("worker.yaml")
	hasK = present("kube.config")
	return
}

// setPresenceFlags writes presence booleans (and cluster_access if kubeconfig present).
func setPresenceFlags(data map[string][]byte, hasTalos, hasCP, hasW, hasK bool) {
	data["talosconfig_present"] = []byte(strconv.FormatBool(hasTalos))
	data["controlplane_yaml_present"] = []byte(strconv.FormatBool(hasCP))
	data["worker_yaml_present"] = []byte(strconv.FormatBool(hasW))
	data["kubeconfig_present"] = []byte(strconv.FormatBool(hasK))
	if hasK {
		data["cluster_access"] = []byte("true")
	}
}

// applyDataToSecret merges the provided data into the secret (creating Data map if needed),
// skipping empty kube.config to avoid accidental deletion.
func applyDataToSecret(secret *corev1.Secret, data map[string][]byte) {
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	for k, v := range data {
		if k == "kube.config" && len(v) == 0 {
			continue
		}
		secret.Data[k] = v
	}
}

// upsertTalosClusterConfigSecretWithRoleYAML creates/updates the consolidated Secret with talosconfig, secrets bundle, and role templates.
func (t *TalosManager) upsertTalosClusterConfigSecretWithRoleYAML(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientCfg *clientconfig.Config,
	controlPlaneYAML []byte,
	workerYAML []byte,
	secretsBundleYAML []byte,
	kubeconfig []byte,
) error {
	secret, err := t.secretService.GetTalosSecret(ctx, cluster)

	data := map[string][]byte{}
	if b, mErr := t.configService.MarshalTalosClientConfig(clientCfg); mErr != nil {
		return mErr
	} else if len(b) > 0 {
		data["talosconfig"] = b
	}
	if len(controlPlaneYAML) > 0 {
		data["controlplane.yaml"] = controlPlaneYAML
	}
	if len(workerYAML) > 0 {
		data["worker.yaml"] = workerYAML
	}
	// Store the secrets bundle for future regeneration of configs
	if len(secretsBundleYAML) > 0 {
		data["secrets.bundle"] = secretsBundleYAML
	}
	if len(kubeconfig) > 0 {
		data["kube.config"] = kubeconfig
	}

	// preserve bootstrapped flag and default to false when missing
	var existing *corev1.Secret
	if err == nil {
		existing = secret
	}
	mergeBootstrappedFlag(existing, data)

	// presence flags
	hasTalos, hasCP, hasW, hasK := computePresence(existing, data)
	setPresenceFlags(data, hasTalos, hasCP, hasW, hasK)

	if err != nil {
		// create
		return t.secretService.CreateTalosSecret(ctx, cluster, data)
	}
	applyDataToSecret(secret, data)
	return t.secretService.UpdateTalosSecret(ctx, secret)
}

// upsertTalosClusterConfigSecret stores Talos client config, role configs, kubeconfig, and bootstrapped flag in a single Secret.
// Secret name: <cluster id>
func (t *TalosManager) upsertTalosClusterConfigSecret(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientCfg *clientconfig.Config,
	kubeconfig []byte,
) error {
	name := fmt.Sprintf("%s%s", viper.GetString(consts.SECRET_PREFIX), cluster.Spec.Cluster.ClusterId)
	secret := &corev1.Secret{}
	err := t.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret)

	// Prepare data payload
	data := map[string][]byte{}

	if b, mErr := t.configService.MarshalTalosClientConfig(clientCfg); mErr != nil {
		return mErr
	} else if len(b) > 0 {
		data["talosconfig"] = b
	}

	// preserve existing role templates
	if err == nil && secret.Data != nil {
		if v, ok := secret.Data["controlplane.yaml"]; ok {
			data["controlplane.yaml"] = v
		}
		if v, ok := secret.Data["worker.yaml"]; ok {
			data["worker.yaml"] = v
		}
	}

	// preserve bootstrapped flag and default to false
	var existing *corev1.Secret
	if err == nil {
		existing = secret
	}
	mergeBootstrappedFlag(existing, data)

	// kubeconfig (optional update)
	if len(kubeconfig) > 0 {
		data["kube.config"] = kubeconfig
	}

	// presence flags
	hasTalos, hasCP, hasW, hasK := computePresence(existing, data)
	setPresenceFlags(data, hasTalos, hasCP, hasW, hasK)

	if err != nil {
		// create
		return t.secretService.CreateTalosSecret(ctx, cluster, data)
	}

	// update existing: merge, preserving existing kube.config if not provided now
	applyDataToSecret(secret, data)
	return t.secretService.UpdateTalosSecret(ctx, secret)
}
