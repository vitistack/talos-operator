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
			return nil
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
	controlPlanes := t.machineService.FilterMachinesByRole(readyMachines, "control-plane")

	// get other nodes except control plane
	workers := []*vitistackv1alpha1.Machine{}
	for _, machine := range readyMachines {
		role := machine.Labels[vitistackv1alpha1.NodeRoleAnnotation]
		if role != "control-plane" {
			workers = append(workers, machine)
		}
	}

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

	networkNamespace, err := t.findNetworkNamespace(ctx, cluster.GetNamespace())
	if err != nil {
		return fmt.Errorf("failed to find network namespace: %w", err)
	}

	_ = t.statusManager.SetPhase(ctx, cluster, "ControlPlaneIPsPending")
	_ = t.statusManager.SetCondition(ctx, cluster, "", "True", "Generated", "Talos client and role configs generated")
	// Create or get ControlPlaneVirtualSharedIP for load balancing
	vipEndpoints, err := t.ensureControlPlaneVIPs(ctx, networkNamespace, cluster, controlPlaneIPs)
	if err != nil {
		return fmt.Errorf("failed to ensure control plane VIP: %w", err)
	}
	endpointIPs := vipEndpoints
	tenantOverrides, err := t.loadTenantOverrides(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to load tenant overrides: %w", err)
	}

	// Prefer existing artifacts from Secret; generate only if missing
	clientConfig, fromSecret, err := t.loadTalosArtifacts(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to load talos artifacts: %w", err)
	}
	if !fromSecret {
		// Generate Talos configuration
		genClientCfg, cpYAML, wYAML, err := t.configService.GenerateTalosConfig(cluster, readyMachines, endpointIPs)
		if err != nil {
			return fmt.Errorf("failed to generate Talos config: %w", err)
		}
		vlog.Info(fmt.Sprintf("Generated Talos client config: cluster=%s hasConfig=%t", cluster.Name, genClientCfg != nil))
		// Update status phase: config generated
		_ = t.statusManager.SetPhase(ctx, cluster, "ConfigGenerated")
		_ = t.statusManager.SetCondition(ctx, cluster, "ConfigGenerated", "True", "Generated", "Talos client and role configs generated")
		// Persist initial Talos configs into a Secret (without kubeconfig yet)
		if err := t.upsertTalosClusterConfigSecretWithRoleYAML(ctx, cluster, genClientCfg, cpYAML, wYAML, nil /* kubeconfig */); err != nil {
			return fmt.Errorf("failed to persist Talos config secret: %w", err)
		}
		clientConfig = genClientCfg
	} else {
		vlog.Info("Loaded Talos artifacts from Secret: cluster=" + cluster.Name)
	}

	if err := t.persistTenantRoleTemplates(ctx, cluster, tenantOverrides); err != nil {
		return fmt.Errorf("failed to persist tenant role templates: %w", err)
	}

	// Use insecure connections only until Talos API is confirmed ready with certificates
	flags, _ := t.getTalosSecretFlags(ctx, cluster)
	insecure := !flags.TalosAPIReady // Use secure connections once API is ready with certs

	// We'll create a secure Talos client later after nodes come back with generated certs
	// talosClientSecure will be established after readiness wait.

	// using per-node application for initial config; central apply path kept for potential future use
	// Only proceed with config application if we have the role templates (generated above if needed)
	if !flags.ControlPlaneApplied {
		// Verify role templates exist before attempting to apply
		secretCheck, errCheck := t.secretService.GetTalosSecret(ctx, cluster)
		if errCheck != nil {
			return fmt.Errorf("failed to verify secret before config application: %w", errCheck)
		}
		if len(secretCheck.Data["controlplane.yaml"]) == 0 || len(secretCheck.Data["worker.yaml"]) == 0 {
			return fmt.Errorf("role templates not present in secret yet, cannot apply config (this should not happen)")
		}

		if err := t.applyPerNodeConfiguration(ctx, cluster, clientConfig, controlPlanes, insecure, tenantOverrides, endpointIPs[0]); err != nil {
			_ = t.statusManager.SetCondition(ctx, cluster, "ControlPlaneConfigApplied", "False", "ApplyError", err.Error())
			return err
		}
		if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"controlplane_applied": true}); err != nil {
			vlog.Error("Failed to set controlplane_applied flag in secret", err)
			return fmt.Errorf("failed to persist controlplane_applied flag: %w", err)
		}
		vlog.Info("Secret status updated: controlplane_applied=true, cluster=" + cluster.Name)
		_ = t.statusManager.SetCondition(ctx, cluster, "ControlPlaneConfigApplied", "True", "Applied", "Talos config applied to control planes")
	} else {
		vlog.Info("Control-plane config already applied, skipping: cluster=" + cluster.Name)
	}

	if !flags.WorkerApplied {
		if err := t.applyPerNodeConfiguration(ctx, cluster, clientConfig, workers, insecure, tenantOverrides, endpointIPs[0]); err != nil {
			_ = t.statusManager.SetCondition(ctx, cluster, "WorkerConfigApplied", "False", "ApplyError", err.Error())
			return err
		}
		if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"worker_applied": true}); err != nil {
			vlog.Error("Failed to set worker_applied flag in secret", err)
			return fmt.Errorf("failed to persist worker_applied flag: %w", err)
		}
		vlog.Info("Secret status updated: worker_applied=true, cluster=" + cluster.Name)
		_ = t.statusManager.SetCondition(ctx, cluster, "WorkerConfigApplied", "True", "Applied", "Talos config applied to workers")
	} else {
		vlog.Info("Worker config already applied, skipping: cluster=" + cluster.Name)
	}

	// Update status phase: configs applied
	_ = t.statusManager.SetPhase(ctx, cluster, "ConfigApplied")
	_ = t.statusManager.SetCondition(ctx, cluster, "ConfigApplied", "True", "Applied", "Talos configs applied to all nodes")

	// todo registert VIP ip addresses to create loadbalancers for talos control planes
	// write to a crd, so others can handle load balancing of control planes

	// Bootstrap the cluster (bootstrap exactly one control-plane)
	clusterState, _ := t.getTalosSecretFlags(ctx, cluster)
	_, hasKubeconfig, _ := t.getTalosSecretState(ctx, cluster)
	if len(controlPlaneIPs) > 0 && !clusterState.Bootstrapped {
		// Wait for control-plane nodes to be ready with their new config (Talos API reachable securely)
		_ = t.statusManager.SetPhase(ctx, cluster, "WaitingForTalosAPI")
		_ = t.statusManager.SetCondition(ctx, cluster, "WaitingForTalosAPI", "True", "Waiting", "Waiting for Talos API on control planes and workers")
		if err := t.clientService.WaitForTalosAPIs(controlPlanes, 10*time.Minute, 10*time.Second); err != nil {
			_ = t.statusManager.SetCondition(ctx, cluster, "TalosAPIReady", "False", "NotReady", err.Error())
			return fmt.Errorf("control planes not ready for bootstrap: %w", err)
		}

		// Also wait for worker nodes to be ready with their new config (Talos API reachable)
		// if len(workers) > 0 {
		// 	vlog.Info("Waiting for worker Talos APIs to be reachable: cluster=" + cluster.Name)
		// 	if err := t.clientService.WaitForTalosAPIs(workers, 10*time.Minute, 10*time.Second); err != nil {
		// 		vlog.Warn(fmt.Sprintf("Workers not ready yet (non-blocking): cluster=%s error=%v", cluster.Name, err))
		// 		// Don't block bootstrap on workers, just log a warning
		// 		// Workers can join later once control plane is ready
		// 	}
		// }

		_ = t.statusManager.SetPhase(ctx, cluster, "TalosAPIReady")
		_ = t.statusManager.SetCondition(ctx, cluster, "TalosAPIReady", "True", "Ready", "Talos API reachable on control planes")
		if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"talos_api_ready": true}); err != nil {
			vlog.Error("Failed to set talos_api_ready flag in secret", err)
			return fmt.Errorf("failed to persist talos_api_ready flag: %w", err)
		}
		vlog.Info("Secret status updated: talos_api_ready=true, cluster=" + cluster.Name)

		// Bootstrap using first control plane node directly (not VIP)
		// Create a client with direct control plane IP for bootstrap operation
		bootstrapClient, err := t.clientService.CreateTalosClient(ctx, false, clientConfig, []string{controlPlaneIPs[0]})
		if err != nil {
			return fmt.Errorf("failed to create Talos client for bootstrap: %w", err)
		}
		if err := t.clientService.BootstrapTalosControlPlaneWithRetry(ctx, bootstrapClient, controlPlaneIPs[0], 5*time.Minute, 10*time.Second); err != nil {
			_ = t.statusManager.SetCondition(ctx, cluster, "Bootstrapped", "False", "BootstrapError", err.Error())
			return fmt.Errorf("failed to bootstrap Talos cluster: %w", err)
		}
		_ = t.statusManager.SetPhase(ctx, cluster, "Bootstrapped")
		_ = t.statusManager.SetCondition(ctx, cluster, "Bootstrapped", "True", "Done", "Talos cluster bootstrapped")

		// mark bootstrapped in the persistent Secret
		if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"bootstrapped": true}); err != nil {
			vlog.Error("Failed to set bootstrapped flag in secret", err)
			return fmt.Errorf("failed to persist bootstrapped flag: %w", err)
		}
		vlog.Info("Secret status updated: bootstrapped=true, cluster=" + cluster.Name)
	} else if clusterState.Bootstrapped {
		vlog.Info("Cluster already bootstrapped, skipping bootstrap step: cluster=" + cluster.Name)
	}

	// Get Kubernetes access (fetch kubeconfig and store it as a Secret)
	if len(controlPlaneIPs) > 0 && !clusterState.ClusterAccess {
		// Use VIP endpoint for kubeconfig retrieval: nodesIp = actual node to target, endpointIp = VIP for connection
		kubeconfigBytes, err := t.clientService.GetKubeconfigWithRetry(ctx, clientConfig, controlPlaneIPs[0], endpointIPs[0], 5*time.Minute, 10*time.Second)
		if err != nil {
			return fmt.Errorf("failed to get kubeconfig: %w", err)
		}

		// Update consolidated Talos secret with kubeconfig and bootstrapped flag
		if err := t.upsertTalosClusterConfigSecret(ctx, cluster, clientConfig, kubeconfigBytes); err != nil {
			_ = t.statusManager.SetCondition(ctx, cluster, "KubeconfigAvailable", "False", "PersistError", err.Error())
			return fmt.Errorf("failed to update Talos config secret with kubeconfig: %w", err)
		}
		if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"cluster_access": true}); err != nil {
			vlog.Error("Failed to set cluster_access flag in secret", err)
			return fmt.Errorf("failed to persist cluster_access flag: %w", err)
		}
		vlog.Info("Secret status updated: cluster_access=true, cluster=" + cluster.Name)

		// Wait for all nodes to be Ready in Kubernetes (using Talos NodeStatus)
		allMachines := make([]*vitistackv1alpha1.Machine, 0, len(controlPlanes)+len(workers))
		allMachines = append(allMachines, controlPlanes...)
		allMachines = append(allMachines, workers...)
		vlog.Info(fmt.Sprintf("Waiting for all %d nodes to report Ready status: cluster=%s", len(allMachines), cluster.Name))
		_ = t.statusManager.SetCondition(ctx, cluster, "NodesReady", "False", "Waiting", "Waiting for all nodes to be ready in Kubernetes")

		// Mark cluster as ready with timestamp
		if err := t.setSecretTimestamp(ctx, cluster, "ready_at"); err != nil {
			vlog.Error("Failed to set ready_at timestamp in secret", err)
		} else {
			vlog.Info("Secret timestamp updated: ready_at, cluster=" + cluster.Name)
		}
		_ = t.statusManager.SetPhase(ctx, cluster, "Ready")
		_ = t.statusManager.SetCondition(ctx, cluster, "KubeconfigAvailable", "True", "Persisted", "Kubeconfig stored in Secret")
		vlog.Info("Cluster status set to Ready: cluster=" + cluster.Name)

		if !hasKubeconfig {
			vlog.Info("Kubeconfig stored in Secret: secret=" + secretservice.GetSecretName(cluster))
		}
	} else if clusterState.ClusterAccess {
		vlog.Info("Cluster access already established (kubeconfig present), skipping fetch: cluster=" + cluster.Name)
	}
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
		"bootstrapped":              []byte(falseStr),
		"talosconfig_present":       []byte(falseStr),
		"controlplane_yaml_present": []byte(falseStr),
		"worker_yaml_present":       []byte(falseStr),
		"kubeconfig_present":        []byte(falseStr),
		"talos_api_ready":           []byte(falseStr),
		"controlplane_applied":      []byte(falseStr),
		"worker_applied":            []byte(falseStr),
		"cluster_access":            []byte(falseStr),
		"created_at":                []byte(now),
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
	ControlPlaneApplied bool
	WorkerApplied       bool
	Bootstrapped        bool
	ClusterAccess       bool
	TalosAPIReady       bool // tracks when Talos API accepts secure (non-insecure) connections
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

func (t *TalosManager) persistTenantRoleTemplates(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, tenantOverrides map[string]any) error {
	if len(tenantOverrides) == 0 {
		return nil
	}

	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := t.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}

		cp := secret.Data["controlplane.yaml"]
		w := secret.Data["worker.yaml"]
		if len(cp) == 0 && len(w) == 0 {
			return nil
		}

		updated := false
		if mergedCP, err := t.configService.MergeRoleTemplateWithOverrides(cp, tenantOverrides); err != nil {
			return err
		} else if len(mergedCP) > 0 {
			secret.Data["controlplane-tenant.yaml"] = mergedCP
			updated = true
		}
		if mergedWorker, err := t.configService.MergeRoleTemplateWithOverrides(w, tenantOverrides); err != nil {
			return err
		} else if len(mergedWorker) > 0 {
			secret.Data["worker-tenant.yaml"] = mergedWorker
			updated = true
		}

		if !updated {
			return nil
		}

		if err := t.secretService.UpdateTalosSecret(ctx, secret); err != nil {
			if apierrors.IsConflict(err) && attempt < maxRetries-1 {
				time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1))
				continue
			}
			return err
		}
		return nil
	}

	return fmt.Errorf("failed to persist tenant role templates after %d retries", maxRetries)
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

// upsertTalosClusterConfigSecretWithRoleYAML creates/updates the consolidated Secret with talosconfig and role templates.
func (t *TalosManager) upsertTalosClusterConfigSecretWithRoleYAML(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientCfg *clientconfig.Config,
	controlPlaneYAML []byte,
	workerYAML []byte,
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
