package talos

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/vitistack/common/pkg/loggers/vlog"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/spf13/viper"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/internal/services/endpointservice"
	"github.com/vitistack/talos-operator/internal/services/etcdservice"
	"github.com/vitistack/talos-operator/internal/services/machineservice"
	"github.com/vitistack/talos-operator/internal/services/secretservice"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	"github.com/vitistack/talos-operator/internal/services/talosconfigservice"
	"github.com/vitistack/talos-operator/internal/services/talosstateservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	yaml "gopkg.in/yaml.v3"
)

const controlPlaneRole = "control-plane"

// TalosManager manages the lifecycle of Talos clusters
type TalosManager struct {
	client.Client
	statusManager   *status.StatusManager
	secretService   *secretservice.SecretService
	configService   *talosconfigservice.TalosConfigService
	clientService   *talosclientservice.TalosClientService
	machineService  *machineservice.MachineService
	endpointService *endpointservice.EndpointService
	stateService    *talosstateservice.TalosStateService
	etcdService     *etcdservice.EtcdService
}

// NewTalosManager creates a new instance of TalosManager
func NewTalosManager(c client.Client, statusManager *status.StatusManager) *TalosManager {
	secretSvc := secretservice.NewSecretService(c)
	clientSvc := talosclientservice.NewTalosClientService()
	return &TalosManager{
		Client:          c,
		statusManager:   statusManager,
		secretService:   secretSvc,
		configService:   talosconfigservice.NewTalosConfigService(),
		clientService:   clientSvc,
		machineService:  machineservice.NewMachineService(c),
		endpointService: endpointservice.NewEndpointService(c),
		stateService:    talosstateservice.NewTalosStateService(secretSvc),
		etcdService:     etcdservice.NewEtcdService(clientSvc),
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
func (t *TalosManager) ReconcileTalosCluster(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	err := initializeTalosCluster(ctx, t, cluster)
	// Always update status based on current persisted flags/conditions
	if sErr := t.statusManager.UpdateKubernetesClusterStatus(ctx, cluster); sErr != nil {
		vlog.Error("failed to update Kubernetes cluster status", sErr)
	}
	return err
}

// ensureTalosConfiguration generates or loads Talos configuration for the cluster
func (t *TalosManager) ensureTalosConfiguration(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, prep *clusterInitPrep) (*clientconfig.Config, error) {
	// Prefer existing artifacts from Secret; generate only if missing
	clientConfig, fromSecret, err := t.loadTalosArtifacts(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to load talos artifacts: %w", err)
	}

	if fromSecret {
		vlog.Info("Loaded Talos artifacts from Secret: cluster=" + cluster.Name)
		return clientConfig, nil
	}

	// Generate Talos configuration using the new bundle approach (like talosctl)
	configBundle, err := t.configService.GenerateTalosConfigBundle(cluster, prep.endpointIPs, prep.tenantPatches, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to generate Talos config bundle: %w", err)
	}

	cpYAML, err := t.configService.SerializeControlPlaneConfig(configBundle.Bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize control plane config: %w", err)
	}

	wYAML, err := t.configService.SerializeWorkerConfig(configBundle.Bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize worker config: %w", err)
	}

	// Serialize the secrets bundle for persistence
	secretsBundleYAML, err := t.configService.SerializeSecretsBundle(configBundle.SecretsBundle)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize secrets bundle: %w", err)
	}

	vlog.Info(fmt.Sprintf("Generated Talos config bundle: cluster=%s hasConfig=%t", cluster.Name, configBundle.ClientConfig != nil))

	// Update status phase: config generated
	_ = t.statusManager.SetPhase(ctx, cluster, "ConfigGenerated")
	_ = t.statusManager.SetCondition(ctx, cluster, "ConfigGenerated", "True", "Generated", "Talos client and role configs generated")

	// Persist initial Talos configs into a Secret
	if err := t.upsertTalosClusterConfigSecretWithRoleYAML(ctx, cluster, configBundle.ClientConfig, cpYAML, wYAML, secretsBundleYAML, nil); err != nil {
		return nil, fmt.Errorf("failed to persist Talos config secret: %w", err)
	}

	return configBundle.ClientConfig, nil
}

// State management delegates - delegate to stateService

// ensureTalosSecretExists creates the consolidated Secret if it does not exist yet with default flags
func (t *TalosManager) ensureTalosSecretExists(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	return t.stateService.EnsureSecretExists(ctx, cluster)
}

// getTalosSecretState returns persisted state flags from the cluster's consolidated Secret
func (t *TalosManager) getTalosSecretState(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (bootstrapped bool, hasKubeconfig bool, err error) {
	return t.stateService.GetState(ctx, cluster)
}

// loadTalosArtifacts attempts to read talosconfig and role templates from the consolidated Secret
func (t *TalosManager) loadTalosArtifacts(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*clientconfig.Config, bool, error) {
	return t.stateService.LoadTalosArtifacts(ctx, cluster)
}

// GetTalosClientConfig returns the Talos client config for a cluster (public wrapper for loadTalosArtifacts)
func (t *TalosManager) GetTalosClientConfig(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*clientconfig.Config, error) {
	clientConfig, found, err := t.loadTalosArtifacts(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to load talos config: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("talos config not found for cluster %s", cluster.Name)
	}
	return clientConfig, nil
}

// GetClientService returns the Talos client service for upgrade operations
func (t *TalosManager) GetClientService() *talosclientservice.TalosClientService {
	return t.clientService
}

// GetStateService returns the Talos state service for version/upgrade state management
func (t *TalosManager) GetStateService() *talosstateservice.TalosStateService {
	return t.stateService
}

// talosSecretFlags is an alias to the state service's TalosSecretFlags type
type talosSecretFlags = talosstateservice.TalosSecretFlags

// getTalosSecretFlags reads boolean flags from the consolidated Secret
func (t *TalosManager) getTalosSecretFlags(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (talosSecretFlags, error) {
	return t.stateService.GetFlags(ctx, cluster)
}

// setSecretTimestamp sets a timestamp field in the consolidated Secret
func (t *TalosManager) setSecretTimestamp(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, key string) error {
	return t.stateService.SetTimestamp(ctx, cluster, key)
}

// setTalosSecretFlags sets provided boolean flags in the consolidated Secret
func (t *TalosManager) setTalosSecretFlags(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, updates map[string]bool) error {
	return t.stateService.SetFlags(ctx, cluster, updates)
}

// getConfiguredNodes returns the list of node names that have been configured
func (t *TalosManager) getConfiguredNodes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) ([]string, error) {
	return t.stateService.GetConfiguredNodes(ctx, cluster)
}

// isNodeConfigured checks if a node has already been configured
func (t *TalosManager) isNodeConfigured(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) bool {
	return t.stateService.IsNodeConfigured(ctx, cluster, nodeName)
}

// addConfiguredNode adds a node name to the list of configured nodes in the secret
func (t *TalosManager) addConfiguredNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	return t.stateService.AddConfiguredNode(ctx, cluster, nodeName)
}

// determineControlPlaneEndpoints determines the control plane endpoints based on the configured endpoint mode
func (t *TalosManager) determineControlPlaneEndpoints(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, controlPlaneIPs []string) ([]string, error) {
	return t.endpointService.DetermineControlPlaneEndpoints(ctx, cluster, controlPlaneIPs)
}

// loadTenantOverrides loads tenant-specific configuration overrides from a ConfigMap
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

// applyPerNodeConfiguration applies Talos configuration to a set of machines
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

		// Add endpoint IPs to node config
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

// applyConfigToMachineGroup applies configuration to a group of machines (control planes or workers)
func (t *TalosManager) applyConfigToMachineGroup(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	machines []*vitistackv1alpha1.Machine,
	insecure bool,
	tenantOverrides map[string]any,
	endpointIP string,
	grpCfg *nodeGroupConfig,
) error {
	if len(machines) > 0 {
		vlog.Info(fmt.Sprintf("Stage %d: Applying config to %ss: count=%d cluster=%s", grpCfg.stageNum, grpCfg.nodeType, len(machines), cluster.Name))
		_ = t.statusManager.SetPhase(ctx, cluster, grpCfg.phase)
		_ = t.statusManager.SetCondition(ctx, cluster, grpCfg.conditionName, "False", "Applying", fmt.Sprintf("Applying config to %ss", grpCfg.nodeType))

		for _, m := range machines {
			if err := t.applyConfigToSingleMachine(ctx, cluster, clientConfig, m, insecure, tenantOverrides, endpointIP, grpCfg); err != nil {
				if grpCfg.stopOnError {
					return err
				}
			}
		}
		vlog.Info(fmt.Sprintf("Stage %d complete: Config applied to all %ss", grpCfg.stageNum, grpCfg.nodeType))
	} else {
		vlog.Info(fmt.Sprintf("Stage %d: No %ss to configure", grpCfg.stageNum, grpCfg.nodeType))
	}

	if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{grpCfg.flagName: true}); err != nil {
		vlog.Error(fmt.Sprintf("Failed to set %s flag in secret", grpCfg.flagName), err)
		return fmt.Errorf("failed to persist %s flag: %w", grpCfg.flagName, err)
	}
	_ = t.statusManager.SetCondition(ctx, cluster, grpCfg.conditionName, "True", "Applied", fmt.Sprintf("Talos config applied to all %ss", grpCfg.nodeType))
	return nil
}

// applyConfigToSingleMachine applies configuration to a single machine and handles post-config steps.
// Returns nil if the machine was already configured or successfully configured, error otherwise.
func (t *TalosManager) applyConfigToSingleMachine(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	m *vitistackv1alpha1.Machine,
	insecure bool,
	tenantOverrides map[string]any,
	endpointIP string,
	grpCfg *nodeGroupConfig,
) error {
	if t.isNodeConfigured(ctx, cluster, m.Name) {
		vlog.Info(fmt.Sprintf("%s already configured, skipping: node=%s", capitalizeFirst(grpCfg.nodeType), m.Name))
		return nil
	}

	nodeIP := getFirstIPv4(m)
	if nodeIP == "" {
		vlog.Warn(fmt.Sprintf("No IPv4 address found for %s %s, skipping", grpCfg.nodeType, m.Name))
		return nil
	}

	vlog.Info(fmt.Sprintf("Applying config to %s: node=%s ip=%s", grpCfg.nodeType, m.Name, nodeIP))
	if err := t.applyPerNodeConfiguration(ctx, cluster, clientConfig, []*vitistackv1alpha1.Machine{m}, insecure, tenantOverrides, endpointIP); err != nil {
		_ = t.statusManager.SetCondition(ctx, cluster, grpCfg.conditionName, "False", "ApplyError", fmt.Sprintf("Failed on node %s: %s", m.Name, err.Error()))
		vlog.Error(fmt.Sprintf("Failed to apply config to %s %s", grpCfg.nodeType, m.Name), err)
		return fmt.Errorf("failed to apply config to %s %s: %w", grpCfg.nodeType, m.Name, err)
	}

	t.handlePostConfigSteps(ctx, cluster, m, nodeIP, insecure, grpCfg)
	return nil
}

// handlePostConfigSteps handles waiting for reboot and marking the machine as configured.
func (t *TalosManager) handlePostConfigSteps(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	m *vitistackv1alpha1.Machine,
	nodeIP string,
	insecure bool,
	grpCfg *nodeGroupConfig,
) {
	if grpCfg.waitForReboot && insecure {
		vlog.Info(fmt.Sprintf("Waiting for %s to reboot after config apply: node=%s ip=%s", grpCfg.nodeType, m.Name, nodeIP))
		if err := t.clientService.WaitForNodeRebootAfterApply(nodeIP, 10*secondDuration, 5*minuteDuration, 10*secondDuration); err != nil {
			vlog.Warn(fmt.Sprintf("Warning: timeout waiting for %s %s to reboot, continuing anyway: %v", grpCfg.nodeType, m.Name, err))
		}
	}

	if err := t.addConfiguredNode(ctx, cluster, m.Name); err != nil {
		vlog.Error(fmt.Sprintf("Failed to add %s %s to configured nodes", grpCfg.nodeType, m.Name), err)
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
			vlog.Warn(fmt.Sprintf("Failed to configure new control plane %s", node.Name), err)
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
			vlog.Warn(fmt.Sprintf("Failed to configure new worker %s", node.Name), err)
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

	// Wait for Talos API to be reachable before attempting to apply config
	// New nodes take time to boot and have the maintenance API available
	if !t.clientService.IsTalosAPIReachable(ip) {
		vlog.Info(fmt.Sprintf("Talos API not yet reachable on new %s, will retry: node=%s ip=%s", nodeType, node.Name, ip))
		return fmt.Errorf("talos API not yet reachable on %s, node still booting", ip)
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
