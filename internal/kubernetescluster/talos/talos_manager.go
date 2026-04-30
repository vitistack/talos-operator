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
	"github.com/vitistack/talos-operator/internal/services/talosversion"
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
		endpointService: endpointservice.NewEndpointService(c, statusManager),
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
	configBundle, err := t.configService.GenerateTalosConfigBundle(cluster, prep.endpointIPs, prep.tenantPatches, prep.controlPlaneTenantPatches, nil)
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

// EnsureNetworkNamespaceReady delegates to the endpoint service. Exposed so the
// KubernetesCluster reconciler can gate Machine creation on the upstream
// NetworkNamespace being Ready, without reaching into endpointService internals.
func (t *TalosManager) EnsureNetworkNamespaceReady(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	return t.endpointService.EnsureNetworkNamespaceReady(ctx, cluster)
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

// getConfiguredNodes returns the configured-node set as name -> Machine UID.
// Legacy entries (written before UID tracking was added) carry an empty UID.
func (t *TalosManager) getConfiguredNodes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (map[string]types.UID, error) {
	return t.stateService.GetConfiguredNodes(ctx, cluster)
}

// isMachineConfigured checks whether the given Machine is in the configured
// set AND its UID matches what was stored when it was configured. A new
// Machine that reuses the name of a deleted one (different UID) returns
// false here so it gets re-configured before kubevirt-operator clears its
// boot ISO.
func (t *TalosManager) isMachineConfigured(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, m *vitistackv1alpha1.Machine) bool {
	return t.stateService.IsNodeConfigured(ctx, cluster, m.Name, m.UID)
}

// addConfiguredMachine upserts (Machine name, Machine UID) into the configured
// set. Replacing the stored UID is what makes the recreated-Machine path
// recover automatically.
func (t *TalosManager) addConfiguredMachine(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, m *vitistackv1alpha1.Machine) error {
	return t.stateService.AddConfiguredNode(ctx, cluster, m.Name, m.UID)
}

// determineControlPlaneEndpoints determines the control plane endpoints based on the configured endpoint mode
func (t *TalosManager) determineControlPlaneEndpoints(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, controlPlaneIPs []string) ([]string, error) {
	return t.endpointService.DetermineControlPlaneEndpoints(ctx, cluster, controlPlaneIPs)
}

// buildVIPPatchIfNeeded returns a Layer2VIPConfig (or legacy VIP) patch string when
// ENDPOINT_MODE=talosvip and a VIP IP has been allocated. Returns "" otherwise.
func (t *TalosManager) buildVIPPatchIfNeeded(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) string {
	endpointMode := consts.EndpointMode(viper.GetString(consts.ENDPOINT_MODE))
	if endpointMode != consts.EndpointModeTalosVIP {
		return ""
	}

	vipIP := t.endpointService.GetAllocatedVIPIP(ctx, cluster)
	if vipIP == "" {
		vlog.Warn(fmt.Sprintf("talosvip mode but no VIP IP allocated yet for cluster %s", cluster.Name))
		return ""
	}

	link := strings.TrimSpace(viper.GetString(consts.TALOS_VIP_LINK))
	if link == "" {
		link = "net0"
	}

	adapter := talosversion.GetCurrentTalosVersionAdapter()
	patch := adapter.BuildVIPPatch(vipIP, link)
	vlog.Info(fmt.Sprintf("Built VIP patch for cluster %s: vipIP=%s link=%s (adapter=%s)", cluster.Name, vipIP, link, adapter.Version()))
	return patch
}

// buildLinkAliasPatchIfNeeded returns a LinkAliasConfig patch when ENDPOINT_MODE=talosvip
// and the Talos version supports multi-doc config (v1.12+). The LinkAliasConfig maps the
// VIP link name (e.g. "net0") to a physical interface via a MAC-based CEL selector.
// The #MACADDRESS# placeholder is replaced per-node in PrepareNodeConfig.
// Returns "" when VIP mode is disabled or the adapter doesn't support LinkAliasConfig.
func (t *TalosManager) buildLinkAliasPatchIfNeeded() string {
	endpointMode := consts.EndpointMode(viper.GetString(consts.ENDPOINT_MODE))
	if endpointMode != consts.EndpointModeTalosVIP {
		return ""
	}

	link := strings.TrimSpace(viper.GetString(consts.TALOS_VIP_LINK))
	if link == "" {
		link = "net0"
	}

	adapter := talosversion.GetCurrentTalosVersionAdapter()
	patch := adapter.BuildLinkAliasConfigPatch(link)
	if patch != "" {
		vlog.Info(fmt.Sprintf("Built LinkAliasConfig patch for VIP link: link=%s (adapter=%s)", link, adapter.Version()))
	}
	return patch
}

// loadTenantOverrides loads tenant-specific configuration overrides from a ConfigMap.
// It returns:
//   - overrides: the first YAML document parsed as a map (for backward compatibility)
//   - patches: the full raw YAML string (including additional config documents like
//     LinkAliasConfig, LinkConfig) as a slice for configpatcher.LoadPatches.
//
// Multi-document YAML (documents separated by ---) is supported since Talos v1.12.
// Additional documents beyond the first (e.g., LinkAliasConfig, LinkConfig) are
// preserved in the patches return value so that configpatcher can apply them properly.
func (t *TalosManager) loadTenantOverrides(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (map[string]any, []string, error) {
	name := strings.TrimSpace(viper.GetString(consts.TENANT_CONFIGMAP_NAME))
	if name == "" {
		return nil, nil, nil
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
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to read tenant overrides ConfigMap %s/%s: %w", namespace, name, err)
	}

	raw, ok := cm.Data[dataKey]
	if !ok || strings.TrimSpace(raw) == "" {
		vlog.Warn(fmt.Sprintf("Tenant overrides ConfigMap %s/%s missing data key %s, skipping overrides", namespace, name, dataKey))
		return nil, nil, nil
	}

	replaced := strings.ReplaceAll(raw, "#CLUSTERID#", cluster.Spec.Cluster.ClusterId)

	// Parse the first YAML document as a map for backward compatibility.
	// yaml.Unmarshal only parses the first document in a multi-document YAML string.
	overrides := map[string]any{}
	if err := yaml.Unmarshal([]byte(replaced), &overrides); err != nil {
		return nil, nil, fmt.Errorf("failed to parse tenant overrides from ConfigMap %s/%s key %s: %w", namespace, name, dataKey, err)
	}

	if len(overrides) == 0 {
		return nil, nil, nil
	}

	// Validate the full YAML (including multi-document sections) before using it.
	if err := talosconfigservice.ValidateTenantConfigYAML(raw); err != nil {
		return nil, nil, fmt.Errorf("tenant config ConfigMap %s/%s key %s has invalid YAML: %w", namespace, name, dataKey, err)
	}

	// Return the full raw YAML (including any additional config documents separated
	// by ---) as a single patch string. configpatcher.LoadPatches → configloader.NewFromBytes
	// uses yaml.NewDecoder to iterate multi-document YAML internally.
	patches := []string{replaced}

	vlog.Info(fmt.Sprintf("Tenant overrides loaded from ConfigMap %s/%s (%d bytes, %d lines)",
		namespace, name, len(replaced), strings.Count(replaced, "\n")+1))

	return overrides, patches, nil
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

	// Resolve the Talos install image once for this batch. Pinning the image at
	// cluster-create time (and refreshing on upgrade) makes new nodes added
	// later install the same Talos version as existing nodes, instead of
	// drifting to whatever TALOS_VM_INSTALL_IMAGE_* the operator currently has.
	// Order: secret -> fetch from running cluster -> env var.
	var resolvedInstallImage string
	if len(machines) > 0 {
		resolvedInstallImage = t.resolveClusterInstallImage(ctx, cluster, machines[0], clientConfig)
	}

	for _, m := range machines {
		if err := t.applySingleNodeConfig(ctx, cluster, clientConfig, m, insecure, tenantOverrides, endpointIP, cpTemplate, wTemplate, resolvedInstallImage); err != nil {
			return err
		}
	}

	t.persistInstallImageIfMissing(ctx, cluster, resolvedInstallImage)
	return nil
}

// applySingleNodeConfig applies Talos configuration to a single machine.
// Skips silently when the machine is missing networking info; returns an error
// only for fatal problems (missing role template, config-apply failure).
func (t *TalosManager) applySingleNodeConfig(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	m *vitistackv1alpha1.Machine,
	insecure bool,
	tenantOverrides map[string]any,
	endpointIP string,
	cpTemplate, wTemplate []byte,
	installImage string,
) error {
	if len(m.Status.NetworkInterfaces) == 0 || len(m.Status.PublicIPAddresses) == 0 {
		return nil
	}

	ip := firstIPv4From(m.Status.PublicIPAddresses)
	if ip == "" {
		return nil
	}

	roleYAML := cpTemplate
	if m.Labels[vitistackv1alpha1.NodeRoleAnnotation] != controlPlaneRole {
		roleYAML = wTemplate
	}
	if len(roleYAML) == 0 {
		return fmt.Errorf("missing role template for node %s (role=%s) in secret %s", m.Name, m.Labels[vitistackv1alpha1.NodeRoleAnnotation], secretservice.GetSecretName(cluster))
	}

	installDisk := t.configService.SelectInstallDisk(m)
	// Primary source: NetworkConfiguration CRD (authoritative, created by the VM operator).
	// Fallback: Machine.Status.NetworkInterfaces (populated from the running VM).
	macAddress := t.getMachineMAC(ctx, m)

	var staticIPPatches []string
	if staticCfg := t.getStaticIPConfig(ctx, cluster, m); staticCfg != nil {
		staticIPPatches = []string{
			talosconfigservice.BuildStaticNetworkPatch(staticCfg),
			talosconfigservice.BuildStaticIPKernelArgPatch(staticCfg),
		}
		vlog.Info(fmt.Sprintf("Static IP config for node %s: ip=%s gw=%s iface=%s",
			m.Name, staticCfg.IP, staticCfg.Gateway, staticCfg.Interface))
	}

	nodeConfig, err := t.configService.PrepareNodeConfig(cluster, roleYAML, installDisk, m, tenantOverrides, macAddress, installImage, staticIPPatches...)
	if err != nil {
		return fmt.Errorf("failed to prepare config for node %s: %w", m.Name, err)
	}

	nodeConfig, err = t.configService.PatchNodeConfigWithEndpointIPs(nodeConfig, endpointIP)
	if err != nil {
		return fmt.Errorf("failed to patch config for node %s with endpoint IPs: %w", m.Name, err)
	}

	if err := t.clientService.ApplyConfigToNode(ctx, insecure, clientConfig, ip, nodeConfig); err != nil {
		return fmt.Errorf("failed to apply config to node %s: %w", ip, err)
	}
	return nil
}

// firstIPv4From returns the first parseable IPv4 address from the slice, or "".
func firstIPv4From(addrs []string) string {
	for _, ipAddr := range addrs {
		parsed := net.ParseIP(ipAddr)
		if parsed != nil && parsed.To4() != nil {
			return ipAddr
		}
	}
	return ""
}

// persistInstallImageIfMissing writes the resolved install image to the cluster
// secret only when it is non-empty and not already saved. Pinning happens once
// per cluster lifetime (and gets refreshed by the upgrade flow).
func (t *TalosManager) persistInstallImageIfMissing(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, image string) {
	if image == "" {
		return
	}
	saved, _ := t.stateService.GetInstallImage(ctx, cluster)
	if saved != "" {
		return
	}
	if err := t.stateService.SetInstallImage(ctx, cluster, image); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to persist install image for cluster %s: %v", cluster.Name, err))
	}
}

// resolveClusterInstallImage returns the Talos install image to bake into
// machine.install.image for nodes in this cluster. Resolution order:
//  1. Saved value in the cluster secret (pinned at cluster create or last upgrade).
//  2. Fetched from a running cluster node (legacy clusters that predate the
//     install_image secret field).
//  3. Operator env var fallback (TALOS_VM_INSTALL_IMAGE_*) — used for the very
//     first apply during initial cluster creation.
//
// Returns "" for non-VM providers and bare metal where no install image patch
// should be applied.
func (t *TalosManager) resolveClusterInstallImage(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	m *vitistackv1alpha1.Machine,
	clientConfig *clientconfig.Config,
) string {
	if saved, err := t.stateService.GetInstallImage(ctx, cluster); err == nil && saved != "" {
		return saved
	}

	if clientConfig != nil {
		if image := t.fetchInstallImageFromCluster(ctx, cluster, clientConfig); image != "" {
			vlog.Info(fmt.Sprintf("Resolved install image from running cluster %s: %s", cluster.Name, image))
			return image
		}
	}

	if m != nil && m.Status.Provider != "" && t.configService.IsVirtualMachineProvider(m.Status.Provider.String()) {
		return t.configService.GetVMInstallImage(m.Status.Provider)
	}
	return ""
}

// fetchInstallImageFromCluster queries the active machine config of any
// reachable control plane node and returns its machine.install.image value.
// Returns "" if no node is reachable or the field is unset.
func (t *TalosManager) fetchInstallImageFromCluster(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
) string {
	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return ""
	}
	controlPlanes := t.machineService.FilterMachinesByRole(machines, controlPlaneRole)
	controlPlaneIPs := extractIPv4Addresses(controlPlanes)
	if len(controlPlaneIPs) == 0 {
		return ""
	}

	tClient, err := t.clientService.CreateTalosClient(ctx, false, clientConfig, controlPlaneIPs)
	if err != nil {
		return ""
	}
	defer func() { _ = tClient.Close() }()

	for _, ip := range controlPlaneIPs {
		image, err := t.clientService.GetMachineInstallImage(ctx, tClient, ip)
		if err == nil && image != "" {
			return image
		}
	}
	return ""
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
	if t.isMachineConfigured(ctx, cluster, m) {
		vlog.Info(fmt.Sprintf("%s already configured, skipping: node=%s", capitalizeFirst(grpCfg.nodeType), m.Name))
		t.markMachineOSInstalled(ctx, m)
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

	if err := t.addConfiguredMachine(ctx, cluster, m); err != nil {
		vlog.Error(fmt.Sprintf("Failed to add %s %s to configured nodes", grpCfg.nodeType, m.Name), err)
	}

	t.markMachineOSInstalled(ctx, m)
}

// backfillOSInstalledAnnotations stamps OSInstalledAnnotation on every Machine
// that's already in the cluster's configured_nodes set. The cluster-init
// short-circuit bypasses the per-stage apply paths for fully-initialized
// clusters, so without this pass legacy clusters would never get the
// annotation and kubevirt-operator would never clean up their boot ISOs.
//
// The UID-aware check in isMachineConfigured guards against the recreation
// race: when a Machine is deleted and recreated with the same name, the new
// Machine has a different UID and is treated as unconfigured here, so we do
// NOT stamp os-installed on a fresh empty VM that hasn't actually had Talos
// applied yet.
func (t *TalosManager) backfillOSInstalledAnnotations(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) {
	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		vlog.Warn(fmt.Sprintf("Failed to list machines for os-installed backfill on cluster %s: %v", cluster.Name, err))
		return
	}
	for _, m := range machines {
		if !t.isMachineConfigured(ctx, cluster, m) {
			continue
		}
		t.markMachineOSInstalled(ctx, m)
	}
}

// markMachineOSInstalled patches the Machine CR with OSInstalledAnnotation so
// downstream providers (e.g. kubevirt-operator) can release the boot ISO. The
// patch is best-effort: transient errors are logged and a subsequent reconcile
// will retry. The annotation is generic and safe to set for all boot sources —
// providers that don't allocate an ISO ignore it.
func (t *TalosManager) markMachineOSInstalled(ctx context.Context, m *vitistackv1alpha1.Machine) {
	const annotationValueTrue = "true"

	if m == nil || m.Annotations[consts.OSInstalledAnnotation] == annotationValueTrue {
		return
	}

	fresh := &vitistackv1alpha1.Machine{}
	if err := t.Get(ctx, types.NamespacedName{Name: m.Name, Namespace: m.Namespace}, fresh); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to fetch Machine %s to mark os-installed: %v", m.Name, err))
		return
	}
	if fresh.Annotations[consts.OSInstalledAnnotation] == annotationValueTrue {
		return
	}

	patch := client.MergeFrom(fresh.DeepCopy())
	if fresh.Annotations == nil {
		fresh.Annotations = make(map[string]string)
	}
	fresh.Annotations[consts.OSInstalledAnnotation] = annotationValueTrue
	if err := t.Patch(ctx, fresh, patch); err != nil {
		if apierrors.IsConflict(err) {
			return
		}
		vlog.Warn(fmt.Sprintf("Failed to set os-installed annotation on Machine %s: %v", m.Name, err))
		return
	}
	vlog.Info(fmt.Sprintf("Marked Machine as OS installed: node=%s", m.Name))
}

// configureNewControlPlanes configures new control plane nodes
func (t *TalosManager) configureNewControlPlanes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, configCtx *newNodeConfigContext, nodes []*vitistackv1alpha1.Machine) error {
	if len(nodes) == 0 {
		return nil
	}

	vlog.Info(fmt.Sprintf("Configuring %d new control planes for cluster %s", len(nodes), cluster.Name))
	_ = t.statusManager.SetMessage(ctx, cluster, fmt.Sprintf("Configuring %d new control plane(s)", len(nodes)))
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
	_ = t.statusManager.SetMessage(ctx, cluster, fmt.Sprintf("Configuring %d new worker(s)", len(nodes)))
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
	_ = t.statusManager.SetMessage(ctx, cluster, fmt.Sprintf("Applying config to %s %s", nodeType, node.Name))
	// Use insecure mode for new nodes - they don't have the cluster CA yet
	if err := t.applyPerNodeConfiguration(ctx, cluster, configCtx.clientConfig, []*vitistackv1alpha1.Machine{node}, true, configCtx.tenantOverrides, configCtx.endpointIP); err != nil {
		return fmt.Errorf("failed to apply config: %w", err)
	}
	if err := t.addConfiguredMachine(ctx, cluster, node); err != nil {
		vlog.Error(fmt.Sprintf("Failed to add new %s %s to configured nodes", nodeType, node.Name), err)
	}

	t.markMachineOSInstalled(ctx, node)

	vlog.Info(fmt.Sprintf("New %s configured successfully: node=%s", nodeType, node.Name))
	return nil
}

// getMachineMAC resolves the DHCP-reserved MAC address for a machine.
// It first checks the NetworkConfiguration CRD (authoritative source, created by the VM operator
// with the MAC reserved in Kea DHCP). If unavailable, it falls back to Machine.Status.NetworkInterfaces.
func (t *TalosManager) getMachineMAC(ctx context.Context, m *vitistackv1alpha1.Machine) string {
	// Try NetworkConfiguration CRD first (same name/namespace as the Machine)
	nc := &vitistackv1alpha1.NetworkConfiguration{}
	if err := t.Get(ctx, types.NamespacedName{Name: m.Name, Namespace: m.Namespace}, nc); err == nil {
		for i := range nc.Spec.NetworkInterfaces {
			if nc.Spec.NetworkInterfaces[i].MacAddress != "" {
				return nc.Spec.NetworkInterfaces[i].MacAddress
			}
		}
	}

	// Fallback to Machine status
	for _, iface := range m.Status.NetworkInterfaces {
		if iface.MACAddress != "" {
			return iface.MACAddress
		}
	}
	return ""
}

// getStaticIPConfig checks if the cluster's NetworkNamespace uses static-ip-operator
// and returns the static IP configuration from the machine's NetworkConfiguration status.
// Returns nil if static IP is not applicable.
func (t *TalosManager) getStaticIPConfig(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, m *vitistackv1alpha1.Machine) *talosconfigservice.StaticIPConfig {
	// Look up the NetworkNamespace
	nnName := cluster.Spec.Cluster.NetworkNamespaceName
	if nnName == "" {
		return nil
	}
	nn := &vitistackv1alpha1.NetworkNamespace{}
	if err := t.Get(ctx, types.NamespacedName{Name: nnName, Namespace: cluster.Namespace}, nn); err != nil {
		return nil
	}

	// Only apply for static-ip-operator
	if nn.Spec.IPAllocation == nil ||
		nn.Spec.IPAllocation.Type != vitistackv1alpha1.IPAllocationTypeStatic ||
		!vitistackv1alpha1.MatchesProvider(nn.Spec.IPAllocation.Provider, vitistackv1alpha1.ProviderNameStaticIP) {
		return nil
	}

	// Read IP config from the machine's NetworkConfiguration status
	nc := &vitistackv1alpha1.NetworkConfiguration{}
	if err := t.Get(ctx, types.NamespacedName{Name: m.Name, Namespace: m.Namespace}, nc); err != nil {
		return nil
	}

	for i := range nc.Status.NetworkInterfaces {
		iface := &nc.Status.NetworkInterfaces[i]
		if iface.IPAllocated && len(iface.IPv4Addresses) > 0 {
			// Determine interface name
			ifaceName := "eth0"
			if viper.GetBool(consts.TALOS_PREDICTABLE_NETWORK_NAMES) {
				ifaceName = "enp1s0" // default virtio interface name
			}

			return &talosconfigservice.StaticIPConfig{
				IP:        iface.IPv4Addresses[0],
				CIDR:      iface.IPv4Subnet,
				Gateway:   iface.IPv4Gateway,
				DNS:       iface.DNS,
				Interface: ifaceName,
			}
		}
	}
	return nil
}
