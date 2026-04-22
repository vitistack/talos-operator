package talos

import (
	"context"
	"fmt"
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// reconcileNewNodes handles adding new nodes to an existing cluster
// This is called when the cluster is already initialized but new machines have been added
func (t *TalosManager) reconcileNewNodes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	// Check for nodes that were marked as configured but never actually joined the cluster.
	// If they are still in Talos maintenance mode, remove them from the configured list
	// so they get reconfigured on this pass.
	if err := t.reconcileFailedNodes(ctx, cluster); err != nil {
		vlog.Warn(fmt.Sprintf("Error during failed node reconciliation: %v", err))
	}

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

// reconcileFailedNodes checks nodes that are marked as configured but are still in
// Talos maintenance mode, meaning the configuration was never successfully applied
// or the node was reset. These nodes are removed from the configured list so they
// get picked up by findUnconfiguredMachines and reconfigured on the next pass.
func (t *TalosManager) reconcileFailedNodes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	configuredNodes, err := t.getConfiguredNodes(ctx, cluster)
	if err != nil || len(configuredNodes) == 0 {
		return err
	}

	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get cluster machines: %w", err)
	}

	// Build a lookup of machine name -> Machine
	machineMap := make(map[string]*vitistackv1alpha1.Machine)
	for _, m := range machines {
		machineMap[m.Name] = m
	}

	for _, nodeName := range configuredNodes {
		m, exists := machineMap[nodeName]
		if !exists {
			continue // Machine CRD was deleted; handled by reconcileRemovedNodes
		}

		ip := getFirstIPv4(m)
		if ip == "" {
			continue
		}

		if t.clientService.IsNodeInMaintenanceMode(ip) {
			vlog.Warn(fmt.Sprintf("Node %s is marked as configured but still in maintenance mode, removing from configured list to retry configuration", nodeName))
			if err := t.stateService.RemoveConfiguredNode(ctx, cluster, nodeName); err != nil {
				vlog.Error(fmt.Sprintf("Failed to remove node %s from configured list: %v", nodeName, err), err)
			}
		}
	}

	return nil
}

// loadNewNodeConfigContext loads configuration context needed for adding new nodes
func (t *TalosManager) loadNewNodeConfigContext(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*newNodeConfigContext, error) {
	clientConfig, _, err := t.loadTalosArtifacts(ctx, cluster)
	if err != nil || clientConfig == nil {
		return nil, fmt.Errorf("failed to load talos artifacts for new node configuration: %w", err)
	}

	tenantOverrides, _, err := t.loadTenantOverrides(ctx, cluster)
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

// reconcileNodeVersions checks all nodes in the workload cluster and upgrades any
// that are running a different Kubernetes version than what the cluster spec requires.
// This handles the case where a node joins with a stale config template that has an
// older Kubernetes version baked in (e.g., after a cluster-wide upgrade).
func (t *TalosManager) reconcileNodeVersions(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	desiredVersion := cluster.Spec.Topology.Version
	if desiredVersion == "" {
		return nil // No explicit version set, nothing to enforce
	}

	// Normalize to "v1.35.0" format for comparison with node.Status.NodeInfo.KubeletVersion
	desiredVersion = consts.EnsureVersionPrefix(desiredVersion)

	clientset, err := t.getWorkloadClusterClient(ctx, cluster)
	if err != nil || clientset == nil {
		return err
	}

	nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list workload cluster nodes: %w", err)
	}

	// Get all machines for this cluster once
	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get cluster machines: %w", err)
	}
	machineMap := make(map[string]*vitistackv1alpha1.Machine)
	for _, m := range machines {
		machineMap[m.Name] = m
	}

	// Parse desired version for comparison
	desiredSemver, err := semver.NewVersion(desiredVersion)
	if err != nil {
		return fmt.Errorf("failed to parse desired Kubernetes version %q: %w", desiredVersion, err)
	}

	// Collect nodes whose version is older than the desired version (upgrades only, no downgrades)
	type mismatchedNode struct {
		info talosclientservice.NodeUpgradeInfo
		node string
	}
	var candidates []mismatchedNode
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		nodeVersion := node.Status.NodeInfo.KubeletVersion
		if nodeVersion == desiredVersion {
			continue
		}

		nodeSemver, err := semver.NewVersion(nodeVersion)
		if err != nil {
			vlog.Warn(fmt.Sprintf("Cannot parse node %s Kubernetes version %q, skipping", node.Name, nodeVersion))
			continue
		}

		// Only upgrade — skip if node is already at or above desired version
		if !nodeSemver.LessThan(desiredSemver) {
			if nodeSemver.GreaterThan(desiredSemver) {
				vlog.Warn(fmt.Sprintf("Node %s is running Kubernetes %s which is newer than desired %s — downgrade is not supported, skipping",
					node.Name, nodeVersion, desiredVersion))
			}
			continue
		}

		m, exists := machineMap[node.Name]
		if !exists {
			continue
		}

		nodeIP := getFirstIPv4(m)
		if nodeIP == "" {
			continue
		}

		candidates = append(candidates, mismatchedNode{
			info: talosclientservice.NodeUpgradeInfo{
				Name:           node.Name,
				IP:             nodeIP,
				IsControlPlane: m.Labels[vitistackv1alpha1.NodeRoleAnnotation] == controlPlaneRole,
			},
			node: node.Name,
		})
	}

	if len(candidates) == 0 {
		return nil
	}

	// Load Talos client config to check machine configs before upgrading
	clientConfig, err := t.GetTalosClientConfig(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to load talos client config: %w", err)
	}

	controlPlanes := t.machineService.FilterMachinesByRole(machines, controlPlaneRole)
	controlPlaneIPs := extractIPv4Addresses(controlPlanes)
	if len(controlPlaneIPs) == 0 {
		return fmt.Errorf("no control plane IPs found for version reconciliation")
	}

	tClient, err := t.clientService.CreateTalosClient(ctx, false, clientConfig, controlPlaneIPs)
	if err != nil {
		return fmt.Errorf("failed to create Talos client for version reconciliation: %w", err)
	}
	defer func() { _ = tClient.Close() }()

	// Filter out nodes whose machine config already has the target kubelet version.
	// Those nodes just need time for the kubelet to restart — no need to re-apply.
	expectedKubeletImage := fmt.Sprintf("ghcr.io/siderolabs/kubelet:%s", desiredVersion)
	var nodesToUpgrade []talosclientservice.NodeUpgradeInfo
	for _, c := range candidates {
		if t.clientService.NodeHasKubeletImage(ctx, tClient, c.info.IP, expectedKubeletImage) {
			continue // Config already correct, kubelet restart pending
		}
		vlog.Info(fmt.Sprintf("Node %s needs Kubernetes upgrade to %s", c.node, desiredVersion))
		nodesToUpgrade = append(nodesToUpgrade, c.info)
	}

	if len(nodesToUpgrade) == 0 {
		return nil
	}

	// Strip the "v" prefix for the upgrade API (it adds it back internally)
	targetVersion := consts.NormalizeKubernetesVersion(desiredVersion)

	if err := t.clientService.UpgradeKubernetes(ctx, tClient, nodesToUpgrade, targetVersion); err != nil {
		return fmt.Errorf("failed to upgrade node Kubernetes versions: %w", err)
	}

	vlog.Info(fmt.Sprintf("Kubernetes version reconciliation complete: %d nodes upgraded to %s",
		len(nodesToUpgrade), desiredVersion))
	return nil
}
