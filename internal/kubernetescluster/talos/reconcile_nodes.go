package talos

import (
	"context"
	"fmt"
	"strings"
	"time"

	semver "github.com/Masterminds/semver/v3"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	"github.com/vitistack/common/pkg/operator/conditions"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// dependentVIPRequeueDelay is how long to wait before re-checking whether the
// cluster's ControlPlaneVirtualSharedIP has reached its Ready condition.
// NAM backend operations typically take 10–20s so this keeps us from
// reconciling faster than the upstream can respond.
const dependentVIPRequeueDelay = 15 * time.Second

// reconcileNewNodes handles adding new nodes to an existing cluster
// This is called when the cluster is already initialized but new machines have been added
func (t *TalosManager) reconcileNewNodes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	// Check for nodes that were marked as configured but never actually joined the cluster.
	// If they are still in Talos maintenance mode, remove them from the configured list
	// so they get reconfigured on this pass.
	if err := t.reconcileFailedNodes(ctx, cluster); err != nil {
		vlog.Warn(fmt.Sprintf("Error during failed node reconciliation %s: %v", clusterLogTag(cluster), err))
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
			vlog.Warn(fmt.Sprintf("VIP %s not found %s, skipping pool member update", vipName, clusterLogTag(cluster)))
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
		vlog.Warn(fmt.Sprintf("Failed to get configured nodes %s: %v", clusterLogTag(cluster), err))
		configuredNodes = nil
	}

	var newMachines []*vitistackv1alpha1.Machine
	for _, m := range machines {
		storedUID, ok := configuredNodes[m.Name]
		if !ok {
			newMachines = append(newMachines, m)
			continue
		}
		// Stored UID empty = legacy entry, treat as still configured.
		// Stored UID non-empty and != current = Machine was deleted and
		// recreated with the same name; the new Machine needs Talos config
		// applied before kubevirt-operator strips its boot ISO.
		if storedUID != "" && storedUID != m.UID {
			vlog.Info(fmt.Sprintf("Machine %s reused configured name with new UID (stored=%s current=%s); will reconfigure", m.Name, storedUID, m.UID))
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

	for nodeName := range configuredNodes {
		m, exists := machineMap[nodeName]
		if !exists {
			continue // Machine CRD was deleted; handled by reconcileRemovedNodes
		}

		ip := getFirstIPv4(m)
		if ip == "" {
			continue
		}

		if t.clientService.IsNodeInMaintenanceMode(ip) {
			vlog.Warn(fmt.Sprintf("Node %s/%s is marked as configured but still in maintenance mode, removing from configured list to retry configuration", clusterLogTag(cluster), nodeName))
			if err := t.stateService.RemoveConfiguredNode(ctx, cluster, nodeName); err != nil {
				vlog.Error(fmt.Sprintf("Failed to remove node %s/%s from configured list: %v", clusterLogTag(cluster), nodeName, err), err)
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
		// For 'networkconfiguration' mode, get from VIP. The VIP is driven
		// to Ready asynchronously by the nms-operator (NAM takes 10–20s),
		// so requeue instead of erroring when it's not ready yet.
		vipName := cluster.Spec.Cluster.ClusterId
		vip := &vitistackv1alpha1.ControlPlaneVirtualSharedIP{}
		if err := t.Get(ctx, types.NamespacedName{Name: vipName, Namespace: cluster.Namespace}, vip); err != nil {
			return "", fmt.Errorf("failed to get VIP for new node configuration: %w", err)
		}
		t.mirrorVIPReadyToKC(ctx, cluster, vip)
		if cond, ok := conditions.Get(vip.Status.Conditions, "Ready"); ok {
			if cond.Status == metav1.ConditionFalse && cond.Reason == "Error" {
				vlog.Warn(fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s is in terminal failure state, spec change required: %s", vip.Namespace, vip.Name, cond.Message))
				return "", fmt.Errorf("ControlPlaneVirtualSharedIP %s/%s is in a terminal failure state: %s", vip.Namespace, vip.Name, cond.Message)
			}
			if cond.Status != metav1.ConditionTrue {
				vlog.Info(fmt.Sprintf("Waiting for ControlPlaneVirtualSharedIP %s/%s to reach Ready before configuring new nodes (phase=%s)", vip.Namespace, vip.Name, vip.Status.Phase))
				return "", talosclientservice.NewRequeueError(
					fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s not ready yet (phase=%s)", vip.Namespace, vip.Name, vip.Status.Phase),
					dependentVIPRequeueDelay,
				)
			}
		} else {
			vlog.Info(fmt.Sprintf("Waiting for ControlPlaneVirtualSharedIP %s/%s to publish a Ready condition", vip.Namespace, vip.Name))
			return "", talosclientservice.NewRequeueError(
				fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s has no Ready condition yet", vip.Namespace, vip.Name),
				dependentVIPRequeueDelay,
			)
		}
		if len(vip.Status.LoadBalancerIps) == 0 {
			vlog.Warn(fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s is Ready but has no LoadBalancerIps yet, waiting", vip.Namespace, vip.Name))
			return "", talosclientservice.NewRequeueError(
				fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s is Ready but has no LoadBalancerIps yet", vip.Namespace, vip.Name),
				dependentVIPRequeueDelay,
			)
		}
		return vip.Status.LoadBalancerIps[0], nil
	}
}

// mirrorVIPReadyToKC copies the VIP's Ready condition onto the
// KubernetesCluster as ControlPlaneVirtualSharedIPReady so the dependency
// state is visible via kubectl describe kubernetescluster.
func (t *TalosManager) mirrorVIPReadyToKC(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, vip *vitistackv1alpha1.ControlPlaneVirtualSharedIP) {
	if t.statusManager == nil || cluster == nil || vip == nil {
		return
	}
	display := fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s", vip.Namespace, vip.Name)
	cond, ok := conditions.Get(vip.Status.Conditions, "Ready")
	if !ok {
		_ = t.statusManager.SetCondition(ctx, cluster, "ControlPlaneVirtualSharedIPReady", "Unknown", "NoCondition",
			fmt.Sprintf("%s has not published a Ready condition yet", display))
		return
	}
	msg := cond.Message
	if msg == "" {
		msg = fmt.Sprintf("%s Ready=%s", display, cond.Status)
	}
	reason := cond.Reason
	if reason == "" {
		reason = "Unknown"
	}
	_ = t.statusManager.SetCondition(ctx, cluster, "ControlPlaneVirtualSharedIPReady", string(cond.Status), reason, msg)
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

	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get cluster machines: %w", err)
	}
	machineMap := make(map[string]*vitistackv1alpha1.Machine, len(machines))
	for _, m := range machines {
		machineMap[m.Name] = m
	}

	desiredSemver, err := semver.NewVersion(desiredVersion)
	if err != nil {
		return fmt.Errorf("failed to parse desired Kubernetes version %q: %w", desiredVersion, err)
	}

	candidates := collectNodeUpgradeCandidates(nodeList.Items, machineMap, desiredSemver, desiredVersion)
	if len(candidates) == 0 {
		return nil
	}

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

	nodesToUpgrade := t.filterNodesNeedingUpgrade(ctx, tClient, candidates, desiredVersion)
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

// collectNodeUpgradeCandidates returns the nodes whose kubelet version is
// older than the desired version. Nodes at or above desiredVersion are skipped
// (downgrades are not supported). Nodes without a matching Machine or without
// a resolvable IPv4 address are also skipped.
func collectNodeUpgradeCandidates(
	nodes []corev1.Node,
	machineMap map[string]*vitistackv1alpha1.Machine,
	desiredSemver *semver.Version,
	desiredVersion string,
) []talosclientservice.NodeUpgradeInfo {
	var candidates []talosclientservice.NodeUpgradeInfo
	for i := range nodes {
		node := &nodes[i]
		nodeVersion := node.Status.NodeInfo.KubeletVersion
		if nodeVersion == desiredVersion {
			continue
		}

		nodeSemver, err := semver.NewVersion(nodeVersion)
		if err != nil {
			vlog.Warn(fmt.Sprintf("Cannot parse node %s Kubernetes version %q, skipping", node.Name, nodeVersion))
			continue
		}

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

		candidates = append(candidates, talosclientservice.NodeUpgradeInfo{
			Name:           node.Name,
			IP:             nodeIP,
			IsControlPlane: m.Labels[vitistackv1alpha1.NodeRoleAnnotation] == controlPlaneRole,
		})
	}
	return candidates
}

// filterNodesNeedingUpgrade drops candidates whose Talos machine config already
// carries the desired kubelet image — those just need the kubelet to restart.
func (t *TalosManager) filterNodesNeedingUpgrade(
	ctx context.Context,
	tClient *talosclient.Client,
	candidates []talosclientservice.NodeUpgradeInfo,
	desiredVersion string,
) []talosclientservice.NodeUpgradeInfo {
	expectedKubeletImage := fmt.Sprintf("ghcr.io/siderolabs/kubelet:%s", desiredVersion)
	var needUpgrade []talosclientservice.NodeUpgradeInfo
	for _, c := range candidates {
		if t.clientService.NodeHasKubeletImage(ctx, tClient, c.IP, expectedKubeletImage) {
			continue
		}
		vlog.Info(fmt.Sprintf("Node %s needs Kubernetes upgrade to %s", c.Name, desiredVersion))
		needUpgrade = append(needUpgrade, c)
	}
	return needUpgrade
}
