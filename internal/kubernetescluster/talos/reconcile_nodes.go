package talos

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/pkg/consts"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

		if t.clientService.IsNodeInMaintenanceMode(ctx, ip) {
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
