package upgradeservice

import (
	"context"
	"fmt"
	"strings"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	"github.com/vitistack/talos-operator/internal/services/talosstateservice"
)

// isConnectionError checks if an error is due to connection issues (node rebooting/unreachable)
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// Common connection error patterns during node reboot
	return strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection error") ||
		strings.Contains(errStr, "Unavailable") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "graceful_stop") ||
		strings.Contains(errStr, "transport") ||
		strings.Contains(errStr, "i/o timeout") ||
		strings.Contains(errStr, "context deadline exceeded")
}

// isUpgradeLockedError checks if an error indicates an upgrade is already in progress
func isUpgradeLockedError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "locked")
}

// UpgradeOrchestrator handles the actual execution of Talos and Kubernetes upgrades
type UpgradeOrchestrator struct {
	upgradeService *UpgradeService
	clientService  *talosclientservice.TalosClientService
}

// NewUpgradeOrchestrator creates a new UpgradeOrchestrator
func NewUpgradeOrchestrator(upgradeService *UpgradeService, clientService *talosclientservice.TalosClientService) *UpgradeOrchestrator {
	return &UpgradeOrchestrator{
		upgradeService: upgradeService,
		clientService:  clientService,
	}
}

// NodeUpgradeInfo contains information about a node to be upgraded
type NodeUpgradeInfo struct {
	Name     string
	IP       string
	Role     string // "control-plane" or "worker"
	Machine  *vitistackv1alpha1.Machine
	Upgraded bool
	Failed   bool // Node previously failed upgrade
	Skipped  bool // Node should be skipped
}

// UpgradeResult contains the result of an upgrade operation
type UpgradeResult struct {
	Success       bool
	NodesUpgraded int
	TotalNodes    int
	FailedNode    string
	SkippedNodes  []string // Nodes that were skipped (e.g., previously failed)
	Error         error
	NeedsRequeue  bool
	RequeueAfter  time.Duration
}

// upgradeState holds the state during a rolling upgrade
type upgradeState struct {
	nodesUpgraded      int
	skippedNodes       []string
	totalNodes         int
	lastUpgradedWorker *NodeUpgradeInfo
}

// PerformTalosUpgrade executes a rolling Talos upgrade across all nodes.
// It upgrades workers first, then control planes (one at a time).
// Returns UpgradeResult indicating progress and whether a requeue is needed.
func (o *UpgradeOrchestrator) PerformTalosUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	nodes []NodeUpgradeInfo,
	targetVersion string,
	talosInstallerImage string,
) *UpgradeResult {
	totalNodes := len(nodes)
	if totalNodes == 0 {
		return &UpgradeResult{
			Success: false,
			Error:   fmt.Errorf("no nodes found for upgrade"),
		}
	}

	// Sort nodes: workers first, then control planes
	orderedNodes := o.buildOrderedNodeList(nodes, targetVersion)

	state := &upgradeState{
		totalNodes: totalNodes,
	}

	// Process each node
	for i, node := range orderedNodes {
		result := o.processNodeForUpgrade(ctx, cluster, clientConfig, node, i, state, talosInstallerImage)
		if result != nil {
			return result
		}
	}

	return o.buildCompletionResult(cluster, state)
}

// buildOrderedNodeList sorts nodes with workers first, then control planes
func (o *UpgradeOrchestrator) buildOrderedNodeList(nodes []NodeUpgradeInfo, targetVersion string) []NodeUpgradeInfo {
	workers, controlPlanes := o.sortNodesByRole(nodes)
	orderedNodes := make([]NodeUpgradeInfo, 0, len(workers)+len(controlPlanes))
	orderedNodes = append(orderedNodes, workers...)
	orderedNodes = append(orderedNodes, controlPlanes...)

	vlog.Info(fmt.Sprintf("Starting Talos upgrade: target=%s workers=%d controlPlanes=%d",
		targetVersion, len(workers), len(controlPlanes)))

	return orderedNodes
}

// processNodeForUpgrade handles upgrade logic for a single node
// Returns an UpgradeResult if the loop should exit, nil to continue
func (o *UpgradeOrchestrator) processNodeForUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	node NodeUpgradeInfo,
	nodeIndex int,
	state *upgradeState,
	talosInstallerImage string,
) *UpgradeResult {
	// Handle already upgraded nodes
	if node.Upgraded {
		state.nodesUpgraded++
		if node.Role == "worker" {
			nodeCopy := node
			state.lastUpgradedWorker = &nodeCopy
		}
		return nil
	}

	// Handle skipped nodes
	if node.Skipped {
		state.skippedNodes = append(state.skippedNodes, node.Name)
		vlog.Info(fmt.Sprintf("Skipping node %s as requested", node.Name))
		return nil
	}

	// Handle failed nodes
	if node.Failed {
		state.skippedNodes = append(state.skippedNodes, node.Name)
		vlog.Warn(fmt.Sprintf("Skipping previously failed node %s (use retry-failed-nodes annotation to retry)", node.Name))
		return nil
	}

	// Check worker readiness before proceeding
	if result := o.checkWorkerReadiness(ctx, clientConfig, node, state); result != nil {
		return result
	}

	// Perform the upgrade
	return o.executeNodeUpgrade(ctx, cluster, clientConfig, node, nodeIndex, state, talosInstallerImage)
}

// checkWorkerReadiness verifies the last upgraded worker is ready before upgrading the next
func (o *UpgradeOrchestrator) checkWorkerReadiness(
	ctx context.Context,
	clientConfig *clientconfig.Config,
	node NodeUpgradeInfo,
	state *upgradeState,
) *UpgradeResult {
	if node.Role != "worker" || state.lastUpgradedWorker == nil {
		return nil
	}

	ready, err := o.clientService.IsKubernetesNodeReady(ctx, clientConfig, state.lastUpgradedWorker.IP, state.lastUpgradedWorker.Name)
	if err != nil {
		vlog.Warn(fmt.Sprintf("Failed to check Kubernetes readiness for node %s: %v", state.lastUpgradedWorker.Name, err))
		return nil // Continue anyway after warning
	}

	if !ready {
		vlog.Info(fmt.Sprintf("Waiting for worker node %s to be Kubernetes-ready before upgrading next worker %s",
			state.lastUpgradedWorker.Name, node.Name))
		return &UpgradeResult{
			Success:       true,
			NodesUpgraded: state.nodesUpgraded,
			TotalNodes:    state.totalNodes,
			SkippedNodes:  state.skippedNodes,
			NeedsRequeue:  true,
			RequeueAfter:  15 * time.Second,
		}
	}

	vlog.Info(fmt.Sprintf("Worker node %s is Kubernetes-ready, proceeding with next worker upgrade", state.lastUpgradedWorker.Name))
	return nil
}

// executeNodeUpgrade performs the actual upgrade on a node
func (o *UpgradeOrchestrator) executeNodeUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	node NodeUpgradeInfo,
	nodeIndex int,
	state *upgradeState,
	talosInstallerImage string,
) *UpgradeResult {
	vlog.Info(fmt.Sprintf("Upgrading Talos on node: cluster=%s node=%s ip=%s role=%s (%d/%d)",
		cluster.Name, node.Name, node.IP, node.Role, nodeIndex+1, state.totalNodes))

	// Update progress annotation
	if err := o.upgradeService.UpdateTalosUpgradeProgress(ctx, cluster, node.Name, state.nodesUpgraded, state.totalNodes); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to update progress: %v", err))
	}

	// Check if node was already marked as having upgrade initiated
	alreadyInitiated := false
	if o.upgradeService.stateService != nil {
		alreadyInitiated = o.upgradeService.stateService.IsNodeUpgraded(ctx, cluster, node.Name)
	}

	// Perform the actual upgrade
	if err := o.upgradeTalosNode(ctx, clientConfig, node.IP, talosInstallerImage); err != nil {
		// If upgrade returns "locked", it means upgrade is already in progress on this node
		// This is NOT an error - the node is actively upgrading, we should wait
		if isUpgradeLockedError(err) {
			vlog.Info(fmt.Sprintf("Node %s upgrade is locked (already in progress), will requeue to check status", node.Name))
			// Mark as initiated so we track it
			if o.upgradeService.stateService != nil {
				_ = o.upgradeService.stateService.MarkNodeUpgraded(ctx, cluster, node.Name)
			}
			return &UpgradeResult{
				Success:       true,
				NodesUpgraded: state.nodesUpgraded,
				TotalNodes:    state.totalNodes,
				SkippedNodes:  state.skippedNodes,
				NeedsRequeue:  true,
				RequeueAfter:  15 * time.Second,
			}
		}

		// If upgrade was already initiated and we get a connection error, node is likely rebooting
		if alreadyInitiated && isConnectionError(err) {
			vlog.Info(fmt.Sprintf("Node %s upgrade already initiated, connection error indicates reboot in progress", node.Name))
			return &UpgradeResult{
				Success:       true,
				NodesUpgraded: state.nodesUpgraded,
				TotalNodes:    state.totalNodes,
				SkippedNodes:  state.skippedNodes,
				NeedsRequeue:  true,
				RequeueAfter:  15 * time.Second,
			}
		}
		vlog.Error(fmt.Sprintf("Failed to upgrade Talos on node %s: %v", node.Name, err), err)
		return &UpgradeResult{
			Success:       false,
			NodesUpgraded: state.nodesUpgraded,
			TotalNodes:    state.totalNodes,
			FailedNode:    node.Name,
			SkippedNodes:  state.skippedNodes,
			Error:         err,
		}
	}

	// Mark node as upgraded in the state service
	if o.upgradeService.stateService != nil {
		if err := o.upgradeService.stateService.MarkNodeUpgraded(ctx, cluster, node.Name); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to mark node %s as upgraded in state: %v", node.Name, err))
		}
	}

	state.nodesUpgraded++
	vlog.Info(fmt.Sprintf("Talos upgrade initiated on node: cluster=%s node=%s (%d/%d complete)",
		cluster.Name, node.Name, state.nodesUpgraded, state.totalNodes))

	return &UpgradeResult{
		Success:       true,
		NodesUpgraded: state.nodesUpgraded,
		TotalNodes:    state.totalNodes,
		SkippedNodes:  state.skippedNodes,
		NeedsRequeue:  true,
		RequeueAfter:  30 * time.Second,
	}
}

// buildCompletionResult creates the result when all nodes have been processed
func (o *UpgradeOrchestrator) buildCompletionResult(cluster *vitistackv1alpha1.KubernetesCluster, state *upgradeState) *UpgradeResult {
	if len(state.skippedNodes) > 0 {
		vlog.Warn(fmt.Sprintf("Upgrade completed with skipped nodes: cluster=%s upgraded=%d skipped=%d nodes=%v",
			cluster.Name, state.nodesUpgraded, len(state.skippedNodes), state.skippedNodes))
	} else {
		vlog.Info(fmt.Sprintf("All nodes upgraded: cluster=%s nodes=%d", cluster.Name, state.nodesUpgraded))
	}
	return &UpgradeResult{
		Success:       true,
		NodesUpgraded: state.nodesUpgraded,
		TotalNodes:    state.totalNodes,
		SkippedNodes:  state.skippedNodes,
		NeedsRequeue:  false,
	}
}

// upgradeTalosNode performs the actual Talos upgrade on a single node
func (o *UpgradeOrchestrator) upgradeTalosNode(
	ctx context.Context,
	clientConfig *clientconfig.Config,
	nodeIP string,
	installerImage string,
) error {
	// Create Talos client for this node
	client, err := o.clientService.CreateTalosClient(ctx, false, clientConfig, []string{nodeIP})
	if err != nil {
		return fmt.Errorf("failed to create Talos client: %w", err)
	}
	defer func() { _ = client.Close() }()

	// Call the Talos upgrade API
	if err := o.clientService.UpgradeNode(ctx, client, nodeIP, installerImage); err != nil {
		return fmt.Errorf("upgrade API call failed: %w", err)
	}

	return nil
}

// PerformKubernetesUpgrade executes a Kubernetes version upgrade on all nodes
func (o *UpgradeOrchestrator) PerformKubernetesUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	nodes []talosclientservice.NodeUpgradeInfo,
	targetVersion string,
) *UpgradeResult {
	vlog.Info(fmt.Sprintf("Starting Kubernetes upgrade: cluster=%s target=%s nodes=%d",
		cluster.Name, targetVersion, len(nodes)))

	if len(nodes) == 0 {
		return &UpgradeResult{
			Success: false,
			Error:   fmt.Errorf("no nodes provided for Kubernetes upgrade"),
		}
	}

	// Get first control plane IP for client connection
	var controlPlaneIP string
	for _, node := range nodes {
		if node.IsControlPlane {
			controlPlaneIP = node.IP
			break
		}
	}
	if controlPlaneIP == "" {
		return &UpgradeResult{
			Success: false,
			Error:   fmt.Errorf("no control plane node found for Kubernetes upgrade"),
		}
	}

	// Create Talos client
	client, err := o.clientService.CreateTalosClient(ctx, false, clientConfig, []string{controlPlaneIP})
	if err != nil {
		return &UpgradeResult{
			Success: false,
			Error:   fmt.Errorf("failed to create Talos client: %w", err),
		}
	}
	defer func() { _ = client.Close() }()

	// Perform Kubernetes upgrade on all nodes
	if err := o.clientService.UpgradeKubernetes(ctx, client, nodes, targetVersion); err != nil {
		return &UpgradeResult{
			Success: false,
			Error:   fmt.Errorf("kubernetes upgrade failed: %w", err),
		}
	}

	vlog.Info(fmt.Sprintf("Kubernetes upgrade completed: cluster=%s version=%s", cluster.Name, targetVersion))
	return &UpgradeResult{
		Success:      true,
		NeedsRequeue: false,
	}
}

// sortNodesByRole separates nodes into workers and control planes
func (o *UpgradeOrchestrator) sortNodesByRole(nodes []NodeUpgradeInfo) (workers, controlPlanes []NodeUpgradeInfo) {
	for _, node := range nodes {
		if node.Role == "control-plane" {
			controlPlanes = append(controlPlanes, node)
		} else {
			workers = append(workers, node)
		}
	}
	return workers, controlPlanes
}

// CheckNodeUpgradeStatus checks if a node has completed its upgrade by verifying the Talos version
func (o *UpgradeOrchestrator) CheckNodeUpgradeStatus(
	ctx context.Context,
	clientConfig *clientconfig.Config,
	nodeIP string,
	expectedVersion string,
) (bool, string, error) {
	client, err := o.clientService.CreateTalosClient(ctx, false, clientConfig, []string{nodeIP})
	if err != nil {
		return false, "", fmt.Errorf("failed to create client: %w", err)
	}
	defer func() { _ = client.Close() }()

	currentVersion, err := o.clientService.GetTalosVersion(ctx, client, nodeIP)
	if err != nil {
		return false, "", fmt.Errorf("failed to get version: %w", err)
	}

	// Normalize versions for comparison (handle with/without 'v' prefix)
	normalizedCurrent := strings.TrimPrefix(currentVersion, "v")
	normalizedExpected := strings.TrimPrefix(expectedVersion, "v")

	return normalizedCurrent == normalizedExpected, currentVersion, nil
}

// WaitForNodeReady waits for a node to be ready after upgrade
func (o *UpgradeOrchestrator) WaitForNodeReady(
	ctx context.Context,
	clientConfig *clientconfig.Config,
	nodeIP string,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// Check if Talos API is reachable
		if o.clientService.IsTalosAPIReachable(nodeIP) {
			// Give it a moment to fully stabilize
			time.Sleep(5 * time.Second)
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
			// Continue checking
		}
	}

	return fmt.Errorf("timeout waiting for node %s to be ready", nodeIP)
}

// PreflightChecks performs comprehensive health checks before starting an upgrade
// and persists the result to the cluster secret for tracking.
func (o *UpgradeOrchestrator) PreflightChecks(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	controlPlaneIP string,
	allNodeIPs []string,
) (*talosstateservice.HealthCheckState, error) {
	healthState := &talosstateservice.HealthCheckState{
		Passed:            false,
		EtcdHealthy:       false,
		NodesReady:        false,
		ControlPlaneReady: false,
	}

	var issues []string

	// 1. Check Talos API reachability on control plane
	vlog.Info(fmt.Sprintf("Pre-upgrade health check: checking Talos API reachability on %s", controlPlaneIP))
	if !o.clientService.IsTalosAPIReachable(controlPlaneIP) {
		issues = append(issues, fmt.Sprintf("Talos API not reachable on control plane %s", controlPlaneIP))
	} else {
		healthState.ControlPlaneReady = true
	}

	// 2. Check etcd health
	vlog.Info("Pre-upgrade health check: checking etcd cluster health")
	client, err := o.clientService.CreateTalosClient(ctx, false, clientConfig, []string{controlPlaneIP})
	if err != nil {
		issues = append(issues, fmt.Sprintf("Failed to create Talos client: %v", err))
	} else {
		defer func() { _ = client.Close() }()

		healthy, etcdErr := o.clientService.IsEtcdHealthy(ctx, client, controlPlaneIP)
		switch {
		case etcdErr != nil:
			issues = append(issues, fmt.Sprintf("Failed to check etcd health: %v", etcdErr))
		case !healthy:
			issues = append(issues, "etcd cluster is not healthy")
		default:
			healthState.EtcdHealthy = true
			vlog.Info("Pre-upgrade health check: etcd cluster is healthy")
		}
	}

	// 3. Check all nodes are reachable
	vlog.Info(fmt.Sprintf("Pre-upgrade health check: checking %d nodes are reachable", len(allNodeIPs)))
	allNodesReachable := true
	unreachableNodes := []string{}
	for _, nodeIP := range allNodeIPs {
		if !o.clientService.IsTalosAPIReachable(nodeIP) {
			allNodesReachable = false
			unreachableNodes = append(unreachableNodes, nodeIP)
		}
	}
	if !allNodesReachable {
		issues = append(issues, fmt.Sprintf("Talos API not reachable on nodes: %v", unreachableNodes))
	} else {
		healthState.NodesReady = true
		vlog.Info(fmt.Sprintf("Pre-upgrade health check: all %d nodes are reachable", len(allNodeIPs)))
	}

	// Build result message
	if len(issues) > 0 {
		healthState.Message = fmt.Sprintf("Health check failed: %s", strings.Join(issues, "; "))
		healthState.Passed = false
		vlog.Error(fmt.Sprintf("Pre-upgrade health check FAILED: cluster=%s issues=%v", cluster.Name, issues), nil)
	} else {
		healthState.Message = "All health checks passed"
		healthState.Passed = true
		vlog.Info(fmt.Sprintf("Pre-upgrade health check PASSED: cluster=%s etcd=%v nodes=%v cp=%v",
			cluster.Name, healthState.EtcdHealthy, healthState.NodesReady, healthState.ControlPlaneReady))
	}

	// Persist health state to secret (if state service available)
	if o.upgradeService != nil && o.upgradeService.stateService != nil {
		if err := o.upgradeService.stateService.SetHealthCheckState(ctx, cluster, healthState); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to persist health check state: %v", err))
		}
	}

	if !healthState.Passed {
		return healthState, fmt.Errorf("pre-upgrade health check failed: %s", healthState.Message)
	}

	return healthState, nil
}
