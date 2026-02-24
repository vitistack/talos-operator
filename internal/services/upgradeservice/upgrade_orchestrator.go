package upgradeservice

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	"github.com/vitistack/talos-operator/internal/services/talosstateservice"
)

// Node role constants
const (
	roleWorker       = "worker"
	roleControlPlane = "control-plane"
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

// NodeDrainer interface for cordon/drain operations
type NodeDrainer interface {
	CordonAndDrainNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error
	UncordonNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error
	IsNodeSchedulable(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) (bool, error)
}

// UpgradeOrchestrator handles the actual execution of Talos and Kubernetes upgrades
type UpgradeOrchestrator struct {
	upgradeService *UpgradeService
	clientService  *talosclientservice.TalosClientService
	nodeDrainer    NodeDrainer
}

// NewUpgradeOrchestrator creates a new UpgradeOrchestrator
func NewUpgradeOrchestrator(upgradeService *UpgradeService, clientService *talosclientservice.TalosClientService, nodeDrainer NodeDrainer) *UpgradeOrchestrator {
	return &UpgradeOrchestrator{
		upgradeService: upgradeService,
		clientService:  clientService,
		nodeDrainer:    nodeDrainer,
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
	nodesUpgraded            int
	skippedNodes             []string
	totalNodes               int
	lastUpgradedWorker       *NodeUpgradeInfo
	lastUpgradedControlPlane *NodeUpgradeInfo
}

// PerformTalosUpgrade executes a rolling Talos upgrade across all nodes.
// It upgrades ALL workers first, waits for them to be ready, then upgrades control planes.
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

	// Separate workers and control planes
	workers, controlPlanes := o.sortNodesByRole(nodes)

	state := &upgradeState{
		totalNodes: totalNodes,
	}

	// Check if there's a current upgrading node from previous reconciliation
	var currentUpgradingNode string
	if o.upgradeService.stateService != nil {
		if node, err := o.upgradeService.stateService.GetCurrentUpgradingNode(ctx, cluster); err == nil {
			currentUpgradingNode = node
		}
	}

	// If there's a current upgrading node, ONLY process that node
	if currentUpgradingNode != "" {
		vlog.Info(fmt.Sprintf("Resuming upgrade of current node: %s", currentUpgradingNode))
		return o.processCurrentUpgradingNode(ctx, cluster, clientConfig, currentUpgradingNode, controlPlanes, workers, state, targetVersion)
	}

	// Phase 1: Upgrade control planes first (one at a time)
	for i, node := range controlPlanes {
		result := o.processNodeForUpgrade(ctx, cluster, clientConfig, node, i, state, targetVersion, talosInstallerImage)
		if result != nil {
			return result
		}
	}

	// Phase 2: Before starting workers, verify ALL control planes are fully ready
	if len(workers) > 0 && len(controlPlanes) > 0 {
		if result := o.ensureAllControlPlanesReady(ctx, cluster, clientConfig, controlPlanes, state); result != nil {
			return result
		}
	}

	// Phase 3: Upgrade workers (one at a time)
	for i, node := range workers {
		result := o.processNodeForUpgrade(ctx, cluster, clientConfig, node, len(controlPlanes)+i, state, targetVersion, talosInstallerImage)
		if result != nil {
			return result
		}
	}

	return o.buildCompletionResult(ctx, cluster, state)
}

// processCurrentUpgradingNode processes the node that is currently being upgraded
// This is called when resuming an upgrade from a previous reconciliation
func (o *UpgradeOrchestrator) processCurrentUpgradingNode(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	nodeName string,
	controlPlanes []NodeUpgradeInfo,
	workers []NodeUpgradeInfo,
	state *upgradeState,
	targetVersion string,
) *UpgradeResult {
	// Find the node in control planes or workers
	var node *NodeUpgradeInfo

	for _, cp := range controlPlanes {
		if cp.Name == nodeName {
			nodeCopy := cp
			node = &nodeCopy
			break
		}
	}

	if node == nil {
		for _, w := range workers {
			if w.Name == nodeName {
				nodeCopy := w
				node = &nodeCopy
				break
			}
		}
	}

	if node == nil {
		// Node not found - clear the current upgrading node and continue
		vlog.Warn(fmt.Sprintf("Current upgrading node %s not found in cluster nodes, clearing state", nodeName))
		if o.upgradeService.stateService != nil {
			_ = o.upgradeService.stateService.ClearCurrentUpgradingNode(ctx, cluster)
		}
		// Requeue to restart the upgrade process
		return o.requeueResult(state, 5*time.Second)
	}

	// Process this specific node
	vlog.Info(fmt.Sprintf("Processing current upgrading node: %s (role=%s)", nodeName, node.Role))
	result := o.handleAlreadyInitiatedNode(ctx, cluster, clientConfig, *node, targetVersion, state)

	// If the node is complete (result == nil), clear current upgrading node
	if result == nil {
		vlog.Info(fmt.Sprintf("Current upgrading node %s completed, clearing current node state", nodeName))
		if o.upgradeService.stateService != nil {
			if err := o.upgradeService.stateService.ClearCurrentUpgradingNode(ctx, cluster); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to clear current upgrading node: %v", err))
			}
		}
		// Requeue to continue with the next node
		return o.requeueResult(state, 5*time.Second)
	}

	return result
}

// settlingTimeAfterNodesReady is the time to wait after all nodes of one type are ready
// before starting upgrades on the next type, allowing pods to fully reschedule
const settlingTimeAfterNodesReady = 60 * time.Second

// ensureAllControlPlanesReady verifies ALL control planes are fully upgraded and ready before worker upgrades.
// Uses state service to track when all control planes became ready, and waits for settling time to pass.
func (o *UpgradeOrchestrator) ensureAllControlPlanesReady(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	controlPlanes []NodeUpgradeInfo,
	state *upgradeState,
) *UpgradeResult {
	vlog.Info("Checking that ALL control planes are ready before starting worker upgrades")

	// First, check if all control planes are ready
	if result := o.checkAllNodesReadyAndUncordon(ctx, cluster, clientConfig, controlPlanes, "control-plane", state); result != nil {
		return result
	}

	// All control planes are ready - check if we need to start or continue settling time
	return o.handleNodesSettlingTime(ctx, cluster, "control-plane", state)
}

// checkAllNodesReadyAndUncordon checks each node's readiness and uncordons them
func (o *UpgradeOrchestrator) checkAllNodesReadyAndUncordon(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	nodes []NodeUpgradeInfo,
	nodeType string, // "control-plane" or "worker"
	state *upgradeState,
) *UpgradeResult {
	readyNodes := make([]string, 0, len(nodes))
	notReadyNodes := make([]string, 0, len(nodes))

	for _, node := range nodes {
		if o.isNodeSkipped(node.Name, state.skippedNodes) {
			continue
		}

		if !o.isNodeUpgradeInitiated(ctx, cluster, node.Name) {
			vlog.Warn(fmt.Sprintf("%s %s upgrade not initiated, this is unexpected", nodeType, node.Name))
			notReadyNodes = append(notReadyNodes, node.Name)
			continue
		}

		// Check if node is Kubernetes-ready
		ready, err := o.clientService.IsKubernetesNodeReady(ctx, clientConfig, node.IP, node.Name)
		if err != nil {
			vlog.Warn(fmt.Sprintf("Cannot check readiness of %s %s: %v, waiting", nodeType, node.Name, err))
			return o.requeueResult(state, 30*time.Second)
		}

		if !ready {
			notReadyNodes = append(notReadyNodes, node.Name)
			vlog.Info(fmt.Sprintf("%s status: ready=[%s] notReady=[%s]",
				nodeType,
				strings.Join(readyNodes, ", "),
				strings.Join(notReadyNodes, ", ")))
			return o.requeueResult(state, 30*time.Second)
		}

		readyNodes = append(readyNodes, node.Name)

		// Ensure node is uncordoned - always attempt to uncordon ready nodes
		// to prevent nodes from being stuck in SchedulingDisabled state
		if o.nodeDrainer != nil {
			// First check if it needs uncordoning
			schedulable, schedErr := o.nodeDrainer.IsNodeSchedulable(ctx, cluster, node.Name)
			if schedErr != nil || !schedulable {
				if schedErr != nil {
					vlog.Info(fmt.Sprintf("Could not verify schedulable status for %s %s, attempting uncordon", nodeType, node.Name))
				}
				if err := o.nodeDrainer.UncordonNode(ctx, cluster, node.Name); err != nil {
					vlog.Warn(fmt.Sprintf("Failed to uncordon %s %s: %v", nodeType, node.Name, err))
				}
				// Don't log success on every loop - reduces spam
			}
		}
	}

	// Don't log "All nodes ready" on every reconciliation loop during settling time
	// It will be logged once when settling period starts

	return nil
}

// isNodeSkipped checks if a node was skipped during upgrade
func (o *UpgradeOrchestrator) isNodeSkipped(nodeName string, skippedNodes []string) bool {
	return slices.Contains(skippedNodes, nodeName)
}

// isNodeUpgradeInitiated checks if a node's upgrade was initiated
func (o *UpgradeOrchestrator) isNodeUpgradeInitiated(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) bool {
	if o.upgradeService.stateService != nil {
		return o.upgradeService.stateService.IsNodeUpgraded(ctx, cluster, nodeName)
	}
	return false
}

// handleNodesSettlingTime manages the settling time after all nodes of a type are ready
func (o *UpgradeOrchestrator) handleNodesSettlingTime(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	nodeType string, // "control-plane" or "worker"
	state *upgradeState,
) *UpgradeResult {
	if o.upgradeService.stateService == nil {
		return nil
	}

	var nodesReadyTime time.Time
	var err error
	var nextPhase string

	if nodeType == controlPlaneRole {
		nodesReadyTime, err = o.upgradeService.stateService.GetControlPlanesReadyTime(ctx, cluster)
		nextPhase = roleWorker
	} else {
		nodesReadyTime, err = o.upgradeService.stateService.GetWorkersReadyTime(ctx, cluster)
		nextPhase = "control plane"
	}

	if err != nil {
		vlog.Warn(fmt.Sprintf("Failed to get %s ready time: %v", nodeType, err))
	}

	if nodesReadyTime.IsZero() {
		// First time all nodes are ready - mark the time and start settling
		if nodeType == "control-plane" {
			if err := o.upgradeService.stateService.MarkControlPlanesReadyTime(ctx, cluster); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to mark control planes ready time: %v", err))
			}
		} else {
			if err := o.upgradeService.stateService.MarkWorkersReadyTime(ctx, cluster); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to mark workers ready time: %v", err))
			}
		}
		vlog.Info(fmt.Sprintf("All %ss ready, starting settling period of %v before %s upgrades", nodeType, settlingTimeAfterNodesReady, nextPhase))
		return o.requeueResult(state, settlingTimeAfterNodesReady)
	}

	// Check if settling time has passed
	elapsed := time.Since(nodesReadyTime)
	if elapsed < settlingTimeAfterNodesReady {
		remaining := settlingTimeAfterNodesReady - elapsed
		// Only log every 10 seconds to reduce log spam
		if int(elapsed.Seconds())%10 == 0 || elapsed < 2*time.Second {
			vlog.Info(fmt.Sprintf("%s settling time: %v elapsed, %v remaining before %s upgrades", nodeType, elapsed.Round(time.Second), remaining.Round(time.Second), nextPhase))
		}
		return o.requeueResult(state, remaining)
	}

	vlog.Info(fmt.Sprintf("%s settling time complete (%v elapsed), proceeding with %s upgrades", nodeType, elapsed.Round(time.Second), nextPhase))
	return nil
}

// requeueResult creates a requeue result with the given delay
func (o *UpgradeOrchestrator) requeueResult(state *upgradeState, delay time.Duration) *UpgradeResult {
	return &UpgradeResult{
		Success:       true,
		NodesUpgraded: state.nodesUpgraded,
		TotalNodes:    state.totalNodes,
		SkippedNodes:  state.skippedNodes,
		NeedsRequeue:  true,
		RequeueAfter:  delay,
	}
}

// handleAlreadyInitiatedNode processes a node whose upgrade was already initiated
// Returns an UpgradeResult if the loop should exit, nil if the node is complete and we should continue
func (o *UpgradeOrchestrator) handleAlreadyInitiatedNode(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	node NodeUpgradeInfo,
	targetVersion string,
	state *upgradeState,
) *UpgradeResult {
	// Check if node upgrade is complete (version matches target)
	upgraded, currentVersion, err := o.CheckNodeUpgradeStatus(ctx, clientConfig, node.IP, targetVersion)
	if err != nil {
		// Can't check version - node might be rebooting
		// Set this node as current upgrading node to ensure only it is processed
		if o.upgradeService.stateService != nil {
			if setErr := o.upgradeService.stateService.SetCurrentUpgradingNode(ctx, cluster, node.Name); setErr != nil {
				vlog.Warn(fmt.Sprintf("Failed to set current upgrading node: %v", setErr))
			}
		}
		vlog.Info(fmt.Sprintf("Waiting for node %s to complete upgrade (cannot verify version: %v)", node.Name, err))
		return o.requeueResult(state, 30*time.Second)
	}

	if upgraded {
		// Node upgrade complete - version matches target
		// Ensure node is uncordoned to prevent stuck SchedulingDisabled state
		if o.nodeDrainer != nil {
			schedulable, schedErr := o.nodeDrainer.IsNodeSchedulable(ctx, cluster, node.Name)
			if schedErr != nil || !schedulable {
				if schedErr != nil {
					vlog.Info(fmt.Sprintf("Could not verify schedulable status for node %s, attempting uncordon", node.Name))
				}
				if err := o.nodeDrainer.UncordonNode(ctx, cluster, node.Name); err != nil {
					vlog.Warn(fmt.Sprintf("Failed to uncordon node %s: %v", node.Name, err))
				}
			}
		}
		state.nodesUpgraded++

		// Update progress annotation
		if err := o.upgradeService.UpdateTalosUpgradeProgress(ctx, cluster, node.Name, state.nodesUpgraded, state.totalNodes); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to update progress for completed node %s: %v", node.Name, err))
		}

		switch node.Role {
		case roleWorker:
			nodeCopy := node
			state.lastUpgradedWorker = &nodeCopy
		case roleControlPlane:
			nodeCopy := node
			state.lastUpgradedControlPlane = &nodeCopy
		}

		vlog.Info(fmt.Sprintf("Node %s upgrade complete: version=%s", node.Name, currentVersion))
		// Continue to next node in the loop (this node is done)
		return nil
	}

	// Node upgrade initiated but version doesn't match yet - still rebooting/upgrading
	// STOP here and wait for it to complete before proceeding to next node
	// Set this node as current upgrading node to ensure only it is processed
	if o.upgradeService.stateService != nil {
		if setErr := o.upgradeService.stateService.SetCurrentUpgradingNode(ctx, cluster, node.Name); setErr != nil {
			vlog.Warn(fmt.Sprintf("Failed to set current upgrading node: %v", setErr))
		}
	}
	vlog.Info(fmt.Sprintf("Waiting for node %s to complete upgrade (version not yet updated)", node.Name))
	return o.requeueResult(state, 30*time.Second)
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
	targetVersion string,
	talosInstallerImage string,
) *UpgradeResult {
	// Check if this node's upgrade was already initiated (in state service)
	alreadyInitiated := false
	if o.upgradeService.stateService != nil {
		alreadyInitiated = o.upgradeService.stateService.IsNodeUpgraded(ctx, cluster, node.Name)
	}

	// Handle nodes where upgrade was already initiated
	if alreadyInitiated {
		return o.handleAlreadyInitiatedNode(ctx, cluster, clientConfig, node, targetVersion, state)
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

	// This is a new node to upgrade - check that previous nodes of same role are ready
	if result := o.checkPreviousNodesReady(ctx, cluster, clientConfig, node, state); result != nil {
		return result
	}

	// Perform the upgrade
	return o.executeNodeUpgrade(ctx, cluster, clientConfig, node, nodeIndex, state, talosInstallerImage)
}

// settlingTimeAfterNodeReady is the time to wait after a single node becomes ready
// before proceeding to the next upgrade, allowing pods to reschedule
const settlingTimeAfterNodeReady = 30 * time.Second

// checkPreviousNodesReady verifies the last upgraded node (worker or control plane) is ready
// before upgrading the next. Also waits for settling time after uncordon.
func (o *UpgradeOrchestrator) checkPreviousNodesReady(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	node NodeUpgradeInfo,
	state *upgradeState,
) *UpgradeResult {
	var lastUpgraded *NodeUpgradeInfo
	var nodeType string

	// Determine which previous node to check based on current node role
	if node.Role == roleWorker && state.lastUpgradedWorker != nil {
		lastUpgraded = state.lastUpgradedWorker
		nodeType = roleWorker
	} else if node.Role == roleControlPlane && state.lastUpgradedControlPlane != nil {
		lastUpgraded = state.lastUpgradedControlPlane
		nodeType = roleControlPlane
	}

	if lastUpgraded == nil {
		return nil
	}

	// Check if Kubernetes node is Ready
	ready, err := o.clientService.IsKubernetesNodeReady(ctx, clientConfig, lastUpgraded.IP, lastUpgraded.Name)
	if err != nil {
		vlog.Warn(fmt.Sprintf("Failed to check Kubernetes readiness for %s node %s: %v", nodeType, lastUpgraded.Name, err))
		// If we can't check readiness (e.g., API down), wait and retry
		return &UpgradeResult{
			Success:       true,
			NodesUpgraded: state.nodesUpgraded,
			TotalNodes:    state.totalNodes,
			SkippedNodes:  state.skippedNodes,
			NeedsRequeue:  true,
			RequeueAfter:  20 * time.Second,
		}
	}

	if !ready {
		// Don't log on every reconciliation loop - already logged "Waiting for node to complete upgrade"
		return &UpgradeResult{
			Success:       true,
			NodesUpgraded: state.nodesUpgraded,
			TotalNodes:    state.totalNodes,
			SkippedNodes:  state.skippedNodes,
			NeedsRequeue:  true,
			RequeueAfter:  20 * time.Second,
		}
	}

	// Check if Kubernetes node is schedulable (not cordoned) and uncordon if needed
	if o.nodeDrainer != nil {
		schedulable, schedErr := o.nodeDrainer.IsNodeSchedulable(ctx, cluster, lastUpgraded.Name)

		// If we can't check status or node is not schedulable, attempt to uncordon
		// This ensures we don't leave nodes cordoned due to check failures
		shouldUncordon := schedErr != nil || !schedulable

		if shouldUncordon {
			if schedErr != nil {
				vlog.Warn(fmt.Sprintf("Could not verify schedulable status for %s node %s (err: %v), attempting uncordon", nodeType, lastUpgraded.Name, schedErr))
			}

			if err := o.nodeDrainer.UncordonNode(ctx, cluster, lastUpgraded.Name); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to uncordon %s node %s: %v", nodeType, lastUpgraded.Name, err))
				// Don't block on uncordon failures - node is still ready
			} else {
				// Return a requeue to allow settling time for pods to reschedule to this node
				// Don't log on every loop - reduces spam
				return &UpgradeResult{
					Success:       true,
					NodesUpgraded: state.nodesUpgraded,
					TotalNodes:    state.totalNodes,
					SkippedNodes:  state.skippedNodes,
					NeedsRequeue:  true,
					RequeueAfter:  settlingTimeAfterNodeReady,
				}
			}
		}
	}

	vlog.Info(fmt.Sprintf("%s node %s is Kubernetes-ready and schedulable, proceeding with next %s upgrade", nodeType, lastUpgraded.Name, nodeType))
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

	// Set this node as the current upgrading node BEFORE starting the upgrade
	// This ensures only this node will be processed on subsequent reconciliations
	if o.upgradeService.stateService != nil {
		if err := o.upgradeService.stateService.SetCurrentUpgradingNode(ctx, cluster, node.Name); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to set current upgrading node: %v", err))
		}
	}

	// Update progress annotation
	if err := o.upgradeService.UpdateTalosUpgradeProgress(ctx, cluster, node.Name, state.nodesUpgraded, state.totalNodes); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to update progress: %v", err))
	}

	// Cordon and drain the node before upgrade
	if o.nodeDrainer != nil {
		vlog.Info(fmt.Sprintf("Cordoning and draining node %s before upgrade", node.Name))
		if err := o.nodeDrainer.CordonAndDrainNode(ctx, cluster, node.Name); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to cordon/drain node %s: %v (continuing with upgrade)", node.Name, err))
			// Don't fail the upgrade if drain fails - the node will reboot anyway
		}
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
				RequeueAfter:  30 * time.Second,
			}
		}

		// If we get a connection error, node may be rebooting already
		if isConnectionError(err) {
			vlog.Info(fmt.Sprintf("Node %s connection error during upgrade, may be rebooting", node.Name))
			// Mark as initiated anyway since upgrade may have started
			if o.upgradeService.stateService != nil {
				_ = o.upgradeService.stateService.MarkNodeUpgraded(ctx, cluster, node.Name)
			}
			return &UpgradeResult{
				Success:       true,
				NodesUpgraded: state.nodesUpgraded,
				TotalNodes:    state.totalNodes,
				SkippedNodes:  state.skippedNodes,
				NeedsRequeue:  true,
				RequeueAfter:  30 * time.Second,
			}
		}
		vlog.Error(fmt.Sprintf("Failed to upgrade Talos on node %s: %v", node.Name, err), err)
		// Clear the current upgrading node on failure so we don't get stuck
		if o.upgradeService.stateService != nil {
			_ = o.upgradeService.stateService.ClearCurrentUpgradingNode(ctx, cluster)
		}
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

	// Return immediately after initiating upgrade on ONE node
	// This ensures rolling upgrade - one node at a time
	// Note: current_node is NOT cleared here - it will be cleared when the node completes
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
func (o *UpgradeOrchestrator) buildCompletionResult(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, state *upgradeState) *UpgradeResult {
	// Clear the current upgrading node since all upgrades are complete
	if o.upgradeService.stateService != nil {
		if err := o.upgradeService.stateService.ClearCurrentUpgradingNode(ctx, cluster); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to clear current upgrading node on completion: %v", err))
		}
	}

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

	// Uncordon all nodes after successful Kubernetes upgrade
	// Nodes may have been cordoned during Talos upgrade
	if o.nodeDrainer != nil {
		vlog.Info(fmt.Sprintf("Uncordoning all nodes after Kubernetes upgrade: cluster=%s", cluster.Name))
		for _, node := range nodes {
			if node.Name == "" {
				vlog.Warn(fmt.Sprintf("Skipping uncordon for node with IP %s - no name available", node.IP))
				continue
			}
			if err := o.nodeDrainer.UncordonNode(ctx, cluster, node.Name); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to uncordon node %s after Kubernetes upgrade: %v", node.Name, err))
				// Don't fail the upgrade for uncordon errors - nodes are still upgraded
			}
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
		if node.Role == roleControlPlane {
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

// SyncMachineConfigsAfterUpgrade fetches the current machine configs from nodes
// and updates the stored templates in the cluster secret. This should be called
// after a successful Talos upgrade to ensure stored configs match the actual running configs.
func (o *UpgradeOrchestrator) SyncMachineConfigsAfterUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	controlPlaneIP string,
	workerIP string,
	talosVersion string,
) error {
	vlog.Info(fmt.Sprintf("Syncing machine configs after upgrade: cluster=%s version=%s", cluster.Name, talosVersion))

	// Fetch configs from nodes
	controlPlaneYAML, cpErr := o.fetchNodeConfig(ctx, clientConfig, controlPlaneIP, "control-plane")
	workerYAML, wErr := o.fetchNodeConfig(ctx, clientConfig, workerIP, "worker")

	// Update stored configs if we got any
	if controlPlaneYAML != nil || workerYAML != nil {
		if o.upgradeService != nil && o.upgradeService.stateService != nil {
			if err := o.updateStoredConfigs(ctx, cluster, controlPlaneYAML, workerYAML, talosVersion); err != nil {
				return fmt.Errorf("failed to update stored configs: %w", err)
			}

			vlog.Info(fmt.Sprintf("Machine configs synced after upgrade: cluster=%s cp=%v worker=%v version=%s",
				cluster.Name, controlPlaneYAML != nil, workerYAML != nil, talosVersion))
		}
	}

	// Return error only if both failed
	if cpErr != nil && wErr != nil {
		return fmt.Errorf("failed to sync any configs: cp error and worker error occurred")
	}

	return nil
}

// fetchNodeConfig fetches the machine config from a node, returning nil if the node IP is empty or on error
func (o *UpgradeOrchestrator) fetchNodeConfig(
	ctx context.Context,
	clientConfig *clientconfig.Config,
	nodeIP string,
	nodeType string,
) ([]byte, error) {
	if nodeIP == "" {
		return nil, nil
	}

	client, err := o.clientService.CreateTalosClient(ctx, false, clientConfig, []string{nodeIP})
	if err != nil {
		vlog.Warn(fmt.Sprintf("Failed to create client for %s: %v", nodeType, err))
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	defer func() { _ = client.Close() }()

	configYAML, err := o.clientService.GetMachineConfigYAML(ctx, client, nodeIP)
	if err != nil {
		vlog.Warn(fmt.Sprintf("Failed to get %s config: %v", nodeType, err))
		return nil, fmt.Errorf("failed to get config: %w", err)
	}

	return configYAML, nil
}

// updateStoredConfigs updates the stored config templates in the secret
func (o *UpgradeOrchestrator) updateStoredConfigs(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	controlPlaneYAML, workerYAML []byte,
	talosVersion string,
) error {
	if o.upgradeService == nil || o.upgradeService.stateService == nil {
		return fmt.Errorf("state service not available")
	}

	// Only update configs that were provided (non-nil)
	// This preserves existing configs if we couldn't fetch new ones
	return o.upgradeService.stateService.UpdateMachineConfigTemplates(ctx, cluster, controlPlaneYAML, workerYAML, talosVersion)
}
