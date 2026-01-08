// Package upgradeservice provides functionality for executing rolling upgrades.
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
)

const (
	// ControlPlaneSettlingDuration is the time to wait after all control planes
	// are ready before starting worker upgrades
	ControlPlaneSettlingDuration = 60 * time.Second

	// NodeReadyCheckInterval is how often to check if a node is ready
	NodeReadyCheckInterval = 15 * time.Second

	// NodeReadyTimeout is how long to wait for a node to become ready
	NodeReadyTimeout = 10 * time.Minute
)

// RollingUpgradeResult contains the result of a rolling upgrade step
type RollingUpgradeResult struct {
	// Continue indicates if we should continue (requeue)
	Continue bool
	// RequeueAfter is how long to wait before the next reconciliation
	RequeueAfter time.Duration
	// Completed indicates the upgrade is fully complete
	Completed bool
	// Error is set if the upgrade failed
	Error error
	// Message provides details about what happened
	Message string
}

// RollingUpgradeExecutor executes rolling upgrades one node at a time
type RollingUpgradeExecutor struct {
	stateManager       *UpgradeStateManager
	readinessChecker   *NodeReadinessChecker
	talosClientService *talosclientservice.TalosClientService
	upgradeService     UpgradeAnnotationUpdater
}

// UpgradeAnnotationUpdater interface for updating upgrade annotations
type UpgradeAnnotationUpdater interface {
	UpdateTalosUpgradeProgress(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string, nodesUpgraded, totalNodes int) error
	UpdateKubernetesUpgradeProgress(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string, nodesUpgraded, totalNodes int) error
}

// NewRollingUpgradeExecutor creates a new RollingUpgradeExecutor
func NewRollingUpgradeExecutor(
	stateManager *UpgradeStateManager,
	readinessChecker *NodeReadinessChecker,
	talosClientService *talosclientservice.TalosClientService,
	upgradeService UpgradeAnnotationUpdater,
) *RollingUpgradeExecutor {
	return &RollingUpgradeExecutor{
		stateManager:       stateManager,
		readinessChecker:   readinessChecker,
		talosClientService: talosClientService,
		upgradeService:     upgradeService,
	}
}

// getUpgradeStateOrResult retrieves and validates the upgrade state.
// Returns (state, nil) if ready to continue, or (nil, result) if we should return early.
func (e *RollingUpgradeExecutor) getUpgradeStateOrResult(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	upgradeType string,
) (*ClusterUpgradeState, *RollingUpgradeResult) {
	state, err := e.stateManager.GetUpgradeState(ctx, cluster)
	if err != nil {
		return nil, &RollingUpgradeResult{
			Error:   fmt.Errorf("failed to get upgrade state: %w", err),
			Message: "Failed to retrieve upgrade state",
		}
	}
	if state == nil {
		return nil, &RollingUpgradeResult{
			Completed: true,
			Message:   "No upgrade in progress",
		}
	}

	// Check if upgrade is already complete or failed
	if state.IsComplete() {
		return nil, &RollingUpgradeResult{
			Completed: true,
			Message:   fmt.Sprintf("%s upgrade completed successfully", upgradeType),
		}
	}
	if state.IsFailed() {
		return nil, &RollingUpgradeResult{
			Error:   fmt.Errorf("upgrade failed on node %s: %s", state.FailedNodeName, state.FailedReason),
			Message: fmt.Sprintf("Upgrade failed on node %s", state.FailedNodeName),
		}
	}

	return state, nil
}

// ExecuteTalosUpgradeStep executes one step of a Talos rolling upgrade.
// This is designed to be called repeatedly during reconciliation.
// It will upgrade one node at a time, waiting for each to be ready before continuing.
func (e *RollingUpgradeExecutor) ExecuteTalosUpgradeStep(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
) *RollingUpgradeResult {
	state, result := e.getUpgradeStateOrResult(ctx, cluster, "Talos")
	if result != nil {
		return result
	}

	// Handle based on current phase
	switch state.Phase {
	case UpgradePhaseControlPlanes:
		return e.handleControlPlaneUpgrade(ctx, cluster, clientConfig, state)
	case UpgradePhaseControlPlanesWait:
		return e.handleControlPlaneWait(ctx, cluster, state)
	case UpgradePhaseWorkers:
		return e.handleWorkerUpgrade(ctx, cluster, clientConfig, state)
	default:
		return &RollingUpgradeResult{
			Error:   fmt.Errorf("unknown upgrade phase: %s", state.Phase),
			Message: "Invalid upgrade state",
		}
	}
}

// handleControlPlaneUpgrade handles upgrading a single control plane node
func (e *RollingUpgradeExecutor) handleControlPlaneUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	state *ClusterUpgradeState,
) *RollingUpgradeResult {
	node := state.GetCurrentNode()
	if node == nil {
		// No more nodes in this phase, transition
		vlog.Info(fmt.Sprintf("Control plane upgrades complete: cluster=%s count=%d", cluster.Name, state.ControlPlaneCount))
		if err := e.stateManager.AdvanceToNextNode(ctx, cluster); err != nil {
			return &RollingUpgradeResult{
				Error:   fmt.Errorf("failed to advance to next phase: %w", err),
				Message: "Failed to transition upgrade phase",
			}
		}
		return &RollingUpgradeResult{
			Continue:     true,
			RequeueAfter: 5 * time.Second,
			Message:      "Transitioning to control plane wait phase",
		}
	}

	return e.processNode(ctx, cluster, clientConfig, state, node)
}

// handleControlPlaneWait handles the waiting period after control plane upgrades
func (e *RollingUpgradeExecutor) handleControlPlaneWait(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	state *ClusterUpgradeState,
) *RollingUpgradeResult {
	// First verify all control planes are actually ready
	for i := range state.Nodes {
		if i >= state.ControlPlaneCount {
			break // Only check control planes
		}
		node := &state.Nodes[i]
		if !node.NodeReady {
			// Check if node is now ready
			status, err := e.readinessChecker.IsNodeReady(ctx, cluster, node.NodeName)
			if err != nil {
				vlog.Warn(fmt.Sprintf("Error checking control plane readiness: node=%s error=%v", node.NodeName, err))
				return &RollingUpgradeResult{
					Continue:     true,
					RequeueAfter: 30 * time.Second,
					Message:      fmt.Sprintf("Waiting for control plane %s to become accessible", node.NodeName),
				}
			}
			if !status.Ready {
				return &RollingUpgradeResult{
					Continue:     true,
					RequeueAfter: 20 * time.Second,
					Message:      fmt.Sprintf("Waiting for control plane %s to become ready", node.NodeName),
				}
			}
			// Mark as ready
			if err := e.stateManager.MarkNodeReady(ctx, cluster, node.NodeName); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to mark node ready: %v", err))
			}
			// Uncordon the node
			if err := e.readinessChecker.UncordonNode(ctx, cluster, node.NodeName); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to uncordon control plane %s: %v", node.NodeName, err))
			}
		}
	}

	// Check if we should still wait (settling time)
	if state.ShouldWaitForSettling(ControlPlaneSettlingDuration) {
		remaining := state.GetRemainingSettlingTime(ControlPlaneSettlingDuration)
		return &RollingUpgradeResult{
			Continue:     true,
			RequeueAfter: remaining,
			Message:      fmt.Sprintf("Waiting for control planes to settle (%v remaining)", remaining.Round(time.Second)),
		}
	}

	// Settling complete, transition to workers
	if err := e.stateManager.TransitionToWorkers(ctx, cluster); err != nil {
		return &RollingUpgradeResult{
			Error:   fmt.Errorf("failed to transition to worker upgrades: %w", err),
			Message: "Failed to start worker upgrades",
		}
	}

	// Check if we're now complete (no workers)
	state, _ = e.stateManager.GetUpgradeState(ctx, cluster)
	if state != nil && state.IsComplete() {
		return &RollingUpgradeResult{
			Completed: true,
			Message:   "Upgrade completed (no workers to upgrade)",
		}
	}

	return &RollingUpgradeResult{
		Continue:     true,
		RequeueAfter: 5 * time.Second,
		Message:      "Starting worker upgrades",
	}
}

// handleWorkerUpgrade handles upgrading a single worker node
func (e *RollingUpgradeExecutor) handleWorkerUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	state *ClusterUpgradeState,
) *RollingUpgradeResult {
	node := state.GetCurrentNode()
	if node == nil {
		// All workers done
		state.Phase = UpgradePhaseCompleted
		state.CompletedAt = time.Now().UTC()
		if err := e.stateManager.SaveUpgradeState(ctx, cluster, state); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to save completion state: %v", err))
		}
		return &RollingUpgradeResult{
			Completed: true,
			Message:   "All nodes upgraded successfully",
		}
	}

	return e.processNode(ctx, cluster, clientConfig, state, node)
}

// processNode processes the upgrade for a single node
func (e *RollingUpgradeExecutor) processNode(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	state *ClusterUpgradeState,
	node *NodeUpgradeState,
) *RollingUpgradeResult {
	// Step 1: If upgrade not initiated, initiate it
	if !node.UpgradeInitiated {
		return e.initiateNodeUpgrade(ctx, cluster, clientConfig, state, node)
	}

	// Step 2: If upgrade initiated but not completed, check version
	if !node.UpgradeCompleted {
		return e.checkNodeUpgradeCompletion(ctx, cluster, clientConfig, state, node)
	}

	// Step 3: If upgraded but not ready, wait for ready
	if !node.NodeReady {
		return e.waitForNodeReady(ctx, cluster, state, node)
	}

	// Step 4: Node is fully done, advance to next
	// Uncordon the node
	if err := e.readinessChecker.UncordonNode(ctx, cluster, node.NodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to uncordon node %s: %v", node.NodeName, err))
	}

	// Advance to next node
	if err := e.stateManager.AdvanceToNextNode(ctx, cluster); err != nil {
		return &RollingUpgradeResult{
			Error:   fmt.Errorf("failed to advance to next node: %w", err),
			Message: "Failed to advance upgrade",
		}
	}

	return &RollingUpgradeResult{
		Continue:     true,
		RequeueAfter: 5 * time.Second,
		Message:      fmt.Sprintf("Node %s upgrade complete, proceeding to next", node.NodeName),
	}
}

// initiateNodeUpgrade initiates the Talos upgrade on a node
func (e *RollingUpgradeExecutor) initiateNodeUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	state *ClusterUpgradeState,
	node *NodeUpgradeState,
) *RollingUpgradeResult {
	vlog.Info(fmt.Sprintf("Upgrading node: cluster=%s node=%s (%d/%d)",
		cluster.Name, node.NodeName, state.CurrentNodeIndex+1, len(state.Nodes)))

	// Cordon and drain the node first
	if err := e.readinessChecker.DrainNode(ctx, cluster, node.NodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Drain failed (continuing anyway): node=%s error=%v", node.NodeName, err))
		// Don't fail on drain errors - the node will reboot anyway
	}

	// Create Talos client
	client, err := e.talosClientService.CreateTalosClient(ctx, false, clientConfig, []string{node.IP})
	if err != nil {
		_ = e.stateManager.MarkUpgradeFailed(ctx, cluster, node.NodeName, fmt.Sprintf("failed to create Talos client: %v", err))
		return &RollingUpgradeResult{
			Error:   fmt.Errorf("failed to create Talos client: %w", err),
			Message: fmt.Sprintf("Cannot connect to node %s", node.NodeName),
		}
	}
	defer func() { _ = client.Close() }()

	// Call the upgrade API
	if err := e.talosClientService.UpgradeNode(ctx, client, node.IP, state.InstallerImage); err != nil {
		// Check if it's a "locked" error (upgrade already in progress)
		if strings.Contains(err.Error(), "locked") {
			vlog.Info(fmt.Sprintf("Upgrade already in progress on node %s", node.NodeName))
			// Mark as initiated and continue
			if err := e.stateManager.MarkNodeUpgradeInitiated(ctx, cluster, node.NodeName); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to mark node as initiated: %v", err))
			}
			return &RollingUpgradeResult{
				Continue:     true,
				RequeueAfter: 30 * time.Second,
				Message:      fmt.Sprintf("Upgrade already in progress on %s, waiting", node.NodeName),
			}
		}

		// Check for connection errors (node may be rebooting)
		if isConnectionError(err) {
			vlog.Info(fmt.Sprintf("Connection lost during upgrade initiation (node may be rebooting): %s", node.NodeName))
			// Mark as initiated - the upgrade API may have succeeded before connection was lost
			if err := e.stateManager.MarkNodeUpgradeInitiated(ctx, cluster, node.NodeName); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to mark node as initiated: %v", err))
			}
			return &RollingUpgradeResult{
				Continue:     true,
				RequeueAfter: 30 * time.Second,
				Message:      fmt.Sprintf("Waiting for node %s to reboot", node.NodeName),
			}
		}

		_ = e.stateManager.MarkUpgradeFailed(ctx, cluster, node.NodeName, fmt.Sprintf("upgrade API failed: %v", err))
		return &RollingUpgradeResult{
			Error:   fmt.Errorf("upgrade failed: %w", err),
			Message: fmt.Sprintf("Upgrade API failed on node %s", node.NodeName),
		}
	}

	// Mark as initiated
	if err := e.stateManager.MarkNodeUpgradeInitiated(ctx, cluster, node.NodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to mark node as initiated: %v", err))
	}

	vlog.Info(fmt.Sprintf("Talos upgrade initiated: cluster=%s node=%s", cluster.Name, node.NodeName))

	return &RollingUpgradeResult{
		Continue:     true,
		RequeueAfter: 30 * time.Second,
		Message:      fmt.Sprintf("Upgrade initiated on %s, waiting for completion", node.NodeName),
	}
}

// checkNodeUpgradeCompletion checks if a node's upgrade has completed by verifying version
func (e *RollingUpgradeExecutor) checkNodeUpgradeCompletion(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	state *ClusterUpgradeState,
	node *NodeUpgradeState,
) *RollingUpgradeResult {
	// Try to create a client to the node
	client, err := e.talosClientService.CreateTalosClient(ctx, false, clientConfig, []string{node.IP})
	if err != nil {
		// Connection error - node is likely still rebooting
		return &RollingUpgradeResult{
			Continue:     true,
			RequeueAfter: 30 * time.Second,
			Message:      fmt.Sprintf("Waiting for node %s to come back online", node.NodeName),
		}
	}
	defer func() { _ = client.Close() }()

	// Get current version
	currentVersion, err := e.talosClientService.GetTalosVersion(ctx, client, node.IP)
	if err != nil {
		return &RollingUpgradeResult{
			Continue:     true,
			RequeueAfter: 30 * time.Second,
			Message:      fmt.Sprintf("Waiting for node %s to respond", node.NodeName),
		}
	}

	// Compare versions (normalize by removing 'v' prefix)
	normalizedCurrent := strings.TrimPrefix(currentVersion, "v")
	normalizedTarget := strings.TrimPrefix(state.TargetVersion, "v")

	if normalizedCurrent == normalizedTarget {
		vlog.Info(fmt.Sprintf("Node upgraded: cluster=%s node=%s version=%s", cluster.Name, node.NodeName, currentVersion))
		if err := e.stateManager.MarkNodeUpgradeCompleted(ctx, cluster, node.NodeName); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to mark node as completed: %v", err))
		}
		// Update progress annotations
		completedCount := state.CurrentNodeIndex + 1
		if e.upgradeService != nil {
			_ = e.upgradeService.UpdateTalosUpgradeProgress(ctx, cluster, node.NodeName, completedCount, len(state.Nodes))
		}
		return &RollingUpgradeResult{
			Continue:     true,
			RequeueAfter: 5 * time.Second,
			Message:      fmt.Sprintf("Node %s upgrade verified, checking readiness", node.NodeName),
		}
	}

	return &RollingUpgradeResult{
		Continue:     true,
		RequeueAfter: 30 * time.Second,
		Message: fmt.Sprintf("Waiting for node %s to update (current=%s, target=%s)",
			node.NodeName, currentVersion, state.TargetVersion),
	}
}

// waitForNodeReady waits for a node to become ready in Kubernetes
func (e *RollingUpgradeExecutor) waitForNodeReady(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	_ *ClusterUpgradeState, // state not used, but kept for API consistency
	node *NodeUpgradeState,
) *RollingUpgradeResult {
	status, err := e.readinessChecker.IsNodeReady(ctx, cluster, node.NodeName)
	if err != nil {
		vlog.Warn(fmt.Sprintf("Error checking node readiness: node=%s error=%v", node.NodeName, err))
		return &RollingUpgradeResult{
			Continue:     true,
			RequeueAfter: 30 * time.Second,
			Message:      fmt.Sprintf("Waiting for node %s to register with Kubernetes", node.NodeName),
		}
	}

	if !status.Ready {
		return &RollingUpgradeResult{
			Continue:     true,
			RequeueAfter: 20 * time.Second,
			Message:      fmt.Sprintf("Waiting for node %s to become ready", node.NodeName),
		}
	}
	if err := e.stateManager.MarkNodeReady(ctx, cluster, node.NodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to mark node as ready: %v", err))
	}

	return &RollingUpgradeResult{
		Continue:     true,
		RequeueAfter: 5 * time.Second,
		Message:      fmt.Sprintf("Node %s is ready", node.NodeName),
	}
}

// KubernetesSettlingDuration is the minimum time to wait after all control planes
// are upgraded before starting worker upgrades for Kubernetes
const KubernetesSettlingDuration = 30 * time.Second

// ExecuteKubernetesUpgradeStep executes one step of a Kubernetes rolling upgrade.
// Uses the same phase-based approach as Talos upgrades:
// ControlPlanes -> ControlPlanesWait -> Workers -> Completed
func (e *RollingUpgradeExecutor) ExecuteKubernetesUpgradeStep(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
) *RollingUpgradeResult {
	state, result := e.getUpgradeStateOrResult(ctx, cluster, "Kubernetes")
	if result != nil {
		return result
	}

	// Handle based on current phase (same as Talos upgrades)
	switch state.Phase {
	case UpgradePhaseControlPlanes:
		return e.handleKubernetesControlPlaneUpgrade(ctx, cluster, clientConfig, state)
	case UpgradePhaseControlPlanesWait:
		return e.handleKubernetesControlPlaneWait(ctx, cluster, state)
	case UpgradePhaseWorkers:
		return e.handleKubernetesWorkerUpgrade(ctx, cluster, clientConfig, state)
	default:
		return &RollingUpgradeResult{
			Error:   fmt.Errorf("unknown upgrade phase: %s", state.Phase),
			Message: "Invalid upgrade state",
		}
	}
}

// handleKubernetesControlPlaneUpgrade handles upgrading a single control plane node for Kubernetes
func (e *RollingUpgradeExecutor) handleKubernetesControlPlaneUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	state *ClusterUpgradeState,
) *RollingUpgradeResult {
	node := state.GetCurrentNode()
	if node == nil {
		// No more control planes, transition to wait phase
		vlog.Info(fmt.Sprintf("Kubernetes control plane upgrades complete: cluster=%s count=%d",
			cluster.Name, state.ControlPlaneCount))
		if err := e.stateManager.AdvanceToNextNode(ctx, cluster); err != nil {
			return &RollingUpgradeResult{
				Error:   fmt.Errorf("failed to advance to next phase: %w", err),
				Message: "Failed to transition upgrade phase",
			}
		}
		return &RollingUpgradeResult{
			Continue:     true,
			RequeueAfter: 5 * time.Second,
			Message:      "Transitioning to control plane wait phase",
		}
	}

	return e.processKubernetesNode(ctx, cluster, clientConfig, state, node)
}

// handleKubernetesControlPlaneWait handles the waiting period after control plane Kubernetes upgrades
func (e *RollingUpgradeExecutor) handleKubernetesControlPlaneWait(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	state *ClusterUpgradeState,
) *RollingUpgradeResult {
	// First verify all control planes are actually ready
	for i := range state.Nodes {
		if i >= state.ControlPlaneCount {
			break // Only check control planes
		}
		node := &state.Nodes[i]
		if !node.NodeReady {
			// Check if node is now ready
			status, err := e.readinessChecker.IsNodeReady(ctx, cluster, node.NodeName)
			if err != nil {
				vlog.Warn(fmt.Sprintf("Error checking control plane readiness: node=%s error=%v", node.NodeName, err))
				return &RollingUpgradeResult{
					Continue:     true,
					RequeueAfter: 30 * time.Second,
					Message:      fmt.Sprintf("Waiting for control plane %s to become accessible", node.NodeName),
				}
			}
			if !status.Ready {
				return &RollingUpgradeResult{
					Continue:     true,
					RequeueAfter: 20 * time.Second,
					Message:      fmt.Sprintf("Waiting for control plane %s to become ready", node.NodeName),
				}
			}
			// Mark as ready
			if err := e.stateManager.MarkNodeReady(ctx, cluster, node.NodeName); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to mark node ready: %v", err))
			}
		}
	}

	// Check if we should still wait (settling time)
	if state.ShouldWaitForSettling(KubernetesSettlingDuration) {
		remaining := state.GetRemainingSettlingTime(KubernetesSettlingDuration)
		return &RollingUpgradeResult{
			Continue:     true,
			RequeueAfter: remaining,
			Message:      fmt.Sprintf("Waiting for Kubernetes control planes to settle (%v remaining)", remaining.Round(time.Second)),
		}
	}

	// Settling complete, transition to workers
	if err := e.stateManager.TransitionToWorkers(ctx, cluster); err != nil {
		return &RollingUpgradeResult{
			Error:   fmt.Errorf("failed to transition to worker upgrades: %w", err),
			Message: "Failed to start worker upgrades",
		}
	}

	// Check if we're now complete (no workers)
	state, _ = e.stateManager.GetUpgradeState(ctx, cluster)
	if state != nil && state.IsComplete() {
		vlog.Info(fmt.Sprintf("Kubernetes upgrade completed (no workers): cluster=%s", cluster.Name))
		return &RollingUpgradeResult{
			Completed: true,
			Message:   "Kubernetes upgrade completed (no workers to upgrade)",
		}
	}

	vlog.Info(fmt.Sprintf("Starting Kubernetes worker upgrades: cluster=%s workers=%d",
		cluster.Name, state.WorkerCount))
	return &RollingUpgradeResult{
		Continue:     true,
		RequeueAfter: 5 * time.Second,
		Message:      "Starting Kubernetes worker upgrades",
	}
}

// handleKubernetesWorkerUpgrade handles upgrading a single worker node for Kubernetes
func (e *RollingUpgradeExecutor) handleKubernetesWorkerUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	state *ClusterUpgradeState,
) *RollingUpgradeResult {
	node := state.GetCurrentNode()
	if node == nil {
		// All workers done
		state.Phase = UpgradePhaseCompleted
		state.CompletedAt = time.Now().UTC()
		if err := e.stateManager.SaveUpgradeState(ctx, cluster, state); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to save completion state: %v", err))
		}
		vlog.Info(fmt.Sprintf("All Kubernetes nodes upgraded successfully: cluster=%s", cluster.Name))
		return &RollingUpgradeResult{
			Completed: true,
			Message:   "All Kubernetes nodes upgraded successfully",
		}
	}

	return e.processKubernetesNode(ctx, cluster, clientConfig, state, node)
}

// processKubernetesNode processes Kubernetes upgrade for a single node
// This follows the same pattern as Talos: initiate -> wait for completion -> wait for ready -> advance
func (e *RollingUpgradeExecutor) processKubernetesNode(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	state *ClusterUpgradeState,
	node *NodeUpgradeState,
) *RollingUpgradeResult {
	// Step 1: If upgrade not initiated, initiate it
	if !node.UpgradeInitiated {
		return e.initiateKubernetesNodeUpgrade(ctx, cluster, clientConfig, state, node)
	}

	// Step 2: If upgrade initiated but not completed, mark as completed
	// (Kubernetes config apply is synchronous, so once initiated it's effectively done)
	if !node.UpgradeCompleted {
		if err := e.stateManager.MarkNodeUpgradeCompleted(ctx, cluster, node.NodeName); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to mark node upgrade completed: %v", err))
		}
		vlog.Info(fmt.Sprintf("Kubernetes config applied to node: cluster=%s node=%s version=%s",
			cluster.Name, node.NodeName, state.TargetVersion))
		return &RollingUpgradeResult{
			Continue:     true,
			RequeueAfter: 5 * time.Second,
			Message:      fmt.Sprintf("Kubernetes config applied to %s, waiting for node ready", node.NodeName),
		}
	}

	// Step 3: If upgraded but not ready, wait for ready
	if !node.NodeReady {
		return e.waitForKubernetesNodeReady(ctx, cluster, node)
	}

	// Step 4: Node is fully done, uncordon and advance
	if err := e.readinessChecker.UncordonNode(ctx, cluster, node.NodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to uncordon node %s: %v", node.NodeName, err))
	}

	// Update progress annotations
	completedCount := state.CurrentNodeIndex + 1
	if e.upgradeService != nil {
		_ = e.upgradeService.UpdateKubernetesUpgradeProgress(ctx, cluster, node.NodeName, completedCount, len(state.Nodes))
	}

	vlog.Info(fmt.Sprintf("Kubernetes upgrade completed on node: cluster=%s node=%s (%d/%d)",
		cluster.Name, node.NodeName, completedCount, len(state.Nodes)))

	// Advance to next node
	if err := e.stateManager.AdvanceToNextNode(ctx, cluster); err != nil {
		return &RollingUpgradeResult{
			Error:   fmt.Errorf("failed to advance to next node: %w", err),
			Message: "Failed to advance upgrade",
		}
	}

	return &RollingUpgradeResult{
		Continue:     true,
		RequeueAfter: 5 * time.Second,
		Message:      fmt.Sprintf("Node %s Kubernetes upgrade complete, proceeding to next", node.NodeName),
	}
}

// initiateKubernetesNodeUpgrade initiates the Kubernetes upgrade on a node
func (e *RollingUpgradeExecutor) initiateKubernetesNodeUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	state *ClusterUpgradeState,
	node *NodeUpgradeState,
) *RollingUpgradeResult {
	nodePosition := state.CurrentNodeIndex + 1
	totalNodes := len(state.Nodes)
	isControlPlane := node.Role == controlPlaneRole
	nodeType := roleWorker
	if isControlPlane {
		nodeType = controlPlaneRole
	}

	vlog.Info(fmt.Sprintf("Upgrading Kubernetes on %s: cluster=%s node=%s (%d/%d) target=%s",
		nodeType, cluster.Name, node.NodeName, nodePosition, totalNodes, state.TargetVersion))

	// Update progress annotation
	if e.upgradeService != nil {
		_ = e.upgradeService.UpdateKubernetesUpgradeProgress(ctx, cluster, node.NodeName, nodePosition-1, totalNodes)
	}

	// Cordon node to prevent new workloads
	if err := e.readinessChecker.CordonNode(ctx, cluster, node.NodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Cordon failed (continuing anyway): node=%s error=%v", node.NodeName, err))
	}

	// Create Talos client for this node
	client, err := e.talosClientService.CreateTalosClient(ctx, false, clientConfig, []string{node.IP})
	if err != nil {
		errMsg := fmt.Sprintf("failed to create Talos client: %v", err)
		vlog.Error(fmt.Sprintf("Kubernetes upgrade failed: cluster=%s node=%s error=%s", cluster.Name, node.NodeName, errMsg), err)
		_ = e.stateManager.MarkUpgradeFailed(ctx, cluster, node.NodeName, errMsg)
		return &RollingUpgradeResult{
			Error:   fmt.Errorf("failed to create Talos client: %w", err),
			Message: fmt.Sprintf("Cannot connect to node %s", node.NodeName),
		}
	}
	defer func() { _ = client.Close() }()

	// Upgrade Kubernetes on this node via Talos machine config
	nodeInfo := []talosclientservice.NodeUpgradeInfo{{
		Name:           node.NodeName,
		IP:             node.IP,
		IsControlPlane: isControlPlane,
	}}
	if err := e.talosClientService.UpgradeKubernetes(ctx, client, nodeInfo, state.TargetVersion); err != nil {
		errMsg := fmt.Sprintf("kubernetes upgrade failed: %v", err)
		vlog.Error(fmt.Sprintf("Kubernetes upgrade failed: cluster=%s node=%s error=%s", cluster.Name, node.NodeName, errMsg), err)
		_ = e.stateManager.MarkUpgradeFailed(ctx, cluster, node.NodeName, errMsg)
		return &RollingUpgradeResult{
			Error:   fmt.Errorf("kubernetes upgrade failed: %w", err),
			Message: fmt.Sprintf("Kubernetes upgrade failed on node %s", node.NodeName),
		}
	}

	// Mark upgrade as initiated
	if err := e.stateManager.MarkNodeUpgradeInitiated(ctx, cluster, node.NodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to mark node as initiated: %v", err))
	}

	vlog.Info(fmt.Sprintf("Kubernetes upgrade initiated: cluster=%s node=%s type=%s version=%s",
		cluster.Name, node.NodeName, nodeType, state.TargetVersion))

	return &RollingUpgradeResult{
		Continue:     true,
		RequeueAfter: 10 * time.Second,
		Message:      fmt.Sprintf("Kubernetes upgrade initiated on %s, waiting for completion", node.NodeName),
	}
}

// waitForKubernetesNodeReady waits for a node to become ready after Kubernetes upgrade
func (e *RollingUpgradeExecutor) waitForKubernetesNodeReady(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	node *NodeUpgradeState,
) *RollingUpgradeResult {
	status, err := e.readinessChecker.IsNodeReady(ctx, cluster, node.NodeName)
	if err != nil {
		vlog.Warn(fmt.Sprintf("Error checking node readiness: node=%s error=%v", node.NodeName, err))
		return &RollingUpgradeResult{
			Continue:     true,
			RequeueAfter: 30 * time.Second,
			Message:      fmt.Sprintf("Waiting for node %s to register with Kubernetes", node.NodeName),
		}
	}

	if !status.Ready {
		return &RollingUpgradeResult{
			Continue:     true,
			RequeueAfter: 20 * time.Second,
			Message:      fmt.Sprintf("Waiting for node %s to become ready after Kubernetes upgrade", node.NodeName),
		}
	}

	// Node is ready, mark it
	if err := e.stateManager.MarkNodeReady(ctx, cluster, node.NodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to mark node as ready: %v", err))
	}

	vlog.Info(fmt.Sprintf("Node ready after Kubernetes upgrade: cluster=%s node=%s", cluster.Name, node.NodeName))

	return &RollingUpgradeResult{
		Continue:     true,
		RequeueAfter: 5 * time.Second,
		Message:      fmt.Sprintf("Node %s is ready", node.NodeName),
	}
}
