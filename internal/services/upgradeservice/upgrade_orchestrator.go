package upgradeservice

import (
	"context"
	"fmt"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
)

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
	workers, controlPlanes := o.sortNodesByRole(nodes)
	orderedNodes := make([]NodeUpgradeInfo, 0, len(workers)+len(controlPlanes))
	orderedNodes = append(orderedNodes, workers...)
	orderedNodes = append(orderedNodes, controlPlanes...)

	vlog.Info(fmt.Sprintf("Starting Talos upgrade: cluster=%s target=%s workers=%d controlPlanes=%d",
		cluster.Name, targetVersion, len(workers), len(controlPlanes)))

	// Find the next node that needs upgrading
	nodesUpgraded := 0
	var skippedNodes []string
	for i, node := range orderedNodes {
		// Count already upgraded nodes
		if node.Upgraded {
			nodesUpgraded++
			continue
		}

		// Skip nodes marked as skipped (e.g., previously failed and user chose to skip)
		if node.Skipped {
			skippedNodes = append(skippedNodes, node.Name)
			vlog.Info(fmt.Sprintf("Skipping node %s as requested", node.Name))
			continue
		}

		// Skip nodes that previously failed (unless retry is requested)
		if node.Failed {
			skippedNodes = append(skippedNodes, node.Name)
			vlog.Warn(fmt.Sprintf("Skipping previously failed node %s (use retry-failed-nodes annotation to retry)", node.Name))
			continue
		}

		// Found a node that needs upgrading
		vlog.Info(fmt.Sprintf("Upgrading Talos on node: cluster=%s node=%s ip=%s role=%s (%d/%d)",
			cluster.Name, node.Name, node.IP, node.Role, i+1, totalNodes))

		// Update progress annotation
		if err := o.upgradeService.UpdateTalosUpgradeProgress(ctx, cluster, node.Name, nodesUpgraded, totalNodes); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to update progress: %v", err))
		}

		// Perform the actual upgrade on this node
		if err := o.upgradeTalosNode(ctx, clientConfig, node.IP, talosInstallerImage); err != nil {
			vlog.Error(fmt.Sprintf("Failed to upgrade Talos on node %s: %v", node.Name, err), err)
			return &UpgradeResult{
				Success:       false,
				NodesUpgraded: nodesUpgraded,
				TotalNodes:    totalNodes,
				FailedNode:    node.Name,
				SkippedNodes:  skippedNodes,
				Error:         err,
			}
		}

		// Mark node as upgraded in the state service
		if o.upgradeService.stateService != nil {
			if err := o.upgradeService.stateService.MarkNodeUpgraded(ctx, cluster, node.Name); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to mark node %s as upgraded in state: %v", node.Name, err))
			}
		}

		nodesUpgraded++
		vlog.Info(fmt.Sprintf("Talos upgrade initiated on node: cluster=%s node=%s (%d/%d complete)",
			cluster.Name, node.Name, nodesUpgraded, totalNodes))

		// After initiating upgrade on one node, requeue to check status
		// The node will reboot during upgrade, so we need to wait
		return &UpgradeResult{
			Success:       true,
			NodesUpgraded: nodesUpgraded,
			TotalNodes:    totalNodes,
			SkippedNodes:  skippedNodes,
			NeedsRequeue:  true,
			RequeueAfter:  30 * time.Second, // Check again after 30s
		}
	}

	// All nodes upgraded (or skipped)
	if len(skippedNodes) > 0 {
		vlog.Warn(fmt.Sprintf("Upgrade completed with skipped nodes: cluster=%s upgraded=%d skipped=%d nodes=%v",
			cluster.Name, nodesUpgraded, len(skippedNodes), skippedNodes))
	} else {
		vlog.Info(fmt.Sprintf("All nodes upgraded: cluster=%s nodes=%d", cluster.Name, nodesUpgraded))
	}
	return &UpgradeResult{
		Success:       true,
		NodesUpgraded: nodesUpgraded,
		TotalNodes:    totalNodes,
		SkippedNodes:  skippedNodes,
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

// PerformKubernetesUpgrade executes a Kubernetes version upgrade using talosctl upgrade-k8s
func (o *UpgradeOrchestrator) PerformKubernetesUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	controlPlaneIP string,
	targetVersion string,
) *UpgradeResult {
	vlog.Info(fmt.Sprintf("Starting Kubernetes upgrade: cluster=%s target=%s endpoint=%s",
		cluster.Name, targetVersion, controlPlaneIP))

	// Create Talos client
	client, err := o.clientService.CreateTalosClient(ctx, false, clientConfig, []string{controlPlaneIP})
	if err != nil {
		return &UpgradeResult{
			Success: false,
			Error:   fmt.Errorf("failed to create Talos client: %w", err),
		}
	}
	defer func() { _ = client.Close() }()

	// Perform Kubernetes upgrade
	if err := o.clientService.UpgradeKubernetes(ctx, client, controlPlaneIP, targetVersion); err != nil {
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

	return currentVersion == expectedVersion, currentVersion, nil
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

// PreflightChecks performs checks before starting an upgrade
func (o *UpgradeOrchestrator) PreflightChecks(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	controlPlaneIP string,
) error {
	// Check that all Talos APIs are reachable
	if !o.clientService.IsTalosAPIReachable(controlPlaneIP) {
		return fmt.Errorf("talos API not reachable on control plane %s", controlPlaneIP)
	}

	// Check etcd health
	client, err := o.clientService.CreateTalosClient(ctx, false, clientConfig, []string{controlPlaneIP})
	if err != nil {
		return fmt.Errorf("failed to create Talos client: %w", err)
	}
	defer func() { _ = client.Close() }()

	healthy, err := o.clientService.IsEtcdHealthy(ctx, client, controlPlaneIP)
	if err != nil {
		return fmt.Errorf("failed to check etcd health: %w", err)
	}
	if !healthy {
		return fmt.Errorf("etcd cluster is not healthy - upgrade aborted for safety")
	}

	vlog.Info(fmt.Sprintf("Preflight checks passed: cluster=%s", cluster.Name))
	return nil
}
