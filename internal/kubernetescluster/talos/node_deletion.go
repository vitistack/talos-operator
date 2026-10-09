package talos

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/drainservice"
	"github.com/vitistack/talos-operator/internal/services/etcdservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
)

// NodeDeletionError represents an error during node deletion
type NodeDeletionError struct {
	NodeName string
	Err      error
}

func (e *NodeDeletionError) Error() string {
	return fmt.Sprintf("failed to delete node %s: %v", e.NodeName, e.Err)
}

// NodeDeletionResult represents the result of a node deletion operation
type NodeDeletionResult struct {
	NodeName       string
	Success        bool
	EtcdRemoved    bool
	VIPUpdated     bool
	ConfigRemoved  bool
	K8sNodeDeleted bool
	Error          error
}

// reconcileRemovedNodes handles removing nodes that are no longer part of the desired state.
// It compares configured nodes against current machines and removes nodes that no longer exist.
func (t *TalosManager) reconcileRemovedNodes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	configuredNodes, err := t.getConfiguredNodes(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get configured nodes: %w", err)
	}
	if len(configuredNodes) == 0 {
		return nil
	}

	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get cluster machines: %w", err)
	}

	// Build a set of current machine names
	currentMachines := make(map[string]bool)
	for _, m := range machines {
		currentMachines[m.Name] = true
	}

	// Find nodes that are configured but no longer exist as machines
	var nodesToRemove []string
	for nodeName := range configuredNodes {
		if !currentMachines[nodeName] {
			nodesToRemove = append(nodesToRemove, nodeName)
		}
	}

	if len(nodesToRemove) == 0 {
		return nil
	}

	vlog.Info(fmt.Sprintf("Found %d nodes to remove from %s: %v", len(nodesToRemove), clusterLogTag(cluster), nodesToRemove))
	_ = t.statusManager.SetMessage(ctx, cluster, fmt.Sprintf("Removing nodes %v", nodesToRemove))

	// Load Talos artifacts for deletion operations
	clientConfig, _, err := t.loadTalosArtifacts(ctx, cluster)
	if err != nil || clientConfig == nil {
		return fmt.Errorf("failed to load talos artifacts for node deletion: %w", err)
	}

	// Process removals
	for _, nodeName := range nodesToRemove {
		if err := t.removeNode(ctx, cluster, clientConfig, nodeName); err != nil {
			vlog.Error(fmt.Sprintf("Failed to remove node %s %s: %v", nodeName, clusterLogTag(cluster), err), err)
			// Continue with other nodes even if one fails
			continue
		}
	}

	return nil
}

// removeNode removes a single node from the cluster.
// For control planes: removes from etcd, updates VIP pool members
// For workers: simply removes from configured nodes list
func (t *TalosManager) removeNode(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	nodeName string) error {
	// We need to determine if this was a control plane by checking the configured nodes
	// Since the machine no longer exists, we need to use stored metadata or heuristics
	// For now, we'll check if the name matches the control plane naming pattern
	isControlPlane := isControlPlaneName(nodeName)

	if isControlPlane {
		if err := t.removeControlPlaneNode(ctx, cluster, clientConfig, nodeName); err != nil {
			return err
		}
	} else {
		if err := t.removeWorkerNode(ctx, cluster, nodeName); err != nil {
			return err
		}
	}

	return nil
}

// isControlPlaneName checks if the node name matches control plane naming conventions.
// Control plane machines typically have "-cp-" or "controlplane" in their names.
func isControlPlaneName(nodeName string) bool {
	// Common patterns for control plane naming
	patterns := []string{"-cp-", "controlplane", "-control-plane-", "-master-"}
	for _, pattern := range patterns {
		if containsIgnoreCase(nodeName, pattern) {
			return true
		}
	}
	return false
}

// removeControlPlaneNode handles removal of a control plane node.
// This includes:
// 1. Validating that we're not removing the last control plane
// 2. Forfeiting leadership if needed
// 3. Removing from etcd cluster
// 4. Updating VIP pool members
// 5. Removing from configured nodes list
func (t *TalosManager) removeControlPlaneNode(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	nodeName string) error {
	vlog.Info(fmt.Sprintf("Removing control plane node from cluster: node=%s %s", nodeName, clusterLogTag(cluster)))

	// Get remaining control planes to find a healthy node to use for etcd operations
	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get cluster machines: %w", err)
	}

	controlPlanes := t.machineService.FilterMachinesByRole(machines, controlPlaneRole)

	// Validate: cannot remove the last control plane
	// Note: Since the machine we're removing no longer exists, we just need at least one remaining
	if len(controlPlanes) < 1 {
		return fmt.Errorf("cannot remove control plane node %s: no remaining control planes", nodeName)
	}

	// Find a healthy control plane to use for etcd operations
	healthyCP := t.findHealthyControlPlaneExcluding(controlPlanes, "")
	if healthyCP == "" {
		return fmt.Errorf("no healthy control plane found to perform etcd operations")
	}

	// Get etcd member list to find the member ID for the node being removed
	members, err := t.etcdService.MemberList(ctx, clientConfig, healthyCP)
	if err != nil {
		vlog.Warn(fmt.Sprintf("Failed to list etcd members %s, continuing with removal: %v", clusterLogTag(cluster), err))
	} else {
		// Find the etcd member for this node
		member := etcdservice.FindMemberByHostname(members, nodeName)
		if member != nil {
			// Remove from etcd
			if err := t.etcdService.RemoveMemberByID(ctx, clientConfig, healthyCP, member.ID); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to remove etcd member %s/%s (ID: %d): %v", clusterLogTag(cluster), nodeName, member.ID, err))
				// Continue anyway - the member may already be gone
			} else {
				vlog.Info(fmt.Sprintf("Removed etcd member: node=%s memberID=%d %s", nodeName, member.ID, clusterLogTag(cluster)))
			}
		} else {
			vlog.Info(fmt.Sprintf("Etcd member not found for node %s/%s, may already be removed", clusterLogTag(cluster), nodeName))
		}
	}

	// Update VIP pool members
	if err := t.updateVIPPoolMembers(ctx, cluster); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to update VIP pool members %s: %v", clusterLogTag(cluster), err))
		// Continue anyway
	}

	// Remove from configured nodes list
	if err := t.stateService.RemoveConfiguredNode(ctx, cluster, nodeName); err != nil {
		return fmt.Errorf("failed to remove node from configured nodes: %w", err)
	}

	vlog.Info(fmt.Sprintf("Successfully removed control plane node: node=%s %s", nodeName, clusterLogTag(cluster)))
	return nil
}

// removeWorkerNode handles removal of a worker node.
// This is simpler than control plane removal as it only needs to:
// 1. Remove from configured nodes list
func (t *TalosManager) removeWorkerNode(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	nodeName string) error {
	vlog.Info(fmt.Sprintf("Removing worker node from cluster: node=%s %s", nodeName, clusterLogTag(cluster)))

	// Remove from configured nodes list
	if err := t.stateService.RemoveConfiguredNode(ctx, cluster, nodeName); err != nil {
		return fmt.Errorf("failed to remove worker node from configured nodes: %w", err)
	}

	vlog.Info(fmt.Sprintf("Successfully removed worker node: node=%s %s", nodeName, clusterLogTag(cluster)))
	return nil
}

// DeleteControlPlaneNode explicitly deletes a control plane node by name.
// This is meant to be called when intentionally scaling down control planes.
// It performs the full deletion process including etcd removal and VIP updates.
//
// Requirements:
// - Cannot delete if only 1 control plane remains
// - If the node is the etcd leader, leadership will be forfeited first
func (t *TalosManager) DeleteControlPlaneNode(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	nodeName string) (*NodeDeletionResult, error) {
	result := &NodeDeletionResult{NodeName: nodeName}

	// Refuse unsafe deletions (last control plane, unknown node) before
	// touching anything.
	controlPlanes, targetIP, err := t.validateControlPlaneDeletion(ctx, cluster, nodeName)
	if err != nil {
		result.Error = err
		return result, err
	}

	// Drain before the costlier preparation below (loading Talos artifacts,
	// probing control planes for a healthy etcd peer): while a PDB holds the
	// drain, this returns on every pass, and the node must be empty before it
	// leaves etcd anyway.
	if err := t.drainWorkloadNode(ctx, cluster, nodeName); errors.Is(err, ErrDrainIncomplete) {
		result.Error = err
		return result, err
	}

	// Prepare deletion context
	deleteCtx, err := t.prepareControlPlaneDeletion(ctx, cluster, nodeName, controlPlanes, targetIP)
	if err != nil {
		result.Error = err
		return result, err
	}

	// Execute deletion steps
	if err := t.executeControlPlaneDeletion(ctx, cluster, nodeName, deleteCtx, result); err != nil {
		return result, err
	}

	result.Success = true
	vlog.Info(fmt.Sprintf("Successfully deleted control plane node: node=%s %s", nodeName, clusterLogTag(cluster)))
	return result, nil
}

// controlPlaneDeletionContext holds context needed for control plane deletion
type controlPlaneDeletionContext struct {
	targetIP     string
	healthyCP    string
	clientConfig *clientconfig.Config
}

// validateControlPlaneDeletion checks that nodeName is a known control plane
// with an IP and not the last one, and returns the control planes and its IP.
func (t *TalosManager) validateControlPlaneDeletion(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	nodeName string) ([]*vitistackv1alpha1.Machine, string, error) {
	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get cluster machines: %w", err)
	}

	controlPlanes := t.machineService.FilterMachinesByRole(machines, controlPlaneRole)

	if len(controlPlanes) <= 1 {
		return nil, "", fmt.Errorf("cannot delete control plane: only %d control plane(s) remaining, minimum 1 required", len(controlPlanes))
	}

	// Find the target machine and its IP
	targetIP := t.findTargetMachineIP(controlPlanes, nodeName)
	if targetIP == "" {
		return nil, "", fmt.Errorf("control plane node %s not found or has no IP address", nodeName)
	}
	return controlPlanes, targetIP, nil
}

// drainWorkloadNode makes one scale-down drain pass over nodeName. An error
// wrapping ErrDrainIncomplete means the node is not drained yet (or the
// workload cluster cannot be reached); a plain error means there is nothing to
// drain through, and the deletion steps handle it as before.
func (t *TalosManager) drainWorkloadNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	clientset, err := t.drainClient(ctx, cluster, nodeName, drainservice.PurposeScaleDown)
	if err != nil {
		return err
	}
	return t.drainNode(ctx, clientset, cluster, nodeName, drainservice.PurposeScaleDown)
}

// prepareControlPlaneDeletion prepares the context needed for control plane deletion
func (t *TalosManager) prepareControlPlaneDeletion(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	nodeName string,
	controlPlanes []*vitistackv1alpha1.Machine,
	targetIP string) (*controlPlaneDeletionContext, error) {
	// Load Talos artifacts
	clientConfig, _, err := t.loadTalosArtifacts(ctx, cluster)
	if err != nil || clientConfig == nil {
		return nil, fmt.Errorf("failed to load talos artifacts: %w", err)
	}

	// Find a healthy control plane for etcd operations
	healthyCP := t.findHealthyControlPlaneExcluding(controlPlanes, nodeName)
	if healthyCP == "" {
		return nil, fmt.Errorf("no healthy control plane found for etcd operations")
	}

	return &controlPlaneDeletionContext{
		targetIP:     targetIP,
		healthyCP:    healthyCP,
		clientConfig: clientConfig,
	}, nil
}

// findTargetMachineIP finds the IP of the target machine by name
func (t *TalosManager) findTargetMachineIP(controlPlanes []*vitistackv1alpha1.Machine, nodeName string) string {
	for _, cp := range controlPlanes {
		if cp.Name == nodeName {
			return getFirstIPv4(cp)
		}
	}
	return ""
}

// findHealthyControlPlaneExcluding finds a healthy control plane excluding the specified node
func (t *TalosManager) findHealthyControlPlaneExcluding(controlPlanes []*vitistackv1alpha1.Machine, excludeNode string) string {
	for _, cp := range controlPlanes {
		if cp.Name == excludeNode {
			continue
		}
		ip := getFirstIPv4(cp)
		if ip != "" && t.clientService.IsTalosAPIReachable(ip) {
			return ip
		}
	}
	return ""
}

// executeControlPlaneDeletion executes the control plane deletion steps
func (t *TalosManager) executeControlPlaneDeletion(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	nodeName string,
	deleteCtx *controlPlaneDeletionContext,
	result *NodeDeletionResult) error {
	// Step 1: Delete the Kubernetes node from the workload cluster. It was
	// drained in DeleteControlPlaneNode, before the etcd preparation.
	if err := t.deleteWorkloadNode(ctx, cluster, nodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to delete Kubernetes node from workload cluster %s/%s: %v", clusterLogTag(cluster), nodeName, err))
		// Continue - the node might not exist in K8s yet
	} else {
		result.K8sNodeDeleted = true
	}

	// Step 2: Forfeit etcd leadership if the node is the leader
	vlog.Info(fmt.Sprintf("Forfeiting etcd leadership if node is leader: node=%s %s", nodeName, clusterLogTag(cluster)))
	if err := t.etcdService.ForfeitLeadership(ctx, deleteCtx.clientConfig, deleteCtx.targetIP); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to forfeit etcd leadership %s/%s (may not be leader): %v", clusterLogTag(cluster), nodeName, err))
	}

	// Step 3: Remove from etcd cluster
	if err := t.removeFromEtcdCluster(ctx, cluster, nodeName, deleteCtx, result); err != nil {
		return err
	}

	// Step 4: Update VIP pool members
	if err := t.removeNodeFromVIPPool(ctx, cluster, deleteCtx.targetIP); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to update VIP pool members %s: %v", clusterLogTag(cluster), err))
	} else {
		result.VIPUpdated = true
	}

	// Step 5: Remove from configured nodes list (internal tracking)
	if err := t.stateService.RemoveConfiguredNode(ctx, cluster, nodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to remove from configured nodes %s/%s: %v", clusterLogTag(cluster), nodeName, err))
	} else {
		result.ConfigRemoved = true
	}

	return nil
}

// removeFromEtcdCluster removes the node from the etcd cluster
func (t *TalosManager) removeFromEtcdCluster(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	nodeName string,
	deleteCtx *controlPlaneDeletionContext,
	result *NodeDeletionResult) error {
	members, err := t.etcdService.MemberList(ctx, deleteCtx.clientConfig, deleteCtx.healthyCP)
	if err != nil {
		return fmt.Errorf("failed to list etcd members: %w", err)
	}

	// Try to find member by hostname first, then by IP
	member := etcdservice.FindMemberByHostname(members, nodeName)
	if member == nil {
		member = etcdservice.FindMemberByIP(members, deleteCtx.targetIP)
	}

	if member == nil {
		vlog.Warn(fmt.Sprintf("Etcd member not found for node %s/%s, may already be removed", clusterLogTag(cluster), nodeName))
		return nil
	}

	vlog.Info(fmt.Sprintf("Removing etcd member: node=%s memberID=%d %s", nodeName, member.ID, clusterLogTag(cluster)))
	if err := t.etcdService.RemoveMemberByID(ctx, deleteCtx.clientConfig, deleteCtx.healthyCP, member.ID); err != nil {
		return fmt.Errorf("failed to remove etcd member: %w", err)
	}
	result.EtcdRemoved = true
	vlog.Info(fmt.Sprintf("Successfully removed etcd member: node=%s memberID=%d %s", nodeName, member.ID, clusterLogTag(cluster)))
	return nil
}

// DeleteWorkerNode explicitly deletes a worker node by name.
// It first cordons and drains the node (waiting for pods to evacuate),
// then deletes the node from Kubernetes, and finally removes it from internal tracking.
func (t *TalosManager) DeleteWorkerNode(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	nodeName string) (*NodeDeletionResult, error) {
	result := &NodeDeletionResult{NodeName: nodeName}

	// Step 1: Cordon, drain, and delete the Kubernetes node from the workload cluster
	// This must happen FIRST to ensure pods are safely evacuated before the machine is deleted
	if err := t.deleteK8sNodeFromWorkloadCluster(ctx, cluster, nodeName); err != nil {
		if errors.Is(err, ErrDrainIncomplete) {
			// The node is not drained yet (pods still being evicted, or held
			// by a PDB): stop here and retry on a later reconcile instead of
			// removing the node anyway.
			result.Error = err
			return result, err
		}
		vlog.Warn(fmt.Sprintf("Failed to delete Kubernetes node from workload cluster %s/%s: %v", clusterLogTag(cluster), nodeName, err))
		// Continue - the node might not exist in K8s yet
	} else {
		result.K8sNodeDeleted = true
	}

	// Step 2: Remove from configured nodes list (internal tracking)
	if err := t.stateService.RemoveConfiguredNode(ctx, cluster, nodeName); err != nil {
		result.Error = fmt.Errorf("failed to remove worker node from configured nodes: %w", err)
		return result, result.Error
	}
	result.ConfigRemoved = true

	result.Success = true
	vlog.Info(fmt.Sprintf("Successfully deleted worker node: node=%s %s", nodeName, clusterLogTag(cluster)))
	return result, nil
}

// ErrDrainIncomplete marks a node operation held because the node is not
// drained yet: pods are still being evicted, or a PodDisruptionBudget keeps
// rejecting their eviction. The NodeDrain condition on the KubernetesCluster
// names the pods. drainservice.RetryAfter(err) says when to try again.
var ErrDrainIncomplete = drainservice.ErrIncomplete

// drainOptionsFor returns the drain options for a cluster; a variable so tests
// can set the clock and timeout.
var drainOptionsFor = drainservice.OptionsForCluster

// deleteK8sNodeFromWorkloadCluster drains a node and deletes it from the workload cluster's API.
// The drain is non-blocking: until it completes, an error wrapping ErrDrainIncomplete is
// returned, the node stays cordoned, and callers must not go on to remove the machine.
// A plain error means there is nothing to drain through (no kubeconfig yet, or force-drain
// let an unreachable cluster pass) and the caller may continue.
func (t *TalosManager) deleteK8sNodeFromWorkloadCluster(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	clientset, err := t.drainClient(ctx, cluster, nodeName, drainservice.PurposeScaleDown)
	if err != nil {
		return err
	}
	return t.drainAndDeleteNode(ctx, clientset, cluster, nodeName)
}

func (t *TalosManager) drainAndDeleteNode(ctx context.Context, clientset kubernetes.Interface, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	// Step 1: Cordon and drain through the Eviction API, honouring PDBs
	if err := t.drainNode(ctx, clientset, cluster, nodeName, drainservice.PurposeScaleDown); err != nil {
		return err
	}

	// Step 2: Delete the node from the workload cluster
	return deleteNodeObject(ctx, clientset, cluster, nodeName)
}

// deleteWorkloadNode deletes an already drained node from the workload cluster.
func (t *TalosManager) deleteWorkloadNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	clientset, err := t.workloadClientset(ctx, cluster)
	if err != nil {
		return err
	}
	if clientset == nil {
		return fmt.Errorf("kubeconfig not available for cluster %s", cluster.Name)
	}
	return deleteNodeObject(ctx, clientset, cluster, nodeName)
}

func deleteNodeObject(ctx context.Context, clientset kubernetes.Interface, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	err := clientset.CoreV1().Nodes().Delete(ctx, nodeName, metav1.DeleteOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			vlog.Info(fmt.Sprintf("Kubernetes node %s already deleted from workload cluster %s", nodeName, clusterLogTag(cluster)))
			return nil
		}
		return fmt.Errorf("failed to delete node %s from workload cluster: %w", nodeName, err)
	}

	vlog.Info(fmt.Sprintf("Successfully deleted Kubernetes node from workload cluster: node=%s %s", nodeName, clusterLogTag(cluster)))
	return nil
}

// drainClient returns a workload-cluster client for draining nodeName. When
// the cluster cannot be reached it reports NodeDrain=Failed and returns an
// error wrapping ErrDrainIncomplete, so the operation is held rather than
// carried out without a drain (unless force-drain is set). A plain error means
// there is nothing to drain through: the cluster has no kubeconfig yet (so no
// workloads), or force-drain let an unreachable cluster pass.
func (t *TalosManager) drainClient(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string, purpose drainservice.Purpose) (kubernetes.Interface, error) {
	clientset, err := t.workloadClientset(ctx, cluster)
	if err != nil {
		opts := drainOptionsFor(cluster, purpose)
		if holdErr := drainservice.ReportUnavailable(ctx, t.drainConditions(), cluster, nodeName, opts, err); holdErr != nil {
			return nil, holdErr
		}
		return nil, fmt.Errorf("workload cluster unreachable, continuing without drain because of force-drain: %w", err)
	}
	if clientset == nil {
		return nil, fmt.Errorf("kubeconfig not available for cluster %s", cluster.Name)
	}
	return clientset, nil
}

// CordonAndDrainNode cordons and drains a node in preparation for maintenance (like upgrades).
// It does NOT delete the node from Kubernetes. It makes one non-blocking drain pass: an error
// wrapping ErrDrainIncomplete means the node is not drained yet and the maintenance must wait.
func (t *TalosManager) CordonAndDrainNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	clientset, err := t.drainClient(ctx, cluster, nodeName, drainservice.PurposeUpgrade)
	if err != nil {
		return err
	}
	return t.drainNode(ctx, clientset, cluster, nodeName, drainservice.PurposeUpgrade)
}

// drainNode makes one pass of the shared PDB-respecting drain and reports it
// as the NodeDrain condition on the cluster.
func (t *TalosManager) drainNode(ctx context.Context, clientset kubernetes.Interface, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string, purpose drainservice.Purpose) error {
	return drainservice.Run(ctx, clientset, t.drainConditions(), cluster, nodeName, drainOptionsFor(cluster, purpose))
}

// drainConditions returns the status manager as a condition setter, or a nil
// interface when there is none.
func (t *TalosManager) drainConditions() drainservice.ConditionSetter {
	if t.statusManager == nil {
		return nil
	}
	return t.statusManager
}

// UncordonNode marks a node as schedulable again after maintenance and clears its drain markers.
func (t *TalosManager) UncordonNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	clientset, err := t.workloadClientset(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get workload cluster client: %w", err)
	}
	if clientset == nil {
		return fmt.Errorf("kubeconfig not available for cluster %s", cluster.Name)
	}
	if err := drainservice.Uncordon(ctx, clientset, nodeName); err != nil {
		return err
	}
	vlog.Info(fmt.Sprintf("Uncordoned node: %s %s", nodeName, clusterLogTag(cluster)))
	return nil
}

// ReleaseAbandonedDrains uncordons workload nodes still cordoned by a drain
// for one of purposes, then reports NodeDrain=True/Idle once every purpose has
// been released. The caller passes only the purposes no operation is using
// (no scale-down pending, no upgrade plan), so such a node was left by an
// operation that was called off: a scale-down reverted, an upgrade reset.
// The node label is the source of truth; the lookup is one label-filtered
// Node list, next to the full Node list a settled cluster makes every pass.
func (t *TalosManager) ReleaseAbandonedDrains(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, purposes ...drainservice.Purpose) error {
	if len(purposes) == 0 {
		return nil
	}
	clientset, err := t.workloadClientset(ctx, cluster)
	if err != nil || clientset == nil {
		return err
	}
	return t.releaseAbandonedDrains(ctx, clientset, cluster, purposes...)
}

func (t *TalosManager) releaseAbandonedDrains(ctx context.Context, clientset kubernetes.Interface, cluster *vitistackv1alpha1.KubernetesCluster, purposes ...drainservice.Purpose) error {
	if _, err := drainservice.ReleaseAbandoned(ctx, clientset, purposes...); err != nil {
		return err
	}
	if slices.Contains(purposes, drainservice.PurposeScaleDown) && slices.Contains(purposes, drainservice.PurposeUpgrade) {
		drainservice.ReportIdle(ctx, t.drainConditions(), cluster)
	}
	return nil
}

// IsNodeSchedulable checks if a node is schedulable (not cordoned).
func (t *TalosManager) IsNodeSchedulable(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) (bool, error) {
	clientset, err := t.getWorkloadClusterClient(ctx, cluster)
	if err != nil {
		return false, fmt.Errorf("failed to get workload cluster client: %w", err)
	}
	if clientset == nil {
		return false, fmt.Errorf("kubeconfig not available for cluster %s", cluster.Name)
	}

	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil // Node doesn't exist
		}
		return false, fmt.Errorf("failed to get node %s: %w", nodeName, err)
	}

	// A node is schedulable if Unschedulable is false
	return !node.Spec.Unschedulable, nil
}

// workloadClientset returns a client for the workload cluster used by drains,
// or a nil interface when no kubeconfig exists yet.
func (t *TalosManager) workloadClientset(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (kubernetes.Interface, error) {
	if t.workloadClient != nil {
		return t.workloadClient(ctx, cluster)
	}
	cs, err := t.getWorkloadClusterClient(ctx, cluster)
	if err != nil || cs == nil {
		return nil, err
	}
	return cs, nil
}

// getWorkloadClusterClient creates a Kubernetes client for the workload cluster
func (t *TalosManager) getWorkloadClusterClient(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*kubernetes.Clientset, error) {
	secret, err := t.secretService.GetTalosSecret(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get talos secret: %w", err)
	}

	kubeconfig, ok := secret.Data["kube.config"]
	if !ok || len(kubeconfig) == 0 {
		return nil, nil // No kubeconfig yet
	}

	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST config from kubeconfig: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	return clientset, nil
}

// getClusterMachineNames returns a set of machine names for the cluster
func (t *TalosManager) getClusterMachineNames(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (map[string]bool, error) {
	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster machines: %w", err)
	}

	machineNames := make(map[string]bool)
	for _, m := range machines {
		machineNames[m.Name] = true
	}
	return machineNames, nil
}

// isNodeOrphaned checks if a node is NotReady and Unschedulable
func isNodeOrphaned(node *corev1.Node) bool {
	if !node.Spec.Unschedulable {
		return false
	}

	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status != corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// removeNodeFromVIPPool removes a specific IP from the VIP pool members.
func (t *TalosManager) removeNodeFromVIPPool(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, ipToRemove string) error {
	// Only update VIP pool members if using networkconfiguration endpoint mode
	endpointMode := consts.EndpointMode(viper.GetString(consts.ENDPOINT_MODE))
	if endpointMode != consts.EndpointModeNetworkConfiguration {
		vlog.Info(fmt.Sprintf("Skipping VIP pool member removal %s: endpoint mode is '%s', not 'networkconfiguration'", clusterLogTag(cluster), endpointMode))
		return nil
	}

	vipName := cluster.Spec.Cluster.ClusterId
	vip := &vitistackv1alpha1.ControlPlaneVirtualSharedIP{}
	if err := t.Get(ctx, types.NamespacedName{Name: vipName, Namespace: cluster.Namespace}, vip); err != nil {
		if apierrors.IsNotFound(err) {
			vlog.Warn(fmt.Sprintf("VIP %s not found %s, skipping pool member removal", vipName, clusterLogTag(cluster)))
			return nil
		}
		return err
	}

	// Filter out the IP to remove
	var newPoolMembers []string
	for _, ip := range vip.Spec.PoolMembers {
		if ip != ipToRemove {
			newPoolMembers = append(newPoolMembers, ip)
		}
	}

	if len(newPoolMembers) != len(vip.Spec.PoolMembers) {
		vip.Spec.PoolMembers = newPoolMembers
		if err := t.Update(ctx, vip); err != nil {
			return fmt.Errorf("failed to update VIP pool members: %w", err)
		}
		vlog.Info(fmt.Sprintf("Removed IP from VIP pool members %s: vip=%s removedIP=%s remaining=%v", clusterLogTag(cluster), vipName, ipToRemove, newPoolMembers))
	}

	return nil
}

// GetControlPlanesForDeletion returns control planes sorted in deletion order.
// When scaling down, control planes should be deleted from highest index first.
// Returns a list of control plane machines sorted by deletion priority (last added first).
func (t *TalosManager) GetControlPlanesForDeletion(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	countToDelete int) ([]*vitistackv1alpha1.Machine, error) {
	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster machines: %w", err)
	}

	controlPlanes := t.machineService.FilterMachinesByRole(machines, controlPlaneRole)

	if len(controlPlanes) == 0 {
		return nil, fmt.Errorf("no control planes found")
	}

	// Cannot delete all control planes
	if countToDelete >= len(controlPlanes) {
		return nil, fmt.Errorf("cannot delete %d control planes: only %d exist, minimum 1 required", countToDelete, len(controlPlanes))
	}

	// Sort by creation timestamp (newest first for deletion)
	sort.Slice(controlPlanes, func(i, j int) bool {
		return controlPlanes[i].CreationTimestamp.After(controlPlanes[j].CreationTimestamp.Time)
	})

	return controlPlanes[:countToDelete], nil
}

// ScaleDownControlPlanes scales down control planes by the specified count.
// Deletes control planes one at a time, starting with the most recently created.
// Returns results for each deletion attempt.
func (t *TalosManager) ScaleDownControlPlanes(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	countToDelete int) ([]*NodeDeletionResult, error) {
	toDelete, err := t.GetControlPlanesForDeletion(ctx, cluster, countToDelete)
	if err != nil {
		return nil, err
	}

	results := make([]*NodeDeletionResult, 0, len(toDelete))

	// Delete one at a time to ensure cluster stability
	for _, cp := range toDelete {
		result, err := t.DeleteControlPlaneNode(ctx, cluster, cp.Name)
		results = append(results, result)
		if err != nil {
			// Stop on first error for safety
			return results, fmt.Errorf("failed to delete control plane %s: %w", cp.Name, err)
		}
	}

	return results, nil
}

// containsIgnoreCase checks if s contains substr (case-insensitive)
func containsIgnoreCase(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsIgnoreCase(s[1:], substr) || len(s) > 0 && containsIgnoreCasePrefix(s, substr))
}

func containsIgnoreCasePrefix(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	for i := 0; i < len(prefix); i++ {
		c1, c2 := s[i], prefix[i]
		if c1 >= 'A' && c1 <= 'Z' {
			c1 += 'a' - 'A'
		}
		if c2 >= 'A' && c2 <= 'Z' {
			c2 += 'a' - 'A'
		}
		if c1 != c2 {
			return false
		}
	}
	return true
}
