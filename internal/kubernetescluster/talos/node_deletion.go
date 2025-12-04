package talos

import (
	"context"
	"fmt"
	"sort"

	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/etcdservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
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
	for _, nodeName := range configuredNodes {
		if !currentMachines[nodeName] {
			nodesToRemove = append(nodesToRemove, nodeName)
		}
	}

	if len(nodesToRemove) == 0 {
		return nil
	}

	vlog.Info(fmt.Sprintf("Found %d nodes to remove from cluster %s: %v", len(nodesToRemove), cluster.Name, nodesToRemove))

	// Load Talos artifacts for deletion operations
	clientConfig, _, err := t.loadTalosArtifacts(ctx, cluster)
	if err != nil || clientConfig == nil {
		return fmt.Errorf("failed to load talos artifacts for node deletion: %w", err)
	}

	// Process removals
	for _, nodeName := range nodesToRemove {
		if err := t.removeNode(ctx, cluster, clientConfig, nodeName); err != nil {
			vlog.Error(fmt.Sprintf("Failed to remove node %s: %v", nodeName, err), err)
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
	vlog.Info(fmt.Sprintf("Removing control plane node from cluster: node=%s cluster=%s", nodeName, cluster.Name))

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
		vlog.Warn(fmt.Sprintf("Failed to list etcd members, continuing with removal: %v", err))
	} else {
		// Find the etcd member for this node
		member := etcdservice.FindMemberByHostname(members, nodeName)
		if member != nil {
			// Remove from etcd
			if err := t.etcdService.RemoveMemberByID(ctx, clientConfig, healthyCP, member.ID); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to remove etcd member %s (ID: %d): %v", nodeName, member.ID, err))
				// Continue anyway - the member may already be gone
			} else {
				vlog.Info(fmt.Sprintf("Removed etcd member: node=%s memberID=%d", nodeName, member.ID))
			}
		} else {
			vlog.Info(fmt.Sprintf("Etcd member not found for node %s, may already be removed", nodeName))
		}
	}

	// Update VIP pool members
	if err := t.updateVIPPoolMembers(ctx, cluster); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to update VIP pool members: %v", err))
		// Continue anyway
	}

	// Remove from configured nodes list
	if err := t.stateService.RemoveConfiguredNode(ctx, cluster, nodeName); err != nil {
		return fmt.Errorf("failed to remove node from configured nodes: %w", err)
	}

	vlog.Info(fmt.Sprintf("Successfully removed control plane node: node=%s cluster=%s", nodeName, cluster.Name))
	return nil
}

// removeWorkerNode handles removal of a worker node.
// This is simpler than control plane removal as it only needs to:
// 1. Remove from configured nodes list
func (t *TalosManager) removeWorkerNode(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	nodeName string) error {
	vlog.Info(fmt.Sprintf("Removing worker node from cluster: node=%s cluster=%s", nodeName, cluster.Name))

	// Remove from configured nodes list
	if err := t.stateService.RemoveConfiguredNode(ctx, cluster, nodeName); err != nil {
		return fmt.Errorf("failed to remove worker node from configured nodes: %w", err)
	}

	vlog.Info(fmt.Sprintf("Successfully removed worker node: node=%s cluster=%s", nodeName, cluster.Name))
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

	// Prepare deletion context
	deleteCtx, err := t.prepareControlPlaneDeletion(ctx, cluster, nodeName)
	if err != nil {
		result.Error = err
		return result, err
	}

	// Execute deletion steps
	if err := t.executeControlPlaneDeletion(ctx, cluster, nodeName, deleteCtx, result); err != nil {
		return result, err
	}

	result.Success = true
	vlog.Info(fmt.Sprintf("Successfully deleted control plane node: node=%s cluster=%s", nodeName, cluster.Name))
	return result, nil
}

// controlPlaneDeletionContext holds context needed for control plane deletion
type controlPlaneDeletionContext struct {
	targetIP     string
	healthyCP    string
	clientConfig *clientconfig.Config
}

// prepareControlPlaneDeletion prepares the context needed for control plane deletion
func (t *TalosManager) prepareControlPlaneDeletion(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	nodeName string) (*controlPlaneDeletionContext, error) {
	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster machines: %w", err)
	}

	controlPlanes := t.machineService.FilterMachinesByRole(machines, controlPlaneRole)

	if len(controlPlanes) <= 1 {
		return nil, fmt.Errorf("cannot delete control plane: only %d control plane(s) remaining, minimum 1 required", len(controlPlanes))
	}

	// Find the target machine and its IP
	targetIP := t.findTargetMachineIP(controlPlanes, nodeName)
	if targetIP == "" {
		return nil, fmt.Errorf("control plane node %s not found or has no IP address", nodeName)
	}

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
	// Step 1: Forfeit etcd leadership if the node is the leader
	vlog.Info(fmt.Sprintf("Forfeiting etcd leadership if node is leader: node=%s", nodeName))
	if err := t.etcdService.ForfeitLeadership(ctx, deleteCtx.clientConfig, deleteCtx.targetIP); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to forfeit etcd leadership (may not be leader): %v", err))
	}

	// Step 2: Remove from etcd cluster
	if err := t.removeFromEtcdCluster(ctx, nodeName, deleteCtx, result); err != nil {
		return err
	}

	// Step 3: Update VIP pool members
	if err := t.removeNodeFromVIPPool(ctx, cluster, deleteCtx.targetIP); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to update VIP pool members: %v", err))
	} else {
		result.VIPUpdated = true
	}

	// Step 4: Remove from configured nodes list
	if err := t.stateService.RemoveConfiguredNode(ctx, cluster, nodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to remove from configured nodes: %v", err))
	} else {
		result.ConfigRemoved = true
	}

	// Step 5: Delete the Kubernetes node from the workload cluster
	if err := t.deleteK8sNodeFromWorkloadCluster(ctx, cluster, nodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to delete Kubernetes node from workload cluster: %v", err))
	} else {
		result.K8sNodeDeleted = true
	}

	return nil
}

// removeFromEtcdCluster removes the node from the etcd cluster
func (t *TalosManager) removeFromEtcdCluster(
	ctx context.Context,
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
		vlog.Warn(fmt.Sprintf("Etcd member not found for node %s, may already be removed", nodeName))
		return nil
	}

	vlog.Info(fmt.Sprintf("Removing etcd member: node=%s memberID=%d", nodeName, member.ID))
	if err := t.etcdService.RemoveMemberByID(ctx, deleteCtx.clientConfig, deleteCtx.healthyCP, member.ID); err != nil {
		return fmt.Errorf("failed to remove etcd member: %w", err)
	}
	result.EtcdRemoved = true
	vlog.Info(fmt.Sprintf("Successfully removed etcd member: node=%s memberID=%d", nodeName, member.ID))
	return nil
}

// DeleteWorkerNode explicitly deletes a worker node by name.
func (t *TalosManager) DeleteWorkerNode(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	nodeName string) (*NodeDeletionResult, error) {
	result := &NodeDeletionResult{NodeName: nodeName}

	// Remove from configured nodes list
	if err := t.stateService.RemoveConfiguredNode(ctx, cluster, nodeName); err != nil {
		result.Error = fmt.Errorf("failed to remove worker node from configured nodes: %w", err)
		return result, result.Error
	}
	result.ConfigRemoved = true

	// Delete the Kubernetes node from the workload cluster
	if err := t.deleteK8sNodeFromWorkloadCluster(ctx, cluster, nodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to delete Kubernetes node from workload cluster: %v", err))
	} else {
		result.K8sNodeDeleted = true
	}

	result.Success = true
	vlog.Info(fmt.Sprintf("Successfully deleted worker node: node=%s cluster=%s", nodeName, cluster.Name))
	return result, nil
}

// deleteK8sNodeFromWorkloadCluster cordons, drains, and deletes a Kubernetes node from the workload cluster's API.
func (t *TalosManager) deleteK8sNodeFromWorkloadCluster(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	clientset, err := t.getWorkloadClusterClient(ctx, cluster)
	if err != nil {
		return err
	}
	if clientset == nil {
		return fmt.Errorf("kubeconfig not available for cluster %s", cluster.Name)
	}

	// Step 1: Cordon the node (mark as unschedulable)
	if err := t.cordonNode(ctx, clientset, nodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to cordon node %s: %v", nodeName, err))
		// Continue even if cordon fails - node might not exist yet in K8s
	}

	// Step 2: Drain the node (evict pods)
	if err := t.drainNode(ctx, clientset, nodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to drain node %s: %v", nodeName, err))
		// Continue with deletion even if drain fails
	}

	// Step 3: Delete the node from the workload cluster
	err = clientset.CoreV1().Nodes().Delete(ctx, nodeName, metav1.DeleteOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			vlog.Info(fmt.Sprintf("Kubernetes node %s already deleted from workload cluster", nodeName))
			return nil
		}
		return fmt.Errorf("failed to delete node %s from workload cluster: %w", nodeName, err)
	}

	vlog.Info(fmt.Sprintf("Successfully deleted Kubernetes node from workload cluster: node=%s cluster=%s", nodeName, cluster.Name))
	return nil
}

// cordonNode marks a node as unschedulable
func (t *TalosManager) cordonNode(ctx context.Context, clientset *kubernetes.Clientset, nodeName string) error {
	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Node doesn't exist, nothing to cordon
		}
		return fmt.Errorf("failed to get node %s: %w", nodeName, err)
	}

	if node.Spec.Unschedulable {
		vlog.Debug(fmt.Sprintf("Node %s is already cordoned", nodeName))
		return nil
	}

	node.Spec.Unschedulable = true
	_, err = clientset.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to cordon node %s: %w", nodeName, err)
	}

	vlog.Info(fmt.Sprintf("Cordoned node: %s", nodeName))
	return nil
}

// drainNode evicts all pods from a node (except DaemonSet pods and mirror pods)
func (t *TalosManager) drainNode(ctx context.Context, clientset *kubernetes.Clientset, nodeName string) error {
	// List all pods on the node
	podList, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	})
	if err != nil {
		return fmt.Errorf("failed to list pods on node %s: %w", nodeName, err)
	}

	podsToEvict := make([]*corev1.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		// Skip DaemonSet pods
		if isDaemonSetPod(pod) {
			vlog.Debug(fmt.Sprintf("Skipping DaemonSet pod %s/%s during drain", pod.Namespace, pod.Name))
			continue
		}

		// Skip mirror pods (static pods)
		if isMirrorPod(pod) {
			vlog.Debug(fmt.Sprintf("Skipping mirror pod %s/%s during drain", pod.Namespace, pod.Name))
			continue
		}

		// Skip pods that are already terminating
		if pod.DeletionTimestamp != nil {
			continue
		}

		podsToEvict = append(podsToEvict, pod)
	}

	if len(podsToEvict) == 0 {
		vlog.Info(fmt.Sprintf("No pods to evict from node %s", nodeName))
		return nil
	}

	vlog.Info(fmt.Sprintf("Draining node %s: evicting %d pods", nodeName, len(podsToEvict)))

	// Evict pods
	for _, pod := range podsToEvict {
		if err := t.evictPod(ctx, clientset, pod); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to evict pod %s/%s: %v", pod.Namespace, pod.Name, err))
			// Continue evicting other pods
		}
	}

	return nil
}

// evictPod evicts a single pod using the Eviction API
func (t *TalosManager) evictPod(ctx context.Context, clientset *kubernetes.Clientset, pod *corev1.Pod) error {
	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
	}

	err := clientset.CoreV1().Pods(pod.Namespace).EvictV1(ctx, eviction)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Pod already gone
		}
		if apierrors.IsTooManyRequests(err) {
			// PodDisruptionBudget is preventing eviction - log and continue
			vlog.Warn(fmt.Sprintf("PDB blocking eviction of pod %s/%s, deleting directly", pod.Namespace, pod.Name))
			// Fallback to direct delete with grace period
			gracePeriod := int64(30)
			return clientset.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
				GracePeriodSeconds: &gracePeriod,
			})
		}
		return err
	}

	vlog.Debug(fmt.Sprintf("Evicted pod %s/%s", pod.Namespace, pod.Name))
	return nil
}

// isDaemonSetPod checks if a pod is managed by a DaemonSet
func isDaemonSetPod(pod *corev1.Pod) bool {
	for _, ownerRef := range pod.OwnerReferences {
		if ownerRef.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

// isMirrorPod checks if a pod is a mirror pod (static pod)
func isMirrorPod(pod *corev1.Pod) bool {
	_, exists := pod.Annotations["kubernetes.io/config.mirror"]
	return exists
}

// CleanupOrphanedK8sNodes finds and deletes Kubernetes nodes in the workload cluster
// that have no corresponding Machine CRD and are in NotReady state with SchedulingDisabled.
// This handles cases where the Machine was deleted but the K8s node wasn't cleaned up properly.
func (t *TalosManager) CleanupOrphanedK8sNodes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	// Get workload cluster clientset
	clientset, err := t.getWorkloadClusterClient(ctx, cluster)
	if err != nil {
		return err
	}
	if clientset == nil {
		// No kubeconfig yet, cluster not fully initialized
		return nil
	}

	// Get all nodes from the workload cluster
	nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list nodes from workload cluster: %w", err)
	}

	// Get machine names for this cluster
	machineNames, err := t.getClusterMachineNames(ctx, cluster)
	if err != nil {
		return err
	}

	// Find and delete orphaned nodes
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if err := t.deleteOrphanedNodeIfNeeded(ctx, clientset, node, machineNames, cluster.Name); err != nil {
			vlog.Error(fmt.Sprintf("Error processing node %s: %v", node.Name, err), err)
		}
	}

	return nil
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

// deleteOrphanedNodeIfNeeded checks if a node is orphaned (no Machine CRD, NotReady, Unschedulable) and deletes it
func (t *TalosManager) deleteOrphanedNodeIfNeeded(
	ctx context.Context,
	clientset *kubernetes.Clientset,
	node *corev1.Node,
	machineNames map[string]bool,
	clusterName string,
) error {
	nodeName := node.Name

	// Skip if there's a corresponding Machine
	if machineNames[nodeName] {
		return nil
	}

	// Check if node is orphaned: NotReady AND Unschedulable
	if !isNodeOrphaned(node) {
		return nil
	}

	vlog.Info(fmt.Sprintf("Found orphaned Kubernetes node (NotReady + SchedulingDisabled, no Machine CRD): node=%s cluster=%s",
		nodeName, clusterName))

	if err := clientset.CoreV1().Nodes().Delete(ctx, nodeName, metav1.DeleteOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete orphaned node %s: %w", nodeName, err)
	}

	vlog.Info(fmt.Sprintf("Successfully deleted orphaned Kubernetes node: node=%s cluster=%s", nodeName, clusterName))
	return nil
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
		vlog.Info(fmt.Sprintf("Skipping VIP pool member removal: endpoint mode is '%s', not 'networkconfiguration'", endpointMode))
		return nil
	}

	vipName := cluster.Spec.Cluster.ClusterId
	vip := &vitistackv1alpha1.ControlPlaneVirtualSharedIP{}
	if err := t.Get(ctx, types.NamespacedName{Name: vipName, Namespace: cluster.Namespace}, vip); err != nil {
		if apierrors.IsNotFound(err) {
			vlog.Warn(fmt.Sprintf("VIP %s not found, skipping pool member removal", vipName))
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
		vlog.Info(fmt.Sprintf("Removed IP from VIP pool members: vip=%s removedIP=%s remaining=%v", vipName, ipToRemove, newPoolMembers))
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
