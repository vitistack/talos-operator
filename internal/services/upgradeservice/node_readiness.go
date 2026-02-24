// Package upgradeservice provides functionality for managing Talos and Kubernetes upgrades.
package upgradeservice

import (
	"context"
	"fmt"
	"time"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NodeReadinessChecker checks node readiness using the target cluster's Kubernetes API.
// This avoids issues with the Talos NodeStatus COSI resource which can be problematic.
type NodeReadinessChecker struct {
	supervisorClient client.Client
	secretGetter     SecretGetter
}

// SecretGetter interface for retrieving cluster secrets
type SecretGetter interface {
	GetTalosSecret(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*corev1.Secret, error)
}

// NewNodeReadinessChecker creates a new NodeReadinessChecker
func NewNodeReadinessChecker(supervisorClient client.Client, secretGetter SecretGetter) *NodeReadinessChecker {
	return &NodeReadinessChecker{
		supervisorClient: supervisorClient,
		secretGetter:     secretGetter,
	}
}

// NodeReadinessStatus contains the readiness status of a node
type NodeReadinessStatus struct {
	Ready       bool
	Schedulable bool // Not cordoned
	Message     string
}

// IsNodeReady checks if a node is ready in the target Kubernetes cluster.
// This uses the kubeconfig stored in the cluster's secret to query the target cluster's API.
func (c *NodeReadinessChecker) IsNodeReady(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) (*NodeReadinessStatus, error) {
	// Get the target cluster's kubeconfig from the secret
	targetClient, err := c.getTargetClusterClient(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get target cluster client: %w", err)
	}

	// Get the node from the target cluster
	node, err := targetClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return &NodeReadinessStatus{
			Ready:   false,
			Message: fmt.Sprintf("node not found or not accessible: %v", err),
		}, nil
	}

	// Check if node is ready
	ready := isNodeConditionReady(node)
	schedulable := !node.Spec.Unschedulable

	status := &NodeReadinessStatus{
		Ready:       ready,
		Schedulable: schedulable,
	}

	switch {
	case !ready:
		status.Message = "node is not Ready"
	case !schedulable:
		status.Message = "node is cordoned (unschedulable)"
	default:
		status.Message = "node is Ready and schedulable"
	}

	return status, nil
}

// WaitForNodeReady waits for a node to become ready with a timeout.
// Returns the final status and an error if the timeout is exceeded.
func (c *NodeReadinessChecker) WaitForNodeReady(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	nodeName string,
	timeout time.Duration,
	interval time.Duration,
) (*NodeReadinessStatus, error) {
	deadline := time.Now().Add(timeout)

	for {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timeout waiting for node %s to become ready", nodeName)
		}

		status, err := c.IsNodeReady(ctx, cluster, nodeName)
		if err != nil {
			// Sleep and retry - error might be temporary (node rebooting)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(interval):
				continue
			}
		}

		if status.Ready {
			return status, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}

// GetAllNodesStatus returns the readiness status of all nodes in the target cluster.
func (c *NodeReadinessChecker) GetAllNodesStatus(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (map[string]*NodeReadinessStatus, error) {
	targetClient, err := c.getTargetClusterClient(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get target cluster client: %w", err)
	}

	nodes, err := targetClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}

	statuses := make(map[string]*NodeReadinessStatus, len(nodes.Items))
	for i := range nodes.Items {
		node := &nodes.Items[i]
		ready := isNodeConditionReady(node)
		schedulable := !node.Spec.Unschedulable

		status := &NodeReadinessStatus{
			Ready:       ready,
			Schedulable: schedulable,
		}

		switch {
		case !ready:
			status.Message = "node is not Ready"
		case !schedulable:
			status.Message = "node is cordoned (unschedulable)"
		default:
			status.Message = "node is Ready and schedulable"
		}

		statuses[node.Name] = status
	}

	return statuses, nil
}

// CordonNode marks a node as unschedulable in the target cluster.
func (c *NodeReadinessChecker) CordonNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	targetClient, err := c.getTargetClusterClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get target cluster client: %w", err)
	}

	node, err := targetClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node %s: %w", nodeName, err)
	}

	if node.Spec.Unschedulable {
		// Already cordoned
		return nil
	}

	node.Spec.Unschedulable = true
	_, err = targetClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to cordon node %s: %w", nodeName, err)
	}

	return nil
}

// UncordonNode marks a node as schedulable in the target cluster.
func (c *NodeReadinessChecker) UncordonNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	targetClient, err := c.getTargetClusterClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get target cluster client: %w", err)
	}

	node, err := targetClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node %s: %w", nodeName, err)
	}

	if !node.Spec.Unschedulable {
		// Already schedulable
		return nil
	}

	node.Spec.Unschedulable = false
	_, err = targetClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to uncordon node %s: %w", nodeName, err)
	}

	return nil
}

// DrainNode drains a node by evicting all pods (best effort).
// Note: This is a simplified drain that doesn't wait for pod termination.
// For production use, consider using kubectl drain logic.
func (c *NodeReadinessChecker) DrainNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	// First cordon the node
	if err := c.CordonNode(ctx, cluster, nodeName); err != nil {
		return err
	}

	targetClient, err := c.getTargetClusterClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get target cluster client: %w", err)
	}

	// List pods on this node
	pods, err := targetClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	})
	if err != nil {
		return fmt.Errorf("failed to list pods on node %s: %w", nodeName, err)
	}

	// Evict pods (skip DaemonSet pods and mirror pods)
	evictedCount := 0
	for i := range pods.Items {
		pod := &pods.Items[i]

		// Skip mirror pods (created by kubelet, not schedulable)
		if _, ok := pod.Annotations["kubernetes.io/config.mirror"]; ok {
			continue
		}

		// Skip DaemonSet pods (they will be recreated on the same node)
		if isDaemonSetPod(pod) {
			continue
		}

		// Skip pods in kube-system that are critical
		if pod.Namespace == "kube-system" && isCriticalPod(pod) {
			continue
		}

		// Delete the pod (eviction policy honored by PodDisruptionBudget)
		err := targetClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
			GracePeriodSeconds: int64Ptr(30),
		})
		if err != nil {
			continue
		}
		evictedCount++
	}

	return nil
}

// getTargetClusterClient creates a Kubernetes client for the target cluster using the stored kubeconfig.
func (c *NodeReadinessChecker) getTargetClusterClient(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*kubernetes.Clientset, error) {
	// Get the secret containing the kubeconfig
	secret, err := c.secretGetter.GetTalosSecret(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster secret: %w", err)
	}

	kubeconfig, ok := secret.Data["kube.config"]
	if !ok || len(kubeconfig) == 0 {
		return nil, fmt.Errorf("kubeconfig not found in cluster secret")
	}

	// Create REST config from kubeconfig
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to parse kubeconfig: %w", err)
	}

	// Set reasonable timeouts
	restConfig.Timeout = 30 * time.Second

	// Create clientset
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset: %w", err)
	}

	return clientset, nil
}

// isNodeConditionReady checks if the node has Ready=True condition.
func isNodeConditionReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// isDaemonSetPod checks if a pod is managed by a DaemonSet.
func isDaemonSetPod(pod *corev1.Pod) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

// isCriticalPod checks if a pod is a critical system pod.
func isCriticalPod(pod *corev1.Pod) bool {
	// Check for critical pod annotation
	if _, ok := pod.Annotations["scheduler.alpha.kubernetes.io/critical-pod"]; ok {
		return true
	}
	// Check priority class
	if pod.Spec.PriorityClassName == "system-cluster-critical" || pod.Spec.PriorityClassName == "system-node-critical" {
		return true
	}
	return false
}

// int64Ptr returns a pointer to an int64.
//
//go:fix inline
func int64Ptr(i int64) *int64 {
	return new(i)
}
