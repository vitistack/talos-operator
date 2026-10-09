// Package upgradeservice provides functionality for managing Talos and Kubernetes upgrades.
package upgradeservice

import (
	"context"
	"fmt"
	"time"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/drainservice"
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
	// conditions receives the NodeDrain condition after each drain; may be nil.
	conditions drainservice.ConditionSetter
	// targetClient returns a client for the workload cluster; replaceable in tests.
	targetClient func(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (kubernetes.Interface, error)
	// drainOptions returns the drain options for a cluster; replaceable in tests.
	drainOptions func(cluster *vitistackv1alpha1.KubernetesCluster) drainservice.Options
}

// SecretGetter interface for retrieving cluster secrets
type SecretGetter interface {
	GetTalosSecret(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*corev1.Secret, error)
}

// NewNodeReadinessChecker creates a new NodeReadinessChecker. conditions may be
// nil; when set, drain outcomes are reported as the NodeDrain condition.
func NewNodeReadinessChecker(supervisorClient client.Client, secretGetter SecretGetter, conditions drainservice.ConditionSetter) *NodeReadinessChecker {
	c := &NodeReadinessChecker{
		supervisorClient: supervisorClient,
		secretGetter:     secretGetter,
		conditions:       conditions,
		drainOptions: func(cluster *vitistackv1alpha1.KubernetesCluster) drainservice.Options {
			return drainservice.OptionsForCluster(cluster, drainservice.PurposeUpgrade)
		},
	}
	c.targetClient = c.getTargetClusterClient
	return c
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
	targetClient, err := c.targetClient(ctx, cluster)
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
	targetClient, err := c.targetClient(ctx, cluster)
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

// CordonNode marks a node as unschedulable in the target cluster, with the
// same drain markers a drain sets, so UncordonNode and the release of
// abandoned drains treat it alike.
func (c *NodeReadinessChecker) CordonNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	targetClient, err := c.targetClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get target cluster client: %w", err)
	}
	if err := drainservice.Cordon(ctx, targetClient, nodeName, drainservice.PurposeUpgrade); err != nil {
		return err
	}
	drainservice.ReportCordoned(ctx, c.conditions, cluster, nodeName, drainservice.PurposeUpgrade)
	return nil
}

// UncordonNode marks a node as schedulable in the target cluster and clears
// the drain markers left on it.
func (c *NodeReadinessChecker) UncordonNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	targetClient, err := c.targetClient(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get target cluster client: %w", err)
	}
	return drainservice.Uncordon(ctx, targetClient, nodeName)
}

// DrainNode makes one non-blocking drain pass over nodeName with the shared
// drainservice: it cordons the node and evicts its pods through the Eviction
// API, so PodDisruptionBudgets are honoured, and reports the NodeDrain
// condition. It returns nil once the node is drained. Until then it returns an
// error wrapping drainservice.ErrIncomplete and the node must not be upgraded;
// drainservice.RetryAfter(err) says when to run the next pass.
func (c *NodeReadinessChecker) DrainNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	opts := c.drainOptions(cluster)
	targetClient, err := c.targetClient(ctx, cluster)
	if err != nil {
		// Without a client the node cannot be drained: hold the upgrade
		// (unless force-drain is set) rather than reboot an undrained node.
		return drainservice.ReportUnavailable(ctx, c.conditions, cluster, nodeName, opts,
			fmt.Errorf("failed to get target cluster client: %w", err))
	}
	return drainservice.Run(ctx, targetClient, c.conditions, cluster, nodeName, opts)
}

// getTargetClusterClient creates a Kubernetes client for the target cluster using the stored kubeconfig.
func (c *NodeReadinessChecker) getTargetClusterClient(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (kubernetes.Interface, error) {
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
