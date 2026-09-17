package talos

import (
	"context"
	"fmt"
	"sort"

	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// workloadNodes is one reconcile pass's view of the workload cluster's Nodes.
// The steady-state pass lists Nodes once and hands this to every check that
// needs them, instead of each check building a client and listing again.
type workloadNodes struct {
	client kubernetes.Interface
	items  []corev1.Node
}

// listWorkloadNodes lists the workload cluster's Nodes. It returns nil and no
// error when the cluster has no kubeconfig yet.
func (t *TalosManager) listWorkloadNodes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*workloadNodes, error) {
	clientset, err := t.getWorkloadClusterClient(ctx, cluster)
	if err != nil {
		return nil, err
	}
	if clientset == nil {
		return nil, nil
	}
	nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes from workload cluster: %w", err)
	}
	return &workloadNodes{client: clientset, items: nodeList.Items}, nil
}

// nodesReadiness reports whether every expected Machine has a Node that is
// Ready. When not, the reason is a short human-readable string suitable for
// status.message. Nodes without a Machine in expected are ignored.
func nodesReadiness(nodes []corev1.Node, expected []*vitistackv1alpha1.Machine) (bool, string) {
	if len(expected) == 0 {
		return false, "no expected machines"
	}

	byName := make(map[string]*corev1.Node, len(nodes))
	for i := range nodes {
		byName[nodes[i].Name] = &nodes[i]
	}

	var missing, notReady []string
	for _, m := range expected {
		node, ok := byName[m.Name]
		if !ok {
			missing = append(missing, m.Name)
			continue
		}
		if !isNodeReady(node) {
			notReady = append(notReady, m.Name)
		}
	}

	if len(missing) == 0 && len(notReady) == 0 {
		return true, ""
	}

	sort.Strings(missing)
	sort.Strings(notReady)
	return false, buildNotReadyMessage(missing, notReady)
}

// cleanupOrphanedK8sNodes deletes the workload cluster Nodes that have no
// corresponding Machine CRD and are NotReady with SchedulingDisabled, and drops
// them from nodes so later checks in the pass do not act on them. This handles
// cases where the Machine was deleted but the K8s node wasn't cleaned up
// properly.
func (t *TalosManager) cleanupOrphanedK8sNodes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodes *workloadNodes) error {
	machineNames, err := t.getClusterMachineNames(ctx, cluster)
	if err != nil {
		return err
	}
	nodes.items = removeOrphanedNodes(ctx, nodes.client, nodes.items, machineNames, clusterLogTag(cluster))
	return nil
}

// removeOrphanedNodes deletes the orphaned nodes (see isNodeOrphaned) that
// have no Machine, and returns the nodes that still exist. A node whose delete
// failed is kept.
func removeOrphanedNodes(
	ctx context.Context,
	clientset kubernetes.Interface,
	nodes []corev1.Node,
	machineNames map[string]bool,
	clusterTag string,
) []corev1.Node {
	survivors := make([]corev1.Node, 0, len(nodes))
	for i := range nodes {
		node := &nodes[i]
		removed, err := deleteOrphanedNodeIfNeeded(ctx, clientset, node, machineNames, clusterTag)
		if err != nil {
			vlog.Error(fmt.Sprintf("Error processing node %s/%s: %v", clusterTag, node.Name, err), err)
		}
		if !removed {
			survivors = append(survivors, *node)
		}
	}
	return survivors
}

// maintenanceProbe is a configured node to check for Talos maintenance mode.
type maintenanceProbe struct {
	name string
	ip   string
}

// maintenanceProbeTargets lists the configured nodes worth probing for Talos
// maintenance mode, sorted by name. Only nodes with a Machine and an IPv4
// address are listed. A node that joined the workload cluster and is Ready
// runs kubelet on a configured Talos, so it is skipped; a NotReady or missing
// node may have been reset and is probed. With no node list (nil) every
// configured node is probed.
func maintenanceProbeTargets(
	configured map[string]types.UID,
	machines []*vitistackv1alpha1.Machine,
	nodes *workloadNodes,
) []maintenanceProbe {
	ready := map[string]bool{}
	if nodes != nil {
		for i := range nodes.items {
			if isNodeReady(&nodes.items[i]) {
				ready[nodes.items[i].Name] = true
			}
		}
	}

	var targets []maintenanceProbe
	for _, m := range machines {
		if _, ok := configured[m.Name]; !ok || ready[m.Name] {
			continue
		}
		ip := getFirstIPv4(m)
		if ip == "" {
			continue
		}
		targets = append(targets, maintenanceProbe{name: m.Name, ip: ip})
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].name < targets[j].name })
	return targets
}

// deleteOrphanedNodeIfNeeded deletes node if it is orphaned (no Machine CRD,
// NotReady, Unschedulable). It reports whether the node is gone.
func deleteOrphanedNodeIfNeeded(
	ctx context.Context,
	clientset kubernetes.Interface,
	node *corev1.Node,
	machineNames map[string]bool,
	clusterTag string,
) (bool, error) {
	nodeName := node.Name

	// Skip if there's a corresponding Machine
	if machineNames[nodeName] {
		return false, nil
	}

	// Check if node is orphaned: NotReady AND Unschedulable
	if !isNodeOrphaned(node) {
		return false, nil
	}

	vlog.Info(fmt.Sprintf("Found orphaned Kubernetes node (NotReady + SchedulingDisabled, no Machine CRD): node=%s %s",
		nodeName, clusterTag))

	if err := clientset.CoreV1().Nodes().Delete(ctx, nodeName, metav1.DeleteOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("failed to delete orphaned node %s: %w", nodeName, err)
	}

	vlog.Info(fmt.Sprintf("Successfully deleted orphaned Kubernetes node: node=%s %s", nodeName, clusterTag))
	return true, nil
}
