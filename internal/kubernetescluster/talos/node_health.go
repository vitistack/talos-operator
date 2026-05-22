package talos

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// checkAllExpectedNodesReady queries the workload cluster's Kubernetes API
// and verifies that every expected Machine has a corresponding Node object
// that reports Ready=True. Returns (true, "") when all expected nodes are
// joined and Ready, (false, reason) when not (reason is a short
// human-readable string suitable for status.message).
//
// expected names come from the supplied Machine list — i.e. exactly the
// Machines this reconcile pass cares about, not whatever happens to be in
// the workload cluster (which may contain stragglers from removed nodes).
func (t *TalosManager) checkAllExpectedNodesReady(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	expected []*vitistackv1alpha1.Machine,
) (bool, string, error) {
	if len(expected) == 0 {
		return false, "no expected machines", nil
	}

	clientset, err := t.getWorkloadClusterClient(ctx, cluster)
	if err != nil {
		return false, "", fmt.Errorf("failed to build workload cluster client: %w", err)
	}
	if clientset == nil {
		return false, "kubeconfig not available yet", nil
	}

	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, "", fmt.Errorf("failed to list nodes in workload cluster: %w", err)
	}

	byName := make(map[string]*corev1.Node, len(nodes.Items))
	for i := range nodes.Items {
		byName[nodes.Items[i].Name] = &nodes.Items[i]
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
		return true, "", nil
	}

	sort.Strings(missing)
	sort.Strings(notReady)
	reason := buildNotReadyMessage(missing, notReady)
	vlog.Info(fmt.Sprintf("Node health check pending %s: %s", clusterLogTag(cluster), reason))
	return false, reason, nil
}

// isNodeReady returns true when the Node has a Ready condition with status True.
func isNodeReady(node *corev1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// buildNotReadyMessage produces a stable, comma-separated description of the
// missing and not-ready nodes for use in the cluster status message.
func buildNotReadyMessage(missing, notReady []string) string {
	switch {
	case len(missing) > 0 && len(notReady) > 0:
		return fmt.Sprintf("waiting for nodes to join (%v) and become Ready (%v)", missing, notReady)
	case len(missing) > 0:
		return fmt.Sprintf("waiting for nodes to join: %v", missing)
	default:
		return fmt.Sprintf("waiting for nodes to become Ready: %v", notReady)
	}
}

// stageWaitNodesHealthy is the gate that holds the cluster Phase below Ready
// until every expected K8s Node has joined and reached Ready. Stage 5 must
// not finalize the cluster until this succeeds.
func (t *TalosManager) stageWaitNodesHealthy(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	expected []*vitistackv1alpha1.Machine,
) error {
	flags, _ := t.getTalosSecretFlags(ctx, cluster)
	if flags.NodesHealthReady {
		return nil
	}

	_ = t.statusManager.SetMessage(ctx, cluster, "Waiting for all nodes to become Ready")
	_ = t.statusManager.SetCondition(ctx, cluster, "NodesReady", "False", "Waiting", "Waiting for all expected nodes to join and become Ready")

	ready, reason, err := t.checkAllExpectedNodesReady(ctx, cluster, expected)
	if err != nil {
		vlog.Warn(fmt.Sprintf("Node health check error %s: %v", clusterLogTag(cluster), err))
		return talosclientservice.NewRequeueError("node health check error, retrying", 15*time.Second)
	}
	if !ready {
		_ = t.statusManager.SetMessage(ctx, cluster, reason)
		return talosclientservice.NewRequeueError(reason, 15*time.Second)
	}

	if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"nodes_health_ready": true}); err != nil {
		vlog.Error("Failed to set nodes_health_ready flag in secret "+clusterLogTag(cluster), err)
		return fmt.Errorf("failed to persist nodes_health_ready flag: %w", err)
	}
	return nil
}

// refreshNodesHealthFlag is the short-circuit path's equivalent — best-effort
// re-evaluation so the flag (and therefore the cluster Phase) tracks reality
// when nodes go NotReady or new Machines have been added but haven't joined
// yet. Never returns an error; logs and moves on.
func (t *TalosManager) refreshNodesHealthFlag(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
) {
	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil || len(machines) == 0 {
		return
	}

	ready, reason, err := t.checkAllExpectedNodesReady(ctx, cluster, machines)
	if err != nil {
		// Don't flip the flag based on transient API errors — leave previous value.
		return
	}

	flags, _ := t.getTalosSecretFlags(ctx, cluster)
	if ready == flags.NodesHealthReady {
		return
	}
	if err := t.setTalosSecretFlags(ctx, cluster, map[string]bool{"nodes_health_ready": ready}); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to update nodes_health_ready flag %s: %v", clusterLogTag(cluster), err))
		return
	}
	if !ready {
		_ = t.statusManager.SetMessage(ctx, cluster, reason)
	}
}
