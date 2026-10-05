package talos

import (
	"context"
	"errors"
	"testing"
	"time"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/drainservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func useTestDrainClock(t *testing.T) {
	t.Helper()
	prev := drainOptionsFor
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	drainOptionsFor = func(_ *vitistackv1alpha1.KubernetesCluster, purpose drainservice.Purpose) drainservice.Options {
		return drainservice.Options{Timeout: 10 * time.Minute, Purpose: purpose, Now: func() time.Time { return now }}
	}
	t.Cleanup(func() { drainOptionsFor = prev })
}

func drainTestObjects(nodeName string) []runtime.Object {
	labels := map[string]string{"app": "db"}
	minAvailable := intstr.FromInt32(2)
	return []runtime.Object{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "db-0", Namespace: "app", UID: "db-0-uid", Labels: labels},
			Spec:       corev1.PodSpec{NodeName: nodeName},
		},
		&policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Name: "db-pdb", Namespace: "app"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &minAvailable,
				Selector:     &metav1.LabelSelector{MatchLabels: labels},
			},
		},
	}
}

// reactToEvictions answers every eviction with evictErr, or accepts it and
// removes the pod when evictErr is nil.
func reactToEvictions(cs *k8sfake.Clientset, evictErr error) {
	cs.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		create := action.(k8stesting.CreateAction)
		if create.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		if evictErr != nil {
			return true, nil, evictErr
		}
		ev := create.GetObject().(*policyv1.Eviction)
		return true, nil, cs.Tracker().Delete(schema.GroupVersionResource{Version: "v1", Resource: "pods"}, ev.Namespace, ev.Name)
	})
}

func TestDrainAndDeleteNodeKeepsNodeWhilePDBBlocksDrain(t *testing.T) {
	useTestDrainClock(t)
	const nodeName = "worker-3"
	cs := k8sfake.NewClientset(drainTestObjects(nodeName)...)
	reactToEvictions(cs, apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 10))
	tm := &TalosManager{}

	err := tm.drainAndDeleteNode(context.Background(), cs, &vitistackv1alpha1.KubernetesCluster{}, nodeName)

	if !errors.Is(err, ErrDrainIncomplete) {
		t.Fatalf("expected ErrDrainIncomplete, got %v", err)
	}
	if drainservice.RetryAfter(err) <= 0 {
		t.Fatal("a pending drain should tell the caller when to retry")
	}
	node, getErr := cs.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if getErr != nil {
		t.Fatalf("node must not be deleted while its drain is blocked: %v", getErr)
	}
	if !node.Spec.Unschedulable || node.Labels[consts.DrainLabel] != string(drainservice.PurposeScaleDown) {
		t.Fatalf("node should stay cordoned and marked for scale-down, got %+v", node.Labels)
	}
	if _, err := cs.CoreV1().Pods("app").Get(context.Background(), "db-0", metav1.GetOptions{}); err != nil {
		t.Fatalf("PDB-protected pod must not be deleted: %v", err)
	}
}

func TestDrainAndDeleteNodeDeletesNodeOnceDrained(t *testing.T) {
	useTestDrainClock(t)
	const nodeName = "worker-3"
	cs := k8sfake.NewClientset(drainTestObjects(nodeName)...)
	reactToEvictions(cs, nil)
	tm := &TalosManager{}
	cluster := &vitistackv1alpha1.KubernetesCluster{}

	// First pass sends the evictions; the node is not drained yet.
	if err := tm.drainAndDeleteNode(context.Background(), cs, cluster, nodeName); !errors.Is(err, ErrDrainIncomplete) {
		t.Fatalf("first pass should report the drain pending, got %v", err)
	}
	if _, err := cs.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{}); err != nil {
		t.Fatalf("node must survive the pass that only evicts: %v", err)
	}

	// Next reconcile finds the node empty and deletes it.
	if err := tm.drainAndDeleteNode(context.Background(), cs, cluster, nodeName); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if _, err := cs.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected node to be deleted, got %v", err)
	}
}

func clusterWithDrainCondition(reason string) *vitistackv1alpha1.KubernetesCluster {
	return &vitistackv1alpha1.KubernetesCluster{
		Status: vitistackv1alpha1.KubernetesClusterStatus{
			Conditions: []vitistackv1alpha1.KubernetesClusterCondition{{Type: drainservice.ConditionType, Reason: reason}},
		},
	}
}

func drainedNode(name string, purpose drainservice.Purpose) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      map[string]string{consts.DrainLabel: string(purpose)},
			Annotations: map[string]string{consts.DrainStartedAtAnnotation: "2026-10-05T12:00:00Z"},
		},
		Spec: corev1.NodeSpec{Unschedulable: true},
	}
}

func TestReleaseAbandonedDrainsReleasesOnlyIdlePurposes(t *testing.T) {
	cs := k8sfake.NewClientset(
		drainedNode("worker-3", drainservice.PurposeScaleDown),
		drainedNode("worker-4", drainservice.PurposeUpgrade),
	)
	tm := &TalosManager{}

	// An upgrade plan still exists: only the scale-down drain is abandoned.
	if err := tm.releaseAbandonedDrains(context.Background(), cs, clusterWithDrainCondition(drainservice.ReasonBlocked),
		drainservice.PurposeScaleDown); err != nil {
		t.Fatalf("releaseAbandonedDrains: %v", err)
	}
	for name, wantCordoned := range map[string]bool{"worker-3": false, "worker-4": true} {
		node, err := cs.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if node.Spec.Unschedulable != wantCordoned {
			t.Errorf("%s unschedulable = %t, want %t", name, node.Spec.Unschedulable, wantCordoned)
		}
	}
}

func TestReleaseAbandonedDrainsTrustsNodeLabelsNotTheCondition(t *testing.T) {
	// Even when NodeDrain says Idle (say a condition write was lost), a node
	// still labelled by an abandoned drain is found and released.
	cs := k8sfake.NewClientset(drainedNode("worker-3", drainservice.PurposeScaleDown))
	tm := &TalosManager{workloadClient: func(context.Context, *vitistackv1alpha1.KubernetesCluster) (kubernetes.Interface, error) {
		return cs, nil
	}}

	if err := tm.ReleaseAbandonedDrains(context.Background(), clusterWithDrainCondition(drainservice.ReasonIdle),
		drainservice.PurposeScaleDown, drainservice.PurposeUpgrade); err != nil {
		t.Fatalf("ReleaseAbandonedDrains: %v", err)
	}
	node, _ := cs.CoreV1().Nodes().Get(context.Background(), "worker-3", metav1.GetOptions{})
	if node.Spec.Unschedulable {
		t.Fatal("a labelled node must be released whatever the condition says")
	}
}

func TestDeleteK8sNodeHoldsWhenWorkloadClusterIsUnreachable(t *testing.T) {
	useTestDrainClock(t)
	tm := &TalosManager{workloadClient: func(context.Context, *vitistackv1alpha1.KubernetesCluster) (kubernetes.Interface, error) {
		return nil, errors.New("failed to get talos secret: connection refused")
	}}

	err := tm.deleteK8sNodeFromWorkloadCluster(context.Background(), &vitistackv1alpha1.KubernetesCluster{}, "worker-3")

	if !errors.Is(err, ErrDrainIncomplete) {
		t.Fatalf("a transient failure to reach the workload cluster must hold the removal, got %v", err)
	}
}

func TestDeleteK8sNodeContinuesWhenClusterHasNoKubeconfigYet(t *testing.T) {
	useTestDrainClock(t)
	tm := &TalosManager{workloadClient: func(context.Context, *vitistackv1alpha1.KubernetesCluster) (kubernetes.Interface, error) {
		return nil, nil
	}}

	err := tm.deleteK8sNodeFromWorkloadCluster(context.Background(), &vitistackv1alpha1.KubernetesCluster{}, "worker-3")

	if err == nil || errors.Is(err, ErrDrainIncomplete) {
		t.Fatalf("a cluster that never got a kubeconfig has no pods to protect; expected a plain error the caller skips, got %v", err)
	}
}

func controlPlaneMachine(name, ip string) *vitistackv1alpha1.Machine {
	return &vitistackv1alpha1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
			Labels: map[string]string{
				vitistackv1alpha1.ClusterIdAnnotation: "c1-id",
				vitistackv1alpha1.NodeRoleAnnotation:  controlPlaneRole,
			},
		},
		Status: vitistackv1alpha1.MachineStatus{PublicIPAddresses: []string{ip}},
	}
}

func controlPlaneTestCluster() *vitistackv1alpha1.KubernetesCluster {
	return &vitistackv1alpha1.KubernetesCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"},
		Spec: vitistackv1alpha1.KubernetesClusterSpec{
			Cluster: vitistackv1alpha1.KubernetesClusterSpecData{ClusterId: "c1-id"},
		},
	}
}

func TestDeleteControlPlaneNodeDrainsBeforePreparingEtcdRemoval(t *testing.T) {
	useTestDrainClock(t)
	cluster := controlPlaneTestCluster()
	tm, _ := newStatusTestTalosManager(t, cluster,
		controlPlaneMachine("cp-0", "192.0.2.1"), controlPlaneMachine("cp-1", "192.0.2.2"))
	cs := k8sfake.NewClientset(drainTestObjects("cp-1")...)
	reactToEvictions(cs, apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 10))
	tm.workloadClient = func(context.Context, *vitistackv1alpha1.KubernetesCluster) (kubernetes.Interface, error) {
		return cs, nil
	}

	// No Talos secret exists, so reaching the etcd preparation would fail with
	// a different error: ErrDrainIncomplete proves the drain came first.
	_, err := tm.DeleteControlPlaneNode(context.Background(), cluster, "cp-1")

	if !errors.Is(err, ErrDrainIncomplete) {
		t.Fatalf("expected the pending drain to stop control plane removal first, got %v", err)
	}
	if _, getErr := cs.CoreV1().Nodes().Get(context.Background(), "cp-1", metav1.GetOptions{}); getErr != nil {
		t.Fatalf("node must not be deleted while its drain is pending: %v", getErr)
	}
}

func TestDeleteControlPlaneNodeRefusesLastControlPlaneBeforeDraining(t *testing.T) {
	useTestDrainClock(t)
	cluster := controlPlaneTestCluster()
	tm, _ := newStatusTestTalosManager(t, cluster, controlPlaneMachine("cp-0", "192.0.2.1"))
	cs := k8sfake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "cp-0"}})
	tm.workloadClient = func(context.Context, *vitistackv1alpha1.KubernetesCluster) (kubernetes.Interface, error) {
		return cs, nil
	}

	if _, err := tm.DeleteControlPlaneNode(context.Background(), cluster, "cp-0"); err == nil || errors.Is(err, ErrDrainIncomplete) {
		t.Fatalf("expected the last control plane to be refused, got %v", err)
	}
	node, _ := cs.CoreV1().Nodes().Get(context.Background(), "cp-0", metav1.GetOptions{})
	if node.Spec.Unschedulable {
		t.Fatal("the last control plane must not be cordoned")
	}
}

func TestReleaseAbandonedDrainsReportsIdleOnlyAfterFullRelease(t *testing.T) {
	cluster := controlPlaneTestCluster()
	cluster.Status.Conditions = []vitistackv1alpha1.KubernetesClusterCondition{{Type: drainservice.ConditionType, Status: "False", Reason: drainservice.ReasonBlocked}}
	tm, c := newStatusTestTalosManager(t, cluster)
	cs := k8sfake.NewClientset()
	reason := func() string {
		t.Helper()
		got := &vitistackv1alpha1.KubernetesCluster{}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(cluster), got); err != nil {
			t.Fatal(err)
		}
		for _, cond := range got.Status.Conditions {
			if cond.Type == drainservice.ConditionType {
				return cond.Reason
			}
		}
		return ""
	}

	// An upgrade plan still exists: its drain may be live, so not Idle yet.
	if err := tm.releaseAbandonedDrains(context.Background(), cs, cluster, drainservice.PurposeScaleDown); err != nil {
		t.Fatal(err)
	}
	if got := reason(); got == drainservice.ReasonIdle {
		t.Fatal("NodeDrain must not report Idle while upgrade drains are still owned")
	}

	if err := tm.releaseAbandonedDrains(context.Background(), cs, cluster, drainservice.PurposeScaleDown, drainservice.PurposeUpgrade); err != nil {
		t.Fatal(err)
	}
	if got := reason(); got != drainservice.ReasonIdle {
		t.Fatalf("NodeDrain reason = %q after a full release, want %q", got, drainservice.ReasonIdle)
	}
}
