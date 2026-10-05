package upgradeservice

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/drainservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// blockedDrainer is a NodeDrainer whose drain never completes.
type blockedDrainer struct {
	drained []string
}

func (d *blockedDrainer) CordonAndDrainNode(_ context.Context, _ *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	d.drained = append(d.drained, nodeName)
	return fmt.Errorf("%w: pod app/db-0 blocked by PodDisruptionBudget db-pdb", drainservice.ErrIncomplete)
}

func (d *blockedDrainer) UncordonNode(context.Context, *vitistackv1alpha1.KubernetesCluster, string) error {
	return nil
}

func (d *blockedDrainer) IsNodeSchedulable(context.Context, *vitistackv1alpha1.KubernetesCluster, string) (bool, error) {
	return false, nil
}

func TestOrchestratorDoesNotUpgradeNodeWhenDrainIsBlocked(t *testing.T) {
	f := newFixture(t, legacyCluster(nil), fixtureSecret(nil))
	drainer := &blockedDrainer{}
	o := NewUpgradeOrchestrator(f.service, f.service.clientService, drainer)
	cluster := f.cluster(t)
	node := NodeUpgradeInfo{Name: "worker-1", IP: "192.0.2.10", Role: "worker"}
	state := &upgradeState{totalNodes: 1}

	// 192.0.2.10 is TEST-NET-1: if the upgrade were attempted, the Talos call
	// would fail with a connection error and the node would be marked upgraded.
	result := o.executeNodeUpgrade(context.Background(), cluster, nil, node, 0, state, "installer:v1")

	if len(drainer.drained) != 1 {
		t.Fatalf("expected one drain attempt, got %v", drainer.drained)
	}
	if result == nil || !result.Success || !result.NeedsRequeue || result.Error != nil || result.FailedNode != "" {
		t.Fatalf("blocked drain should requeue without failing the upgrade, got %+v", result)
	}
	if f.service.stateService.IsNodeUpgraded(context.Background(), cluster, node.Name) {
		t.Fatal("node must not be marked as upgraded when its drain is blocked")
	}
	if current, _ := f.service.stateService.GetCurrentUpgradingNode(context.Background(), cluster); current != "" {
		t.Fatalf("current upgrading node should be cleared so the node is retried from the drain step, got %q", current)
	}
}

func TestRollingUpgradeDoesNotInitiateUpgradeWhenPDBBlocksDrain(t *testing.T) {
	f := newFixture(t, legacyCluster(nil), fixtureSecret(nil))
	labels := map[string]string{"app": "db"}
	cs := k8sfake.NewClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "db-0", Namespace: "app", UID: "db-0-uid", Labels: labels},
			Spec:       corev1.PodSpec{NodeName: "worker-1"},
		},
	)
	var deleted []string
	cs.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.(k8stesting.CreateAction).GetSubresource() != "eviction" {
			return false, nil, nil
		}
		return true, nil, apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 10)
	})
	cs.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		deleted = append(deleted, action.(k8stesting.DeleteAction).GetName())
		return false, nil, nil
	})

	checker := NewNodeReadinessChecker(f.client, nil, f.service.statusManager)
	checker.targetClient = func(context.Context, *vitistackv1alpha1.KubernetesCluster) (kubernetes.Interface, error) {
		return cs, nil
	}
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	checker.drainOptions = func(*vitistackv1alpha1.KubernetesCluster) drainservice.Options {
		return drainservice.Options{Timeout: 10 * time.Minute, Purpose: drainservice.PurposeUpgrade, Now: func() time.Time { return now }}
	}
	// stateManager and talosClientService are nil: touching either after a
	// blocked drain would panic, which is exactly what must not happen.
	e := NewRollingUpgradeExecutor(nil, checker, nil, f.service)
	cluster := f.cluster(t)
	state := &ClusterUpgradeState{Nodes: []NodeUpgradeState{{NodeName: "worker-1", IP: "192.0.2.10"}}}

	result := e.initiateNodeUpgrade(context.Background(), cluster, nil, state, &state.Nodes[0])

	if result.Error != nil || !result.Continue || result.RequeueAfter != drainservice.MinRetryInterval {
		t.Fatalf("pending drain should requeue on the drain's backoff without error, got %+v", result)
	}
	if !strings.Contains(result.Message, "db-0") {
		t.Fatalf("result message should name the blocking pod, got %q", result.Message)
	}
	if len(deleted) != 0 {
		t.Fatalf("PDB-protected pods must not be deleted, deleted %v", deleted)
	}
	node, err := cs.CoreV1().Nodes().Get(context.Background(), "worker-1", metav1.GetOptions{})
	if err != nil || !node.Spec.Unschedulable {
		t.Fatalf("node should be cordoned, got %+v err=%v", node, err)
	}
	cond := condition(f.cluster(t), drainservice.ConditionType)
	if cond == nil || cond.Status != "False" || cond.Reason != drainservice.ReasonDraining {
		t.Fatalf("expected NodeDrain=False/Draining condition, got %+v", cond)
	}
}

func TestNodeReadinessCheckerDrainsEmptyNodeAndUncordonClearsMarkers(t *testing.T) {
	cs := k8sfake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}})
	checker := NewNodeReadinessChecker(nil, nil, nil)
	checker.targetClient = func(context.Context, *vitistackv1alpha1.KubernetesCluster) (kubernetes.Interface, error) {
		return cs, nil
	}
	cluster := &vitistackv1alpha1.KubernetesCluster{}

	if err := checker.DrainNode(context.Background(), cluster, "worker-1"); err != nil {
		t.Fatalf("an empty node should drain in one pass: %v", err)
	}
	node, _ := cs.CoreV1().Nodes().Get(context.Background(), "worker-1", metav1.GetOptions{})
	if !node.Spec.Unschedulable || node.Labels[consts.DrainLabel] != string(drainservice.PurposeUpgrade) {
		t.Fatalf("drained node should be cordoned and marked for upgrade, got %+v", node.Labels)
	}

	if err := checker.UncordonNode(context.Background(), cluster, "worker-1"); err != nil {
		t.Fatalf("UncordonNode: %v", err)
	}
	node, _ = cs.CoreV1().Nodes().Get(context.Background(), "worker-1", metav1.GetOptions{})
	if node.Spec.Unschedulable {
		t.Fatal("node should be schedulable after uncordon")
	}
	if _, ok := node.Labels[consts.DrainLabel]; ok {
		t.Fatal("uncordon should clear the drain label")
	}
}

func TestHasUpgradePlan(t *testing.T) {
	f := newFixture(t, legacyCluster(nil), fixtureSecret(nil))
	cluster := f.cluster(t)

	has, err := f.controller.HasUpgradePlan(context.Background(), cluster)
	if err != nil || has {
		t.Fatalf("no plan stored: HasUpgradePlan = (%t, %v), want (false, nil)", has, err)
	}

	state := &ClusterUpgradeState{UpgradeType: UpgradeTypeTalos, Phase: UpgradePhaseFailed}
	if err := f.controller.stateManager.SaveUpgradeState(context.Background(), cluster, state); err != nil {
		t.Fatal(err)
	}
	has, err = f.controller.HasUpgradePlan(context.Background(), cluster)
	if err != nil || !has {
		t.Fatalf("a failed plan still owns its cordoned node: HasUpgradePlan = (%t, %v), want (true, nil)", has, err)
	}
}

func TestCordonNodeMarksNodeForUpgradeSoItCanBeReleased(t *testing.T) {
	f := newFixture(t, legacyCluster(nil), fixtureSecret(nil))
	cs := k8sfake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}})
	checker := NewNodeReadinessChecker(f.client, nil, f.service.statusManager)
	checker.targetClient = func(context.Context, *vitistackv1alpha1.KubernetesCluster) (kubernetes.Interface, error) {
		return cs, nil
	}

	if err := checker.CordonNode(context.Background(), f.cluster(t), "worker-1"); err != nil {
		t.Fatalf("CordonNode: %v", err)
	}
	node, _ := cs.CoreV1().Nodes().Get(context.Background(), "worker-1", metav1.GetOptions{})
	if !node.Spec.Unschedulable || node.Labels[consts.DrainLabel] != string(drainservice.PurposeUpgrade) {
		t.Fatalf("Kubernetes-upgrade cordon should carry the upgrade drain label, got %+v", node.Labels)
	}
	// The release of abandoned drains only looks when NodeDrain is not Idle.
	cond := condition(f.cluster(t), drainservice.ConditionType)
	if cond == nil || cond.Reason != drainservice.ReasonCordoned {
		t.Fatalf("expected NodeDrain=False/Cordoned so the cordon can be released later, got %+v", cond)
	}
}

// recordingDrainer records the order of drain calls against upgrade state.
type recordingDrainer struct {
	blockedDrainer
	f       *fixture
	cluster *vitistackv1alpha1.KubernetesCluster
	current string
}

func (d *recordingDrainer) CordonAndDrainNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	d.current, _ = d.f.service.stateService.GetCurrentUpgradingNode(ctx, d.cluster)
	return d.blockedDrainer.CordonAndDrainNode(ctx, cluster, nodeName)
}

func TestOrchestratorDrainsBeforeClaimingTheNode(t *testing.T) {
	f := newFixture(t, legacyCluster(nil), fixtureSecret(nil))
	cluster := f.cluster(t)
	drainer := &recordingDrainer{f: f, cluster: cluster}
	o := NewUpgradeOrchestrator(f.service, f.service.clientService, drainer)

	o.executeNodeUpgrade(context.Background(), cluster, nil, NodeUpgradeInfo{Name: "worker-1", IP: "192.0.2.10", Role: "worker"}, 0, &upgradeState{totalNodes: 1}, "installer:v1")

	if drainer.current != "" {
		t.Fatalf("the node was claimed as current upgrading node (%q) before its drain finished", drainer.current)
	}
}

func TestDrainNodeHoldsUpgradeWhenWorkloadClusterIsUnreachableUnlessForced(t *testing.T) {
	f := newFixture(t, legacyCluster(nil), fixtureSecret(nil))
	checker := NewNodeReadinessChecker(f.client, nil, f.service.statusManager)
	checker.targetClient = func(context.Context, *vitistackv1alpha1.KubernetesCluster) (kubernetes.Interface, error) {
		return nil, errors.New("kubeconfig not found in cluster secret")
	}
	force := false
	checker.drainOptions = func(*vitistackv1alpha1.KubernetesCluster) drainservice.Options {
		return drainservice.Options{Purpose: drainservice.PurposeUpgrade, Force: force}
	}

	if err := checker.DrainNode(context.Background(), f.cluster(t), "worker-1"); !errors.Is(err, drainservice.ErrIncomplete) {
		t.Fatalf("expected the upgrade to be held, got %v", err)
	}
	if cond := condition(f.cluster(t), drainservice.ConditionType); cond == nil || cond.Reason != drainservice.ReasonFailed {
		t.Fatalf("expected NodeDrain=False/Failed, got %+v", cond)
	}

	force = true
	if err := checker.DrainNode(context.Background(), f.cluster(t), "worker-1"); err != nil {
		t.Fatalf("force-drain must let the upgrade proceed, got %v", err)
	}
}
