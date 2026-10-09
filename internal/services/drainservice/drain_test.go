package drainservice

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vitistack/talos-operator/pkg/consts"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const testNode = "worker-1"

var (
	podsGVR = schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	t0      = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
)

// optionsAt returns drain options whose clock reads now.
func optionsAt(now time.Time) Options {
	return Options{Timeout: 10 * time.Minute, Purpose: PurposeUpgrade, Now: func() time.Time { return now }}
}

func node(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

// drainingNode is a node a previous pass already cordoned and marked.
func drainingNode(startedAt time.Time) *corev1.Node {
	n := node(testNode)
	n.Spec.Unschedulable = true
	n.Labels = map[string]string{consts.DrainLabel: string(PurposeUpgrade)}
	n.Annotations = map[string]string{consts.DrainStartedAtAnnotation: startedAt.Format(time.RFC3339)}
	return n
}

func appPod(name string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "app",
			UID:       types.UID(name + "-uid"),
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "ReplicaSet", Name: "rs", Controller: new(true)},
			},
		},
		Spec: corev1.PodSpec{NodeName: testNode},
	}
}

func pdb(labels map[string]string) *policyv1.PodDisruptionBudget {
	minAvailable := intstr.FromInt32(2)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "db-pdb", Namespace: "app"},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector:     &metav1.LabelSelector{MatchLabels: labels},
		},
	}
}

func pdbViolation() error {
	return apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 10)
}

// apiRecorder installs reactors on the fake clientset. decide returns the
// error the API server would answer an eviction with; nil accepts it, and the
// pod is then removed like the real API server would do once it terminates.
type apiRecorder struct {
	mu        sync.Mutex
	evictions map[string]int
	deletes   []string
}

func record(t *testing.T, cs *fake.Clientset, decide func(pod string) error) *apiRecorder {
	t.Helper()
	rec := &apiRecorder{evictions: map[string]int{}}
	cs.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		create, ok := action.(k8stesting.CreateAction)
		if !ok || create.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		ev := create.GetObject().(*policyv1.Eviction)
		rec.mu.Lock()
		rec.evictions[ev.Name]++
		rec.mu.Unlock()
		if err := decide(ev.Name); err != nil {
			return true, nil, err
		}
		return true, nil, cs.Tracker().Delete(podsGVR, ev.Namespace, ev.Name)
	})
	cs.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		rec.mu.Lock()
		rec.deletes = append(rec.deletes, action.(k8stesting.DeleteAction).GetName())
		rec.mu.Unlock()
		return false, nil, nil
	})
	return rec
}

func (r *apiRecorder) evictionsOf(pod string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.evictions[pod]
}

func (r *apiRecorder) deleted() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.deletes...)
}

func getNode(t *testing.T, cs *fake.Clientset, name string) *corev1.Node {
	t.Helper()
	n, err := cs.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get node %s: %v", name, err)
	}
	return n
}

func TestStepCordonsAndMarksNodeOnFirstPass(t *testing.T) {
	cs := fake.NewClientset(node(testNode))

	st, err := Step(context.Background(), cs, testNode, optionsAt(t0))
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !st.Done {
		t.Fatalf("an empty node is drained at once, got %+v", st)
	}
	n := getNode(t, cs, testNode)
	if !n.Spec.Unschedulable {
		t.Fatal("node should be cordoned")
	}
	if n.Labels[consts.DrainLabel] != string(PurposeUpgrade) {
		t.Fatalf("drain label = %q, want %q", n.Labels[consts.DrainLabel], PurposeUpgrade)
	}
	if got := n.Annotations[consts.DrainStartedAtAnnotation]; got != t0.Format(time.RFC3339) {
		t.Fatalf("drain start annotation = %q, want %q", got, t0.Format(time.RFC3339))
	}
}

func TestStepKeepsStartTimeAcrossPasses(t *testing.T) {
	cs := fake.NewClientset(drainingNode(t0), appPod("db-0", nil))
	record(t, cs, func(string) error { return pdbViolation() })

	st, err := Step(context.Background(), cs, testNode, optionsAt(t0.Add(3*time.Minute)))
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !st.StartedAt.Equal(t0) {
		t.Fatalf("StartedAt = %s, want %s", st.StartedAt, t0)
	}
	if got := getNode(t, cs, testNode).Annotations[consts.DrainStartedAtAnnotation]; got != t0.Format(time.RFC3339) {
		t.Fatalf("start annotation was rewritten to %q", got)
	}
}

func TestStepEvictsThroughEvictionAPIAndFinishesWhenPodsAreGone(t *testing.T) {
	cs := fake.NewClientset(node(testNode), appPod("web-1", nil), appPod("web-2", nil))
	rec := record(t, cs, func(string) error { return nil })

	st, err := Step(context.Background(), cs, testNode, optionsAt(t0))
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if st.Done {
		t.Fatal("the pass that sends evictions must not report the node drained")
	}
	if rec.evictionsOf("web-1") != 1 || rec.evictionsOf("web-2") != 1 {
		t.Fatalf("expected one eviction per pod, got %v", rec.evictions)
	}

	st, err = Step(context.Background(), cs, testNode, optionsAt(t0.Add(5*time.Second)))
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !st.Done {
		t.Fatalf("node should be drained once its pods are gone, got %+v", st)
	}
	if d := rec.deleted(); len(d) != 0 {
		t.Fatalf("drain must not delete pods directly, deleted %v", d)
	}
}

func TestStepRetriesPDBRejectionWithoutDeleting(t *testing.T) {
	labels := map[string]string{"app": "db"}
	cs := fake.NewClientset(node(testNode), appPod("db-0", labels), pdb(labels))
	rec := record(t, cs, func(string) error { return pdbViolation() })

	for i := range 3 {
		st, err := Step(context.Background(), cs, testNode, optionsAt(t0.Add(time.Duration(i)*time.Minute)))
		if err != nil {
			t.Fatalf("Step: %v", err)
		}
		if st.Done || st.TimedOut {
			t.Fatalf("a PDB-blocked drain within its timeout is pending, got %+v", st)
		}
		if len(st.Remaining) != 1 || st.Remaining[0].Name != "db-0" || len(st.Remaining[0].PDBs) != 1 || st.Remaining[0].PDBs[0] != "db-pdb" {
			t.Fatalf("expected db-0 blocked by db-pdb, got %+v", st.Remaining)
		}
	}
	if rec.evictionsOf("db-0") != 3 {
		t.Fatalf("expected an eviction attempt per pass, got %d", rec.evictionsOf("db-0"))
	}
	if d := rec.deleted(); len(d) != 0 {
		t.Fatalf("drain must not delete PDB-protected pods, deleted %v", d)
	}
}

func TestStepTimeoutReportsAndWarnsPodOwnerOnce(t *testing.T) {
	labels := map[string]string{"app": "db"}
	cs := fake.NewClientset(drainingNode(t0), appPod("db-0", labels), pdb(labels))
	rec := record(t, cs, func(string) error { return pdbViolation() })

	for _, after := range []time.Duration{11 * time.Minute, 12 * time.Minute} {
		st, err := Step(context.Background(), cs, testNode, optionsAt(t0.Add(after)))
		if err != nil {
			t.Fatalf("Step: %v", err)
		}
		if st.Done || !st.TimedOut {
			t.Fatalf("expected a timed-out, unfinished drain, got %+v", st)
		}
		if st.RetryAfter != MaxRetryInterval {
			t.Fatalf("a timed-out drain retries every %s, got %s", MaxRetryInterval, st.RetryAfter)
		}
	}

	events, err := cs.CoreV1().Events("app").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Items) != 1 {
		t.Fatalf("expected exactly one warning event for the blocked pod, got %d", len(events.Items))
	}
	ev := events.Items[0]
	if ev.Type != corev1.EventTypeWarning || ev.Reason != EventReasonEvictionBlocked ||
		ev.InvolvedObject.Kind != "Pod" || ev.InvolvedObject.Name != "db-0" {
		t.Fatalf("unexpected event %+v", ev)
	}
	if d := rec.deleted(); len(d) != 0 {
		t.Fatalf("a timed-out drain must still not delete pods, deleted %v", d)
	}
}

func TestStepForceDeletesPodsBlockedByPDB(t *testing.T) {
	cs := fake.NewClientset(node(testNode), appPod("db-0", nil))
	rec := record(t, cs, func(string) error { return pdbViolation() })

	opts := optionsAt(t0)
	opts.Force = true
	if _, err := Step(context.Background(), cs, testNode, opts); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if d := rec.deleted(); len(d) != 1 || d[0] != "db-0" {
		t.Fatalf("forced drain should delete the blocked pod, deleted %v", d)
	}
}

func TestStepForceProceedsAfterTimeout(t *testing.T) {
	slow := appPod("slow", nil)
	deleting := metav1.NewTime(t0.Add(time.Hour))
	slow.DeletionTimestamp = &deleting
	slow.DeletionGracePeriodSeconds = new(int64(3600))
	cs := fake.NewClientset(drainingNode(t0), slow)

	opts := optionsAt(t0.Add(11 * time.Minute))
	opts.Force = true
	st, err := Step(context.Background(), cs, testNode, opts)
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !st.Done || !st.Forced {
		t.Fatalf("force should let a timed-out drain proceed, got %+v", st)
	}
}

func TestStepSkipsDaemonSetMirrorAndOtherNodePods(t *testing.T) {
	ds := appPod("ds-pod", nil)
	ds.OwnerReferences = []metav1.OwnerReference{{Kind: "DaemonSet", Name: "ds"}}
	mirror := appPod("kube-apiserver", nil)
	mirror.Annotations = map[string]string{corev1.MirrorPodAnnotationKey: "x"}
	elsewhere := appPod("elsewhere", nil)
	elsewhere.Spec.NodeName = "worker-2"

	cs := fake.NewClientset(node(testNode), ds, mirror, elsewhere)
	rec := record(t, cs, func(string) error { return nil })

	st, err := Step(context.Background(), cs, testNode, optionsAt(t0))
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !st.Done {
		t.Fatalf("only skipped pods remain, node should be drained, got %+v", st)
	}
	for _, name := range []string{"ds-pod", "kube-apiserver", "elsewhere"} {
		if rec.evictionsOf(name) != 0 {
			t.Fatalf("pod %s should not have been evicted", name)
		}
	}
}

func TestStepWaitsForTerminatingPods(t *testing.T) {
	terminating := appPod("terminating", nil)
	deleting := metav1.NewTime(t0)
	terminating.DeletionTimestamp = &deleting
	terminating.DeletionGracePeriodSeconds = new(int64(30))
	cs := fake.NewClientset(node(testNode), terminating)
	rec := record(t, cs, func(string) error { return nil })

	st, err := Step(context.Background(), cs, testNode, optionsAt(t0.Add(10*time.Second)))
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if st.Done || len(st.Remaining) != 1 || !st.Remaining[0].Terminating {
		t.Fatalf("expected to wait for the terminating pod, got %+v", st)
	}
	if rec.evictionsOf("terminating") != 0 {
		t.Fatal("a terminating pod must not be evicted again")
	}
}

func TestStepDoesNotWaitForPodsStuckTerminating(t *testing.T) {
	// The API server sets deletionTimestamp to request time + grace period,
	// so a pod still present 90s after it is a pod nothing will finish.
	stuck := appPod("stuck", nil)
	deleting := metav1.NewTime(t0)
	stuck.DeletionTimestamp = &deleting
	stuck.DeletionGracePeriodSeconds = new(int64(30))
	cs := fake.NewClientset(node(testNode), stuck)

	st, err := Step(context.Background(), cs, testNode, optionsAt(t0.Add(90*time.Second)))
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !st.Done {
		t.Fatalf("a pod stuck terminating past its deletion deadline must not block the drain, got %+v", st)
	}
}

func TestStepRestartsClockWhenPreviousDrainWasCancelled(t *testing.T) {
	// Someone ran `kubectl uncordon` on an old drain: the annotation is left
	// but the node is schedulable again, so a new drain starts from zero.
	n := drainingNode(t0)
	n.Spec.Unschedulable = false
	delete(n.Labels, consts.DrainLabel)
	cs := fake.NewClientset(n, appPod("db-0", nil))
	record(t, cs, func(string) error { return pdbViolation() })
	now := t0.Add(5 * time.Hour)

	st, err := Step(context.Background(), cs, testNode, optionsAt(now))
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !st.StartedAt.Equal(now) || st.TimedOut {
		t.Fatalf("a new drain must start its own clock, got StartedAt=%s TimedOut=%t", st.StartedAt, st.TimedOut)
	}
}

func TestStepMissingNodeIsDrained(t *testing.T) {
	cs := fake.NewClientset()

	st, err := Step(context.Background(), cs, testNode, optionsAt(t0))
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !st.Done {
		t.Fatalf("a node that does not exist has nothing to drain, got %+v", st)
	}
}

func TestRetryAfterBacksOffWithElapsedTime(t *testing.T) {
	cases := []struct {
		elapsed time.Duration
		want    time.Duration
	}{
		{0, MinRetryInterval},
		{20 * time.Second, 10 * time.Second},
		{time.Minute, 30 * time.Second},
		{5 * time.Minute, MaxRetryInterval},
	}
	for _, c := range cases {
		if got := retryAfter(c.elapsed); got != c.want {
			t.Errorf("retryAfter(%s) = %s, want %s", c.elapsed, got, c.want)
		}
	}
}

func TestUncordonClearsDrainMarkers(t *testing.T) {
	cs := fake.NewClientset(drainingNode(t0))

	if err := Uncordon(context.Background(), cs, testNode); err != nil {
		t.Fatalf("Uncordon: %v", err)
	}
	n := getNode(t, cs, testNode)
	if n.Spec.Unschedulable {
		t.Fatal("node should be schedulable")
	}
	if _, ok := n.Labels[consts.DrainLabel]; ok {
		t.Fatal("drain label should be removed")
	}
	if _, ok := n.Annotations[consts.DrainStartedAtAnnotation]; ok {
		t.Fatal("drain start annotation should be removed")
	}
}

func TestUncordonMissingNodeIsNoop(t *testing.T) {
	if err := Uncordon(context.Background(), fake.NewClientset(), testNode); err != nil {
		t.Fatalf("Uncordon of a missing node: %v", err)
	}
}

func TestUncordonKeepsNodeThatWasCordonedBeforeTheDrain(t *testing.T) {
	adminCordoned := node(testNode)
	adminCordoned.Spec.Unschedulable = true
	cs := fake.NewClientset(adminCordoned)

	if _, err := Step(context.Background(), cs, testNode, optionsAt(t0)); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if err := Uncordon(context.Background(), cs, testNode); err != nil {
		t.Fatalf("Uncordon: %v", err)
	}
	n := getNode(t, cs, testNode)
	if !n.Spec.Unschedulable {
		t.Fatal("a node cordoned before the drain must stay cordoned")
	}
	if len(n.Labels) != 0 || len(n.Annotations) != 0 {
		t.Fatalf("drain markers should be removed, got labels=%v annotations=%v", n.Labels, n.Annotations)
	}
}

func TestCordonMarksNodeWithoutDraining(t *testing.T) {
	cs := fake.NewClientset(node(testNode), appPod("web-1", nil))
	rec := record(t, cs, func(string) error { return nil })

	if err := Cordon(context.Background(), cs, testNode, PurposeUpgrade); err != nil {
		t.Fatalf("Cordon: %v", err)
	}
	n := getNode(t, cs, testNode)
	if !n.Spec.Unschedulable || n.Labels[consts.DrainLabel] != string(PurposeUpgrade) {
		t.Fatalf("node should be cordoned and marked, got %+v", n.Labels)
	}
	if rec.evictionsOf("web-1") != 0 {
		t.Fatal("Cordon must not evict pods")
	}
}

func TestReleaseAbandonedReleasesOnlyTheGivenPurposes(t *testing.T) {
	upgrading := drainingNode(t0)
	scalingDown := drainingNode(t0)
	scalingDown.Name = "worker-2"
	scalingDown.Labels[consts.DrainLabel] = string(PurposeScaleDown)
	adminCordoned := node("worker-3")
	adminCordoned.Spec.Unschedulable = true
	cs := fake.NewClientset(upgrading, scalingDown, adminCordoned)

	released, err := ReleaseAbandoned(context.Background(), cs, PurposeScaleDown)
	if err != nil {
		t.Fatalf("ReleaseAbandoned: %v", err)
	}
	if len(released) != 1 || released[0] != "worker-2" {
		t.Fatalf("expected only the scale-down node released, got %v", released)
	}
	if !getNode(t, cs, testNode).Spec.Unschedulable {
		t.Fatal("a node drained for an upgrade still in progress must stay cordoned")
	}
	if !getNode(t, cs, "worker-3").Spec.Unschedulable {
		t.Fatal("a node cordoned by someone else must be left alone")
	}
}

func TestStepClearsStaleWasCordonedMarkerWhenRestarting(t *testing.T) {
	// The admin uncordoned the node to cancel an old drain; a new drain must
	// not keep the old "was cordoned" note, or the node would end cordoned.
	n := drainingNode(t0)
	n.Spec.Unschedulable = false
	delete(n.Labels, consts.DrainLabel)
	n.Annotations[consts.DrainNodeWasCordonedAnnotation] = "true"
	cs := fake.NewClientset(n)

	if _, err := Step(context.Background(), cs, testNode, optionsAt(t0.Add(time.Hour))); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if err := Uncordon(context.Background(), cs, testNode); err != nil {
		t.Fatalf("Uncordon: %v", err)
	}
	if getNode(t, cs, testNode).Spec.Unschedulable {
		t.Fatal("node should be schedulable again: it was not cordoned when this drain began")
	}
}

func TestStepNamesPDBWithEmptySelector(t *testing.T) {
	// In policy/v1 an empty selector selects every pod in the namespace.
	all := pdb(nil)
	all.Spec.Selector = &metav1.LabelSelector{}
	cs := fake.NewClientset(node(testNode), appPod("db-0", map[string]string{"app": "db"}), all)
	record(t, cs, func(string) error { return pdbViolation() })

	st, err := Step(context.Background(), cs, testNode, optionsAt(t0))
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if len(st.Remaining) != 1 || len(st.Remaining[0].PDBs) != 1 || st.Remaining[0].PDBs[0] != "db-pdb" {
		t.Fatalf("expected the empty-selector PDB to be named, got %+v", st.Remaining)
	}
}

func TestStepMissingNodeIsFlagged(t *testing.T) {
	st, err := Step(context.Background(), fake.NewClientset(), testNode, optionsAt(t0))
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !st.NodeMissing {
		t.Fatal("a drain of a node that does not exist should say so")
	}
}

func TestEventNameFitsObjectNameLimit(t *testing.T) {
	long := strings.Repeat("a", 253)
	if got := eventName(long, t0); len(got) > 253 {
		t.Fatalf("event name is %d characters, limit is 253", len(got))
	}
	if got := eventName("db-0", t0); !strings.HasPrefix(got, "db-0.drain-") {
		t.Fatalf("short pod names should be kept whole, got %q", got)
	}
}
