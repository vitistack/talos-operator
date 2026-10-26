package drainservice

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/pkg/consts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

type recordedCondition struct {
	condType, status, reason, message string
}

type fakeConditionSetter struct {
	conditions []recordedCondition
}

func (f *fakeConditionSetter) SetCondition(_ context.Context, _ *vitistackv1alpha1.KubernetesCluster, condType, status, reason, message string) error {
	f.conditions = append(f.conditions, recordedCondition{condType, status, reason, message})
	return nil
}

func (f *fakeConditionSetter) last(t *testing.T) recordedCondition {
	t.Helper()
	if len(f.conditions) == 0 {
		t.Fatal("expected a condition to be set")
	}
	c := f.conditions[len(f.conditions)-1]
	if c.condType != ConditionType {
		t.Fatalf("condition type = %q, want %q", c.condType, ConditionType)
	}
	return c
}

func cluster(annotations map[string]string) *vitistackv1alpha1.KubernetesCluster {
	return &vitistackv1alpha1.KubernetesCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns", Annotations: annotations},
	}
}

func TestOptionsForClusterDefaultsToPDBSafeDrain(t *testing.T) {
	opts := OptionsForCluster(cluster(nil), PurposeScaleDown)
	if opts.Force {
		t.Fatal("force drain must not be the default")
	}
	if opts.Timeout != DefaultTimeout {
		t.Fatalf("timeout = %s, want %s", opts.Timeout, DefaultTimeout)
	}
	if opts.Purpose != PurposeScaleDown {
		t.Fatalf("purpose = %q, want %q", opts.Purpose, PurposeScaleDown)
	}
}

func TestOptionsForClusterHonoursForceAnnotationAndTimeoutSetting(t *testing.T) {
	viper.Set(consts.DRAIN_TIMEOUT_MINUTES, 30)
	t.Cleanup(func() { viper.Set(consts.DRAIN_TIMEOUT_MINUTES, nil) })

	opts := OptionsForCluster(cluster(map[string]string{consts.ForceDrainAnnotation: "true"}), PurposeUpgrade)
	if !opts.Force {
		t.Fatal("force annotation should enable force drain")
	}
	if opts.Timeout != 30*time.Minute {
		t.Fatalf("timeout = %s, want 30m", opts.Timeout)
	}
}

func TestRunPendingDrainReportsDrainingAndAsksForRequeue(t *testing.T) {
	labels := map[string]string{"app": "db"}
	cs := fake.NewClientset(node(testNode), appPod("db-0", labels), pdb(labels))
	record(t, cs, func(string) error { return pdbViolation() })
	setter := &fakeConditionSetter{}

	err := Run(context.Background(), cs, setter, cluster(nil), testNode, optionsAt(t0))

	var pending *PendingError
	if !errors.As(err, &pending) || !errors.Is(err, ErrIncomplete) {
		t.Fatalf("expected a *PendingError wrapping ErrIncomplete, got %v", err)
	}
	if pending.RetryAfter != MinRetryInterval {
		t.Fatalf("RetryAfter = %s, want %s", pending.RetryAfter, MinRetryInterval)
	}
	c := setter.last(t)
	if c.status != conditionFalse || c.reason != ReasonDraining {
		t.Fatalf("unexpected condition %+v", c)
	}
	if !strings.Contains(c.message, "app/db-0") || !strings.Contains(c.message, "db-pdb") {
		t.Fatalf("condition should name the waiting pod and its PDB, got %q", c.message)
	}
}

func TestRunTimedOutDrainReportsBlocked(t *testing.T) {
	labels := map[string]string{"app": "db"}
	cs := fake.NewClientset(drainingNode(t0), appPod("db-0", labels), pdb(labels))
	record(t, cs, func(string) error { return pdbViolation() })
	setter := &fakeConditionSetter{}

	err := Run(context.Background(), cs, setter, cluster(nil), testNode, optionsAt(t0.Add(11*time.Minute)))

	var pending *PendingError
	if !errors.As(err, &pending) {
		t.Fatalf("expected a *PendingError, got %v", err)
	}
	c := setter.last(t)
	if c.status != conditionFalse || c.reason != ReasonBlocked {
		t.Fatalf("unexpected condition %+v", c)
	}
	for _, want := range []string{testNode, "app/db-0", "db-pdb", consts.ForceDrainAnnotation} {
		if !strings.Contains(c.message, want) {
			t.Fatalf("blocked condition should mention %q, got %q", want, c.message)
		}
	}
}

func TestRunCompletedDrainReportsDrained(t *testing.T) {
	cs := fake.NewClientset(node(testNode))
	setter := &fakeConditionSetter{}

	if err := Run(context.Background(), cs, setter, cluster(nil), testNode, optionsAt(t0)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if c := setter.last(t); c.status != conditionTrue || c.reason != ReasonDrained {
		t.Fatalf("unexpected condition %+v", c)
	}
}

func TestRunForcedDrainReportsForcedAndProceeds(t *testing.T) {
	slow := appPod("slow", nil)
	deleting := metav1.NewTime(t0.Add(time.Hour))
	slow.DeletionTimestamp = &deleting
	slow.DeletionGracePeriodSeconds = new(int64(3600))
	cs := fake.NewClientset(drainingNode(t0), slow)
	setter := &fakeConditionSetter{}

	opts := optionsAt(t0.Add(11 * time.Minute))
	opts.Force = true
	if err := Run(context.Background(), cs, setter, cluster(nil), testNode, opts); err != nil {
		t.Fatalf("forced drain must let the caller proceed, got %v", err)
	}
	c := setter.last(t)
	if c.status != conditionTrue || c.reason != ReasonForced || !strings.Contains(c.message, "app/slow") {
		t.Fatalf("forced drain should be reported with what it bypassed, got %+v", c)
	}
}

func TestRunAPIErrorReportsFailed(t *testing.T) {
	cs := fake.NewClientset(node(testNode))
	cs.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver unavailable")
	})
	setter := &fakeConditionSetter{}

	err := Run(context.Background(), cs, setter, cluster(nil), testNode, optionsAt(t0))

	var pending *PendingError
	if !errors.Is(err, ErrIncomplete) || errors.As(err, &pending) {
		t.Fatalf("expected a non-pending error wrapping ErrIncomplete, got %v", err)
	}
	if c := setter.last(t); c.status != conditionFalse || c.reason != ReasonFailed {
		t.Fatalf("unexpected condition %+v", c)
	}
}

func TestRetryAfterReadsPendingErrorAndDefaults(t *testing.T) {
	if got := RetryAfter(&PendingError{RetryAfter: 42 * time.Second}); got != 42*time.Second {
		t.Fatalf("RetryAfter(pending) = %s", got)
	}
	if got := RetryAfter(errors.New("boom")); got != DefaultErrorRetry {
		t.Fatalf("RetryAfter(other) = %s, want %s", got, DefaultErrorRetry)
	}
}

func TestRunForceProceedsWhenWorkloadAPIIsUnreachable(t *testing.T) {
	cs := fake.NewClientset()
	cs.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("connection refused")
	})
	setter := &fakeConditionSetter{}

	opts := optionsAt(t0)
	opts.Force = true
	if err := Run(context.Background(), cs, setter, cluster(nil), testNode, opts); err != nil {
		t.Fatalf("force drain must not hold the operation on an API error, got %v", err)
	}
	if c := setter.last(t); c.status != conditionTrue || c.reason != ReasonForced {
		t.Fatalf("expected NodeDrain=True/Forced, got %+v", c)
	}
}

func TestRunMissingNodeHoldsAnUpgrade(t *testing.T) {
	setter := &fakeConditionSetter{}

	err := Run(context.Background(), fake.NewClientset(), setter, cluster(nil), testNode, optionsAt(t0))

	if !errors.Is(err, ErrIncomplete) {
		t.Fatalf("an upgrade must not reboot a node whose Kubernetes Node cannot be found, got %v", err)
	}
	if c := setter.last(t); c.status != conditionFalse || c.reason != ReasonFailed {
		t.Fatalf("unexpected condition %+v", c)
	}
}

func TestRunMissingNodeIsNothingToDrainForScaleDown(t *testing.T) {
	setter := &fakeConditionSetter{}
	opts := optionsAt(t0)
	opts.Purpose = PurposeScaleDown

	if err := Run(context.Background(), fake.NewClientset(), setter, cluster(nil), testNode, opts); err != nil {
		t.Fatalf("a machine that never joined has nothing to drain, got %v", err)
	}
	if c := setter.last(t); c.status != conditionTrue || !strings.Contains(c.message, "not found") {
		t.Fatalf("condition should say the node was not found, got %+v", c)
	}
}

func TestReportUnavailableHoldsUnlessForced(t *testing.T) {
	setter := &fakeConditionSetter{}
	cause := errors.New("failed to get talos secret")

	if err := ReportUnavailable(context.Background(), setter, cluster(nil), testNode, optionsAt(t0), cause); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("an unreachable workload cluster must hold the operation, got %v", err)
	}
	if c := setter.last(t); c.status != conditionFalse || c.reason != ReasonFailed {
		t.Fatalf("unexpected condition %+v", c)
	}

	opts := optionsAt(t0)
	opts.Force = true
	if err := ReportUnavailable(context.Background(), setter, cluster(nil), testNode, opts, cause); err != nil {
		t.Fatalf("force must let the operation proceed, got %v", err)
	}
	if c := setter.last(t); c.reason != ReasonForced {
		t.Fatalf("unexpected condition %+v", c)
	}
}
