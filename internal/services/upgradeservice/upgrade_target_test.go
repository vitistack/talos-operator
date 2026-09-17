package upgradeservice

import (
	"context"
	"testing"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/pkg/consts"
)

// d-amk-003 after its upgrade to v1.13.10 finished but the operator could not
// clear talos-target: the target equals the running version, the status says
// failed and the user asked to resume.
func finishedUpgradeStuckAsFailed() *vitistackv1alpha1.KubernetesCluster {
	kc := legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation:  "v1.13.10",
		consts.TalosTargetAnnotation:   "v1.13.10",
		consts.TalosStatusAnnotation:   string(consts.UpgradeStatusFailed),
		consts.TalosMessageAnnotation:  "Talos upgrade failed: target version v1.13.10 must be greater than current version v1.13.10",
		consts.ResumeUpgradeAnnotation: "true",
	})
	kc.Status.Phase = status.PhaseUpgradeFailed
	kc.Status.Conditions = []vitistackv1alpha1.KubernetesClusterCondition{{
		Type:    "TalosUpgrade",
		Status:  "False",
		Reason:  "Failed",
		Message: "Talos upgrade to v1.13.10 failed: target version v1.13.10 must be greater than current version v1.13.10",
	}}
	return kc
}

func upgradedSecret() map[string]string {
	return map[string]string{
		"talos_version":       "v1.13.10",
		"upgrade_in_progress": "false",
	}
}

func TestHandleUpgrade_TalosTargetEqualToCurrentCompletesWithoutUpgrading(t *testing.T) {
	f := newFixture(t, finishedUpgradeStuckAsFailed(), fixtureSecret(upgradedSecret()))
	ctx := context.Background()

	if _, _, err := f.controller.HandleUpgrade(ctx, f.cluster(t), nil, nil); err != nil {
		t.Fatalf("HandleUpgrade: %v", err)
	}

	kc := f.cluster(t)
	for _, key := range []string{consts.TalosTargetAnnotation, consts.ResumeUpgradeAnnotation} {
		if v, ok := kc.Annotations[key]; ok {
			t.Errorf("%s still set to %q", key, v)
		}
	}
	if got := consts.UpgradeStatus(kc.Annotations[consts.TalosStatusAnnotation]); got != consts.UpgradeStatusCompleted {
		t.Errorf("talos-status = %q, want %q", got, consts.UpgradeStatusCompleted)
	}
	if kc.Status.Phase != status.PhaseReady {
		t.Errorf("phase = %q, want %q", kc.Status.Phase, status.PhaseReady)
	}
	if c := condition(kc, "TalosUpgrade"); c == nil || c.Reason != "Completed" || c.Status != "False" {
		t.Errorf("TalosUpgrade condition = %+v, want False/Completed", c)
	}

	// The next pass has nothing left to do: no retry loop.
	if _, handled, err := f.controller.HandleUpgrade(ctx, f.cluster(t), nil, nil); err != nil || handled {
		t.Fatalf("second HandleUpgrade handled=%t err=%v, want nothing to handle", handled, err)
	}
}

// A rolling upgrade that failed leaves a plan behind. talos-current is the
// lowest version among the nodes that ANSWER, so a node that failed and went
// unreachable can let it reach the target while that node is still behind.
// Closing the request as completed there would erase a real failure.
func TestHandleUpgrade_TargetEqualToCurrentKeepsAFailedUpgradeVisible(t *testing.T) {
	kc := finishedUpgradeStuckAsFailed()
	delete(kc.Annotations, consts.ResumeUpgradeAnnotation)
	f := newFixture(t, kc, fixtureSecret(map[string]string{
		"talos_version": "v1.13.10",
		"upgrade_state": `{"upgradeType":"talos","targetVersion":"v1.13.10","phase":"failed","failedNodeName":"d-amk-003-gsax-wrk4","failedReason":"node did not come back online"}`,
	}))

	if _, handled, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, nil); err != nil || handled {
		t.Fatalf("HandleUpgrade handled=%t err=%v, want the failure left for the user", handled, err)
	}

	stored := f.cluster(t)
	if got := consts.UpgradeStatus(stored.Annotations[consts.TalosStatusAnnotation]); got != consts.UpgradeStatusFailed {
		t.Errorf("talos-status = %q, want it still %q", got, consts.UpgradeStatusFailed)
	}
	if got := stored.Annotations[consts.TalosTargetAnnotation]; got != "v1.13.10" {
		t.Errorf("talos-target = %q, want it kept so resume still applies", got)
	}
	if data := f.secretData(t); data["upgrade_state"] == "" {
		t.Error("the failed upgrade plan was cleared; resume would have nothing to resume")
	}
}

// The target is a user request; an unknown current version is a temporary
// state (the operator fills it in on the same pass), not a bad request.
func TestHandleUpgrade_TargetKeptWhileCurrentVersionUnknown(t *testing.T) {
	kc := legacyCluster(map[string]string{
		consts.TalosTargetAnnotation: "v1.13.10",
	})
	kc.Status.Phase = status.PhaseReady
	f := newFixture(t, kc, fixtureSecret(upgradedSecret()))

	if _, handled, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, nil); err != nil || handled {
		t.Fatalf("HandleUpgrade handled=%t err=%v, want the pass to fall through", handled, err)
	}

	stored := f.cluster(t)
	if got := stored.Annotations[consts.TalosTargetAnnotation]; got != "v1.13.10" {
		t.Errorf("talos-target = %q, want the request kept", got)
	}
	if got := consts.UpgradeStatus(stored.Annotations[consts.TalosStatusAnnotation]); got == consts.UpgradeStatusFailed {
		t.Error("talos-status = failed, want no failure for a version the operator has not read yet")
	}
}

// A target the operator will never accept (here a downgrade) is reported once
// and then dropped, instead of being re-validated and re-failed every 30s.
func TestHandleUpgrade_InvalidTalosTargetFailsOnce(t *testing.T) {
	kc := legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation: "v1.13.10",
		consts.TalosTargetAnnotation:  "v1.13.2",
		consts.TalosStatusAnnotation:  string(consts.UpgradeStatusIdle),
	})
	kc.Status.Phase = status.PhaseReady
	f := newFixture(t, kc, fixtureSecret(upgradedSecret()))
	ctx := context.Background()

	if _, _, err := f.controller.HandleUpgrade(ctx, f.cluster(t), nil, nil); err != nil {
		t.Fatalf("HandleUpgrade: %v", err)
	}

	stored := f.cluster(t)
	if v, ok := stored.Annotations[consts.TalosTargetAnnotation]; ok {
		t.Errorf("talos-target still set to %q", v)
	}
	if got := consts.UpgradeStatus(stored.Annotations[consts.TalosStatusAnnotation]); got != consts.UpgradeStatusFailed {
		t.Errorf("talos-status = %q, want %q", got, consts.UpgradeStatusFailed)
	}

	if _, handled, err := f.controller.HandleUpgrade(ctx, f.cluster(t), nil, nil); err != nil || handled {
		t.Fatalf("second HandleUpgrade handled=%t err=%v, want nothing to handle", handled, err)
	}
}

func TestHandleUpgrade_InvalidKubernetesTargetFailsOnce(t *testing.T) {
	kc := legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation:      "v1.13.10",
		consts.KubernetesCurrentAnnotation: "1.36.1",
		consts.KubernetesTargetAnnotation:  "1.35.0",
		consts.KubernetesStatusAnnotation:  string(consts.UpgradeStatusIdle),
	})
	kc.Status.Phase = status.PhaseReady
	f := newFixture(t, kc, fixtureSecret(upgradedSecret()))
	ctx := context.Background()

	if _, _, err := f.controller.HandleUpgrade(ctx, f.cluster(t), nil, nil); err != nil {
		t.Fatalf("HandleUpgrade: %v", err)
	}

	stored := f.cluster(t)
	if v, ok := stored.Annotations[consts.KubernetesTargetAnnotation]; ok {
		t.Errorf("kubernetes-target still set to %q", v)
	}
	if got := consts.UpgradeStatus(stored.Annotations[consts.KubernetesStatusAnnotation]); got != consts.UpgradeStatusFailed {
		t.Errorf("kubernetes-status = %q, want %q", got, consts.UpgradeStatusFailed)
	}

	if _, handled, err := f.controller.HandleUpgrade(ctx, f.cluster(t), nil, nil); err != nil || handled {
		t.Fatalf("second HandleUpgrade handled=%t err=%v, want nothing to handle", handled, err)
	}
}
