package upgradeservice

import (
	"context"
	"strings"
	"testing"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/pkg/consts"
)

// A resume request with no persisted upgrade to resume can never be honoured;
// it must be dropped rather than returned as an error on every pass.
func TestHandleUpgrade_ResumeWithoutPersistedStateIsDropped(t *testing.T) {
	kc := legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation:  "v1.13.10",
		consts.TalosStatusAnnotation:   string(consts.UpgradeStatusFailed),
		consts.ResumeUpgradeAnnotation: "true",
	})
	kc.Status.Phase = status.PhaseUpgradeFailed
	f := newFixture(t, kc, fixtureSecret(upgradedSecret()))
	ctx := context.Background()

	if _, _, err := f.controller.HandleUpgrade(ctx, f.cluster(t), nil, nil); err != nil {
		t.Fatalf("HandleUpgrade: %v", err)
	}
	if v, ok := f.cluster(t).Annotations[consts.ResumeUpgradeAnnotation]; ok {
		t.Fatalf("resume still set to %q", v)
	}
	if _, handled, err := f.controller.HandleUpgrade(ctx, f.cluster(t), nil, nil); err != nil || handled {
		t.Fatalf("second HandleUpgrade handled=%t err=%v, want nothing to handle", handled, err)
	}
}

func TestHandleTalosResetUpgradeState_ClearsControlFlagsAndCondition(t *testing.T) {
	kc := finishedUpgradeStuckAsFailed()
	kc.Annotations[consts.TalosResetUpgradeStateAnnotation] = "true"
	kc.Annotations[consts.SkipFailedNodesAnnotation] = "true"
	f := newFixture(t, kc, fixtureSecret(upgradedSecret()))

	handled, err := f.controller.HandleTalosResetUpgradeState(context.Background(), f.cluster(t))
	if err != nil || !handled {
		t.Fatalf("HandleTalosResetUpgradeState handled=%t err=%v", handled, err)
	}

	stored := f.cluster(t)
	for _, key := range []string{
		consts.TalosResetUpgradeStateAnnotation,
		consts.TalosTargetAnnotation,
		consts.TalosStatusAnnotation,
		consts.TalosMessageAnnotation,
		consts.ResumeUpgradeAnnotation,
		consts.SkipFailedNodesAnnotation,
	} {
		if v, ok := stored.Annotations[key]; ok {
			t.Errorf("%s still set to %q", key, v)
		}
	}
	if stored.Status.Phase != status.PhaseReady {
		t.Errorf("phase = %q, want %q", stored.Status.Phase, status.PhaseReady)
	}
	if c := condition(stored, "TalosUpgrade"); c == nil || c.Reason != "Reset" || c.Status != "False" {
		t.Errorf("TalosUpgrade condition = %+v, want False/Reset", c)
	}
}

// d-amk-003, d-trd-atlas-001 and t-trd-obao-001 reported a Kubernetes upgrade
// "InProgress" for months after it stopped. With no upgrade running, the
// condition must stop claiming one is.
func TestHandleUpgrade_RetiresInProgressConditionWhenNothingRuns(t *testing.T) {
	kc := legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation:      "v1.13.10",
		consts.TalosStatusAnnotation:       string(consts.UpgradeStatusIdle),
		consts.KubernetesCurrentAnnotation: "1.36.1",
		consts.KubernetesStatusAnnotation:  string(consts.UpgradeStatusIdle),
	})
	kc.Status.Phase = status.PhaseReady
	kc.Status.Conditions = []vitistackv1alpha1.KubernetesClusterCondition{{
		Type:    "KubernetesUpgrade",
		Status:  "True",
		Reason:  "InProgress",
		Message: "Upgrading node d-amk-003-gsax-ctp0 (1/6 nodes completed)",
	}}
	f := newFixture(t, kc, fixtureSecret(upgradedSecret()))

	if _, handled, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, nil); err != nil || handled {
		t.Fatalf("HandleUpgrade handled=%t err=%v, want nothing to handle", handled, err)
	}

	c := condition(f.cluster(t), "KubernetesUpgrade")
	if c == nil || c.Status != "False" || c.Reason != "Interrupted" {
		t.Fatalf("KubernetesUpgrade condition = %+v, want False/Interrupted", c)
	}
	if !strings.Contains(c.Message, "1/6 nodes completed") {
		t.Errorf("message %q lost the last reported progress", c.Message)
	}
}

// d-trd-atlas-001 and t-trd-obao-001 have reported TalosUpgrade False/Failed
// since May and June with "target version 1.13.2 must be greater than current
// version 1.13.2", while the operator's own status says idle and no target is
// requested. Nothing rewrites a failed condition, so it would sit there until
// the next upgrade.
func TestHandleUpgrade_ClearsAFailureThatNoLongerApplies(t *testing.T) {
	kc := legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation: "1.13.2",
		consts.TalosStatusAnnotation:  string(consts.UpgradeStatusIdle),
		consts.TalosMessageAnnotation: "Upgrade available: 1.13.2 → 1.13.10",
	})
	kc.Status.Phase = status.PhaseReady
	kc.Status.Conditions = []vitistackv1alpha1.KubernetesClusterCondition{{
		Type:    "TalosUpgrade",
		Status:  "False",
		Reason:  "Failed",
		Message: "Talos upgrade to 1.13.2 failed: target version 1.13.2 must be greater than current version 1.13.2",
	}}
	f := newFixture(t, kc, fixtureSecret(upgradedSecret()))

	if _, handled, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, nil); err != nil || handled {
		t.Fatalf("HandleUpgrade handled=%t err=%v, want nothing to handle", handled, err)
	}

	c := condition(f.cluster(t), "TalosUpgrade")
	if c == nil || c.Status != "False" || c.Reason != "Cleared" {
		t.Fatalf("TalosUpgrade condition = %+v, want False/Cleared", c)
	}
	if strings.Contains(c.Message, "must be greater") {
		t.Errorf("message %q still reports the old failure", c.Message)
	}
	if !strings.Contains(c.Message, "1.13.2") {
		t.Errorf("message %q does not say what the cluster runs", c.Message)
	}
}

// A failure the operator still stands behind is left alone.
func TestHandleUpgrade_KeepsAFailureThatStillApplies(t *testing.T) {
	kc := legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation: "1.13.2",
		consts.TalosStatusAnnotation:  string(consts.UpgradeStatusFailed),
		consts.TalosMessageAnnotation: "Talos upgrade failed: node wrk4 did not come back online",
	})
	kc.Status.Phase = status.PhaseUpgradeFailed
	kc.Status.Conditions = []vitistackv1alpha1.KubernetesClusterCondition{{
		Type:    "TalosUpgrade",
		Status:  "False",
		Reason:  "Failed",
		Message: "Talos upgrade to 1.13.10 failed: node wrk4 did not come back online",
	}}
	f := newFixture(t, kc, fixtureSecret(upgradedSecret()))

	if _, _, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, nil); err != nil {
		t.Fatalf("HandleUpgrade: %v", err)
	}

	if c := condition(f.cluster(t), "TalosUpgrade"); c == nil || c.Reason != "Failed" {
		t.Fatalf("TalosUpgrade condition = %+v, want the failure kept", c)
	}
}

// A status annotation stuck at in-progress is itself left over: a real
// upgrade is driven by the persisted plan, which HandleUpgrade acts on before
// this point. The condition must not keep claiming progress because of it.
func TestHandleUpgrade_RetiresInProgressConditionDespiteStuckStatusAnnotation(t *testing.T) {
	kc := legacyCluster(map[string]string{
		consts.KubernetesCurrentAnnotation: "1.36.1",
		consts.KubernetesStatusAnnotation:  string(consts.UpgradeStatusInProgress),
	})
	kc.Status.Phase = status.PhaseUpgradingKubernetes
	kc.Status.Conditions = []vitistackv1alpha1.KubernetesClusterCondition{{
		Type:    "KubernetesUpgrade",
		Status:  "True",
		Reason:  "InProgress",
		Message: "Upgrading node d-amk-003-gsax-ctp0 (1/6 nodes completed)",
	}}
	f := newFixture(t, kc, fixtureSecret(upgradedSecret()))

	if _, _, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, nil); err != nil {
		t.Fatalf("HandleUpgrade: %v", err)
	}

	if c := condition(f.cluster(t), "KubernetesUpgrade"); c == nil || c.Reason != "Interrupted" {
		t.Fatalf("KubernetesUpgrade condition = %+v, want False/Interrupted", c)
	}
}
