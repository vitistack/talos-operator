package upgradeservice

import (
	"context"
	"errors"
	"testing"

	"github.com/vitistack/talos-operator/pkg/consts"
)

// Only a completed orchestrated upgrade wrote talos_version, so 152 of 293
// clusters had none and 9 had a stale one while talos-current was right.
func TestReconcileTalosCurrentVersion_KeepsSecretInStep(t *testing.T) {
	tests := []struct {
		name   string
		stored string
	}{
		{name: "missing", stored: ""},
		{name: "stale", stored: "1.13.6"},
		{name: "ahead of the nodes", stored: "1.13.10"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := map[string]string{}
			if tt.stored != "" {
				data["talos_version"] = tt.stored
			}
			kc := legacyCluster(map[string]string{consts.TalosCurrentAnnotation: "v1.12.7"})
			f := newFixture(t, kc, fixtureSecret(data))

			if err := f.service.ReconcileTalosCurrentVersion(context.Background(), f.cluster(t), "1.12.7"); err != nil {
				t.Fatal(err)
			}
			if got := f.secretData(t)["talos_version"]; got != "v1.12.7" {
				t.Errorf("talos_version = %q, want v1.12.7", got)
			}
		})
	}
}

func TestReconcileTalosCurrentVersion_NoWriteWhenSecretMatches(t *testing.T) {
	kc := legacyCluster(map[string]string{consts.TalosCurrentAnnotation: "v1.13.12"})
	f := newFixture(t, kc, fixtureSecret(map[string]string{"talos_version": "1.13.12"}))
	before := f.secretVersion(t)

	if err := f.service.ReconcileTalosCurrentVersion(context.Background(), f.cluster(t), "v1.13.12"); err != nil {
		t.Fatal(err)
	}
	if after := f.secretVersion(t); after != before {
		t.Error("secret written although talos_version already matched (only the v prefix differs)")
	}
}

// A completion whose secret write was lost used to leave
// upgrade_in_progress=true with no plan to retry it, and the version catch-up
// and enforcement skipped the cluster from then on.
func TestHandleUpgrade_ClearsStaleUpgradeInProgress(t *testing.T) {
	f := newFixture(t, legacyCluster(nil), fixtureSecret(map[string]string{
		"upgrade_in_progress": "true",
		"upgrade_type":        "talos",
		"upgrade_target":      "v1.13.12",
	}))

	if _, _, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := f.secretData(t)["upgrade_in_progress"]; got != "false" {
		t.Errorf("upgrade_in_progress = %q, want false", got)
	}
}

func TestHandleUpgrade_KeepsCompletedPlanWhenCompletionIsNotRecorded(t *testing.T) {
	plan := rollingPlan(func(s *ClusterUpgradeState) { s.Phase = UpgradePhaseCompleted })
	f := newFixture(t, legacyCluster(map[string]string{consts.TalosTargetAnnotation: "v1.13.12"}),
		fixtureSecret(map[string]string{"upgrade_state": planJSON(t, plan), "upgrade_in_progress": "true"}))
	f.failPatch = errors.New("apiserver unavailable")

	requeue, handled, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, nil)
	if err != nil || !handled || requeue != completionRetryDelay {
		t.Fatalf("HandleUpgrade requeue=%s handled=%t err=%v, want a retry in %s", requeue, handled, err, completionRetryDelay)
	}
	if f.storedPlan(t) == nil {
		t.Fatal("completed plan cleared although the completion was not recorded; nothing would retry it")
	}

	// Once writes go through, the next pass records it and clears the plan.
	f.failPatch = nil
	if _, _, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, nil); err != nil {
		t.Fatal(err)
	}
	if f.storedPlan(t) != nil {
		t.Error("completed plan still stored after a successful completion")
	}
	data := f.secretData(t)
	if data["upgrade_in_progress"] != "false" || data["talos_version"] != "v1.13.12" {
		t.Errorf("secret upgrade_in_progress=%q talos_version=%q, want false and v1.13.12", data["upgrade_in_progress"], data["talos_version"])
	}
}
