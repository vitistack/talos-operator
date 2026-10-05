package upgradeservice

import (
	"context"
	"fmt"
	"strings"
	"testing"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/spf13/viper"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/pkg/consts"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func withOperatorTalosVersion(t *testing.T, v string) {
	t.Helper()
	prev := viper.Get(consts.TALOS_VERSION)
	viper.Set(consts.TALOS_VERSION, v)
	t.Cleanup(func() { viper.Set(consts.TALOS_VERSION, prev) })
}

// machinesNamed returns machines with an IPv4 address each, as the rolling
// upgrade needs to include them in its plan.
func machinesNamed(names ...string) []vitistackv1alpha1.Machine {
	out := make([]vitistackv1alpha1.Machine, len(names))
	for i, n := range names {
		out[i] = vitistackv1alpha1.Machine{ObjectMeta: metav1.ObjectMeta{Name: n}}
		out[i].Status.PublicIPAddresses = []string{fmt.Sprintf("192.0.2.%d", i+10)}
	}
	return out
}

// nodesRun makes the controller's Talos version probe report versions.
// Machines missing from versions are unreachable.
func (f *fixture) nodesRun(versions map[string]string) {
	f.controller.probeTalosVersions = func(context.Context, *clientconfig.Config, []vitistackv1alpha1.Machine) map[string]string {
		return versions
	}
}

func TestCheckForAvailableUpgradesAdvertisesTheNextStepNotTheFinalVersion(t *testing.T) {
	withOperatorTalosVersion(t, "v1.14.2")
	tests := []struct{ current, want string }{
		{"v1.12.7", "1.12.12"},
		{"v1.12.12", "1.13.11"},
		{"v1.13.11", "1.14.2"},
	}
	for _, tt := range tests {
		t.Run(tt.current, func(t *testing.T) {
			f := newFixture(t, legacyCluster(map[string]string{consts.TalosCurrentAnnotation: tt.current}), fixtureSecret(nil))
			if err := f.service.CheckForAvailableUpgrades(context.Background(), f.cluster(t)); err != nil {
				t.Fatal(err)
			}
			if got := f.cluster(t).Annotations[consts.TalosAvailableAnnotation]; got != tt.want {
				t.Errorf("talos-available = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCheckForAvailableUpgradesReplacesAVersionThatSkipsMinors(t *testing.T) {
	withOperatorTalosVersion(t, "v1.14.2")
	f := newFixture(t, legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation:   "v1.12.7",
		consts.TalosAvailableAnnotation: "1.14.1", // advertised before this fix
	}), fixtureSecret(nil))

	if err := f.service.CheckForAvailableUpgrades(context.Background(), f.cluster(t)); err != nil {
		t.Fatal(err)
	}
	if got := f.cluster(t).Annotations[consts.TalosAvailableAnnotation]; got != "1.12.12" {
		t.Errorf("talos-available = %q, want the next step 1.12.12", got)
	}
}

func TestCheckForAvailableUpgradesWithholdsAStepTheKubernetesVersionCannotRunOn(t *testing.T) {
	withOperatorTalosVersion(t, "v1.13.11")
	f := newFixture(t, legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation:      "v1.12.12",
		consts.KubernetesCurrentAnnotation: "1.30.5", // Talos 1.13 needs Kubernetes >= 1.31
		consts.TalosAvailableAnnotation:    "1.13.11",
	}), fixtureSecret(nil))

	if err := f.service.CheckForAvailableUpgrades(context.Background(), f.cluster(t)); err != nil {
		t.Fatal(err)
	}
	kc := f.cluster(t)
	if got, ok := kc.Annotations[consts.TalosAvailableAnnotation]; ok {
		t.Errorf("talos-available = %q, want it withdrawn until Kubernetes is upgraded", got)
	}
	if msg := kc.Annotations[consts.TalosMessageAnnotation]; !strings.Contains(msg, "1.13.11") || !strings.Contains(msg, "Kubernetes") {
		t.Errorf("talos-message should explain the Kubernetes requirement, got %q", msg)
	}
}

func TestCheckForAvailableUpgradesWithdrawsWhenThePathIsUnknown(t *testing.T) {
	withOperatorTalosVersion(t, "v1.12.12")
	f := newFixture(t, legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation:   "v1.10.3", // latest 1.10 patch not in the table
		consts.TalosAvailableAnnotation: "1.12.12",
	}), fixtureSecret(nil))

	if err := f.service.CheckForAvailableUpgrades(context.Background(), f.cluster(t)); err != nil {
		t.Fatal(err)
	}
	kc := f.cluster(t)
	if got, ok := kc.Annotations[consts.TalosAvailableAnnotation]; ok {
		t.Errorf("talos-available = %q, want it withdrawn while the path is unknown", got)
	}
	if msg := kc.Annotations[consts.TalosMessageAnnotation]; !strings.Contains(msg, consts.TALOS_LATEST_PATCH_VERSIONS) {
		t.Errorf("talos-message should say how to fix the path, got %q", msg)
	}
}

// The Trondheim case from issue #61: talos-current says 1.13.2 while every
// node still runs 1.12.7, and 1.14.1 is requested.
func trondheimCluster(target string) *vitistackv1alpha1.KubernetesCluster {
	return legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation: "v1.13.2",
		consts.TalosTargetAnnotation:  target,
		consts.TalosStatusAnnotation:  string(consts.UpgradeStatusIdle),
	})
}

func TestPlanTalosUpgradeWalksTowardsAFarTarget(t *testing.T) {
	f := newFixture(t, legacyCluster(nil), fixtureSecret(nil))
	tests := []struct {
		name     string
		state    UpgradeState
		versions map[string]string
		want     string
	}{
		{"patch up the running minor first", UpgradeState{TalosCurrent: "v1.12.7", TalosTarget: "1.14.1"}, map[string]string{"cp-0": "v1.12.7"}, "v1.12.12"},
		{"then the next minor's latest patch", UpgradeState{TalosCurrent: "v1.12.12", TalosTarget: "1.14.1"}, map[string]string{"cp-0": "v1.12.12"}, "v1.13.11"},
		{"final hop to the target", UpgradeState{TalosCurrent: "v1.13.11", TalosTarget: "1.14.1"}, map[string]string{"cp-0": "v1.13.11"}, "v1.14.1"},
		{"a target that is the next step is rolled as is", UpgradeState{TalosCurrent: "v1.12.12", TalosTarget: "1.13.10"}, nil, "v1.13.10"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := f.controller.planTalosUpgrade(&tt.state, tt.versions)
			if err != nil || got != tt.want {
				t.Fatalf("planTalosUpgrade() = (%q, %v), want %q", got, err, tt.want)
			}
		})
	}
}

func TestPlanTalosUpgradeRefusesWhatNoStepCanReach(t *testing.T) {
	f := newFixture(t, legacyCluster(nil), fixtureSecret(nil))
	tests := []struct {
		name     string
		state    UpgradeState
		versions map[string]string
		mention  string
	}{
		{"next step cannot run the Kubernetes version", UpgradeState{TalosCurrent: "v1.12.12", TalosTarget: "1.13.11", KubernetesCurrent: "1.30.5"}, nil, "1.30.5"},
		{"target below a node that is ahead", UpgradeState{TalosCurrent: "v1.12.7", TalosTarget: "1.12.10"}, map[string]string{"cp-0": "v1.12.12", "wrk-0": "v1.12.7"}, "1.12.12"},
		{"latest patch of the running minor unknown", UpgradeState{TalosCurrent: "v1.10.3", TalosTarget: "1.12.12"}, nil, consts.TALOS_LATEST_PATCH_VERSIONS},
		{"not an upgrade", UpgradeState{TalosCurrent: "v1.13.11", TalosTarget: "1.13.10"}, nil, "greater"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := f.controller.planTalosUpgrade(&tt.state, tt.versions)
			if err == nil || !strings.Contains(err.Error(), tt.mention) {
				t.Fatalf("planTalosUpgrade() error = %v, want one mentioning %q", err, tt.mention)
			}
		})
	}
}

func TestStartTalosUpgradeWalksFromWhatTheNodesRun(t *testing.T) {
	withOperatorTalosVersion(t, "v1.14.2")
	f := newFixture(t, trondheimCluster("1.14.1"), fixtureSecret(nil))
	f.nodesRun(map[string]string{"cp-0": "v1.12.7", "wrk-0": "v1.12.7"})

	if _, _, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, machinesNamed("cp-0", "wrk-0")); err != nil {
		t.Fatalf("HandleUpgrade: %v", err)
	}

	kc := f.cluster(t)
	if got := kc.Annotations[consts.TalosCurrentAnnotation]; got != "v1.12.7" {
		t.Errorf("talos-current = %q, want it corrected to what the nodes run (v1.12.7)", got)
	}
	if got := kc.Annotations[consts.TalosTargetAnnotation]; got != "1.14.1" {
		t.Errorf("talos-target = %q, want the far target kept as the goal of the walk", got)
	}
	// The fixture has no installer image, so the first step fails there; what
	// matters is that the request was planned, not rejected on the path.
	if msg := kc.Annotations[consts.TalosMessageAnnotation]; strings.Contains(msg, "cannot upgrade") || strings.Contains(msg, "first") {
		t.Errorf("a far target must be walked, not rejected: %q", msg)
	}
}

func TestStartTalosUpgradeDoesNotCloseARequestTheNodesHaveNotReached(t *testing.T) {
	withOperatorTalosVersion(t, "v1.14.2")
	// talos-current already reads the requested version, but the nodes do not run it.
	f := newFixture(t, trondheimCluster("v1.13.2"), fixtureSecret(nil))
	f.nodesRun(map[string]string{"cp-0": "v1.12.7"})

	if _, _, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, machinesNamed("cp-0")); err != nil {
		t.Fatalf("HandleUpgrade: %v", err)
	}
	if got := consts.UpgradeStatus(f.cluster(t).Annotations[consts.TalosStatusAnnotation]); got == consts.UpgradeStatusCompleted {
		t.Fatal("a target the nodes do not run must not be closed as already running")
	}
}

func TestStartTalosUpgradeHoldsWhileANodeCannotBeAsked(t *testing.T) {
	withOperatorTalosVersion(t, "v1.14.2")
	f := newFixture(t, legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation: "v1.12.12",
		consts.TalosTargetAnnotation:  "1.13.11",
		consts.TalosStatusAnnotation:  string(consts.UpgradeStatusIdle),
	}), fixtureSecret(nil))
	f.nodesRun(map[string]string{"cp-0": "v1.12.12"}) // wrk-0 does not answer

	requeue, handled, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, machinesNamed("cp-0", "wrk-0"))
	if err != nil || !handled || requeue <= 0 {
		t.Fatalf("HandleUpgrade = (%s, %t, %v), want a requeue", requeue, handled, err)
	}
	kc := f.cluster(t)
	if kc.Annotations[consts.TalosTargetAnnotation] != "1.13.11" {
		t.Error("the request must be kept while a node's version cannot be read")
	}
	if got := consts.UpgradeStatus(kc.Annotations[consts.TalosStatusAnnotation]); got == consts.UpgradeStatusFailed {
		t.Error("an unreachable node is not a reason to fail the request")
	}
	if msg := kc.Annotations[consts.TalosMessageAnnotation]; !strings.Contains(msg, "wrk-0") {
		t.Errorf("talos-message should name the node that did not answer, got %q", msg)
	}
}

func TestStartTalosUpgradeIgnoresMachinesWithoutAnAddress(t *testing.T) {
	withOperatorTalosVersion(t, "v1.14.2")
	f := newFixture(t, legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation: "v1.12.12",
		consts.TalosTargetAnnotation:  "1.13.11",
		consts.TalosStatusAnnotation:  string(consts.UpgradeStatusIdle),
	}), fixtureSecret(nil))
	f.nodesRun(map[string]string{"cp-0": "v1.12.12"})
	machines := machinesNamed("cp-0", "new-0")
	machines[1].Status.PublicIPAddresses = nil // still provisioning; the upgrade plan skips it too

	if _, _, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, machines); err != nil {
		t.Fatalf("HandleUpgrade: %v", err)
	}
	if msg := f.cluster(t).Annotations[consts.TalosMessageAnnotation]; strings.Contains(msg, "new-0") {
		t.Errorf("a machine without an address is not part of the upgrade and must not hold it, got %q", msg)
	}
}

func TestWithdrawingAnOfferKeepsAFailureMessage(t *testing.T) {
	withOperatorTalosVersion(t, "v1.12.12")
	const failure = "Talos upgrade failed: node wrk-0 did not come back"
	f := newFixture(t, legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation:   "v1.10.3", // path unknown, so the offer is withdrawn
		consts.TalosAvailableAnnotation: "1.12.12",
		consts.TalosStatusAnnotation:    string(consts.UpgradeStatusFailed),
		consts.TalosMessageAnnotation:   failure,
	}), fixtureSecret(nil))

	if err := f.service.CheckForAvailableUpgrades(context.Background(), f.cluster(t)); err != nil {
		t.Fatal(err)
	}
	kc := f.cluster(t)
	if _, ok := kc.Annotations[consts.TalosAvailableAnnotation]; ok {
		t.Error("the offer should still be withdrawn")
	}
	if got := kc.Annotations[consts.TalosMessageAnnotation]; got != failure {
		t.Errorf("talos-message = %q, want the failure kept", got)
	}
}

func completedStepSecret(step string) *corev1.Secret {
	return fixtureSecret(map[string]string{
		"upgrade_state": fmt.Sprintf(`{"upgradeType":"talos","targetVersion":%q,"installerImage":"factory.talos.dev/installer/abc:%s","phase":"completed"}`, step, step),
	})
}

func TestCompletedStepKeepsTheTargetAndContinues(t *testing.T) {
	f := newFixture(t, legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation: "v1.12.7",
		consts.TalosTargetAnnotation:  "1.14.1",
		consts.TalosStatusAnnotation:  string(consts.UpgradeStatusInProgress),
	}), completedStepSecret("v1.12.12"))

	if _, _, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, nil); err != nil {
		t.Fatalf("HandleUpgrade: %v", err)
	}
	kc := f.cluster(t)
	if got := kc.Annotations[consts.TalosTargetAnnotation]; got != "1.14.1" {
		t.Errorf("talos-target = %q, want it kept until the walk reaches it", got)
	}
	if got := kc.Annotations[consts.TalosCurrentAnnotation]; got != "v1.12.12" {
		t.Errorf("talos-current = %q, want the completed step", got)
	}
	if msg := kc.Annotations[consts.TalosMessageAnnotation]; !strings.Contains(msg, "1.14.1") {
		t.Errorf("talos-message should say the walk continues towards 1.14.1, got %q", msg)
	}
}

func TestCompletedFinalStepClosesTheRequest(t *testing.T) {
	f := newFixture(t, legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation: "v1.13.11",
		consts.TalosTargetAnnotation:  "1.14.1",
		consts.TalosStatusAnnotation:  string(consts.UpgradeStatusInProgress),
	}), completedStepSecret("v1.14.1"))

	if _, _, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, nil); err != nil {
		t.Fatalf("HandleUpgrade: %v", err)
	}
	if got, ok := f.cluster(t).Annotations[consts.TalosTargetAnnotation]; ok {
		t.Errorf("talos-target = %q, want it removed once reached", got)
	}
}

func TestStartTalosUpgradeSkipFailedNodesProceedsWithoutASilentNode(t *testing.T) {
	withOperatorTalosVersion(t, "v1.14.2")
	f := newFixture(t, legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation:    "v1.12.12",
		consts.TalosTargetAnnotation:     "1.13.11",
		consts.TalosStatusAnnotation:     string(consts.UpgradeStatusIdle),
		consts.SkipFailedNodesAnnotation: "true",
	}), fixtureSecret(nil))
	f.nodesRun(map[string]string{"cp-0": "v1.12.12"}) // wrk-0 is gone

	if _, _, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, machinesNamed("cp-0", "wrk-0")); err != nil {
		t.Fatalf("HandleUpgrade: %v", err)
	}
	if msg := f.cluster(t).Annotations[consts.TalosMessageAnnotation]; strings.Contains(msg, "Waiting to start") {
		t.Errorf("skip-failed-nodes should let the upgrade start without the silent node, got %q", msg)
	}
}

func TestStartTalosUpgradeTreatsAnUnreadableVersionAsNoAnswer(t *testing.T) {
	withOperatorTalosVersion(t, "v1.14.2")
	f := newFixture(t, legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation: "v1.12.12",
		consts.TalosTargetAnnotation:  "1.13.11",
		consts.TalosStatusAnnotation:  string(consts.UpgradeStatusIdle),
	}), fixtureSecret(nil))
	f.nodesRun(map[string]string{"cp-0": "v1.12.12", "wrk-0": ""})

	if _, _, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, machinesNamed("cp-0", "wrk-0")); err != nil {
		t.Fatalf("HandleUpgrade: %v", err)
	}
	if msg := f.cluster(t).Annotations[consts.TalosMessageAnnotation]; !strings.Contains(msg, "Waiting to start") || !strings.Contains(msg, "wrk-0") {
		t.Errorf("a node whose version cannot be read must hold the start like a silent one, got %q", msg)
	}
}

func TestStartTalosUpgradeRejectsWhatNoStepCanReach(t *testing.T) {
	withOperatorTalosVersion(t, "v1.13.11")
	f := newFixture(t, legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation:      "v1.12.12",
		consts.KubernetesCurrentAnnotation: "1.30.5", // Talos 1.13 needs Kubernetes >= 1.31
		consts.TalosTargetAnnotation:       "1.13.11",
		consts.TalosStatusAnnotation:       string(consts.UpgradeStatusIdle),
	}), fixtureSecret(nil))
	f.nodesRun(map[string]string{"cp-0": "v1.12.12"})

	if _, _, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, machinesNamed("cp-0")); err != nil {
		t.Fatalf("HandleUpgrade: %v", err)
	}
	kc := f.cluster(t)
	if got := consts.UpgradeStatus(kc.Annotations[consts.TalosStatusAnnotation]); got != consts.UpgradeStatusFailed {
		t.Fatalf("talos-status = %q, want rejected", got)
	}
	if msg := kc.Annotations[consts.TalosMessageAnnotation]; !strings.Contains(msg, "1.30.5") {
		t.Errorf("rejection should name the Kubernetes version, got %q", msg)
	}
	if _, ok := kc.Annotations[consts.TalosTargetAnnotation]; ok {
		t.Error("a target no step can reach should be dropped so it is not re-evaluated every pass")
	}
}

func TestStartTalosUpgradeRollsTheStepNotTheFarTarget(t *testing.T) {
	withOperatorTalosVersion(t, "v1.14.2")
	const pinned = "factory.talos.dev/nocloud-installer/abc123:v1.12.7"
	f := newFixture(t, legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation: "v1.12.7",
		consts.TalosTargetAnnotation:  "1.14.1",
		consts.TalosStatusAnnotation:  string(consts.UpgradeStatusIdle),
	}), fixtureSecret(map[string]string{"install_image": pinned}))
	f.nodesRun(map[string]string{"cp-0": "v1.12.7"})
	f.service.healthCheck = (&healthStub{}).check
	// Cancelled: the rolling step after planning must not dial the nodes.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, _ = f.controller.HandleUpgrade(ctx, f.cluster(t), nil, machinesNamed("cp-0"))

	plan, err := f.controller.stateManager.GetUpgradeState(context.Background(), f.cluster(t))
	if err != nil || plan == nil {
		t.Fatalf("expected a stored upgrade plan, got %+v, %v", plan, err)
	}
	if plan.TargetVersion != "v1.12.12" {
		t.Errorf("plan target = %q, want the first step v1.12.12", plan.TargetVersion)
	}
	if !strings.HasSuffix(plan.InstallerImage, ":v1.12.12") {
		t.Errorf("installer image = %q, want it tagged with the step, not the far target", plan.InstallerImage)
	}
}
