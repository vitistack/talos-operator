package upgradeservice

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/vitistack/talos-operator/pkg/consts"
)

func TestClassifyTalosNode(t *testing.T) {
	tests := []struct {
		name            string
		before, now     string
		running, target string
		want            talosNodeProgress
	}{
		{name: "rebooted into the target", before: "boot-a", now: "boot-b", running: "v1.13.12", target: "v1.13.12", want: talosNodeOnTarget},
		{name: "target without v prefix", before: "boot-a", now: "boot-b", running: "v1.13.12", target: "1.13.12", want: talosNodeOnTarget},
		{name: "not rebooted yet", before: "boot-a", now: "boot-a", running: "v1.13.11", target: "v1.13.12", want: talosNodePending},
		{name: "rebooted into the old version", before: "boot-a", now: "boot-b", running: "v1.13.11", target: "v1.13.12", want: talosNodeRolledBack},
		{name: "on target without a reboot", before: "boot-a", now: "boot-a", running: "v1.13.12", target: "v1.13.12", want: talosNodePending},
		{name: "boot ID unknown before, judged by version", before: "", now: "boot-b", running: "v1.13.12", target: "v1.13.12", want: talosNodeOnTarget},
		{name: "boot ID unreadable now, judged by version", before: "boot-a", now: "", running: "v1.13.12", target: "v1.13.12", want: talosNodeOnTarget},
		{name: "boot ID unknown and old version is not a rollback", before: "", now: "boot-b", running: "v1.13.11", target: "v1.13.12", want: talosNodePending},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyTalosNode(tt.before, tt.now, tt.running, tt.target); got != tt.want {
				t.Errorf("classifyTalosNode = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeTimedOut(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	timeout := 15 * time.Minute
	tests := []struct {
		name        string
		node        NodeUpgradeState
		upgradeType UpgradeType
		wantTimeout bool
		wantStep    string
	}{
		{name: "not started", node: NodeUpgradeState{}, upgradeType: UpgradeTypeTalos},
		{name: "within the timeout", node: NodeUpgradeState{StartedAt: now.Add(-10 * time.Minute), UpgradeInitiated: true}, upgradeType: UpgradeTypeTalos},
		{
			name:        "Talos node never came back on the target",
			node:        NodeUpgradeState{StartedAt: now.Add(-16 * time.Minute), UpgradeInitiated: true},
			upgradeType: UpgradeTypeTalos, wantTimeout: true, wantStep: "reboot",
		},
		{
			name:        "Kubernetes node kubelet never reported the target",
			node:        NodeUpgradeState{StartedAt: now.Add(-16 * time.Minute), UpgradeInitiated: true},
			upgradeType: UpgradeTypeKubernetes, wantTimeout: true, wantStep: "kubelet",
		},
		{
			name:        "upgraded but never Ready",
			node:        NodeUpgradeState{StartedAt: now.Add(-16 * time.Minute), UpgradeInitiated: true, UpgradeCompleted: true},
			upgradeType: UpgradeTypeTalos, wantTimeout: true, wantStep: "Ready",
		},
		{
			name:        "Ready nodes never time out",
			node:        NodeUpgradeState{StartedAt: now.Add(-time.Hour), UpgradeInitiated: true, UpgradeCompleted: true, NodeReady: true},
			upgradeType: UpgradeTypeTalos,
		},
		{
			// Plans stored by an operator before StartedAt existed.
			name:        "falls back to InitiatedAt",
			node:        NodeUpgradeState{InitiatedAt: now.Add(-16 * time.Minute), UpgradeInitiated: true},
			upgradeType: UpgradeTypeTalos, wantTimeout: true, wantStep: "reboot",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step, timedOut := nodeTimedOut(&tt.node, tt.upgradeType, now, timeout)
			if timedOut != tt.wantTimeout {
				t.Fatalf("timedOut = %t, want %t", timedOut, tt.wantTimeout)
			}
			if !strings.Contains(step, tt.wantStep) {
				t.Errorf("step = %q, want it to mention %q", step, tt.wantStep)
			}
		})
	}
}

func TestFailedPlanBlocksStart(t *testing.T) {
	failedTalos := &ClusterUpgradeState{UpgradeType: UpgradeTypeTalos, TargetVersion: "v1.13.12", Phase: UpgradePhaseFailed}
	tests := []struct {
		name        string
		state       *ClusterUpgradeState
		upgradeType UpgradeType
		target      string
		want        bool
	}{
		{name: "no plan", state: nil, upgradeType: UpgradeTypeTalos, target: "v1.13.12"},
		{name: "plan not failed", state: &ClusterUpgradeState{UpgradeType: UpgradeTypeTalos, TargetVersion: "v1.13.12", Phase: UpgradePhaseCompleted}, upgradeType: UpgradeTypeTalos, target: "v1.13.12"},
		{name: "same target waits for resume", state: failedTalos, upgradeType: UpgradeTypeTalos, target: "1.13.12", want: true},
		{name: "new target replaces the failed plan", state: failedTalos, upgradeType: UpgradeTypeTalos, target: "v1.13.13"},
		{name: "other kind waits for the failed one", state: failedTalos, upgradeType: UpgradeTypeKubernetes, target: "1.36.5", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := failedPlanBlocksStart(tt.state, tt.upgradeType, tt.target); got != tt.want {
				t.Errorf("failedPlanBlocksStart = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestUpgradePacingSettings(t *testing.T) {
	if got := nodeUpgradeTimeout(); got != defaultNodeUpgradeTimeout {
		t.Errorf("unset node timeout = %s, want %s", got, defaultNodeUpgradeTimeout)
	}
	if got := nodeSettleDuration(); got != defaultNodeSettleDuration {
		t.Errorf("unset settle = %s, want %s", got, defaultNodeSettleDuration)
	}

	setViper(t, consts.UPGRADE_NODE_TIMEOUT_MINUTES, "25")
	setViper(t, consts.UPGRADE_ETCD_HEALTHY_TIMEOUT_MINUTES, "0")
	setViper(t, consts.UPGRADE_NODE_SETTLE_SECONDS, "0")

	if got := nodeUpgradeTimeout(); got != 25*time.Minute {
		t.Errorf("node timeout = %s, want 25m", got)
	}
	if got := etcdHealthyTimeout(); got != defaultEtcdHealthyTimeout {
		t.Errorf("etcd timeout below the minimum = %s, want the default %s", got, defaultEtcdHealthyTimeout)
	}
	if got := nodeSettleDuration(); got != 0 {
		t.Errorf("settle = %s, want 0 (pause disabled)", got)
	}
}

func TestResetFailedNodeForResume(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	state := &ClusterUpgradeState{
		EtcdUnhealthySince: now.Add(-6 * time.Minute),
		Nodes: []NodeUpgradeState{
			{NodeName: "rolled-back", UpgradeInitiated: true, StartedAt: now.Add(-20 * time.Minute), InitiatedAt: now.Add(-18 * time.Minute), BootIDBefore: "boot-a", Error: "rolled back"},
			{NodeName: "never-ready", UpgradeInitiated: true, UpgradeCompleted: true, StartedAt: now.Add(-20 * time.Minute), Error: "timed out"},
		},
	}

	state.resetFailedNodeForResume(0, now)
	if n := state.Nodes[0]; n.Error != "" || n.UpgradeInitiated || !n.StartedAt.IsZero() || !n.InitiatedAt.IsZero() || n.BootIDBefore != "" {
		t.Errorf("unfinished node after reset = %+v, want it to start over", n)
	}
	if !state.EtcdUnhealthySince.IsZero() {
		t.Error("etcd wait kept across resume")
	}

	state.resetFailedNodeForResume(1, now)
	if n := state.Nodes[1]; n.Error != "" || !n.UpgradeCompleted || !n.StartedAt.Equal(now) {
		t.Errorf("upgraded node after reset = %+v, want a fresh timeout for Ready only", n)
	}
}

// etcdStub stands in for the Talos API etcd check.
type etcdStub struct {
	err   error
	calls int
	ips   []string
}

func (e *etcdStub) check(_ context.Context, _ *clientconfig.Config, ips []string) error {
	e.calls++
	e.ips = ips
	return e.err
}

// rollingPlan is a Talos plan for two control planes and a worker.
func rollingPlan(mutate func(*ClusterUpgradeState)) *ClusterUpgradeState {
	state := &ClusterUpgradeState{
		UpgradeType:       UpgradeTypeTalos,
		TargetVersion:     "v1.13.12",
		InstallerImage:    newSchematicRef + ":v1.13.12",
		Phase:             UpgradePhaseControlPlanes,
		ControlPlaneCount: 2,
		WorkerCount:       1,
		Nodes: []NodeUpgradeState{
			{NodeName: "d-amk-003-gsax-ctp0", Role: controlPlaneRole, IP: "100.64.11.245"},
			{NodeName: "d-amk-003-gsax-ctp1", Role: controlPlaneRole, IP: "100.64.11.91"},
			{NodeName: "d-amk-003-gsax-wrk3", Role: roleWorker, IP: "100.64.11.170"},
		},
		CurrentNodeName: "d-amk-003-gsax-ctp0",
	}
	if mutate != nil {
		mutate(state)
	}
	return state
}

func planJSON(t *testing.T, state *ClusterUpgradeState) string {
	t.Helper()
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func (f *fixture) storedPlan(t *testing.T) *ClusterUpgradeState {
	t.Helper()
	raw := f.secretData(t)["upgrade_state"]
	if raw == "" {
		return nil
	}
	var state ClusterUpgradeState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		t.Fatal(err)
	}
	return &state
}

// A node that never comes back used to be waited on forever.
func TestExecuteTalosUpgradeStep_NodePastTimeoutFailsTheUpgrade(t *testing.T) {
	plan := rollingPlan(func(s *ClusterUpgradeState) {
		s.Nodes[0].UpgradeInitiated = true
		s.Nodes[0].StartedAt = time.Now().UTC().Add(-16 * time.Minute)
	})
	f := newFixture(t, legacyCluster(nil), fixtureSecret(map[string]string{"upgrade_state": planJSON(t, plan)}))

	result := f.controller.rollingUpgrade.ExecuteTalosUpgradeStep(context.Background(), f.cluster(t), nil)

	if result.Error == nil {
		t.Fatalf("result = %+v, want the upgrade failed", result)
	}
	stored := f.storedPlan(t)
	if !stored.IsFailed() || stored.FailedNodeName != "d-amk-003-gsax-ctp0" {
		t.Fatalf("stored plan phase=%s failedNode=%q, want failed on ctp0", stored.Phase, stored.FailedNodeName)
	}
	if !strings.Contains(stored.FailedReason, "15m0s") || !strings.Contains(stored.FailedReason, "reboot") {
		t.Errorf("failed reason %q, want the timeout and the step it was stuck in", stored.FailedReason)
	}
}

// The previous control plane's etcd member may still be rejoining; taking the
// next one down then can cost quorum.
func TestExecuteTalosUpgradeStep_WaitsForEtcdBeforeAControlPlane(t *testing.T) {
	plan := rollingPlan(nil)
	f := newFixture(t, legacyCluster(nil), fixtureSecret(map[string]string{"upgrade_state": planJSON(t, plan)}))
	etcd := &etcdStub{err: errors.New("etcd is not healthy: 100.64.11.91: etcd not running and healthy")}
	f.controller.rollingUpgrade.etcdHealth = etcd.check

	result := f.controller.rollingUpgrade.ExecuteTalosUpgradeStep(context.Background(), f.cluster(t), nil)

	if result.Error != nil || !result.Continue || result.RequeueAfter != etcdHealthyRecheckInterval {
		t.Fatalf("result = %+v, want a wait for etcd", result)
	}
	if want := []string{"100.64.11.245", "100.64.11.91"}; strings.Join(etcd.ips, ",") != strings.Join(want, ",") {
		t.Errorf("checked etcd on %v, want every control plane %v", etcd.ips, want)
	}
	stored := f.storedPlan(t)
	if stored.EtcdUnhealthySince.IsZero() {
		t.Error("etcd wait start not recorded; the timeout would never run out")
	}
	if stored.Nodes[0].UpgradeInitiated || !stored.Nodes[0].StartedAt.IsZero() {
		t.Error("control plane was touched while etcd was unhealthy")
	}
}

func TestExecuteTalosUpgradeStep_EtcdUnhealthyPastTimeoutFailsTheUpgrade(t *testing.T) {
	plan := rollingPlan(func(s *ClusterUpgradeState) {
		s.EtcdUnhealthySince = time.Now().UTC().Add(-6 * time.Minute)
	})
	f := newFixture(t, legacyCluster(nil), fixtureSecret(map[string]string{"upgrade_state": planJSON(t, plan)}))
	f.controller.rollingUpgrade.etcdHealth = (&etcdStub{err: errors.New("etcd is not healthy")}).check

	result := f.controller.rollingUpgrade.ExecuteTalosUpgradeStep(context.Background(), f.cluster(t), nil)

	if result.Error == nil {
		t.Fatalf("result = %+v, want the upgrade failed", result)
	}
	stored := f.storedPlan(t)
	if !stored.IsFailed() || !strings.Contains(stored.FailedReason, "etcd") {
		t.Errorf("stored plan phase=%s reason=%q, want failed on etcd", stored.Phase, stored.FailedReason)
	}
}

func TestExecuteTalosUpgradeStep_PausesBetweenNodes(t *testing.T) {
	plan := rollingPlan(func(s *ClusterUpgradeState) {
		s.Nodes[0].UpgradeInitiated, s.Nodes[0].UpgradeCompleted, s.Nodes[0].NodeReady = true, true, true
		s.CurrentNodeIndex = 1
		s.CurrentNodeName = s.Nodes[1].NodeName
		s.LastNodeDoneAt = time.Now().UTC().Add(-10 * time.Second)
	})
	f := newFixture(t, legacyCluster(nil), fixtureSecret(map[string]string{"upgrade_state": planJSON(t, plan)}))
	etcd := &etcdStub{}
	f.controller.rollingUpgrade.etcdHealth = etcd.check

	result := f.controller.rollingUpgrade.ExecuteTalosUpgradeStep(context.Background(), f.cluster(t), nil)

	if !result.Continue || result.RequeueAfter < 45*time.Second || result.RequeueAfter > 50*time.Second {
		t.Fatalf("result = %+v, want the rest of the 60s pause", result)
	}
	if etcd.calls != 0 {
		t.Error("checked etcd during the pause; the check must be fresh when the node starts")
	}
}

// A failed upgrade used to start over from the first node 30 seconds later,
// draining and upgrading the nodes it had already finished.
func TestHandleUpgrade_FailedPlanWaitsForResume(t *testing.T) {
	plan := rollingPlan(func(s *ClusterUpgradeState) {
		s.Nodes[0].UpgradeInitiated, s.Nodes[0].UpgradeCompleted, s.Nodes[0].NodeReady = true, true, true
		s.CurrentNodeIndex = 1
		s.CurrentNodeName = s.Nodes[1].NodeName
		s.Phase = UpgradePhaseFailed
		s.FailedNodeName = s.Nodes[1].NodeName
		s.FailedReason = "node rolled back"
	})
	kc := legacyCluster(map[string]string{
		consts.TalosCurrentAnnotation: "v1.13.11",
		consts.TalosTargetAnnotation:  "v1.13.12",
		consts.TalosStatusAnnotation:  string(consts.UpgradeStatusFailed),
	})
	f := newFixture(t, kc, fixtureSecret(map[string]string{"upgrade_state": planJSON(t, plan)}))

	if _, handled, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, nil); err != nil || handled {
		t.Fatalf("HandleUpgrade handled=%t err=%v, want the failure left for resume", handled, err)
	}

	stored := f.storedPlan(t)
	if !stored.IsFailed() || stored.CurrentNodeIndex != 1 || !stored.Nodes[0].NodeReady {
		t.Errorf("stored plan phase=%s index=%d, want the failed plan untouched", stored.Phase, stored.CurrentNodeIndex)
	}
}

func TestHandleUpgrade_FailedTalosPlanHoldsBackKubernetesUpgrade(t *testing.T) {
	plan := rollingPlan(func(s *ClusterUpgradeState) {
		s.Phase = UpgradePhaseFailed
		s.FailedNodeName = s.Nodes[0].NodeName
	})
	kc := legacyCluster(map[string]string{
		consts.KubernetesCurrentAnnotation: "1.36.3",
		consts.KubernetesTargetAnnotation:  "1.36.5",
	})
	f := newFixture(t, kc, fixtureSecret(map[string]string{"upgrade_state": planJSON(t, plan)}))

	if _, handled, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, nil); err != nil || handled {
		t.Fatalf("HandleUpgrade handled=%t err=%v, want the Kubernetes upgrade held back", handled, err)
	}
	if stored := f.storedPlan(t); stored.UpgradeType != UpgradeTypeTalos || !stored.IsFailed() {
		t.Errorf("stored plan type=%s phase=%s, want the failed Talos plan kept", stored.UpgradeType, stored.Phase)
	}
}

func TestHandleUpgrade_UnhealthyEtcdHoldsBackTheStart(t *testing.T) {
	kc := legacyCluster(map[string]string{
		consts.KubernetesCurrentAnnotation: "1.36.3",
		consts.KubernetesTargetAnnotation:  "1.36.5",
	})
	ctp0 := clusterMachine("d-amk-003-gsax-ctp0", controlPlaneRole, "")
	ctp0.Status.PublicIPAddresses = []string{"100.64.11.245"}
	f := newFixture(t, kc, ctp0, fixtureSecret(nil))
	f.service.healthCheck = (&healthStub{}).check
	f.controller.rollingUpgrade.etcdHealth = (&etcdStub{err: errors.New("etcd is not healthy: 100.64.11.245: etcd not running and healthy")}).check

	requeue, handled, err := f.controller.HandleUpgrade(context.Background(), f.cluster(t), nil, nil)
	if err != nil || !handled || requeue != etcdStartRecheckDelay {
		t.Fatalf("HandleUpgrade requeue=%s handled=%t err=%v, want a recheck in %s", requeue, handled, err, etcdStartRecheckDelay)
	}

	if f.storedPlan(t) != nil {
		t.Error("an upgrade plan was stored although etcd was unhealthy")
	}
	stored := f.cluster(t)
	if got := consts.UpgradeStatus(stored.Annotations[consts.KubernetesStatusAnnotation]); got != consts.UpgradeStatusFailed {
		t.Errorf("kubernetes-status = %q, want %q", got, consts.UpgradeStatusFailed)
	}
	if msg := stored.Annotations[consts.KubernetesMessageAnnotation]; !strings.Contains(msg, "etcd") {
		t.Errorf("kubernetes-message = %q, want the etcd problem", msg)
	}
	if got := stored.Annotations[consts.KubernetesTargetAnnotation]; got != "1.36.5" {
		t.Errorf("kubernetes-target = %q, want it kept so the upgrade starts once etcd recovers", got)
	}
}
