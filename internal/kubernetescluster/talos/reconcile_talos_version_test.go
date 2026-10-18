package talos

import (
	"context"
	"strings"
	"testing"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/internal/services/secretservice"
	"github.com/vitistack/talos-operator/internal/services/talosstateservice"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testNameCP0  = "cp0"
	testNameCP1  = "cp1"
	testNameWrk0 = "wrk0"
	testNameWrk1 = "wrk1"
	testVerOld   = "v1.12.4"
	testVerNew   = "v1.12.7"
)

// makeMachine constructs a Machine with the role label and a single IPv4
// address. role should be "control-plane" or anything else for worker.
func makeMachine(name, role, ip string) *vitistackv1alpha1.Machine {
	m := &vitistackv1alpha1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				vitistackv1alpha1.NodeRoleAnnotation: role,
			},
		},
	}
	if ip != "" {
		m.Status.PublicIPAddresses = []string{ip}
	}
	return m
}

func TestSelectEnforcementTarget(t *testing.T) {
	t.Parallel()

	cp0 := makeMachine(testNameCP0, "control-plane", "10.0.0.1")
	cp1 := makeMachine(testNameCP1, "control-plane", "10.0.0.2")
	wrk0 := makeMachine(testNameWrk0, "worker", "10.0.0.3")
	wrk1 := makeMachine(testNameWrk1, "worker", "10.0.0.4")
	machines := []*vitistackv1alpha1.Machine{cp0, cp1, wrk0, wrk1}

	tests := []struct {
		name    string
		actual  map[string]string
		desired string
		want    string // machine name, or "" for nil
	}{
		{
			name: "all match desired",
			actual: map[string]string{
				testNameCP0: testVerNew, testNameCP1: testVerNew,
				testNameWrk0: testVerNew, testNameWrk1: testVerNew,
			},
			desired: testVerNew,
			want:    "",
		},
		{
			name: "control plane out of sync picks control plane first",
			actual: map[string]string{
				testNameCP0: testVerOld, testNameCP1: testVerNew,
				testNameWrk0: testVerOld, testNameWrk1: testVerOld,
			},
			desired: testVerNew,
			want:    testNameCP0,
		},
		{
			name: "all CPs match, worker out of sync picks worker",
			actual: map[string]string{
				testNameCP0: testVerNew, testNameCP1: testVerNew,
				testNameWrk0: testVerOld, testNameWrk1: testVerNew,
			},
			desired: testVerNew,
			want:    testNameWrk0,
		},
		{
			name: "unreachable node never picked even if ordered first",
			actual: map[string]string{
				// cp0 absent (unreachable)
				testNameCP1:  testVerNew,
				testNameWrk0: testVerOld,
			},
			desired: testVerNew,
			want:    testNameWrk0,
		},
		{
			// While the cluster steps through an intermediate version, a node
			// already past that step must not be "upgraded" back down to it.
			name: "node ahead of the step is never picked",
			actual: map[string]string{
				testNameCP0: "v1.13.11", testNameCP1: "v1.12.12",
				testNameWrk0: "v1.12.12", testNameWrk1: "v1.12.7",
			},
			desired: "v1.12.12",
			want:    testNameWrk1,
		},
		{
			name:    "empty actual map returns nil",
			actual:  map[string]string{},
			desired: testVerNew,
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := selectEnforcementTarget(machines, tt.actual, tt.desired)
			if tt.want == "" {
				if got != nil {
					t.Fatalf("expected nil, got %s", got.Name)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected %s, got nil", tt.want)
			}
			if got.Name != tt.want {
				t.Fatalf("expected %s, got %s", tt.want, got.Name)
			}
		})
	}
}

func TestVersionEnforcementSkip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		enabled    bool
		target     string
		wantSkip   bool
		wantReason string
	}{
		{name: "feature flag off", enabled: false, target: "1.13.10", wantSkip: true, wantReason: "Disabled"},
		{name: "enabled without a target", enabled: true, target: "", wantSkip: true, wantReason: "NoTarget"},
		{name: "enabled with a blank target", enabled: true, target: "  ", wantSkip: true, wantReason: "NoTarget"},
		{name: "enabled with a target", enabled: true, target: "1.13.10", wantSkip: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reason, message, skip := versionEnforcementSkip(tt.enabled, tt.target)
			if skip != tt.wantSkip || reason != tt.wantReason {
				t.Errorf("versionEnforcementSkip(%v, %q) = (%q, %q, %v), want reason %q skip %v",
					tt.enabled, tt.target, reason, message, skip, tt.wantReason, tt.wantSkip)
			}
			if skip && message == "" {
				t.Error("a skip must explain itself in the condition message")
			}
		})
	}
}

// Conditions written before enforcement became opt-in (2026-05-22) still
// claimed "desired v1.12.7" four months later, because a pass that does not
// enforce returned before touching the condition.
func TestRetireEnforcementCondition(t *testing.T) {
	t.Parallel()

	const (
		disabledMsg = "Talos version enforcement is disabled"
		stale       = "2026-05-22T10:37:57.649560523Z"
	)
	bootstrapped := vitistackv1alpha1.KubernetesClusterCondition{
		Type: "Bootstrapped", Status: "True", Reason: "Done", Message: "Talos cluster bootstrapped", LastTransitionTime: stale,
	}

	tests := []struct {
		name  string
		conds []vitistackv1alpha1.KubernetesClusterCondition
		want  []vitistackv1alpha1.KubernetesClusterCondition // compared without LastTransitionTime
	}{
		{
			name: "stale downgrade refusal",
			conds: []vitistackv1alpha1.KubernetesClusterCondition{bootstrapped, {
				Type: "TalosVersionEnforcement", Status: "True", Reason: "Downgrade", LastTransitionTime: stale,
				Message: "Node d-wh-osl-001-k732-wrk0 runs Talos v1.13.2 which is newer than desired v1.12.7 — refusing to downgrade",
			}},
			want: []vitistackv1alpha1.KubernetesClusterCondition{
				{Type: "Bootstrapped", Status: "True", Reason: "Done", Message: "Talos cluster bootstrapped"},
				{Type: "TalosVersionEnforcement", Status: "False", Reason: "Disabled", Message: disabledMsg},
			},
		},
		{
			name: "stale in-sync claim",
			conds: []vitistackv1alpha1.KubernetesClusterCondition{{
				Type: "TalosVersionEnforcement", Status: "False", Reason: "InSync", LastTransitionTime: stale,
				Message: "All nodes run Talos v1.12.7",
			}},
			want: []vitistackv1alpha1.KubernetesClusterCondition{
				{Type: "TalosVersionEnforcement", Status: "False", Reason: "Disabled", Message: disabledMsg},
			},
		},
		{
			name:  "cluster that never had the condition gets none",
			conds: []vitistackv1alpha1.KubernetesClusterCondition{bootstrapped},
			want: []vitistackv1alpha1.KubernetesClusterCondition{
				{Type: "Bootstrapped", Status: "True", Reason: "Done", Message: "Talos cluster bootstrapped"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cluster := &vitistackv1alpha1.KubernetesCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "d-wh-osl-001", Namespace: "team-meldingsflyt"},
				Status:     vitistackv1alpha1.KubernetesClusterStatus{Phase: "Ready", Conditions: tt.conds},
			}
			tm, c := newStatusTestTalosManager(t, cluster)
			held := &vitistackv1alpha1.KubernetesCluster{}
			key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
			if err := c.Get(context.Background(), key, held); err != nil {
				t.Fatal(err)
			}

			tm.retireEnforcementCondition(context.Background(), held, "Disabled", disabledMsg)

			got := &vitistackv1alpha1.KubernetesCluster{}
			if err := c.Get(context.Background(), key, got); err != nil {
				t.Fatal(err)
			}
			assertConditions(t, got.Status.Conditions, tt.want)
		})
	}
}

// assertConditions compares conditions by type, status, reason and message.
func assertConditions(t *testing.T, got, want []vitistackv1alpha1.KubernetesClusterCondition) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("conditions = %+v, want %+v", got, want)
	}
	byType := make(map[string]vitistackv1alpha1.KubernetesClusterCondition, len(got))
	for _, g := range got {
		byType[g.Type] = g
	}
	for _, w := range want {
		g, ok := byType[w.Type]
		if !ok {
			t.Errorf("condition %s missing from %+v", w.Type, got)
			continue
		}
		if g.Status != w.Status || g.Reason != w.Reason || g.Message != w.Message {
			t.Errorf("%s = %s/%s/%q, want %s/%s/%q", w.Type, g.Status, g.Reason, g.Message, w.Status, w.Reason, w.Message)
		}
	}
}

func newStatusTestTalosManager(t *testing.T, objs ...client.Object) (*TalosManager, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := vitistackv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&vitistackv1alpha1.KubernetesCluster{}).
		Build()
	secretSvc := secretservice.NewSecretService(c)
	return NewTalosManager(c, status.NewManager(c, secretSvc, talosstateservice.NewTalosStateService(secretSvc))), c
}

func TestEnforcementStepWalksTheClusterThroughEachMinor(t *testing.T) {
	t.Parallel()
	machines := []*vitistackv1alpha1.Machine{
		makeMachine(testNameCP0, "control-plane", "10.0.0.1"),
		makeMachine(testNameWrk0, "worker", "10.0.0.3"),
	}
	tests := []struct {
		name   string
		actual map[string]string
		want   string
	}{
		{"patch up the running minor first", map[string]string{testNameCP0: "v1.12.7", testNameWrk0: "v1.12.7"}, "v1.12.12"},
		{"the least-upgraded node decides the step", map[string]string{testNameCP0: "v1.13.11", testNameWrk0: "v1.12.12"}, "v1.13.11"},
		{"final hop to the target", map[string]string{testNameCP0: "v1.13.11", testNameWrk0: "v1.13.11"}, "v1.14.2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, reason, msg := enforcementStep(machines, tt.actual, "v1.14.2", "")
			if reason != "" || got != tt.want {
				t.Fatalf("enforcementStep() = (%q, %q, %q), want %q", got, reason, msg, tt.want)
			}
		})
	}
}

func TestEnforcementStepStopsWhenThePathOrKubernetesForbidsIt(t *testing.T) {
	t.Parallel()
	machines := []*vitistackv1alpha1.Machine{makeMachine(testNameCP0, "control-plane", "10.0.0.1")}
	if _, reason, msg := enforcementStep(machines, map[string]string{testNameCP0: "v1.10.3"}, "v1.12.12", ""); reason != "PathUnknown" || msg == "" {
		t.Errorf("unknown latest 1.10 patch: got reason %q msg %q, want PathUnknown", reason, msg)
	}
	if _, reason, msg := enforcementStep(machines, map[string]string{testNameCP0: "v1.12.12"}, "v1.13.11", "1.30.5"); reason != "KubernetesIncompatible" || msg == "" {
		t.Errorf("Talos 1.13 with Kubernetes 1.30: got reason %q msg %q, want KubernetesIncompatible", reason, msg)
	}
}

func TestEnforcementStepWaitsForEveryNodeToAnswer(t *testing.T) {
	t.Parallel()
	// wrk0 is rebooting into the current step (or rolled back to an older
	// version): stepping on from the others could leave it a minor behind.
	machines := []*vitistackv1alpha1.Machine{
		makeMachine(testNameCP0, "control-plane", "10.0.0.1"),
		makeMachine(testNameWrk0, "worker", "10.0.0.3"),
		makeMachine("new0", "worker", ""), // no address yet: not part of any upgrade
	}
	_, reason, msg := enforcementStep(machines, map[string]string{testNameCP0: "v1.12.12"}, "v1.14.2", "")
	if reason != "WaitingForNodes" || !strings.Contains(msg, testNameWrk0) || strings.Contains(msg, "new0") {
		t.Errorf("got reason %q msg %q, want WaitingForNodes naming only %s", reason, msg, testNameWrk0)
	}
}

func TestEnforcementStepTreatsAnUnreadableVersionAsSilent(t *testing.T) {
	t.Parallel()
	machines := []*vitistackv1alpha1.Machine{
		makeMachine(testNameCP0, "control-plane", "10.0.0.1"),
		makeMachine(testNameWrk0, "worker", "10.0.0.3"),
	}
	_, reason, msg := enforcementStep(machines, map[string]string{testNameCP0: "v1.12.12", testNameWrk0: ""}, "v1.14.2", "")
	if reason != "WaitingForNodes" || !strings.Contains(msg, testNameWrk0) {
		t.Errorf("got reason %q msg %q, want WaitingForNodes naming %s", reason, msg, testNameWrk0)
	}
}
