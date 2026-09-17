package talos

import (
	"context"
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

func TestSwapImageTagForEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ref  string
		tag  string
		want string
	}{
		{
			name: "factory image with version tag rewrites tag",
			ref:  "factory.talos.dev/nocloud-installer/abc123:" + testVerOld,
			tag:  testVerNew,
			want: "factory.talos.dev/nocloud-installer/abc123:" + testVerNew,
		},
		{
			name: "image without tag appends tag",
			ref:  "ghcr.io/siderolabs/installer",
			tag:  testVerNew,
			want: "ghcr.io/siderolabs/installer:" + testVerNew,
		},
		{
			name: "registry port is not mistaken for tag",
			ref:  "localhost:5000/foo:v1.0.0",
			tag:  "v2.0.0",
			want: "localhost:5000/foo:v2.0.0",
		},
		{
			name: "registry port without image tag",
			ref:  "localhost:5000/foo",
			tag:  "v1.0.0",
			want: "localhost:5000/foo:v1.0.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := swapImageTagForEnforcement(tt.ref, tt.tag)
			if got != tt.want {
				t.Fatalf("swapImageTagForEnforcement(%q, %q) = %q, want %q", tt.ref, tt.tag, got, tt.want)
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
