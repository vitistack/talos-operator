package talos

import (
	"testing"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
