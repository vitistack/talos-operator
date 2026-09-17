package talos

import (
	"context"
	"errors"
	"slices"
	"testing"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func node(name string, ready, unschedulable bool) corev1.Node {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.NodeSpec{Unschedulable: unschedulable},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse},
			{Type: corev1.NodeReady, Status: status},
		}},
	}
}

func machineWithIP(name, ip string) *vitistackv1alpha1.Machine {
	m := &vitistackv1alpha1.Machine{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid")}}
	if ip != "" {
		m.Status.PublicIPAddresses = []string{"fd00::11", ip}
	}
	return m
}

func nodeNames(nodes []corev1.Node) []string {
	names := make([]string, 0, len(nodes))
	for i := range nodes {
		names = append(names, nodes[i].Name)
	}
	return names
}

func TestNodesReadiness(t *testing.T) {
	t.Parallel()

	expected := []*vitistackv1alpha1.Machine{machineWithIP("ctp0", "100.64.4.14"), machineWithIP("wrk0", "100.64.4.20"), machineWithIP("wrk3", "100.64.4.11")}
	noCondition := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "wrk3"}}

	tests := []struct {
		name       string
		nodes      []corev1.Node
		expected   []*vitistackv1alpha1.Machine
		wantReady  bool
		wantReason string
	}{
		{
			name:      "every expected node joined and Ready",
			nodes:     []corev1.Node{node("wrk3", true, false), node("ctp0", true, false), node("wrk0", true, false), node("old-wrk9", false, true)},
			expected:  expected,
			wantReady: true,
		},
		{
			name:       "new worker has not joined yet",
			nodes:      []corev1.Node{node("ctp0", true, false), node("wrk0", true, false)},
			expected:   expected,
			wantReason: "waiting for nodes to join: [wrk3]",
		},
		{
			name:       "joined but NotReady",
			nodes:      []corev1.Node{node("ctp0", true, false), node("wrk0", false, false), node("wrk3", true, false)},
			expected:   expected,
			wantReason: "waiting for nodes to become Ready: [wrk0]",
		},
		{
			name:       "missing and NotReady together",
			nodes:      []corev1.Node{node("ctp0", true, false), node("wrk0", false, false)},
			expected:   expected,
			wantReason: "waiting for nodes to join ([wrk3]) and become Ready ([wrk0])",
		},
		{
			name:       "node without a Ready condition counts as NotReady",
			nodes:      []corev1.Node{node("ctp0", true, false), node("wrk0", true, false), noCondition},
			expected:   expected,
			wantReason: "waiting for nodes to become Ready: [wrk3]",
		},
		{
			name:       "no expected machines",
			nodes:      []corev1.Node{node("ctp0", true, false)},
			wantReason: "no expected machines",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ready, reason := nodesReadiness(tt.nodes, tt.expected)
			if ready != tt.wantReady || reason != tt.wantReason {
				t.Errorf("nodesReadiness() = (%v, %q), want (%v, %q)", ready, reason, tt.wantReady, tt.wantReason)
			}
		})
	}
}

func TestRemoveOrphanedNodes(t *testing.T) {
	t.Parallel()

	machineNames := map[string]bool{"ctp0": true, "wrk1": true}

	t.Run("deletes only orphans and returns the nodes that remain", func(t *testing.T) {
		t.Parallel()
		nodes := []corev1.Node{
			node("ctp0", true, false),
			node("gone-wrk7", false, true),   // orphan
			node("wrk1", false, true),        // cordoned and down, but its Machine exists
			node("drained-wrk8", true, true), // no Machine, but still Ready
			node("down-wrk9", false, false),  // no Machine, NotReady, but schedulable
		}
		clientset := fake.NewClientset(&nodes[1])

		survivors := removeOrphanedNodes(context.Background(), clientset, nodes, machineNames, "test")

		if want := []string{"ctp0", "wrk1", "drained-wrk8", "down-wrk9"}; !slices.Equal(nodeNames(survivors), want) {
			t.Errorf("survivors = %v, want %v", nodeNames(survivors), want)
		}
		var deleted []string
		for _, a := range clientset.Actions() {
			if d, ok := a.(k8stesting.DeleteAction); ok {
				deleted = append(deleted, d.GetName())
			}
		}
		if want := []string{"gone-wrk7"}; !slices.Equal(deleted, want) {
			t.Errorf("deleted = %v, want %v", deleted, want)
		}
	})

	t.Run("a node that could not be deleted still exists", func(t *testing.T) {
		t.Parallel()
		nodes := []corev1.Node{node("ctp0", true, false), node("gone-wrk7", false, true)}
		clientset := fake.NewClientset()
		clientset.PrependReactor("delete", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("http2: client connection lost")
		})

		survivors := removeOrphanedNodes(context.Background(), clientset, nodes, machineNames, "test")

		if want := []string{"ctp0", "gone-wrk7"}; !slices.Equal(nodeNames(survivors), want) {
			t.Errorf("survivors = %v, want %v", nodeNames(survivors), want)
		}
	})

	t.Run("a node already gone is dropped", func(t *testing.T) {
		t.Parallel()
		nodes := []corev1.Node{node("ctp0", true, false), node("gone-wrk7", false, true)}
		clientset := fake.NewClientset()
		clientset.PrependReactor("delete", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewNotFound(corev1.Resource("nodes"), "gone-wrk7")
		})

		survivors := removeOrphanedNodes(context.Background(), clientset, nodes, machineNames, "test")

		if want := []string{"ctp0"}; !slices.Equal(nodeNames(survivors), want) {
			t.Errorf("survivors = %v, want %v", nodeNames(survivors), want)
		}
	})
}

func TestMaintenanceProbeTargets(t *testing.T) {
	t.Parallel()

	configured := map[string]types.UID{
		"ctp0":     "ctp0-uid",
		"wrk0":     "wrk0-uid",
		"wrk1":     "wrk1-uid",
		"wrk2":     "wrk2-uid",
		"wrk-noip": "wrk-noip-uid",
		"deleted":  "deleted-uid",
	}
	machines := []*vitistackv1alpha1.Machine{
		machineWithIP("ctp0", "100.64.4.14"),
		machineWithIP("wrk0", "100.64.4.20"),
		machineWithIP("wrk1", "100.64.4.21"),
		machineWithIP("wrk2", "100.64.4.22"),
		machineWithIP("wrk-noip", ""),
		machineWithIP("unconfigured", "100.64.4.30"),
	}

	tests := []struct {
		name  string
		nodes *workloadNodes
		want  []maintenanceProbe
	}{
		{
			// A Ready node is running kubelet on a configured Talos, so it
			// cannot be in maintenance mode; only the rest need the probe.
			name: "skip nodes that joined and are Ready",
			nodes: &workloadNodes{items: []corev1.Node{
				node("ctp0", true, false),
				node("wrk0", true, false),
				node("wrk1", false, false), // NotReady: may have been reset
				// wrk2 has not joined
			}},
			want: []maintenanceProbe{{name: "wrk1", ip: "100.64.4.21"}, {name: "wrk2", ip: "100.64.4.22"}},
		},
		{
			// Without a node list there is nothing to rule nodes out with.
			name:  "node list unavailable probes every configured node",
			nodes: nil,
			want: []maintenanceProbe{
				{name: "ctp0", ip: "100.64.4.14"},
				{name: "wrk0", ip: "100.64.4.20"},
				{name: "wrk1", ip: "100.64.4.21"},
				{name: "wrk2", ip: "100.64.4.22"},
			},
		},
		{
			name:  "empty cluster probes every configured node",
			nodes: &workloadNodes{},
			want: []maintenanceProbe{
				{name: "ctp0", ip: "100.64.4.14"},
				{name: "wrk0", ip: "100.64.4.20"},
				{name: "wrk1", ip: "100.64.4.21"},
				{name: "wrk2", ip: "100.64.4.22"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := maintenanceProbeTargets(configured, machines, tt.nodes)
			if !slices.Equal(got, tt.want) {
				t.Errorf("maintenanceProbeTargets() = %v, want %v", got, tt.want)
			}
		})
	}
}
