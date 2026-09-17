package v1alpha1

import (
	"testing"
	"time"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestKubernetesClusterEventFilter(t *testing.T) {
	t.Parallel()

	heartbeat := metav1.NewTime(time.Date(2026, 9, 17, 7, 59, 5, 0, time.UTC))

	newCluster := func() *vitistackv1alpha1.KubernetesCluster {
		return &vitistackv1alpha1.KubernetesCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "p-bgo-ek-viti",
				Namespace:   "vitistack-ek-prod",
				Generation:  3,
				Annotations: map[string]string{"upgrade.vitistack.io/talos-current": "1.13.10"},
				Labels:      map[string]string{"team": "ek"},
			},
			Status: vitistackv1alpha1.KubernetesClusterStatus{Phase: "Ready"},
		}
	}

	tests := []struct {
		name   string
		mutate func(c *vitistackv1alpha1.KubernetesCluster)
		want   bool
	}{
		{
			// The operator writes this on every pass; letting it through is
			// what kept every cluster reconciling back-to-back.
			name:   "operator progress message",
			mutate: func(c *vitistackv1alpha1.KubernetesCluster) { c.Status.Message = "Reconciling new nodes" },
			want:   false,
		},
		{
			name:   "status lastUpdated heartbeat",
			mutate: func(c *vitistackv1alpha1.KubernetesCluster) { c.Status.State.LastUpdated = heartbeat },
			want:   false,
		},
		{
			name:   "spec edit bumps generation",
			mutate: func(c *vitistackv1alpha1.KubernetesCluster) { c.Generation = 4 },
			want:   true,
		},
		{
			// Upgrades are requested through annotations, not spec.
			name: "user requests a talos upgrade",
			mutate: func(c *vitistackv1alpha1.KubernetesCluster) {
				c.Annotations["upgrade.vitistack.io/talos-target"] = "1.13.11"
			},
			want: true,
		},
		{
			// Removing do-not-reconcile must wake a cluster that stopped requeueing.
			name: "annotation removed",
			mutate: func(c *vitistackv1alpha1.KubernetesCluster) {
				delete(c.Annotations, "upgrade.vitistack.io/talos-current")
			},
			want: true,
		},
		{
			name:   "label change",
			mutate: func(c *vitistackv1alpha1.KubernetesCluster) { c.Labels["team"] = "viti" },
			want:   true,
		},
		{
			name:   "deletion requested without generation bump",
			mutate: func(c *vitistackv1alpha1.KubernetesCluster) { c.DeletionTimestamp = &heartbeat },
			want:   true,
		},
	}

	filter := kubernetesClusterEventFilter()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			oldObj, newObj := newCluster(), newCluster()
			tt.mutate(newObj)
			if got := filter.Update(event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}); got != tt.want {
				t.Errorf("Update() = %v, want %v", got, tt.want)
			}
		})
	}

	t.Run("create and delete always pass", func(t *testing.T) {
		t.Parallel()
		if !filter.Create(event.CreateEvent{Object: newCluster()}) {
			t.Error("Create() = false, want true")
		}
		if !filter.Delete(event.DeleteEvent{Object: newCluster()}) {
			t.Error("Delete() = false, want true")
		}
	})
}

func TestMachineEventFilter(t *testing.T) {
	t.Parallel()

	newMachine := func() *vitistackv1alpha1.Machine {
		return &vitistackv1alpha1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "p-bgo-ek-viti-68ax-wrk3",
				Namespace:   "vitistack-ek-prod",
				Generation:  1,
				Annotations: map[string]string{"vitistack.io/nodepool": "workers2"},
				Labels:      map[string]string{"vitistack.io/cluster-id": "68ax"},
			},
			Status: vitistackv1alpha1.MachineStatus{
				Phase:       "Running",
				State:       "Running",
				LastUpdated: metav1.NewTime(time.Date(2026, 9, 17, 7, 58, 26, 0, time.UTC)),
				IPAddresses: []string{"100.124.6.215", "100.64.4.11"},
			},
		}
	}

	tests := []struct {
		name   string
		mutate func(m *vitistackv1alpha1.Machine)
		want   bool
	}{
		{
			// kv-operator rewrites only this field every few seconds.
			name: "provider heartbeat touches only lastUpdated",
			mutate: func(m *vitistackv1alpha1.Machine) {
				m.Status.LastUpdated = metav1.NewTime(time.Date(2026, 9, 17, 7, 58, 32, 0, time.UTC))
			},
			want: false,
		},
		{
			name:   "machine fails",
			mutate: func(m *vitistackv1alpha1.Machine) { m.Status.Phase = "Failed" },
			want:   true,
		},
		{
			name: "machine gets its first IP",
			mutate: func(m *vitistackv1alpha1.Machine) {
				m.Status.IPAddresses = append(m.Status.IPAddresses, "100.64.4.12")
			},
			want: true,
		},
		{
			name:   "spec edit bumps generation",
			mutate: func(m *vitistackv1alpha1.Machine) { m.Generation = 2 },
			want:   true,
		},
		{
			name: "annotation change",
			mutate: func(m *vitistackv1alpha1.Machine) {
				m.Annotations["vitistack.io/os-installed"] = "true"
			},
			want: true,
		},
		{
			name:   "label change",
			mutate: func(m *vitistackv1alpha1.Machine) { m.Labels["vitistack.io/cluster-id"] = "other" },
			want:   true,
		},
	}

	filter := machineEventFilter()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			oldObj, newObj := newMachine(), newMachine()
			tt.mutate(newObj)
			if got := filter.Update(event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}); got != tt.want {
				t.Errorf("Update() = %v, want %v", got, tt.want)
			}
		})
	}

	t.Run("create and delete always pass", func(t *testing.T) {
		t.Parallel()
		if !filter.Create(event.CreateEvent{Object: newMachine()}) {
			t.Error("Create() = false, want true")
		}
		if !filter.Delete(event.DeleteEvent{Object: newMachine()}) {
			t.Error("Delete() = false, want true")
		}
	})
}

func TestUpgradeRequeueAfter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{
			// Status writes no longer re-queue the cluster, so a zero delay
			// from a step whose settling window just ran out would stall the
			// upgrade for good.
			name: "settling window already elapsed",
			in:   0,
			want: 5 * time.Second,
		},
		{
			name: "negative remainder",
			in:   -2 * time.Second,
			want: 5 * time.Second,
		},
		{
			name: "completion asks for a quick requeue",
			in:   1 * time.Second,
			want: 1 * time.Second,
		},
		{
			name: "step asks for a long wait",
			in:   30 * time.Second,
			want: 30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := upgradeRequeueAfter(tt.in); got != tt.want {
				t.Errorf("upgradeRequeueAfter(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
