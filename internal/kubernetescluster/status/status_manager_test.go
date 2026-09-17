package status

import (
	"context"
	"sync"
	"testing"
	"time"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/secretservice"
	"github.com/vitistack/talos-operator/internal/services/talosstateservice"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	testNamespace  = "vitistack-ek-prod"
	testClusterID  = "68ax"
	kubeSystemUID  = "3f1c2b9e-0d4a-4c43-9d57-6a1e2f8b7c10"
	kubeSystemUIDK = "vitistack.io/kube-system-uid"
)

var kubeSystemCreated = time.Date(2026, 9, 1, 10, 51, 2, 0, time.UTC)

// apiCalls counts the calls that cost the API server: unstructured reads
// (the cache does not serve them, so each one is a live GET) and writes.
type apiCalls struct {
	mu              sync.Mutex
	liveReads       int
	statusWrites    int
	updates         int
	patches         int
	kubeSystemReads int
	conflictStatus  bool
}

func (a *apiCalls) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.liveReads, a.statusWrites, a.updates, a.patches, a.kubeSystemReads = 0, 0, 0, 0, 0
}

func (a *apiCalls) writes() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.statusWrites + a.updates + a.patches
}

func (a *apiCalls) count(field *int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	*field++
}

func newTestManager(t *testing.T, objs ...client.Object) (*StatusManager, client.Client, *apiCalls) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := vitistackv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	calls := &apiCalls{}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&vitistackv1alpha1.KubernetesCluster{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*unstructured.Unstructured); ok {
					calls.count(&calls.liveReads)
				}
				return cl.Get(ctx, key, obj, opts...)
			},
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				calls.count(&calls.updates)
				return cl.Update(ctx, obj, opts...)
			},
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				calls.count(&calls.patches)
				return cl.Patch(ctx, obj, patch, opts...)
			},
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				calls.count(&calls.statusWrites)
				calls.mu.Lock()
				conflict := calls.conflictStatus
				calls.mu.Unlock()
				if conflict {
					return apierrors.NewConflict(schema.GroupResource{Group: "vitistack.io", Resource: "kubernetesclusters"}, obj.GetName(), nil)
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).
		Build()

	secretSvc := secretservice.NewSecretService(c)
	m := NewManager(c, secretSvc, talosstateservice.NewTalosStateService(secretSvc))
	m.kubeSystemNamespace = func(context.Context, []byte) (*corev1.Namespace, error) {
		calls.count(&calls.kubeSystemReads)
		return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name:              "kube-system",
			UID:               types.UID(kubeSystemUID),
			CreationTimestamp: metav1.NewTime(kubeSystemCreated),
		}}, nil
	}
	return m, c, calls
}

func testCluster() *vitistackv1alpha1.KubernetesCluster {
	return &vitistackv1alpha1.KubernetesCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "p-bgo-ek-viti", Namespace: testNamespace},
		Spec: vitistackv1alpha1.KubernetesClusterSpec{
			Cluster: vitistackv1alpha1.KubernetesClusterSpecData{ClusterId: testClusterID, Provider: "talos"},
		},
	}
}

func readyTalosSecret() *corev1.Secret {
	flag := []byte("true")
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterID, Namespace: testNamespace},
		Data: map[string][]byte{
			"talosconfig_present":       flag,
			"controlplane_yaml_present": flag,
			"worker_yaml_present":       flag,
			"controlplane_applied":      flag,
			"worker_applied":            flag,
			"bootstrapped":              flag,
			"cluster_access":            flag,
			"nodes_health_ready":        flag,
			"kube.config":               []byte("apiVersion: v1\nkind: Config\n"),
		},
	}
}

const (
	gib = int64(1) << 30
)

func testMachine(name, role string, cpus int, memory, disk, used int64) *vitistackv1alpha1.Machine {
	return &vitistackv1alpha1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels: map[string]string{
				vitistackv1alpha1.ClusterIdAnnotation: testClusterID,
				vitistackv1alpha1.NodeRoleAnnotation:  role,
			},
		},
		Status: vitistackv1alpha1.MachineStatus{
			Phase:  "Running",
			CPUs:   cpus,
			Memory: memory,
			Disks:  []vitistackv1alpha1.MachineStatusDisk{{Name: "root", Size: disk, UsedBytes: used}},
		},
	}
}

// fiveNodeCluster is 3 control planes (2 CPU, 4GiB, 50GiB disk with 10GiB
// used) and 2 workers (4 CPU, 8GiB, 100GiB disk with 20GiB used).
func fiveNodeCluster() []client.Object {
	return []client.Object{
		testCluster(),
		readyTalosSecret(),
		testMachine("p-bgo-ek-viti-68ax-ctp2", "control-plane", 2, 4*gib, 50*gib, 10*gib),
		testMachine("p-bgo-ek-viti-68ax-ctp0", "control-plane", 2, 4*gib, 50*gib, 10*gib),
		testMachine("p-bgo-ek-viti-68ax-ctp1", "control-plane", 2, 4*gib, 50*gib, 10*gib),
		testMachine("p-bgo-ek-viti-68ax-wrk0", "worker", 4, 8*gib, 100*gib, 20*gib),
		testMachine("p-bgo-ek-viti-68ax-wrk1", "worker", 4, 8*gib, 100*gib, 20*gib),
	}
}

// getCluster reads the cluster the way a reconcile pass starts: a typed read,
// which production serves from the informer cache.
func getCluster(t *testing.T, c client.Client) *vitistackv1alpha1.KubernetesCluster {
	t.Helper()
	kc := &vitistackv1alpha1.KubernetesCluster{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "p-bgo-ek-viti", Namespace: testNamespace}, kc); err != nil {
		t.Fatal(err)
	}
	return kc
}

func TestUpdateKubernetesClusterStatus_FirstPassRecordsEverything(t *testing.T) {
	t.Parallel()
	m, c, calls := newTestManager(t, fiveNodeCluster()...)

	if err := m.UpdateKubernetesClusterStatus(context.Background(), getCluster(t, c)); err != nil {
		t.Fatal(err)
	}

	got := getCluster(t, c)
	if got.Status.Phase != "Ready" {
		t.Errorf("phase = %q, want Ready", got.Status.Phase)
	}
	if len(got.Status.Conditions) != 5 {
		t.Errorf("got %d conditions, want 5: %+v", len(got.Status.Conditions), got.Status.Conditions)
	}
	if !got.Status.State.Created.Time.Equal(kubeSystemCreated) {
		t.Errorf("state.created = %v, want %v", got.Status.State.Created.Time, kubeSystemCreated)
	}
	if got.Annotations[kubeSystemUIDK] != kubeSystemUID {
		t.Errorf("kube-system-uid annotation = %q, want %q", got.Annotations[kubeSystemUIDK], kubeSystemUID)
	}
	if got.Status.Workers != 2 {
		t.Errorf("workers = %d, want 2", got.Status.Workers)
	}
	cp := got.Status.State.Cluster.ControlPlaneStatus
	if cp.Scale != 3 || cp.Status != "Running" {
		t.Errorf("controlplane scale/status = %d/%q, want 3/Running", cp.Scale, cp.Status)
	}
	res := got.Status.State.Cluster.Resources
	if res.CPU.Capacity.Value() != 14 {
		t.Errorf("cpu capacity = %d, want 14", res.CPU.Capacity.Value())
	}
	if res.Memory.Capacity.Value() != 30064771072 {
		t.Errorf("memory capacity = %d, want 30064771072", res.Memory.Capacity.Value())
	}
	if res.Disk.Capacity.Value() != 375809638400 || res.Disk.Used.Value() != 75161927680 || res.Disk.Percetage != 20 {
		t.Errorf("disk = %d/%d/%d%%, want 375809638400/75161927680/20%%",
			res.Disk.Capacity.Value(), res.Disk.Used.Value(), res.Disk.Percetage)
	}
	if calls.kubeSystemReads != 1 {
		t.Errorf("kube-system was read %d times, want 1", calls.kubeSystemReads)
	}
}

func TestUpdateKubernetesClusterStatus_SteadyStatePassCostsNothing(t *testing.T) {
	t.Parallel()
	m, c, calls := newTestManager(t, fiveNodeCluster()...)
	ctx := context.Background()

	if err := m.UpdateKubernetesClusterStatus(ctx, getCluster(t, c)); err != nil {
		t.Fatal(err)
	}
	before := getCluster(t, c)
	calls.reset()

	if err := m.UpdateKubernetesClusterStatus(ctx, before); err != nil {
		t.Fatal(err)
	}

	if got := calls.writes(); got != 0 {
		t.Errorf("unchanged cluster: %d writes, want 0", got)
	}
	if calls.liveReads != 0 {
		t.Errorf("unchanged cluster: %d live reads, want 0", calls.liveReads)
	}
	if calls.kubeSystemReads != 0 {
		t.Errorf("kube-system already recorded: read %d times, want 0", calls.kubeSystemReads)
	}
	if after := getCluster(t, c); after.ResourceVersion != before.ResourceVersion {
		t.Errorf("resourceVersion moved from %s to %s", before.ResourceVersion, after.ResourceVersion)
	}
}

func TestUpdateKubernetesClusterStatus_MachineChangeIsWritten(t *testing.T) {
	t.Parallel()
	m, c, _ := newTestManager(t, fiveNodeCluster()...)
	ctx := context.Background()

	if err := m.UpdateKubernetesClusterStatus(ctx, getCluster(t, c)); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, testMachine("p-bgo-ek-viti-68ax-wrk2", "worker", 4, 8*gib, 100*gib, 20*gib)); err != nil {
		t.Fatal(err)
	}

	if err := m.UpdateKubernetesClusterStatus(ctx, getCluster(t, c)); err != nil {
		t.Fatal(err)
	}

	got := getCluster(t, c)
	if got.Status.Workers != 3 {
		t.Errorf("workers = %d, want 3", got.Status.Workers)
	}
	if got.Status.State.Cluster.Resources.CPU.Capacity.Value() != 18 {
		t.Errorf("cpu capacity = %d, want 18", got.Status.State.Cluster.Resources.CPU.Capacity.Value())
	}
}

func TestAggregateFromMachines_ConflictDoesNotFallBackToFullUpdate(t *testing.T) {
	t.Parallel()
	m, c, calls := newTestManager(t, fiveNodeCluster()...)
	calls.conflictStatus = true

	if err := m.AggregateFromMachines(context.Background(), getCluster(t, c)); err != nil {
		t.Fatalf("conflict should be skipped quietly, got %v", err)
	}
	if calls.updates != 0 {
		t.Errorf("a status conflict triggered %d full-object updates, want 0", calls.updates)
	}
}

// The held object is the only thing the unchanged-check looks at, so it has to
// follow the operator's own writes or a later change back would be skipped.
func TestStatusSetters_FollowUpChangeIsWritten(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		steps []func(ctx context.Context, m *StatusManager, kc *vitistackv1alpha1.KubernetesCluster) error
		check func(t *testing.T, got *vitistackv1alpha1.KubernetesCluster)
	}{
		{
			name: "message set then cleared",
			steps: []func(context.Context, *StatusManager, *vitistackv1alpha1.KubernetesCluster) error{
				func(ctx context.Context, m *StatusManager, kc *vitistackv1alpha1.KubernetesCluster) error {
					return m.SetMessage(ctx, kc, "Applying config to worker p-bgo-ek-viti-68ax-wrk3")
				},
				func(ctx context.Context, m *StatusManager, kc *vitistackv1alpha1.KubernetesCluster) error {
					return m.SetMessage(ctx, kc, "")
				},
			},
			check: func(t *testing.T, got *vitistackv1alpha1.KubernetesCluster) {
				if got.Status.Message != "" {
					t.Errorf("message = %q, want empty", got.Status.Message)
				}
			},
		},
		{
			name: "phase moves away and back",
			steps: []func(context.Context, *StatusManager, *vitistackv1alpha1.KubernetesCluster) error{
				func(ctx context.Context, m *StatusManager, kc *vitistackv1alpha1.KubernetesCluster) error {
					return m.SetPhase(ctx, kc, "Ready")
				},
				func(ctx context.Context, m *StatusManager, kc *vitistackv1alpha1.KubernetesCluster) error {
					return m.SetPhase(ctx, kc, "WaitingForNodes")
				},
				func(ctx context.Context, m *StatusManager, kc *vitistackv1alpha1.KubernetesCluster) error {
					return m.SetPhase(ctx, kc, "Ready")
				},
			},
			check: func(t *testing.T, got *vitistackv1alpha1.KubernetesCluster) {
				if got.Status.Phase != "Ready" {
					t.Errorf("phase = %q, want Ready", got.Status.Phase)
				}
			},
		},
		{
			name: "condition flips and flips back",
			steps: []func(context.Context, *StatusManager, *vitistackv1alpha1.KubernetesCluster) error{
				func(ctx context.Context, m *StatusManager, kc *vitistackv1alpha1.KubernetesCluster) error {
					return m.SetCondition(ctx, kc, "NodesReady", "False", "Waiting", "Waiting for nodes")
				},
				func(ctx context.Context, m *StatusManager, kc *vitistackv1alpha1.KubernetesCluster) error {
					return m.SetCondition(ctx, kc, "NodesReady", "True", "AllReady", "All nodes Ready")
				},
				func(ctx context.Context, m *StatusManager, kc *vitistackv1alpha1.KubernetesCluster) error {
					return m.SetCondition(ctx, kc, "NodesReady", "False", "Waiting", "Waiting for nodes")
				},
			},
			check: func(t *testing.T, got *vitistackv1alpha1.KubernetesCluster) {
				if len(got.Status.Conditions) != 1 {
					t.Fatalf("got %d conditions, want 1: %+v", len(got.Status.Conditions), got.Status.Conditions)
				}
				if c := got.Status.Conditions[0]; c.Status != "False" || c.Reason != "Waiting" {
					t.Errorf("NodesReady = %s/%s, want False/Waiting", c.Status, c.Reason)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m, c, _ := newTestManager(t, testCluster())
			ctx := context.Background()
			kc := getCluster(t, c) // one object for the whole pass, as in a reconcile
			for i, step := range tt.steps {
				if err := step(ctx, m, kc); err != nil {
					t.Fatalf("step %d: %v", i, err)
				}
			}
			tt.check(t, getCluster(t, c))
		})
	}
}

func TestStatusSetters_NoCallsWhenHeldObjectMatches(t *testing.T) {
	t.Parallel()

	held := testCluster()
	held.Status.Message = "Scaling down"
	held.Status.Phase = "Ready"
	held.Status.Conditions = []vitistackv1alpha1.KubernetesClusterCondition{{
		Type: "Valid", Status: "True", Reason: "ValidationPassed", Message: "Cluster spec is valid",
		LastTransitionTime: "2026-09-01T10:48:28.886302175Z",
	}}

	m, _, calls := newTestManager(t, held.DeepCopy())
	ctx := context.Background()

	if err := m.SetMessage(ctx, held, "Scaling down"); err != nil {
		t.Fatal(err)
	}
	if err := m.SetPhase(ctx, held, "Ready"); err != nil {
		t.Fatal(err)
	}
	if err := m.SetCondition(ctx, held, "Valid", "True", "ValidationPassed", "Cluster spec is valid"); err != nil {
		t.Fatal(err)
	}

	if calls.liveReads != 0 || calls.writes() != 0 {
		t.Errorf("unchanged values: %d live reads and %d writes, want 0 and 0", calls.liveReads, calls.writes())
	}
}

// A pass can hold an object read before its own writes landed (or the cache
// can lag); the live comparison must still see there is nothing to change.
func TestAggregateFromMachines_StaleHeldObjectDoesNotRewrite(t *testing.T) {
	t.Parallel()
	m, c, calls := newTestManager(t, fiveNodeCluster()...)
	ctx := context.Background()

	fresh := getCluster(t, c)
	stale := getCluster(t, c)
	if err := m.AggregateFromMachines(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	calls.reset()

	if err := m.AggregateFromMachines(ctx, stale); err != nil {
		t.Fatal(err)
	}
	if got := calls.writes(); got != 0 {
		t.Errorf("aggregates already stored: %d writes, want 0", got)
	}
}

// The UID annotation is what stops kube-system being read again, so it must
// not be stored while the creation time is still missing.
func TestUpdateKubernetesClusterStatus_CreatedTimeConflictIsRetried(t *testing.T) {
	t.Parallel()
	m, c, calls := newTestManager(t, fiveNodeCluster()...)
	ctx := context.Background()

	calls.conflictStatus = true
	if err := m.UpdateKubernetesClusterStatus(ctx, getCluster(t, c)); err != nil {
		t.Fatal(err)
	}
	if got := getCluster(t, c).Annotations[kubeSystemUIDK]; got != "" {
		t.Fatalf("kube-system-uid recorded as %q while state.created was never written", got)
	}

	calls.mu.Lock()
	calls.conflictStatus = false
	calls.mu.Unlock()
	if err := m.UpdateKubernetesClusterStatus(ctx, getCluster(t, c)); err != nil {
		t.Fatal(err)
	}
	got := getCluster(t, c)
	if !got.Status.State.Created.Time.Equal(kubeSystemCreated) {
		t.Errorf("state.created = %v, want %v", got.Status.State.Created.Time, kubeSystemCreated)
	}
	if got.Annotations[kubeSystemUIDK] != kubeSystemUID {
		t.Errorf("kube-system-uid annotation = %q, want %q", got.Annotations[kubeSystemUIDK], kubeSystemUID)
	}
}
