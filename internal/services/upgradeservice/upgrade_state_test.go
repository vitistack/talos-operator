package upgradeservice

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// laggingSecretStore stands in for the operator's cached secret client: right
// after a write, the next read still returns the secret as it was before that
// write, and a write carrying an outdated resourceVersion is rejected.
type laggingSecretStore struct {
	mu             sync.Mutex
	live           *corev1.Secret
	cached         *corev1.Secret
	staleReads     int
	updates        int
	alwaysConflict bool
}

func newLaggingSecretStore(t *testing.T, data map[string][]byte) *laggingSecretStore {
	t.Helper()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "team-feskbaill-001-1337", Namespace: "team-feskbaill", ResourceVersion: "100"},
		Data:       data,
	}
	return &laggingSecretStore{live: secret, cached: secret.DeepCopy()}
}

func (s *laggingSecretStore) GetTalosSecret(context.Context, *vitistackv1alpha1.KubernetesCluster) (*corev1.Secret, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.staleReads > 0 {
		s.staleReads--
		return s.cached.DeepCopy(), nil
	}
	s.cached = s.live.DeepCopy()
	return s.cached.DeepCopy(), nil
}

func (s *laggingSecretStore) UpdateTalosSecret(_ context.Context, secret *corev1.Secret) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates++
	if s.alwaysConflict || secret.ResourceVersion != s.live.ResourceVersion {
		return apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, secret.Name, nil)
	}
	rv, _ := strconv.Atoi(s.live.ResourceVersion)
	next := secret.DeepCopy()
	next.ResourceVersion = strconv.Itoa(rv + 1)
	s.live = next
	s.staleReads = 1
	return nil
}

func (s *laggingSecretStore) storedState(t *testing.T) (*ClusterUpgradeState, map[string][]byte) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var state ClusterUpgradeState
	if err := json.Unmarshal(s.live.Data["upgrade_state"], &state); err != nil {
		t.Fatal(err)
	}
	return &state, s.live.Data
}

// controlPlanesSettledState is a Talos upgrade whose control plane has been
// upgraded and has settled, with one worker still to go.
func controlPlanesSettledState(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(ClusterUpgradeState{
		UpgradeType:       UpgradeTypeTalos,
		TargetVersion:     "v1.13.10",
		Phase:             UpgradePhaseControlPlanesWait,
		CurrentNodeIndex:  0,
		ControlPlaneCount: 1,
		WorkerCount:       1,
		Nodes: []NodeUpgradeState{
			{NodeName: "ctp0", Role: "control-plane", UpgradeInitiated: true, UpgradeCompleted: true},
			{NodeName: "wrk0", Role: "worker"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

var testUpgradeCluster = &vitistackv1alpha1.KubernetesCluster{
	ObjectMeta: metav1.ObjectMeta{Name: "team-feskbaill-001-1337", Namespace: "team-feskbaill"},
}

// The failure seen on team-feskbaill-001-1337: MarkNodeReady and
// TransitionToWorkers write the secret back-to-back, the second read comes from
// a cache that has not seen the first write, and the conflict failed the whole
// Talos upgrade.
func TestTransitionToWorkers_RetriesStaleReadWithoutLosingEarlierWrite(t *testing.T) {
	t.Parallel()
	store := newLaggingSecretStore(t, map[string][]byte{"upgrade_state": controlPlanesSettledState(t)})
	m := NewUpgradeStateManager(store, store)
	ctx := context.Background()

	if err := m.MarkNodeReady(ctx, testUpgradeCluster, "ctp0"); err != nil {
		t.Fatalf("MarkNodeReady: %v", err)
	}
	if err := m.TransitionToWorkers(ctx, testUpgradeCluster); err != nil {
		t.Fatalf("TransitionToWorkers: %v", err)
	}

	state, _ := store.storedState(t)
	if state.Phase != UpgradePhaseWorkers || state.CurrentNodeIndex != 1 || state.CurrentNodeName != "wrk0" {
		t.Errorf("phase/index/node = %s/%d/%s, want workers/1/wrk0", state.Phase, state.CurrentNodeIndex, state.CurrentNodeName)
	}
	if !state.Nodes[0].NodeReady {
		t.Error("ctp0 lost the NodeReady mark written just before the transition")
	}
}

func TestSaveUpgradeState_RetriesStaleReadAndKeepsOtherKeys(t *testing.T) {
	t.Parallel()
	store := newLaggingSecretStore(t, map[string][]byte{"upgrade_state": controlPlanesSettledState(t)})
	m := NewUpgradeStateManager(store, store)
	ctx := context.Background()

	// Another write in the same pass, e.g. markDriftRecoveryRan.
	fresh, _ := store.GetTalosSecret(ctx, testUpgradeCluster)
	fresh.Data["drift_recovery_at"] = []byte("2026-09-17T11:21:19Z")
	if err := store.UpdateTalosSecret(ctx, fresh); err != nil {
		t.Fatal(err)
	}

	completed := &ClusterUpgradeState{UpgradeType: UpgradeTypeTalos, TargetVersion: "v1.13.10", Phase: UpgradePhaseCompleted}
	if err := m.SaveUpgradeState(ctx, testUpgradeCluster, completed); err != nil {
		t.Fatalf("SaveUpgradeState: %v", err)
	}

	state, data := store.storedState(t)
	if state.Phase != UpgradePhaseCompleted {
		t.Errorf("phase = %s, want completed", state.Phase)
	}
	if got := string(data["drift_recovery_at"]); got != "2026-09-17T11:21:19Z" {
		t.Errorf("drift_recovery_at = %q, want the value written before the save", got)
	}
}

func TestUpgradeStateWrite_GivesUpOnPersistentConflict(t *testing.T) {
	t.Parallel()
	store := newLaggingSecretStore(t, map[string][]byte{"upgrade_state": controlPlanesSettledState(t)})
	store.alwaysConflict = true
	m := NewUpgradeStateManager(store, store)

	err := m.TransitionToWorkers(context.Background(), testUpgradeCluster)

	if !apierrors.IsConflict(err) {
		t.Fatalf("error = %v, want a conflict", err)
	}
	if store.updates < 2 {
		t.Errorf("gave up after %d write attempt(s), want a retry", store.updates)
	}
}
