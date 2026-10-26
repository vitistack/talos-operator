package v1alpha1

import (
	"errors"
	"fmt"
	"testing"
	"time"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/drainservice"
)

func TestScaleDownRequeueAfter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		err         error
		want        time.Duration
		wantPending bool
	}{
		{
			// A drain waiting on a PDB is progress, not a failure: requeue on
			// the drain's own backoff instead of the 5s controller cadence.
			name:        "drain pending",
			err:         fmt.Errorf("delete worker: %w", &drainservice.PendingError{RetryAfter: 40 * time.Second}),
			want:        40 * time.Second,
			wantPending: true,
		},
		{
			name: "drain failed talking to the API server",
			err:  fmt.Errorf("%w: list pods: boom", drainservice.ErrIncomplete),
			want: drainservice.DefaultErrorRetry,
		},
		{
			name: "any other deletion error",
			err:  errors.New("etcd member removal failed"),
			want: ControllerRequeueDelay,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, pending := scaleDownRequeueAfter(tt.err)
			if got != tt.want || pending != tt.wantPending {
				t.Errorf("scaleDownRequeueAfter() = (%s, %t), want (%s, %t)", got, pending, tt.want, tt.wantPending)
			}
		})
	}
}

func TestAbandonedDrainPurposes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		hasPlan bool
		err     error
		want    []drainservice.Purpose
	}{
		{
			// Reaching the release step means no scale-down is pending, and
			// no upgrade plan means no upgrade owns a drain either.
			name: "nothing in progress",
			want: []drainservice.Purpose{drainservice.PurposeScaleDown, drainservice.PurposeUpgrade},
		},
		{
			// The phase can read Ready mid-upgrade; the plan is authoritative.
			name:    "upgrade plan present",
			hasPlan: true,
			want:    []drainservice.Purpose{drainservice.PurposeScaleDown},
		},
		{
			name: "upgrade state unreadable",
			err:  errors.New("secret read failed"),
			want: []drainservice.Purpose{drainservice.PurposeScaleDown},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := abandonedDrainPurposes(tt.hasPlan, tt.err)
			if len(got) != len(tt.want) {
				t.Fatalf("abandonedDrainPurposes() = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("abandonedDrainPurposes() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestScaleDownRetryLetsReconcileContinueWhileDrainIsPending(t *testing.T) {
	t.Parallel()
	cluster := &vitistackv1alpha1.KubernetesCluster{}

	pending := scaleDownRetry(cluster, "worker", "w-3", &drainservice.PendingError{RetryAfter: 20 * time.Second})
	if pending.stop || !pending.drainPending || pending.result.RequeueAfter != 20*time.Second {
		t.Fatalf("a pending drain must not stop the rest of the reconcile, got %+v", pending)
	}

	failed := scaleDownRetry(cluster, "worker", "w-3", errors.New("etcd member removal failed"))
	if !failed.stop || failed.drainPending {
		t.Fatalf("a failed removal stops the reconcile as before, got %+v", failed)
	}
}
