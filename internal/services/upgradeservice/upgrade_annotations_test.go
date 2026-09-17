package upgradeservice

import (
	"context"
	"errors"
	"testing"

	"github.com/vitistack/talos-operator/pkg/consts"
)

// Annotation writes must not resend spec: on a cluster created before
// networkNamespaceName existed, a full Update is rejected and the finished
// upgrade could never clear its talos-target (d-amk-003, 2026-09-17).
func TestRemoveAnnotation_LegacyClusterWithoutNetworkNamespaceName(t *testing.T) {
	f := newFixture(t, legacyCluster(map[string]string{consts.TalosTargetAnnotation: "v1.13.10"}))
	kc := f.cluster(t)

	if err := f.service.RemoveAnnotation(context.Background(), kc, consts.TalosTargetAnnotation); err != nil {
		t.Fatalf("RemoveAnnotation: %v", err)
	}

	if v, ok := f.cluster(t).Annotations[consts.TalosTargetAnnotation]; ok {
		t.Fatalf("talos-target still stored: %q", v)
	}
}

func TestSetAnnotation_LegacyClusterWithoutNetworkNamespaceName(t *testing.T) {
	f := newFixture(t, legacyCluster(nil))
	kc := f.cluster(t)

	if err := f.service.SetAnnotation(context.Background(), kc, consts.TalosMessageAnnotation, "hello"); err != nil {
		t.Fatalf("SetAnnotation: %v", err)
	}

	if got := f.cluster(t).Annotations[consts.TalosMessageAnnotation]; got != "hello" {
		t.Fatalf("talos-message = %q, want %q", got, "hello")
	}
}

// A failed write must leave the held object as it was; otherwise later code in
// the same pass believes the annotation is gone when it is still stored.
func TestRemoveAnnotation_FailedWriteKeepsHeldAnnotation(t *testing.T) {
	f := newFixture(t, legacyCluster(map[string]string{consts.TalosTargetAnnotation: "v1.13.10"}))
	kc := f.cluster(t)
	f.failPatch = errors.New("apiserver unavailable")

	if err := f.service.RemoveAnnotation(context.Background(), kc, consts.TalosTargetAnnotation); err == nil {
		t.Fatal("RemoveAnnotation succeeded, want error")
	}

	if got := kc.Annotations[consts.TalosTargetAnnotation]; got != "v1.13.10" {
		t.Fatalf("held talos-target = %q after failed write, want %q", got, "v1.13.10")
	}
}

func TestSetAnnotation_FailedWriteKeepsHeldAnnotation(t *testing.T) {
	f := newFixture(t, legacyCluster(map[string]string{consts.TalosMessageAnnotation: "old"}))
	kc := f.cluster(t)
	f.failPatch = errors.New("apiserver unavailable")

	if err := f.service.SetAnnotation(context.Background(), kc, consts.TalosMessageAnnotation, "new"); err == nil {
		t.Fatal("SetAnnotation succeeded, want error")
	}

	if got := kc.Annotations[consts.TalosMessageAnnotation]; got != "old" {
		t.Fatalf("held talos-message = %q after failed write, want %q", got, "old")
	}
}
