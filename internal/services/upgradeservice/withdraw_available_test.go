package upgradeservice

import (
	"context"
	"testing"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/vitistack/talos-operator/pkg/consts"
)

func withdrawTestService(t *testing.T, annotations map[string]string) (*UpgradeService, *vitistackv1alpha1.KubernetesCluster) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := vitistackv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	cluster := &vitistackv1alpha1.KubernetesCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "cluster",
			Namespace:   "ns",
			Annotations: annotations,
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	return &UpgradeService{Client: c}, cluster
}

const withdrawReason = "Talos 1.12.x supports Kubernetes up to 1.35; upgrade Talos first"

// A cluster that was advertised an incompatible version must lose the offer
// and gain the reason. This is what clears the ones advertised before the
// compatibility gate existed.
func TestWithdrawKubernetesUpgradeAvailable_RetractsAndExplains(t *testing.T) {
	s, cluster := withdrawTestService(t, map[string]string{
		consts.KubernetesCurrentAnnotation:   "1.35.0",
		consts.KubernetesAvailableAnnotation: "1.36.3",
		consts.KubernetesStatusAnnotation:    string(consts.UpgradeStatusIdle),
	})

	if err := s.WithdrawKubernetesUpgradeAvailable(context.Background(), cluster, withdrawReason); err != nil {
		t.Fatalf("WithdrawKubernetesUpgradeAvailable: %v", err)
	}

	ann := cluster.GetAnnotations()
	if got, ok := ann[consts.KubernetesAvailableAnnotation]; ok {
		t.Errorf("kubernetes-available = %q, want it removed", got)
	}
	if got := ann[consts.KubernetesMessageAnnotation]; got != withdrawReason {
		t.Errorf("kubernetes-message = %q, want %q", got, withdrawReason)
	}
}

// The case the first implementation missed: a cluster that was never offered
// anything still has to say why nothing is on offer, or the UI shows an
// unexplained blank.
func TestWithdrawKubernetesUpgradeAvailable_ExplainsWithNothingAdvertised(t *testing.T) {
	s, cluster := withdrawTestService(t, map[string]string{
		consts.KubernetesCurrentAnnotation: "1.35.0",
	})

	if err := s.WithdrawKubernetesUpgradeAvailable(context.Background(), cluster, withdrawReason); err != nil {
		t.Fatalf("WithdrawKubernetesUpgradeAvailable: %v", err)
	}

	ann := cluster.GetAnnotations()
	if got := ann[consts.KubernetesMessageAnnotation]; got != withdrawReason {
		t.Errorf("kubernetes-message = %q, want %q", got, withdrawReason)
	}
	if got := ann[consts.KubernetesStatusAnnotation]; got != string(consts.UpgradeStatusIdle) {
		t.Errorf("kubernetes-status = %q, want %q", got, consts.UpgradeStatusIdle)
	}
}

// Runs on every reconcile of an affected cluster, so it must settle rather
// than rewrite. SetUpgradeAnnotations patches only on change; this guards
// that the surrounding logic does not defeat it.
func TestWithdrawKubernetesUpgradeAvailable_Idempotent(t *testing.T) {
	s, cluster := withdrawTestService(t, map[string]string{
		consts.KubernetesCurrentAnnotation:   "1.35.0",
		consts.KubernetesAvailableAnnotation: "1.36.3",
	})

	ctx := context.Background()
	for i := range 3 {
		if err := s.WithdrawKubernetesUpgradeAvailable(ctx, cluster, withdrawReason); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}

	ann := cluster.GetAnnotations()
	if _, ok := ann[consts.KubernetesAvailableAnnotation]; ok {
		t.Error("kubernetes-available reappeared")
	}
	if got := ann[consts.KubernetesMessageAnnotation]; got != withdrawReason {
		t.Errorf("kubernetes-message = %q, want %q", got, withdrawReason)
	}
}

// An upgrade the operator is actively running owns the status field; a
// withdrawal must not reset it to idle underneath the state machine.
func TestWithdrawKubernetesUpgradeAvailable_LeavesInProgressStatus(t *testing.T) {
	s, cluster := withdrawTestService(t, map[string]string{
		consts.KubernetesCurrentAnnotation:   "1.35.0",
		consts.KubernetesAvailableAnnotation: "1.36.3",
		consts.KubernetesStatusAnnotation:    string(consts.UpgradeStatusInProgress),
	})

	if err := s.WithdrawKubernetesUpgradeAvailable(context.Background(), cluster, withdrawReason); err != nil {
		t.Fatalf("WithdrawKubernetesUpgradeAvailable: %v", err)
	}

	if got := cluster.GetAnnotations()[consts.KubernetesStatusAnnotation]; got != string(consts.UpgradeStatusInProgress) {
		t.Errorf("kubernetes-status = %q, want it left at %q", got, consts.UpgradeStatusInProgress)
	}
}
