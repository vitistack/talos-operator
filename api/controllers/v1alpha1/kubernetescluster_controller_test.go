package v1alpha1

import (
	"context"
	"testing"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEnsureFinalizer(t *testing.T) {
	testcases := []struct {
		desc        string
		existing    *vitistackv1alpha1.KubernetesCluster
		wantChanged bool
	}{
		{
			desc:        "adds when missing",
			existing:    &vitistackv1alpha1.KubernetesCluster{ObjectMeta: metav1.ObjectMeta{Name: "test"}},
			wantChanged: true,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			s := runtime.NewScheme()
			if err := vitistackv1alpha1.AddToScheme(s); err != nil {
				t.Fatalf("add scheme: %v", err)
			}

			fakeclient := fake.NewClientBuilder().WithObjects(tc.existing).WithScheme(s).Build()
			r := &KubernetesClusterReconciler{Client: fakeclient}

			changed, err := r.ensureFinalizer(context.Background(), tc.existing)
			if err != nil {
				t.Fatalf("ensureFinalizer() unexpected error: %v", err)
			}
			if changed != tc.wantChanged {
				t.Errorf("got %v, want %v", changed, tc.wantChanged)
			}
		})
	}
}
