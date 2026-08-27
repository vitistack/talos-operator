package v1alpha1

import (
	"context"
	"fmt"
	"testing"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestEnsureFinalizer(t *testing.T) {
	testScheme := runtime.NewScheme()
	if err := vitistackv1alpha1.AddToScheme(testScheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	testcases := []struct {
		desc        string
		existing    *vitistackv1alpha1.KubernetesCluster
		wantChanged bool
	}{
		{
			desc: "adds when missing",
			existing: &vitistackv1alpha1.KubernetesCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
				},
			},
			wantChanged: true,
		},
		{
			desc: "no-op when already present",
			existing: &vitistackv1alpha1.KubernetesCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test",
					Finalizers: []string{KubernetesClusterFinalizer},
				},
			},
			wantChanged: false,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			fakeclient := fake.NewClientBuilder().WithObjects(tc.existing).WithScheme(testScheme).Build()

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

	t.Run("retries on conflict", func(t *testing.T) {
		kc := &vitistackv1alpha1.KubernetesCluster{ObjectMeta: metav1.ObjectMeta{Name: "test"}}

		var updateCalls int

		fakeclient := fake.NewClientBuilder().
			WithScheme(testScheme).
			WithObjects(kc).
			WithInterceptorFuncs(interceptor.Funcs{
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					updateCalls++
					if updateCalls == 1 {
						return apierrors.NewConflict(
							vitistackv1alpha1.GroupVersion.WithResource("kubernetesclusters").GroupResource(),
							obj.GetName(),
							fmt.Errorf("simulated conflict"))
					}
					return c.Update(ctx, obj, opts...)
				},
			}).Build()

		r := &KubernetesClusterReconciler{Client: fakeclient}

		changed, err := r.ensureFinalizer(context.Background(), kc)
		if err != nil {
			t.Fatalf("ensureFinalizer() unexpected error: %v", err)
		}

		if !changed {
			t.Errorf("got changed = false, want true")
		}

		if updateCalls != 2 {
			t.Errorf("got %d update attempts, want 2", updateCalls)
		}
	})
}
