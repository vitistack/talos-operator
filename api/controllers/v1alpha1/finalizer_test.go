package v1alpha1

import (
	"context"
	"testing"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// A cluster created before spec.data.networkNamespaceName existed cannot take a
// full Update (the typed object sends the field as "" and fails minLength), so
// adding the finalizer must patch only metadata.
func TestEnsureFinalizer_LegacyClusterWithoutNetworkNamespaceName(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := vitistackv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	kc := &vitistackv1alpha1.KubernetesCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "d-trd-atlas-001", Namespace: "vitistack-atlas"},
		Spec: vitistackv1alpha1.KubernetesClusterSpec{
			Cluster: vitistackv1alpha1.KubernetesClusterSpecData{ClusterId: "d-trd-atlas-001-xdf2", Provider: "talos"},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(kc).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if held, ok := obj.(*vitistackv1alpha1.KubernetesCluster); ok && held.Spec.Cluster.NetworkNamespaceName == "" {
					return apierrors.NewInvalid(schema.GroupKind{Group: "vitistack.io", Kind: "KubernetesCluster"}, held.Name,
						field.ErrorList{field.Invalid(field.NewPath("spec", "data", "networkNamespaceName"), "", "should be at least 1 chars long")})
				}
				return cl.Update(ctx, obj, opts...)
			},
		}).
		Build()
	r := &KubernetesClusterReconciler{Client: c, Scheme: scheme}

	held := &vitistackv1alpha1.KubernetesCluster{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(kc), held); err != nil {
		t.Fatal(err)
	}
	added, err := r.ensureFinalizer(context.Background(), held)
	if err != nil {
		t.Fatalf("ensureFinalizer: %v", err)
	}
	if !added {
		t.Fatal("ensureFinalizer reported no change, want the finalizer added")
	}

	stored := &vitistackv1alpha1.KubernetesCluster{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(kc), stored); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(stored, KubernetesClusterFinalizer) {
		t.Fatalf("stored finalizers = %v, want %s", stored.Finalizers, KubernetesClusterFinalizer)
	}
}
