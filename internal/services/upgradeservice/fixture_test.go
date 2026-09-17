package upgradeservice

import (
	"context"
	"strings"
	"testing"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/internal/services/machineservice"
	"github.com/vitistack/talos-operator/internal/services/secretservice"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	"github.com/vitistack/talos-operator/internal/services/talosconfigservice"
	"github.com/vitistack/talos-operator/internal/services/talosstateservice"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	fixtureNamespace = "vitistack-amk"
	fixtureName      = "d-amk-003"
	fixtureClusterID = "d-amk-003-gsax"
)

// legacyCluster is a KubernetesCluster created before spec.data.networkNamespaceName
// existed. The CRD now requires the field with minLength 1, so the API server
// rejects any write that sends it as "" — which every full Update of the typed
// object does, because the Go field has no omitempty.
func legacyCluster(annotations map[string]string) *vitistackv1alpha1.KubernetesCluster {
	return &vitistackv1alpha1.KubernetesCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:        fixtureName,
			Namespace:   fixtureNamespace,
			Annotations: annotations,
		},
		Spec: vitistackv1alpha1.KubernetesClusterSpec{
			Cluster: vitistackv1alpha1.KubernetesClusterSpecData{ClusterId: fixtureClusterID, Provider: "talos"},
		},
	}
}

func fixtureSecret(data map[string]string) *corev1.Secret {
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: fixtureClusterID, Namespace: fixtureNamespace},
		Data:       map[string][]byte{},
	}
	for k, v := range data {
		s.Data[k] = []byte(v)
	}
	return s
}

// fixture bundles an UpgradeService and UpgradeController wired to a fake API
// server that enforces the networkNamespaceName validation the real one does.
type fixture struct {
	client     client.Client
	service    *UpgradeService
	controller *UpgradeController
	failPatch  error
}

func newFixture(t *testing.T, objs ...client.Object) *fixture {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := vitistackv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	f := &fixture{}
	f.client = fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&vitistackv1alpha1.KubernetesCluster{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if kc, ok := obj.(*vitistackv1alpha1.KubernetesCluster); ok && kc.Spec.Cluster.NetworkNamespaceName == "" {
					return invalidNetworkNamespaceName(kc.Name)
				}
				return cl.Update(ctx, obj, opts...)
			},
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if f.failPatch != nil {
					return f.failPatch
				}
				if kc, ok := obj.(*vitistackv1alpha1.KubernetesCluster); ok {
					data, err := patch.Data(obj)
					if err != nil {
						return err
					}
					// A patch that carries spec re-validates it; one that only
					// touches metadata leaves the absent field alone.
					if strings.Contains(string(data), `"spec"`) && kc.Spec.Cluster.NetworkNamespaceName == "" {
						return invalidNetworkNamespaceName(kc.Name)
					}
				}
				return cl.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	secretSvc := secretservice.NewSecretService(f.client)
	stateSvc := talosstateservice.NewTalosStateService(secretSvc)
	statusMgr := status.NewManager(f.client, secretSvc, stateSvc)
	clientSvc := talosclientservice.NewTalosClientService()
	f.service = NewUpgradeService(f.client, statusMgr, clientSvc, machineservice.NewMachineService(f.client), stateSvc, talosconfigservice.NewTalosConfigService())
	f.controller = NewUpgradeController(f.client, secretSvc, statusMgr, clientSvc, f.service)
	return f
}

func invalidNetworkNamespaceName(name string) error {
	return apierrors.NewInvalid(
		schema.GroupKind{Group: "vitistack.io", Kind: "KubernetesCluster"},
		name,
		field.ErrorList{field.Invalid(field.NewPath("spec", "data", "networkNamespaceName"), "",
			"spec.data.networkNamespaceName in body should be at least 1 chars long")},
	)
}

// cluster reads the stored cluster the way a reconcile pass starts.
func (f *fixture) cluster(t *testing.T) *vitistackv1alpha1.KubernetesCluster {
	t.Helper()
	kc := &vitistackv1alpha1.KubernetesCluster{}
	if err := f.client.Get(context.Background(), types.NamespacedName{Name: fixtureName, Namespace: fixtureNamespace}, kc); err != nil {
		t.Fatal(err)
	}
	return kc
}

func (f *fixture) secretData(t *testing.T) map[string]string {
	t.Helper()
	s := &corev1.Secret{}
	if err := f.client.Get(context.Background(), types.NamespacedName{Name: fixtureClusterID, Namespace: fixtureNamespace}, s); err != nil {
		t.Fatal(err)
	}
	out := make(map[string]string, len(s.Data))
	for k, v := range s.Data {
		out[k] = string(v)
	}
	return out
}

func condition(kc *vitistackv1alpha1.KubernetesCluster, condType string) *vitistackv1alpha1.KubernetesClusterCondition {
	for i := range kc.Status.Conditions {
		if kc.Status.Conditions[i].Type == condType {
			return &kc.Status.Conditions[i]
		}
	}
	return nil
}
