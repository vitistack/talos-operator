package status

import (
	"context"
	"fmt"
	"testing"

	// apierrors "k8s.io/apimachinery/pkg/api/errors"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestSetMessage_Success(t *testing.T) {
	testScheme := runtime.NewScheme()
	if err := vitistackv1alpha1.AddToScheme(testScheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	testcases := []struct {
		desc     string
		kc       *vitistackv1alpha1.KubernetesCluster
		wantMsg  string
		wantNoop bool
	}{
		{
			desc: "successfully set a status message",
			kc: &vitistackv1alpha1.KubernetesCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "testcluster",
				},
			},
			wantMsg: "updated",
		},
		{
			desc: "successfully overwrite a status message",
			kc: &vitistackv1alpha1.KubernetesCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "testcluster",
				},
				Status: vitistackv1alpha1.KubernetesClusterStatus{
					Message: "stale",
				},
			},
			wantMsg: "updated",
		},
		{
			desc: "successfully clear a status message",
			kc: &vitistackv1alpha1.KubernetesCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "testcluster",
				},
				Status: vitistackv1alpha1.KubernetesClusterStatus{
					Message: "stale",
				},
			},
			wantMsg: "",
		},
		{
			desc: "no-op when old message matches new",
			kc: &vitistackv1alpha1.KubernetesCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "testcluster",
				},
				Status: vitistackv1alpha1.KubernetesClusterStatus{
					Message: "old message",
				},
			},
			wantMsg:  "old message",
			wantNoop: true,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			var updateCalls int
			fakeclient := fake.NewClientBuilder().
				WithScheme(testScheme).
				WithStatusSubresource(&vitistackv1alpha1.KubernetesCluster{}).
				WithObjects(tc.kc).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
						updateCalls++
						return c.SubResource(subResourceName).Update(context.Background(), obj, opts...)
					},
				}).
				Build()

			m := NewManager(fakeclient, nil, nil)

			err := m.SetMessage(context.Background(), tc.kc, tc.wantMsg)
			if err != nil {
				t.Fatalf("setMessage() unexpected error: %v", err)
			}

			got := &vitistackv1alpha1.KubernetesCluster{}
			if err := fakeclient.Get(context.Background(), client.ObjectKeyFromObject(tc.kc), got); err != nil {
				t.Fatalf("failed to refetch: %v", err)
			}

			if got.Status.Message != tc.wantMsg {
				t.Errorf("got status message %q, want %q", got.Status.Message, tc.wantMsg)
			}

			if tc.wantNoop {
				if updateCalls > 0 {
					t.Errorf("got %d update calls, wanted 0 (no-op)", updateCalls)
				}
			}

			// fails since the in-memory copy of kc is not updated, only the client store
			// if kc.Status.Message != tc.msg {
			// 	t.Errorf("got status message %q, want %q", kc.Status.Message, tc.msg)
			// }
		})
	}
}

// TODO: implement retry functionality in SetMessage
func TestSetMessage_Conflict(t *testing.T) {
	t.Skip("awaiting retry functionality")
	testScheme := runtime.NewScheme()
	if err := vitistackv1alpha1.AddToScheme(testScheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	kc := &vitistackv1alpha1.KubernetesCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "asdf",
		},
	}

	var updateCalls int
	fakeclient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithStatusSubresource(&vitistackv1alpha1.KubernetesCluster{}).
		WithObjects(kc).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				updateCalls++
				if updateCalls == 1 {
					return apierrors.NewConflict(
						vitistackv1alpha1.GroupVersion.WithResource("kubernetesclusters").GroupResource(),
						obj.GetName(),
						fmt.Errorf("simulated conflict"),
					)
				}
				return c.SubResource(subResourceName).Update(context.Background(), obj, opts...)
			},
		}).Build()

	m := NewManager(fakeclient, nil, nil)

	wantMsg := "updation"

	err := m.SetMessage(context.Background(), kc, wantMsg)
	if err != nil {
		t.Fatalf("setMessage() unexpected error: %v", err)
	}

	if updateCalls != 2 {
		t.Errorf("got %d update attempts, want 2", updateCalls)
	}

	got := &vitistackv1alpha1.KubernetesCluster{}
	if err := fakeclient.Get(context.Background(), client.ObjectKeyFromObject(kc), got); err != nil {
		t.Fatalf("failed to refetch: %v", err)
	}

	if got.Status.Message != wantMsg {
		t.Errorf("got status message %q, want %q", got.Status.Message, wantMsg)
	}
}
