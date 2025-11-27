package vitistackservice

import (
	"context"
	"fmt"

	"github.com/vitistack/common/pkg/clients/k8sclient"
	"github.com/vitistack/common/pkg/unstructuredutil"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// vitistackGVR is the GroupVersionResource for vitistack.io/v1alpha1 Vitistack
var vitistackGVR = schema.GroupVersionResource{
	Group:    "vitistack.io",
	Version:  "v1alpha1",
	Resource: "vitistacks",
}

// FetchVitistackByName fetches a cluster-scoped Vitistack resource by name
func FetchVitistackByName(ctx context.Context, name string) (*vitistackv1alpha1.Vitistack, error) {
	// Use the dynamic client to fetch the cluster-scoped resource
	unstructured, err := k8sclient.DynamicClient.Resource(vitistackGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get Vitistack %q: %w", name, err)
	}

	// Convert unstructured to Vitistack using the common util
	vitistack, err := unstructuredutil.VitistackFromUnstructured(unstructured)
	if err != nil {
		return nil, fmt.Errorf("failed to convert Vitistack: %w", err)
	}

	return vitistack, nil
}
