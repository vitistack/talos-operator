package networknamespaceservice

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
	Resource: "networknamespaces",
}

// FetchNetworkNamespacesByNamespace fetches a cluster-scoped NetworkNamespace resource by name
func FetchNetworkNamespacesByNamespace(ctx context.Context, namespace string) ([]*vitistackv1alpha1.NetworkNamespace, error) {
	// Use the dynamic client to fetch the cluster-scoped resource
	unstructuredList, err := k8sclient.DynamicClient.Resource(vitistackGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get NetworkNamespaces from namespace %q: %w", namespace, err)
	}

	networkNamespaces := make([]*vitistackv1alpha1.NetworkNamespace, len(unstructuredList.Items))
	for i, unstructured := range unstructuredList.Items {
		// Convert unstructured to NetworkNamespace using the common util
		networkNamespace, err := unstructuredutil.NetworkNamespaceFromUnstructured(&unstructured)
		if err != nil {
			return nil, fmt.Errorf("failed to convert NetworkNamespace: %w", err)
		}
		networkNamespaces[i] = networkNamespace
	}

	return networkNamespaces, nil
}

func FetchFirstNetworkNamespaceByNamespace(ctx context.Context, namespace string) (*vitistackv1alpha1.NetworkNamespace, error) {
	networkNamespaces, err := FetchNetworkNamespacesByNamespace(ctx, namespace)
	if err != nil {
		return nil, err
	}

	if len(networkNamespaces) == 0 {
		return nil, fmt.Errorf("no NetworkNamespaces found in namespace %q", namespace)
	}

	return networkNamespaces[0], nil
}
