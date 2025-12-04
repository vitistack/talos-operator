package machineclassservice

import (
	"context"
	"fmt"

	"github.com/vitistack/common/pkg/clients/k8sclient"
	"github.com/vitistack/common/pkg/unstructuredutil"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// machineClassGVR is the GroupVersionResource for vitistack.io/v1alpha1 MachineClass
var machineClassGVR = schema.GroupVersionResource{
	Group:    "vitistack.io",
	Version:  "v1alpha1",
	Resource: "machineclasses",
}

// FetchMachineClassByName fetches a cluster-scoped MachineClass resource by name
func FetchMachineClassByName(ctx context.Context, name string) (*vitistackv1alpha1.MachineClass, error) {
	// Use the dynamic client to fetch the cluster-scoped resource
	unstructured, err := k8sclient.DynamicClient.Resource(machineClassGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get MachineClass %q: %w", name, err)
	}

	// Convert unstructured to MachineClass using the generic converter
	machineClass := &vitistackv1alpha1.MachineClass{}
	if err := unstructuredutil.FromUnstructured(unstructured, machineClass); err != nil {
		return nil, fmt.Errorf("failed to convert MachineClass: %w", err)
	}

	return machineClass, nil
}

// ListMachineClasses lists all MachineClass resources in the cluster
func ListMachineClasses(ctx context.Context) ([]*vitistackv1alpha1.MachineClass, error) {
	// Use the dynamic client to list the cluster-scoped resources
	unstructuredList, err := k8sclient.DynamicClient.Resource(machineClassGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list MachineClasses: %w", err)
	}

	machineClasses := make([]*vitistackv1alpha1.MachineClass, 0, len(unstructuredList.Items))
	for i := range unstructuredList.Items {
		machineClass := &vitistackv1alpha1.MachineClass{}
		if err := unstructuredutil.FromUnstructured(&unstructuredList.Items[i], machineClass); err != nil {
			return nil, fmt.Errorf("failed to convert MachineClass: %w", err)
		}
		machineClasses = append(machineClasses, machineClass)
	}

	return machineClasses, nil
}

// ValidateMachineClass validates that a MachineClass exists and is enabled
func ValidateMachineClass(ctx context.Context, name string) error {
	machineClass, err := FetchMachineClassByName(ctx, name)
	if err != nil {
		return fmt.Errorf("machineClass %q not found: %w", name, err)
	}

	if !machineClass.Spec.Enabled {
		return fmt.Errorf("machineClass %q is not enabled", name)
	}

	return nil
}

// GetDefaultMachineClass returns the default MachineClass if one exists
func GetDefaultMachineClass(ctx context.Context) (*vitistackv1alpha1.MachineClass, error) {
	machineClasses, err := ListMachineClasses(ctx)
	if err != nil {
		return nil, err
	}

	for _, mc := range machineClasses {
		if mc.Spec.Default && mc.Spec.Enabled {
			return mc, nil
		}
	}

	return nil, fmt.Errorf("no default MachineClass found")
}
