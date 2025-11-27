package kubernetesproviderservice

import (
	"context"
	"fmt"

	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/clients/k8sclient"
	"github.com/vitistack/common/pkg/loggers/vlog"
	"github.com/vitistack/common/pkg/unstructuredutil"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/vitistackservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// OperatorManagedByLabel is the label used to identify resources managed by this operator
	OperatorManagedByLabel = "vitistack.io/managed-by"
	// OperatorName is the name of this operator
	OperatorName = "talos-operator"
	// DefaultRegion is used when no Vitistack is found
	DefaultRegion = "default"
	// TalosProviderType is the provider type for Talos-based Kubernetes clusters
	TalosProviderType = "talos"
)

// kubernetesProviderGVR is the GroupVersionResource for vitistack.io/v1alpha1 KubernetesProvider
var kubernetesProviderGVR = schema.GroupVersionResource{
	Group:    "vitistack.io",
	Version:  "v1alpha1",
	Resource: "kubernetesproviders",
}

// EnsureKubernetesProviderExists checks if a KubernetesProvider with providerType "talos" exists
// and creates one if not. This allows multiple KubernetesProviders to coexist in the cluster.
// It uses the managed-by label to identify the KubernetesProvider owned by this operator.
// This function is idempotent and safe to call multiple times.
func EnsureKubernetesProviderExists(ctx context.Context) error {
	vlog.Info("Checking for existing Talos KubernetesProviders...")

	kubernetesProviderName := viper.GetString(consts.NAME_KUBERNETES_PROVIDER)

	// Check if any KubernetesProvider with providerType "talos" already exists
	talosProviders, err := ListKubernetesProvidersByType(ctx, TalosProviderType)
	if err != nil {
		return fmt.Errorf("failed to list Talos KubernetesProviders: %w", err)
	}

	if len(talosProviders) > 0 {
		vlog.Info("Talos KubernetesProvider(s) already exist, skipping creation",
			"count", len(talosProviders),
			"names", getProviderNames(talosProviders))
		return nil
	}

	// Check if a KubernetesProvider managed by this operator already exists (with our label)
	existingManagedProvider, err := findManagedKubernetesProvider(ctx)
	if err != nil {
		return fmt.Errorf("failed to check for existing managed KubernetesProvider: %w", err)
	}

	if existingManagedProvider != nil {
		vlog.Info("KubernetesProvider managed by this operator already exists",
			"name", existingManagedProvider.Name,
			"phase", existingManagedProvider.Status.Phase)
		return nil
	}

	// Check if a KubernetesProvider with the default name already exists (but not managed by us)
	exists, err := kubernetesProviderExistsByName(ctx, kubernetesProviderName)
	if err != nil {
		return fmt.Errorf("failed to check for KubernetesProvider by name: %w", err)
	}

	if exists {
		vlog.Warn("KubernetesProvider with default name already exists but is not managed by this operator",
			"name", kubernetesProviderName)
		return nil
	}

	// Create the default KubernetesProvider
	vlog.Info("Creating default KubernetesProvider for talos-operator...")
	if err := createDefaultKubernetesProvider(ctx); err != nil {
		return fmt.Errorf("failed to create default KubernetesProvider: %w", err)
	}

	vlog.Info("✅ Successfully created default KubernetesProvider", "name", kubernetesProviderName)
	return nil
}

// getProviderNames extracts names from a list of KubernetesProviders
func getProviderNames(providers []*vitistackv1alpha1.KubernetesProvider) []string {
	names := make([]string, len(providers))
	for i, p := range providers {
		names[i] = p.Name
	}
	return names
}

// ListAllKubernetesProviders lists all KubernetesProviders in the cluster
func ListAllKubernetesProviders(ctx context.Context) ([]*vitistackv1alpha1.KubernetesProvider, error) {
	unstructuredList, err := k8sclient.DynamicClient.Resource(kubernetesProviderGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list KubernetesProviders: %w", err)
	}

	providers := make([]*vitistackv1alpha1.KubernetesProvider, 0, len(unstructuredList.Items))
	for i := range unstructuredList.Items {
		provider, err := unstructuredutil.KubernetesProviderFromUnstructured(&unstructuredList.Items[i])
		if err != nil {
			vlog.Warn("Failed to convert KubernetesProvider, skipping",
				"name", unstructuredList.Items[i].GetName(),
				"error", err)
			continue
		}
		providers = append(providers, provider)
	}

	return providers, nil
}

// ListKubernetesProvidersByType lists all KubernetesProviders with the specified providerType
func ListKubernetesProvidersByType(ctx context.Context, providerType string) ([]*vitistackv1alpha1.KubernetesProvider, error) {
	allProviders, err := ListAllKubernetesProviders(ctx)
	if err != nil {
		return nil, err
	}

	filtered := make([]*vitistackv1alpha1.KubernetesProvider, 0)
	for _, provider := range allProviders {
		if provider.Spec.ProviderType == providerType {
			filtered = append(filtered, provider)
		}
	}

	return filtered, nil
}

// findManagedKubernetesProvider finds a KubernetesProvider that is managed by this operator
func findManagedKubernetesProvider(ctx context.Context) (*vitistackv1alpha1.KubernetesProvider, error) {
	// List all KubernetesProviders with the managed-by label
	labelSelector := fmt.Sprintf("%s=%s", OperatorManagedByLabel, OperatorName)
	unstructuredList, err := k8sclient.DynamicClient.Resource(kubernetesProviderGVR).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list KubernetesProviders: %w", err)
	}

	if len(unstructuredList.Items) == 0 {
		return nil, nil
	}

	// Return the first one found (there should typically only be one managed by this operator)
	provider, err := unstructuredutil.KubernetesProviderFromUnstructured(&unstructuredList.Items[0])
	if err != nil {
		return nil, fmt.Errorf("failed to convert KubernetesProvider: %w", err)
	}

	return provider, nil
}

// kubernetesProviderExistsByName checks if a KubernetesProvider exists with the given name
func kubernetesProviderExistsByName(ctx context.Context, name string) (bool, error) {
	_, err := k8sclient.DynamicClient.Resource(kubernetesProviderGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// getRegionFromVitistack attempts to fetch the region from the Vitistack object
// Falls back to DefaultRegion if Vitistack is not found or doesn't have a region
func getRegionFromVitistack(ctx context.Context) string {
	vitistackName := viper.GetString(consts.VITISTACK_NAME)
	if vitistackName == "" {
		vlog.Info("VITISTACK_NAME not configured, using default region", "region", DefaultRegion)
		return DefaultRegion
	}

	vitistack, err := vitistackservice.FetchVitistackByName(ctx, vitistackName)
	if err != nil {
		vlog.Warn("Failed to fetch Vitistack, using default region",
			"vitistackName", vitistackName,
			"error", err,
			"region", DefaultRegion)
		return DefaultRegion
	}

	if vitistack.Spec.Region != "" {
		vlog.Info("Using region from Vitistack", "region", vitistack.Spec.Region)
		return vitistack.Spec.Region
	}

	vlog.Info("Vitistack has no region configured, using default region", "region", DefaultRegion)
	return DefaultRegion
}

// createDefaultKubernetesProvider creates the default KubernetesProvider for this operator
func createDefaultKubernetesProvider(ctx context.Context) error {
	// Get region from Vitistack object if available
	region := getRegionFromVitistack(ctx)

	kubernetesProviderName := viper.GetString(consts.NAME_KUBERNETES_PROVIDER)

	provider := &vitistackv1alpha1.KubernetesProvider{
		ObjectMeta: metav1.ObjectMeta{
			Name: kubernetesProviderName,
			Labels: map[string]string{
				OperatorManagedByLabel: OperatorName,
			},
			Annotations: map[string]string{
				vitistackv1alpha1.ManagedByAnnotation: OperatorName,
			},
		},
		Spec: vitistackv1alpha1.KubernetesProviderSpec{
			ProviderType: TalosProviderType,
			DisplayName:  "Talos Operator Provider",
			Version:      "v1.34.1", // Default Kubernetes version for Talos 1.11.5 clusters
			Region:       region,
		},
	}

	// Convert to unstructured for dynamic client
	unstructuredProvider, err := unstructuredutil.KubernetesProviderToUnstructured(provider)
	if err != nil {
		return fmt.Errorf("failed to convert KubernetesProvider to unstructured: %w", err)
	}

	// Create the resource
	_, err = k8sclient.DynamicClient.Resource(kubernetesProviderGVR).Create(ctx, unstructuredProvider, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			vlog.Info("KubernetesProvider already exists (race condition), skipping creation", "name", kubernetesProviderName)
			return nil
		}
		return fmt.Errorf("failed to create KubernetesProvider: %w", err)
	}

	return nil
}

// GetKubernetesProviderByName fetches a KubernetesProvider by name
func GetKubernetesProviderByName(ctx context.Context, name string) (*vitistackv1alpha1.KubernetesProvider, error) {
	unstructured, err := k8sclient.DynamicClient.Resource(kubernetesProviderGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get KubernetesProvider %q: %w", name, err)
	}

	provider, err := unstructuredutil.KubernetesProviderFromUnstructured(unstructured)
	if err != nil {
		return nil, fmt.Errorf("failed to convert KubernetesProvider: %w", err)
	}

	return provider, nil
}

// GetManagedKubernetesProvider returns the KubernetesProvider managed by this operator
func GetManagedKubernetesProvider(ctx context.Context) (*vitistackv1alpha1.KubernetesProvider, error) {
	return findManagedKubernetesProvider(ctx)
}
