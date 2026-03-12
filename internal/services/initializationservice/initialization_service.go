package initializationservice

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/clients/k8sclient"
	"github.com/vitistack/common/pkg/loggers/vlog"
	"github.com/vitistack/common/pkg/operator/crdcheck"
	"github.com/vitistack/talos-operator/internal/services/kubernetesproviderservice"
	"github.com/vitistack/talos-operator/internal/services/talosconfigservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CheckPrerequisites verifies that required CRDs are installed before starting.
// It exits the process with code 1 if mandatory APIs are missing.
func CheckPrerequisites() {
	vlog.Info("Running prerequisite checks...")

	crdcheck.MustEnsureInstalled(context.TODO(),
		// your CRD plural
		crdcheck.Ref{Group: "vitistack.io", Version: "v1alpha1", Resource: "machines"},
		crdcheck.Ref{Group: "vitistack.io", Version: "v1alpha1", Resource: "kubernetesclusters"},
		crdcheck.Ref{Group: "vitistack.io", Version: "v1alpha1", Resource: "kubernetesproviders"},
		crdcheck.Ref{Group: "vitistack.io", Version: "v1alpha1", Resource: "machineproviders"},
		crdcheck.Ref{Group: "vitistack.io", Version: "v1alpha1", Resource: "networknamespaces"},
		crdcheck.Ref{Group: "vitistack.io", Version: "v1alpha1", Resource: "networkconfigurations"},
		crdcheck.Ref{Group: "vitistack.io", Version: "v1alpha1", Resource: "vitistacks"},
	)

	ValidateBootImageConfiguration()
	ValidateTenantConfig()

	vlog.Info("✅ Prerequisite checks passed")
}

// EnsureKubernetesProvider ensures that a KubernetesProvider resource exists for this operator.
// This should be called after CheckPrerequisites to ensure the CRD is available.
// The function is idempotent and will not create duplicate resources.
func EnsureKubernetesProvider() {
	vlog.Info("Ensuring KubernetesProvider exists...")

	ctx := context.TODO()
	if err := kubernetesproviderservice.EnsureKubernetesProviderExists(ctx); err != nil {
		vlog.Error("Failed to ensure KubernetesProvider exists", err)
		os.Exit(1)
	}

	vlog.Info("✅ KubernetesProvider check completed")
}

// ValidateBootImageConfiguration validates that boot image settings are correctly configured.
// If BOOT_IMAGE_SOURCE is set to "bootimage", BOOT_IMAGE must also be set.
// It exits the process with code 1 if the configuration is invalid.
func ValidateBootImageConfiguration() {
	vlog.Info("Validating boot image configuration...")

	bootImageSource := viper.GetString(consts.BOOT_IMAGE_SOURCE)

	// Validate that the boot image source is valid
	if !consts.IsValidBootImageSource(bootImageSource) {
		vlog.Error("Invalid BOOT_IMAGE_SOURCE value", nil,
			"value", bootImageSource,
			"valid_values", consts.ValidBootImageSources())
		os.Exit(1)
	}

	// If boot image source is "bootimage", BOOT_IMAGE must be set
	if consts.BootImageSource(bootImageSource) == consts.BootImageSourceBootImage {
		bootImage := viper.GetString(consts.BOOT_IMAGE)
		if bootImage == "" {
			vlog.Error("BOOT_IMAGE must be set when BOOT_IMAGE_SOURCE is 'bootimage'", nil,
				"boot_image_source", bootImageSource,
				"hint", "Set BOOT_IMAGE to the URL of the Talos ISO (e.g., https://github.com/siderolabs/talos/releases/download/v1.11.6/metal-amd64.iso)")
			os.Exit(1)
		}
		vlog.Info("Boot image configuration validated",
			"boot_image_source", bootImageSource,
			"boot_image", bootImage)
	} else {
		vlog.Info("Boot image configuration validated",
			"boot_image_source", bootImageSource)
	}

	vlog.Info("✅ Boot image configuration check completed")
}

// ValidateTenantConfig validates that the tenant config ConfigMap (if configured)
// contains valid multi-document YAML that can be parsed by configpatcher.
// This catches issues like incorrect indentation of --- document separators early at startup.
// Logs an error but does not exit, since the ConfigMap is optional.
func ValidateTenantConfig() {
	vlog.Info("Validating tenant config...")

	name := strings.TrimSpace(viper.GetString(consts.TENANT_CONFIGMAP_NAME))
	if name == "" {
		vlog.Info("No tenant config ConfigMap configured, skipping validation")
		return
	}

	namespace := strings.TrimSpace(viper.GetString(consts.TENANT_CONFIGMAP_NAMESPACE))
	if namespace == "" {
		namespace = "default"
	}

	dataKey := strings.TrimSpace(viper.GetString(consts.TENANT_CONFIGMAP_DATA_KEY))
	if dataKey == "" {
		dataKey = "config.yaml"
	}

	ctx := context.TODO()
	cm, err := k8sclient.Kubernetes.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		vlog.Warn(fmt.Sprintf("Could not read tenant config ConfigMap %s/%s for validation: %v", namespace, name, err))
		return
	}

	raw, ok := cm.Data[dataKey]
	if !ok || strings.TrimSpace(raw) == "" {
		vlog.Warn(fmt.Sprintf("Tenant config ConfigMap %s/%s missing data key %s", namespace, name, dataKey))
		return
	}

	if err := talosconfigservice.ValidateTenantConfigYAML(raw); err != nil {
		vlog.Error(fmt.Sprintf("Tenant config ConfigMap %s/%s key %s contains invalid YAML. "+
			"Check that --- document separators and all documents are at the same indentation level.", namespace, name, dataKey), err)
		os.Exit(1)
	}

	vlog.Info("✅ Tenant config validation passed")
}
