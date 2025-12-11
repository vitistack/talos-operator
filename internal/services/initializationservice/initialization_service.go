package initializationservice

import (
	"context"
	"os"

	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	"github.com/vitistack/common/pkg/operator/crdcheck"
	"github.com/vitistack/talos-operator/internal/services/kubernetesproviderservice"
	"github.com/vitistack/talos-operator/pkg/consts"
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
				"hint", "Set BOOT_IMAGE to the URL of the Talos ISO (e.g., https://github.com/siderolabs/talos/releases/download/v1.11.5/metal-amd64.iso)")
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
