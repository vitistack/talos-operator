package settings

// Package settings provides configuration management utilities for the Talos Operator.
// It leverages the Viper library for handling configuration files and environment variables,
// and utilizes constants defined in the vitistack/talos-operator package.
import (
	"github.com/spf13/viper"

	"github.com/vitistack/common/pkg/settings/dotenv"
	"github.com/vitistack/talos-operator/pkg/consts"
)

// Init initializes default configuration settings for the application using Viper.
// It sets default values for machine manifests path and persistence, and enables
// automatic environment variable binding.
func Init() {
	viper.SetDefault(consts.DEVELOPMENT, false)
	viper.SetDefault(consts.LOG_JSON, true)
	viper.SetDefault(consts.LOG_LEVEL, "info")
	viper.SetDefault(consts.SECRET_PREFIX, "")
	viper.SetDefault(consts.VITISTACK_NAME, "vitistack")
	viper.SetDefault(consts.NAME_KUBERNETES_PROVIDER, "talos-provider")
	viper.SetDefault(consts.TENANT_CONFIGMAP_NAME, "talos-tenant-config")
	viper.SetDefault(consts.TENANT_CONFIGMAP_NAMESPACE, "vitistack")
	viper.SetDefault(consts.TENANT_CONFIGMAP_DATA_KEY, "config.yaml")
	viper.SetDefault(consts.TALOS_VERSION, "v1.12.3")
	viper.SetDefault(consts.DEFAULT_KUBERNETES_VERSION, "1.35.0")
	viper.SetDefault(consts.TALOS_VM_INSTALL_IMAGE_KUBEVIRT, "factory.talos.dev/metal-installer/b0f2a8b575460a3dcb1234cc081c73c88e795aaef36eda9b88a6f4dddbd49365:v1.12.3")
	viper.SetDefault(consts.TALOS_VM_INSTALL_IMAGE_DEFAULT, "factory.talos.dev/metal-installer/b0f2a8b575460a3dcb1234cc081c73c88e795aaef36eda9b88a6f4dddbd49365:v1.12.3")
	viper.SetDefault(consts.TALOS_PREDICTABLE_NETWORK_NAMES, true)

	// Endpoint mode configuration
	// Valid values: "none", "networkconfiguration", "talosvip", "custom"
	// Default: "networkconfiguration" (uses ControlPlaneVirtualSharedIP from NetworkNamespace)
	viper.SetDefault(consts.ENDPOINT_MODE, string(consts.DefaultEndpointMode))
	viper.SetDefault(consts.CUSTOM_ENDPOINT, "")

	// Boot image source configuration
	// Valid values: "pxe", "bootimage"
	// Default: "pxe" (uses PXE boot for machine provisioning)
	// When set to "bootimage", BOOT_IMAGE must also be set with the URL to the Talos ISO
	viper.SetDefault(consts.BOOT_IMAGE_SOURCE, string(consts.DefaultBootImageSource))
	viper.SetDefault(consts.BOOT_IMAGE, "https://factory.talos.dev/image/b0f2a8b575460a3dcb1234cc081c73c88e795aaef36eda9b88a6f4dddbd49365/v1.12.3/metal-amd64.iso")

	dotenv.LoadDotEnv()
	viper.AutomaticEnv()
}
