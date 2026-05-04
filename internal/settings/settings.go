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
	viper.SetDefault(consts.TENANT_CONFIGMAP_NAMESPACE, "default")
	viper.SetDefault(consts.TENANT_CONFIGMAP_DATA_KEY, "config.yaml")
	viper.SetDefault(consts.TALOS_VERSION, "v1.12.7")
	viper.SetDefault(consts.DEFAULT_KUBERNETES_VERSION, "1.35.4")
	viper.SetDefault(consts.TALOS_VM_INSTALL_IMAGE_KUBEVIRT, "factory.talos.dev/nocloud-installer/b0f2a8b575460a3dcb1234cc081c73c88e795aaef36eda9b88a6f4dddbd49365:v1.12.7")
	viper.SetDefault(consts.TALOS_VM_INSTALL_IMAGE_DEFAULT, "factory.talos.dev/nocloud-installer/b0f2a8b575460a3dcb1234cc081c73c88e795aaef36eda9b88a6f4dddbd49365:v1.12.7")
	// TALOS_REQUIRED_EXTENSIONS lists Talos system extensions every node must
	// have installed. Must stay in sync with the schematic baked into the
	// TALOS_VM_INSTALL_IMAGE_* default above (factory schematic
	// b0f2a8b5…d49365). Override to "" to disable the check.
	viper.SetDefault(consts.TALOS_REQUIRED_EXTENSIONS, "siderolabs/iscsi-tools,siderolabs/qemu-guest-agent,siderolabs/trident-iscsi-tools,siderolabs/util-linux-tools")
	// Cooldown between Talos upgrade triggers on the same node + image. 10
	// minutes covers the typical reboot + extension unpack window with margin.
	viper.SetDefault(consts.TALOS_EXTENSION_COOLDOWN_MINUTES, 5)
	viper.SetDefault(consts.TALOS_PREDICTABLE_NETWORK_NAMES, true)
	// Talos version enforcement reconciler: probes node Talos version on
	// every reconcile and triggers `talosctl upgrade` for nodes that
	// disagree with TALOS_VERSION. Never downgrades. Enabled by default;
	// set to false to disable and revert to annotation-driven upgrades
	// only.
	viper.SetDefault(consts.TALOS_VERSION_ENFORCE_ENABLED, true)
	// Cooldown between version-enforcement Talos upgrade triggers on the same
	// node + target version + image. Covers Talos reboot + cordon/drain +
	// API recovery so the 5s reconcile loop doesn't hammer a node that's
	// already mid-upgrade. Drain is the long pole; 5 minutes is comfortable.
	viper.SetDefault(consts.TALOS_VERSION_ENFORCE_COOLDOWN_MINUTES, 5)

	// Endpoint mode configuration
	// Valid values: "none", "networkconfiguration", "talosvip", "custom"
	// Default: "networkconfiguration" (uses ControlPlaneVirtualSharedIP from NetworkNamespace)
	viper.SetDefault(consts.ENDPOINT_MODE, string(consts.DefaultEndpointMode))
	viper.SetDefault(consts.CUSTOM_ENDPOINT, "")

	// Boot image source configuration
	// Valid values: "pxe", "bootimage"
	// Default: "bootimage" (uses the BOOT_IMAGE URL below as the Talos ISO).
	// Set to "pxe" to provision via netboot instead.
	viper.SetDefault(consts.BOOT_IMAGE_SOURCE, string(consts.DefaultBootImageSource))
	viper.SetDefault(consts.BOOT_IMAGE, "https://factory.talos.dev/image/b0f2a8b575460a3dcb1234cc081c73c88e795aaef36eda9b88a6f4dddbd49365/v1.12.7/nocloud-amd64.iso")

	dotenv.LoadDotEnv()
	viper.AutomaticEnv()
}
