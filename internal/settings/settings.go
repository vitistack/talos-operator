package settings

// Package settings provides configuration management utilities for the Talos Operator.
// It leverages the Viper library for handling configuration files and environment variables,
// and utilizes constants defined in the vitistack/talos-operator package.
import (
	"github.com/spf13/viper"

	"github.com/vitistack/talos-operator/pkg/consts"
)

// Init initializes default configuration settings for the application using Viper.
// It sets default values for machine manifests path and persistence, and enables
// automatic environment variable binding.
func Init() {
	viper.SetDefault(consts.MACHINE_MANIFESTS_PATH, "/")
	viper.SetDefault(consts.PERSIST_MACHINE_MANIFESTS, false)
	viper.SetDefault(consts.JSON_LOGGING, true)
	viper.SetDefault(consts.LOG_LEVEL, "info")
	viper.SetDefault(consts.SECRET_PREFIX, "")
	viper.SetDefault(consts.TENANT_CONFIGMAP_NAME, "talos-tenant-config")
	viper.SetDefault(consts.TENANT_CONFIGMAP_NAMESPACE, "default")
	viper.SetDefault(consts.TENANT_CONFIGMAP_DATA_KEY, "config.yaml")
	viper.SetDefault(consts.TALOS_VM_INSTALL_IMAGE_KUBEVIRT, "factory.talos.dev/metal-installer/ce4c980550dd2ab1b17bbf2b08801c7eb59418eafe8f279833297925d67c7515:v1.11.5")
	viper.SetDefault(consts.TALOS_VM_INSTALL_IMAGE_LIBVIRT, "factory.talos.dev/metal-installer/ce4c980550dd2ab1b17bbf2b08801c7eb59418eafe8f279833297925d67c7515:v1.11.5")
	viper.SetDefault(consts.TALOS_VM_INSTALL_IMAGE_KVM, "factory.talos.dev/metal-installer/ce4c980550dd2ab1b17bbf2b08801c7eb59418eafe8f279833297925d67c7515:v1.11.5")
	viper.SetDefault(consts.TALOS_VM_INSTALL_IMAGE_PROXMOX, "factory.talos.dev/metal-installer/ce4c980550dd2ab1b17bbf2b08801c7eb59418eafe8f279833297925d67c7515:v1.11.5")
	viper.SetDefault(consts.TALOS_VM_INSTALL_IMAGE_DEFAULT, "factory.talos.dev/metal-installer/ce4c980550dd2ab1b17bbf2b08801c7eb59418eafe8f279833297925d67c7515:v1.11.5")

	viper.AutomaticEnv()
}
