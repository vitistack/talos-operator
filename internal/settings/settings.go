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
	viper.SetDefault(consts.SECRET_PREFIX, "k8s-")

	viper.AutomaticEnv()
}
