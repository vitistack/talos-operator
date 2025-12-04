package talos

import (
	"context"
	"fmt"
	"strconv"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/spf13/viper"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/pkg/consts"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// mergeBootstrappedFlag ensures we preserve the bootstrapped flag from an existing Secret,
// and defaults it to false if not present.
func mergeBootstrappedFlag(existing *corev1.Secret, data map[string][]byte) {
	if existing != nil && existing.Data != nil {
		if v, ok := existing.Data["bootstrapped"]; ok {
			data["bootstrapped"] = v
		}
	}
	if _, ok := data["bootstrapped"]; !ok {
		data["bootstrapped"] = []byte("false")
	}
}

// computePresence inspects both new data and existing secret to determine presence flags.
func computePresence(existing *corev1.Secret, data map[string][]byte) (hasTalos, hasCP, hasW, hasK bool) {
	// helper to check key in new data or existing secret
	present := func(key string) bool {
		if len(data[key]) > 0 {
			return true
		}
		if existing != nil && existing.Data != nil && len(existing.Data[key]) > 0 {
			return true
		}
		return false
	}
	hasTalos = present("talosconfig")
	hasCP = present("controlplane.yaml")
	hasW = present("worker.yaml")
	hasK = present("kube.config")
	return
}

// setPresenceFlags writes presence booleans (and cluster_access if kubeconfig present).
func setPresenceFlags(data map[string][]byte, hasTalos, hasCP, hasW, hasK bool) {
	data["talosconfig_present"] = []byte(strconv.FormatBool(hasTalos))
	data["controlplane_yaml_present"] = []byte(strconv.FormatBool(hasCP))
	data["worker_yaml_present"] = []byte(strconv.FormatBool(hasW))
	data["kubeconfig_present"] = []byte(strconv.FormatBool(hasK))
	if hasK {
		data["cluster_access"] = []byte("true")
	}
}

// applyDataToSecret merges the provided data into the secret (creating Data map if needed),
// skipping empty kube.config to avoid accidental deletion.
func applyDataToSecret(secret *corev1.Secret, data map[string][]byte) {
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	for k, v := range data {
		if k == "kube.config" && len(v) == 0 {
			continue
		}
		secret.Data[k] = v
	}
}

// upsertTalosClusterConfigSecretWithRoleYAML creates/updates the consolidated Secret with talosconfig, secrets bundle, and role templates.
func (t *TalosManager) upsertTalosClusterConfigSecretWithRoleYAML(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientCfg *clientconfig.Config,
	controlPlaneYAML []byte,
	workerYAML []byte,
	secretsBundleYAML []byte,
	kubeconfig []byte,
) error {
	secret, err := t.secretService.GetTalosSecret(ctx, cluster)

	data := map[string][]byte{}
	if b, mErr := t.configService.MarshalTalosClientConfig(clientCfg); mErr != nil {
		return mErr
	} else if len(b) > 0 {
		data["talosconfig"] = b
	}
	if len(controlPlaneYAML) > 0 {
		data["controlplane.yaml"] = controlPlaneYAML
	}
	if len(workerYAML) > 0 {
		data["worker.yaml"] = workerYAML
	}
	// Store the secrets bundle for future regeneration of configs
	if len(secretsBundleYAML) > 0 {
		data["secrets.bundle"] = secretsBundleYAML
	}
	if len(kubeconfig) > 0 {
		data["kube.config"] = kubeconfig
	}

	// preserve bootstrapped flag and default to false when missing
	var existing *corev1.Secret
	if err == nil {
		existing = secret
	}
	mergeBootstrappedFlag(existing, data)

	// presence flags
	hasTalos, hasCP, hasW, hasK := computePresence(existing, data)
	setPresenceFlags(data, hasTalos, hasCP, hasW, hasK)

	if err != nil {
		// create
		return t.secretService.CreateTalosSecret(ctx, cluster, data)
	}
	applyDataToSecret(secret, data)
	return t.secretService.UpdateTalosSecret(ctx, secret)
}

// upsertTalosClusterConfigSecret stores Talos client config, role configs, kubeconfig, and bootstrapped flag in a single Secret.
// Secret name: <cluster id>
func (t *TalosManager) upsertTalosClusterConfigSecret(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientCfg *clientconfig.Config,
	kubeconfig []byte,
) error {
	name := fmt.Sprintf("%s%s", viper.GetString(consts.SECRET_PREFIX), cluster.Spec.Cluster.ClusterId)
	secret := &corev1.Secret{}
	err := t.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret)

	// Prepare data payload
	data := map[string][]byte{}

	if b, mErr := t.configService.MarshalTalosClientConfig(clientCfg); mErr != nil {
		return mErr
	} else if len(b) > 0 {
		data["talosconfig"] = b
	}

	// preserve existing role templates
	if err == nil && secret.Data != nil {
		if v, ok := secret.Data["controlplane.yaml"]; ok {
			data["controlplane.yaml"] = v
		}
		if v, ok := secret.Data["worker.yaml"]; ok {
			data["worker.yaml"] = v
		}
	}

	// preserve bootstrapped flag and default to false
	var existing *corev1.Secret
	if err == nil {
		existing = secret
	}
	mergeBootstrappedFlag(existing, data)

	// kubeconfig (optional update)
	if len(kubeconfig) > 0 {
		data["kube.config"] = kubeconfig
	}

	// presence flags
	hasTalos, hasCP, hasW, hasK := computePresence(existing, data)
	setPresenceFlags(data, hasTalos, hasCP, hasW, hasK)

	if err != nil {
		// create
		return t.secretService.CreateTalosSecret(ctx, cluster, data)
	}

	// update existing: merge, preserving existing kube.config if not provided now
	applyDataToSecret(secret, data)
	return t.secretService.UpdateTalosSecret(ctx, secret)
}
