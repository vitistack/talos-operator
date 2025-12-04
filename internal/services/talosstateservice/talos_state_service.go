package talosstateservice

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/secretservice"
	yaml "gopkg.in/yaml.v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
)

const (
	trueStr  = "true"
	falseStr = "false"
)

// TalosSecretFlags captures persisted boolean flags stored in the consolidated Secret
type TalosSecretFlags struct {
	ControlPlaneApplied      bool
	WorkerApplied            bool
	Bootstrapped             bool
	ClusterAccess            bool
	TalosAPIReady            bool // tracks when Talos API accepts secure (non-insecure) connections
	KubernetesAPIReady       bool // tracks when Kubernetes API server is reachable
	FirstControlPlaneApplied bool // tracks if the first control plane has config applied
	FirstControlPlaneReady   bool // tracks if the first control plane is ready (API reachable)
}

// TalosStateService manages Talos cluster state in Kubernetes secrets
type TalosStateService struct {
	secretService *secretservice.SecretService
}

// NewTalosStateService creates a new TalosStateService
func NewTalosStateService(secretSvc *secretservice.SecretService) *TalosStateService {
	return &TalosStateService{
		secretService: secretSvc,
	}
}

// EnsureSecretExists creates the consolidated Secret if it does not exist yet with default flags
func (s *TalosStateService) EnsureSecretExists(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	_, err := s.secretService.GetTalosSecret(ctx, cluster)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	// create minimal secret with default flags and lifecycle timestamps
	now := time.Now().UTC().Format(time.RFC3339)
	data := map[string][]byte{
		"bootstrapped":               []byte(falseStr),
		"talosconfig_present":        []byte(falseStr),
		"controlplane_yaml_present":  []byte(falseStr),
		"worker_yaml_present":        []byte(falseStr),
		"kubeconfig_present":         []byte(falseStr),
		"talos_api_ready":            []byte(falseStr),
		"controlplane_applied":       []byte(falseStr),
		"worker_applied":             []byte(falseStr),
		"cluster_access":             []byte(falseStr),
		"first_controlplane_applied": []byte(falseStr),
		"first_controlplane_ready":   []byte(falseStr),
		"configured_nodes":           []byte(""), // comma-separated list of node names that have been configured
		"created_at":                 []byte(now),
	}
	err = s.secretService.CreateTalosSecret(ctx, cluster, data)
	if err == nil {
		vlog.Info("Secret created with initial status flags, cluster=" + cluster.Name)
	}
	return err
}

// GetFlags reads boolean flags from the consolidated Secret
func (s *TalosStateService) GetFlags(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (TalosSecretFlags, error) {
	secret, err := s.secretService.GetTalosSecret(ctx, cluster)
	if err != nil {
		return TalosSecretFlags{}, err
	}
	return parseSecretFlags(secret.Data), nil
}

// parseSecretFlags extracts boolean flags from secret data
func parseSecretFlags(data map[string][]byte) TalosSecretFlags {
	flags := TalosSecretFlags{}
	if data == nil {
		return flags
	}

	flags.ControlPlaneApplied = isFlagTrue(data, "controlplane_applied")
	flags.WorkerApplied = isFlagTrue(data, "worker_applied")
	flags.Bootstrapped = isFlagTrue(data, "bootstrapped")
	flags.ClusterAccess = parseClusterAccessFlag(data)
	flags.TalosAPIReady = isFlagTrue(data, "talos_api_ready")
	flags.KubernetesAPIReady = isFlagTrue(data, "kubernetes_api_ready")
	flags.FirstControlPlaneApplied = isFlagTrue(data, "first_controlplane_applied")
	flags.FirstControlPlaneReady = isFlagTrue(data, "first_controlplane_ready")

	return flags
}

// isFlagTrue checks if a flag in secret data is set to "true"
func isFlagTrue(data map[string][]byte, key string) bool {
	if b, ok := data[key]; ok && string(b) == trueStr {
		return true
	}
	return false
}

// parseClusterAccessFlag determines cluster access from multiple possible flags
func parseClusterAccessFlag(data map[string][]byte) bool {
	if isFlagTrue(data, "cluster_access") {
		return true
	}
	if k, ok := data["kube.config"]; ok && len(k) > 0 {
		return true
	}
	if isFlagTrue(data, "kubeconfig_present") {
		return true
	}
	return false
}

// SetFlags sets provided boolean flags in the consolidated Secret
func (s *TalosStateService) SetFlags(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, updates map[string]bool) error {
	// Retry logic to handle concurrent modifications
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := s.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		for k, v := range updates {
			if v {
				secret.Data[k] = []byte(trueStr)
			} else {
				secret.Data[k] = []byte(falseStr)
			}
		}

		err = s.secretService.UpdateTalosSecret(ctx, secret)
		if err == nil {
			return nil
		}

		// If conflict error, retry with fresh data
		if apierrors.IsConflict(err) {
			if attempt < maxRetries-1 {
				vlog.Warn(fmt.Sprintf("Secret conflict on attempt %d, retrying: %v", attempt+1, err))
				time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1)) // exponential backoff
				continue
			}
		}
		return err
	}
	return fmt.Errorf("failed to update secret flags after %d retries", maxRetries)
}

// SetTimestamp sets a timestamp field in the consolidated Secret
func (s *TalosStateService) SetTimestamp(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, key string) error {
	// Retry logic to handle concurrent modifications
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := s.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		secret.Data[key] = []byte(time.Now().UTC().Format(time.RFC3339))

		err = s.secretService.UpdateTalosSecret(ctx, secret)
		if err == nil {
			return nil
		}

		// If conflict error, retry with fresh data
		if apierrors.IsConflict(err) {
			if attempt < maxRetries-1 {
				time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1))
				continue
			}
		}
		return err
	}
	return fmt.Errorf("failed to update secret timestamp after %d retries", maxRetries)
}

// GetState returns persisted state flags from the cluster's consolidated Secret.
func (s *TalosStateService) GetState(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (bootstrapped bool, hasKubeconfig bool, err error) {
	secret, e := s.secretService.GetTalosSecret(ctx, cluster)
	if e != nil {
		return false, false, e
	}
	if secret.Data == nil {
		return false, false, nil
	}
	if b, ok := secret.Data["bootstrapped"]; ok && string(b) == trueStr {
		bootstrapped = true
	}
	if k, ok := secret.Data["kube.config"]; ok && len(k) > 0 {
		hasKubeconfig = true
	}
	return bootstrapped, hasKubeconfig, nil
}

// LoadTalosArtifacts attempts to read talosconfig (and ensures role templates exist) from the consolidated Secret.
// Returns fromSecret=true when talosconfig and both role templates are present.
func (s *TalosStateService) LoadTalosArtifacts(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*clientconfig.Config, bool, error) {
	secret, err := s.secretService.GetTalosSecret(ctx, cluster)
	if err != nil {
		return nil, false, nil
	}
	if secret.Data == nil {
		return nil, false, nil
	}
	var cfg *clientconfig.Config
	if b, ok := secret.Data["talosconfig"]; ok && len(b) > 0 {
		// talosclient config is YAML; unmarshal back
		c := &clientconfig.Config{}
		if err := yaml.Unmarshal(b, c); err == nil {
			cfg = c
		} else {
			return nil, false, fmt.Errorf("failed to unmarshal talosconfig from secret: %w", err)
		}
	}

	cp := secret.Data["controlplane.yaml"]
	w := secret.Data["worker.yaml"]
	if cfg != nil && len(cp) > 0 && len(w) > 0 {
		return cfg, true, nil
	}
	return nil, false, nil
}

// GetConfiguredNodes returns the list of node names that have been configured
func (s *TalosStateService) GetConfiguredNodes(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) ([]string, error) {
	secret, err := s.secretService.GetTalosSecret(ctx, cluster)
	if err != nil {
		return nil, err
	}
	if secret.Data == nil {
		return nil, nil
	}
	nodesStr := string(secret.Data["configured_nodes"])
	if nodesStr == "" {
		return nil, nil
	}
	return strings.Split(nodesStr, ","), nil
}

// IsNodeConfigured checks if a node has already been configured
func (s *TalosStateService) IsNodeConfigured(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) bool {
	nodes, err := s.GetConfiguredNodes(ctx, cluster)
	if err != nil {
		return false
	}
	for _, n := range nodes {
		if n == nodeName {
			return true
		}
	}
	return false
}

// AddConfiguredNode adds a node name to the list of configured nodes in the secret
func (s *TalosStateService) AddConfiguredNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := s.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}

		existingNodes := string(secret.Data["configured_nodes"])
		var nodes []string
		if existingNodes != "" {
			nodes = strings.Split(existingNodes, ",")
		}

		// Check if already exists
		for _, n := range nodes {
			if n == nodeName {
				return nil // Already configured
			}
		}

		nodes = append(nodes, nodeName)
		secret.Data["configured_nodes"] = []byte(strings.Join(nodes, ","))

		err = s.secretService.UpdateTalosSecret(ctx, secret)
		if err == nil {
			return nil
		}

		if apierrors.IsConflict(err) && attempt < maxRetries-1 {
			time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1))
			continue
		}
		return err
	}
	return fmt.Errorf("failed to add configured node after %d retries", maxRetries)
}

// UpsertConfigWithRoleYAML stores config artifacts (talosconfig, role templates) in the secret
func (s *TalosStateService) UpsertConfigWithRoleYAML(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster,
	talosconfigYAML, controlPlaneYAML, workerYAML []byte) error {
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := s.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}

		secret.Data["talosconfig"] = talosconfigYAML
		secret.Data["controlplane.yaml"] = controlPlaneYAML
		secret.Data["worker.yaml"] = workerYAML
		secret.Data["talosconfig_present"] = []byte(trueStr)
		secret.Data["controlplane_yaml_present"] = []byte(trueStr)
		secret.Data["worker_yaml_present"] = []byte(trueStr)

		err = s.secretService.UpdateTalosSecret(ctx, secret)
		if err == nil {
			return nil
		}

		if apierrors.IsConflict(err) && attempt < maxRetries-1 {
			time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1))
			continue
		}
		return err
	}
	return fmt.Errorf("failed to upsert config after %d retries", maxRetries)
}
