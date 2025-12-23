package talosstateservice

import (
	"context"
	"fmt"
	"strconv"
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
		// Version tracking - persisted for recovery and verification
		"talos_version":      []byte(""),
		"kubernetes_version": []byte(""),
		// Upgrade state tracking - for resume/recovery of interrupted upgrades
		"upgrade_in_progress":    []byte(falseStr),
		"upgrade_type":           []byte(""),  // "talos" or "kubernetes"
		"upgrade_target":         []byte(""),  // target version
		"upgrade_started_at":     []byte(""),  // when upgrade started
		"upgrade_last_node":      []byte(""),  // last node being upgraded (for resume)
		"upgrade_nodes_done":     []byte("0"), // count of completed nodes
		"upgrade_nodes_total":    []byte("0"), // total nodes to upgrade
		"upgrade_upgraded_nodes": []byte(""),  // comma-separated list of successfully upgraded node names
		"upgrade_failed_nodes":   []byte(""),  // comma-separated list of failed node names with error
		"upgrade_failed_reason":  []byte(""),  // reason for last failure
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

// ClusterVersions holds the persisted version information from the secret
type ClusterVersions struct {
	TalosVersion      string
	KubernetesVersion string
}

// UpgradeStateInfo holds the persisted upgrade state for recovery
type UpgradeStateInfo struct {
	InProgress    bool
	UpgradeType   string   // "talos" or "kubernetes"
	Target        string   // target version
	StartedAt     string   // RFC3339 timestamp
	LastNode      string   // last node being upgraded
	NodesDone     int      // count of completed nodes
	NodesTotal    int      // total nodes to upgrade
	UpgradedNodes []string // list of successfully upgraded node names
	FailedNodes   []string // list of failed node names
	FailedReason  string   // reason for last failure
}

// GetClusterVersions retrieves the persisted Talos and Kubernetes versions from the secret
func (s *TalosStateService) GetClusterVersions(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*ClusterVersions, error) {
	secret, err := s.secretService.GetTalosSecret(ctx, cluster)
	if err != nil {
		return nil, err
	}
	if secret.Data == nil {
		return &ClusterVersions{}, nil
	}
	return &ClusterVersions{
		TalosVersion:      string(secret.Data["talos_version"]),
		KubernetesVersion: string(secret.Data["kubernetes_version"]),
	}, nil
}

// SetClusterVersions persists the Talos and Kubernetes versions in the secret
func (s *TalosStateService) SetClusterVersions(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, talosVersion, kubernetesVersion string) error {
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := s.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}

		if talosVersion != "" {
			secret.Data["talos_version"] = []byte(talosVersion)
		}
		if kubernetesVersion != "" {
			secret.Data["kubernetes_version"] = []byte(kubernetesVersion)
		}

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
	return fmt.Errorf("failed to set cluster versions after %d retries", maxRetries)
}

// GetUpgradeState retrieves the persisted upgrade state from the secret
func (s *TalosStateService) GetUpgradeState(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*UpgradeStateInfo, error) {
	secret, err := s.secretService.GetTalosSecret(ctx, cluster)
	if err != nil {
		return nil, err
	}
	if secret.Data == nil {
		return &UpgradeStateInfo{}, nil
	}

	nodesDone := 0
	nodesTotal := 0
	if v := string(secret.Data["upgrade_nodes_done"]); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			nodesDone = parsed
		}
	}
	if v := string(secret.Data["upgrade_nodes_total"]); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			nodesTotal = parsed
		}
	}

	// Parse upgraded and failed nodes lists
	var upgradedNodes, failedNodes []string
	if v := string(secret.Data["upgrade_upgraded_nodes"]); v != "" {
		upgradedNodes = strings.Split(v, ",")
	}
	if v := string(secret.Data["upgrade_failed_nodes"]); v != "" {
		failedNodes = strings.Split(v, ",")
	}

	return &UpgradeStateInfo{
		InProgress:    isFlagTrue(secret.Data, "upgrade_in_progress"),
		UpgradeType:   string(secret.Data["upgrade_type"]),
		Target:        string(secret.Data["upgrade_target"]),
		StartedAt:     string(secret.Data["upgrade_started_at"]),
		LastNode:      string(secret.Data["upgrade_last_node"]),
		NodesDone:     nodesDone,
		NodesTotal:    nodesTotal,
		UpgradedNodes: upgradedNodes,
		FailedNodes:   failedNodes,
		FailedReason:  string(secret.Data["upgrade_failed_reason"]),
	}, nil
}

// StartUpgradeState marks an upgrade as in progress in the secret for recovery purposes
func (s *TalosStateService) StartUpgradeState(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, upgradeType, targetVersion string, totalNodes int) error {
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := s.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}

		now := time.Now().UTC().Format(time.RFC3339)
		secret.Data["upgrade_in_progress"] = []byte(trueStr)
		secret.Data["upgrade_type"] = []byte(upgradeType)
		secret.Data["upgrade_target"] = []byte(targetVersion)
		secret.Data["upgrade_started_at"] = []byte(now)
		secret.Data["upgrade_last_node"] = []byte("")
		secret.Data["upgrade_nodes_done"] = []byte("0")
		secret.Data["upgrade_nodes_total"] = []byte(fmt.Sprintf("%d", totalNodes))
		// Clear node tracking lists for fresh upgrade
		secret.Data["upgrade_upgraded_nodes"] = []byte("")
		secret.Data["upgrade_failed_nodes"] = []byte("")
		secret.Data["upgrade_failed_reason"] = []byte("")

		err = s.secretService.UpdateTalosSecret(ctx, secret)
		if err == nil {
			vlog.Info(fmt.Sprintf("Upgrade state persisted: cluster=%s type=%s target=%s nodes=%d",
				cluster.Name, upgradeType, targetVersion, totalNodes))
			return nil
		}

		if apierrors.IsConflict(err) && attempt < maxRetries-1 {
			time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1))
			continue
		}
		return err
	}
	return fmt.Errorf("failed to start upgrade state after %d retries", maxRetries)
}

// UpdateUpgradeProgress updates the upgrade progress in the secret
func (s *TalosStateService) UpdateUpgradeProgress(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string, nodesDone int) error {
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := s.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}

		secret.Data["upgrade_last_node"] = []byte(nodeName)
		secret.Data["upgrade_nodes_done"] = []byte(fmt.Sprintf("%d", nodesDone))

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
	return fmt.Errorf("failed to update upgrade progress after %d retries", maxRetries)
}

// MarkNodeUpgraded adds a node to the list of successfully upgraded nodes
func (s *TalosStateService) MarkNodeUpgraded(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := s.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}

		// Parse existing upgraded nodes and add new one
		existingNodes := string(secret.Data["upgrade_upgraded_nodes"])
		var nodes []string
		if existingNodes != "" {
			nodes = strings.Split(existingNodes, ",")
		}

		// Check if already in list
		for _, n := range nodes {
			if n == nodeName {
				return nil // Already marked
			}
		}

		nodes = append(nodes, nodeName)
		secret.Data["upgrade_upgraded_nodes"] = []byte(strings.Join(nodes, ","))

		// Also update nodes done count
		nodesDone := len(nodes)
		secret.Data["upgrade_nodes_done"] = []byte(fmt.Sprintf("%d", nodesDone))

		err = s.secretService.UpdateTalosSecret(ctx, secret)
		if err == nil {
			vlog.Info(fmt.Sprintf("Node marked as upgraded: cluster=%s node=%s (%d done)", cluster.Name, nodeName, nodesDone))
			return nil
		}

		if apierrors.IsConflict(err) && attempt < maxRetries-1 {
			time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1))
			continue
		}
		return err
	}
	return fmt.Errorf("failed to mark node upgraded after %d retries", maxRetries)
}

// MarkNodeFailed adds a node to the list of failed nodes with a reason
func (s *TalosStateService) MarkNodeFailed(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName, reason string) error {
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := s.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}

		// Parse existing failed nodes and add new one
		existingNodes := string(secret.Data["upgrade_failed_nodes"])
		var nodes []string
		if existingNodes != "" {
			nodes = strings.Split(existingNodes, ",")
		}

		// Check if already in list
		for _, n := range nodes {
			if n == nodeName {
				// Update reason only
				secret.Data["upgrade_failed_reason"] = []byte(reason)
				return s.secretService.UpdateTalosSecret(ctx, secret)
			}
		}

		nodes = append(nodes, nodeName)
		secret.Data["upgrade_failed_nodes"] = []byte(strings.Join(nodes, ","))
		secret.Data["upgrade_failed_reason"] = []byte(reason)

		err = s.secretService.UpdateTalosSecret(ctx, secret)
		if err == nil {
			vlog.Warn(fmt.Sprintf("Node marked as failed: cluster=%s node=%s reason=%s", cluster.Name, nodeName, reason))
			return nil
		}

		if apierrors.IsConflict(err) && attempt < maxRetries-1 {
			time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1))
			continue
		}
		return err
	}
	return fmt.Errorf("failed to mark node failed after %d retries", maxRetries)
}

// IsNodeUpgraded checks if a node is in the upgraded list
func (s *TalosStateService) IsNodeUpgraded(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) bool {
	state, err := s.GetUpgradeState(ctx, cluster)
	if err != nil {
		return false
	}
	for _, n := range state.UpgradedNodes {
		if n == nodeName {
			return true
		}
	}
	return false
}

// IsNodeFailed checks if a node is in the failed list
func (s *TalosStateService) IsNodeFailed(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) bool {
	state, err := s.GetUpgradeState(ctx, cluster)
	if err != nil {
		return false
	}
	for _, n := range state.FailedNodes {
		if n == nodeName {
			return true
		}
	}
	return false
}

// ClearNodeFromFailed removes a node from the failed list (for retry)
func (s *TalosStateService) ClearNodeFromFailed(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := s.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			return nil
		}

		existingNodes := string(secret.Data["upgrade_failed_nodes"])
		if existingNodes == "" {
			return nil
		}

		nodes := strings.Split(existingNodes, ",")
		var newNodes []string
		found := false
		for _, n := range nodes {
			if n != nodeName {
				newNodes = append(newNodes, n)
			} else {
				found = true
			}
		}

		if !found {
			return nil
		}

		secret.Data["upgrade_failed_nodes"] = []byte(strings.Join(newNodes, ","))

		err = s.secretService.UpdateTalosSecret(ctx, secret)
		if err == nil {
			vlog.Info(fmt.Sprintf("Node cleared from failed list for retry: cluster=%s node=%s", cluster.Name, nodeName))
			return nil
		}

		if apierrors.IsConflict(err) && attempt < maxRetries-1 {
			time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1))
			continue
		}
		return err
	}
	return fmt.Errorf("failed to clear node from failed list after %d retries", maxRetries)
}

// CompleteUpgradeState marks an upgrade as complete and updates the version in the secret
func (s *TalosStateService) CompleteUpgradeState(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, upgradeType, newVersion string) error {
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := s.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}

		// Update the appropriate version field
		switch upgradeType {
		case "talos":
			secret.Data["talos_version"] = []byte(newVersion)
		case "kubernetes":
			secret.Data["kubernetes_version"] = []byte(newVersion)
		}

		// Clear upgrade state
		now := time.Now().UTC().Format(time.RFC3339)
		secret.Data["upgrade_in_progress"] = []byte(falseStr)
		secret.Data["upgrade_type"] = []byte("")
		secret.Data["upgrade_target"] = []byte("")
		secret.Data["upgrade_completed_at"] = []byte(now)
		secret.Data["upgrade_last_node"] = []byte("")

		err = s.secretService.UpdateTalosSecret(ctx, secret)
		if err == nil {
			vlog.Info(fmt.Sprintf("Upgrade completed and version persisted: cluster=%s type=%s version=%s",
				cluster.Name, upgradeType, newVersion))
			return nil
		}

		if apierrors.IsConflict(err) && attempt < maxRetries-1 {
			time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1))
			continue
		}
		return err
	}
	return fmt.Errorf("failed to complete upgrade state after %d retries", maxRetries)
}

// ClearUpgradeState clears the upgrade state (e.g., on failure or manual reset)
func (s *TalosStateService) ClearUpgradeState(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := s.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			return nil // Nothing to clear
		}

		secret.Data["upgrade_in_progress"] = []byte(falseStr)
		secret.Data["upgrade_type"] = []byte("")
		secret.Data["upgrade_target"] = []byte("")
		secret.Data["upgrade_last_node"] = []byte("")
		secret.Data["upgrade_nodes_done"] = []byte("0")
		secret.Data["upgrade_nodes_total"] = []byte("0")

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
	return fmt.Errorf("failed to clear upgrade state after %d retries", maxRetries)
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

// RemoveConfiguredNode removes a node name from the list of configured nodes in the secret
func (s *TalosStateService) RemoveConfiguredNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	maxRetries := 3
	for attempt := range maxRetries {
		secret, err := s.secretService.GetTalosSecret(ctx, cluster)
		if err != nil {
			return err
		}
		if secret.Data == nil {
			return nil // No nodes configured
		}

		existingNodes := string(secret.Data["configured_nodes"])
		if existingNodes == "" {
			return nil // No nodes to remove
		}

		nodes := strings.Split(existingNodes, ",")
		var newNodes []string
		found := false
		for _, n := range nodes {
			if n != nodeName {
				newNodes = append(newNodes, n)
			} else {
				found = true
			}
		}

		if !found {
			return nil // Node was not in the list
		}

		secret.Data["configured_nodes"] = []byte(strings.Join(newNodes, ","))

		err = s.secretService.UpdateTalosSecret(ctx, secret)
		if err == nil {
			vlog.Info(fmt.Sprintf("Removed node from configured nodes: node=%s cluster=%s", nodeName, cluster.Name))
			return nil
		}

		if apierrors.IsConflict(err) && attempt < maxRetries-1 {
			time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1))
			continue
		}
		return err
	}
	return fmt.Errorf("failed to remove configured node after %d retries", maxRetries)
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
