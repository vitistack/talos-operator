// Package upgradeservice provides functionality for managing Talos and Kubernetes
// upgrades on KubernetesCluster resources using an annotation-based workflow.
//
// The upgrade flow is provider-agnostic and works as follows:
// 1. Operator detects available upgrades and sets *-available annotations
// 2. User triggers upgrades by setting *-target annotations
// 3. Operator performs rolling upgrades and updates *-status annotations
// 4. After completion, operator cleans up trigger annotations
package upgradeservice

import (
	"context"
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/internal/services/machineservice"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// UpgradeService manages upgrade detection, annotation handling, and orchestration
// for Talos and Kubernetes upgrades on KubernetesCluster resources.
type UpgradeService struct {
	client.Client
	statusManager  *status.StatusManager
	clientService  *talosclientservice.TalosClientService
	machineService *machineservice.MachineService
}

// NewUpgradeService creates a new UpgradeService instance
func NewUpgradeService(
	c client.Client,
	statusManager *status.StatusManager,
	clientService *talosclientservice.TalosClientService,
	machineService *machineservice.MachineService,
) *UpgradeService {
	return &UpgradeService{
		Client:         c,
		statusManager:  statusManager,
		clientService:  clientService,
		machineService: machineService,
	}
}

// UpgradeState represents the current upgrade state for a cluster
type UpgradeState struct {
	// Talos upgrade state
	TalosCurrent   string
	TalosAvailable string
	TalosTarget    string
	TalosStatus    consts.UpgradeStatus
	TalosMessage   string
	TalosProgress  string // "upgraded/total" e.g., "2/5"

	// Kubernetes upgrade state
	KubernetesCurrent   string
	KubernetesAvailable string
	KubernetesTarget    string
	KubernetesStatus    consts.UpgradeStatus
	KubernetesMessage   string
}

// failUpgradeParams contains parameters for marking an upgrade as failed
type failUpgradeParams struct {
	upgradeType       string // "Talos" or "Kubernetes"
	targetVersion     string
	reason            string
	statusAnnotation  string
	messageAnnotation string
	conditionType     string
}

// failUpgrade is a helper to mark any upgrade type as failed
func (s *UpgradeService) failUpgrade(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, params *failUpgradeParams) error {
	updates := map[string]string{
		params.statusAnnotation:  string(consts.UpgradeStatusFailed),
		params.messageAnnotation: fmt.Sprintf("%s upgrade failed: %s", params.upgradeType, params.reason),
	}

	if err := s.SetUpgradeAnnotations(ctx, cluster, updates); err != nil {
		return err
	}

	// Update status phase and condition
	if err := s.statusManager.SetPhase(ctx, cluster, status.PhaseUpgradeFailed); err != nil {
		vlog.Error("Failed to set phase to UpgradeFailed", err)
	}
	if err := s.statusManager.SetCondition(ctx, cluster, params.conditionType, "False", "Failed",
		fmt.Sprintf("%s upgrade to %s failed: %s", params.upgradeType, params.targetVersion, params.reason)); err != nil {
		vlog.Error(fmt.Sprintf("Failed to set %s condition", params.conditionType), err)
	}

	vlog.Error(fmt.Sprintf("%s upgrade failed: cluster=%s version=%s reason=%s",
		params.upgradeType, cluster.Name, params.targetVersion, params.reason), nil)
	return nil
}

// GetUpgradeState reads the current upgrade state from cluster annotations
func (s *UpgradeService) GetUpgradeState(cluster *vitistackv1alpha1.KubernetesCluster) *UpgradeState {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	return &UpgradeState{
		TalosCurrent:        annotations[consts.TalosCurrentAnnotation],
		TalosAvailable:      annotations[consts.TalosAvailableAnnotation],
		TalosTarget:         annotations[consts.TalosTargetAnnotation],
		TalosStatus:         consts.UpgradeStatus(annotations[consts.TalosStatusAnnotation]),
		TalosMessage:        annotations[consts.TalosMessageAnnotation],
		TalosProgress:       annotations[consts.TalosProgressAnnotation],
		KubernetesCurrent:   annotations[consts.KubernetesCurrentAnnotation],
		KubernetesAvailable: annotations[consts.KubernetesAvailableAnnotation],
		KubernetesTarget:    annotations[consts.KubernetesTargetAnnotation],
		KubernetesStatus:    consts.UpgradeStatus(annotations[consts.KubernetesStatusAnnotation]),
		KubernetesMessage:   annotations[consts.KubernetesMessageAnnotation],
	}
}

// SetAnnotation sets a single annotation on the cluster
func (s *UpgradeService) SetAnnotation(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, key, value string) error {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	// Skip if value unchanged
	if annotations[key] == value {
		return nil
	}

	annotations[key] = value
	cluster.SetAnnotations(annotations)

	if err := s.Update(ctx, cluster); err != nil {
		return fmt.Errorf("failed to set annotation %s: %w", key, err)
	}
	return nil
}

// RemoveAnnotation removes an annotation from the cluster
func (s *UpgradeService) RemoveAnnotation(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, key string) error {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		return nil
	}

	if _, exists := annotations[key]; !exists {
		return nil
	}

	delete(annotations, key)
	cluster.SetAnnotations(annotations)

	if err := s.Update(ctx, cluster); err != nil {
		return fmt.Errorf("failed to remove annotation %s: %w", key, err)
	}
	return nil
}

// SetUpgradeAnnotations sets multiple upgrade-related annotations at once
func (s *UpgradeService) SetUpgradeAnnotations(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, updates map[string]string) error {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	changed := false
	for key, value := range updates {
		if annotations[key] != value {
			annotations[key] = value
			changed = true
		}
	}

	if !changed {
		return nil
	}

	cluster.SetAnnotations(annotations)
	if err := s.Update(ctx, cluster); err != nil {
		return fmt.Errorf("failed to set upgrade annotations: %w", err)
	}
	return nil
}

// InitializeCurrentVersions sets the current version annotations if not already set.
// This should be called when a cluster first becomes ready.
func (s *UpgradeService) InitializeCurrentVersions(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, talosVersion, k8sVersion string) error {
	state := s.GetUpgradeState(cluster)
	updates := make(map[string]string)

	if state.TalosCurrent == "" && talosVersion != "" {
		updates[consts.TalosCurrentAnnotation] = talosVersion
		updates[consts.TalosStatusAnnotation] = string(consts.UpgradeStatusIdle)
	}

	if state.KubernetesCurrent == "" && k8sVersion != "" {
		updates[consts.KubernetesCurrentAnnotation] = k8sVersion
		updates[consts.KubernetesStatusAnnotation] = string(consts.UpgradeStatusIdle)
	}

	if len(updates) > 0 {
		return s.SetUpgradeAnnotations(ctx, cluster, updates)
	}
	return nil
}

// SetTalosUpgradeAvailable marks a new Talos version as available for upgrade
func (s *UpgradeService) SetTalosUpgradeAvailable(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, availableVersion string) error {
	state := s.GetUpgradeState(cluster)

	// Don't set available if already at this version or upgrade already in progress
	if state.TalosCurrent == availableVersion {
		return nil
	}
	if state.TalosStatus == consts.UpgradeStatusInProgress {
		return nil
	}

	updates := map[string]string{
		consts.TalosAvailableAnnotation: availableVersion,
		consts.TalosMessageAnnotation:   fmt.Sprintf("Upgrade available: %s → %s", state.TalosCurrent, availableVersion),
	}

	if state.TalosStatus == "" || state.TalosStatus == consts.UpgradeStatusCompleted {
		updates[consts.TalosStatusAnnotation] = string(consts.UpgradeStatusIdle)
	}

	vlog.Info(fmt.Sprintf("Talos upgrade available: cluster=%s current=%s available=%s", cluster.Name, state.TalosCurrent, availableVersion))
	return s.SetUpgradeAnnotations(ctx, cluster, updates)
}

// SetKubernetesUpgradeAvailable marks a new Kubernetes version as available for upgrade.
// This should only be called when the current Talos version supports the new K8s version.
func (s *UpgradeService) SetKubernetesUpgradeAvailable(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, availableVersion string) error {
	state := s.GetUpgradeState(cluster)

	// Don't set available if already at this version or upgrade in progress
	if state.KubernetesCurrent == availableVersion {
		return nil
	}
	if state.KubernetesStatus == consts.UpgradeStatusInProgress {
		return nil
	}

	// Check if Talos upgrade is in progress - wait until complete
	if state.TalosStatus == consts.UpgradeStatusInProgress {
		vlog.Info(fmt.Sprintf("Deferring K8s upgrade availability: cluster=%s talos_upgrade_in_progress=true", cluster.Name))
		return nil
	}

	updates := map[string]string{
		consts.KubernetesAvailableAnnotation: availableVersion,
		consts.KubernetesMessageAnnotation:   fmt.Sprintf("Upgrade available: %s → %s", state.KubernetesCurrent, availableVersion),
	}

	if state.KubernetesStatus == "" || state.KubernetesStatus == consts.UpgradeStatusCompleted {
		updates[consts.KubernetesStatusAnnotation] = string(consts.UpgradeStatusIdle)
	}

	vlog.Info(fmt.Sprintf("Kubernetes upgrade available: cluster=%s current=%s available=%s", cluster.Name, state.KubernetesCurrent, availableVersion))
	return s.SetUpgradeAnnotations(ctx, cluster, updates)
}

// IsTalosUpgradeRequested checks if user has requested a Talos upgrade
func (s *UpgradeService) IsTalosUpgradeRequested(cluster *vitistackv1alpha1.KubernetesCluster) bool {
	state := s.GetUpgradeState(cluster)
	return state.TalosTarget != "" && state.TalosStatus != consts.UpgradeStatusInProgress
}

// IsKubernetesUpgradeRequested checks if user has requested a Kubernetes upgrade
func (s *UpgradeService) IsKubernetesUpgradeRequested(cluster *vitistackv1alpha1.KubernetesCluster) bool {
	state := s.GetUpgradeState(cluster)
	return state.KubernetesTarget != "" && state.KubernetesStatus != consts.UpgradeStatusInProgress
}

// StartTalosUpgrade marks the beginning of a Talos upgrade and updates status/conditions
func (s *UpgradeService) StartTalosUpgrade(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, totalNodes int) error {
	state := s.GetUpgradeState(cluster)

	updates := map[string]string{
		consts.TalosStatusAnnotation:   string(consts.UpgradeStatusInProgress),
		consts.TalosMessageAnnotation:  fmt.Sprintf("Starting Talos upgrade: %s → %s", state.TalosCurrent, state.TalosTarget),
		consts.TalosProgressAnnotation: fmt.Sprintf("0/%d", totalNodes),
	}

	if err := s.SetUpgradeAnnotations(ctx, cluster, updates); err != nil {
		return err
	}

	// Update status phase and condition
	if err := s.statusManager.SetPhase(ctx, cluster, status.PhaseUpgradingTalos); err != nil {
		vlog.Error("Failed to set phase to UpgradingTalos", err)
	}
	if err := s.statusManager.SetCondition(ctx, cluster, "TalosUpgrade", "True", "InProgress",
		fmt.Sprintf("Upgrading Talos from %s to %s", state.TalosCurrent, state.TalosTarget)); err != nil {
		vlog.Error("Failed to set TalosUpgrade condition", err)
	}

	return nil
}

// UpdateTalosUpgradeProgress updates the progress during a rolling Talos upgrade
func (s *UpgradeService) UpdateTalosUpgradeProgress(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string, nodesUpgraded, totalNodes int) error {
	updates := map[string]string{
		consts.TalosMessageAnnotation:  fmt.Sprintf("Upgrading node %s (%d/%d)", nodeName, nodesUpgraded, totalNodes),
		consts.TalosProgressAnnotation: fmt.Sprintf("%d/%d", nodesUpgraded, totalNodes),
	}

	if err := s.SetUpgradeAnnotations(ctx, cluster, updates); err != nil {
		return err
	}

	// Update condition with progress
	if err := s.statusManager.SetCondition(ctx, cluster, "TalosUpgrade", "True", "InProgress",
		fmt.Sprintf("Upgrading node %s (%d/%d nodes completed)", nodeName, nodesUpgraded, totalNodes)); err != nil {
		vlog.Error("Failed to update TalosUpgrade condition", err)
	}

	return nil
}

// CompleteTalosUpgrade marks a successful Talos upgrade completion
func (s *UpgradeService) CompleteTalosUpgrade(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	state := s.GetUpgradeState(cluster)

	updates := map[string]string{
		consts.TalosCurrentAnnotation:  state.TalosTarget,
		consts.TalosStatusAnnotation:   string(consts.UpgradeStatusCompleted),
		consts.TalosMessageAnnotation:  fmt.Sprintf("Talos upgrade completed: now running %s", state.TalosTarget),
		consts.TalosProgressAnnotation: "",
	}

	if err := s.SetUpgradeAnnotations(ctx, cluster, updates); err != nil {
		return err
	}

	// Remove target and available annotations
	if err := s.RemoveAnnotation(ctx, cluster, consts.TalosTargetAnnotation); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to remove talos-target annotation: %v", err))
	}
	if err := s.RemoveAnnotation(ctx, cluster, consts.TalosAvailableAnnotation); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to remove talos-available annotation: %v", err))
	}
	if err := s.RemoveAnnotation(ctx, cluster, consts.TalosProgressAnnotation); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to remove talos-progress annotation: %v", err))
	}

	// Update status phase and condition
	if err := s.statusManager.SetPhase(ctx, cluster, status.PhaseReady); err != nil {
		vlog.Error("Failed to set phase to Ready", err)
	}
	if err := s.statusManager.SetCondition(ctx, cluster, "TalosUpgrade", "False", "Completed",
		fmt.Sprintf("Talos successfully upgraded to %s", state.TalosTarget)); err != nil {
		vlog.Error("Failed to set TalosUpgrade condition", err)
	}

	vlog.Info(fmt.Sprintf("Talos upgrade completed: cluster=%s version=%s", cluster.Name, state.TalosTarget))
	return nil
}

// FailTalosUpgrade marks a failed Talos upgrade
func (s *UpgradeService) FailTalosUpgrade(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, reason string) error {
	state := s.GetUpgradeState(cluster)
	return s.failUpgrade(ctx, cluster, &failUpgradeParams{
		upgradeType:       "Talos",
		targetVersion:     state.TalosTarget,
		reason:            reason,
		statusAnnotation:  consts.TalosStatusAnnotation,
		messageAnnotation: consts.TalosMessageAnnotation,
		conditionType:     "TalosUpgrade",
	})
}

// StartKubernetesUpgrade marks the beginning of a Kubernetes upgrade
func (s *UpgradeService) StartKubernetesUpgrade(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	state := s.GetUpgradeState(cluster)

	updates := map[string]string{
		consts.KubernetesStatusAnnotation:  string(consts.UpgradeStatusInProgress),
		consts.KubernetesMessageAnnotation: fmt.Sprintf("Starting Kubernetes upgrade: %s → %s", state.KubernetesCurrent, state.KubernetesTarget),
	}

	if err := s.SetUpgradeAnnotations(ctx, cluster, updates); err != nil {
		return err
	}

	// Update status phase and condition
	if err := s.statusManager.SetPhase(ctx, cluster, status.PhaseUpgradingKubernetes); err != nil {
		vlog.Error("Failed to set phase to UpgradingKubernetes", err)
	}
	if err := s.statusManager.SetCondition(ctx, cluster, "KubernetesUpgrade", "True", "InProgress",
		fmt.Sprintf("Upgrading Kubernetes from %s to %s", state.KubernetesCurrent, state.KubernetesTarget)); err != nil {
		vlog.Error("Failed to set KubernetesUpgrade condition", err)
	}

	return nil
}

// CompleteKubernetesUpgrade marks a successful Kubernetes upgrade completion
func (s *UpgradeService) CompleteKubernetesUpgrade(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	state := s.GetUpgradeState(cluster)

	updates := map[string]string{
		consts.KubernetesCurrentAnnotation: state.KubernetesTarget,
		consts.KubernetesStatusAnnotation:  string(consts.UpgradeStatusCompleted),
		consts.KubernetesMessageAnnotation: fmt.Sprintf("Kubernetes upgrade completed: now running %s", state.KubernetesTarget),
	}

	if err := s.SetUpgradeAnnotations(ctx, cluster, updates); err != nil {
		return err
	}

	// Remove target and available annotations
	if err := s.RemoveAnnotation(ctx, cluster, consts.KubernetesTargetAnnotation); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to remove kubernetes-target annotation: %v", err))
	}
	if err := s.RemoveAnnotation(ctx, cluster, consts.KubernetesAvailableAnnotation); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to remove kubernetes-available annotation: %v", err))
	}

	// Update status phase and condition
	if err := s.statusManager.SetPhase(ctx, cluster, status.PhaseReady); err != nil {
		vlog.Error("Failed to set phase to Ready", err)
	}
	if err := s.statusManager.SetCondition(ctx, cluster, "KubernetesUpgrade", "False", "Completed",
		fmt.Sprintf("Kubernetes successfully upgraded to %s", state.KubernetesTarget)); err != nil {
		vlog.Error("Failed to set KubernetesUpgrade condition", err)
	}

	vlog.Info(fmt.Sprintf("Kubernetes upgrade completed: cluster=%s version=%s", cluster.Name, state.KubernetesTarget))
	return nil
}

// FailKubernetesUpgrade marks a failed Kubernetes upgrade
func (s *UpgradeService) FailKubernetesUpgrade(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, reason string) error {
	state := s.GetUpgradeState(cluster)
	return s.failUpgrade(ctx, cluster, &failUpgradeParams{
		upgradeType:       "Kubernetes",
		targetVersion:     state.KubernetesTarget,
		reason:            reason,
		statusAnnotation:  consts.KubernetesStatusAnnotation,
		messageAnnotation: consts.KubernetesMessageAnnotation,
		conditionType:     "KubernetesUpgrade",
	})
}

// BlockKubernetesUpgrade marks a Kubernetes upgrade as blocked (e.g., waiting for Talos upgrade)
func (s *UpgradeService) BlockKubernetesUpgrade(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, reason string) error {
	updates := map[string]string{
		consts.KubernetesStatusAnnotation:  string(consts.UpgradeStatusBlocked),
		consts.KubernetesMessageAnnotation: reason,
	}

	if err := s.SetUpgradeAnnotations(ctx, cluster, updates); err != nil {
		return err
	}

	// Update condition
	if err := s.statusManager.SetCondition(ctx, cluster, "KubernetesUpgrade", "False", "Blocked", reason); err != nil {
		vlog.Error("Failed to set KubernetesUpgrade condition", err)
	}

	vlog.Info(fmt.Sprintf("Kubernetes upgrade blocked: cluster=%s reason=%s", cluster.Name, reason))
	return nil
}

// ValidateUpgradeTarget validates that a target version is a valid upgrade path
func (s *UpgradeService) ValidateUpgradeTarget(currentVersion, targetVersion string) error {
	if currentVersion == "" || targetVersion == "" {
		return fmt.Errorf("current and target versions must be specified")
	}

	// Parse versions (strip 'v' prefix if present)
	current, err := semver.NewVersion(strings.TrimPrefix(currentVersion, "v"))
	if err != nil {
		return fmt.Errorf("invalid current version %s: %w", currentVersion, err)
	}

	target, err := semver.NewVersion(strings.TrimPrefix(targetVersion, "v"))
	if err != nil {
		return fmt.Errorf("invalid target version %s: %w", targetVersion, err)
	}

	// Target must be greater than current
	if !target.GreaterThan(current) {
		return fmt.Errorf("target version %s must be greater than current version %s", targetVersion, currentVersion)
	}

	// For Kubernetes, only allow one minor version upgrade at a time
	// This is a Kubernetes best practice
	if target.Major() == current.Major() {
		if target.Minor()-current.Minor() > 1 {
			return fmt.Errorf("can only upgrade one minor version at a time: %s → %s (max: %d.%d.x)",
				currentVersion, targetVersion, current.Major(), current.Minor()+1)
		}
	}

	return nil
}

// IsUpgradeInProgress returns true if any upgrade is currently in progress
func (s *UpgradeService) IsUpgradeInProgress(cluster *vitistackv1alpha1.KubernetesCluster) bool {
	state := s.GetUpgradeState(cluster)
	return state.TalosStatus == consts.UpgradeStatusInProgress ||
		state.KubernetesStatus == consts.UpgradeStatusInProgress
}

// ShouldBlockKubernetesUpgrade checks if Kubernetes upgrade should be blocked
// due to Talos version incompatibility or Talos upgrade in progress
func (s *UpgradeService) ShouldBlockKubernetesUpgrade(cluster *vitistackv1alpha1.KubernetesCluster) (bool, string) {
	state := s.GetUpgradeState(cluster)

	// Block if Talos upgrade is in progress
	if state.TalosStatus == consts.UpgradeStatusInProgress {
		return true, "Talos upgrade is in progress. Complete Talos upgrade before upgrading Kubernetes."
	}

	// Block if Talos upgrade is pending/requested
	if state.TalosTarget != "" && state.TalosStatus != consts.UpgradeStatusCompleted {
		return true, fmt.Sprintf("Talos upgrade to %s is pending. Complete Talos upgrade before upgrading Kubernetes.", state.TalosTarget)
	}

	return false, ""
}
