// Package upgradeservice provides functionality for managing upgrade state.
package upgradeservice

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// UpgradeType represents the type of upgrade being performed
type UpgradeType string

const (
	UpgradeTypeTalos      UpgradeType = "talos"
	UpgradeTypeKubernetes UpgradeType = "kubernetes"
)

// UpgradePhase represents the current phase of an upgrade
type UpgradePhase string

const (
	UpgradePhaseNotStarted        UpgradePhase = "not_started"
	UpgradePhaseControlPlanes     UpgradePhase = "control_planes"
	UpgradePhaseControlPlanesWait UpgradePhase = "control_planes_wait" // Waiting for CPs to settle
	UpgradePhaseWorkers           UpgradePhase = "workers"
	UpgradePhaseCompleted         UpgradePhase = "completed"
	UpgradePhaseFailed            UpgradePhase = "failed"
)

// NodeUpgradeState tracks the upgrade state of a single node
type NodeUpgradeState struct {
	NodeName         string    `json:"nodeName"`
	Role             string    `json:"role"` // "control-plane" or "worker"
	IP               string    `json:"ip"`
	UpgradeInitiated bool      `json:"upgradeInitiated"` // Upgrade API called
	UpgradeCompleted bool      `json:"upgradeCompleted"` // Version verified
	NodeReady        bool      `json:"nodeReady"`        // Kubernetes Ready condition
	Error            string    `json:"error,omitempty"`  // Error message if failed
	InitiatedAt      time.Time `json:"initiatedAt,omitempty"`
	CompletedAt      time.Time `json:"completedAt,omitempty"`
}

// ClusterUpgradeState tracks the overall upgrade state for a cluster
type ClusterUpgradeState struct {
	// Upgrade configuration
	UpgradeType    UpgradeType `json:"upgradeType"`
	TargetVersion  string      `json:"targetVersion"`
	InstallerImage string      `json:"installerImage,omitempty"` // For Talos upgrades

	// Current progress
	Phase            UpgradePhase `json:"phase"`
	CurrentNodeName  string       `json:"currentNodeName,omitempty"` // Node currently being upgraded
	CurrentNodeIndex int          `json:"currentNodeIndex"`          // Index of current node

	// Node lists (ordered: control planes first, then workers)
	Nodes             []NodeUpgradeState `json:"nodes"`
	ControlPlaneCount int                `json:"controlPlaneCount"`
	WorkerCount       int                `json:"workerCount"`

	// Timestamps
	StartedAt     time.Time `json:"startedAt,omitempty"`
	CompletedAt   time.Time `json:"completedAt,omitempty"`
	LastUpdatedAt time.Time `json:"lastUpdatedAt"`

	// Settling time tracking (wait after all CPs ready before workers)
	ControlPlanesReadyAt time.Time `json:"controlPlanesReadyAt,omitempty"`

	// Error tracking
	FailedNodeName string `json:"failedNodeName,omitempty"`
	FailedReason   string `json:"failedReason,omitempty"`
}

// UpgradeStateManager manages upgrade state in the cluster secret
type UpgradeStateManager struct {
	secretGetter  SecretGetter
	secretUpdater SecretUpdater
}

// SecretUpdater interface for updating secrets
type SecretUpdater interface {
	UpdateTalosSecret(ctx context.Context, secret *corev1.Secret) error
}

// NewUpgradeStateManager creates a new UpgradeStateManager
func NewUpgradeStateManager(secretGetter SecretGetter, secretUpdater SecretUpdater) *UpgradeStateManager {
	return &UpgradeStateManager{
		secretGetter:  secretGetter,
		secretUpdater: secretUpdater,
	}
}

// GetUpgradeState retrieves the current upgrade state from the cluster secret
func (m *UpgradeStateManager) GetUpgradeState(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (*ClusterUpgradeState, error) {
	secret, err := m.secretGetter.GetTalosSecret(ctx, cluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil // No secret yet, no upgrade state
		}
		return nil, fmt.Errorf("failed to get cluster secret: %w", err)
	}

	stateJSON, ok := secret.Data["upgrade_state"]
	if !ok || len(stateJSON) == 0 {
		return nil, nil // No upgrade state stored
	}

	var state ClusterUpgradeState
	if err := json.Unmarshal(stateJSON, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal upgrade state: %w", err)
	}

	return &state, nil
}

// SaveUpgradeState saves the upgrade state to the cluster secret
func (m *UpgradeStateManager) SaveUpgradeState(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, state *ClusterUpgradeState) error {
	// Update timestamp
	state.LastUpdatedAt = time.Now().UTC()

	// Marshal state to JSON
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal upgrade state: %w", err)
	}

	// Get current secret
	secret, err := m.secretGetter.GetTalosSecret(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get cluster secret: %w", err)
	}

	if secret.Data == nil {
		secret.Data = make(map[string][]byte)
	}
	secret.Data["upgrade_state"] = stateJSON

	// Update secret
	if err := m.secretUpdater.UpdateTalosSecret(ctx, secret); err != nil {
		return fmt.Errorf("failed to save upgrade state: %w", err)
	}

	return nil
}

// ClearUpgradeState removes the upgrade state from the cluster secret
func (m *UpgradeStateManager) ClearUpgradeState(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	secret, err := m.secretGetter.GetTalosSecret(ctx, cluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Nothing to clear
		}
		return fmt.Errorf("failed to get cluster secret: %w", err)
	}

	delete(secret.Data, "upgrade_state")

	if err := m.secretUpdater.UpdateTalosSecret(ctx, secret); err != nil {
		return fmt.Errorf("failed to clear upgrade state: %w", err)
	}

	return nil
}

// InitializeUpgradeState creates a new upgrade state for starting an upgrade
func (m *UpgradeStateManager) InitializeUpgradeState(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	upgradeType UpgradeType,
	targetVersion string,
	installerImage string,
	controlPlanes []NodeUpgradeState,
	workers []NodeUpgradeState,
) (*ClusterUpgradeState, error) {
	// Combine nodes: control planes first, then workers
	nodes := make([]NodeUpgradeState, 0, len(controlPlanes)+len(workers))
	nodes = append(nodes, controlPlanes...)
	nodes = append(nodes, workers...)

	state := &ClusterUpgradeState{
		UpgradeType:       upgradeType,
		TargetVersion:     targetVersion,
		InstallerImage:    installerImage,
		Phase:             UpgradePhaseControlPlanes,
		CurrentNodeIndex:  0,
		Nodes:             nodes,
		ControlPlaneCount: len(controlPlanes),
		WorkerCount:       len(workers),
		StartedAt:         time.Now().UTC(),
		LastUpdatedAt:     time.Now().UTC(),
	}

	// Set current node if we have any
	if len(nodes) > 0 {
		state.CurrentNodeName = nodes[0].NodeName
	}

	if err := m.SaveUpgradeState(ctx, cluster, state); err != nil {
		return nil, err
	}

	vlog.Info(fmt.Sprintf("Initialized upgrade state: cluster=%s type=%s target=%s nodes=%d (cp=%d, w=%d)",
		cluster.Name, upgradeType, targetVersion, len(nodes), len(controlPlanes), len(workers)))

	return state, nil
}

// MarkNodeUpgradeInitiated marks that an upgrade has been initiated on a node
func (m *UpgradeStateManager) MarkNodeUpgradeInitiated(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	state, err := m.GetUpgradeState(ctx, cluster)
	if err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("no upgrade state found")
	}

	for i := range state.Nodes {
		if state.Nodes[i].NodeName == nodeName {
			state.Nodes[i].UpgradeInitiated = true
			state.Nodes[i].InitiatedAt = time.Now().UTC()
			break
		}
	}

	return m.SaveUpgradeState(ctx, cluster, state)
}

// MarkNodeUpgradeCompleted marks that a node upgrade has completed (version verified)
func (m *UpgradeStateManager) MarkNodeUpgradeCompleted(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	state, err := m.GetUpgradeState(ctx, cluster)
	if err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("no upgrade state found")
	}

	for i := range state.Nodes {
		if state.Nodes[i].NodeName == nodeName {
			state.Nodes[i].UpgradeCompleted = true
			state.Nodes[i].CompletedAt = time.Now().UTC()
			break
		}
	}

	return m.SaveUpgradeState(ctx, cluster, state)
}

// MarkNodeReady marks that a node is ready in Kubernetes
func (m *UpgradeStateManager) MarkNodeReady(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string) error {
	state, err := m.GetUpgradeState(ctx, cluster)
	if err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("no upgrade state found")
	}

	for i := range state.Nodes {
		if state.Nodes[i].NodeName == nodeName {
			state.Nodes[i].NodeReady = true
			break
		}
	}

	return m.SaveUpgradeState(ctx, cluster, state)
}

// AdvanceToNextNode moves to the next node in the upgrade sequence
func (m *UpgradeStateManager) AdvanceToNextNode(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	state, err := m.GetUpgradeState(ctx, cluster)
	if err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("no upgrade state found")
	}

	nextIndex := state.CurrentNodeIndex + 1

	// Check if we're done with control planes and need to wait
	switch {
	case state.Phase == UpgradePhaseControlPlanes && nextIndex >= state.ControlPlaneCount:
		// All control planes done, transition to waiting phase
		state.Phase = UpgradePhaseControlPlanesWait
		state.ControlPlanesReadyAt = time.Now().UTC()
		state.CurrentNodeName = ""
		vlog.Info(fmt.Sprintf("All control planes upgraded, entering wait phase: cluster=%s", cluster.Name))
	case state.Phase == UpgradePhaseWorkers && nextIndex >= len(state.Nodes):
		// All workers done, upgrade complete
		state.Phase = UpgradePhaseCompleted
		state.CompletedAt = time.Now().UTC()
		state.CurrentNodeName = ""
		vlog.Info(fmt.Sprintf("All nodes upgraded, upgrade complete: cluster=%s", cluster.Name))
	default:
		// Move to next node
		state.CurrentNodeIndex = nextIndex
		if nextIndex < len(state.Nodes) {
			state.CurrentNodeName = state.Nodes[nextIndex].NodeName
		}
	}

	return m.SaveUpgradeState(ctx, cluster, state)
}

// TransitionToWorkers transitions from control plane wait phase to workers
func (m *UpgradeStateManager) TransitionToWorkers(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	state, err := m.GetUpgradeState(ctx, cluster)
	if err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("no upgrade state found")
	}

	if state.WorkerCount == 0 {
		// No workers, upgrade is complete
		state.Phase = UpgradePhaseCompleted
		state.CompletedAt = time.Now().UTC()
		vlog.Info(fmt.Sprintf("No workers to upgrade, upgrade complete: cluster=%s", cluster.Name))
	} else {
		state.Phase = UpgradePhaseWorkers
		state.CurrentNodeIndex = state.ControlPlaneCount // First worker
		state.CurrentNodeName = state.Nodes[state.CurrentNodeIndex].NodeName
		vlog.Info(fmt.Sprintf("Starting worker upgrades: cluster=%s workers=%d", cluster.Name, state.WorkerCount))
	}

	return m.SaveUpgradeState(ctx, cluster, state)
}

// MarkUpgradeFailed marks the upgrade as failed with a reason
func (m *UpgradeStateManager) MarkUpgradeFailed(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodeName, reason string) error {
	state, err := m.GetUpgradeState(ctx, cluster)
	if err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("no upgrade state found")
	}

	state.Phase = UpgradePhaseFailed
	state.FailedNodeName = nodeName
	state.FailedReason = reason

	// Mark the node as failed
	for i := range state.Nodes {
		if state.Nodes[i].NodeName == nodeName {
			state.Nodes[i].Error = reason
			break
		}
	}

	vlog.Error(fmt.Sprintf("Upgrade failed: cluster=%s node=%s reason=%s", cluster.Name, nodeName, reason), nil)

	return m.SaveUpgradeState(ctx, cluster, state)
}

// GetCurrentNode returns the current node being upgraded
func (state *ClusterUpgradeState) GetCurrentNode() *NodeUpgradeState {
	if state.CurrentNodeIndex < 0 || state.CurrentNodeIndex >= len(state.Nodes) {
		return nil
	}
	return &state.Nodes[state.CurrentNodeIndex]
}

// IsComplete returns true if the upgrade is complete
func (state *ClusterUpgradeState) IsComplete() bool {
	return state.Phase == UpgradePhaseCompleted
}

// IsFailed returns true if the upgrade has failed
func (state *ClusterUpgradeState) IsFailed() bool {
	return state.Phase == UpgradePhaseFailed
}

// IsInProgress returns true if an upgrade is in progress
func (state *ClusterUpgradeState) IsInProgress() bool {
	return state.Phase != UpgradePhaseNotStarted &&
		state.Phase != UpgradePhaseCompleted &&
		state.Phase != UpgradePhaseFailed
}

// GetProgress returns upgrade progress as "completed/total"
func (state *ClusterUpgradeState) GetProgress() string {
	completed := 0
	for _, node := range state.Nodes {
		if node.UpgradeCompleted && node.NodeReady {
			completed++
		}
	}
	return fmt.Sprintf("%d/%d", completed, len(state.Nodes))
}

// ShouldWaitForSettling returns true if we should wait for control planes to settle
func (state *ClusterUpgradeState) ShouldWaitForSettling(settlingDuration time.Duration) bool {
	if state.Phase != UpgradePhaseControlPlanesWait {
		return false
	}
	if state.ControlPlanesReadyAt.IsZero() {
		return true
	}
	return time.Since(state.ControlPlanesReadyAt) < settlingDuration
}

// GetRemainingSettlingTime returns how much settling time remains
func (state *ClusterUpgradeState) GetRemainingSettlingTime(settlingDuration time.Duration) time.Duration {
	if state.ControlPlanesReadyAt.IsZero() {
		return settlingDuration
	}
	elapsed := time.Since(state.ControlPlanesReadyAt)
	if elapsed >= settlingDuration {
		return 0
	}
	return settlingDuration - elapsed
}
