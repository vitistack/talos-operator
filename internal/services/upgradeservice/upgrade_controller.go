// Package upgradeservice provides integration between the upgrade system and the controller.
package upgradeservice

import (
	"context"
	"fmt"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/helpers/nodehelper"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/internal/services/secretservice"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	controlPlaneRole = "control-plane"
)

// UpgradeController manages the upgrade lifecycle for a KubernetesCluster
type UpgradeController struct {
	client.Client

	stateManager       *UpgradeStateManager
	readinessChecker   *NodeReadinessChecker
	rollingUpgrade     *RollingUpgradeExecutor
	talosClientService *talosclientservice.TalosClientService
	secretService      *secretservice.SecretService
	statusManager      *status.StatusManager
	upgradeService     *UpgradeService
}

// NewUpgradeController creates a new UpgradeController
func NewUpgradeController(
	c client.Client,
	secretService *secretservice.SecretService,
	statusManager *status.StatusManager,
	talosClientService *talosclientservice.TalosClientService,
	upgradeService *UpgradeService,
) *UpgradeController {
	// Create secret adapter that implements both SecretGetter and SecretUpdater
	secretAdapter := &secretServiceAdapter{secretService}

	stateManager := NewUpgradeStateManager(secretAdapter, secretAdapter)
	readinessChecker := NewNodeReadinessChecker(c, secretAdapter)
	rollingUpgrade := NewRollingUpgradeExecutor(stateManager, readinessChecker, talosClientService, upgradeService)

	return &UpgradeController{
		Client:             c,
		stateManager:       stateManager,
		readinessChecker:   readinessChecker,
		rollingUpgrade:     rollingUpgrade,
		talosClientService: talosClientService,
		secretService:      secretService,
		statusManager:      statusManager,
		upgradeService:     upgradeService,
	}
}

// secretServiceAdapter adapts secretservice.SecretService to the SecretGetter and SecretUpdater interfaces
type secretServiceAdapter struct {
	*secretservice.SecretService
}

// HandleUpgrade is the main entry point for handling upgrades during reconciliation.
// It returns (result, handled) where handled=true means an upgrade was processed.
func (c *UpgradeController) HandleUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	machines []vitistackv1alpha1.Machine,
) (requeueAfter time.Duration, handled bool, err error) {
	// Check for existing upgrade state
	state, err := c.stateManager.GetUpgradeState(ctx, cluster)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get upgrade state: %w", err)
	}

	// If there's an ongoing upgrade, continue it
	if state != nil && state.IsInProgress() {
		return c.continueUpgrade(ctx, cluster, clientConfig, state)
	}

	// If upgrade just completed, handle completion
	if state != nil && state.IsComplete() {
		return c.handleCompletedUpgrade(ctx, cluster, clientConfig, state)
	}

	// Check for new upgrade requests via annotations
	upgState := c.upgradeService.GetUpgradeState(cluster)

	// Check for Talos upgrade request
	if c.upgradeService.IsTalosUpgradeRequested(cluster) {
		return c.startTalosUpgrade(ctx, cluster, clientConfig, machines, upgState)
	}

	// Check for Kubernetes upgrade request
	if c.upgradeService.IsKubernetesUpgradeRequested(cluster) {
		return c.startKubernetesUpgrade(ctx, cluster, clientConfig, machines, upgState)
	}

	// Handle resume request for failed upgrades
	if upgState.ResumeRequested && (upgState.TalosStatus == consts.UpgradeStatusFailed ||
		upgState.KubernetesStatus == consts.UpgradeStatusFailed) {
		return c.resumeFailedUpgrade(ctx, cluster, clientConfig, upgState)
	}

	return 0, false, nil
}

// handleCompletedUpgrade handles an upgrade that has just completed
func (c *UpgradeController) handleCompletedUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	state *ClusterUpgradeState,
) (time.Duration, bool, error) {
	vlog.Info(fmt.Sprintf("Upgrade completed: cluster=%s type=%s targetVersion=%s", cluster.Name, state.UpgradeType, state.TargetVersion))

	// Mark upgrade as complete and regenerate configs
	if state.UpgradeType == UpgradeTypeTalos {
		_ = c.upgradeService.CompleteTalosUpgrade(ctx, cluster, state.TargetVersion)

		// Regenerate machine configs after Talos upgrade (no K8s version override needed)
		if clientConfig != nil {
			endpointIPs := clientConfig.Contexts[clientConfig.Context].Endpoints
			if err := c.upgradeService.RegenerateMachineConfigsAfterUpgrade(ctx, cluster, endpointIPs, state.TargetVersion, ""); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to regenerate machine configs after upgrade: %v", err))
			}
		}
	} else {
		_ = c.upgradeService.CompleteKubernetesUpgrade(ctx, cluster, state.TargetVersion)

		// Regenerate machine configs after Kubernetes upgrade
		// Pass the current Talos version (for installer image) and the NEW K8s version (for config)
		if clientConfig != nil {
			endpointIPs := clientConfig.Contexts[clientConfig.Context].Endpoints
			// Use current Talos version from annotations for config regeneration
			talosVersion := c.upgradeService.GetUpgradeState(cluster).TalosCurrent
			// Pass the upgraded K8s version as override since the cluster spec hasn't been updated
			if err := c.upgradeService.RegenerateMachineConfigsAfterUpgrade(ctx, cluster, endpointIPs, talosVersion, state.TargetVersion); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to regenerate machine configs after upgrade: %v", err))
			}
		}
	}

	// Clear upgrade state from secret
	_ = c.stateManager.ClearUpgradeState(ctx, cluster)

	// Reset phase to ready
	_ = c.statusManager.SetPhase(ctx, cluster, status.PhaseReady)

	return 10 * time.Second, true, nil
}

// continueUpgrade continues an in-progress upgrade
func (c *UpgradeController) continueUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	state *ClusterUpgradeState,
) (time.Duration, bool, error) {
	var result *RollingUpgradeResult

	switch state.UpgradeType {
	case UpgradeTypeTalos:
		result = c.rollingUpgrade.ExecuteTalosUpgradeStep(ctx, cluster, clientConfig)
	case UpgradeTypeKubernetes:
		result = c.rollingUpgrade.ExecuteKubernetesUpgradeStep(ctx, cluster, clientConfig)
	default:
		return 0, false, fmt.Errorf("unknown upgrade type: %s", state.UpgradeType)
	}

	return c.handleUpgradeResult(ctx, cluster, result, state.UpgradeType)
}

// startTalosUpgrade initiates a new Talos upgrade
func (c *UpgradeController) startTalosUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	machines []vitistackv1alpha1.Machine,
	upgState *UpgradeState,
) (time.Duration, bool, error) {
	vlog.Info(fmt.Sprintf("Starting Talos upgrade: cluster=%s current=%s target=%s",
		cluster.Name, upgState.TalosCurrent, upgState.TalosTarget))

	// Validate upgrade
	if err := c.upgradeService.ValidateUpgradeTarget(upgState.TalosCurrent, upgState.TalosTarget); err != nil {
		vlog.Error(fmt.Sprintf("Invalid Talos upgrade target: %v", err), nil)
		_ = c.upgradeService.FailTalosUpgrade(ctx, cluster, err.Error())
		return 30 * time.Second, true, nil
	}

	// Build installer image
	installerImage := c.buildTalosInstallerImage(upgState.TalosTarget)

	// Build node lists
	controlPlanes, workers := c.buildNodeLists(machines)

	// Initialize upgrade state
	_, err := c.stateManager.InitializeUpgradeState(
		ctx, cluster,
		UpgradeTypeTalos,
		upgState.TalosTarget,
		installerImage,
		controlPlanes,
		workers,
	)
	if err != nil {
		return 30 * time.Second, true, fmt.Errorf("failed to initialize upgrade state: %w", err)
	}

	// Update status
	_ = c.statusManager.SetPhase(ctx, cluster, status.PhaseUpgradingTalos)
	_ = c.upgradeService.StartTalosUpgrade(ctx, cluster, len(machines))

	// Continue with upgrade
	result := c.rollingUpgrade.ExecuteTalosUpgradeStep(ctx, cluster, clientConfig)
	return c.handleUpgradeResult(ctx, cluster, result, UpgradeTypeTalos)
}

// startKubernetesUpgrade initiates a new Kubernetes upgrade
func (c *UpgradeController) startKubernetesUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	machines []vitistackv1alpha1.Machine,
	upgState *UpgradeState,
) (time.Duration, bool, error) {
	vlog.Info(fmt.Sprintf("Starting Kubernetes upgrade: cluster=%s current=%s target=%s",
		cluster.Name, upgState.KubernetesCurrent, upgState.KubernetesTarget))

	// Validate upgrade
	if err := c.upgradeService.ValidateKubernetesUpgradeTarget(upgState.KubernetesCurrent, upgState.KubernetesTarget); err != nil {
		vlog.Error(fmt.Sprintf("Invalid Kubernetes upgrade target: %v", err), nil)
		_ = c.upgradeService.FailKubernetesUpgrade(ctx, cluster, err.Error())
		return 30 * time.Second, true, nil
	}

	// Build node lists
	controlPlanes, workers := c.buildNodeLists(machines)

	// Initialize upgrade state
	_, err := c.stateManager.InitializeUpgradeState(
		ctx, cluster,
		UpgradeTypeKubernetes,
		upgState.KubernetesTarget,
		"", // No installer image for K8s upgrades
		controlPlanes,
		workers,
	)
	if err != nil {
		return 30 * time.Second, true, fmt.Errorf("failed to initialize upgrade state: %w", err)
	}

	// Update status
	_ = c.statusManager.SetPhase(ctx, cluster, status.PhaseUpgradingKubernetes)
	_ = c.upgradeService.StartKubernetesUpgrade(ctx, cluster)

	// Continue with upgrade
	result := c.rollingUpgrade.ExecuteKubernetesUpgradeStep(ctx, cluster, clientConfig)
	return c.handleUpgradeResult(ctx, cluster, result, UpgradeTypeKubernetes)
}

// resumeFailedUpgrade resumes a failed upgrade
func (c *UpgradeController) resumeFailedUpgrade(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	_ *UpgradeState, // upgState from service state; actual state is retrieved from secret
) (time.Duration, bool, error) {
	vlog.Info(fmt.Sprintf("Resuming failed upgrade: cluster=%s", cluster.Name))

	// Get current state
	state, err := c.stateManager.GetUpgradeState(ctx, cluster)
	if err != nil || state == nil {
		return 0, false, fmt.Errorf("no upgrade state to resume")
	}

	// Reset phase from failed
	if state.Phase == UpgradePhaseFailed {
		// Determine which phase to resume from
		failedNodeIndex := -1
		for i, node := range state.Nodes {
			if node.NodeName == state.FailedNodeName {
				failedNodeIndex = i
				break
			}
		}

		if failedNodeIndex >= 0 {
			// Reset the failed node state
			state.Nodes[failedNodeIndex].Error = ""
			state.CurrentNodeIndex = failedNodeIndex
			state.CurrentNodeName = state.Nodes[failedNodeIndex].NodeName

			if failedNodeIndex < state.ControlPlaneCount {
				state.Phase = UpgradePhaseControlPlanes
			} else {
				state.Phase = UpgradePhaseWorkers
			}

			state.FailedNodeName = ""
			state.FailedReason = ""

			if err := c.stateManager.SaveUpgradeState(ctx, cluster, state); err != nil {
				return 30 * time.Second, true, fmt.Errorf("failed to save resumed state: %w", err)
			}
		}
	}

	// Clear resume annotation
	if state.UpgradeType == UpgradeTypeTalos {
		_ = c.upgradeService.ResumeTalosUpgrade(ctx, cluster)
	} else {
		_ = c.upgradeService.ResumeKubernetesUpgrade(ctx, cluster)
	}

	// Continue upgrade
	return c.continueUpgrade(ctx, cluster, clientConfig, state)
}

// handleUpgradeResult processes the result of an upgrade step
func (c *UpgradeController) handleUpgradeResult(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	result *RollingUpgradeResult,
	upgradeType UpgradeType,
) (time.Duration, bool, error) {
	if result.Error != nil {
		vlog.Error(fmt.Sprintf("Upgrade failed: %s", result.Message), result.Error)

		if upgradeType == UpgradeTypeTalos {
			_ = c.upgradeService.FailTalosUpgrade(ctx, cluster, result.Error.Error())
		} else {
			_ = c.upgradeService.FailKubernetesUpgrade(ctx, cluster, result.Error.Error())
		}
		_ = c.statusManager.SetPhase(ctx, cluster, status.PhaseUpgradeFailed)

		return 30 * time.Second, true, nil
	}

	// Completion is now handled by handleCompletedUpgrade when phase is UpgradePhaseCompleted
	// This handles the case where result.Completed is returned directly from the executor
	if result.Completed {
		// Requeue immediately to let handleCompletedUpgrade process the completion
		return 1 * time.Second, true, nil
	}

	if result.Continue {
		return result.RequeueAfter, true, nil
	}

	return 0, false, nil
}

// buildNodeLists creates NodeUpgradeState slices from machines
func (c *UpgradeController) buildNodeLists(machines []vitistackv1alpha1.Machine) (controlPlanes, workers []NodeUpgradeState) {
	for i := range machines {
		m := &machines[i]
		ip := nodehelper.GetFirstIPv4(m)
		if ip == "" {
			vlog.Warn(fmt.Sprintf("Machine %s has no IPv4 address, skipping", m.Name))
			continue
		}

		nodeState := NodeUpgradeState{
			NodeName: m.Name,
			IP:       ip,
		}

		if m.Labels[vitistackv1alpha1.NodeRoleAnnotation] == controlPlaneRole {
			nodeState.Role = controlPlaneRole
			controlPlanes = append(controlPlanes, nodeState)
		} else {
			nodeState.Role = "worker"
			workers = append(workers, nodeState)
		}
	}

	return controlPlanes, workers
}

// buildTalosInstallerImage constructs the installer image URL
func (c *UpgradeController) buildTalosInstallerImage(version string) string {
	// Ensure version has 'v' prefix
	if len(version) > 0 && version[0] != 'v' {
		version = "v" + version
	}

	// Use official Talos installer
	return fmt.Sprintf("ghcr.io/siderolabs/installer:%s", version)
}

// GetStateManager returns the upgrade state manager for external access
func (c *UpgradeController) GetStateManager() *UpgradeStateManager {
	return c.stateManager
}

// GetReadinessChecker returns the node readiness checker for external access
func (c *UpgradeController) GetReadinessChecker() *NodeReadinessChecker {
	return c.readinessChecker
}
