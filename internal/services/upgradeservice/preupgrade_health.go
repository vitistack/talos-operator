package upgradeservice

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/helpers/clusterlog"
	"github.com/vitistack/talos-operator/internal/services/talosstateservice"
)

// preUpgradeHealth is the outcome of probing a cluster's nodes before an upgrade.
type preUpgradeHealth struct {
	ControlPlaneReady bool     // Talos API reachable on the probed control plane
	EtcdHealthy       bool     // etcd reports running and healthy
	NodesReady        bool     // Talos API reachable on every node
	Issues            []string // one entry per failed check
}

// nodeHealthCheck probes controlPlaneIP (Talos API and etcd) and nodeIPs
// (Talos API) and reports what it found.
type nodeHealthCheck func(ctx context.Context, clientConfig *clientconfig.Config, controlPlaneIP string, nodeIPs []string) preUpgradeHealth

// RecordPreUpgradeHealth checks the cluster's nodes as an upgrade starts and
// records the result in the cluster secret's health_check_* keys. The result
// is informational: a failed check is logged and recorded, and the caller
// decides what to do with it.
func (s *UpgradeService) RecordPreUpgradeHealth(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	controlPlanes, workers []NodeUpgradeState,
) *talosstateservice.HealthCheckState {
	nodeIPs := make([]string, 0, len(controlPlanes)+len(workers))
	for _, n := range controlPlanes {
		nodeIPs = append(nodeIPs, n.IP)
	}
	for _, n := range workers {
		nodeIPs = append(nodeIPs, n.IP)
	}

	var health preUpgradeHealth
	if len(controlPlanes) == 0 {
		health.Issues = []string{"no control plane with an IPv4 address to check"}
	} else {
		health = s.healthCheck(ctx, clientConfig, controlPlanes[0].IP, nodeIPs)
	}
	return s.recordHealth(ctx, cluster, health)
}

// recordHealth turns a probe result into the persisted health check state.
func (s *UpgradeService) recordHealth(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, health preUpgradeHealth) *talosstateservice.HealthCheckState {
	state := &talosstateservice.HealthCheckState{
		Passed:            len(health.Issues) == 0,
		EtcdHealthy:       health.EtcdHealthy,
		NodesReady:        health.NodesReady,
		ControlPlaneReady: health.ControlPlaneReady,
		Message:           "All health checks passed",
	}
	if !state.Passed {
		state.Message = "Health check failed: " + strings.Join(health.Issues, "; ")
		vlog.Warn(fmt.Sprintf("Pre-upgrade health check failed %s: %s", clusterlog.Tag(cluster), state.Message))
	} else {
		vlog.Info(fmt.Sprintf("Pre-upgrade health check passed %s", clusterlog.Tag(cluster)))
	}

	if s.stateService != nil {
		if err := s.stateService.SetHealthCheckState(ctx, cluster, state); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to persist health check state %s: %v", clusterlog.Tag(cluster), err))
		}
	}
	return state
}

// checkNodeHealth is the Talos API implementation of nodeHealthCheck.
func (s *UpgradeService) checkNodeHealth(
	ctx context.Context,
	clientConfig *clientconfig.Config,
	controlPlaneIP string,
	nodeIPs []string,
) preUpgradeHealth {
	var health preUpgradeHealth

	if s.clientService.IsTalosAPIReachable(controlPlaneIP) {
		health.ControlPlaneReady = true
	} else {
		health.Issues = append(health.Issues, fmt.Sprintf("Talos API not reachable on control plane %s", controlPlaneIP))
	}

	if clientConfig == nil {
		health.Issues = append(health.Issues, "no Talos client config to check etcd health")
	} else if client, err := s.clientService.CreateTalosClient(ctx, false, clientConfig, []string{controlPlaneIP}); err != nil {
		health.Issues = append(health.Issues, fmt.Sprintf("Failed to create Talos client: %v", err))
	} else {
		// A node whose API port accepts connections without answering would
		// otherwise hold this reconcile worker for as long as it takes.
		etcdCtx, cancel := context.WithTimeout(ctx, extensionProbeTimeout)
		healthy, etcdErr := s.clientService.IsEtcdHealthy(etcdCtx, client, controlPlaneIP)
		cancel()
		_ = client.Close()
		switch {
		case etcdErr != nil:
			health.Issues = append(health.Issues, fmt.Sprintf("Failed to check etcd health: %v", etcdErr))
		case !healthy:
			health.Issues = append(health.Issues, "etcd cluster is not healthy")
		default:
			health.EtcdHealthy = true
		}
	}

	// Dialled in parallel: each unreachable node costs a full dial timeout,
	// and this runs before every upgrade.
	unreachable := make([]string, len(nodeIPs))
	var wg sync.WaitGroup
	for i, ip := range nodeIPs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !s.clientService.IsTalosAPIReachable(ip) {
				unreachable[i] = ip
			}
		}()
	}
	wg.Wait()
	unreachable = slices.DeleteFunc(unreachable, func(ip string) bool { return ip == "" })
	if len(unreachable) > 0 {
		health.Issues = append(health.Issues, fmt.Sprintf("Talos API not reachable on nodes: %v", unreachable))
	} else {
		health.NodesReady = true
	}

	return health
}
