package upgradeservice

import (
	"context"
	"fmt"
	"strings"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/spf13/viper"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	"github.com/vitistack/talos-operator/pkg/consts"
)

// Defaults for the upgrade pacing settings, used when a setting is unset or
// below its minimum. They match internal/settings.
const (
	defaultNodeUpgradeTimeout  = 15 * time.Minute
	defaultEtcdHealthyTimeout  = 5 * time.Minute
	defaultNodeSettleDuration  = 60 * time.Second
	etcdHealthyRecheckInterval = 20 * time.Second
	// nodeProbeTimeout bounds one round of Talos API reads against a node
	// that is rebooting or booting, so a node that accepts the connection and
	// never answers does not hold the reconcile worker.
	nodeProbeTimeout = 20 * time.Second
)

// nodeUpgradeTimeout reads UPGRADE_NODE_TIMEOUT_MINUTES; values < 1 fall back
// to the default.
func nodeUpgradeTimeout() time.Duration {
	if mins := viper.GetInt(consts.UPGRADE_NODE_TIMEOUT_MINUTES); mins >= 1 {
		return time.Duration(mins) * time.Minute
	}
	return defaultNodeUpgradeTimeout
}

// etcdHealthyTimeout reads UPGRADE_ETCD_HEALTHY_TIMEOUT_MINUTES; values < 1
// fall back to the default.
func etcdHealthyTimeout() time.Duration {
	if mins := viper.GetInt(consts.UPGRADE_ETCD_HEALTHY_TIMEOUT_MINUTES); mins >= 1 {
		return time.Duration(mins) * time.Minute
	}
	return defaultEtcdHealthyTimeout
}

// nodeSettleDuration reads UPGRADE_NODE_SETTLE_SECONDS. 0 disables the pause;
// unset or negative falls back to the default.
func nodeSettleDuration() time.Duration {
	if !viper.IsSet(consts.UPGRADE_NODE_SETTLE_SECONDS) {
		return defaultNodeSettleDuration
	}
	if secs := viper.GetInt(consts.UPGRADE_NODE_SETTLE_SECONDS); secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	return defaultNodeSettleDuration
}

// etcdHealthCheck returns nil when etcd is healthy on every given control
// plane, or an error naming each one where it is not.
type etcdHealthCheck func(ctx context.Context, clientConfig *clientconfig.Config, controlPlaneIPs []string) error

// talosEtcdHealthCheck is the Talos API implementation of etcdHealthCheck: the
// etcd service must be Running and healthy on each control plane. Talos marks
// it healthy only while the local member answers, so a member that has not
// rejoined after a reboot shows up here.
func talosEtcdHealthCheck(clientService *talosclientservice.TalosClientService) etcdHealthCheck {
	return func(ctx context.Context, clientConfig *clientconfig.Config, controlPlaneIPs []string) error {
		if len(controlPlaneIPs) == 0 {
			return fmt.Errorf("no control plane with an IPv4 address to check etcd on")
		}
		if clientConfig == nil {
			return fmt.Errorf("no Talos client config to check etcd health")
		}

		var issues []string
		for _, ip := range controlPlaneIPs {
			if err := checkEtcdOnNode(ctx, clientService, clientConfig, ip); err != nil {
				issues = append(issues, err.Error())
			}
		}
		if len(issues) > 0 {
			return fmt.Errorf("etcd is not healthy: %s", strings.Join(issues, "; "))
		}
		return nil
	}
}

func checkEtcdOnNode(ctx context.Context, clientService *talosclientservice.TalosClientService, clientConfig *clientconfig.Config, ip string) error {
	ctx, cancel := context.WithTimeout(ctx, extensionProbeTimeout)
	defer cancel()

	client, err := clientService.CreateTalosClient(ctx, false, clientConfig, []string{ip})
	if err != nil {
		return fmt.Errorf("%s: %w", ip, err)
	}
	defer func() { _ = client.Close() }()

	healthy, err := clientService.IsEtcdHealthy(ctx, client, ip)
	switch {
	case err != nil:
		return fmt.Errorf("%s: %w", ip, err)
	case !healthy:
		return fmt.Errorf("%s: etcd not running and healthy", ip)
	}
	return nil
}

// controlPlaneIPs returns the IPs of the plan's control planes.
func (state *ClusterUpgradeState) controlPlaneIPs() []string {
	ips := make([]string, 0, state.ControlPlaneCount)
	for i := 0; i < state.ControlPlaneCount && i < len(state.Nodes); i++ {
		ips = append(ips, state.Nodes[i].IP)
	}
	return ips
}

// nodeStartedAt is when the operator started on the node. Plans written before
// StartedAt existed only carry InitiatedAt.
func (n *NodeUpgradeState) nodeStartedAt() time.Time {
	if !n.StartedAt.IsZero() {
		return n.StartedAt
	}
	return n.InitiatedAt
}

// nodeTimedOut reports whether a node still on its way back has run past
// timeout since the operator started on it, and which step it is stuck in.
func nodeTimedOut(n *NodeUpgradeState, upgradeType UpgradeType, now time.Time, timeout time.Duration) (string, bool) {
	started := n.nodeStartedAt()
	if started.IsZero() || n.NodeReady || now.Sub(started) <= timeout {
		return "", false
	}

	var step string
	switch {
	case !n.UpgradeInitiated:
		step = "starting the upgrade"
	case !n.UpgradeCompleted && upgradeType == UpgradeTypeTalos:
		step = "waiting for the node to reboot into the target version"
	case !n.UpgradeCompleted:
		step = "waiting for the kubelet to report the target version"
	default:
		step = "waiting for the node to become Ready"
	}
	return step, true
}

// talosNodeProgress is what a node mid Talos upgrade turned out to be doing.
type talosNodeProgress int

const (
	// talosNodePending: not rebooted yet, or rebooted into the target and
	// still booting.
	talosNodePending talosNodeProgress = iota
	// talosNodeOnTarget: rebooted and running the target version.
	talosNodeOnTarget
	// talosNodeRolledBack: rebooted, but into a version other than the target.
	// Talos falls back to its previous install when the new one does not boot.
	talosNodeRolledBack
)

// classifyTalosNode decides how far a node has got from its boot ID before the
// upgrade, its boot ID now and the version it runs. An unknown boot ID (empty,
// e.g. it could not be read) leaves the decision to the version alone, as
// before boot IDs were recorded.
func classifyTalosNode(bootIDBefore, bootIDNow, runningVersion, targetVersion string) talosNodeProgress {
	onTarget := sameVersion(runningVersion, targetVersion)
	known := bootIDBefore != "" && bootIDNow != ""
	rebooted := known && bootIDBefore != bootIDNow

	switch {
	case onTarget && (rebooted || !known):
		return talosNodeOnTarget
	case rebooted:
		return talosNodeRolledBack
	default:
		return talosNodePending
	}
}

// failedPlanBlocksStart reports whether a stored failed plan must keep a new
// upgrade of upgradeType to target from starting. A failed upgrade waits for
// resume or reset instead of starting over from the first node (which would
// drain and upgrade the nodes it already finished), and keeps an upgrade of
// the other kind from starting on a half-upgraded cluster. A new target of the
// same kind replaces the failed plan.
func failedPlanBlocksStart(state *ClusterUpgradeState, upgradeType UpgradeType, target string) bool {
	if state == nil || !state.IsFailed() {
		return false
	}
	return state.UpgradeType != upgradeType || sameVersion(state.TargetVersion, target)
}
