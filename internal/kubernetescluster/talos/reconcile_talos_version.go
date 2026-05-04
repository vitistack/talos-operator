package talos

import (
	"context"
	"fmt"
	"strings"
	"time"

	semver "github.com/Masterminds/semver/v3"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"

	"github.com/vitistack/talos-operator/pkg/consts"
)

// defaultVersionEnforceCooldown is the fallback when
// TALOS_VERSION_ENFORCE_COOLDOWN_MINUTES is unset or invalid. Covers Talos
// reboot + cordon/drain + API recovery for the upgraded node; once it
// elapses with the node still on the wrong version the operator
// re-triggers (assumed prior upgrade failed).
const defaultVersionEnforceCooldown = 5 * time.Minute

// versionEnforceCooldown returns the configured per-node cooldown duration.
// Reads TALOS_VERSION_ENFORCE_COOLDOWN_MINUTES via viper; values < 1 fall
// back to the default.
func versionEnforceCooldown() time.Duration {
	mins := viper.GetInt(consts.TALOS_VERSION_ENFORCE_COOLDOWN_MINUTES)
	if mins < 1 {
		return defaultVersionEnforceCooldown
	}
	return time.Duration(mins) * time.Minute
}

// reconcileTalosVersion brings every node's running Talos version into
// agreement with the operator's desired version (TALOS_VERSION env var).
// One node is upgraded per pass so the cluster rolls under the
// rolling-upgrade pacing rather than restarting everything at once.
//
// # Source-of-truth model
//
// The operator's annotation/secret bookkeeping has historically been the
// authoritative version record, but reality can diverge from it (a
// previously-attempted upgrade that left the annotation reading
// "completed" while a node actually rebooted onto an older partition; a
// Talos auto-rollback after a failed boot that the operator never
// re-checked). This reconciler treats the running node — queried via
// the Talos Version API — as ground truth and the operator config as
// intent. Any disagreement is resolved by re-issuing the Talos upgrade
// RPC with the cluster's pinned installer image (so system extensions
// are preserved).
//
// Skip rules:
//   - feature flag TALOS_VERSION_ENFORCE_ENABLED=false disables the pass entirely;
//   - desired version (TALOS_VERSION) unset → no intent, no action;
//   - upgrade_in_progress=true → the rolling-upgrade flow owns the cluster;
//   - any node runs a version newer than desired → never downgrade. We
//     warn and surface a TalosVersionEnforcement=True/Downgrade condition
//     so it's visible without touching the cluster.
//
// Order: control planes first (so the API stays available longest, same
// reasoning as reconcileExtensions but inverted relative to the rolling
// upgrade — that one upgrades workers first because upgrades reboot
// nodes, but here we want the *intent* on the CPs first so the
// machine-config resolution path always finds an authoritative source).
//
//nolint:gocognit,gocyclo,funlen // single linear flow with explicit branches
func (t *TalosManager) reconcileTalosVersion(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	if !viper.GetBool(consts.TALOS_VERSION_ENFORCE_ENABLED) {
		return nil
	}

	desired := viper.GetString(consts.TALOS_VERSION)
	if desired == "" {
		return nil // no intent
	}
	desired = consts.NormalizeTalosVersion(desired) // "v1.12.7"

	desiredSemver, err := semver.NewVersion(consts.StripVersionPrefix(desired))
	if err != nil {
		return fmt.Errorf("invalid TALOS_VERSION %q: %w", desired, err)
	}

	clusterTag := clusterLogTag(cluster)

	// Don't fight the rolling-upgrade flow.
	if upState, err := t.stateService.GetUpgradeState(ctx, cluster); err == nil && upState != nil && upState.InProgress {
		vlog.Debug(fmt.Sprintf("Talos version enforcement skipped for %s: rolling upgrade in progress", clusterTag))
		return nil
	}

	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to list cluster machines: %w", err)
	}
	if len(machines) == 0 {
		return nil
	}

	clientConfig, err := t.GetTalosClientConfig(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to load talos client config for version enforcement on %s: %w", clusterTag, err)
	}

	// Backfill the cluster's pinned install_image once for every existing
	// cluster (Bug C). Without a pinned image, BuildTalosInstallerImage
	// can't resolve the right factory schematic for an upgrade; live-fetch
	// from a running CP recovers it for legacy clusters.
	t.ensureInstallImagePinned(ctx, cluster, machines, clientConfig)

	// Probe every reachable node and compare to desired. Nodes that are
	// unreachable surface as an error in actualByMachine and are skipped
	// (we can't trigger an upgrade on a node we can't reach anyway).
	actualByMachine := t.probeTalosVersions(ctx, cluster, machines, clientConfig)

	// Detect any node above desired → never downgrade, surface a condition.
	for _, m := range machines {
		actual, ok := actualByMachine[m.Name]
		if !ok || actual == "" {
			continue
		}
		actualSemver, perr := semver.NewVersion(consts.StripVersionPrefix(actual))
		if perr != nil {
			continue
		}
		if actualSemver.GreaterThan(desiredSemver) {
			msg := fmt.Sprintf("Node %s runs Talos %s which is newer than desired %s — refusing to downgrade", m.Name, actual, desired)
			vlog.Warn(msg)
			_ = t.statusManager.SetCondition(ctx, cluster, "TalosVersionEnforcement", "True", "Downgrade", msg)
			return nil
		}
	}

	// Pick a single enforcement target: control plane first, then workers.
	target := selectEnforcementTarget(machines, actualByMachine, desired)
	if target == nil {
		// Everyone matches — clear any previous enforcement condition AND
		// drop any stale per-node throttle records so the next mismatch
		// starts with a clean slate.
		_ = t.statusManager.SetCondition(ctx, cluster, "TalosVersionEnforcement", "False", "InSync",
			fmt.Sprintf("All nodes run Talos %s", desired))
		if cerr := t.stateService.ClearVersionEnforcementStates(ctx, cluster); cerr != nil {
			vlog.Debug(fmt.Sprintf("failed to clear version-enforcement records on in-sync for %s: %v", clusterTag, cerr))
		}
		return nil
	}

	// Resolve the installer image for THIS upgrade. Same resolution path
	// as user-driven upgrades: secret → live-fetch → env var. Never the
	// generic image — Talos rejects it on clusters with system extensions.
	installerImage := resolveEnforcementInstallerImage(ctx, t, cluster, target, clientConfig, desired)
	if installerImage == "" {
		msg := fmt.Sprintf("Cannot resolve Talos installer image to enforce version on %s: no install_image pinned, no control plane reachable to live-fetch, and TALOS_VM_INSTALL_IMAGE_* unset", clusterTag)
		vlog.Warn(msg)
		_ = t.statusManager.SetCondition(ctx, cluster, "TalosVersionEnforcement", "True", "ImageUnresolvable", msg)
		return nil
	}

	// Cooldown check: if we already triggered an enforcement upgrade for
	// THIS node with the SAME image and target version recently, skip the
	// RPC and wait for the node to come back. Without this, the 5s reconcile
	// loop spams the upgrade RPC 12 times/min while the node is still
	// rebooting/draining. (record absent / different image / different
	// target / cooldown elapsed → fall through and re-trigger.)
	cooldown := versionEnforceCooldown()
	enforceStates, _ := t.stateService.GetVersionEnforcementStates(ctx, cluster)
	if rec, ok := enforceStates[target.Name]; ok &&
		rec.Image == installerImage &&
		rec.TargetVersion == desired &&
		time.Since(rec.TriggeredAt) < cooldown {
		elapsed := time.Since(rec.TriggeredAt).Round(time.Second)
		remaining := (cooldown - time.Since(rec.TriggeredAt)).Round(time.Second)
		msg := fmt.Sprintf("Waiting for Talos upgrade to settle on %s/%s: running=%s target=%s initiated=%s ago (cooldown %s remaining)",
			clusterTag, target.Name, actualByMachine[target.Name], desired, elapsed, remaining)
		vlog.Info(msg)
		_ = t.statusManager.SetCondition(ctx, cluster, "TalosVersionEnforcement", "True", "WaitingForReboot", msg)
		return nil
	}

	// Cross-reconciler defer: if the extension reconciler recently triggered
	// a Talos upgrade for this node (with a different schematic to bring in
	// missing extensions), do not stack a second upgrade on top within
	// seconds. Wait for the extension upgrade to settle; on the next pass
	// the node will report the version baked into that schematic and either
	// match `desired` (no enforcement work) or still mismatch (we'll then
	// re-issue with the cluster's current schematic).
	if exCooldown := extensionUpgradeCooldown(); exCooldown > 0 {
		if exStates, err := t.stateService.GetExtensionUpgradeStates(ctx, cluster); err == nil {
			if exRec, ok := exStates[target.Name]; ok && !exRec.TriggeredAt.IsZero() {
				if since := time.Since(exRec.TriggeredAt); since < exCooldown {
					msg := fmt.Sprintf("Talos version enforcement deferred on %s/%s: extension reconciler triggered Talos upgrade %s ago with image=%s (cooldown %s)",
						clusterTag, target.Name, since.Round(time.Second), exRec.Image, exCooldown)
					vlog.Info(msg)
					_ = t.statusManager.SetCondition(ctx, cluster, "TalosVersionEnforcement", "True", "WaitingForExtensionUpgrade", msg)
					return nil
				}
			}
		}
	}

	targetIP := getFirstIPv4(target)
	if targetIP == "" {
		return nil
	}
	tClient, err := t.clientService.CreateTalosClient(ctx, false, clientConfig, []string{targetIP})
	if err != nil {
		return fmt.Errorf("failed to create direct Talos client to enforce version on %s/%s: %w", clusterTag, target.Name, err)
	}
	defer func() { _ = tClient.Close() }()

	vlog.Info(fmt.Sprintf("Talos version mismatch on %s/%s: running=%s desired=%s — triggering upgrade with %s",
		clusterTag, target.Name, actualByMachine[target.Name], desired, installerImage))

	if uerr := t.clientService.UpgradeNode(ctx, tClient, targetIP, installerImage); uerr != nil {
		msg := fmt.Sprintf("Failed to trigger enforcement upgrade on %s/%s: %v", clusterTag, target.Name, uerr)
		vlog.Error(msg, uerr)
		_ = t.statusManager.SetCondition(ctx, cluster, "TalosVersionEnforcement", "True", "UpgradeFailed", msg)
		return nil
	}

	// Persist the throttle record BEFORE returning so the next reconcile
	// (5s away) sees it and skips the RPC. Best-effort: a write failure
	// just means we'll re-trigger on the next pass — annoying but safe.
	if serr := t.stateService.SetVersionEnforcementState(ctx, cluster, target.Name, installerImage, desired); serr != nil {
		vlog.Warn(fmt.Sprintf("Failed to persist version-enforcement record for %s/%s: %v — will re-trigger next pass", clusterTag, target.Name, serr))
	}

	_ = t.statusManager.SetCondition(ctx, cluster, "TalosVersionEnforcement", "True", "Reconciling",
		fmt.Sprintf("Upgrading node %s from %s to %s", target.Name, actualByMachine[target.Name], desired))

	return nil
}

// ensureInstallImagePinned writes machine.install.image into the cluster
// secret when nothing is pinned yet, using the same resolution chain as
// the bootstrap path. Idempotent and best-effort: failures are logged
// and swallowed because version enforcement still works without a pinned
// image (BuildTalosInstallerImage will live-fetch on demand).
func (t *TalosManager) ensureInstallImagePinned(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	machines []*vitistackv1alpha1.Machine,
	clientConfig *clientconfig.Config,
) {
	if t.stateService == nil {
		return
	}
	if saved, _ := t.stateService.GetInstallImage(ctx, cluster); saved != "" {
		return
	}
	if len(machines) == 0 {
		return
	}
	resolved := t.resolveClusterInstallImage(ctx, cluster, machines[0], clientConfig)
	t.persistInstallImageIfMissing(ctx, cluster, resolved)
}

// probeTalosVersions returns a map[machineName] -> running Talos version
// (e.g. "v1.12.4"). Unreachable nodes are omitted. One Talos client is
// reused per cluster — connecting straight to each node's IP — so a
// degraded API server doesn't block the probe.
func (t *TalosManager) probeTalosVersions(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	machines []*vitistackv1alpha1.Machine,
	clientConfig *clientconfig.Config,
) map[string]string {
	out := make(map[string]string, len(machines))
	clusterTag := clusterLogTag(cluster)
	for _, m := range machines {
		ip := getFirstIPv4(m)
		if ip == "" {
			continue
		}
		client, err := t.clientService.CreateTalosClient(ctx, false, clientConfig, []string{ip})
		if err != nil {
			vlog.Debug(fmt.Sprintf("Talos version probe skipped %s/%s: %v", clusterTag, m.Name, err))
			continue
		}
		ver, verr := t.clientService.GetTalosVersion(ctx, client, ip)
		_ = client.Close()
		if verr != nil {
			vlog.Debug(fmt.Sprintf("Talos version probe failed %s/%s: %v", clusterTag, m.Name, verr))
			continue
		}
		out[m.Name] = ver
	}
	return out
}

// resolveEnforcementInstallerImage returns the installer image to use
// when enforcing the desired Talos version on a node. Resolution order
// matches upgradeservice.BuildTalosInstallerImage:
//
//  1. The cluster's pinned install_image (secret) with tag rewritten to
//     desiredVersion. Preserves a factory schematic so system extensions
//     survive the upgrade.
//  2. Live-fetch machine.install.image from a running control plane,
//     same tag rewrite. Covers legacy clusters that never had
//     install_image persisted.
//  3. Per-provider TALOS_VM_INSTALL_IMAGE_* env var, falling back to
//     TALOS_VM_INSTALL_IMAGE_DEFAULT, same tag rewrite.
//
// Returns "" if none resolve. The caller must NOT fall through to a
// generic ghcr.io/siderolabs/installer image — Talos rejects upgrades
// onto an installer that lacks the cluster's currently-installed
// extensions, with a confusing "file exists" validation error that
// strands the upgrade state machine.
func resolveEnforcementInstallerImage(
	ctx context.Context,
	t *TalosManager,
	cluster *vitistackv1alpha1.KubernetesCluster,
	machine *vitistackv1alpha1.Machine,
	clientConfig *clientconfig.Config,
	desiredVersion string,
) string {
	versionTag := consts.NormalizeTalosVersion(desiredVersion)
	base := t.resolveClusterInstallImage(ctx, cluster, machine, clientConfig)
	if base == "" {
		return ""
	}
	return swapImageTagForEnforcement(base, versionTag)
}

// swapImageTagForEnforcement returns ref with its tag replaced by tag.
// If ref has no tag, tag is appended. Anchored after the last "/" so a
// registry port (e.g. "localhost:5000/foo") is not mistaken for a tag
// separator.
//
// Local copy of upgradeservice.swapImageTag — the two packages are at
// the same architectural level and we'd rather duplicate ten lines than
// let one reach into the other's internals.
func swapImageTagForEnforcement(ref, tag string) string {
	slash := strings.LastIndexByte(ref, '/')
	colon := strings.LastIndexByte(ref, ':')
	if colon > slash {
		return ref[:colon] + ":" + tag
	}
	return ref + ":" + tag
}

// selectEnforcementTarget returns the first machine that needs an
// upgrade, or nil if every reachable node matches desired. Order:
// control planes first, then workers. Within a role the input order is
// preserved so callers can sort upstream if they want stable picks.
func selectEnforcementTarget(
	machines []*vitistackv1alpha1.Machine,
	actual map[string]string,
	desired string,
) *vitistackv1alpha1.Machine {
	pickFromRole := func(role string) *vitistackv1alpha1.Machine {
		for _, m := range machines {
			if m.Labels[vitistackv1alpha1.NodeRoleAnnotation] != role {
				continue
			}
			running, ok := actual[m.Name]
			if !ok || running == "" {
				continue // unreachable — leave alone, retry next pass
			}
			if running != desired {
				return m
			}
		}
		return nil
	}
	if cp := pickFromRole(controlPlaneRole); cp != nil {
		return cp
	}
	// Workers: anything that isn't control-plane.
	for _, m := range machines {
		if m.Labels[vitistackv1alpha1.NodeRoleAnnotation] == controlPlaneRole {
			continue
		}
		running, ok := actual[m.Name]
		if !ok || running == "" {
			continue
		}
		if running != desired {
			return m
		}
	}
	return nil
}
