package talos

import (
	"context"
	"fmt"
	"github.com/vitistack/talos-operator/internal/helpers/imageref"
	"strings"
	"time"

	semver "github.com/Masterminds/semver/v3"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"

	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	"github.com/vitistack/talos-operator/internal/services/talosversion"
	"github.com/vitistack/talos-operator/pkg/consts"
)

// defaultVersionEnforceCooldown is the fallback when
// TALOS_VERSION_ENFORCE_COOLDOWN_MINUTES is unset or invalid. Covers Talos
// reboot + cordon/drain + API recovery for the upgraded node; once it
// elapses with the node still on the wrong version the operator
// re-triggers (assumed prior upgrade failed).
const defaultVersionEnforceCooldown = 5 * time.Minute

// talosNodeProbeTimeout bounds a single per-node Talos API round trip (version
// probe, extension query). During bring-up a node's API port is often
// TCP-reachable before the gRPC/mTLS server is actually serving; without a
// deadline the call inherits the reconcile context (which has none) and
// blocks, stalling the whole reconcile. 8s sits comfortably above a healthy
// handshake + round trip while still failing fast on a stuck node.
const talosNodeProbeTimeout = talosclientservice.VersionProbeTimeout

// driftRecoveryInterval throttles the Talos-version and extension drift
// recovery passes. They reconcile a node's running state back toward intent
// and only matter on a settled cluster, so re-probing every node on the 5s
// control loop is wasteful. Running them at most this often keeps steady-state
// node API traffic low without meaningfully delaying recovery — any resulting
// upgrade is paced by the minutes-long per-node cooldowns regardless.
const driftRecoveryInterval = 90 * time.Second

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
// agreement with the cluster's desired version, read from the per-cluster
// upgrade.vitistack.io/talos-target annotation (the future spec field) —
// never from a global default. One node is upgraded per pass so the
// cluster rolls under the rolling-upgrade pacing rather than restarting
// everything at once.
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
//   - feature flag TALOS_VERSION_ENFORCE_ENABLED=false disables the pass entirely (default);
//   - talos-target annotation unset → no intent for this cluster, no action;
//     in both cases a TalosVersionEnforcement condition left by an earlier
//     pass is set to False with the reason, so it stops reporting a stale
//     comparison;
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
	clusterTag := clusterLogTag(cluster)

	// Desired version is the per-cluster intent expressed via the
	// upgrade.vitistack.io/talos-target annotation (the future spec field).
	// An absent annotation means no intent for THIS cluster, so we never
	// act: the operator must not auto-upgrade clusters from a global
	// default. We never downgrade — a target below a node's running
	// version is rejected by the GreaterThan guard below.
	desired := strings.TrimSpace(cluster.GetAnnotations()[consts.TalosTargetAnnotation])
	if reason, message, skip := versionEnforcementSkip(viper.GetBool(consts.TALOS_VERSION_ENFORCE_ENABLED), desired); skip {
		t.retireEnforcementCondition(ctx, cluster, reason, message)
		return nil
	}
	desired = consts.NormalizeTalosVersion(desired) // "v1.12.7"

	desiredSemver, err := semver.NewVersion(consts.StripVersionPrefix(desired))
	if err != nil {
		return fmt.Errorf("invalid %s=%q on %s: %w", consts.TalosTargetAnnotation, desired, clusterTag, err)
	}

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
	actualByMachine := t.probeTalosVersions(ctx, machines, clientConfig)

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
			_ = t.statusManager.SetCondition(ctx, cluster, enforcementConditionType, "True", "Downgrade", msg)
			return nil
		}
	}

	// Walk towards desired one supported step at a time: latest patch of the
	// running minor, then of each minor in between, then desired.
	step, reason, msg := enforcementStep(machines, actualByMachine, desired, cluster.GetAnnotations()[consts.KubernetesCurrentAnnotation])
	if reason != "" {
		vlog.Warn(fmt.Sprintf("%s %s", msg, clusterTag))
		_ = t.statusManager.SetCondition(ctx, cluster, enforcementConditionType, "True", reason, msg)
		return nil
	}

	// Pick a single enforcement target: control plane first, then workers.
	target := selectEnforcementTarget(machines, actualByMachine, step)
	if target == nil {
		// Everyone matches — clear any previous enforcement condition AND
		// drop any stale per-node throttle records so the next mismatch
		// starts with a clean slate.
		_ = t.statusManager.SetCondition(ctx, cluster, enforcementConditionType, "False", "InSync",
			fmt.Sprintf("All nodes run Talos %s", desired))
		if cerr := t.stateService.ClearVersionEnforcementStates(ctx, cluster); cerr != nil {
			vlog.Debug(fmt.Sprintf("failed to clear version-enforcement records on in-sync for %s: %v", clusterTag, cerr))
		}
		return nil
	}

	// Resolve the installer image for THIS upgrade. Same resolution path
	// as user-driven upgrades: secret → live-fetch → env var. Never the
	// generic image — Talos rejects it on clusters with system extensions.
	installerImage := resolveEnforcementInstallerImage(ctx, t, cluster, target, clientConfig, step)
	if installerImage == "" {
		msg := fmt.Sprintf("Cannot resolve Talos installer image to enforce version on %s: no install_image pinned, no control plane reachable to live-fetch, and TALOS_VM_INSTALL_IMAGE_* unset", clusterTag)
		vlog.Warn(msg)
		_ = t.statusManager.SetCondition(ctx, cluster, enforcementConditionType, "True", "ImageUnresolvable", msg)
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
		rec.TargetVersion == step &&
		time.Since(rec.TriggeredAt) < cooldown {
		elapsed := time.Since(rec.TriggeredAt).Round(time.Second)
		remaining := (cooldown - time.Since(rec.TriggeredAt)).Round(time.Second)
		msg := fmt.Sprintf("Waiting for Talos upgrade to settle on %s/%s: running=%s target=%s initiated=%s ago (cooldown %s remaining)",
			clusterTag, target.Name, actualByMachine[target.Name], step, elapsed, remaining)
		vlog.Info(msg)
		_ = t.statusManager.SetCondition(ctx, cluster, enforcementConditionType, "True", "WaitingForReboot", msg)
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
					_ = t.statusManager.SetCondition(ctx, cluster, enforcementConditionType, "True", "WaitingForExtensionUpgrade", msg)
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

	vlog.Info(fmt.Sprintf("Talos version mismatch on %s/%s: running=%s desired=%s step=%s — triggering upgrade with %s",
		clusterTag, target.Name, actualByMachine[target.Name], desired, step, installerImage))

	if uerr := t.clientService.UpgradeNode(ctx, tClient, targetIP, installerImage); uerr != nil {
		msg := fmt.Sprintf("Failed to trigger enforcement upgrade on %s/%s: %v", clusterTag, target.Name, uerr)
		vlog.Error(msg, uerr)
		_ = t.statusManager.SetCondition(ctx, cluster, enforcementConditionType, "True", "UpgradeFailed", msg)
		return nil
	}

	// Persist the throttle record BEFORE returning so the next reconcile
	// (5s away) sees it and skips the RPC. Best-effort: a write failure
	// just means we'll re-trigger on the next pass — annoying but safe.
	if serr := t.stateService.SetVersionEnforcementState(ctx, cluster, target.Name, installerImage, step); serr != nil {
		vlog.Warn(fmt.Sprintf("Failed to persist version-enforcement record for %s/%s: %v — will re-trigger next pass", clusterTag, target.Name, serr))
	}

	_ = t.statusManager.SetCondition(ctx, cluster, enforcementConditionType, "True", "Reconciling",
		fmt.Sprintf("Upgrading node %s from %s to %s (towards %s)", target.Name, actualByMachine[target.Name], step, desired))

	return nil
}

// enforcementConditionType is the cluster condition reconcileTalosVersion reports on.
const enforcementConditionType = "TalosVersionEnforcement"

// versionEnforcementSkip reports whether reconcileTalosVersion has nothing to
// enforce for a cluster, with a condition reason and message saying why.
func versionEnforcementSkip(enabled bool, target string) (reason, message string, skip bool) {
	if !enabled {
		return "Disabled", "Talos version enforcement is disabled (" + consts.TALOS_VERSION_ENFORCE_ENABLED + "=false)", true
	}
	if strings.TrimSpace(target) == "" {
		return "NoTarget", "Talos version is not enforced: no " + consts.TalosTargetAnnotation + " annotation", true
	}
	return "", "", false
}

// retireEnforcementCondition replaces a TalosVersionEnforcement condition
// left by an earlier pass once enforcement no longer runs, so it stops
// reporting a comparison that no longer applies. Clusters that never had the
// condition are left alone.
func (t *TalosManager) retireEnforcementCondition(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, reason, message string) {
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == enforcementConditionType {
			_ = t.statusManager.SetCondition(ctx, cluster, enforcementConditionType, "False", reason, message)
			return
		}
	}
}

// refreshTalosCurrentVersion keeps the upgrade.vitistack.io/talos-current
// annotation aligned with the version actually running on the cluster's nodes,
// independent of the version-enforcement feature flag. The orchestrated upgrade
// flow maintains this annotation for user-driven upgrades, but an enforced (or
// otherwise out-of-band) upgrade changes the running version without going
// through it; this closes that gap so upgrade-availability reporting stays
// truthful. Best-effort and runs on the throttled drift-recovery cadence.
func (t *TalosManager) refreshTalosCurrentVersion(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) {
	if t.upgradeService == nil {
		return
	}
	running := t.runningTalosVersion(ctx, cluster)
	if running == "" {
		return
	}
	if err := t.upgradeService.ReconcileTalosCurrentVersion(ctx, cluster, running); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to refresh talos-current for %s: %v", clusterLogTag(cluster), err))
	}
}

// refreshInstallImageSchematic moves the cluster's pinned install image onto
// the operator's configured schematic when its nodes lack a required system
// extension, so nodes added later and the next upgrade install the full set.
// Independent of the enforcement flags: it changes only the pin, never a node.
// Best-effort and runs on the throttled drift-recovery cadence.
func (t *TalosManager) refreshInstallImageSchematic(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) {
	if t.upgradeService == nil {
		return
	}
	clientConfig, err := t.GetTalosClientConfig(ctx, cluster)
	if err != nil {
		return
	}
	if _, err := t.upgradeService.ReconcileInstallImageSchematic(ctx, cluster, clientConfig); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to refresh install image schematic for %s: %v", clusterLogTag(cluster), err))
	}
}

// runningTalosVersion returns the lowest Talos version observed across the
// cluster's reachable nodes (normalized, e.g. "v1.13.9"), or "" if none are
// reachable. The minimum is used so talos-current only advances once every
// node has reached the new version (mid-rolling-upgrade it reflects the
// least-upgraded node).
func (t *TalosManager) runningTalosVersion(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) string {
	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil || len(machines) == 0 {
		return ""
	}
	clientConfig, err := t.GetTalosClientConfig(ctx, cluster)
	if err != nil {
		return ""
	}
	return talosversion.LowestVersion(t.probeTalosVersions(ctx, machines, clientConfig))
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
	machines []*vitistackv1alpha1.Machine,
	clientConfig *clientconfig.Config,
) map[string]string {
	nodes := make([]talosclientservice.NodeEndpoint, 0, len(machines))
	for _, m := range machines {
		nodes = append(nodes, talosclientservice.NodeEndpoint{Name: m.Name, IP: getFirstIPv4(m)})
	}
	return t.clientService.ProbeTalosVersions(ctx, clientConfig, nodes, talosNodeProbeTimeout)
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
	return imageref.WithTag(base, versionTag)
}

// selectEnforcementTarget returns the first machine running a version older
// than desired, or nil if every reachable node has reached it. A node already
// past desired (ahead of an intermediate step) is never picked: that would be
// a downgrade. Order:
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
			if talosversion.IsOlder(running, desired) {
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
		if talosversion.IsOlder(running, desired) {
			return m
		}
	}
	return nil
}

// enforcementStep returns the version to enforce on this pass: the next step on
// the supported upgrade path from the least-upgraded node towards desired,
// normalized ("v1.12.12"). Every node is brought to that step before any goes
// further, so the cluster never spans more than one step.
//
// When no step may be taken it returns a condition reason and message instead:
// WaitingForNodes while a machine with an address has not reported its version
// (it may be rebooting into the current step, or rolled back to an older one,
// and stepping on without it could leave it a minor behind), PathUnknown when
// the latest patch of a minor on the way is not known, and
// KubernetesIncompatible when the step's Talos cannot run the cluster's
// Kubernetes version.
func enforcementStep(machines []*vitistackv1alpha1.Machine, actual map[string]string, desired, kubernetesVersion string) (step, reason, message string) {
	var silent []string
	for _, m := range machines {
		if getFirstIPv4(m) == "" {
			continue // not reachable at all yet; not part of any upgrade
		}
		if v, ok := actual[m.Name]; !ok || !talosversion.Valid(v) {
			silent = append(silent, m.Name) // no answer, or one that does not parse
		}
	}
	if len(silent) > 0 {
		return "", "WaitingForNodes", fmt.Sprintf("Waiting to enforce Talos %s: no version reported by %s", desired, strings.Join(silent, ", "))
	}

	lowest := talosversion.LowestVersion(actual)
	if lowest == "" {
		return desired, "", ""
	}
	next, err := talosversion.NextUpgradeStep(lowest, desired)
	if err != nil {
		return "", "PathUnknown", fmt.Sprintf("Cannot enforce Talos %s: %v", desired, err)
	}
	step = desired
	if next != "" {
		step = consts.NormalizeTalosVersion(next)
	}
	if ok, why := talosversion.SupportsKubernetesVersion(step, kubernetesVersion); !ok {
		return "", "KubernetesIncompatible", fmt.Sprintf("Cannot enforce Talos %s: the next step %s cannot run Kubernetes %s (%s); upgrade Kubernetes first",
			desired, step, kubernetesVersion, why)
	}
	return step, "", ""
}
