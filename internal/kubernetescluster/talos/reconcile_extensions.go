package talos

import (
	"context"
	"fmt"
	"strings"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"

	"github.com/vitistack/talos-operator/internal/services/talosstateservice"
	"github.com/vitistack/talos-operator/pkg/consts"
)

// defaultExtensionUpgradeCooldown is the fallback when
// TALOS_EXTENSION_COOLDOWN_MINUTES is unset or invalid. The window must cover
// Talos reboot + extension unpack + API recovery; once it elapses with the
// node still missing extensions, the operator re-triggers (assumed prior
// upgrade failed).
const defaultExtensionUpgradeCooldown = 10 * time.Minute

// extensionUpgradeCooldown returns the configured per-node cooldown duration.
// Reads TALOS_EXTENSION_COOLDOWN_MINUTES via viper; values < 1 fall back to
// the default.
func extensionUpgradeCooldown() time.Duration {
	mins := viper.GetInt(consts.TALOS_EXTENSION_COOLDOWN_MINUTES)
	if mins < 1 {
		return defaultExtensionUpgradeCooldown
	}
	return time.Duration(mins) * time.Minute
}

// reconcileExtensions ensures every reachable Machine in the cluster has the
// system extensions named in the TALOS_REQUIRED_EXTENSIONS env var installed.
//
// Talos extensions are baked into the OS image — the operator cannot pull
// them onto a node post-install. The recovery path is therefore a Talos
// upgrade to the per-provider TALOS_VM_INSTALL_IMAGE_* image (DEFAULT as
// fallback), whose schematic must already contain the required extensions.
// Keeping TALOS_REQUIRED_EXTENSIONS in sync with the schematic baked into
// TALOS_VM_INSTALL_IMAGE_* is the developer's responsibility.
//
// Per-cluster, per-node throttling: each trigger is recorded in the cluster
// secret as {image, triggeredAt}. While the cooldown is active for the same
// image we leave the node alone (it's rebooting). If the operator changes
// TALOS_VM_INSTALL_IMAGE_* mid-flight, the new image won't match the stored
// one and we'll retrigger immediately. Stale state for compliant or removed
// nodes is cleaned up on every pass.
//
// Direct node API: each node's Talos client connects straight to the node's
// own IP — the upgrade RPC and the extension query never go through the
// cluster's control-plane proxy or any load balancer. That makes the flow
// resilient to a degraded API server (the very thing extension upgrades are
// often racing to fix).
//
// Throttled to one upgrade trigger per reconcile pass (rolling); workers go
// before control planes so the API server stays available longest. A cluster
// with TALOS_REQUIRED_EXTENSIONS unset (the default) is a no-op.
func (t *TalosManager) reconcileExtensions(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	required := parseRequiredExtensions(viper.GetString(consts.TALOS_REQUIRED_EXTENSIONS))
	if len(required) == 0 {
		return nil // feature disabled
	}

	clusterTag := clusterLogTag(cluster)
	cooldown := extensionUpgradeCooldown()

	// Skip if a Talos upgrade is already in progress; we don't want to fight
	// the rolling-upgrade flow or stack a second upgrade on the same node.
	if upState, err := t.stateService.GetUpgradeState(ctx, cluster); err == nil && upState != nil && upState.InProgress {
		return nil
	}

	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to list cluster machines: %w", err)
	}
	if len(machines) == 0 {
		return nil
	}

	state, err := t.stateService.GetExtensionUpgradeStates(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to load extension upgrade state for %s: %w", clusterTag, err)
	}
	t.pruneStaleExtensionState(ctx, cluster, state, machines)

	// Cluster-level fast path: every Machine has an in-flight record for the
	// image we'd use AND is still inside the cooldown window. Skip the talos
	// config load and per-node API calls — there's nothing to do this pass.
	if t.allMachinesInCooldown(machines, state, cooldown) {
		vlog.Debug(fmt.Sprintf("Extension reconcile %s: all %d machine(s) within cooldown of %s; skipping pass",
			clusterTag, len(machines), cooldown))
		return nil
	}

	clientConfig, err := t.GetTalosClientConfig(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to load talos client config for extension check on %s: %w", clusterTag, err)
	}

	// Workers first, then control planes — keeps the Kubernetes API server
	// available as long as possible during a roll.
	controlPlanes := t.machineService.FilterMachinesByRole(machines, controlPlaneRole)
	workers := t.machineService.FilterMachinesByRole(machines, "worker")
	ordered := make([]*vitistackv1alpha1.Machine, 0, len(workers)+len(controlPlanes))
	ordered = append(ordered, workers...)
	ordered = append(ordered, controlPlanes...)

	vlog.Debug(fmt.Sprintf("Extension reconcile %s: walking %d machine(s) (workers=%d, controlPlanes=%d, cooldown=%s, stateRecords=%d)",
		clusterTag, len(ordered), len(workers), len(controlPlanes), cooldown, len(state)))

	t.walkExtensionCandidates(ctx, cluster, clusterTag, clientConfig, ordered, required, state, cooldown)
	return nil
}

// clusterLogTag builds a stable, grep-friendly identifier for log lines.
// Uses spec.data.clusterId (the persistent identifier) and falls back to the
// resource name when clusterId hasn't been set yet (very early in the cluster
// lifecycle).
func clusterLogTag(cluster *vitistackv1alpha1.KubernetesCluster) string {
	clusterID := cluster.Spec.Cluster.ClusterId
	if clusterID == "" {
		clusterID = cluster.Name
	}
	return fmt.Sprintf("cluster=%s/%s", cluster.Namespace, clusterID)
}

// walkExtensionCandidates drives processMachineExtensions in priority order
// and applies the rolling-upgrade rule: stop the entire pass as soon as a
// node was triggered or is in cooldown. Compliant nodes have any leftover
// state record cleared.
func (t *TalosManager) walkExtensionCandidates(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clusterTag string,
	clientConfig *clientconfig.Config,
	ordered []*vitistackv1alpha1.Machine,
	required []string,
	state map[string]talosstateservice.ExtensionUpgradeRecord,
	cooldown time.Duration,
) {
	for _, m := range ordered {
		outcome, perr := t.processMachineExtensions(ctx, cluster, clusterTag, clientConfig, m, required, state[m.Name], cooldown)
		if perr != nil {
			vlog.Warn(fmt.Sprintf("Extension check skipped %s node=%s: %v", clusterTag, m.Name, perr))
			continue
		}
		if outcome == extOutcomeCompliant {
			t.clearExtensionStateIfPresent(ctx, cluster, clusterTag, state, m.Name)
			continue
		}
		if outcome == extOutcomeWaiting || outcome == extOutcomeTriggered {
			return // serialize per cluster — let the next reconcile take the next node
		}
		// extOutcomeSkipped: keep walking.
	}
}

// clearExtensionStateIfPresent drops the persisted record for nodeName when
// one exists, so a future regression triggers a fresh upgrade instead of
// being blocked by a stale cooldown timestamp.
func (t *TalosManager) clearExtensionStateIfPresent(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clusterTag string,
	state map[string]talosstateservice.ExtensionUpgradeRecord,
	nodeName string,
) {
	if _, ok := state[nodeName]; !ok {
		return
	}
	if err := t.stateService.RemoveExtensionUpgradeState(ctx, cluster, nodeName); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to clear extension state %s node=%s: %v", clusterTag, nodeName, err))
		return
	}
	delete(state, nodeName)
}

// extOutcome describes what reconcileExtensions did with a single Machine.
type extOutcome int

const (
	// extOutcomeCompliant — node has every required extension installed.
	extOutcomeCompliant extOutcome = iota
	// extOutcomeWaiting — node is missing extensions but a recent upgrade
	// trigger is still inside the cooldown window; the caller should leave
	// the rest of the cluster alone for this pass.
	extOutcomeWaiting
	// extOutcomeTriggered — operator just kicked off a Talos upgrade for
	// this node.
	extOutcomeTriggered
	// extOutcomeSkipped — couldn't evaluate or act on this node, but the
	// caller is free to keep walking.
	extOutcomeSkipped
)

// processMachineExtensions evaluates one Machine. record is the persisted
// extension upgrade state for this Machine (zero value if none).
//
// Performance + reliability notes:
//   - When a node is inside the cooldown window for the same image we'd use,
//     this returns extOutcomeWaiting WITHOUT opening a Talos client and
//     WITHOUT logging at INFO. A Debug log is emitted so operators can
//     observe the steady-state behaviour with LOG_LEVEL=debug.
//   - Otherwise we open a Talos client connected DIRECTLY to nodeIP — never
//     to a control-plane proxy or load balancer. Both the extension query
//     and the upgrade RPC therefore work even when the cluster's API server
//     is unhealthy.
func (t *TalosManager) processMachineExtensions(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clusterTag string,
	clientConfig *clientconfig.Config,
	m *vitistackv1alpha1.Machine,
	required []string,
	record talosstateservice.ExtensionUpgradeRecord,
	cooldown time.Duration,
) (extOutcome, error) {
	nodeIP := getFirstIPv4(m)
	if nodeIP == "" {
		return extOutcomeSkipped, nil
	}

	image := t.resolveProviderInstallImage(m)
	if image == "" {
		return extOutcomeSkipped, nil
	}

	if extensionCooldownActive(record, image, cooldown, clusterTag, m.Name, nodeIP) {
		return extOutcomeWaiting, nil
	}

	if t.versionEnforcementInFlight(ctx, cluster, clusterTag, m.Name) {
		return extOutcomeWaiting, nil
	}

	// Per-node Talos client: connect straight to the target node's API. No
	// CP proxy, no LB, no NetworkConfiguration VIP in the path.
	tClient, err := t.clientService.CreateTalosClient(ctx, false, clientConfig, []string{nodeIP})
	if err != nil {
		return extOutcomeSkipped, fmt.Errorf("failed to create direct Talos client for node %s (%s): %w", m.Name, nodeIP, err)
	}
	defer func() { _ = tClient.Close() }()

	installed, err := t.clientService.GetInstalledExtensions(ctx, tClient, nodeIP)
	if err != nil {
		// Cooldown expired but the node is still unreachable — surface a
		// warning so the operator can investigate (the VM may be stuck and
		// the previous Talos upgrade may have failed silently).
		if !record.TriggeredAt.IsZero() {
			vlog.Warn(fmt.Sprintf("Node %s/%s unreachable %s after extension upgrade trigger (image=%s): %v",
				clusterTag, m.Name, time.Since(record.TriggeredAt).Round(time.Second), record.Image, err))
		}
		return extOutcomeSkipped, err
	}

	missing := diffExtensions(required, installed)
	if len(missing) == 0 {
		// Tell the operator the roll completed for this node — useful
		// counterpart to the "triggering" log so the trigger/complete pair
		// is visible.
		if !record.TriggeredAt.IsZero() {
			vlog.Info(fmt.Sprintf("Node %s/%s now has all required extensions (took %s)",
				clusterTag, m.Name, time.Since(record.TriggeredAt).Round(time.Second)))
		}
		return extOutcomeCompliant, nil
	}

	// Cooldown elapsed and extensions still missing — log the re-trigger
	// distinctly so it's clear we're not stuck looping on the same node.
	if !record.TriggeredAt.IsZero() && record.Image == image {
		vlog.Warn(fmt.Sprintf("Node %s/%s still missing extensions %v after cooldown of %s — re-triggering Talos upgrade with image %s",
			clusterTag, m.Name, missing, cooldown, image))
	}

	// Include the installed list so a future "the operator thinks X is
	// missing but it's right there" debugging round has the data on hand —
	// most often the cause is a name-format mismatch (factory display path
	// vs Talos manifest name) and seeing both lists makes it obvious.
	vlog.Info(fmt.Sprintf("Node %s/%s ip=%s missing extensions %v (installed: %v) — triggering Talos upgrade with image %s",
		clusterTag, m.Name, nodeIP, missing, installed, image))
	if uerr := t.clientService.UpgradeNode(ctx, tClient, nodeIP, image); uerr != nil {
		vlog.Error(fmt.Sprintf("Failed to trigger extension upgrade %s node=%s ip=%s", clusterTag, m.Name, nodeIP), uerr)
		return extOutcomeSkipped, nil
	}
	if serr := t.stateService.SetExtensionUpgradeState(ctx, cluster, m.Name, image); serr != nil {
		// Logging only — the Upgrade RPC already went out, and the worst case
		// is we re-trigger on the next reconcile (the cooldown protection is
		// degraded but the upgrade still completes).
		vlog.Warn(fmt.Sprintf("Failed to persist extension upgrade state %s node=%s: %v", clusterTag, m.Name, serr))
	}
	// Pin the new install image so any future scale-up uses the same
	// schematic. Best-effort; failure is non-fatal.
	t.persistInstallImageIfMissing(ctx, cluster, image)
	return extOutcomeTriggered, nil
}

// allMachinesInCooldown reports whether every Machine has an extension-upgrade
// record whose image matches what we'd use today AND is still within the
// cooldown window. When true, the reconciler can short-circuit before opening
// any Talos clients. A Machine with no resolvable install image is treated as
// "in cooldown" because there's nothing actionable to do for it either.
func (t *TalosManager) allMachinesInCooldown(
	machines []*vitistackv1alpha1.Machine,
	state map[string]talosstateservice.ExtensionUpgradeRecord,
	cooldown time.Duration,
) bool {
	now := time.Now()
	for _, m := range machines {
		image := t.resolveProviderInstallImage(m)
		if image == "" {
			continue // nothing actionable; treat as quiescent
		}
		record, ok := state[m.Name]
		if !ok {
			return false
		}
		if record.Image != image {
			return false
		}
		if now.Sub(record.TriggeredAt) >= cooldown {
			return false
		}
	}
	return true
}

// pruneStaleExtensionState drops persisted records whose Machine no longer
// exists in the cluster (it was deleted or renamed). Mutates the in-memory
// state map alongside the secret so the caller sees the same view.
func (t *TalosManager) pruneStaleExtensionState(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	state map[string]talosstateservice.ExtensionUpgradeRecord,
	machines []*vitistackv1alpha1.Machine,
) {
	if len(state) == 0 {
		return
	}
	live := make(map[string]struct{}, len(machines))
	for _, m := range machines {
		live[m.Name] = struct{}{}
	}
	for name := range state {
		if _, ok := live[name]; ok {
			continue
		}
		if err := t.stateService.RemoveExtensionUpgradeState(ctx, cluster, name); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to prune stale extension state %s node=%s: %v", clusterLogTag(cluster), name, err))
			continue
		}
		delete(state, name)
	}
}

// parseRequiredExtensions splits the comma-separated env value, trims
// whitespace, and drops empty entries.
func parseRequiredExtensions(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// diffExtensions returns the entries from required that are not present in
// installed. Order follows required, so warning lines stay readable.
//
// Names are compared case-sensitively after stripping any "<author>/" prefix:
// Talos's ExtensionStatus reports the bare manifest name (e.g. "iscsi-tools")
// while the image-factory display path is "siderolabs/iscsi-tools". Both
// forms in the env var are accepted and treated as equivalent so operators
// can paste straight from factory.talos.dev without a transformation step.
func diffExtensions(required, installed []string) []string {
	have := make(map[string]struct{}, len(installed))
	for _, e := range installed {
		have[normalizeExtensionName(e)] = struct{}{}
	}
	var missing []string
	for _, r := range required {
		if _, ok := have[normalizeExtensionName(r)]; !ok {
			missing = append(missing, r)
		}
	}
	return missing
}

// normalizeExtensionName returns the substring after the last "/" so that
// "siderolabs/iscsi-tools" and "iscsi-tools" compare equal.
func normalizeExtensionName(name string) string {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// extensionCooldownActive reports whether the previously-persisted record
// for this node points at the same image we'd use right now AND the trigger
// is still inside the cooldown window — i.e. the previous extension upgrade
// is still in flight and we should leave the node alone. Quiet at INFO,
// visible at DEBUG so steady-state behaviour is observable without noise.
func extensionCooldownActive(
	record talosstateservice.ExtensionUpgradeRecord,
	image string,
	cooldown time.Duration,
	clusterTag, nodeName, nodeIP string,
) bool {
	if record.TriggeredAt.IsZero() || record.Image != image {
		return false
	}
	since := time.Since(record.TriggeredAt)
	if since >= cooldown {
		return false
	}
	vlog.Debug(fmt.Sprintf("Extension cooldown active %s node=%s ip=%s image=%s triggered %s ago (cooldown %s)",
		clusterTag, nodeName, nodeIP, image, since.Round(time.Second), cooldown))
	return true
}

// versionEnforcementInFlight reports whether the version-enforcement
// reconciler triggered a Talos upgrade on nodeName within its cooldown
// window. The two reconcilers use different schematics (version enforcer
// preserves the cluster's pinned image, extension reconciler swaps to a
// schematic that has the missing extensions), so we serialize per node:
// whichever fires first owns the upgrade slot until cooldown elapses.
// A read error is treated as "not in flight" — the worst case is the
// extensions roll triggers an extra upgrade, which the next reconcile
// will smooth out.
func (t *TalosManager) versionEnforcementInFlight(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clusterTag, nodeName string,
) bool {
	cooldown := versionEnforceCooldown()
	if cooldown <= 0 {
		return false
	}
	veStates, err := t.stateService.GetVersionEnforcementStates(ctx, cluster)
	if err != nil {
		return false
	}
	rec, ok := veStates[nodeName]
	if !ok || rec.TriggeredAt.IsZero() {
		return false
	}
	since := time.Since(rec.TriggeredAt)
	if since >= cooldown {
		return false
	}
	vlog.Info(fmt.Sprintf("Extension upgrade deferred %s node=%s: version enforcer triggered Talos upgrade %s ago with image=%s (cooldown %s)",
		clusterTag, nodeName, since.Round(time.Second), rec.Image, cooldown))
	return true
}

// resolveProviderInstallImage picks the TALOS_VM_INSTALL_IMAGE env var that
// matches the Machine's provider (currently kubevirt-specific or DEFAULT),
// falling back to DEFAULT when the per-provider value is unset. Returns ""
// only when DEFAULT itself is empty.
func (t *TalosManager) resolveProviderInstallImage(m *vitistackv1alpha1.Machine) string {
	if m != nil && m.Status.Provider != "" && t.configService.IsVirtualMachineProvider(m.Status.Provider.String()) {
		if image := t.configService.GetVMInstallImage(m.Status.Provider); image != "" {
			return image
		}
	}
	return viper.GetString(consts.TALOS_VM_INSTALL_IMAGE_DEFAULT)
}
