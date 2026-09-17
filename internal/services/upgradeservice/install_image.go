package upgradeservice

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/helpers/clusterlog"
	"github.com/vitistack/talos-operator/internal/helpers/talosextensions"
	"github.com/vitistack/talos-operator/pkg/consts"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// extensionProbeTimeout bounds one node's extension query, so a node whose API
// port is open but not serving cannot stall the reconcile.
const extensionProbeTimeout = 8 * time.Second

// extensionProbe returns the system extensions installed on each reachable
// machine, keyed by machine name. Unreachable machines are omitted.
type extensionProbe func(ctx context.Context, clientConfig *clientconfig.Config, machines []*vitistackv1alpha1.Machine) map[string][]string

// ReconcileInstallImageSchematic is the steady-state entry point to
// alignInstallImageSchematic. It leaves the pin alone while an upgrade runs:
// that upgrade has already chosen its installer and pins it on completion.
func (s *UpgradeService) ReconcileInstallImageSchematic(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
) (string, error) {
	if s.stateService == nil {
		return "", nil
	}
	// The secret flag, not the status annotation: an annotation stuck at
	// in-progress would otherwise block this for good.
	if flags, err := s.stateService.GetUpgradeState(ctx, cluster); err == nil && flags.InProgress {
		return s.stateService.GetInstallImage(ctx, cluster)
	}
	return s.alignInstallImageSchematic(ctx, cluster, clientConfig)
}

// alignInstallImageSchematic keeps the cluster's pinned install_image on a
// factory schematic that carries TALOS_REQUIRED_EXTENSIONS, and returns the pin
// as it stands afterwards ("" when nothing is pinned).
//
// The pin is written once and then only replaced by the upgrade flow, which
// keeps its repository — the schematic — and changes the tag. So when the
// operator's configured schematic gains an extension (v1.13.10 added
// nfs-utils), every cluster pinned earlier keeps the old set: its upgrades and
// its new nodes install without the extension. When a reachable node lacks a
// required extension, the pin moves to the configured image's schematic and
// keeps its own version tag; the nodes themselves change on their next upgrade.
//
// The pin is left alone when no extensions are required, when it already uses
// the configured schematic, when every probed node has the required
// extensions (a deliberately different schematic keeps its extras), when no
// node answers, and for clusters without virtual-machine providers, whose
// installers are not the configured VM images.
func (s *UpgradeService) alignInstallImageSchematic(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
) (string, error) {
	if s.stateService == nil {
		return "", nil
	}
	pinned, err := s.stateService.GetInstallImage(ctx, cluster)
	if err != nil {
		return "", fmt.Errorf("failed to read pinned install image: %w", err)
	}
	required := talosextensions.ParseRequired(viper.GetString(consts.TALOS_REQUIRED_EXTENSIONS))
	if pinned == "" || len(required) == 0 || s.machineService == nil {
		return pinned, nil
	}

	machines, err := s.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return pinned, fmt.Errorf("failed to list cluster machines: %w", err)
	}
	configured := s.configuredVMInstallImage(machines)
	if configured == "" || schematicID(configured) == schematicID(pinned) {
		return pinned, nil
	}
	if s.schematicDecisionHolds(cluster, pinned, configured, required) {
		return pinned, nil
	}

	probed := s.installedExtensions(ctx, clientConfig, machines)
	if len(probed) == 0 {
		// No node answered, so there is nothing to conclude. Try again next pass.
		return pinned, nil
	}

	// Whatever is decided below holds until the pin, the configured image or
	// the required list changes; remember it so the nodes are not probed again
	// on every drift pass.
	s.rememberSchematicDecision(cluster, pinned, configured, required)
	return s.movePinToConfiguredSchematic(ctx, cluster, pinned, configured, probed, required)
}

// movePinToConfiguredSchematic writes the configured schematic into the
// cluster's pin when the probed nodes show it is needed and nothing would be
// lost, and returns the pin as it stands afterwards.
func (s *UpgradeService) movePinToConfiguredSchematic(
	ctx context.Context,
	cluster *vitistackv1alpha1.KubernetesCluster,
	pinned, configured string,
	probed map[string][]string,
	required []string,
) (string, error) {
	var lacking, extras, missing []string
	for name, installed := range probed {
		if m := talosextensions.Missing(required, installed); len(m) > 0 {
			lacking = append(lacking, name)
			missing = m
		}
		extras = append(extras, unexpectedExtensions(installed, required)...)
	}
	slices.Sort(lacking)
	slices.Sort(extras)
	extras = slices.Compact(extras)

	switch {
	case len(extras) > 0:
		// The schematic carries extensions this operator does not know about,
		// so it was built for this cluster. Replacing it would drop them.
		vlog.Info(fmt.Sprintf("Keeping pinned install image %s: schematic %s carries %v beyond %s",
			clusterlog.Tag(cluster), pinned, extras, consts.TALOS_REQUIRED_EXTENSIONS))
		return pinned, nil
	case len(lacking) == 0:
		return pinned, nil
	}

	tag := imageTag(pinned)
	if tag == "" {
		vlog.Warn(fmt.Sprintf("Cannot move pinned install image %s: %s has no version tag to keep", clusterlog.Tag(cluster), pinned))
		return pinned, nil
	}
	updated, ok := swapSchematic(pinned, configured, tag)
	if !ok {
		vlog.Warn(fmt.Sprintf("Cannot move pinned install image %s: %s and configured %s are not the same registry and installer layout",
			clusterlog.Tag(cluster), pinned, configured))
		return pinned, nil
	}
	if err := s.stateService.SetInstallImage(ctx, cluster, updated); err != nil {
		return pinned, fmt.Errorf("failed to pin install image %s: %w", updated, err)
	}
	vlog.Info(fmt.Sprintf("Pinned install image moved to the configured schematic: %s from=%s to=%s missing=%v nodes=%v",
		clusterlog.Tag(cluster), pinned, updated, missing, lacking))
	return updated, nil
}

// configuredVMInstallImage returns the operator's install image for the
// cluster's virtual-machine provider, or "" when no machine runs on one.
func (s *UpgradeService) configuredVMInstallImage(machines []*vitistackv1alpha1.Machine) string {
	if s.configService == nil {
		return ""
	}
	for _, m := range machines {
		if m.Status.Provider != "" && s.configService.IsVirtualMachineProvider(m.Status.Provider.String()) {
			return s.configService.GetVMInstallImage(m.Status.Provider)
		}
	}
	return ""
}

// probeInstalledExtensions asks each machine's own Talos API which extensions
// it has installed. Machines without an IPv4 address or that do not answer
// are left out.
func (s *UpgradeService) probeInstalledExtensions(
	ctx context.Context,
	clientConfig *clientconfig.Config,
	machines []*vitistackv1alpha1.Machine,
) map[string][]string {
	out := make(map[string][]string, len(machines))
	if clientConfig == nil || s.clientService == nil {
		return out
	}
	for _, m := range machines {
		ip := ""
		for _, addr := range m.Status.PublicIPAddresses {
			if isIPv4(addr) {
				ip = addr
				break
			}
		}
		if ip == "" {
			continue
		}
		tClient, err := s.clientService.CreateTalosClient(ctx, false, clientConfig, []string{ip})
		if err != nil {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, extensionProbeTimeout)
		installed, err := s.clientService.GetInstalledExtensions(probeCtx, tClient, ip)
		cancel()
		_ = tClient.Close()
		if err != nil {
			vlog.Debug(fmt.Sprintf("Extension probe failed node=%s: %v", m.Name, err))
			continue
		}
		out[m.Name] = installed
	}
	return out
}

// unexpectedExtensions returns the installed extensions that are neither
// required nor written by Talos itself. "schematic" is the marker the image
// factory installs to record the schematic ID, and "modules.dep" is generated
// during install; neither comes from the schematic's extension list.
func unexpectedExtensions(installed, required []string) []string {
	var extras []string
	for _, e := range installed {
		switch e {
		case "schematic", "modules.dep":
			continue
		}
		if len(talosextensions.Missing([]string{e}, required)) > 0 {
			extras = append(extras, e)
		}
	}
	return extras
}

// swapSchematic returns pinned with configured's schematic ID and the given
// tag, keeping pinned's registry and installer platform: the schematic is
// platform-independent, but the platform decides how Talos boots, and two
// clusters in the fleet install from metal-installer rather than
// nocloud-installer. Reports false when either reference is not a
// <registry>/<platform>-installer/<schematic> path on the same registry, which
// leaves the caller with a pin it should not rewrite.
func swapSchematic(pinned, configured, tag string) (string, bool) {
	pinnedParts := strings.Split(imageRepository(pinned), "/")
	configuredParts := strings.Split(imageRepository(configured), "/")
	if len(pinnedParts) < 2 || len(configuredParts) < 2 || pinnedParts[0] != configuredParts[0] {
		return "", false
	}
	pinnedParts[len(pinnedParts)-1] = configuredParts[len(configuredParts)-1]
	return strings.Join(pinnedParts, "/") + ":" + tag, true
}

// schematicID returns the last path element of ref's repository — the factory
// schematic ID for a factory installer image.
func schematicID(ref string) string {
	repo := imageRepository(ref)
	if slash := strings.LastIndexByte(repo, '/'); slash >= 0 {
		return repo[slash+1:]
	}
	return repo
}

// schematicDecisionHolds reports whether the nodes were already probed for
// this combination of pin, configured image and required extensions.
func (s *UpgradeService) schematicDecisionHolds(cluster *vitistackv1alpha1.KubernetesCluster, pinned, configured string, required []string) bool {
	seen, ok := s.schematicChecked.Load(client.ObjectKeyFromObject(cluster))
	return ok && seen == schematicFingerprint(pinned, configured, required)
}

func (s *UpgradeService) rememberSchematicDecision(cluster *vitistackv1alpha1.KubernetesCluster, pinned, configured string, required []string) {
	s.schematicChecked.Store(client.ObjectKeyFromObject(cluster), schematicFingerprint(pinned, configured, required))
}

func schematicFingerprint(pinned, configured string, required []string) string {
	return pinned + "|" + configured + "|" + strings.Join(required, ",")
}

// imageRepository returns ref without its tag or digest. For a factory
// installer that is the platform plus schematic ID.
func imageRepository(ref string) string {
	ref, _, _ = strings.Cut(ref, "@")
	if colon := strings.LastIndexByte(ref, ':'); colon > strings.LastIndexByte(ref, '/') {
		return ref[:colon]
	}
	return ref
}

// imageTag returns ref's tag, or "" when it has none. As in swapImageTag, a
// colon before the last "/" is a registry port, not a tag separator.
func imageTag(ref string) string {
	ref, _, _ = strings.Cut(ref, "@")
	if colon := strings.LastIndexByte(ref, ':'); colon > strings.LastIndexByte(ref, '/') {
		return ref[colon+1:]
	}
	return ""
}
