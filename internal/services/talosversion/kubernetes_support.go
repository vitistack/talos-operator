package talosversion

import (
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/compatibility"
	"github.com/siderolabs/talos/pkg/machinery/compatibility/talos111"
	"github.com/siderolabs/talos/pkg/machinery/compatibility/talos112"
	"github.com/siderolabs/talos/pkg/machinery/compatibility/talos113"
)

// upstreamKnownTalos lists the Talos releases the vendored machinery can
// answer compatibility questions about, overlapped with the ones this operator
// supports (see ListSupportedVersions).
//
// The values are machinery's own, never numbers written here: the supported
// Kubernetes range for each release lives in the Talos packages and is updated
// by upgrading the machinery dependency. A new Talos release needs a module
// bump plus one line in this list, and no version data in this repository.
//
// compatibility.KubernetesVersion.SupportedWith returns an error for any Talos
// release machinery does not recognise, which would mean a freshly released
// Talos blocking every Kubernetes upgrade until the bump landed. This list is
// what lets SupportsKubernetesVersion tell "unsupported pairing" apart from
// "machinery has not heard of this release" without matching on error text.
var upstreamKnownTalos = [][2]uint64{
	talos111.MajorMinor,
	talos112.MajorMinor,
	talos113.MajorMinor,
}

// SupportsKubernetesVersion reports whether a node running talosVersion can run
// k8sTarget, along with the reason when it cannot.
//
// The supported range comes from
// github.com/siderolabs/talos/pkg/machinery/compatibility, so both the ceiling
// and the floor are Sidero's published numbers rather than a copy maintained
// here.
//
// It fails open — returning true — whenever the answer would be a guess:
//
//   - talosVersion is empty or unparseable. The cluster's Talos version is read
//     from an annotation that may not be populated yet.
//   - talosVersion is a release the vendored machinery does not cover, which
//     includes anything newer than it. Judging such a release against an older
//     release's range would block legitimate upgrades until the dependency was
//     bumped, turning a stale module into a silent fleet-wide freeze.
//   - k8sTarget is empty or unparseable. That is a malformed target, which the
//     caller's own validation reports with a better message than this could.
//
// Failing open preserves the behaviour that existed before this check, so the
// only clusters affected are the ones with a definitively unsupported pair.
func SupportsKubernetesVersion(talosVersion, k8sTarget string) (supported bool, reason string) {
	k8sVer, err := compatibility.ParseKubernetesVersion(strings.TrimSpace(k8sTarget))
	if err != nil {
		return true, ""
	}

	// Parsed separately because compatibility.TalosVersion does not expose the
	// major/minor needed for the known-release check.
	parsed, err := semver.NewVersion(strings.TrimPrefix(strings.TrimSpace(talosVersion), "v"))
	if err != nil {
		return true, ""
	}
	if !upstreamCovers(parsed.Major(), parsed.Minor()) {
		return true, ""
	}

	talosVer, err := compatibility.ParseTalosVersion(&machine.VersionInfo{Tag: parsed.String()})
	if err != nil {
		return true, ""
	}

	if err := k8sVer.SupportedWith(talosVer); err != nil {
		return false, err.Error()
	}

	return true, ""
}

// upstreamCovers reports whether machinery has a compatibility entry for this
// Talos release.
func upstreamCovers(major, minor uint64) bool {
	for _, known := range upstreamKnownTalos {
		if known[0] == major && known[1] == minor {
			return true
		}
	}
	return false
}
