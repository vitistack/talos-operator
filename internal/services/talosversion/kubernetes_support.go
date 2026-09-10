package talosversion

import (
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
)

// parseMajorMinor extracts the major and minor components of a version string,
// tolerating a leading "v".
func parseMajorMinor(version string) (major, minor uint64, ok bool) {
	v, err := semver.NewVersion(strings.TrimPrefix(strings.TrimSpace(version), "v"))
	if err != nil {
		return 0, 0, false
	}
	return v.Major(), v.Minor(), true
}

// SupportsKubernetesVersion reports whether a node running talosVersion can run
// k8sTarget, along with a human-readable reason when it cannot.
//
// The check is deliberately one-sided: it rejects a target above the Talos
// release's ceiling and permits everything else. It does not enforce a lower
// bound, because the operator never drives a cluster backwards and an existing
// cluster below the floor is a situation to report, not to block an upgrade
// over.
//
// It fails open — returning true — whenever the answer would be a guess:
//
//   - talosVersion is empty or unparseable. The cluster's Talos version is read
//     from an annotation that may not be populated yet.
//   - talosVersion resolves only to a stand-in adapter (a future Talos release,
//     or one older than v1.11). Judging 1.14 against v1.13's ceiling would block
//     legitimate upgrades until someone adds an adapter, turning a missing table
//     entry into a silent fleet-wide freeze.
//   - k8sTarget is empty or unparseable. That is a malformed target, which the
//     caller's own semver validation reports with a better message than this
//     function could.
//
// Failing open preserves the behaviour that existed before this gate, so the
// only clusters affected are the ones with a definitively unsupported pair.
func SupportsKubernetesVersion(talosVersion, k8sTarget string) (supported bool, reason string) {
	targetMajor, targetMinor, ok := parseMajorMinor(k8sTarget)
	if !ok {
		return true, ""
	}

	if _, _, ok := parseMajorMinor(talosVersion); !ok {
		return true, ""
	}

	adapter, exact := GetTalosVersionAdapterFor(talosVersion)
	if !exact {
		return true, ""
	}

	maxMajor, maxMinor := adapter.MaxKubernetesMinor()
	if targetMajor > maxMajor || (targetMajor == maxMajor && targetMinor > maxMinor) {
		return false, fmt.Sprintf(
			"Talos %s supports Kubernetes up to %d.%d; upgrade Talos before moving to Kubernetes %s",
			adapter.Version(), maxMajor, maxMinor, strings.TrimPrefix(k8sTarget, "v"))
	}

	return true, ""
}
