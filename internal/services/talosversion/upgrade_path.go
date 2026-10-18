package talosversion

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	semver "github.com/Masterminds/semver/v3"
	"github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/compatibility"
	"github.com/siderolabs/talos/pkg/machinery/compatibility/talos110"
	"github.com/siderolabs/talos/pkg/machinery/compatibility/talos111"
	"github.com/siderolabs/talos/pkg/machinery/compatibility/talos112"
	"github.com/siderolabs/talos/pkg/machinery/compatibility/talos113"
	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"

	"github.com/vitistack/talos-operator/pkg/consts"
)

// Sidero only tests the configuration migration between adjacent minor
// releases, so the supported upgrade path goes through the latest patch of
// the running minor and of every minor in between before the final hop:
//
//	1.12.7 -> 1.12.12 -> 1.13.11 -> 1.14.2
//
// https://docs.siderolabs.com/talos/v1.14/configure-your-talos-cluster/lifecycle-management/upgrading-talos#supported-upgrade-paths
//
// defaultLatestPatches is the latest stable patch of each Talos minor, taken
// from https://github.com/siderolabs/talos/releases (checked 2026-10-05).
// Update it when the operator's TALOS_VERSION moves to a new minor;
// TestDefaultLatestPatchesCoverTheDefaultTalosVersion fails if a minor below
// the default is missing. Between operator releases, operations can add newer
// patches with TALOS_LATEST_PATCH_VERSIONS.
var defaultLatestPatches = map[[2]uint64]string{
	{1, 11}: "1.11.6",
	{1, 12}: "1.12.12",
	{1, 13}: "1.13.11",
	{1, 14}: "1.14.2",
}

// ErrUnknownLatestPatch means the latest patch of a minor on the upgrade path
// is not known, so the next step cannot be worked out.
var ErrUnknownLatestPatch = errors.New("latest Talos patch release unknown")

// NextUpgradeStep returns the next version on the supported upgrade path from
// running towards goal, without a "v" prefix: the latest patch of running's
// minor if it is not there yet, otherwise the latest patch of the next minor,
// and goal itself once it is one minor away or in the same minor. It never
// goes past goal and returns "" when running has already reached goal (it
// never downgrades).
//
// It returns an error wrapping ErrUnknownLatestPatch when a minor on the path
// is missing from the latest-patch table, and an error for a major version
// change, which has no known path.
func NextUpgradeStep(running, goal string) (string, error) {
	r, err := parseVersion(running)
	if err != nil {
		return "", fmt.Errorf("invalid running Talos version %q: %w", running, err)
	}
	g, err := parseVersion(goal)
	if err != nil {
		return "", fmt.Errorf("invalid target Talos version %q: %w", goal, err)
	}
	if !g.GreaterThan(r) {
		return "", nil
	}
	if g.Major() != r.Major() {
		return "", fmt.Errorf("no known Talos upgrade path from %s to %s across major versions", r, g)
	}
	if g.Minor() == r.Minor() {
		return g.String(), nil
	}

	latest, err := latestPatch(r.Major(), r.Minor())
	if err != nil {
		return "", err
	}
	if r.LessThan(latest) {
		return latest.String(), nil
	}
	if r.Minor()+1 == g.Minor() {
		return g.String(), nil
	}
	next, err := latestPatch(r.Major(), r.Minor()+1)
	if err != nil {
		return "", err
	}
	return next.String(), nil
}

// latestPatch returns the latest known patch of major.minor.
func latestPatch(major, minor uint64) (*semver.Version, error) {
	if v, ok := latestPatches()[[2]uint64{major, minor}]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("%w: latest patch of Talos %d.%d is not known; add it to %s (e.g. %d.%d.N)",
		ErrUnknownLatestPatch, major, minor, consts.TALOS_LATEST_PATCH_VERSIONS, major, minor)
}

var (
	latestPatchesMu       sync.Mutex
	latestPatchesOverride string
	latestPatchesCache    map[[2]uint64]*semver.Version
)

// latestPatches merges the default table with TALOS_LATEST_PATCH_VERSIONS. An
// override can add a minor or raise a patch, never lower one: a stale override
// left in place after an operator upgrade must not pull clusters backwards.
// The merged table is cached until the override changes.
func latestPatches() map[[2]uint64]*semver.Version {
	override := viper.GetString(consts.TALOS_LATEST_PATCH_VERSIONS)
	latestPatchesMu.Lock()
	defer latestPatchesMu.Unlock()
	if latestPatchesCache != nil && latestPatchesOverride == override {
		return latestPatchesCache
	}
	latestPatchesCache = mergeLatestPatches(override)
	latestPatchesOverride = override
	return latestPatchesCache
}

func mergeLatestPatches(override string) map[[2]uint64]*semver.Version {
	out := make(map[[2]uint64]*semver.Version, len(defaultLatestPatches))
	add := func(v *semver.Version) {
		key := [2]uint64{v.Major(), v.Minor()}
		if cur, ok := out[key]; !ok || v.GreaterThan(cur) {
			out[key] = v
		}
	}
	for _, s := range defaultLatestPatches {
		add(semver.MustParse(s))
	}

	var invalid []string
	for _, field := range strings.FieldsFunc(override, func(r rune) bool { return r == ',' || r == ' ' }) {
		v, ok := parseRelease(field)
		if !ok {
			invalid = append(invalid, field)
			continue
		}
		add(v)
	}
	// Logged once per distinct setting: the merge is cached until it changes.
	if len(invalid) > 0 {
		vlog.Warn(fmt.Sprintf("Ignoring %s entries that are not full release versions (vX.Y.Z): %s",
			consts.TALOS_LATEST_PATCH_VERSIONS, strings.Join(invalid, ", ")))
	}
	return out
}

// LowestVersion returns the lowest Talos version in versions (keyed by node),
// normalized with a "v" prefix, or "" when none parses. The least-upgraded
// node decides which step a cluster may take.
func LowestVersion(versions map[string]string) string {
	return extremeVersion(versions, func(a, b *semver.Version) bool { return a.LessThan(b) })
}

func extremeVersion(versions map[string]string, better func(a, b *semver.Version) bool) string {
	var best *semver.Version
	for _, v := range versions {
		sv, err := parseVersion(v)
		if err != nil {
			continue
		}
		if best == nil || better(sv, best) {
			best = sv
		}
	}
	if best == nil {
		return ""
	}
	return consts.NormalizeTalosVersion(best.String())
}

// Valid reports whether v parses as a Talos version.
func Valid(v string) bool {
	_, err := parseVersion(v)
	return err == nil
}

// IsOlder reports whether Talos version a is older than b. Unparseable
// versions fall back to a plain inequality.
func IsOlder(a, b string) bool {
	av, aerr := parseVersion(a)
	bv, berr := parseVersion(b)
	if aerr != nil || berr != nil {
		return a != b
	}
	return av.LessThan(bv)
}

// parseRelease parses a full release version ("v1.13.12"): all three numbers
// and no prerelease or build suffix, so "v1.14" (which semver would read as
// 1.14.0) or a beta cannot become a minor's latest patch.
func parseRelease(v string) (*semver.Version, bool) {
	sv, err := semver.StrictNewVersion(consts.StripVersionPrefix(strings.TrimSpace(v)))
	if err != nil || sv.Prerelease() != "" || sv.Metadata() != "" {
		return nil, false
	}
	return sv, true
}

func parseVersion(v string) (*semver.Version, error) {
	return semver.NewVersion(consts.StripVersionPrefix(strings.TrimSpace(v)))
}

// upstreamKnownTalos lists the Talos releases the vendored machinery can
// answer Kubernetes compatibility questions about. The ranges themselves are
// machinery's (MinimumKubernetesVersion / MaximumKubernetesVersion per
// release); a new Talos release needs a machinery bump plus one line here.
var upstreamKnownTalos = [][2]uint64{
	talos110.MajorMinor,
	talos111.MajorMinor,
	talos112.MajorMinor,
	talos113.MajorMinor,
}

// SupportsKubernetesVersion reports whether nodes running talosVersion can run
// Kubernetes k8sVersion, with machinery's reason when they cannot.
//
// It fails open, returning true, whenever the answer would be a guess: an
// empty or unparseable version, or a Talos release the vendored machinery
// does not cover (anything newer than it). Judging such a release against an
// older release's range would block legitimate upgrades until the dependency
// is bumped. This follows the approach of the closed PR #51 for issue #50.
func SupportsKubernetesVersion(talosVersion, k8sVersion string) (supported bool, reason string) {
	k8sVer, err := compatibility.ParseKubernetesVersion(strings.TrimSpace(k8sVersion))
	if err != nil {
		return true, ""
	}
	parsed, err := parseVersion(talosVersion)
	if err != nil {
		return true, ""
	}
	known := false
	for _, mm := range upstreamKnownTalos {
		if mm[0] == parsed.Major() && mm[1] == parsed.Minor() {
			known = true
			break
		}
	}
	if !known {
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
