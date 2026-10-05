package talosversion

import (
	"strings"
	"testing"

	semver "github.com/Masterminds/semver/v3"
	"github.com/spf13/viper"
	"github.com/vitistack/talos-operator/internal/settings"
	"github.com/vitistack/talos-operator/pkg/consts"
)

// withLatestPatchOverride sets TALOS_LATEST_PATCH_VERSIONS for one test.
func withLatestPatchOverride(t *testing.T, value string) {
	t.Helper()
	viper.Set(consts.TALOS_LATEST_PATCH_VERSIONS, value)
	t.Cleanup(func() { viper.Set(consts.TALOS_LATEST_PATCH_VERSIONS, nil) })
}

func TestNextUpgradeStepFollowsLatestPatchOfEachMinor(t *testing.T) {
	tests := []struct {
		name          string
		running, goal string
		want          string
	}{
		// The issue's example: 1.12.7 -> 1.12.latest -> 1.13.latest -> goal.
		{"patch up within own minor first", "1.12.7", "1.14.2", "1.12.12"},
		{"then latest patch of the next minor", "1.12.12", "1.14.2", "1.13.11"},
		{"final hop goes straight to the goal", "1.13.11", "1.14.2", "1.14.2"},
		{"final hop may land on any patch of the goal minor", "1.12.12", "1.13.10", "1.13.10"},
		{"patch upgrade within the goal minor", "1.13.5", "1.13.10", "1.13.10"},
		{"accepts a v prefix", "v1.12.7", "v1.14.2", "1.12.12"},
		{"running past the known latest patch counts as latest", "1.12.13", "1.14.2", "1.13.11"},
		{"already at the goal", "1.14.2", "1.14.2", ""},
		{"never downgrades", "1.14.2", "1.13.10", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NextUpgradeStep(tt.running, tt.goal)
			if err != nil {
				t.Fatalf("NextUpgradeStep(%q, %q): %v", tt.running, tt.goal, err)
			}
			if got != tt.want {
				t.Errorf("NextUpgradeStep(%q, %q) = %q, want %q", tt.running, tt.goal, got, tt.want)
			}
		})
	}
}

func TestNextUpgradeStepRefusesToGuessAnUnknownLatestPatch(t *testing.T) {
	_, err := NextUpgradeStep("1.10.3", "1.12.12")
	if err == nil {
		t.Fatal("expected an error when the latest 1.10 patch is unknown")
	}
	for _, want := range []string{"1.10", consts.TALOS_LATEST_PATCH_VERSIONS} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q, got %q", want, err)
		}
	}
}

func TestNextUpgradeStepRejectsMajorVersionChange(t *testing.T) {
	if _, err := NextUpgradeStep("1.14.2", "2.0.0"); err == nil {
		t.Fatal("a major version upgrade has no known path and must not be stepped blindly")
	}
}

func TestLatestPatchOverrideRaisesAndAddsMinors(t *testing.T) {
	withLatestPatchOverride(t, "v1.13.12, 1.10.9, not-a-version, 1.12.1")

	if got, _ := NextUpgradeStep("1.12.12", "1.14.2"); got != "1.13.12" {
		t.Errorf("override should raise the latest 1.13 patch to 1.13.12, got %q", got)
	}
	if got, _ := NextUpgradeStep("1.10.3", "1.12.12"); got != "1.10.9" {
		t.Errorf("override should add the missing 1.10 minor, got %q", got)
	}
	// An override can raise a patch but not lower one below the default table.
	if got, _ := NextUpgradeStep("1.12.7", "1.14.2"); got != "1.12.12" {
		t.Errorf("an older override entry must not lower the latest 1.12 patch, got %q", got)
	}
}

func TestLatestPatchOverrideAcceptsOnlyFullReleases(t *testing.T) {
	// "v1.10" would parse as 1.10.0 and a prerelease is not a patch release:
	// neither may become the latest patch of a minor clusters are routed through.
	withLatestPatchOverride(t, "v1.10, 1.10.9-beta.1")

	if _, err := NextUpgradeStep("1.10.3", "1.12.12"); err == nil {
		t.Error("a short version or a prerelease must not add a minor to the path")
	}
}

func TestLowestVersion(t *testing.T) {
	tests := []struct {
		name     string
		versions map[string]string
		want     string
	}{
		{"lowest of several", map[string]string{"cp-0": "v1.13.11", "wrk-0": "v1.12.7", "wrk-1": "1.13.2"}, "v1.12.7"},
		{"normalizes the prefix", map[string]string{"cp-0": "1.12.12"}, "v1.12.12"},
		{"skips unparseable entries", map[string]string{"cp-0": "garbage", "wrk-0": "v1.13.11"}, "v1.13.11"},
		{"nothing known", map[string]string{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LowestVersion(tt.versions); got != tt.want {
				t.Errorf("LowestVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsOlder(t *testing.T) {
	if !IsOlder("v1.12.7", "1.12.12") || IsOlder("v1.13.11", "v1.12.12") || IsOlder("v1.12.12", "1.12.12") {
		t.Error("IsOlder should compare semver versions, ignoring the v prefix")
	}
}

func TestSupportsKubernetesVersion(t *testing.T) {
	tests := []struct {
		name       string
		talos, k8s string
		want       bool
	}{
		{"inside the range", "1.13.11", "1.35.2", true},
		{"below the Talos minimum", "1.13.11", "1.30.5", false},
		{"above the Talos maximum", "1.12.12", "1.36.3", false},
		{"Talos release machinery does not know fails open", "1.14.2", "1.30.5", true},
		{"unknown Kubernetes version fails open", "1.13.11", "", true},
		{"unknown Talos version fails open", "", "1.30.5", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, reason := SupportsKubernetesVersion(tt.talos, tt.k8s)
			if got != tt.want {
				t.Errorf("SupportsKubernetesVersion(%q, %q) = %t (%s), want %t", tt.talos, tt.k8s, got, reason, tt.want)
			}
			if !got && reason == "" {
				t.Error("a refusal must say why")
			}
		})
	}
}

// TestDefaultLatestPatchesCoverTheDefaultTalosVersion fails when the operator's
// default TALOS_VERSION is bumped to a minor whose predecessors are missing
// from the latest-patch table, which would stop clusters from being offered
// a path there.
func TestDefaultLatestPatchesCoverTheDefaultTalosVersion(t *testing.T) {
	settings.Init()
	// Other tests in this package override TALOS_VERSION; a nil override
	// falls through to the default this test is about.
	prev := viper.Get(consts.TALOS_VERSION)
	viper.Set(consts.TALOS_VERSION, nil)
	t.Cleanup(func() { viper.Set(consts.TALOS_VERSION, prev) })
	target, err := semver.NewVersion(consts.StripVersionPrefix(viper.GetString(consts.TALOS_VERSION)))
	if err != nil {
		t.Fatalf("default TALOS_VERSION does not parse: %v", err)
	}
	oldest, err := semver.NewVersion(strings.TrimSuffix(versionV1_11, ".x") + ".0")
	if err != nil {
		t.Fatal(err)
	}
	for minor := oldest.Minor(); minor < target.Minor(); minor++ {
		if _, ok := defaultLatestPatches[[2]uint64{target.Major(), minor}]; !ok {
			t.Errorf("latest-patch table has no entry for %d.%d, needed to reach the default TALOS_VERSION %s", target.Major(), minor, target)
		}
	}
}
