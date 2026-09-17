package talosversion

import (
	"strings"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/compatibility/talos112"
	"github.com/siderolabs/talos/pkg/machinery/compatibility/talos113"
)

func TestSupportsKubernetesVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		talosVersion string
		k8sTarget    string
		want         bool
	}{
		// The case this check exists for: Talos 1.12 tops out at Kubernetes
		// 1.35, but 1.36.3 was being advertised and accepted.
		{"1.12 rejects 1.36", "1.12.7", "1.36.3", false},
		{"1.12 rejects 1.37", "1.12.7", "1.37.0", false},
		{"1.12 allows 1.35", "1.12.7", "1.35.4", true},
		{"1.12 allows 1.34", "1.12.7", "1.34.1", true},

		// Regression guard for a real upgrade: Kubernetes 1.36.3 on Talos
		// 1.13.4 succeeded in production, and the adapter's own default for
		// 1.13 reads 1.36.1. Anything comparing against that default rather
		// than the supported range would wrongly refuse this.
		{"1.13 allows 1.36.3", "v1.13.4", "1.36.3", true},
		{"1.13 allows the top of its range", "1.13.9", "1.36.98", true},
		{"1.13 rejects 1.37", "1.13.4", "1.37.0", false},

		// Below the floor is refused too, now that the range is machinery's.
		{"1.13 rejects Kubernetes below its floor", "1.13.4", "1.30.0", false},

		// Fail open wherever the answer would be a guess.
		{"Talos newer than machinery fails open", "1.14.0", "1.37.0", true},
		{"far future Talos fails open", "1.20.3", "1.99.0", true},
		{"unknown major fails open", "2.12.0", "1.36.3", true},
		{"Talos older than the operator supports fails open", "1.10.5", "1.36.3", true},
		{"unparseable Talos fails open", "bogus", "1.37.0", true},
		{"empty Talos fails open", "", "1.36.3", true},
		{"unparseable target fails open", "1.12.7", "bogus", true},
		{"empty target fails open", "1.12.7", "", true},

		{"v-prefixed both sides", "v1.12.7", "v1.36.3", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, reason := SupportsKubernetesVersion(tt.talosVersion, tt.k8sTarget)
			if got != tt.want {
				t.Errorf("SupportsKubernetesVersion(%q, %q) = %v (%s), want %v",
					tt.talosVersion, tt.k8sTarget, got, reason, tt.want)
			}
			if !got && reason == "" {
				t.Errorf("SupportsKubernetesVersion(%q, %q) denied without a reason",
					tt.talosVersion, tt.k8sTarget)
			}
		})
	}
}

// The denial text is machinery's, and it is what lands on the cluster as
// kubernetes-message, so it has to name both versions.
func TestSupportsKubernetesVersionReason(t *testing.T) {
	t.Parallel()

	_, reason := SupportsKubernetesVersion("1.12.7", "1.36.3")
	for _, want := range []string{"1.36.3", "1.12.7", "too new"} {
		if !strings.Contains(reason, want) {
			t.Errorf("reason = %q, want it to mention %q", reason, want)
		}
	}
}

// Guards the one thing this package still asserts about specific numbers:
// that the list names releases machinery actually knows. If a bump renames or
// drops one of these packages, this fails to compile rather than silently
// failing open for a release we meant to check.
func TestUpstreamKnownTalosMatchesMachinery(t *testing.T) {
	t.Parallel()

	for _, known := range [][2]uint64{talos112.MajorMinor, talos113.MajorMinor} {
		if !upstreamCovers(known[0], known[1]) {
			t.Errorf("machinery knows Talos %d.%d but upstreamKnownTalos does not list it", known[0], known[1])
		}
	}
}

// An entry in the list must genuinely be answerable by machinery, or the
// check would deny on the "not supported" error instead of failing open.
func TestEveryKnownReleaseIsAnswerable(t *testing.T) {
	t.Parallel()

	for _, known := range upstreamKnownTalos {
		if known[0] != 1 {
			t.Errorf("unexpected major %d in upstreamKnownTalos", known[0])
			continue
		}
		// A Kubernetes version inside every modern Talos range.
		talos := strings.Join([]string{"1", itoa(known[1]), "0"}, ".")
		if supported, reason := SupportsKubernetesVersion(talos, "1.34.0"); !supported {
			t.Errorf("Talos %s with Kubernetes 1.34.0 denied: %s", talos, reason)
		}
	}
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	return string(b)
}
