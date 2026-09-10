package talosversion

import "testing"

func TestMaxKubernetesMinor(t *testing.T) {
	tests := []struct {
		name      string
		adapter   TalosVersionAdapter
		wantMajor uint64
		wantMinor uint64
	}{
		{"v1.11", NewV1_11Adapter(), 1, 34},
		{"v1.12", NewV1_12Adapter(), 1, 35},
		{"v1.13", NewV1_13Adapter(), 1, 36},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			major, minor := tt.adapter.MaxKubernetesMinor()
			if major != tt.wantMajor || minor != tt.wantMinor {
				t.Errorf("MaxKubernetesMinor() = %d.%d, want %d.%d", major, minor, tt.wantMajor, tt.wantMinor)
			}
		})
	}
}

// The ceiling must never sit below the version the same adapter hands out as
// its default, or config generation would produce a cluster the upgrade gate
// then refuses to touch.
func TestMaxKubernetesMinorCoversDefault(t *testing.T) {
	for _, adapter := range []TalosVersionAdapter{
		NewV1_11Adapter(), NewV1_12Adapter(), NewV1_13Adapter(),
	} {
		t.Run(adapter.Version(), func(t *testing.T) {
			defMajor, defMinor, ok := parseMajorMinor(adapter.DefaultKubernetesVersion())
			if !ok {
				t.Fatalf("DefaultKubernetesVersion() = %q is unparseable", adapter.DefaultKubernetesVersion())
			}
			maxMajor, maxMinor := adapter.MaxKubernetesMinor()
			if defMajor > maxMajor || (defMajor == maxMajor && defMinor > maxMinor) {
				t.Errorf("default %d.%d exceeds ceiling %d.%d", defMajor, defMinor, maxMajor, maxMinor)
			}
		})
	}
}

func TestGetTalosVersionAdapterFor_Exact(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    string
	}{
		{"v1.11.6", "v1.11.6", versionV1_11},
		{"1.12.7", "1.12.7", versionV1_12},
		{"v1.13.4", "v1.13.4", versionV1_13},
		{"v1.13.9", "v1.13.9", versionV1_13},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, exact := GetTalosVersionAdapterFor(tt.version)
			if !exact {
				t.Errorf("GetTalosVersionAdapterFor(%q) exact = false, want true", tt.version)
			}
			if got := adapter.Version(); got != tt.want {
				t.Errorf("Version() = %q, want %q", got, tt.want)
			}
		})
	}
}

// Anything the switch cannot actually speak for must say so, so callers can
// fail open instead of judging a cluster against the wrong ceiling.
func TestGetTalosVersionAdapterFor_Fallback(t *testing.T) {
	tests := []struct {
		name    string
		version string
	}{
		{"future minor", "1.14.0"},
		{"far future minor", "1.20.3"},
		{"below oldest adapter", "1.10.5"},
		{"unparseable", "not-a-version"},
		{"empty", ""},
		// A 2.12 is not a 1.12. Matching on the minor alone would claim the
		// v1.12 adapter describes it and enforce that release's ceiling.
		{"unknown major, known minor", "2.12.0"},
		{"unknown major, newest minor", "v2.13.4"},
		{"major zero", "0.12.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, exact := GetTalosVersionAdapterFor(tt.version)
			if exact {
				t.Errorf("GetTalosVersionAdapterFor(%q) exact = true, want false", tt.version)
			}
			if adapter == nil {
				t.Errorf("GetTalosVersionAdapterFor(%q) returned nil adapter", tt.version)
			}
		})
	}
}

// GetTalosVersionAdapter is the pre-existing entry point and must keep
// resolving exactly as before now that it delegates to the new lookup.
func TestGetTalosVersionAdapterUnchanged(t *testing.T) {
	tests := []struct {
		version string
		want    string
	}{
		{"1.10.5", versionV1_11},
		{"v1.11.6", versionV1_11},
		{"1.12.7", versionV1_12},
		{"v1.13.4", versionV1_13},
		{"1.14.0", versionV1_13},
		{"garbage", versionV1_12},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			if got := GetTalosVersionAdapter(tt.version).Version(); got != tt.want {
				t.Errorf("GetTalosVersionAdapter(%q).Version() = %q, want %q", tt.version, got, tt.want)
			}
		})
	}
}

func TestSupportsKubernetesVersion(t *testing.T) {
	tests := []struct {
		name         string
		talosVersion string
		k8sTarget    string
		want         bool
	}{
		// The case this gate exists for: Talos 1.12 tops out at Kubernetes
		// 1.35, but 1.36.3 was being advertised and accepted.
		{"1.12 rejects 1.36", "1.12.7", "1.36.3", false},
		{"1.12 rejects 1.37", "1.12.7", "1.37.0", false},
		{"1.12 allows 1.35", "1.12.7", "1.35.4", true},
		{"1.12 allows 1.34", "1.12.7", "1.34.1", true},

		// Regression guard: a patch above the adapter's *default* is still
		// within the supported minor and must not be blocked.
		{"1.13 allows 1.36.3 above default 1.36.1", "v1.13.4", "1.36.3", true},
		{"1.13 allows 1.36.99", "1.13.9", "1.36.99", true},
		{"1.13 rejects 1.37", "1.13.4", "1.37.0", false},

		// Fail open wherever the adapter cannot speak for the version.
		{"future Talos fails open", "1.14.0", "1.37.0", true},
		{"unparseable Talos fails open", "bogus", "1.37.0", true},
		{"empty Talos fails open", "", "1.36.3", true},
		{"unparseable target fails open", "1.12.7", "bogus", true},
		{"empty target fails open", "1.12.7", "", true},

		{"v-prefixed both sides", "v1.12.7", "v1.36.3", false},

		// An unrecognised major must fail open, not inherit the ceiling of
		// the same-numbered minor.
		{"unknown major fails open", "2.12.0", "1.36.3", true},
		{"major zero fails open", "0.12.0", "1.36.3", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
