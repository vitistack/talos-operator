package talos

import (
	"testing"
)

const testFactoryImage = "factory.talos.dev/nocloud-installer/060a945412986c731a206fa0dd0b736e1363e94a38c1f3eb62dc38e63933ba8e"

// A reinstall for missing extensions takes the configured image's schematic
// but keeps the Talos version the node runs: version changes belong to the
// upgrade paths, which follow the supported path for the whole cluster.
func TestExtensionInstallImageKeepsTheNodesTalosVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, image, running, want string
	}{
		{"same version keeps the image", testFactoryImage + ":v1.13.10", "v1.13.10", testFactoryImage + ":v1.13.10"},
		{"newer image does not upgrade the node", testFactoryImage + ":v1.14.2", "v1.12.7", testFactoryImage + ":v1.12.7"},
		{"older image does not downgrade the node", testFactoryImage + ":v1.13.10", "v1.14.2", testFactoryImage + ":v1.14.2"},
		{"version reported without prefix", testFactoryImage + ":v1.13.10", "1.12.12", testFactoryImage + ":v1.12.12"},
		{"digest-pinned image", testFactoryImage + ":v1.13.10@sha256:0123abcd", "v1.12.7", testFactoryImage + ":v1.12.7"},
		{"registry port is not mistaken for a tag", "localhost:5000/installer:v1.13.10", "v1.12.7", "localhost:5000/installer:v1.12.7"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := extensionInstallImage(tt.image, tt.running)
			if err != nil {
				t.Fatalf("extensionInstallImage(%q, %q): %v", tt.image, tt.running, err)
			}
			if got != tt.want {
				t.Errorf("extensionInstallImage(%q, %q) = %q, want %q", tt.image, tt.running, got, tt.want)
			}
		})
	}
}

func TestExtensionInstallImageNeedsTheRunningVersion(t *testing.T) {
	t.Parallel()
	for _, running := range []string{"", "not-a-version"} {
		if _, err := extensionInstallImage(testFactoryImage+":v1.13.10", running); err == nil {
			t.Errorf("running=%q: without the node's version the reinstall cannot keep it, want an error", running)
		}
	}
}
