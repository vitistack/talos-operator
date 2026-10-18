package imageref

import "testing"

const factory = "factory.talos.dev/nocloud-installer/b0f2a8b575460a3dcb1234cc081c73c88e795aaef36eda9b88a6f4dddbd49365"

func TestWithTag(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, ref, tag, want string }{
		{"factory image rewrites tag and keeps the schematic", factory + ":v1.12.4", "v1.12.7", factory + ":v1.12.7"},
		{"tagless image appends", "ghcr.io/siderolabs/installer", "v1.12.7", "ghcr.io/siderolabs/installer:v1.12.7"},
		{"registry port is not a tag separator", "localhost:5000/foo:v1", "v2", "localhost:5000/foo:v2"},
		{"registry port without tag appends", "localhost:5000/foo", "v1.12.7", "localhost:5000/foo:v1.12.7"},
		{"digest is dropped, it would pin the old image", factory + ":v1.13.10@sha256:0123abcd", "v1.12.7", factory + ":v1.12.7"},
		{"digest without tag", factory + "@sha256:0123abcd", "v1.12.7", factory + ":v1.12.7"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := WithTag(tt.ref, tt.tag); got != tt.want {
				t.Errorf("WithTag(%q, %q) = %q, want %q", tt.ref, tt.tag, got, tt.want)
			}
		})
	}
}

func TestTagAndBase(t *testing.T) {
	t.Parallel()
	tests := []struct{ ref, tag, base string }{
		{factory + ":v1.13.10", "v1.13.10", factory},
		{factory + ":v1.13.10@sha256:0123abcd", "v1.13.10", factory},
		{"localhost:5000/foo", "", "localhost:5000/foo"},
		{"localhost:5000/foo:v2", "v2", "localhost:5000/foo"},
	}
	for _, tt := range tests {
		if got := Tag(tt.ref); got != tt.tag {
			t.Errorf("Tag(%q) = %q, want %q", tt.ref, got, tt.tag)
		}
		if got := Base(tt.ref); got != tt.base {
			t.Errorf("Base(%q) = %q, want %q", tt.ref, got, tt.base)
		}
	}
}
