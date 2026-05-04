package upgradeservice

import "testing"

const (
	testTag        = "v1.12.7"
	testFactoryRef = "factory.talos.dev/nocloud-installer/b0f2a8b575460a3dcb1234cc081c73c88e795aaef36eda9b88a6f4dddbd49365"
)

func TestSwapImageTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ref  string
		tag  string
		want string
	}{
		{
			name: "factory image rewrites tag and preserves schematic",
			ref:  testFactoryRef + ":v1.12.4",
			tag:  testTag,
			want: testFactoryRef + ":" + testTag,
		},
		{
			name: "tagless image appends",
			ref:  "ghcr.io/siderolabs/installer",
			tag:  testTag,
			want: "ghcr.io/siderolabs/installer:" + testTag,
		},
		{
			name: "registry port is not a tag separator",
			ref:  "localhost:5000/foo:v1",
			tag:  "v2",
			want: "localhost:5000/foo:v2",
		},
		{
			name: "registry port without image tag still appends",
			ref:  "localhost:5000/foo",
			tag:  "v1.12.7",
			want: "localhost:5000/foo:v1.12.7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := swapImageTag(tt.ref, tt.tag)
			if got != tt.want {
				t.Fatalf("swapImageTag(%q, %q) = %q, want %q", tt.ref, tt.tag, got, tt.want)
			}
		})
	}
}

func TestIsIPv4(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want bool
	}{
		{"10.0.0.1", true},
		{"100.64.6.205", true},
		{"::1", false},
		{"2a05:ec0:1013:2c1:12:9aff:feea:546c", false},
		{"", false},
		{"not-an-ip", false},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			if got := isIPv4(tt.in); got != tt.want {
				t.Fatalf("isIPv4(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
