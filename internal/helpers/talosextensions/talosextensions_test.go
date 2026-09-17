package talosextensions

import (
	"slices"
	"testing"
)

func TestParseRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{name: "empty disables the check", raw: "", want: nil},
		{name: "trims and drops blanks", raw: " siderolabs/iscsi-tools, ,siderolabs/nfs-utils ", want: []string{"siderolabs/iscsi-tools", "siderolabs/nfs-utils"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ParseRequired(tt.raw); !slices.Equal(got, tt.want) {
				t.Fatalf("ParseRequired(%q) = %v, want %v", tt.raw, got, tt.want)
			}
		})
	}
}

func TestMissing(t *testing.T) {
	t.Parallel()

	required := []string{"siderolabs/iscsi-tools", "siderolabs/nfs-utils", "qemu-guest-agent"}
	tests := []struct {
		name      string
		installed []string
		want      []string
	}{
		{
			// Talos reports bare manifest names; the env var may use factory paths.
			name:      "author prefix is ignored on either side",
			installed: []string{"iscsi-tools", "nfs-utils", "siderolabs/qemu-guest-agent"},
			want:      nil,
		},
		{
			// The v1.13.10 schematic added nfs-utils; clusters pinned to the
			// previous schematic lack it.
			name:      "extension added to the schematic later",
			installed: []string{"iscsi-tools", "qemu-guest-agent", "trident-iscsi-tools", "util-linux-tools", "schematic"},
			want:      []string{"siderolabs/nfs-utils"},
		},
		{
			name:      "nothing installed",
			installed: nil,
			want:      required,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Missing(required, tt.installed); !slices.Equal(got, tt.want) {
				t.Fatalf("Missing() = %v, want %v", got, tt.want)
			}
		})
	}
}
