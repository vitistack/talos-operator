package upgradeservice

import (
	"testing"

	"github.com/Masterminds/semver/v3"
)

func TestRecordIsBehind(t *testing.T) {
	t.Parallel()

	running := semver.MustParse("1.36.2")

	tests := []struct {
		name     string
		recorded string
		want     bool
	}{
		{
			// The observed failure: the recorded version sat a minor behind
			// what every node was running.
			name:     "recorded version is older",
			recorded: "1.35.3",
			want:     true,
		},
		{
			name:     "recorded version matches",
			recorded: "1.36.2",
			want:     false,
		},
		{
			// An orchestrated upgrade may record its target before the nodes
			// get there. Dragging it back would undo the upgrade's own record.
			name:     "recorded version is newer",
			recorded: "1.37.0",
			want:     false,
		},
		{
			name:     "nothing recorded yet",
			recorded: "",
			want:     true,
		},
		{
			// A value no reader can parse is not a reason to keep reporting it.
			name:     "recorded version is unparseable",
			recorded: "latest",
			want:     true,
		},
		{
			name:     "prefixed record still compares",
			recorded: "v1.35.3",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := recordIsBehind(tt.recorded, running); got != tt.want {
				t.Errorf("recordIsBehind(%q, %s) = %v, want %v", tt.recorded, running, got, tt.want)
			}
		})
	}
}
