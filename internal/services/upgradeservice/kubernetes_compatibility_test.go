package upgradeservice

import (
	"strings"
	"testing"
)

// ValidateKubernetesUpgradeTarget gates the annotation-driven path. It must
// refuse a target the running Talos release cannot support, while leaving
// every case it already accepted untouched.
func TestValidateKubernetesUpgradeTarget_TalosCompatibility(t *testing.T) {
	t.Parallel()

	s := &UpgradeService{}

	tests := []struct {
		name         string
		talosCurrent string
		current      string
		target       string
		wantErr      bool
		wantErrHas   string
	}{
		{
			// The bug: Talos 1.12 tops out at Kubernetes 1.35, but the
			// one-minor-at-a-time rule alone lets 1.35 -> 1.36 through.
			name:         "rejects target above the Talos ceiling",
			talosCurrent: "v1.12.7",
			current:      "1.35.0",
			target:       "1.36.3",
			wantErr:      true,
			// Machinery's own wording, which is what reaches the cluster.
			wantErrHas: "too new to be used with Talos",
		},
		{
			// Regression guard for the upgrade this gate was written after: a
			// patch above the adapter's default is still inside the supported
			// minor and must remain allowed.
			name:         "allows patch above the adapter default",
			talosCurrent: "v1.13.4",
			current:      "1.36.1",
			target:       "1.36.3",
			wantErr:      false,
		},
		{
			name:         "allows target at the ceiling",
			talosCurrent: "v1.12.7",
			current:      "1.34.1",
			target:       "1.35.4",
			wantErr:      false,
		},
		{
			// Fail open: an unknown Talos release must not freeze upgrades.
			name:         "allows when Talos version is a future release",
			talosCurrent: "v1.14.0",
			current:      "1.36.3",
			target:       "1.37.0",
			wantErr:      false,
		},
		{
			name:         "allows when Talos version is unknown",
			talosCurrent: "",
			current:      "1.35.0",
			target:       "1.36.3",
			wantErr:      false,
		},
		{
			// Pre-existing rules must still apply and must take precedence,
			// so their messages stay the ones operators see.
			name:         "still rejects a two-minor jump",
			talosCurrent: "v1.13.4",
			current:      "1.34.0",
			target:       "1.36.0",
			wantErr:      true,
			wantErrHas:   "one minor version at a time",
		},
		{
			name:         "still rejects a downgrade",
			talosCurrent: "v1.13.4",
			current:      "1.36.3",
			target:       "1.36.1",
			wantErr:      true,
			wantErrHas:   "must be greater than or equal",
		},
		{
			name:         "still allows a same-version re-apply",
			talosCurrent: "v1.13.4",
			current:      "1.36.3",
			target:       "1.36.3",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := s.ValidateKubernetesUpgradeTarget(tt.talosCurrent, tt.current, tt.target)
			if tt.wantErr && err == nil {
				t.Fatalf("ValidateKubernetesUpgradeTarget(%q, %q, %q) = nil, want error",
					tt.talosCurrent, tt.current, tt.target)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ValidateKubernetesUpgradeTarget(%q, %q, %q) = %v, want nil",
					tt.talosCurrent, tt.current, tt.target, err)
			}
			if tt.wantErrHas != "" && !strings.Contains(err.Error(), tt.wantErrHas) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tt.wantErrHas)
			}
		})
	}
}
