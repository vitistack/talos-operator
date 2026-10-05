package upgradeservice

import "testing"

const (
	testTag        = "v1.12.7"
	testFactoryRef = "factory.talos.dev/nocloud-installer/b0f2a8b575460a3dcb1234cc081c73c88e795aaef36eda9b88a6f4dddbd49365"
)

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
