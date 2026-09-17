// Package talosextensions compares the Talos system extensions a cluster must
// have (TALOS_REQUIRED_EXTENSIONS) with the ones a node reports installed.
package talosextensions

import "strings"

// ParseRequired splits the comma-separated env value, trims whitespace, and
// drops empty entries. An empty value yields nil (the check is disabled).
func ParseRequired(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// Missing returns the entries from required that are not present in
// installed. Order follows required, so warning lines stay readable.
//
// Names are compared case-sensitively after stripping any "<author>/" prefix:
// Talos's ExtensionStatus reports the bare manifest name (e.g. "iscsi-tools")
// while the image-factory display path is "siderolabs/iscsi-tools". Both
// forms in the env var are accepted and treated as equivalent so operators
// can paste straight from factory.talos.dev without a transformation step.
func Missing(required, installed []string) []string {
	have := make(map[string]struct{}, len(installed))
	for _, e := range installed {
		have[normalize(e)] = struct{}{}
	}
	var missing []string
	for _, r := range required {
		if _, ok := have[normalize(r)]; !ok {
			missing = append(missing, r)
		}
	}
	return missing
}

// normalize returns the substring after the last "/" so that
// "siderolabs/iscsi-tools" and "iscsi-tools" compare equal.
func normalize(name string) string {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}
