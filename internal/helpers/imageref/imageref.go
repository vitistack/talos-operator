// Package imageref splits and rewrites container image references such as
// factory.talos.dev/nocloud-installer/<schematic>:v1.13.10@sha256:<digest>.
//
// A colon before the last "/" is a registry port ("localhost:5000/foo"), not
// a tag separator.
package imageref

import "strings"

// WithTag returns ref with its tag replaced by tag, or tag appended when ref
// has none. A digest is dropped: it pins the old image exactly and would
// contradict the new tag.
func WithTag(ref, tag string) string {
	return Base(ref) + ":" + tag
}

// Tag returns ref's tag, or "" when it has none.
func Tag(ref string) string {
	ref, _, _ = strings.Cut(ref, "@")
	if colon := strings.LastIndexByte(ref, ':'); colon > strings.LastIndexByte(ref, '/') {
		return ref[colon+1:]
	}
	return ""
}

// Base returns ref without its tag and digest.
func Base(ref string) string {
	ref, _, _ = strings.Cut(ref, "@")
	if colon := strings.LastIndexByte(ref, ':'); colon > strings.LastIndexByte(ref, '/') {
		return ref[:colon]
	}
	return ref
}
