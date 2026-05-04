package talosversion

// Talos minor-version range identifiers used as the `version` field of
// each adapter and surfaced to callers (logs, ListSupportedVersions).
// Same string with the conventional "v" prefix is exposed alongside for
// users that emit version-prefixed labels.
const (
	versionV1_11 = "1.11.x"
	versionV1_12 = "1.12.x"
	versionV1_13 = "1.13.x"

	versionTagV1_11 = "v" + versionV1_11
	versionTagV1_12 = "v" + versionV1_12
	versionTagV1_13 = "v" + versionV1_13
)

// Shared adapter defaults for the post-v1.12 platform: etcd 3.6.6 from
// registry.k8s.io. v1.11 still ships gcr.io/etcd-development/etcd at
// 3.5.17 and is hardcoded in v1_11.go.
const (
	defaultEtcdVersion  = "3.6.6"
	defaultEtcdRegistry = "registry.k8s.io/etcd"
)
