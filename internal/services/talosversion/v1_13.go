//nolint:dupl // Each version adapter intentionally has similar structure
package talosversion

// V1_13Adapter handles Talos v1.13.x configuration
// This is a forward-looking adapter that extends v1.12.x behavior
type V1_13Adapter struct {
	multiDocAdapter
}

// NewV1_13Adapter creates a new v1.13.x adapter
func NewV1_13Adapter() *V1_13Adapter {
	return &V1_13Adapter{
		multiDocAdapter: multiDocAdapter{
			baseAdapter: baseAdapter{
				config: adapterConfig{
					version:           "1.13.x",
					kubernetesVersion: "1.36.0", // Estimated
					etcdVersion:       "3.6.6",
					etcdRegistry:      "registry.k8s.io/etcd",
					multiDoc:          true,
					grubUKICmdline:    true,
				},
			},
		},
	}
}
