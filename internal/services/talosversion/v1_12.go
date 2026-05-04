//nolint:dupl // Each version adapter intentionally has similar structure
package talosversion

// V1_12Adapter handles Talos v1.12.x configuration
type V1_12Adapter struct {
	multiDocAdapter
}

// NewV1_12Adapter creates a new v1.12.x adapter
func NewV1_12Adapter() *V1_12Adapter {
	return &V1_12Adapter{
		multiDocAdapter: multiDocAdapter{
			baseAdapter: baseAdapter{
				config: adapterConfig{
					version:           versionV1_12,
					kubernetesVersion: "1.35.3",
					etcdVersion:       defaultEtcdVersion,
					etcdRegistry:      defaultEtcdRegistry,
					multiDoc:          true,
					grubUKICmdline:    true,
				},
			},
		},
	}
}
