# Control Plane Endpoint Modes

The Talos Operator supports multiple modes for configuring how control plane endpoints are determined. This allows flexibility in how the Kubernetes API server is accessed, depending on your infrastructure setup.

## Configuration

The endpoint mode is configured via the `ENDPOINT_MODE` environment variable or through the Helm chart's `config.endpointMode` value.

### Environment Variables

| Variable          | Description                                   | Default                |
| ----------------- | --------------------------------------------- | ---------------------- |
| `ENDPOINT_MODE`   | The endpoint mode to use                      | `networkconfiguration` |
| `CUSTOM_ENDPOINT` | Custom endpoint address(es) for `custom` mode | (empty)                |

### Helm Chart Values

```yaml
config:
  endpointMode: "networkconfiguration" # Options: none, networkconfiguration, talosvip, custom
  customEndpoint: "" # Only used when endpointMode is "custom"
```

## Endpoint Modes

### `networkconfiguration` (Default)

Uses the `ControlPlaneVirtualSharedIP` resource from the `NetworkNamespace` to obtain load balancer IPs for the control plane.

**How it works:**

1. The operator creates a `ControlPlaneVirtualSharedIP` resource for the cluster
2. The NetworkNamespace controller provisions load balancer IPs
3. These IPs are used as the control plane endpoint in Talos configuration
4. As control planes are added, the VIP pool members are updated

**Use case:** Production environments with proper network infrastructure that supports load balancer provisioning.

**Example:**

```yaml
config:
  endpointMode: "networkconfiguration"
```

### `none`

Uses control plane IPs directly without any load balancing. The first control plane IP is used as the endpoint.

**How it works:**

1. The operator uses the first control plane's IP address directly
2. No VIP or load balancer is created
3. If the first control plane goes down, the endpoint becomes unavailable

**Use case:** Development/testing environments or single control plane setups.

**Example:**

```yaml
config:
  endpointMode: "none"
```

### `talosvip`

Uses Talos' built-in VIP feature for control plane high availability. This mode requires additional configuration in the Talos machine config via tenant overrides.

**How it works:**

1. The operator uses control plane IPs for initial configuration
2. Talos handles VIP failover internally through its machine config
3. The VIP address must be configured in tenant overrides

**Use case:** Environments where you want to use Talos' native VIP support instead of external load balancers.

**Tenant Override Example:**

```yaml
# In your tenant config ConfigMap
machine:
  network:
    interfaces:
      - interface: eth0
        vip:
          ip: 10.0.0.100 # Your VIP address
```

**Helm Values:**

```yaml
config:
  endpointMode: "talosvip"
```

### `custom`

Uses user-provided endpoint addresses. This allows you to specify your own load balancer, VIP, or DNS name.

**How it works:**

1. The operator reads endpoint addresses from the `CUSTOM_ENDPOINT` environment variable
2. These addresses are used directly in Talos configuration
3. No VIP resources are created or managed

**Use case:** Environments with existing load balancers, external VIP solutions, or DNS-based routing.

**Example with single endpoint:**

```yaml
config:
  endpointMode: "custom"
  customEndpoint: "10.0.0.100"
```

**Example with multiple endpoints:**

```yaml
config:
  endpointMode: "custom"
  customEndpoint: "lb1.example.com,lb2.example.com"
```

**Example with DNS:**

```yaml
config:
  endpointMode: "custom"
  customEndpoint: "k8s-api.example.com"
```

## Migration Between Modes

When changing endpoint modes on an existing cluster:

1. **To `none`**: Workers will continue to use the previously configured endpoint until reconfigured
2. **To `networkconfiguration`**: A new VIP will be created and workers may need reconfiguration
3. **To `talosvip`**: Update tenant overrides first, then change the mode
4. **To `custom`**: Ensure the custom endpoint is accessible before changing

**Note:** Changing endpoint modes on a running cluster is not recommended without proper planning. It may require reconfiguring worker nodes to use the new endpoint.

## Troubleshooting

### Mode Not Taking Effect

1. Verify the environment variable is set correctly:

   ```bash
   kubectl exec -n <namespace> <operator-pod> -- env | grep ENDPOINT_MODE
   ```

2. Check operator logs for endpoint mode selection:
   ```bash
   kubectl logs -n <namespace> <operator-pod> | grep "endpoint mode"
   ```

### Custom Endpoint Not Working

1. Ensure `CUSTOM_ENDPOINT` is set when using `custom` mode
2. Verify the endpoint is reachable from both the operator and the cluster nodes
3. Check for proper formatting (comma-separated for multiple endpoints)

### VIP Not Ready (networkconfiguration mode)

1. Check the `ControlPlaneVirtualSharedIP` resource status:

   ```bash
   kubectl get controlplanevirtualsharedip -n <namespace>
   ```

2. Verify the NetworkNamespace is properly configured
3. Check if the load balancer controller is functioning

## Best Practices

1. **Production**: Use `networkconfiguration` with proper network infrastructure
2. **Development**: Use `none` for simplicity
3. **Bare Metal with Talos VIP**: Use `talosvip` with proper tenant overrides
4. **Existing Infrastructure**: Use `custom` with your load balancer/DNS
