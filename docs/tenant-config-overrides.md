# Tenant Config Overrides

This operator can merge tenant/customer-specific Talos settings into every generated node configuration. The overrides live in a cluster-wide ConfigMap so they can be managed centrally.

## Overview

- Manifest location in repo: `hack/manifests/tenant-configmap.yaml`.
- Default ConfigMap name/namespace/key: `talos-tenant-config` in `default`, data key `config.yaml`.
- The operator replaces `#CLUSTERID#` with the reconciled `spec.cluster.clusterId` before applying the YAML fragment.
- The operator replaces `#MACADDRESS#` per-node with the MAC address resolved from the `NetworkConfiguration` CRD (or `Machine.Status.NetworkInterfaces` as fallback).
- Values are merged as YAML: map entries override defaults, lists replace the original arrays entirely.
- **Multi-document YAML** is supported (Talos v1.12+). Separate documents with `---`. The first document is the standard machine/cluster config; additional documents can be any Talos v1alpha1 config type (e.g. `ResolverConfig`, `TimeSyncConfig`, `LinkAliasConfig`, `LinkConfig`, `DHCPv4Config`).
- When overrides exist, the operator also writes `controlplane-tenant.yaml` and `worker-tenant.yaml` into the Talos cluster secret so you can inspect the merged role templates (these files contain the base template plus tenant patch, without node-specific tweaks or comments).
- The operator validates the full multi-document YAML at startup using `configpatcher.LoadPatches`. If the ConfigMap contains invalid YAML (e.g. indented `---` separators), the operator will exit with an error early.

## Applying the ConfigMap

Use the supervisor cluster kubeconfig (example path: `~/kubeconfig/viti-super-test.config`).

```bash
cd /Users/rogerwesterbo/dev/github/viti/talos-operator
kubectl --kubeconfig ~/kubeconfig/viti-super-test.config apply -f hack/manifests/tenant-configmap.yaml
```

Re-run the command whenever you change the manifest.

## Customizing the lookup

Environment variables (or matching Viper settings) control how the operator locates the ConfigMap:

| Setting                             | Env var                      | Default               | Description                                       |
| ----------------------------------- | ---------------------------- | --------------------- | ------------------------------------------------- |
| `consts.TENANT_CONFIGMAP_NAME`      | `TENANT_CONFIGMAP_NAME`      | `talos-tenant-config` | ConfigMap metadata.name                           |
| `consts.TENANT_CONFIGMAP_NAMESPACE` | `TENANT_CONFIGMAP_NAMESPACE` | `default`             | Namespace where the ConfigMap resides             |
| `consts.TENANT_CONFIGMAP_DATA_KEY`  | `TENANT_CONFIGMAP_DATA_KEY`  | `config.yaml`         | Key inside `.data` that stores the YAML overrides |

Update the operator deployment (env vars or ConfigMap) before reconciling clusters if you move the tenant ConfigMap.

## Editing the overrides

1. Start from the example in `hack/manifests/tenant-configmap.yaml`.
2. Keep the structure under `data.config.yaml:` valid Talos YAML. Remove sections you do not need; missing keys default back to generated settings.
3. Use `#CLUSTERID#` wherever you need the cluster ID string substituted at reconciliation time (for example in OIDC claims).
4. Use `#MACADDRESS#` in link selectors to match physical interfaces per-node (see multi-document example below).
5. Commit the changes if they should be tracked; otherwise maintain an external copy and just apply it via kubectl.

## Multi-document YAML (Talos v1.12+)

Starting with Talos v1.12, machine configuration supports multiple YAML documents. The tenant ConfigMap can contain additional config documents separated by `---`. The first document is the standard `machine`/`cluster` config patch. Additional documents can be any Talos v1alpha1 config type.

### Supported document types

| Kind | Purpose |
|------|---------|
| `ResolverConfig` | Custom DNS nameservers |
| `TimeSyncConfig` | NTP time synchronization settings |
| `LinkAliasConfig` | Network link aliases with CEL-based selectors |
| `LinkConfig` | Network link settings (MTU, etc.) |
| `DHCPv4Config` | DHCPv4 client configuration |
| `Layer2VIPConfig` | Virtual IP using Layer 2 (gratuitous ARP) for control plane HA |
| `HostnameConfig` | Hostname configuration (managed by operator, avoid overriding) |

### Example multi-document config

```yaml
# First document: standard machine/cluster config
machine:
  kubelet:
    extraArgs:
      rotate-server-certificates: true
cluster:
  proxy:
    disabled: true
---
# DNS resolver configuration
apiVersion: v1alpha1
kind: ResolverConfig
nameservers:
  - address: 10.0.0.53
---
# NTP time sync
apiVersion: v1alpha1
kind: TimeSyncConfig
enabled: true
bootTimeout: 2m0s
ntp:
  servers:
    - pool.ntp.org
---
# Link alias with per-node MAC address matching
apiVersion: v1alpha1
kind: LinkAliasConfig
name: net0
selector:
  match: mac(link.permanent_addr) == "#MACADDRESS#"
---
# Link MTU configuration
apiVersion: v1alpha1
kind: LinkConfig
name: net0
mtu: 1500
---
# DHCP client configuration
apiVersion: v1alpha1
kind: DHCPv4Config
name: net0
clientIdentifier: mac
---
# Layer 2 VIP for control plane HA (only applied to controlplane nodes)
# Uses gratuitous ARP to advertise the VIP from one controlplane at a time.
# Requirements: all controlplane nodes on same L2 network, IP must be reserved/unused.
apiVersion: v1alpha1
kind: Layer2VIPConfig
name: 10.0.0.100
link: net0
```

### Per-node MAC address substitution

The `#MACADDRESS#` placeholder is replaced per-node with the actual MAC address of the machine. The operator resolves the MAC from:

1. **NetworkConfiguration CRD** (primary, authoritative) — looks up the `NetworkConfiguration` resource matching the Machine name/namespace.
2. **Machine.Status.NetworkInterfaces** (fallback) — uses the first interface with a MAC address from the Machine status.

If no MAC address is available, the placeholder is left as-is (the node will still get its config, but link selectors using `#MACADDRESS#` will not match).

### Important notes on multi-document YAML

- The `---` document separator **must not be indented**. An indented separator is a common mistake and will cause a validation error at operator startup.
- `yaml.Unmarshal` only parses the first document. The operator preserves the full raw YAML and passes it to `configpatcher.LoadPatches`, which handles multi-document YAML correctly.
- All documents are validated at startup, during cluster initialization, and during upgrades.

## Example tenant metadata overrides

Add lightweight metadata without touching the operator code by setting annotations that travel with every node:

```yaml
machine:
  nodeAnnotations:
    tenant.vitistack.io/customer: a-cool-customer
    tenant.vitistack.io/environment: production
    tenant.vitistack.io/contact-email: ops@a-cool-customer.com
```

Because overrides are merged, these annotations sit on top of the defaults set by `TalosConfigService` and help tooling discover who owns a cluster.

## Verification steps

- After applying the ConfigMap, reconcile or create a `KubernetesCluster` resource and watch the operator logs for `Tenant overrides ConfigMap` messages.
- The operator validates the tenant ConfigMap at startup. If the YAML is invalid, the operator logs the error and exits — check logs for `ValidateTenantConfig` messages.
- Inspect the generated node configs (in the Talos secret or via Talos CLI) to ensure the merged settings match your expectations.
- If the ConfigMap is missing or empty, the operator logs a warning and proceeds with defaults.
