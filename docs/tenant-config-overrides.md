# Tenant Config Overrides

This operator can merge tenant/customer-specific Talos settings into every generated node configuration. The overrides live in a cluster-wide ConfigMap so they can be managed centrally.

## Overview

- Manifest location in repo: `hack/manifests/tenant-configmap.yaml`.
- Default ConfigMap name/namespace/key: `talos-tenant-config` in `default`, data key `config.yaml`.
- The operator replaces `#CLUSTERID#` with the reconciled `spec.cluster.clusterId` before applying the YAML fragment.
- Values are merged as YAML: map entries override defaults, lists replace the original arrays entirely.
- When overrides exist, the operator also writes `controlplane-tenant.yaml` and `worker-tenant.yaml` into the Talos cluster secret so you can inspect the merged role templates (these files contain the base template plus tenant patch, without node-specific tweaks or comments).

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
4. Commit the changes if they should be tracked; otherwise maintain an external copy and just apply it via kubectl.

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
- Inspect the generated node configs (in the Talos secret or via Talos CLI) to ensure the merged settings match your expectations.
- If the ConfigMap is missing, empty, or malformed YAML, the operator logs a warning and proceeds with defaults.
