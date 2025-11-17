# Talos Operator

Talos operator for maintaining talos os

## Features

### KubernetesCluster Reconciler

The operator includes a comprehensive reconciler for KubernetesCluster CRDs that:

- **Watches KubernetesCluster resources** and generates Machine manifests based on cluster topology
- **Saves machine manifests** as YAML files in `hack/results/{cluster-name}/` for debugging and manual inspection
- **Applies Machine resources** to Kubernetes with proper owner references for automatic cleanup
- **Handles deletion** by cleaning up associated machines and generated files
- **Uses finalizers** to ensure graceful resource cleanup

#### Quick Start

1. Deploy the operator:

   ```bash
   make deploy IMG=ghcr.io/vitistack/talos-operator:latest
   ```

2. Create a KubernetesCluster:

   ```bash
   kubectl apply -f examples/simple-kubernetescluster.yaml
   ```

3. Verify machines are created:

   ```bash
   kubectl get machines -l cluster.vitistack.io/cluster-name=simple-cluster
   ```

4. Check generated files:
   ```bash
   ls hack/results/simple-cluster/
   ```

See [`docs/kubernetescluster-reconciler.md`](docs/kubernetescluster-reconciler.md) for detailed documentation.

## Tenant-specific Talos overrides

- Customize cluster-wide Talos settings through the ConfigMap manifest in `hack/manifests/tenant-configmap.yaml`. The data under `config.yaml` must be valid Talos machine/cluster YAML; the operator replaces `#CLUSTERID#` with the reconciled `spec.cluster.clusterId` before applying it. See `docs/tenant-config-overrides.md` for a deeper walkthrough.
- By default the operator looks for `talos-tenant-config` in the `default` namespace and merges it into every node config (lists are replaced, map entries override defaults). Use environment variables `TENANT_CONFIGMAP_NAME`, `TENANT_CONFIGMAP_NAMESPACE`, and `TENANT_CONFIGMAP_DATA_KEY` to change the lookup.
- Apply or update the ConfigMap in the supervisor cluster (example kubeconfig from this repo):
  ```bash
  kubectl --kubeconfig ~/kubeconfig/viti-super-test.config apply -f hack/manifests/tenant-configmap.yaml
  ```
- Remove or comment settings you do not want to override; missing keys default back to the Talos operator's generated values.

# Documentation

Using https://kubebuilder.io and https://kubevirt.io/user-guide/

## CI/CD

This project uses GitHub Actions for continuous integration and delivery:

- **Build and Tests**: Runs on each push and pull request to verify code integrity.
- **Security Scan**: Regular vulnerability scanning with govulncheck and CodeQL.
- **Release Process**: Tagged commits trigger automatic builds and publish to:
  - Container images: `ghcr.io/vitistack/talos-operator`
  - Helm charts: `oci://ghcr.io/vitistack/helm/talos-operator`
- **Dependabot**: Automated dependency updates for GitHub Actions, Go modules, Docker, and Helm charts.

### Creating a Release

To create a new release:

1. Tag the commit: `git tag -a v1.0.0 -m "Release v1.0.0"`
2. Push the tag: `git push origin v1.0.0`

The GitHub Actions workflow will automatically build and publish the container image and Helm chart.

### Dependency Management

Dependabot is configured to automatically open pull requests for:

- GitHub Actions workflow dependencies
- Go module dependencies
- Docker image dependencies
- Helm chart dependencies

Pull requests for minor and patch updates are automatically approved and merged. Major updates require manual review and approval.
