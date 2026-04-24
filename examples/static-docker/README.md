# static-docker example

Manifests for exercising the Vitistack control plane on a Docker-hosted
Kubernetes cluster (e.g. `createlocalk8s`, kind-style Talos nodes).

**For the full setup, prerequisites, what works, what doesn't, and
troubleshooting, see
[docs/howtoguide/clusters/local-dev-docker.md](../../../docs/docs/howtoguide/clusters/local-dev-docker.md).**

Short version: these manifests validate the operator pipeline
(allocation → cidata synthesis → Talos static-IP application) end-to-end,
but the resulting VMs sit on an isolated per-node `br0` that's not routable
from the cluster pod network — so `talos-operator` cannot reach them and the
cluster does not fully bootstrap. That is expected for this topology; for a
reachable cluster, see [`../static/`](../static/) and run on bare metal.

## Apply order

```bash
kubectl create namespace static
kubectl -n static apply -f 00-machineclasses.yaml
kubectl -n static apply -f 01-kubevirtconfig.yaml
kubectl -n static apply -f networkNamespace.yaml
kubectl -n static apply -f kubernetescluster.yaml
```
