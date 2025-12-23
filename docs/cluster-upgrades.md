# Cluster Upgrades

This document describes how to upgrade Talos OS and Kubernetes versions on clusters managed by the talos-operator.

## Overview

The talos-operator uses an **annotation-based upgrade system** that is provider-agnostic and gives cluster owners full control over when upgrades occur. There are two separate upgrade paths:

1. **Talos OS Upgrade** - Upgrades the underlying Talos Linux operating system
2. **Kubernetes Version Upgrade** - Upgrades Kubernetes components (API server, kubelet, etc.)

> **Important:** Talos must be upgraded **before** Kubernetes if the new Kubernetes version requires a newer Talos version.

## How Talos Upgrades Work

Understanding the Talos upgrade mechanism helps explain why upgrades are safe and what happens during the process.

### A/B Partition Scheme

Talos uses an A/B partition layout for safe, atomic upgrades:

```
┌────────────────────────────────────────┐
│            DISK LAYOUT                 │
├────────────────────────────────────────┤
│  EFI Partition (boot)                  │
├────────────────────────────────────────┤
│  SYSTEM-A  ◄── Currently running       │
│  (Talos 1.11.6)                        │
├────────────────────────────────────────┤
│  SYSTEM-B  ◄── Upgrade target          │
│  (Talos 1.12.0 written here)           │
├────────────────────────────────────────┤
│  STATE (persistent data)               │
│  - Machine config                      │
│  - etcd data (control planes)          │
├────────────────────────────────────────┤
│  EPHEMERAL (pods, containers)          │
└────────────────────────────────────────┘
```

This design means:

- **No in-place modification** - The new version is written to the inactive partition
- **Atomic switch** - Bootloader switches partitions on reboot
- **Easy rollback** - Previous version remains intact on the other partition

### Per-Node Upgrade Process

When each node is upgraded, it goes through these stages:

```
┌─────────────────────────────────────────────────────────────────┐
│  1. DOWNLOAD NEW IMAGE                                          │
│     Node downloads the new Talos installer image                │
│     (e.g., ghcr.io/siderolabs/installer:v1.12.0)               │
│     Duration: ~30-60 seconds (depends on network)               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. STAGE THE UPGRADE                                           │
│     New image is written to the inactive system partition       │
│     Duration: ~10-30 seconds                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  3. REBOOT                                                      │
│     Node reboots into the new partition with new Talos version  │
│     Duration: ~30-90 seconds                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  4. REJOIN CLUSTER                                              │
│     - Talos services start                                      │
│     - Kubelet registers with API server                         │
│     - etcd member rejoins (control planes only)                 │
│     Duration: ~30-60 seconds                                    │
└─────────────────────────────────────────────────────────────────┘
```

**Total time per node: ~2-4 minutes**

### Rolling Upgrade Strategy

The operator performs upgrades **one node at a time** to maintain cluster availability:

```
1. WORKERS FIRST (less critical, can be drained)
   ├── worker-0  ──► upgrade ──► wait for ready ──► ✓
   ├── worker-1  ──► upgrade ──► wait for ready ──► ✓
   └── worker-2  ──► upgrade ──► wait for ready ──► ✓

2. CONTROL PLANES LAST (critical, maintains etcd quorum)
   ├── cp-0  ──► upgrade ──► wait for etcd healthy ──► ✓
   ├── cp-1  ──► upgrade ──► wait for etcd healthy ──► ✓
   └── cp-2  ──► upgrade ──► wait for etcd healthy ──► ✓
```

**Why workers first?**

- Worker failures have less impact on cluster stability
- Control plane must remain available to reschedule pods
- Validates the upgrade works before touching critical nodes

**Why one at a time?**

- Maintains etcd quorum (for 3 control planes, 2 must be healthy)
- Allows workloads to be rescheduled
- Enables early failure detection

### What Gets Upgraded vs Preserved

| Component            | Upgraded? | Notes                                         |
| -------------------- | --------- | --------------------------------------------- |
| Talos kernel         | ✅ Yes    | New Linux kernel version                      |
| Talos userspace      | ✅ Yes    | containerd, kubelet binaries, system services |
| System extensions    | ✅ Yes    | If included in the installer image            |
| Machine config       | ❌ No     | Preserved in STATE partition                  |
| etcd data            | ❌ No     | Preserved in STATE partition                  |
| Certificates         | ❌ No     | Preserved in STATE partition                  |
| Kubernetes workloads | ❌ No     | Rescheduled during node reboot                |

### Pre-flight Checks

Before upgrading each node, the operator verifies:

1. **etcd health** - Cluster must have quorum before proceeding
2. **Talos API reachable** - Node must be responding to API calls
3. **Version validation** - Target version must be newer than current

### Rollback

If a node fails to come back after upgrade, Talos can roll back:

**Automatic rollback**: If the new system fails to boot properly, the bootloader can fall back to the previous partition.

**Manual rollback**: You can explicitly roll back a node:

```bash
talosctl rollback --nodes <node-ip>
```

This switches back to the previous system partition without losing data.

### Upgrade Timeline Example

For a typical 5-node cluster (3 control planes + 2 workers):

| Node      | Type          | Time        |
| --------- | ------------- | ----------- |
| worker-0  | Worker        | ~3 min      |
| worker-1  | Worker        | ~3 min      |
| cp-0      | Control Plane | ~4 min      |
| cp-1      | Control Plane | ~4 min      |
| cp-2      | Control Plane | ~4 min      |
| **Total** |               | **~18 min** |

> **Note:** Times vary based on network speed, hardware, and cluster size.

## How Kubernetes Upgrades Work

Kubernetes upgrades in Talos are **different from Talos OS upgrades** - no reboots are required.

### What Gets Upgraded

Talos manages Kubernetes components directly:

```
CONTROL PLANE NODES:
├── kube-apiserver          → Updated image
├── kube-controller-manager → Updated image
├── kube-scheduler          → Updated image
└── kubelet                 → Updated binary

WORKER NODES:
└── kubelet                 → Updated binary

CLUSTER-WIDE:
├── kube-proxy (DaemonSet)  → Updated image
└── CoreDNS (Deployment)    → Compatible version
```

### Upgrade Process

```
┌─────────────────────────────────────────────────────────────────┐
│  1. UPDATE CONTROL PLANE COMPONENTS                             │
│     Static pods are updated with new images                     │
│     (kube-apiserver, controller-manager, scheduler)             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. UPDATE KUBELET ON ALL NODES                                 │
│     Talos updates kubelet binary and restarts it                │
│     No node reboot required!                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  3. UPDATE CLUSTER COMPONENTS                                   │
│     kube-proxy DaemonSet and CoreDNS Deployment updated         │
└─────────────────────────────────────────────────────────────────┘
```

### Key Differences from Talos Upgrade

| Aspect                 | Talos OS Upgrade | Kubernetes Upgrade             |
| ---------------------- | ---------------- | ------------------------------ |
| Reboot required        | ✅ Yes           | ❌ No                          |
| Rolling (node-by-node) | ✅ Yes           | ❌ Components updated directly |
| Downtime               | Brief per node   | Minimal/none                   |
| Duration               | ~2-4 min/node    | ~5-10 min total                |

### Version Skew Policy

Kubernetes requires upgrading **one minor version at a time**:

```
✅ Valid:   1.33 → 1.34 → 1.35
❌ Invalid: 1.33 → 1.35 (skipping 1.34)
```

This ensures compatibility between control plane and kubelet versions.

## Upgrade Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  1. Operator detects new Talos version available                │
│     → Sets: upgrade.vitistack.io/talos-available: "1.12.0"      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. User triggers Talos upgrade                                 │
│     → Sets: upgrade.vitistack.io/talos-target: "1.12.0"         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  3. Operator performs rolling Talos upgrade                     │
│     → Status: upgrade.vitistack.io/talos-status: "in-progress"  │
│     → Phase:  status.phase: "UpgradingTalos"                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  4. Talos upgrade completes                                     │
│     → Status: upgrade.vitistack.io/talos-status: "completed"    │
│     → K8s upgrade now available                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  5. User triggers Kubernetes upgrade                            │
│     → Sets: upgrade.vitistack.io/kubernetes-target: "1.35.0"    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  6. Operator performs Kubernetes upgrade                        │
│     → Status: upgrade.vitistack.io/kubernetes-status: "in-progress" │
│     → Phase:  status.phase: "UpgradingKubernetes"               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  7. Kubernetes upgrade completes                                │
│     → Status: upgrade.vitistack.io/kubernetes-status: "completed" │
│     → Phase:  status.phase: "Ready"                             │
└─────────────────────────────────────────────────────────────────┘
```

## Annotations Reference

### Talos Upgrade Annotations

| Annotation                             | Set By   | Description                                                  |
| -------------------------------------- | -------- | ------------------------------------------------------------ |
| `upgrade.vitistack.io/talos-available` | Operator | New Talos version available for upgrade                      |
| `upgrade.vitistack.io/talos-current`   | Operator | Current running Talos version                                |
| `upgrade.vitistack.io/talos-target`    | **User** | Target version to upgrade to (triggers upgrade)              |
| `upgrade.vitistack.io/talos-status`    | Operator | Current status: `idle`, `in-progress`, `completed`, `failed` |
| `upgrade.vitistack.io/talos-message`   | Operator | Human-readable status message                                |
| `upgrade.vitistack.io/talos-progress`  | Operator | Progress indicator (e.g., "2/5" nodes upgraded)              |

### Kubernetes Upgrade Annotations

| Annotation                                  | Set By   | Description                                                             |
| ------------------------------------------- | -------- | ----------------------------------------------------------------------- |
| `upgrade.vitistack.io/kubernetes-available` | Operator | New K8s version available (only after Talos is compatible)              |
| `upgrade.vitistack.io/kubernetes-current`   | Operator | Current running Kubernetes version                                      |
| `upgrade.vitistack.io/kubernetes-target`    | **User** | Target version to upgrade to (triggers upgrade)                         |
| `upgrade.vitistack.io/kubernetes-status`    | Operator | Current status: `idle`, `in-progress`, `completed`, `failed`, `blocked` |
| `upgrade.vitistack.io/kubernetes-message`   | Operator | Human-readable status message                                           |

## Upgrading Talos OS

### Step 1: Check Available Upgrades

View the current cluster annotations to see if an upgrade is available:

```bash
kubectl get kubernetescluster my-cluster -o jsonpath='{.metadata.annotations}' | jq
```

Look for:

```json
{
  "upgrade.vitistack.io/talos-available": "1.12.0",
  "upgrade.vitistack.io/talos-current": "1.11.6",
  "upgrade.vitistack.io/talos-status": "idle",
  "upgrade.vitistack.io/talos-message": "Upgrade available: 1.11.6 → 1.12.0"
}
```

### Step 2: Trigger the Upgrade

Set the target version annotation to trigger the upgrade:

```bash
kubectl annotate kubernetescluster my-cluster \
  upgrade.vitistack.io/talos-target="1.12.0"
```

Or using `kubectl patch`:

```bash
kubectl patch kubernetescluster my-cluster --type=merge -p '
metadata:
  annotations:
    upgrade.vitistack.io/talos-target: "1.12.0"
'
```

### Step 3: Monitor Progress

Watch the upgrade progress:

```bash
# Watch annotations
kubectl get kubernetescluster my-cluster -w -o jsonpath='{.metadata.annotations.upgrade\.vitistack\.io/talos-message}{"\n"}'

# Check status
kubectl get kubernetescluster my-cluster -o jsonpath='{.status.phase}'
# Output: UpgradingTalos

# Check progress
kubectl get kubernetescluster my-cluster -o jsonpath='{.metadata.annotations.upgrade\.vitistack\.io/talos-progress}'
# Output: 2/5
```

### Step 4: Verify Completion

Once complete, verify the upgrade:

```bash
kubectl get kubernetescluster my-cluster -o jsonpath='{.metadata.annotations.upgrade\.vitistack\.io/talos-current}'
# Output: 1.12.0

kubectl get kubernetescluster my-cluster -o jsonpath='{.metadata.annotations.upgrade\.vitistack\.io/talos-status}'
# Output: completed
```

## Upgrading Kubernetes

### Prerequisites

- Talos OS must be at a compatible version for the target Kubernetes version
- No Talos upgrade should be in progress or pending

### Step 1: Check Available Upgrades

```bash
kubectl get kubernetescluster my-cluster -o jsonpath='{.metadata.annotations}' | jq
```

Look for:

```json
{
  "upgrade.vitistack.io/kubernetes-available": "1.35.0",
  "upgrade.vitistack.io/kubernetes-current": "1.34.1",
  "upgrade.vitistack.io/kubernetes-status": "idle"
}
```

> **Note:** The `kubernetes-available` annotation only appears after Talos is at a compatible version.

### Step 2: Trigger the Upgrade

```bash
kubectl annotate kubernetescluster my-cluster \
  upgrade.vitistack.io/kubernetes-target="1.35.0"
```

### Step 3: Monitor Progress

```bash
kubectl get kubernetescluster my-cluster -o jsonpath='{.status.phase}'
# Output: UpgradingKubernetes

kubectl get kubernetescluster my-cluster -o jsonpath='{.metadata.annotations.upgrade\.vitistack\.io/kubernetes-message}'
```

### Step 4: Verify Completion

```bash
kubectl get kubernetescluster my-cluster -o jsonpath='{.metadata.annotations.upgrade\.vitistack\.io/kubernetes-current}'
# Output: 1.35.0
```

## Blocked Upgrades

Kubernetes upgrades may be blocked if:

1. **Talos upgrade is in progress** - Wait for Talos upgrade to complete
2. **Talos version is incompatible** - Upgrade Talos first

When blocked, you'll see:

```json
{
  "upgrade.vitistack.io/kubernetes-status": "blocked",
  "upgrade.vitistack.io/kubernetes-message": "Talos 1.11.6 does not support K8s 1.35.0. Upgrade Talos first."
}
```

## Failed Upgrades

If an upgrade fails, the status will show:

```json
{
  "upgrade.vitistack.io/talos-status": "failed",
  "upgrade.vitistack.io/talos-message": "Talos upgrade failed: node cp-1 did not come back online"
}
```

The cluster phase will be `UpgradeFailed`.

### Recovering from Failed Upgrades

1. Check the operator logs for detailed error information:

   ```bash
   kubectl logs -n vitistack-system deploy/talos-operator -f
   ```

2. Investigate the specific node that failed

3. To retry the upgrade, first clear the failed status by removing the target annotation, then set it again:

   ```bash
   # Remove the target annotation
   kubectl annotate kubernetescluster my-cluster \
     upgrade.vitistack.io/talos-target-

   # Set it again to retry
   kubectl annotate kubernetescluster my-cluster \
     upgrade.vitistack.io/talos-target="1.12.0"
   ```

## Upgrade Order and Best Practices

### Recommended Upgrade Order

1. **Workers first, then Control Planes** - The operator automatically upgrades worker nodes before control plane nodes to minimize disruption

2. **One node at a time** - Rolling upgrades are performed one node at a time with health checks between each node

3. **Talos before Kubernetes** - Always upgrade Talos OS before upgrading Kubernetes version

### Version Constraints

- **Kubernetes**: Can only upgrade one minor version at a time (e.g., 1.30 → 1.31 → 1.32)
- **Talos**: Check the [Talos support matrix](https://www.talos.dev/docs/support-matrix/) for compatible Kubernetes versions

### Pre-upgrade Checklist

- [ ] Verify etcd cluster is healthy
- [ ] Ensure sufficient cluster resources
- [ ] Check Talos/Kubernetes version compatibility
- [ ] Review release notes for breaking changes
- [ ] Have a backup/recovery plan
- [ ] Schedule upgrade during maintenance window

## Status Phases

| Phase                 | Description                                    |
| --------------------- | ---------------------------------------------- |
| `Ready`               | Cluster is healthy and running                 |
| `UpgradingTalos`      | Talos OS upgrade in progress                   |
| `UpgradingKubernetes` | Kubernetes upgrade in progress                 |
| `UpgradeFailed`       | Upgrade failed - check annotations for details |

## Conditions

The operator sets conditions to track upgrade state:

```yaml
status:
  conditions:
    - type: TalosUpgrade
      status: "True"
      reason: InProgress
      message: "Upgrading node cp-0 (2/5 nodes completed)"
    - type: KubernetesUpgrade
      status: "False"
      reason: Blocked
      message: "Waiting for Talos upgrade to complete"
```

## Example: Full Upgrade Workflow

```bash
# 1. Check current versions
kubectl get kubernetescluster my-cluster -o yaml | grep -A5 "annotations:"

# 2. Trigger Talos upgrade
kubectl annotate kubernetescluster my-cluster \
  upgrade.vitistack.io/talos-target="1.12.0"

# 3. Wait for Talos upgrade to complete
kubectl wait kubernetescluster my-cluster \
  --for=jsonpath='{.metadata.annotations.upgrade\.vitistack\.io/talos-status}'=completed \
  --timeout=30m

# 4. Trigger Kubernetes upgrade
kubectl annotate kubernetescluster my-cluster \
  upgrade.vitistack.io/kubernetes-target="1.35.0"

# 5. Wait for Kubernetes upgrade to complete
kubectl wait kubernetescluster my-cluster \
  --for=jsonpath='{.metadata.annotations.upgrade\.vitistack\.io/kubernetes-status}'=completed \
  --timeout=30m

# 6. Verify final state
kubectl get kubernetescluster my-cluster -o jsonpath='{.status.phase}'
# Output: Ready
```
