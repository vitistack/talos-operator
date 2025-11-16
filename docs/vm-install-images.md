# Virtual Machine Install Images Configuration

When deploying Talos clusters on virtual machine platforms (Proxmox, KubeVirt, libvirt, KVM, VMware, VirtualBox, Hyper-V), the operator can automatically configure platform-specific Talos install images and customizations.

## Automatic Customizations

When a machine's `status.provider` field indicates it's running on a virtual platform, the operator automatically adds:

1. **Serial console kernel arguments** for better debugging and access:

   ```yaml
   console=ttyS0,115200
   ```

2. **QEMU guest agent** system extension for better VM integration:

   ```yaml
   machine:
     install:
       customization:
         systemExtensions:
           officialExtensions:
             - siderolabs/qemu-guest-agent
   ```

3. **Platform-specific install images** (when configured):
   ```yaml
   machine:
     install:
       image: factory.talos.dev/openstack-installer/ce4c980550dd2ab1b17bbf2b08801c7eb59418eafe8f279833297925d67c7515:v1.11.5
   ```

## Configuration

Configure platform-specific Talos install images using environment variables:

### Provider-Specific Images

```bash
# Proxmox
TALOS_VM_INSTALL_IMAGE_PROXMOX="factory.talos.dev/openstack-installer/ce4c980550dd2ab1b17bbf2b08801c7eb59418eafe8f279833297925d67c7515:v1.11.5"

# KubeVirt
TALOS_VM_INSTALL_IMAGE_KUBEVIRT="factory.talos.dev/kubevirt-installer/abc123...:v1.11.5"

# libvirt
TALOS_VM_INSTALL_IMAGE_LIBVIRT="factory.talos.dev/libvirt-installer/def456...:v1.11.5"

# KVM
TALOS_VM_INSTALL_IMAGE_KVM="factory.talos.dev/kvm-installer/ghi789...:v1.11.5"

# VMware
TALOS_VM_INSTALL_IMAGE_VMWARE="factory.talos.dev/vmware-installer/jkl012...:v1.11.5"

# VirtualBox
TALOS_VM_INSTALL_IMAGE_VIRTUALBOX="factory.talos.dev/virtualbox-installer/mno345...:v1.11.5"

# Hyper-V
TALOS_VM_INSTALL_IMAGE_HYPERV="factory.talos.dev/hyperv-installer/pqr678...:v1.11.5"

# Default fallback for any unmatched virtual provider
TALOS_VM_INSTALL_IMAGE_DEFAULT="factory.talos.dev/nocloud-installer/b027a2d9dddfa5c0752c249cf3194bb5c62294dc7cba591f3bec8119ab578aea:v1.11.5"
```

### Image Selection Logic

The operator selects the install image based on the machine's `status.provider` field:

1. First, it checks for a provider-specific image configuration (e.g., `TALOS_VM_INSTALL_IMAGE_PROXMOX` for Proxmox)
2. If no provider-specific image is configured, it falls back to `TALOS_VM_INSTALL_IMAGE_DEFAULT`
3. If no images are configured, the customizations are still applied but without a custom install image

## Supported Providers

The following provider names (case-insensitive) trigger VM customizations:

- `proxmox`
- `kubevirt`
- `libvirt`
- `kvm` / `qemu`
- `vmware`
- `virtualbox`
- `hyper-v`

The provider detection is substring-based, so providers named like "proxmox-prod" or "kubevirt-cluster" will also match.

## Example Deployment

```yaml
apiVersion: vitistack.io/v1alpha1
kind: KubernetesCluster
metadata:
  name: vm-cluster
  namespace: default
spec:
  clusterName: vm-cluster
  kubernetes:
    version: "1.31.0"
  data:
    provider: talos
  topology:
    controlPlane:
      replicas: 3
    workers:
      nodePools:
        - name: default
          replicas: 3
          machineClass: medium
---
# Machines will be created with status.provider set by the machine provider
# When status.provider contains "proxmox", "kubevirt", etc., the VM customizations
# will be automatically applied during Talos configuration
```

## Generating Custom Install Images

You can generate custom Talos install images with specific extensions using the Talos Image Factory:

1. Visit https://factory.talos.dev/
2. Select your Talos version
3. Choose your platform (NoCloud, OpenStack, etc.)
4. Add any required system extensions (like qemu-guest-agent)
5. Generate the installer image URL

Example for OpenStack/NoCloud with QEMU guest agent:

```
factory.talos.dev/openstack-installer/ce4c980550dd2ab1b17bbf2b08801c7eb59418eafe8f279833297925d67c7515:v1.11.5
```

## Logging

When VM customizations are applied, the operator logs:

```
Detected virtual machine provider proxmox for node vm-cluster-control-plane-0, applying VM customizations with install image: factory.talos.dev/openstack-installer/...:v1.11.5
```

Or without a custom image:

```
Detected virtual machine provider proxmox for node vm-cluster-control-plane-0, applying VM customizations (no custom install image configured)
```
