#!/bin/bash
# Debug script for worker join issues

# Replace these with your actual IPs
CONTROLPLANE_IP="10.28.0.193"  # The control plane VIP or actual IP
WORKER_IP="<WORKER_IP_HERE>"   # Replace with your worker IP
TALOSCONFIG="hack/talos-manifests/talosconfig"

echo "=== 1. Check Control Plane Status ==="
talosctl --talosconfig=$TALOSCONFIG -n $CONTROLPLANE_IP health 2>&1 || echo "Health check failed"

echo ""
echo "=== 2. Check Control Plane Members ==="
talosctl --talosconfig=$TALOSCONFIG -n $CONTROLPLANE_IP get members 2>&1 || echo "Get members failed"

echo ""
echo "=== 3. Check etcd Status ==="
talosctl --talosconfig=$TALOSCONFIG -n $CONTROLPLANE_IP etcd status 2>&1 || echo "Etcd status failed"

echo ""
echo "=== 4. Get Kubernetes Nodes ==="
talosctl --talosconfig=$TALOSCONFIG -n $CONTROLPLANE_IP kubeconfig /tmp/debug-kubeconfig 2>&1
kubectl --kubeconfig=/tmp/debug-kubeconfig get nodes -o wide 2>&1 || echo "Get nodes failed"

echo ""
echo "=== 5. Check Worker Talos API (requires insecure if not configured) ==="
talosctl --talosconfig=$TALOSCONFIG -n $WORKER_IP version --insecure 2>&1 || echo "Worker version check failed"

echo ""
echo "=== 6. Check Worker Services ==="
talosctl --talosconfig=$TALOSCONFIG -n $WORKER_IP services 2>&1 || echo "Worker services failed"

echo ""
echo "=== 7. Check Worker kubelet logs ==="
talosctl --talosconfig=$TALOSCONFIG -n $WORKER_IP logs kubelet --tail 50 2>&1 || echo "Kubelet logs failed"

echo ""
echo "=== 8. Check Worker Networking (can it reach control plane?) ==="
talosctl --talosconfig=$TALOSCONFIG -n $WORKER_IP read /proc/net/tcp 2>&1 | head -20 || echo "Failed to read network info"

echo ""
echo "=== 9. Check for Certificate Issues ==="
talosctl --talosconfig=$TALOSCONFIG -n $WORKER_IP dmesg 2>&1 | grep -i "tls\|cert\|x509" | tail -20 || echo "No cert issues found"

echo ""
echo "=== 10. Check Worker machined logs ==="
talosctl --talosconfig=$TALOSCONFIG -n $WORKER_IP logs machined --tail 30 2>&1 || echo "Machined logs failed"
