package talos

import (
	"context"
	"strings"
	"testing"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func kubeletNode(name, version string) corev1.Node {
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{KubeletVersion: version}},
	}
}

// Seen on cephtest-060 while it moved from 1.35.0 to 1.36.3: the desired
// version only advances when the rolling upgrade completes, so every pass
// during the upgrade called the already-upgraded control plane "newer than
// desired" and would have acted on the nodes the upgrade was handling.
func TestReconcileNodeVersions_LeavesNodesToTheRollingUpgrade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		inProgress string
		wantActed  bool
	}{
		{name: "rolling upgrade running", inProgress: "true", wantActed: false},
		{name: "no upgrade running", inProgress: "false", wantActed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cluster := &vitistackv1alpha1.KubernetesCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "cephtest-060", Namespace: "cephtest5"},
				Spec: vitistackv1alpha1.KubernetesClusterSpec{
					Cluster:  vitistackv1alpha1.KubernetesClusterSpecData{ClusterId: "ab60"},
					Topology: vitistackv1alpha1.KubernetesClusterSpecTopology{Version: "1.36.3"},
				},
			}
			// No talosconfig: acting on a stale node stops when loading the
			// Talos client config, which is how the test sees that it acted.
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "ab60", Namespace: "cephtest5"},
				Data:       map[string][]byte{"upgrade_in_progress": []byte(tt.inProgress)},
			}
			worker := &vitistackv1alpha1.Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cephtest-060-ab60-wrk1",
					Namespace: "cephtest5",
					Labels:    map[string]string{vitistackv1alpha1.ClusterIdAnnotation: "ab60"},
				},
				Status: vitistackv1alpha1.MachineStatus{PublicIPAddresses: []string{"100.64.9.21"}},
			}
			tm, _ := newStatusTestTalosManager(t, cluster, secret, worker)
			nodes := []corev1.Node{
				kubeletNode("cephtest-060-ab60-ctp0", "v1.36.3"),
				kubeletNode("cephtest-060-ab60-wrk1", "v1.35.0"),
			}

			err := tm.reconcileNodeVersions(context.Background(), cluster, nodes)

			acted := err != nil && strings.Contains(err.Error(), "failed to load talos client config")
			if acted != tt.wantActed {
				t.Errorf("acted on stale nodes = %v (err: %v), want %v", acted, err, tt.wantActed)
			}
		})
	}
}
