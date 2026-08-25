package talos

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func nodesRunning(versions ...string) []corev1.Node {
	nodes := make([]corev1.Node, 0, len(versions))
	for _, v := range versions {
		var n corev1.Node
		n.Status.NodeInfo.KubeletVersion = v
		nodes = append(nodes, n)
	}
	return nodes
}

func TestHighestKubeletVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		nodes []corev1.Node
		want  string
	}{
		{
			// A settled cluster after an upgrade: every node agrees.
			name:  "all nodes on the same version",
			nodes: nodesRunning("v1.36.2", "v1.36.2", "v1.36.2", "v1.36.2"),
			want:  "1.36.2",
		},
		{
			// The straggler case reconcileNodeVersions exists for. Taking the
			// lowest would make the straggler the target and strand it there.
			name:  "one node lagging behind the rest",
			nodes: nodesRunning("v1.36.2", "v1.36.2", "v1.35.3"),
			want:  "1.36.2",
		},
		{
			name:  "mid-upgrade, most nodes not yet moved",
			nodes: nodesRunning("v1.35.3", "v1.35.3", "v1.36.2"),
			want:  "1.36.2",
		},
		{
			// An unreadable version must not sink the answer for the rest.
			name:  "one node reports something unparseable",
			nodes: nodesRunning("v1.36.2", "not-a-version"),
			want:  "1.36.2",
		},
		{
			name:  "nothing parses",
			nodes: nodesRunning("not-a-version"),
			want:  "",
		},
		{
			name:  "no nodes",
			nodes: nil,
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := highestKubeletVersion(tt.nodes); got != tt.want {
				t.Errorf("highestKubeletVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}
