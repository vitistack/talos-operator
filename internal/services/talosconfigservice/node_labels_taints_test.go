package talosconfigservice

import (
	"strings"
	"testing"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDesiredNodeLabelsAndTaints(t *testing.T) {
	cluster := &vitistackv1alpha1.KubernetesCluster{}
	cluster.Spec.Topology.ControlPlane.Metadata.Labels = map[string]string{"tier": "cp"}
	cluster.Spec.Topology.Workers.NodePools = []vitistackv1alpha1.KubernetesClusterNodePool{{
		Name:     "gpu",
		Metadata: vitistackv1alpha1.KubernetesClusterSpecMetadataDetails{Labels: map[string]string{"pool": "gpu"}},
		Taint: []vitistackv1alpha1.KubernetesClusterTaint{
			{Key: "gpu", Value: "true", Effect: "NoSchedule"},
			{Key: "bad", Value: "x", Effect: "Bogus"},
		},
	}}

	cp := &vitistackv1alpha1.Machine{ObjectMeta: metav1.ObjectMeta{
		Labels: map[string]string{vitistackv1alpha1.NodeRoleAnnotation: "control-plane"},
	}}
	labels, taints := DesiredNodeLabelsAndTaints(cluster, cp)
	if labels["tier"] != "cp" || len(taints) != 0 {
		t.Errorf("unexpected control plane result: %v %v", labels, taints)
	}

	worker := &vitistackv1alpha1.Machine{ObjectMeta: metav1.ObjectMeta{
		Labels:      map[string]string{vitistackv1alpha1.NodeRoleAnnotation: "worker"},
		Annotations: map[string]string{vitistackv1alpha1.NodePoolAnnotation: "gpu"},
	}}
	labels, taints = DesiredNodeLabelsAndTaints(cluster, worker)
	if labels["pool"] != "gpu" {
		t.Errorf("expected pool label, got %v", labels)
	}
	if len(taints) != 1 || taints[0].Key != "gpu" {
		t.Errorf("expected only valid taint, got %v", taints)
	}

	patch := buildRegisterWithTaintsPatch(taints)
	if !strings.Contains(patch, "registerWithTaints:") || !strings.Contains(patch, "effect: NoSchedule") {
		t.Errorf("unexpected patch: %s", patch)
	}
}
