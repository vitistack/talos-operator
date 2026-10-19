package talos

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestComputeLabelTaintPatch_AddsLabelsAndTaints(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{ResourceVersion: "7", Labels: map[string]string{"keep": "x"}},
		Spec:       corev1.NodeSpec{Taints: []corev1.Taint{{Key: "node.kubernetes.io/not-ready", Effect: corev1.TaintEffectNoSchedule}}},
	}
	desired := []corev1.Taint{{Key: "gpu", Value: "true", Effect: corev1.TaintEffectNoSchedule}}

	patch := computeLabelTaintPatch(node, map[string]string{"pool": "gpu"}, desired, nil)
	if patch == nil {
		t.Fatal("expected patch")
	}
	md := patch["metadata"].(map[string]any)
	if md["resourceVersion"] != "7" {
		t.Errorf("expected resourceVersion precondition, got %v", md["resourceVersion"])
	}
	if md["labels"].(map[string]any)["pool"] != "gpu" {
		t.Errorf("expected pool label, got %v", md["labels"])
	}
	taints := patch["spec"].(map[string]any)["taints"].([]corev1.Taint)
	if len(taints) != 2 {
		t.Fatalf("expected unmanaged taint kept plus desired taint, got %v", taints)
	}
	ann := md["annotations"].(map[string]any)
	if ann[managedNodeLabelsAnnotation] != `["pool"]` || ann[managedNodeTaintsAnnotation] != `["gpu:NoSchedule"]` {
		t.Errorf("unexpected managed annotations: %v", ann)
	}
}

func TestComputeLabelTaintPatch_RemovesPreviouslyManaged(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"pool": "gpu", "manual": "y"},
			Annotations: map[string]string{
				managedNodeLabelsAnnotation: `["pool"]`,
				managedNodeTaintsAnnotation: `["gpu:NoSchedule"]`,
			},
		},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{
			{Key: "gpu", Value: "true", Effect: corev1.TaintEffectNoSchedule},
			{Key: "manual", Effect: corev1.TaintEffectNoExecute},
		}},
	}

	patch := computeLabelTaintPatch(node, nil, nil, nil)
	md := patch["metadata"].(map[string]any)
	labels := md["labels"].(map[string]any)
	if v, ok := labels["pool"]; !ok || v != nil {
		t.Errorf("expected pool label removal, got %v", labels)
	}
	if _, ok := labels["manual"]; ok {
		t.Errorf("unmanaged label must not be touched")
	}
	taints := patch["spec"].(map[string]any)["taints"].([]corev1.Taint)
	if len(taints) != 1 || taints[0].Key != "manual" {
		t.Errorf("expected only unmanaged taint left, got %v", taints)
	}
	ann := md["annotations"].(map[string]any)
	if v, ok := ann[managedNodeLabelsAnnotation]; !ok || v != nil {
		t.Errorf("expected managed labels annotation removal, got %v", ann)
	}
}

func TestComputeLabelTaintPatch_InSync(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"pool": "gpu"},
			Annotations: map[string]string{
				managedNodeLabelsAnnotation: `["pool"]`,
				managedNodeTaintsAnnotation: `["gpu:NoSchedule"]`,
			},
		},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: "gpu", Value: "true", Effect: corev1.TaintEffectNoSchedule}}},
	}
	desired := []corev1.Taint{{Key: "gpu", Value: "true", Effect: corev1.TaintEffectNoSchedule}}

	if patch := computeLabelTaintPatch(node, map[string]string{"pool": "gpu"}, desired, nil); patch != nil {
		t.Errorf("expected no patch, got %v", patch)
	}
}

func TestComputeLabelTaintPatch_SkipsTalosOwnedLabels(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{"pool": "old"},
			Annotations: map[string]string{talosOwnedLabelsKey: `["pool"]`},
		},
	}
	if patch := computeLabelTaintPatch(node, map[string]string{"pool": "new"}, nil, nil); patch != nil {
		t.Errorf("expected Talos-owned label to be skipped, got %v", patch)
	}
}

// A bootstrap taint has no Node-side ownership record yet (or lost it when the Node
// re-registered); the Machine's registration record must still let it be pruned.
func TestComputeLabelTaintPatch_PrunesRegisteredTaintWithoutNodeRecord(t *testing.T) {
	node := &corev1.Node{
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{
			{Key: "gpu", Value: "true", Effect: corev1.TaintEffectNoSchedule},
			{Key: "manual", Effect: corev1.TaintEffectNoExecute},
		}},
	}

	patch := computeLabelTaintPatch(node, nil, nil, []string{"gpu:NoSchedule"})
	if patch == nil {
		t.Fatal("expected patch removing registered taint")
	}
	taints := patch["spec"].(map[string]any)["taints"].([]corev1.Taint)
	if len(taints) != 1 || taints[0].Key != "manual" {
		t.Errorf("expected only unmanaged taint left, got %v", taints)
	}
}

func TestComputeLabelTaintPatch_RegisteredTaintEffectChanged(t *testing.T) {
	node := &corev1.Node{
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: "gpu", Value: "true", Effect: corev1.TaintEffectNoSchedule}}},
	}
	desired := []corev1.Taint{{Key: "gpu", Value: "true", Effect: corev1.TaintEffectNoExecute}}

	patch := computeLabelTaintPatch(node, nil, desired, []string{"gpu:NoSchedule"})
	taints := patch["spec"].(map[string]any)["taints"].([]corev1.Taint)
	if len(taints) != 1 || taints[0].Effect != corev1.TaintEffectNoExecute {
		t.Errorf("expected only the new-effect taint, got %v", taints)
	}
}
