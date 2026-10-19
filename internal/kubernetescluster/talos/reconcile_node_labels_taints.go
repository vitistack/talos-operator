package talos

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/talosconfigservice"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Record which labels/taints came from the KubernetesCluster spec, so removing
// them from the spec removes them from the Node without touching anything else.
const (
	managedNodeLabelsAnnotation = "vitistack.io/managed-node-labels"
	managedNodeTaintsAnnotation = "vitistack.io/managed-node-taints"
	talosOwnedLabelsKey         = "talos.dev/owned-labels"
)

// registeredNodeTaintsAnnotation lives on the Machine and lists every taint ever written
// to the node's kubelet registerWithTaints; the kubelet re-adds them if the Node re-registers.
const registeredNodeTaintsAnnotation = "vitistack.io/registered-node-taints"

// reconcileNodeLabelsAndTaints propagates spec.topology controlPlane/nodePool
// metadata.labels and nodePool taints to the workload cluster's Nodes.
func (t *TalosManager) reconcileNodeLabelsAndTaints(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, nodes *workloadNodes) error {
	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get cluster machines for label/taint reconciliation: %w", err)
	}
	machineMap := make(map[string]*vitistackv1alpha1.Machine, len(machines))
	for _, m := range machines {
		machineMap[m.Name] = m
	}

	for i := range nodes.items {
		node := &nodes.items[i]
		m, exists := machineMap[node.Name]
		if !exists {
			continue
		}

		labels, taints := talosconfigservice.DesiredNodeLabelsAndTaints(cluster, m)
		registered := parseKeyList(m.Annotations[registeredNodeTaintsAnnotation])
		patch := computeLabelTaintPatch(node, labels, taints, registered)
		if patch == nil {
			continue
		}

		patchBytes, err := json.Marshal(patch)
		if err != nil {
			vlog.Warn(fmt.Sprintf("Failed to marshal label/taint patch for node %s: %v", node.Name, err))
			continue
		}
		patched, err := nodes.client.CoreV1().Nodes().Patch(ctx, node.Name, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{})
		if err != nil {
			vlog.Warn(fmt.Sprintf("Failed to patch labels/taints on node %s: %v", node.Name, err))
			continue
		}
		nodes.items[i] = *patched
		vlog.Info(fmt.Sprintf("Reconciled labels/taints on node %s %s", node.Name, clusterLogTag(cluster)))
	}
	return nil
}

// recordRegisteredTaints adds the taints' IDs to the Machine's registration record.
// Entries are never removed: the kubelet config on the node keeps them until it is replaced.
func (t *TalosManager) recordRegisteredTaints(ctx context.Context, m *vitistackv1alpha1.Machine, taints []corev1.Taint) error {
	fresh := &vitistackv1alpha1.Machine{}
	if err := t.Get(ctx, k8stypes.NamespacedName{Name: m.Name, Namespace: m.Namespace}, fresh); err != nil {
		return err
	}
	ids := parseKeyList(fresh.Annotations[registeredNodeTaintsAnnotation])
	for _, taint := range taints {
		if id := taintID(taint); !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	encoded, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	if fresh.Annotations[registeredNodeTaintsAnnotation] == string(encoded) {
		return nil
	}

	patch := client.MergeFromWithOptions(fresh.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if fresh.Annotations == nil {
		fresh.Annotations = map[string]string{}
	}
	fresh.Annotations[registeredNodeTaintsAnnotation] = string(encoded)
	return t.Patch(ctx, fresh, patch)
}

// computeLabelTaintPatch returns a JSON merge patch for the Node, or nil when it is in sync.
// registeredTaints are taint IDs from the Machine's kubelet registration record and are
// pruned like previously managed taints when no longer desired.
// The patch carries the Node's resourceVersion because spec.taints is replaced as a whole.
func computeLabelTaintPatch(node *corev1.Node, desiredLabels map[string]string, desiredTaints []corev1.Taint, registeredTaints []string) map[string]any {
	metadata := map[string]any{}
	spec := map[string]any{}

	// Labels Talos owns via machine config nodeLabels would be reverted by Talos.
	talosOwned := parseTalosOwnedAnnotations(node.Annotations[talosOwnedLabelsKey])
	labelPatch := map[string]any{}
	managedLabels := make([]string, 0, len(desiredLabels))
	for k, v := range desiredLabels {
		if talosOwned[k] {
			continue
		}
		managedLabels = append(managedLabels, k)
		if cur, ok := node.Labels[k]; !ok || cur != v {
			labelPatch[k] = v
		}
	}
	for _, k := range parseKeyList(node.Annotations[managedNodeLabelsAnnotation]) {
		if _, desired := desiredLabels[k]; desired || talosOwned[k] {
			continue
		}
		if _, ok := node.Labels[k]; ok {
			labelPatch[k] = nil
		}
	}
	if len(labelPatch) > 0 {
		metadata["labels"] = labelPatch
	}

	managedTaints := make([]string, 0, len(desiredTaints))
	for _, t := range desiredTaints {
		managedTaints = append(managedTaints, taintID(t))
	}
	previous := append(parseKeyList(node.Annotations[managedNodeTaintsAnnotation]), registeredTaints...)
	newTaints := make([]corev1.Taint, 0, len(node.Spec.Taints)+len(desiredTaints))
	for _, t := range node.Spec.Taints {
		id := taintID(t)
		if slices.Contains(previous, id) || slices.Contains(managedTaints, id) {
			continue
		}
		newTaints = append(newTaints, t)
	}
	newTaints = append(newTaints, desiredTaints...)
	if !taintSetsEqual(node.Spec.Taints, newTaints) {
		spec["taints"] = newTaints
	}

	annPatch := map[string]any{}
	setManagedKeysAnnotation(annPatch, node.Annotations, managedNodeLabelsAnnotation, managedLabels)
	setManagedKeysAnnotation(annPatch, node.Annotations, managedNodeTaintsAnnotation, managedTaints)
	if len(annPatch) > 0 {
		metadata["annotations"] = annPatch
	}

	if len(metadata) == 0 && len(spec) == 0 {
		return nil
	}
	metadata["resourceVersion"] = node.ResourceVersion
	patch := map[string]any{"metadata": metadata}
	if len(spec) > 0 {
		patch["spec"] = spec
	}
	return patch
}

// taintID identifies a taint the way the API server does: by key and effect.
func taintID(t corev1.Taint) string {
	return t.Key + ":" + string(t.Effect)
}

func taintSetsEqual(a, b []corev1.Taint) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		if !slices.ContainsFunc(b, func(y corev1.Taint) bool {
			return x.Key == y.Key && x.Value == y.Value && x.Effect == y.Effect
		}) {
			return false
		}
	}
	return true
}

func parseKeyList(raw string) []string {
	if raw == "" {
		return nil
	}
	var keys []string
	if err := json.Unmarshal([]byte(raw), &keys); err != nil {
		return nil
	}
	return keys
}

// setManagedKeysAnnotation writes the sorted key list, or removes the annotation when empty.
func setManagedKeysAnnotation(patch map[string]any, current map[string]string, key string, keys []string) {
	cur, exists := current[key]
	if len(keys) == 0 {
		if exists {
			patch[key] = nil
		}
		return
	}
	slices.Sort(keys)
	encoded, _ := json.Marshal(keys)
	if cur != string(encoded) {
		patch[key] = string(encoded)
	}
}
