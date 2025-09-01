package status

import (
	"context"
	"time"

	vitistackv1alpha1 "github.com/vitistack/crds/pkg/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// StatusManager handles machine status updates and monitoring
type StatusManager struct {
	client.Client
}

// NewManager creates a new status manager
func NewManager(c client.Client) *StatusManager {
	return &StatusManager{
		Client: c,
	}
}

// UpdateMachineStatus updates the machine status with the given state
func (m *StatusManager) UpdateKubernetesClusterStatus(ctx context.Context, kubernetesCluster *vitistackv1alpha1.KubernetesCluster) error {
	// Derive phase and key conditions from consolidated Secret flags if present
	secretName := "k8s-" + kubernetesCluster.Name
	secret := &corev1.Secret{}
	err := m.Get(ctx, types.NamespacedName{Name: secretName, Namespace: kubernetesCluster.Namespace}, secret)
	if err != nil || secret.Data == nil {
		// Secret not found yet; mark Pending if not already
		_ = m.SetPhase(ctx, kubernetesCluster, "Pending")
		_ = m.SetCondition(ctx, kubernetesCluster, "TalosSecretReady", "False", "NotFound", "Talos secret not created yet")
		return nil
	}

	getFlag := func(key string) bool {
		if b, ok := secret.Data[key]; ok && string(b) == "true" {
			return true
		}
		return false
	}

	cfgPresent := getFlag("talosconfig_present") && getFlag("controlplane_yaml_present") && getFlag("worker_yaml_present")
	applied := getFlag("controlplane_applied") && getFlag("worker_applied")
	bootstrapped := getFlag("bootstrapped")
	clusterAccess := getFlag("cluster_access") || len(secret.Data["kube.config"]) > 0

	switch {
	case clusterAccess:
		_ = m.SetPhase(ctx, kubernetesCluster, "Ready")
		_ = m.SetCondition(ctx, kubernetesCluster, "KubeconfigAvailable", "True", "Persisted", "Kubeconfig stored in Secret")
		_ = m.SetCondition(ctx, kubernetesCluster, "Bootstrapped", "True", "Done", "Talos cluster bootstrapped")
		_ = m.SetCondition(ctx, kubernetesCluster, "ConfigApplied", "True", "Applied", "Talos configs applied to all nodes")
		_ = m.SetCondition(ctx, kubernetesCluster, "ConfigGenerated", "True", "Generated", "Talos client and role configs generated")
	case bootstrapped:
		_ = m.SetPhase(ctx, kubernetesCluster, "Bootstrapped")
		_ = m.SetCondition(ctx, kubernetesCluster, "Bootstrapped", "True", "Done", "Talos cluster bootstrapped")
		_ = m.SetCondition(ctx, kubernetesCluster, "ConfigApplied", "True", "Applied", "Talos configs applied to all nodes")
		_ = m.SetCondition(ctx, kubernetesCluster, "ConfigGenerated", "True", "Generated", "Talos client and role configs generated")
	case applied:
		_ = m.SetPhase(ctx, kubernetesCluster, "ConfigApplied")
		_ = m.SetCondition(ctx, kubernetesCluster, "ConfigApplied", "True", "Applied", "Talos configs applied to all nodes")
		_ = m.SetCondition(ctx, kubernetesCluster, "ConfigGenerated", "True", "Generated", "Talos client and role configs generated")
	case cfgPresent:
		_ = m.SetPhase(ctx, kubernetesCluster, "ConfigGenerated")
		_ = m.SetCondition(ctx, kubernetesCluster, "ConfigGenerated", "True", "Generated", "Talos client and role configs generated")
	default:
		_ = m.SetPhase(ctx, kubernetesCluster, "Pending")
	}
	return nil
}

// SetPhase sets the simple phase string on status.
func (m *StatusManager) SetPhase(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster, phase string) error {
	gvk := schema.GroupVersionKind{Group: "vitistack.io", Version: "v1alpha1", Kind: "KubernetesCluster"}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(kc.Namespace)
	u.SetName(kc.Name)

	if err := m.Get(ctx, client.ObjectKeyFromObject(kc), u); err != nil {
		return err
	}
	if err := unstructured.SetNestedField(u.Object, phase, "status", "phase"); err != nil {
		return err
	}
	if err := m.Status().Update(ctx, u); err != nil {
		// fallback for CRDs without status subresource
		return m.Update(ctx, u)
	}
	return nil
}

// SetCondition updates status.conditions with a condition entry (type, status, reason, message, lastTransitionTime).
// Uses unstructured to avoid coupling to generated condition types from external CRD module.
func (m *StatusManager) SetCondition(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster,
	condType, status, reason, message string,
) error {
	gvk := schema.GroupVersionKind{Group: "vitistack.io", Version: "v1alpha1", Kind: "KubernetesCluster"}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(kc.Namespace)
	u.SetName(kc.Name)

	if err := m.Get(ctx, client.ObjectKeyFromObject(kc), u); err != nil {
		return err
	}

	conds, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	if !found {
		conds = []interface{}{}
	}

	// Build new condition map
	newCond := map[string]interface{}{
		"type":               condType,
		"status":             status,
		"reason":             reason,
		"message":            message,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339Nano),
	}

	// Replace existing condition of same type or append
	replaced := false
	for i, ci := range conds {
		if cm, ok := ci.(map[string]interface{}); ok {
			if t, _ := cm["type"].(string); t == condType {
				// Check if unchanged
				if cm["status"] == status && cm["reason"] == reason && cm["message"] == message {
					return nil
				}
				conds[i] = newCond
				replaced = true
				break
			}
		}
	}
	if !replaced {
		conds = append(conds, newCond)
	}

	if err := unstructured.SetNestedSlice(u.Object, conds, "status", "conditions"); err != nil {
		return err
	}
	if err := m.Status().Update(ctx, u); err != nil {
		// fallback for CRDs without status subresource
		return m.Update(ctx, u)
	}
	return nil
}
