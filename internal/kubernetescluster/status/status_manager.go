package status

import (
	"context"
	"time"

	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/crds/pkg/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	phasePending       = "Pending"
	phaseConfigGen     = "ConfigGenerated"
	phaseConfigApplied = "ConfigApplied"
	phaseBootstrapped  = "Bootstrapped"
	phaseReady         = "Ready"
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
	// Load cluster Secret and derive phase/conditions
	secretName := "k8s-" + kubernetesCluster.Name
	secret := &corev1.Secret{}
	_ = m.Get(ctx, types.NamespacedName{Name: secretName, Namespace: kubernetesCluster.Namespace}, secret)

	phase, conds, kubeconfig := deriveStatusFromSecret(secret)
	_ = m.SetPhase(ctx, kubernetesCluster, phase)
	for _, c := range conds {
		_ = m.SetCondition(ctx, kubernetesCluster, c.Type, c.Status, c.Reason, c.Message)
	}

	// If kubeconfig is present, attempt to set status.state.created from kube-system Namespace creation time.
	if len(kubeconfig) > 0 {
		if ts, err := getKubeSystemCreated(ctx, kubeconfig); err == nil && !ts.IsZero() {
			_ = m.SetStateCreated(ctx, kubernetesCluster, ts)
			// If we can reach the target cluster via kubeconfig, mark Ready
			_ = m.SetPhase(ctx, kubernetesCluster, phaseReady)
		} else if err != nil {
			vlog.Debug("Failed to get kube-system creation time from target cluster: " + err.Error())
		}
	}
	// Aggregate machine info into status (best effort)
	_ = m.AggregateFromMachines(ctx, kubernetesCluster)
	return nil
}

// condSpec represents a status condition update request.
type condSpec struct {
	Type    string
	Status  string
	Reason  string
	Message string
}

// deriveStatusFromSecret returns phase, list of conditions to set, and kubeconfig bytes if found.
func deriveStatusFromSecret(secret *corev1.Secret) (string, []condSpec, []byte) {
	if secret == nil || secret.Data == nil {
		return phasePending, []condSpec{{Type: "TalosSecretReady", Status: "False", Reason: "NotFound", Message: "Talos secret not created yet"}}, nil
	}
	cfgPresent := getSecretFlag(secret, "talosconfig_present") && getSecretFlag(secret, "controlplane_yaml_present") && getSecretFlag(secret, "worker_yaml_present")
	applied := getSecretFlag(secret, "controlplane_applied") && getSecretFlag(secret, "worker_applied")
	bootstrapped := getSecretFlag(secret, "bootstrapped")
	clusterAccess := getSecretFlag(secret, "cluster_access") || len(secret.Data["kube.config"]) > 0

	phase := phaseFromFlags(cfgPresent, applied, bootstrapped, clusterAccess)
	conds := condsFromFlags(cfgPresent, applied, bootstrapped, clusterAccess)
	return phase, conds, secret.Data["kube.config"]
}

func getSecretFlag(secret *corev1.Secret, key string) bool {
	if secret == nil || secret.Data == nil {
		return false
	}
	if b, ok := secret.Data[key]; ok && string(b) == "true" {
		return true
	}
	return false
}

func phaseFromFlags(cfgPresent, applied, bootstrapped, clusterAccess bool) string {
	if cfgPresent && applied && bootstrapped && clusterAccess {
		return phaseReady
	}
	if bootstrapped {
		return phaseBootstrapped
	}
	if applied {
		return phaseConfigApplied
	}
	if cfgPresent {
		return phaseConfigGen
	}
	return phasePending
}

func condsFromFlags(cfgPresent, applied, bootstrapped, clusterAccess bool) []condSpec {
	conds := []condSpec{}
	if cfgPresent {
		conds = append(conds, condSpec{"ConfigGenerated", "True", "Generated", "Talos client and role configs generated"})
	}
	if applied {
		conds = append(conds, condSpec{"ConfigApplied", "True", "Applied", "Talos configs applied to all nodes"})
	}
	if bootstrapped {
		conds = append(conds, condSpec{"Bootstrapped", "True", "Done", "Talos cluster bootstrapped"})
	}
	if clusterAccess {
		conds = append(conds, condSpec{"KubeconfigAvailable", "True", "Persisted", "Kubeconfig stored in Secret"})
	}
	return conds
}

// SetStateCreated sets status.state.created to the provided timestamp (RFC3339Nano) and bumps lastUpdated fields.
func (m *StatusManager) SetStateCreated(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster, created time.Time) error {
	gvk := schema.GroupVersionKind{Group: "vitistack.io", Version: "v1alpha1", Kind: "KubernetesCluster"}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(kc.Namespace)
	u.SetName(kc.Name)

	if err := m.Get(ctx, client.ObjectKeyFromObject(kc), u); err != nil {
		return err
	}
	if err := ensureStatusMap(u); err != nil {
		vlog.Error("Failed to ensure status map exists for SetStateCreated: cluster="+kc.Name, err)
		return err
	}
	createdStr := created.UTC().Format(time.RFC3339Nano)
	_ = unstructured.SetNestedField(u.Object, createdStr, "status", "state", "created")
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_ = unstructured.SetNestedField(u.Object, now, "status", "state", "lastUpdated")
	_ = unstructured.SetNestedField(u.Object, "talos-operator", "status", "state", "lastUpdatedBy")

	if err := m.Status().Update(ctx, u); err != nil {
		if fallbackErr := m.Update(ctx, u); fallbackErr != nil {
			return fallbackErr
		}
	}
	return nil
}

// getKubeSystemCreated returns the creation timestamp of the kube-system namespace from the target cluster referenced by kubeconfig.
func getKubeSystemCreated(ctx context.Context, kubeconfig []byte) (time.Time, error) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return time.Time{}, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return time.Time{}, err
	}
	ns, err := cs.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
	if err != nil {
		return time.Time{}, err
	}
	return ns.CreationTimestamp.Time, nil
}

// ensureStatusMap creates an empty status map on the object if it doesn't exist yet.
func ensureStatusMap(u *unstructured.Unstructured) error {
	if err := ensureStatusRoot(u); err != nil {
		return err
	}
	if err := ensurePhaseAndConditions(u); err != nil {
		return err
	}
	if err := ensureState(u); err != nil {
		return err
	}
	return nil
}

func ensureStatusRoot(u *unstructured.Unstructured) error {
	if _, found, _ := unstructured.NestedMap(u.Object, "status"); !found {
		return unstructured.SetNestedMap(u.Object, map[string]any{}, "status")
	}
	return nil
}

func ensurePhaseAndConditions(u *unstructured.Unstructured) error {
	if _, found, _ := unstructured.NestedString(u.Object, "status", "phase"); !found {
		if err := unstructured.SetNestedField(u.Object, "Pending", "status", "phase"); err != nil {
			return err
		}
	}
	if _, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions"); !found {
		if err := unstructured.SetNestedSlice(u.Object, []any{}, "status", "conditions"); err != nil {
			return err
		}
	}
	return nil
}

func ensureState(u *unstructured.Unstructured) error {
	state, found, _ := unstructured.NestedMap(u.Object, "status", "state")
	if !found || state == nil {
		state = map[string]any{}
	}
	if err := ensureStateCluster(state); err != nil {
		return err
	}
	if _, ok := state["versions"]; !ok {
		state["versions"] = []any{}
	}
	if _, ok := state["endpoints"]; !ok {
		state["endpoints"] = []any{}
	}
	if _, ok := state["egressIP"]; !ok {
		state["egressIP"] = ""
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, ok := state["created"]; !ok {
		state["created"] = now
	}
	if _, ok := state["lastUpdated"]; !ok {
		state["lastUpdated"] = now
	}
	if _, ok := state["lastUpdatedBy"]; !ok {
		state["lastUpdatedBy"] = "talos-operator"
	}
	return unstructured.SetNestedMap(u.Object, state, "status", "state")
}

func ensureStateCluster(state map[string]any) error {
	cluster, ok := state["cluster"].(map[string]any)
	if !ok || cluster == nil {
		cluster = map[string]any{}
	}
	if _, ok := cluster["externalId"]; !ok {
		cluster["externalId"] = ""
	}
	if _, ok := cluster["price"]; !ok {
		cluster["price"] = map[string]any{"monthly": int64(0), "yearly": int64(0)}
	}
	if _, ok := cluster["resources"]; !ok {
		cluster["resources"] = defaultResources()
	}
	if _, ok := cluster["controlplane"]; !ok {
		cluster["controlplane"] = map[string]any{
			"machineClass": "",
			"message":      "",
			"status":       "Pending",
			"scale":        int64(0),
			"resources":    defaultResources(),
		}
	}
	if _, ok := cluster["nodepools"]; !ok {
		cluster["nodepools"] = []any{}
	}
	state["cluster"] = cluster
	return nil
}

func defaultResources() map[string]any {
	return map[string]any{
		"cpu":    defaultResourceUsage(),
		"memory": defaultResourceUsage(),
		"gpu":    defaultResourceUsage(),
		"disk":   defaultResourceUsage(),
	}
}

func defaultResourceUsage() map[string]any {
	return map[string]any{"capacity": "0", "used": "0", "percentage": int64(0)}
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
	if err := ensureStatusMap(u); err != nil {
		vlog.Error("Failed to ensure status map exists: cluster="+kc.Name, err)
		return err
	}
	if err := unstructured.SetNestedField(u.Object, phase, "status", "phase"); err != nil {
		vlog.Error("Failed to set nested field for phase: cluster="+kc.Name+" phase="+phase, err)
		return err
	}
	// Update state.lastUpdated and lastUpdatedBy
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_ = unstructured.SetNestedField(u.Object, now, "status", "state", "lastUpdated")
	_ = unstructured.SetNestedField(u.Object, "talos-operator", "status", "state", "lastUpdatedBy")
	// Do not set status.state here; it's an object in the CRD. ensureStatusMap already ensures it exists.
	if err := m.Status().Update(ctx, u); err != nil {
		vlog.Error("Status().Update failed, trying fallback Update: cluster="+kc.Name, err)
		// fallback for CRDs without status subresource
		if fallbackErr := m.Update(ctx, u); fallbackErr != nil {
			vlog.Error("Fallback Update also failed: cluster="+kc.Name, fallbackErr)
			return fallbackErr
		}
		vlog.Info("Fallback Update succeeded: cluster=" + kc.Name)
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
	if err := ensureStatusMap(u); err != nil {
		vlog.Error("Failed to ensure status map exists for condition: cluster="+kc.Name, err)
		return err
	}

	conds, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	if !found {
		conds = []any{}
	}

	// Build new condition map
	newCond := map[string]any{
		"type":               condType,
		"status":             status,
		"reason":             reason,
		"message":            message,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339Nano),
	}

	// Replace existing condition of same type or append
	replaced := false
	for i, ci := range conds {
		if cm, ok := ci.(map[string]any); ok {
			if t, _ := cm["type"].(string); t == condType {
				// Check if unchanged
				if cm["status"] == status && cm["reason"] == reason && cm["message"] == message {
					// Still bump lastObserved fields
					cm["lastTransitionTime"] = newCond["lastTransitionTime"]
					conds[i] = cm
					replaced = true
					break
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
		vlog.Error("Failed to set nested slice for conditions: cluster="+kc.Name+" condition="+condType, err)
		return err
	}
	// Touch state.lastUpdated and lastUpdatedBy
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_ = unstructured.SetNestedField(u.Object, now, "status", "state", "lastUpdated")
	_ = unstructured.SetNestedField(u.Object, "talos-operator", "status", "state", "lastUpdatedBy")

	if err := m.Status().Update(ctx, u); err != nil {
		vlog.Error("Status().Update failed for condition, trying fallback Update: cluster="+kc.Name+" condition="+condType, err)
		// fallback for CRDs without status subresource
		if fallbackErr := m.Update(ctx, u); fallbackErr != nil {
			vlog.Error("Fallback Update also failed for condition: cluster="+kc.Name+" condition="+condType, fallbackErr)
			return fallbackErr
		}
		vlog.Info("Fallback Update succeeded for condition: cluster=" + kc.Name + " condition=" + condType)
	}
	return nil
}

// AggregateFromMachines fetches Machines for the given cluster and updates status aggregates.
func (m *StatusManager) AggregateFromMachines(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster) error {
	// List Machines labeled with this cluster
	ml := &vitistackv1alpha1.MachineList{}
	if err := m.List(ctx, ml,
		client.InNamespace(kc.Namespace),
		client.MatchingLabels{"cluster.vitistack.io/cluster-name": kc.Name},
	); err != nil {
		vlog.Debug("failed to list machines for aggregation: cluster=" + kc.Name + " error=" + err.Error())
		return err
	}

	// Build aggregates
	var totalCPU, totalMem, diskCap, diskUsed int64
	var cpCount, cpRunning int64
	for i := range ml.Items {
		mObj := &ml.Items[i]
		// Sum resources
		totalCPU += int64(mObj.Status.CPUs)
		totalMem += mObj.Status.Memory
		for i := range mObj.Status.Disks {
			d := mObj.Status.Disks[i]
			diskCap += d.Size
			diskUsed += d.UsedBytes
		}
		// Control-plane specifics
		if role, ok := mObj.Labels["cluster.vitistack.io/role"]; ok && role == "control-plane" {
			cpCount++
			if mObj.Status.Phase == "Running" {
				cpRunning++
			}
		}
	}

	// Fetch unstructured cluster to update status
	gvk := schema.GroupVersionKind{Group: "vitistack.io", Version: "v1alpha1", Kind: "KubernetesCluster"}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(kc.Namespace)
	u.SetName(kc.Name)
	if err := m.Get(ctx, client.ObjectKeyFromObject(kc), u); err != nil {
		return err
	}
	if err := ensureStatusMap(u); err != nil {
		return err
	}

	// Update controlplane summary
	_ = unstructured.SetNestedField(u.Object, cpCount, "status", "state", "cluster", "controlplane", "scale")
	cpStatus := "Pending"
	if cpCount > 0 && cpRunning == cpCount {
		cpStatus = "Running"
		// All control plane machines are running; mark cluster Ready
		_ = m.SetPhase(ctx, kc, phaseReady)
	} else if cpCount > 0 && cpRunning > 0 {
		cpStatus = "Partial"
	}
	_ = unstructured.SetNestedField(u.Object, cpStatus, "status", "state", "cluster", "controlplane", "status")

	// Update cluster.resources aggregates
	// CPU and memory used are unknown here; set used=0, percentage=0
	_ = setResourceUsage(u, []string{"status", "state", "cluster", "resources", "cpu"}, totalCPU, 0)
	_ = setResourceUsage(u, []string{"status", "state", "cluster", "resources", "memory"}, totalMem, 0)
	// Disk: can compute used percentage
	_ = setResourceUsage(u, []string{"status", "state", "cluster", "resources", "disk"}, diskCap, diskUsed)

	// Touch timestamps
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_ = unstructured.SetNestedField(u.Object, now, "status", "state", "lastUpdated")
	_ = unstructured.SetNestedField(u.Object, "talos-operator", "status", "state", "lastUpdatedBy")

	if err := m.Status().Update(ctx, u); err != nil {
		if fallbackErr := m.Update(ctx, u); fallbackErr != nil {
			return fallbackErr
		}
	}
	return nil
}

// setResourceUsage writes capacity/used/percentage for a given resource path.
func setResourceUsage(u *unstructured.Unstructured, path []string, capacity, used int64) error {
	// Ensure map exists
	// path points to ... , "cpu"|"memory"|"disk"
	usage := map[string]any{
		"capacity":   capacity,
		"used":       used,
		"percentage": int64(0),
	}
	if capacity > 0 && used > 0 {
		usage["percentage"] = (used * 100) / capacity
	}
	// Build full path for the object map
	if err := unstructured.SetNestedMap(u.Object, usage, path...); err != nil {
		return err
	}
	return nil
}
