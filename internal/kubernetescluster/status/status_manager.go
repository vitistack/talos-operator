package status

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/vitistack/common/pkg/loggers/vlog"
	"github.com/vitistack/common/pkg/unstructuredutil"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/secretservice"
	"github.com/vitistack/talos-operator/internal/services/talosstateservice"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	PhasePending         = "Pending"
	phaseConfigGen       = "ConfigGenerated"
	phaseConfigApplied   = "ConfigApplied"
	phaseBootstrapped    = "Bootstrapped"
	phaseWaitingForNodes = "WaitingForNodes" // configs/bootstrap done, waiting for K8s nodes to become Ready
	phaseReady           = "Ready"
	phaseRunning         = "Running"
	PhaseValidationError = "ValidationError"
	PhaseFailed          = "Failed" // terminal failure surfaced when machines can't provision and the timeout elapses

	// Upgrade-related phases
	PhaseUpgradingTalos      = "UpgradingTalos"
	PhaseUpgradingKubernetes = "UpgradingKubernetes"
	PhaseUpgradeFailed       = "UpgradeFailed"

	// Exported phases for use by other packages
	PhaseReady = phaseReady

	// Condition status string used by metav1-style status conditions.
	condStatusTrue = "True"

	// Field names of the unstructured cluster-state map under
	// .status.state.cluster.* — used as map keys when scaffolding the
	// nested object graph and when traversing it to update resource usage.
	stateFieldStatus    = "status"
	stateFieldResources = "resources"
	stateFieldCluster   = "cluster"
	stateFieldState     = "state"
)

// StatusManager handles machine status updates and monitoring
type StatusManager struct {
	client.Client
	SecretService *secretservice.SecretService
	StateService  *talosstateservice.TalosStateService

	// kubeSystemNamespace reads the workload cluster's kube-system namespace.
	kubeSystemNamespace func(ctx context.Context, kubeconfig []byte) (*corev1.Namespace, error)
}

// NewManager creates a new status manager
func NewManager(c client.Client, secretService *secretservice.SecretService, stateService *talosstateservice.TalosStateService) *StatusManager {
	return &StatusManager{
		Client:              c,
		SecretService:       secretService,
		StateService:        stateService,
		kubeSystemNamespace: fetchKubeSystemNamespace,
	}
}

// UpdateMachineStatus updates the machine status with the given state
func (m *StatusManager) UpdateKubernetesClusterStatus(ctx context.Context, kubernetesCluster *vitistackv1alpha1.KubernetesCluster) error {
	// Load cluster Secret and derive phase/conditions
	secret, err := m.SecretService.GetTalosSecret(ctx, kubernetesCluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Secret not found, treat as nil
			secret = nil
		} else {
			vlog.Error("Failed to get Talos secret for cluster "+kubernetesCluster.Name, err)
			return err
		}
	}

	phase, conds, kubeconfig := deriveStatusFromSecret(secret)
	_ = m.SetPhase(ctx, kubernetesCluster, phase)
	for _, c := range conds {
		_ = m.SetCondition(ctx, kubernetesCluster, c.Type, c.Status, c.Reason, c.Message)
	}

	// If kubeconfig is present, record kube-system's creation time and UID but
	// do NOT flip the cluster Phase to Ready here. Reaching kube-system means
	// the API server is up; it does not mean every expected node has joined and
	// reached Ready. The authoritative Ready transition happens in the
	// Talos init flow's final stage, gated by the node-health check.
	if len(kubeconfig) > 0 && m.shouldFetchKubeSystemUID(kubernetesCluster) {
		m.recordKubeSystemIdentity(ctx, kubernetesCluster, kubeconfig)
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
		return PhasePending, []condSpec{{Type: "TalosSecretReady", Status: "False", Reason: "NotFound", Message: "Talos secret not created yet"}}, nil
	}
	cfgPresent := getSecretFlag(secret, "talosconfig_present") && getSecretFlag(secret, "controlplane_yaml_present") && getSecretFlag(secret, "worker_yaml_present")
	applied := getSecretFlag(secret, "controlplane_applied") && getSecretFlag(secret, "worker_applied")
	bootstrapped := getSecretFlag(secret, "bootstrapped")
	clusterAccess := getSecretFlag(secret, "cluster_access") || len(secret.Data["kube.config"]) > 0
	nodesHealthReady := getSecretFlag(secret, "nodes_health_ready")

	phase := phaseFromFlags(cfgPresent, applied, bootstrapped, clusterAccess, nodesHealthReady)
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

func phaseFromFlags(cfgPresent, applied, bootstrapped, clusterAccess, nodesHealthReady bool) string {
	// Ready means everything is in place AND we have confirmed every expected
	// K8s node has joined and reached Ready (nodes_health_ready). Without that
	// last gate the cluster can flip Ready while workers are still missing.
	if cfgPresent && applied && bootstrapped && clusterAccess && nodesHealthReady {
		return phaseReady
	}
	// All the per-stage work is done but the workload cluster still has
	// nodes that haven't joined or aren't Ready yet.
	if cfgPresent && applied && bootstrapped && clusterAccess {
		return phaseWaitingForNodes
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
	return PhasePending
}

func condsFromFlags(cfgPresent, applied, bootstrapped, clusterAccess bool) []condSpec {
	conds := []condSpec{
		// Created condition is always true once the cluster resource exists
		{"Created", condStatusTrue, "ClusterCreated", "Kubernetes cluster resource has been created"},
	}
	if cfgPresent {
		conds = append(conds, condSpec{"ConfigGenerated", condStatusTrue, "Generated", "Talos client and role configs generated"})
	}
	if applied {
		conds = append(conds, condSpec{"ConfigApplied", condStatusTrue, "Applied", "Talos configs applied to all nodes"})
	}
	if bootstrapped {
		conds = append(conds, condSpec{"Bootstrapped", condStatusTrue, "Done", "Talos cluster bootstrapped"})
	}
	if clusterAccess {
		conds = append(conds, condSpec{"KubeconfigAvailable", condStatusTrue, "Persisted", "Kubeconfig stored in Secret"})
	}
	return conds
}

// SetStateCreated sets status.state.created to the provided timestamp (RFC3339Nano) and bumps lastUpdated fields.
// It does nothing when the timestamp is already recorded. A write conflict is returned as an error.
func (m *StatusManager) SetStateCreated(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster, created time.Time) error {
	if kc.Status.State.Created.Time.Equal(created) {
		return nil
	}

	// Convert typed KubernetesCluster to unstructured for status manipulation
	u, err := unstructuredutil.KubernetesClusterToUnstructured(kc)
	if err != nil {
		return err
	}

	if err := m.Get(ctx, client.ObjectKeyFromObject(kc), u); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Resource was deleted, nothing to update
		}
		return err
	}
	if err := ensureStatusMap(u); err != nil {
		vlog.Error("Failed to ensure status map exists for SetStateCreated: cluster="+kc.Name, err)
		return err
	}
	createdStr := created.UTC().Format(time.RFC3339Nano)
	if current, _, _ := unstructured.NestedString(u.Object, stateFieldStatus, stateFieldState, "created"); current == createdStr {
		kc.Status.State.Created = metav1.NewTime(created)
		return nil
	}
	_ = unstructured.SetNestedField(u.Object, createdStr, stateFieldStatus, stateFieldState, "created")
	updateStatusTimestamps(u)

	if err := m.Status().Update(ctx, u); err != nil {
		if apierrors.IsConflict(err) {
			return err
		}
		if fallbackErr := m.Update(ctx, u); fallbackErr != nil {
			return fallbackErr
		}
	}
	kc.Status.State.Created = metav1.NewTime(created)
	return nil
}

// recordKubeSystemIdentity reads the workload cluster's kube-system namespace
// and records its creation time (status.state.created) and UID (annotation and
// secret). Both are fixed for the life of the cluster, so callers only invoke
// this until the UID annotation is stored. The UID is stored last, so a failed
// creation-time write is retried on the next pass.
func (m *StatusManager) recordKubeSystemIdentity(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster, kubeconfig []byte) {
	ns, err := m.kubeSystemNamespace(ctx, kubeconfig)
	if err != nil {
		vlog.Debug("Failed to get kube-system namespace from target cluster: " + err.Error())
		return
	}
	if !ns.CreationTimestamp.IsZero() {
		if err := m.SetStateCreated(ctx, kc, ns.CreationTimestamp.Time); err != nil {
			vlog.Debug("Failed to record kube-system creation time: cluster=" + kc.Name + " error=" + err.Error())
			return
		}
	}
	uid := string(ns.UID)
	if uid == "" {
		return
	}
	_ = m.SetKubeSystemUID(ctx, kc, uid)
	if m.StateService != nil {
		_ = m.StateService.SetKubeSystemUID(ctx, kc, uid)
	}
}

// fetchKubeSystemNamespace reads the kube-system namespace from the target cluster referenced by kubeconfig.
func fetchKubeSystemNamespace(ctx context.Context, kubeconfig []byte) (*corev1.Namespace, error) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return cs.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
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
	if _, found, _ := unstructured.NestedMap(u.Object, stateFieldStatus); !found {
		return unstructured.SetNestedMap(u.Object, map[string]any{}, stateFieldStatus)
	}
	return nil
}

func ensurePhaseAndConditions(u *unstructured.Unstructured) error {
	if _, found, _ := unstructured.NestedString(u.Object, stateFieldStatus, "phase"); !found {
		if err := unstructured.SetNestedField(u.Object, "Pending", stateFieldStatus, "phase"); err != nil {
			return err
		}
	}
	if _, found, _ := unstructured.NestedSlice(u.Object, stateFieldStatus, "conditions"); !found {
		if err := unstructured.SetNestedSlice(u.Object, []any{}, stateFieldStatus, "conditions"); err != nil {
			return err
		}
	}
	return nil
}

func ensureState(u *unstructured.Unstructured) error {
	state, found, _ := unstructured.NestedMap(u.Object, stateFieldStatus, stateFieldState)
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
	return unstructured.SetNestedMap(u.Object, state, stateFieldStatus, stateFieldState)
}

func ensureStateCluster(state map[string]any) error {
	cluster, ok := state[stateFieldCluster].(map[string]any)
	if !ok || cluster == nil {
		cluster = map[string]any{}
	}
	if _, ok := cluster["externalId"]; !ok {
		cluster["externalId"] = ""
	}
	if _, ok := cluster["price"]; !ok {
		cluster["price"] = map[string]any{"monthly": int64(0), "yearly": int64(0)}
	}
	if _, ok := cluster[stateFieldResources]; !ok {
		cluster[stateFieldResources] = defaultResources()
	}
	if _, ok := cluster["controlplane"]; !ok {
		cluster["controlplane"] = map[string]any{
			"machineClass":      "",
			"message":           "",
			stateFieldStatus:    "Pending",
			"scale":             int64(0),
			"nodes":             []any{},
			stateFieldResources: defaultResources(),
		}
	}
	if _, ok := cluster["nodepools"]; !ok {
		cluster["nodepools"] = []any{}
	}
	state[stateFieldCluster] = cluster
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
	// The held object already has this phase: skip the live read.
	if kc.Status.Phase == phase {
		return nil
	}

	// Convert typed KubernetesCluster to unstructured for status manipulation
	u, err := unstructuredutil.KubernetesClusterToUnstructured(kc)
	if err != nil {
		return err
	}

	if err := m.Get(ctx, client.ObjectKeyFromObject(kc), u); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Resource was deleted, nothing to update
		}
		return err
	}

	// Check if phase is already set to the desired value - skip update if unchanged
	currentPhase, _, _ := unstructured.NestedString(u.Object, stateFieldStatus, "phase")
	if currentPhase == phase {
		kc.Status.Phase = phase
		return nil // No change needed
	}

	if err := ensureStatusMap(u); err != nil {
		vlog.Error("Failed to ensure status map exists: cluster="+kc.Name, err)
		return err
	}
	if err := unstructured.SetNestedField(u.Object, phase, stateFieldStatus, "phase"); err != nil {
		vlog.Error("Failed to set nested field for phase: cluster="+kc.Name+" phase="+phase, err)
		return err
	}
	// Update state.lastUpdated and lastUpdatedBy
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_ = unstructured.SetNestedField(u.Object, now, stateFieldStatus, stateFieldState, "lastUpdated")
	_ = unstructured.SetNestedField(u.Object, "talos-operator", stateFieldStatus, stateFieldState, "lastUpdatedBy")
	// Do not set status.state here; it's an object in the CRD. ensureStatusMap already ensures it exists.
	if err := m.Status().Update(ctx, u); err != nil {
		// Check if this is a conflict error - if so, just log and skip
		if apierrors.IsConflict(err) {
			vlog.Debug("Status update conflict (object modified), skipping: cluster=" + kc.Name)
			return nil
		}
		vlog.Error("Status().Update failed, trying fallback Update: cluster="+kc.Name, err)
		// fallback for CRDs without status subresource
		if fallbackErr := m.Update(ctx, u); fallbackErr != nil {
			if apierrors.IsConflict(fallbackErr) {
				vlog.Debug("Fallback update conflict, skipping: cluster=" + kc.Name)
				return nil
			}
			vlog.Error("Fallback Update also failed: cluster="+kc.Name, fallbackErr)
			return fallbackErr
		}
		vlog.Info("Fallback Update succeeded: cluster=" + kc.Name)
	}
	kc.Status.Phase = phase
	return nil
}

// SetMessage sets the human-readable message on status.message describing the current activity.
// Pass an empty string to clear the message.
func (m *StatusManager) SetMessage(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster, message string) error {
	// The held object already has this message: skip the live read.
	if kc.Status.Message == message {
		return nil
	}

	u, err := unstructuredutil.KubernetesClusterToUnstructured(kc)
	if err != nil {
		return err
	}

	if err := m.Get(ctx, client.ObjectKeyFromObject(kc), u); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	currentMessage, _, _ := unstructured.NestedString(u.Object, stateFieldStatus, "message")
	if currentMessage == message {
		kc.Status.Message = message
		return nil
	}

	if err := ensureStatusMap(u); err != nil {
		return err
	}
	if err := unstructured.SetNestedField(u.Object, message, stateFieldStatus, "message"); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	_ = unstructured.SetNestedField(u.Object, now, stateFieldStatus, stateFieldState, "lastUpdated")
	_ = unstructured.SetNestedField(u.Object, "talos-operator", stateFieldStatus, stateFieldState, "lastUpdatedBy")

	if err := m.Status().Update(ctx, u); err != nil {
		if apierrors.IsConflict(err) {
			return nil
		}
		return err
	}
	kc.Status.Message = message
	return nil
}

// SetCondition updates status.conditions with a condition entry (type, status, reason, message, lastTransitionTime).
// Uses unstructured to avoid coupling to generated condition types from external CRD module.
// nolint:gocyclo // Complexity handled through helper functions
func (m *StatusManager) SetCondition(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster,
	condType, status, reason, message string,
) error {
	// The held object already has this condition: skip the live read.
	if heldConditionMatches(kc, condType, status, reason, message) {
		return nil
	}

	// Convert typed KubernetesCluster to unstructured for status manipulation
	u, err := unstructuredutil.KubernetesClusterToUnstructured(kc)
	if err != nil {
		return err
	}

	if err := m.Get(ctx, client.ObjectKeyFromObject(kc), u); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Resource was deleted, nothing to update
		}
		return err
	}
	if err := ensureStatusMap(u); err != nil {
		vlog.Error("Failed to ensure status map exists for condition: cluster="+kc.Name, err)
		return err
	}

	// Update conditions - returns changed=false if condition already exists with same values
	changed, err := m.updateConditionInStatus(u, condType, status, reason, message, kc.Name)
	if err != nil {
		return err
	}
	if !changed {
		setHeldCondition(kc, condType, status, reason, message)
		return nil // No change needed, skip API update
	}

	// Touch state.lastUpdated and lastUpdatedBy
	updateStatusTimestamps(u)

	written, err := m.updateStatusWithConflictHandling(ctx, u, kc.Name, condType)
	if written {
		setHeldCondition(kc, condType, status, reason, message)
	}
	return err
}

// heldConditionMatches reports whether kc already carries condType with the given status, reason and message.
func heldConditionMatches(kc *vitistackv1alpha1.KubernetesCluster, condType, status, reason, message string) bool {
	for i := range kc.Status.Conditions {
		c := &kc.Status.Conditions[i]
		if c.Type == condType {
			return c.Status == status && c.Reason == reason && c.Message == message
		}
	}
	return false
}

// setHeldCondition records a condition the API server now has on kc, so later
// checks in the same pass compare against it.
func setHeldCondition(kc *vitistackv1alpha1.KubernetesCluster, condType, status, reason, message string) {
	for i := range kc.Status.Conditions {
		c := &kc.Status.Conditions[i]
		if c.Type == condType {
			if c.Status != status || c.Reason != reason || c.Message != message {
				c.LastTransitionTime = time.Now().UTC().Format(time.RFC3339Nano)
			}
			c.Status, c.Reason, c.Message = status, reason, message
			return
		}
	}
	kc.Status.Conditions = append(kc.Status.Conditions, vitistackv1alpha1.KubernetesClusterCondition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: time.Now().UTC().Format(time.RFC3339Nano),
	})
}

// ClearValidationError resets the phase from ValidationError to Pending if needed.
// This allows the reconciliation to proceed normally after a validation error is fixed.
func (m *StatusManager) ClearValidationError(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster) error {
	// The held object is not in ValidationError: skip the live read.
	if kc.Status.Phase != PhaseValidationError {
		return nil
	}

	// Convert typed KubernetesCluster to unstructured for status manipulation
	u, err := unstructuredutil.KubernetesClusterToUnstructured(kc)
	if err != nil {
		return err
	}

	if err := m.Get(ctx, client.ObjectKeyFromObject(kc), u); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Resource was deleted, nothing to update
		}
		return err
	}

	// Check current phase
	currentPhase, _, _ := unstructured.NestedString(u.Object, stateFieldStatus, "phase")
	if currentPhase != PhaseValidationError {
		return nil // Not in ValidationError state, nothing to do
	}

	// Reset phase to Pending so normal reconciliation can determine the correct phase
	if err := unstructured.SetNestedField(u.Object, PhasePending, stateFieldStatus, "phase"); err != nil {
		vlog.Error("Failed to reset phase from ValidationError: cluster="+kc.Name, err)
		return err
	}

	// Touch state.lastUpdated and lastUpdatedBy
	updateStatusTimestamps(u)

	vlog.Info("Cleared ValidationError phase, resetting to Pending: cluster=" + kc.Name)
	written, err := m.updateStatusWithConflictHandling(ctx, u, kc.Name, "phase-reset")
	if written {
		kc.Status.Phase = PhasePending
	}
	return err
}

// updateConditionInStatus updates the condition in the status.conditions slice
// Returns (changed bool, error). changed is false if the condition already exists with the same values.
func (m *StatusManager) updateConditionInStatus(u *unstructured.Unstructured, condType, status, reason, message, clusterName string) (bool, error) {
	conds, found, _ := unstructured.NestedSlice(u.Object, stateFieldStatus, "conditions")
	if !found {
		conds = []any{}
	}

	// Build new condition map
	newCond := map[string]any{
		"type":               condType,
		stateFieldStatus:     status,
		"reason":             reason,
		"message":            message,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339Nano),
	}

	// Replace existing condition of same type or append
	conds, changed := replaceOrAppendCondition(conds, newCond, condType)
	if !changed {
		return false, nil // No change needed
	}

	// Sort conditions by lastTransitionTime (newest first)
	conds = sortConditionsByTime(conds)

	if err := unstructured.SetNestedSlice(u.Object, conds, stateFieldStatus, "conditions"); err != nil {
		vlog.Error("Failed to set nested slice for conditions: cluster="+clusterName+" condition="+condType, err)
		return false, err
	}
	return true, nil
}

// replaceOrAppendCondition replaces an existing condition or appends a new one.
// Returns (conditions, changed). changed is false if the condition already exists with the same values.
func replaceOrAppendCondition(conds []any, newCond map[string]any, condType string) ([]any, bool) {
	for i, ci := range conds {
		if cm, ok := ci.(map[string]any); ok {
			if t, _ := cm["type"].(string); t == condType {
				// Check if unchanged - if status, reason, and message are the same, no update needed
				if cm[stateFieldStatus] == newCond[stateFieldStatus] && cm["reason"] == newCond["reason"] && cm["message"] == newCond["message"] {
					// No change needed - don't update lastTransitionTime
					return conds, false
				}
				conds[i] = newCond
				return conds, true
			}
		}
	}
	return append(conds, newCond), true
}

// sortConditionsByTime sorts conditions by lastTransitionTime (newest first)
func sortConditionsByTime(conds []any) []any {
	sort.SliceStable(conds, func(i, j int) bool {
		ci, okI := conds[i].(map[string]any)
		cj, okJ := conds[j].(map[string]any)
		if !okI || !okJ {
			return false
		}
		timeI, _ := ci["lastTransitionTime"].(string)
		timeJ, _ := cj["lastTransitionTime"].(string)
		// Parse times - if parse fails, treat as zero time
		parsedI, errI := time.Parse(time.RFC3339Nano, timeI)
		parsedJ, errJ := time.Parse(time.RFC3339Nano, timeJ)
		if errI != nil || errJ != nil {
			return false
		}
		// Sort newest first (descending)
		return parsedI.After(parsedJ)
	})
	return conds
}

// updateStatusWithConflictHandling handles status update with conflict error handling.
// It reports whether the object was written; a conflict is skipped without an error.
func (m *StatusManager) updateStatusWithConflictHandling(ctx context.Context, u *unstructured.Unstructured, clusterName, condType string) (bool, error) {
	if err := m.Status().Update(ctx, u); err != nil {
		// Check if this is a conflict error - if so, just log and skip
		if apierrors.IsConflict(err) {
			vlog.Debug("Status update conflict for condition (object modified), skipping: cluster=" + clusterName + " condition=" + condType)
			return false, nil
		}
		vlog.Error("Status().Update failed for condition, trying fallback Update: cluster="+clusterName+" condition="+condType, err)
		// fallback for CRDs without status subresource
		if fallbackErr := m.Update(ctx, u); fallbackErr != nil {
			if apierrors.IsConflict(fallbackErr) {
				vlog.Debug("Fallback update conflict for condition, skipping: cluster=" + clusterName + " condition=" + condType)
				return false, nil
			}
			vlog.Error("Fallback Update also failed for condition: cluster="+clusterName+" condition="+condType, fallbackErr)
			return false, fallbackErr
		}
		vlog.Info("Fallback Update succeeded for condition: cluster=" + clusterName + " condition=" + condType)
	}
	return true, nil
}

// AggregateFromMachines fetches Machines for the given cluster and updates status aggregates.
// It writes only when an aggregate changed.
func (m *StatusManager) AggregateFromMachines(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster) error {
	// List Machines labeled with this cluster
	ml := &vitistackv1alpha1.MachineList{}
	if err := m.List(ctx, ml,
		client.InNamespace(kc.Namespace),
		client.MatchingLabels{vitistackv1alpha1.ClusterIdAnnotation: kc.Spec.Cluster.ClusterId},
	); err != nil {
		vlog.Debug("failed to list machines for aggregation: cluster=" + kc.Name + " error=" + err.Error())
		return err
	}

	agg := aggregateMachineResources(ml)

	// The held object already has these aggregates: skip the live read.
	if agg.matches(kc) {
		return nil
	}

	// Convert typed KubernetesCluster to unstructured for status manipulation
	u, err := unstructuredutil.KubernetesClusterToUnstructured(kc)
	if err != nil {
		return err
	}

	if err := m.Get(ctx, client.ObjectKeyFromObject(kc), u); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Resource was deleted, nothing to update
		}
		return err
	}
	stored := u.DeepCopy()
	if err := ensureStatusMap(u); err != nil {
		return err
	}

	m.updateControlPlaneStatus(u, agg.cpCount, agg.cpRunning, agg.cpNodes)

	// Update worker count
	_ = unstructured.SetNestedField(u.Object, agg.workerCount, stateFieldStatus, "workers")

	// Update cluster resource aggregates
	updateClusterResourceStatus(u, agg.totalCPU, agg.totalMem, agg.diskCap, agg.diskUsed)

	if sameStatus(stored, u) {
		agg.applyTo(kc)
		return nil
	}

	// Touch timestamps
	updateStatusTimestamps(u)

	written, err := m.updateStatusWithConflictHandling(ctx, u, kc.Name, "aggregates")
	if written {
		agg.applyTo(kc)
	}
	return err
}

// sameStatus reports whether two KubernetesClusters carry the same status once
// decoded, so a quantity stored as 14 and one stored as "14" compare equal.
func sameStatus(a, b *unstructured.Unstructured) bool {
	typedA, errA := unstructuredutil.KubernetesClusterFromUnstructured(a)
	typedB, errB := unstructuredutil.KubernetesClusterFromUnstructured(b)
	if errA != nil || errB != nil {
		return false
	}
	return equality.Semantic.DeepEqual(typedA.Status, typedB.Status)
}

// machineAggregates are the status totals derived from a cluster's Machines.
type machineAggregates struct {
	totalCPU, totalMem, diskCap, diskUsed int64
	cpCount, cpRunning, workerCount       int64
	cpNodes                               []string
}

// aggregateMachineResources aggregates resource usage from all machines in the list.
// cpNodes is sorted so the result does not depend on list order.
func aggregateMachineResources(ml *vitistackv1alpha1.MachineList) machineAggregates {
	var agg machineAggregates
	for i := range ml.Items {
		mObj := &ml.Items[i]
		// Sum resources
		agg.totalCPU += int64(mObj.Status.CPUs)
		agg.totalMem += mObj.Status.Memory
		for i := range mObj.Status.Disks {
			d := mObj.Status.Disks[i]
			agg.diskCap += d.Size
			agg.diskUsed += d.UsedBytes
		}
		// Control-plane specifics
		if isControlPlaneMachine(mObj) {
			agg.cpCount++
			agg.cpNodes = append(agg.cpNodes, mObj.Name)
			if mObj.Status.Phase == phaseRunning {
				agg.cpRunning++
			}
		} else {
			agg.workerCount++
		}
	}
	sort.Strings(agg.cpNodes)
	return agg
}

// matches reports whether kc's status already carries these aggregates.
func (a *machineAggregates) matches(kc *vitistackv1alpha1.KubernetesCluster) bool {
	cp := &kc.Status.State.Cluster.ControlPlaneStatus
	res := &kc.Status.State.Cluster.Resources
	return int64(kc.Status.Workers) == a.workerCount &&
		int64(cp.Scale) == a.cpCount &&
		cp.Status == determineControlPlaneStatus(a.cpCount, a.cpRunning) &&
		slices.Equal(cp.Nodes, a.cpNodes) &&
		resourceUsageMatches(&res.CPU, a.totalCPU, 0) &&
		resourceUsageMatches(&res.Memory, a.totalMem, 0) &&
		resourceUsageMatches(&res.Disk, a.diskCap, a.diskUsed)
}

// applyTo records the aggregates on kc after the API server has them.
func (a *machineAggregates) applyTo(kc *vitistackv1alpha1.KubernetesCluster) {
	kc.Status.Workers = int(a.workerCount)
	cp := &kc.Status.State.Cluster.ControlPlaneStatus
	cp.Scale = int(a.cpCount)
	cp.Nodes = a.cpNodes
	cp.Status = determineControlPlaneStatus(a.cpCount, a.cpRunning)
	res := &kc.Status.State.Cluster.Resources
	setHeldResourceUsage(&res.CPU, a.totalCPU, 0)
	setHeldResourceUsage(&res.Memory, a.totalMem, 0)
	setHeldResourceUsage(&res.Disk, a.diskCap, a.diskUsed)
}

func resourceUsageMatches(r *vitistackv1alpha1.KubernetesClusterStatusClusterStatusResource, capacity, used int64) bool {
	return r.Capacity.Value() == capacity && r.Used.Value() == used && int64(r.Percetage) == usagePercentage(capacity, used)
}

func setHeldResourceUsage(r *vitistackv1alpha1.KubernetesClusterStatusClusterStatusResource, capacity, used int64) {
	r.Capacity = *resource.NewQuantity(capacity, resource.DecimalSI)
	r.Used = *resource.NewQuantity(used, resource.DecimalSI)
	r.Percetage = int(usagePercentage(capacity, used))
}

// usagePercentage is used as a whole percentage of capacity, or 0 when either is unknown.
func usagePercentage(capacity, used int64) int64 {
	if capacity > 0 && used > 0 {
		return (used * 100) / capacity
	}
	return 0
}

// isControlPlaneMachine checks if a machine is a control plane node
func isControlPlaneMachine(m *vitistackv1alpha1.Machine) bool {
	role, ok := m.Labels[vitistackv1alpha1.NodeRoleAnnotation]
	return ok && role == "control-plane"
}

// updateControlPlaneStatus writes control-plane scale, node names, and a
// derived status string ("Running" / "Partial" / "Pending") into the
// unstructured cluster status. This is informational only — it does not
// drive the cluster Phase.
func (m *StatusManager) updateControlPlaneStatus(u *unstructured.Unstructured, cpCount, cpRunning int64, cpNodes []string) {
	_ = unstructured.SetNestedField(u.Object, cpCount, stateFieldStatus, stateFieldState, stateFieldCluster, "controlplane", "scale")

	nodesAny := make([]any, len(cpNodes))
	for i, node := range cpNodes {
		nodesAny[i] = node
	}
	_ = unstructured.SetNestedSlice(u.Object, nodesAny, stateFieldStatus, stateFieldState, stateFieldCluster, "controlplane", "nodes")

	cpStatus := determineControlPlaneStatus(cpCount, cpRunning)
	_ = unstructured.SetNestedField(u.Object, cpStatus, stateFieldStatus, stateFieldState, stateFieldCluster, "controlplane", stateFieldStatus)
}

// determineControlPlaneStatus determines the control plane status based on machine counts
func determineControlPlaneStatus(cpCount, cpRunning int64) string {
	if cpCount > 0 && cpRunning == cpCount {
		return phaseRunning
	} else if cpCount > 0 && cpRunning > 0 {
		return "Partial"
	}
	return "Pending"
}

// updateClusterResourceStatus updates resource usage in the unstructured object
func updateClusterResourceStatus(u *unstructured.Unstructured, totalCPU, totalMem, diskCap, diskUsed int64) {
	// CPU and memory used are unknown here; set used=0, percentage=0
	_ = setResourceUsage(u, []string{stateFieldStatus, stateFieldState, stateFieldCluster, stateFieldResources, "cpu"}, totalCPU, 0)
	_ = setResourceUsage(u, []string{stateFieldStatus, stateFieldState, stateFieldCluster, stateFieldResources, "memory"}, totalMem, 0)
	// Disk: can compute used percentage
	_ = setResourceUsage(u, []string{stateFieldStatus, stateFieldState, stateFieldCluster, stateFieldResources, "disk"}, diskCap, diskUsed)
}

// updateStatusTimestamps updates the lastUpdated and lastUpdatedBy fields
func updateStatusTimestamps(u *unstructured.Unstructured) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_ = unstructured.SetNestedField(u.Object, now, stateFieldStatus, stateFieldState, "lastUpdated")
	_ = unstructured.SetNestedField(u.Object, "talos-operator", stateFieldStatus, stateFieldState, "lastUpdatedBy")
}

// setResourceUsage writes capacity/used/percentage for a given resource path.
func setResourceUsage(u *unstructured.Unstructured, path []string, capacity, used int64) error {
	// Ensure map exists
	// path points to ... , "cpu"|"memory"|"disk"
	usage := map[string]any{
		"capacity":   capacity,
		"used":       used,
		"percentage": usagePercentage(capacity, used),
	}
	// Build full path for the object map
	if err := unstructured.SetNestedMap(u.Object, usage, path...); err != nil {
		return err
	}
	return nil
}

// shouldFetchKubeSystemUID checks if we need to fetch the kube-system UID
// Returns true only if the UID is not already stored in the annotation
func (m *StatusManager) shouldFetchKubeSystemUID(cluster *vitistackv1alpha1.KubernetesCluster) bool {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		return true
	}

	// If annotation is already present and non-empty, no need to fetch
	uid := annotations["vitistack.io/kube-system-uid"]
	return uid == ""
}

// SetKubeSystemUID sets the kube-system namespace UID annotation on the cluster
func (m *StatusManager) SetKubeSystemUID(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, uid string) error {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	// Skip if value unchanged
	if annotations["vitistack.io/kube-system-uid"] == uid {
		return nil
	}

	// A merge patch carries no resourceVersion, so it cannot lose to the status
	// writes made earlier in the same pass the way an Update would.
	base := cluster.DeepCopy()
	annotations["vitistack.io/kube-system-uid"] = uid
	cluster.SetAnnotations(annotations)

	if err := m.Patch(ctx, cluster, client.MergeFrom(base)); err != nil {
		cluster.SetAnnotations(base.GetAnnotations())
		return err
	}
	return nil
}
