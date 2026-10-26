// Package drainservice drains Kubernetes nodes the way `kubectl drain` does:
// through the policy/v1 Eviction API, so PodDisruptionBudgets are honoured.
//
// It is the single drain implementation shared by rolling upgrades and node
// removal (scale-down / nodepool deletion), so the two cannot drift apart.
//
// A drain is non-blocking. Step makes one pass (cordon, evict what is left,
// look at what remains) and returns at once; callers requeue the reconcile
// until Step reports the node drained. The drain's start time lives on the
// Node itself (consts.DrainStartedAtAnnotation), so the timeout spans
// reconciles and operator restarts, and anyone can see the drain with
// `kubectl describe node`.
package drainservice

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/vitistack/common/pkg/loggers/vlog"
	"github.com/vitistack/talos-operator/pkg/consts"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// Purpose says why a node is drained. It is recorded as the DrainLabel value.
type Purpose string

const (
	PurposeUpgrade   Purpose = "upgrade"
	PurposeScaleDown Purpose = "scale-down"
)

const (
	// DefaultTimeout is how long a drain may wait on PodDisruptionBudgets
	// before it is reported as blocked. Like `kubectl drain`, the operator
	// does not give up and delete pods: the upgrade or removal stays on hold
	// and the drain keeps retrying until it completes or is forced.
	DefaultTimeout = 10 * time.Minute

	// MinRetryInterval and MaxRetryInterval bound the requeue delay between
	// drain passes. The delay grows with the drain's age (see retryAfter), so
	// a drain that is waiting on a PDB backs off without any stored counter.
	MinRetryInterval = 5 * time.Second
	MaxRetryInterval = 60 * time.Second

	// EventReasonEvictionBlocked is the reason of the Warning event recorded on
	// a pod whose eviction a PDB still rejects when the drain times out. The
	// event lands in the pod's own namespace, where its owner will see it.
	EventReasonEvictionBlocked = "EvictionBlocked"

	// stuckTerminatingSlack is how long past its deletion deadline a
	// Terminating pod is waited for. Beyond that the kubelet is not going to confirm the
	// deletion (node down, stuck finalizer), and the eviction was already
	// admitted against the PDB, so it no longer holds the drain. Mirrors
	// `kubectl drain --skip-wait-for-delete-timeout`.
	stuckTerminatingSlack = 60 * time.Second

	eventSource = "talos-operator"

	annotationTrue = "true"
)

// Options controls a drain.
type Options struct {
	// Timeout after which a drain that has not finished is reported as
	// blocked (and, with Force, allowed to proceed). Zero means DefaultTimeout.
	Timeout time.Duration
	// Force deletes pods whose eviction a PDB rejects, and lets a drain that
	// has not finished by Timeout proceed anyway. It bypasses PDBs and must
	// only be set on explicit request (consts.ForceDrainAnnotation).
	Force bool
	// Purpose is recorded on the node and in reports.
	Purpose Purpose
	// Now returns the current time; nil means time.Now. Set in tests.
	Now func() time.Time
}

func (o Options) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

func (o Options) timeout() time.Duration {
	if o.Timeout <= 0 {
		return DefaultTimeout
	}
	return o.Timeout
}

// BlockedPod is a pod that keeps a drain from completing.
type BlockedPod struct {
	Namespace string
	Name      string
	// PDBs are the PodDisruptionBudgets selecting this pod, filled in when an
	// eviction was rejected with 429.
	PDBs []string
	// Terminating is true when the pod is already going away and the drain
	// only waits for it; false when its eviction is being rejected.
	Terminating bool
	// Reason is the error the API server returned for the eviction, if any.
	Reason string
}

func (p BlockedPod) String() string {
	s := p.Namespace + "/" + p.Name
	switch {
	case p.Terminating:
		s += " (terminating)"
	case len(p.PDBs) > 0:
		s += " (blocked by PodDisruptionBudget " + strings.Join(p.PDBs, ", ") + ")"
	case p.Reason != "":
		s += " (" + p.Reason + ")"
	}
	return s
}

// Status is the outcome of one drain pass.
type Status struct {
	Node    string
	Purpose Purpose
	// Done means the caller may go on with the upgrade or removal.
	Done bool
	// Forced means Done was reached only because Options.Force let a timed-out
	// drain proceed; Remaining lists what was left behind.
	Forced    bool
	StartedAt time.Time
	Elapsed   time.Duration
	Timeout   time.Duration
	// TimedOut means the drain has been running longer than Timeout.
	TimedOut bool
	// Remaining are the pods still on the node, sorted by namespace/name.
	Remaining []BlockedPod
	// RetryAfter is when the caller should run the next pass.
	RetryAfter time.Duration
	// NodeMissing means no Kubernetes Node of that name exists, so there was
	// nothing to drain. Done is true; whether that is safe is the caller's call.
	NodeMissing bool
}

// Step makes one non-blocking drain pass over nodeName:
//
//  1. cordon the node and mark it with DrainLabel and, on the first pass,
//     DrainStartedAtAnnotation;
//  2. evict every pod still on it through the Eviction API, except DaemonSet
//     and mirror pods. An eviction rejected by a PDB (429) is left for the
//     next pass; the pod is never deleted unless opts.Force is set;
//  3. report what remains. The node is drained once a pass finds no pods
//     left, apart from pods stuck terminating past their grace period.
//
// A node that does not exist is reported as Done with NodeMissing set. Step returns an error
// only when the API server could not be asked.
func Step(ctx context.Context, client kubernetes.Interface, nodeName string, opts Options) (*Status, error) {
	now := opts.now()
	st := &Status{Node: nodeName, Purpose: opts.Purpose, Timeout: opts.timeout()}

	startedAt, found, err := markDraining(ctx, client, nodeName, opts.Purpose, now)
	if err != nil {
		return nil, err
	}
	if !found {
		st.Done, st.NodeMissing = true, true
		return st, nil
	}
	st.StartedAt = startedAt
	st.Elapsed = max(now.Sub(startedAt), 0)
	st.TimedOut = st.Elapsed >= st.Timeout

	podList, err := client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods on node %s: %w", nodeName, err)
	}

	rejected := evictPods(ctx, client, st, podList.Items, opts.Force, now)

	if len(rejected) > 0 {
		attachPDBs(ctx, client, rejected, st.Remaining)
		if st.TimedOut {
			warnPodOwners(ctx, client, st, rejected, now)
		}
	}
	sort.Slice(st.Remaining, func(i, j int) bool {
		return st.Remaining[i].Namespace+"/"+st.Remaining[i].Name < st.Remaining[j].Namespace+"/"+st.Remaining[j].Name
	})

	switch {
	case len(st.Remaining) == 0:
		st.Done = true
	case st.TimedOut && opts.Force:
		st.Done, st.Forced = true, true
	}
	st.RetryAfter = retryAfter(st.Elapsed)
	return st, nil
}

// evictPods evicts the pods on st.Node that a drain must remove, records in
// st.Remaining every pod still there, and returns the pods a PDB rejected.
// DaemonSet and mirror pods are skipped like `kubectl drain --ignore-daemonsets`;
// terminating pods are waited for unless stuck past their grace period.
func evictPods(ctx context.Context, client kubernetes.Interface, st *Status, pods []corev1.Pod, force bool, now time.Time) []*corev1.Pod {
	var rejected []*corev1.Pod
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName != st.Node || isDaemonSetPod(pod) || isMirrorPod(pod) {
			continue
		}
		if pod.DeletionTimestamp != nil {
			if !stuckTerminating(pod, now) {
				st.Remaining = append(st.Remaining, BlockedPod{Namespace: pod.Namespace, Name: pod.Name, Terminating: true})
			}
			continue
		}

		err := evictPod(ctx, client, pod, force)
		switch {
		case err == nil:
			// Admitted: the pod is terminating now and is confirmed gone on a
			// later pass.
			st.Remaining = append(st.Remaining, BlockedPod{Namespace: pod.Namespace, Name: pod.Name, Terminating: true})
		case apierrors.IsNotFound(err):
		default:
			vlog.Debug(fmt.Sprintf("Eviction of pod %s/%s on node %s rejected, will retry: %v", pod.Namespace, pod.Name, st.Node, err))
			st.Remaining = append(st.Remaining, BlockedPod{Namespace: pod.Namespace, Name: pod.Name, Reason: err.Error()})
			if apierrors.IsTooManyRequests(err) {
				rejected = append(rejected, pod)
			}
		}
	}
	return rejected
}

// retryAfter is the delay before the next drain pass: half the drain's age,
// clamped to [MinRetryInterval, MaxRetryInterval]. Successive passes therefore
// back off roughly geometrically while a PDB keeps rejecting evictions.
func retryAfter(elapsed time.Duration) time.Duration {
	return min(max(elapsed/2, MinRetryInterval), MaxRetryInterval)
}

// markDraining cordons nodeName and records the drain on it, keeping an
// existing start time. It returns the drain's start time and whether the node
// exists.
func markDraining(ctx context.Context, client kubernetes.Interface, nodeName string, purpose Purpose, now time.Time) (time.Time, bool, error) {
	n, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("failed to get node %s: %w", nodeName, err)
	}

	// The recorded start only counts while that drain is still under way: the
	// node is cordoned and carries our label. Otherwise (first pass, or an old
	// drain someone cancelled with `kubectl uncordon`) the clock starts now.
	_, ours := n.Labels[consts.DrainLabel]
	startedAt, parseErr := time.Parse(time.RFC3339, n.Annotations[consts.DrainStartedAtAnnotation])
	ongoing := parseErr == nil && ours && n.Spec.Unschedulable
	if !ongoing {
		startedAt = now.UTC().Truncate(time.Second)
	}
	if ongoing && n.Labels[consts.DrainLabel] == string(purpose) {
		return startedAt, true, nil
	}

	annotations := map[string]any{consts.DrainStartedAtAnnotation: startedAt.Format(time.RFC3339)}
	if !ongoing {
		// A new drain: note whether someone else had cordoned the node, so
		// Uncordon hands it back the way it was, and drop any note left by an
		// earlier drain.
		var wasCordoned any
		if n.Spec.Unschedulable && !ours {
			wasCordoned = annotationTrue
		}
		annotations[consts.DrainNodeWasCordonedAnnotation] = wasCordoned
	}
	patch := map[string]any{
		"spec": map[string]any{"unschedulable": true},
		"metadata": map[string]any{
			"labels":      map[string]any{consts.DrainLabel: string(purpose)},
			"annotations": annotations,
		},
	}
	if err := patchNode(ctx, client, nodeName, patch); err != nil {
		if apierrors.IsNotFound(err) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, fmt.Errorf("failed to cordon node %s: %w", nodeName, err)
	}
	if !n.Spec.Unschedulable {
		vlog.Info(fmt.Sprintf("Cordoned node %s for %s drain", nodeName, purpose))
	}
	return startedAt, true, nil
}

// Cordon cordons nodeName and marks it like a drain does, without evicting
// anything, so Uncordon and ReleaseAbandoned treat it the same way. A node that
// does not exist is not an error.
func Cordon(ctx context.Context, client kubernetes.Interface, nodeName string, purpose Purpose) error {
	_, _, err := markDraining(ctx, client, nodeName, purpose, time.Now())
	return err
}

// Uncordon removes the drain markers from nodeName and makes it schedulable
// again, unless it was already cordoned by someone else before the drain. A
// node that does not exist is not an error.
func Uncordon(ctx context.Context, client kubernetes.Interface, nodeName string) error {
	n, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get node %s: %w", nodeName, err)
	}
	patch := map[string]any{
		"metadata": map[string]any{
			"labels": map[string]any{consts.DrainLabel: nil},
			"annotations": map[string]any{
				consts.DrainStartedAtAnnotation:       nil,
				consts.DrainNodeWasCordonedAnnotation: nil,
			},
		},
	}
	if n.Annotations[consts.DrainNodeWasCordonedAnnotation] != annotationTrue {
		patch["spec"] = map[string]any{"unschedulable": false}
	}
	if err := patchNode(ctx, client, nodeName, patch); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to uncordon node %s: %w", nodeName, err)
	}
	return nil
}

// ReleaseAbandoned uncordons the nodes still marked with DrainLabel for one of
// purposes. The caller passes only the purposes it knows no operation is using
// any more (no scale-down pending, no upgrade plan), so any such node belongs
// to a drain whose operation was called off: a scale-down reverted, an
// upgrade reset. Nodes cordoned by anyone else carry no label and are left
// alone. It returns the nodes it released.
func ReleaseAbandoned(ctx context.Context, client kubernetes.Interface, purposes ...Purpose) ([]string, error) {
	if len(purposes) == 0 {
		return nil, nil
	}
	values := make([]string, len(purposes))
	for i, p := range purposes {
		values[i] = string(p)
	}
	selector := fmt.Sprintf("%s in (%s)", consts.DrainLabel, strings.Join(values, ","))
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("failed to list drained nodes: %w", err)
	}
	var released []string
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if err := Uncordon(ctx, client, n.Name); err != nil {
			return released, err
		}
		vlog.Info(fmt.Sprintf("Released node %s: the %s it was drained for is no longer in progress", n.Name, n.Labels[consts.DrainLabel]))
		released = append(released, n.Name)
	}
	return released, nil
}

func patchNode(ctx context.Context, client kubernetes.Interface, nodeName string, patch map[string]any) error {
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = client.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}

// evictPod asks the Eviction API to evict pod. With force, an eviction a PDB
// rejects falls back to a delete.
func evictPod(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod, force bool) error {
	preconditions := &metav1.Preconditions{UID: &pod.UID}
	err := client.CoreV1().Pods(pod.Namespace).EvictV1(ctx, &policyv1.Eviction{
		ObjectMeta:    metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
		DeleteOptions: &metav1.DeleteOptions{Preconditions: preconditions},
	})
	if err == nil || !force || !apierrors.IsTooManyRequests(err) {
		return err
	}

	vlog.Warn(fmt.Sprintf("Force drain: deleting pod %s/%s despite PodDisruptionBudget: %v", pod.Namespace, pod.Name, err))
	return client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: preconditions})
}

// stuckTerminating reports whether pod is still present well after its
// deletion deadline. The API server sets deletionTimestamp to the request time
// plus the grace period, so past it plus stuckTerminatingSlack nothing on the
// node is going to finish the pod.
func stuckTerminating(pod *corev1.Pod, now time.Time) bool {
	return now.Sub(pod.DeletionTimestamp.Time) > stuckTerminatingSlack
}

// attachPDBs fills in the PodDisruptionBudgets selecting each rejected pod.
func attachPDBs(ctx context.Context, client kubernetes.Interface, rejected []*corev1.Pod, remaining []BlockedPod) {
	pdbsByNamespace := map[string][]policyv1.PodDisruptionBudget{}
	for _, pod := range rejected {
		pdbs, ok := pdbsByNamespace[pod.Namespace]
		if !ok {
			list, err := client.PolicyV1().PodDisruptionBudgets(pod.Namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				vlog.Warn(fmt.Sprintf("Failed to list PodDisruptionBudgets in %s: %v", pod.Namespace, err))
			} else {
				pdbs = list.Items
			}
			pdbsByNamespace[pod.Namespace] = pdbs
		}
		names := matchingPDBs(pod, pdbs)
		for i := range remaining {
			if remaining[i].Namespace == pod.Namespace && remaining[i].Name == pod.Name {
				remaining[i].PDBs = names
			}
		}
	}
}

func matchingPDBs(pod *corev1.Pod, pdbs []policyv1.PodDisruptionBudget) []string {
	var names []string
	for i := range pdbs {
		// policy/v1: a nil selector matches no pods, an empty one matches every
		// pod in the namespace.
		if pdbs[i].Spec.Selector == nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(pdbs[i].Spec.Selector)
		if err != nil {
			continue
		}
		if selector.Matches(labels.Set(pod.Labels)) {
			names = append(names, pdbs[i].Name)
		}
	}
	return names
}

// warnPodOwners records a Warning event on each pod whose eviction is still
// rejected after the drain timed out. The event name is derived from the pod
// and the drain's start, so each pod is warned once per drain.
func warnPodOwners(ctx context.Context, client kubernetes.Interface, st *Status, rejected []*corev1.Pod, now time.Time) {
	for _, pod := range rejected {
		var pdbs []string
		for _, r := range st.Remaining {
			if r.Namespace == pod.Namespace && r.Name == pod.Name {
				pdbs = r.PDBs
			}
		}
		blocker := "a PodDisruptionBudget"
		if len(pdbs) > 0 {
			blocker = "PodDisruptionBudget " + strings.Join(pdbs, ", ")
		}
		ts := metav1.NewTime(now)
		event := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Name:      eventName(pod.Name, st.StartedAt),
				Namespace: pod.Namespace,
			},
			InvolvedObject: corev1.ObjectReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Namespace:  pod.Namespace,
				Name:       pod.Name,
				UID:        pod.UID,
			},
			Type:   corev1.EventTypeWarning,
			Reason: EventReasonEvictionBlocked,
			Message: fmt.Sprintf("Node %s has been draining for %s (%s) and this pod cannot be evicted because of %s. "+
				"The %s is on hold until the pod can be evicted.",
				st.Node, st.Elapsed.Round(time.Second), st.Purpose, blocker, st.Purpose),
			Source:         corev1.EventSource{Component: eventSource},
			FirstTimestamp: ts,
			LastTimestamp:  ts,
			Count:          1,
		}
		if _, err := client.CoreV1().Events(pod.Namespace).Create(ctx, event, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			vlog.Warn(fmt.Sprintf("Failed to record %s event on pod %s/%s: %v", EventReasonEvictionBlocked, pod.Namespace, pod.Name, err))
		}
	}
}

func isDaemonSetPod(pod *corev1.Pod) bool {
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

func isMirrorPod(pod *corev1.Pod) bool {
	_, ok := pod.Annotations[corev1.MirrorPodAnnotationKey]
	return ok
}

// eventName names the EvictionBlocked event for a pod and drain, trimming the
// pod name so the result stays within the 253-character object name limit.
func eventName(podName string, startedAt time.Time) string {
	const maxNameLength = 253
	suffix := fmt.Sprintf(".drain-%x", startedAt.Unix())
	if len(podName)+len(suffix) > maxNameLength {
		podName = strings.TrimRight(podName[:maxNameLength-len(suffix)], ".-")
	}
	return podName + suffix
}
