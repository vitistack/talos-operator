package drainservice

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/pkg/consts"
	"k8s.io/client-go/kubernetes"
)

const (
	conditionTrue  = "True"
	conditionFalse = "False"

	// ConditionType is the KubernetesCluster condition reporting node drains.
	// True means no drain is holding anything up.
	ConditionType = "NodeDrain"
	// ReasonDraining (False): a drain is in progress within its timeout.
	ReasonDraining = "Draining"
	// ReasonBlocked (False): the drain has passed its timeout; the message
	// names the pods and PodDisruptionBudgets holding it. The upgrade or
	// removal is on hold and the drain keeps retrying.
	ReasonBlocked = "Blocked"
	// ReasonCordoned (False): a node was cordoned without a drain (Kubernetes
	// upgrade) and is uncordoned when that operation ends.
	ReasonCordoned = "Cordoned"
	// ReasonFailed (False): the drain could not be attempted: the workload
	// cluster's API is unreachable, or the Node is missing before an upgrade.
	ReasonFailed = "Failed"
	// ReasonDrained (True): the node was drained without bypassing any PDB.
	ReasonDrained = "Drained"
	// ReasonForced (True): ForceDrainAnnotation let a timed-out drain proceed.
	ReasonForced = "Forced"
	// ReasonIdle (True): no drain is in progress and no node is left cordoned
	// by an abandoned drain.
	ReasonIdle = "Idle"

	// DefaultErrorRetry is the requeue delay after a drain error that carries
	// no delay of its own.
	DefaultErrorRetry = 30 * time.Second
)

// ErrIncomplete wraps every error returned by Run: the node is not drained,
// and the upgrade or removal that needs the drain must not proceed yet.
var ErrIncomplete = errors.New("node drain incomplete")

// PendingError is returned by Run while a drain is still in progress (or
// blocked past its timeout). It is not a failure: requeue after RetryAfter.
type PendingError struct {
	Status     *Status
	RetryAfter time.Duration
	msg        string
}

func (e *PendingError) Error() string { return e.msg }
func (e *PendingError) Unwrap() error { return ErrIncomplete }

// RetryAfter returns when to retry after err: the drain's own backoff for a
// PendingError, DefaultErrorRetry otherwise.
func RetryAfter(err error) time.Duration {
	var pending *PendingError
	if errors.As(err, &pending) && pending.RetryAfter > 0 {
		return pending.RetryAfter
	}
	return DefaultErrorRetry
}

// ConditionSetter is the part of the status manager used to report drains.
type ConditionSetter interface {
	SetCondition(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster, condType, status, reason, message string) error
}

// OptionsForCluster returns the drain options for cluster: the timeout from
// DRAIN_TIMEOUT_MINUTES and Force only when ForceDrainAnnotation is "true".
func OptionsForCluster(cluster *vitistackv1alpha1.KubernetesCluster, purpose Purpose) Options {
	opts := Options{Timeout: DefaultTimeout, Purpose: purpose}
	if mins := viper.GetInt(consts.DRAIN_TIMEOUT_MINUTES); mins >= 1 {
		opts.Timeout = time.Duration(mins) * time.Minute
	}
	opts.Force = cluster.GetAnnotations()[consts.ForceDrainAnnotation] == annotationTrue
	return opts
}

// Run makes one drain pass over nodeName (see Step) and records the outcome as
// the NodeDrain condition on cluster.
//
// It returns nil once the node is drained, or forced past its timeout. While
// the drain is still running it returns a *PendingError: the caller must not
// go on with the upgrade or removal, and should requeue after RetryAfter(err).
// Any other error also wraps ErrIncomplete.
func Run(ctx context.Context, client kubernetes.Interface, setter ConditionSetter,
	cluster *vitistackv1alpha1.KubernetesCluster, nodeName string, opts Options) error {
	st, err := Step(ctx, client, nodeName, opts)
	if err != nil {
		return ReportUnavailable(ctx, setter, cluster, nodeName, opts, err)
	}

	switch {
	case st.NodeMissing && opts.Purpose == PurposeUpgrade:
		// The upgrade reboots the machine whatever the Node object says, so a
		// missing Node is no proof that it runs no pods.
		return ReportUnavailable(ctx, setter, cluster, nodeName, opts,
			fmt.Errorf("no Kubernetes node named %s found, so its pods cannot be drained", nodeName))
	case st.NodeMissing:
		report(ctx, setter, cluster, conditionTrue, ReasonDrained,
			fmt.Sprintf("Node %s not found in the workload cluster; nothing to drain for %s", nodeName, st.Purpose))
		return nil
	case st.Done && st.Forced:
		msg := fmt.Sprintf("Node %s did not drain within %s; continuing with the %s because %s=true. Left on the node: %s",
			nodeName, st.Timeout, st.Purpose, consts.ForceDrainAnnotation, listPods(st.Remaining))
		vlog.Warn(msg)
		report(ctx, setter, cluster, conditionTrue, ReasonForced, msg)
		return nil
	case st.Done:
		report(ctx, setter, cluster, conditionTrue, ReasonDrained, fmt.Sprintf("Node %s drained for %s", nodeName, st.Purpose))
		return nil
	}

	var reason, msg string
	if st.TimedOut {
		reason = ReasonBlocked
		msg = fmt.Sprintf("Drain of node %s for %s has not completed after %s (started %s); the %s is on hold. Waiting on: %s. "+
			"Retrying every %s; set %s=true on the KubernetesCluster to force it.",
			nodeName, st.Purpose, st.Timeout, st.StartedAt.Format(time.RFC3339), st.Purpose,
			listPods(st.Remaining), MaxRetryInterval, consts.ForceDrainAnnotation)
	} else {
		reason = ReasonDraining
		msg = fmt.Sprintf("Draining node %s for %s (started %s). Waiting on: %s",
			nodeName, st.Purpose, st.StartedAt.Format(time.RFC3339), listPods(st.Remaining))
	}
	report(ctx, setter, cluster, conditionFalse, reason, msg)
	return &PendingError{Status: st, RetryAfter: st.RetryAfter, msg: msg}
}

// ReportUnavailable handles a drain of nodeName that could not be attempted or
// completed because of cause (the workload cluster's API is unreachable, the
// Node is missing before an upgrade). It reports NodeDrain=False/Failed and
// returns an error wrapping ErrIncomplete, so the operation is held; with
// opts.Force it reports True/Forced and returns nil instead.
func ReportUnavailable(ctx context.Context, setter ConditionSetter, cluster *vitistackv1alpha1.KubernetesCluster,
	nodeName string, opts Options, cause error) error {
	if opts.Force {
		msg := fmt.Sprintf("Drain of node %s for %s failed; continuing because %s=true: %v",
			nodeName, opts.Purpose, consts.ForceDrainAnnotation, cause)
		vlog.Warn(msg)
		report(ctx, setter, cluster, conditionTrue, ReasonForced, msg)
		return nil
	}
	report(ctx, setter, cluster, conditionFalse, ReasonFailed,
		fmt.Sprintf("Drain of node %s for %s failed: %v", nodeName, opts.Purpose, cause))
	return fmt.Errorf("%w: %w", ErrIncomplete, cause)
}

// ReportCordoned records that nodeName was cordoned for purpose without a
// drain, so the release of abandoned drains keeps an eye on it.
func ReportCordoned(ctx context.Context, setter ConditionSetter, cluster *vitistackv1alpha1.KubernetesCluster, nodeName string, purpose Purpose) {
	report(ctx, setter, cluster, conditionFalse, ReasonCordoned, fmt.Sprintf("Node %s cordoned for %s", nodeName, purpose))
}

// ReportIdle records that no drain is in progress.
func ReportIdle(ctx context.Context, setter ConditionSetter, cluster *vitistackv1alpha1.KubernetesCluster) {
	report(ctx, setter, cluster, conditionTrue, ReasonIdle, "No node drain in progress")
}

func report(ctx context.Context, setter ConditionSetter, cluster *vitistackv1alpha1.KubernetesCluster, status, reason, message string) {
	if setter == nil || cluster == nil {
		return
	}
	if err := setter.SetCondition(ctx, cluster, ConditionType, status, reason, message); err != nil {
		vlog.Warn(fmt.Sprintf("Failed to set %s condition on %s/%s: %v", ConditionType, cluster.Namespace, cluster.Name, err))
	}
}

// listPods renders pods for a condition message, capped so a node with many
// pods cannot blow up the status.
func listPods(pods []BlockedPod) string {
	const maxListed = 10
	parts := make([]string, 0, min(len(pods), maxListed)+1)
	for i, p := range pods {
		if i == maxListed {
			parts = append(parts, fmt.Sprintf("and %d more", len(pods)-maxListed))
			break
		}
		parts = append(parts, p.String())
	}
	return strings.Join(parts, "; ")
}
