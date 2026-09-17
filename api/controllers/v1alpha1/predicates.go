package v1alpha1

import (
	"time"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// kubernetesClusterEventFilter admits the KubernetesCluster updates that can
// change what a reconcile does: spec edits (generation), annotation and label
// edits (upgrade requests, do-not-reconcile, reset-upgrade-state) and
// deletion. Status-only updates are dropped. The operator writes status itself
// on every pass, and letting those writes re-queue the cluster kept every
// cluster reconciling back-to-back instead of every ControllerRequeueDelay.
// Create and delete events always pass.
func kubernetesClusterEventFilter() predicate.Predicate {
	return predicate.Or(
		predicate.GenerationChangedPredicate{},
		predicate.AnnotationChangedPredicate{},
		predicate.LabelChangedPredicate{},
		deletionRequested(),
	)
}

// machineEventFilter admits Machine updates the cluster reconcile can act on,
// and drops provider heartbeats that only move status.lastUpdated. Anything
// else the filter drops is still seen on the next ControllerRequeueDelay poll.
func machineEventFilter() predicate.Predicate {
	return predicate.Or(
		predicate.GenerationChangedPredicate{},
		predicate.AnnotationChangedPredicate{},
		predicate.LabelChangedPredicate{},
		machineStatusChanged(),
	)
}

// deletionRequested passes updates that set a deletion timestamp, so cleanup
// starts right away even if the API server did not bump the generation.
func deletionRequested() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}
			return e.ObjectOld.GetDeletionTimestamp().IsZero() && !e.ObjectNew.GetDeletionTimestamp().IsZero()
		},
	}
}

// machineStatusChanged passes Machine updates whose status differs in anything
// other than LastUpdated.
func machineStatusChanged() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldMachine, okOld := e.ObjectOld.(*vitistackv1alpha1.Machine)
			newMachine, okNew := e.ObjectNew.(*vitistackv1alpha1.Machine)
			if !okOld || !okNew {
				return true
			}
			oldStatus := oldMachine.Status.DeepCopy()
			newStatus := newMachine.Status.DeepCopy()
			oldStatus.LastUpdated = metav1.Time{}
			newStatus.LastUpdated = metav1.Time{}
			return !equality.Semantic.DeepEqual(oldStatus, newStatus)
		},
	}
}

// upgradeRequeueAfter guarantees an in-progress upgrade is polled again. A
// step can return a zero or negative delay (for example when its settling
// window runs out between the check and the calculation), and status writes
// no longer re-queue the cluster, so a zero delay would stall the upgrade.
func upgradeRequeueAfter(d time.Duration) time.Duration {
	if d <= 0 {
		return ControllerRequeueDelay
	}
	return d
}
