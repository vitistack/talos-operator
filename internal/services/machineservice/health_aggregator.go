package machineservice

import (
	"fmt"
	"sort"
	"strings"
	"time"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
)

// MachineHealth summarises the health of a cluster's Machines for surfacing
// onto the parent KubernetesCluster status. AnyRunning means at least one
// Machine has reached the Running phase at any point in this evaluation — the
// signal used to suppress the provisioning-stuck timeout.
type MachineHealth struct {
	Total      int
	Failed     int
	Running    int
	AnyRunning bool
	// Message is a short human-readable summary suitable for
	// status.message ("VM provisioning failed: <first failure>") or empty
	// when nothing is wrong.
	Message string
	// FailureReason is a short tag (e.g. "MachineProvisioningFailed") used
	// for the Conditions Reason field. Empty when there are no failures.
	FailureReason string
}

// SummariseHealth walks a list of cluster Machines and returns a structured
// MachineHealth report. Failed machines are surfaced via Message including
// the first FailureMessage so the operator can see *why* provisioning broke
// (typical case: kubevirt missing storage class).
func SummariseHealth(machines []*vitistackv1alpha1.Machine) MachineHealth {
	h := MachineHealth{Total: len(machines)}
	if len(machines) == 0 {
		return h
	}

	failed := make([]string, 0)
	var firstFailureMsg string
	for _, m := range machines {
		switch m.Status.Phase {
		case MachinePhaseRunning:
			h.Running++
			h.AnyRunning = true
		case vitistackv1alpha1.MachinePhaseFailed:
			h.Failed++
			failed = append(failed, m.Name)
			if firstFailureMsg == "" {
				firstFailureMsg = firstNonEmpty(strPtr(m.Status.FailureMessage), strPtr(m.Status.FailureReason))
			}
		}
	}

	if h.Failed > 0 {
		sort.Strings(failed)
		h.FailureReason = "MachineProvisioningFailed"
		if firstFailureMsg == "" {
			firstFailureMsg = "no FailureMessage reported"
		}
		h.Message = fmt.Sprintf("%d/%d machines failed (%s): %s",
			h.Failed, h.Total, strings.Join(failed, ","), firstFailureMsg)
	}
	return h
}

// IsStuckProvisioning returns true when the cluster has lived past the
// configured provisioning timeout without any Machine ever reaching Running,
// and the cluster Phase is not yet a terminal/operational one. Caller is
// responsible for mapping that to Phase=Failed.
//
// timeout == 0 disables the check.
func IsStuckProvisioning(
	clusterCreation time.Time,
	currentPhase string,
	h MachineHealth,
	timeout time.Duration,
) bool {
	if timeout <= 0 {
		return false
	}
	if h.AnyRunning {
		return false
	}
	// Don't flap a cluster that is already operational or already terminal.
	switch currentPhase {
	case "Ready", "Failed", "ValidationError",
		"UpgradingTalos", "UpgradingKubernetes", "UpgradeFailed":
		return false
	}
	return time.Since(clusterCreation) > timeout
}

// strPtr derefs a *string, returning "" for nil.
func strPtr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// firstNonEmpty returns the first non-empty string from its arguments.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
