package initializationservice

import (
	"context"

	"github.com/vitistack/common/pkg/loggers/vlog"
	"github.com/vitistack/common/pkg/operator/crdcheck"
)

// CheckPrerequisites verifies that required CRDs are installed before starting.
// It exits the process with code 1 if mandatory APIs are missing.
func CheckPrerequisites() {
	vlog.Info("Running prerequisite checks...")

	crdcheck.MustEnsureInstalled(context.TODO(),
		// your CRD plural
		crdcheck.Ref{Group: "vitistack.io", Version: "v1alpha1", Resource: "machines"},
		crdcheck.Ref{Group: "vitistack.io", Version: "v1alpha1", Resource: "kubernetesclusters"},
		crdcheck.Ref{Group: "vitistack.io", Version: "v1alpha1", Resource: "kubernetesproviders"},
		crdcheck.Ref{Group: "vitistack.io", Version: "v1alpha1", Resource: "machineproviders"},
		crdcheck.Ref{Group: "vitistack.io", Version: "v1alpha1", Resource: "networknamespaces"},
		crdcheck.Ref{Group: "vitistack.io", Version: "v1alpha1", Resource: "networkconfigurations"},
		crdcheck.Ref{Group: "vitistack.io", Version: "v1alpha1", Resource: "vitistacks"},
	)

	vlog.Info("✅ Prerequisite checks passed")
}
