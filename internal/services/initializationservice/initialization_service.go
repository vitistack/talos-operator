package initializationservice

import (
	"context"
	"os"

	"github.com/vitistack/common/pkg/loggers/vlog"
	"github.com/vitistack/common/pkg/operator/crdcheck"
	"github.com/vitistack/talos-operator/internal/services/kubernetesproviderservice"
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

// EnsureKubernetesProvider ensures that a KubernetesProvider resource exists for this operator.
// This should be called after CheckPrerequisites to ensure the CRD is available.
// The function is idempotent and will not create duplicate resources.
func EnsureKubernetesProvider() {
	vlog.Info("Ensuring KubernetesProvider exists...")

	ctx := context.TODO()
	if err := kubernetesproviderservice.EnsureKubernetesProviderExists(ctx); err != nil {
		vlog.Error("Failed to ensure KubernetesProvider exists", err)
		os.Exit(1)
	}

	vlog.Info("✅ KubernetesProvider check completed")
}
