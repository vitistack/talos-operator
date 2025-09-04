package initializationservice

import (
	"os"
	"sort"

	"github.com/vitistack/common/pkg/clients/k8sclient"
	"github.com/vitistack/common/pkg/loggers/vlog"
)

// CheckPrerequisites verifies that required CRDs are installed before starting.
// It exits the process with code 1 if mandatory APIs are missing.
func CheckPrerequisites() {
	vlog.Info("Running prerequisite checks...")

	// Ensure the vitistack.io/v1alpha1 API exists with kubernetesclusters and machines resources
	gv := "vitistack.io/v1alpha1"
	required := []string{"kubernetesclusters", "machines"}

	// Discovery client should be initialized by k8sclient.Init()
	if k8sclient.DiscoveryClient == nil {
		vlog.Error("Kubernetes discovery client is not initialized", nil)
		os.Exit(1)
	}

	rl, err := k8sclient.DiscoveryClient.ServerResourcesForGroupVersion(gv)
	if err != nil {
		vlog.Error("Failed to discover API resources for "+gv, err)
		os.Exit(1)
	}

	got := map[string]bool{}
	for i := range rl.APIResources {
		// resource names are plural (e.g., kubernetesclusters, machines)
		got[rl.APIResources[i].Name] = true
	}

	missing := make([]string, 0)
	for _, name := range required {
		if !got[name] {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		sort.Strings(missing)
		vlog.Error("Missing required CRDs in "+gv+": "+joinStrings(missing, ", "), nil)
		os.Exit(1)
	}

	vlog.Info("✅ Prerequisite checks passed")
}

func joinStrings(items []string, sep string) string {
	switch len(items) {
	case 0:
		return ""
	case 1:
		return items[0]
	default:
		// Simple join to avoid importing strings just for this
		out := items[0]
		for i := 1; i < len(items); i++ {
			out += sep + " " + items[i]
		}
		return out
	}
}
