package talos

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/networknamespaceservice"
	"github.com/vitistack/talos-operator/internal/services/talosconfigservice"
	"github.com/vitistack/talos-operator/internal/services/vitistackservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
)

// reconcileNodeAnnotations ensures that all nodes in the workload cluster have
// the expected vitistack annotations. This covers cases where the cluster spec
// has changed after initial provisioning (e.g. project, environment, region)
// and keeps annotations in sync without requiring a node re-provision.
func (t *TalosManager) reconcileNodeAnnotations(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	clientset, err := t.getWorkloadClusterClient(ctx, cluster)
	if err != nil || clientset == nil {
		return err
	}

	nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list workload cluster nodes for annotation reconciliation: %w", err)
	}

	machines, err := t.machineService.GetClusterMachines(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get cluster machines for annotation reconciliation: %w", err)
	}
	machineMap := make(map[string]*vitistackv1alpha1.Machine, len(machines))
	for _, m := range machines {
		machineMap[m.Name] = m
	}

	// Resolve shared annotation values once; track keys to skip on lookup failure
	// so we don't clobber valid annotations with empty strings due to transient errors.
	country, az := talosconfigservice.ExtractDatacenterInfo(cluster.Spec.Cluster.Datacenter)
	skipKeys := make(map[string]bool)

	networkNamespaceName, err := resolveNetworkNamespaceName(ctx, cluster)
	if err != nil {
		skipKeys[vitistackv1alpha1.ClusterWorkspaceAnnotation] = true
	}

	infrastructure, err := resolveInfrastructure(ctx)
	if err != nil {
		skipKeys[vitistackv1alpha1.MachineInfrastructureAnnotation] = true
		skipKeys[vitistackv1alpha1.InfrastructureAnnotation] = true //nolint:staticcheck // backward compatibility
	}

	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		m, exists := machineMap[node.Name]
		if !exists {
			continue
		}

		desired := buildDesiredNodeAnnotations(cluster, m, country, az, networkNamespaceName, infrastructure)
		// Remove keys that couldn't be resolved reliably to avoid
		// overwriting valid annotations with empty strings.
		for k := range skipKeys {
			delete(desired, k)
		}
		// Skip annotations Talos owns via machine-config nodeAnnotations:
		// Talos's KubernetesNode controller will overwrite them on each
		// pass, causing an infinite update loop here. The durable path to
		// change a Talos-owned annotation is to update the machine config.
		talosOwned := parseTalosOwnedAnnotations(node.Annotations[talosOwnedAnnotationsKey])
		patch := computeAnnotationPatch(node.Annotations, desired, talosOwned)
		if len(patch) == 0 {
			continue
		}

		patchBytes, err := json.Marshal(map[string]interface{}{
			"metadata": map[string]interface{}{
				"annotations": patch,
			},
		})
		if err != nil {
			vlog.Warn(fmt.Sprintf("Failed to marshal annotation patch for node %s: %v", node.Name, err))
			continue
		}
		updated, removed := countPatchOps(patch)

		if _, err := clientset.CoreV1().Nodes().Patch(ctx, node.Name, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{}); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to patch annotations on node %s: %v", node.Name, err))
			continue
		}

		vlog.Info(fmt.Sprintf("Reconciled annotations on node %s: %d updated, %d removed", node.Name, updated, removed))
	}

	return nil
}

// buildDesiredNodeAnnotations returns the expected annotation map for a node.
func buildDesiredNodeAnnotations(
	cluster *vitistackv1alpha1.KubernetesCluster,
	m *vitistackv1alpha1.Machine,
	country, az, networkNamespaceName, infrastructure string,
) map[string]string {
	annotations := map[string]string{
		vitistackv1alpha1.ClusterIdAnnotation:             cluster.Spec.Cluster.ClusterId,
		vitistackv1alpha1.ClusterNameAnnotation:           cluster.Name,
		vitistackv1alpha1.ClusterProjectAnnotation:        cluster.Spec.Cluster.Project,
		vitistackv1alpha1.EnvironmentAnnotation:           cluster.Spec.Cluster.Environment,
		vitistackv1alpha1.CountryAnnotation:               country,
		vitistackv1alpha1.AzAnnotation:                    az,
		vitistackv1alpha1.RegionAnnotation:                cluster.Spec.Cluster.Region,
		vitistackv1alpha1.KubernetesProviderAnnotation:    string(cluster.Spec.Cluster.Provider),
		vitistackv1alpha1.MachineProviderAnnotation:       string(m.Status.Provider),
		vitistackv1alpha1.MachineClassAnnotation:          m.Spec.MachineClass,
		vitistackv1alpha1.MachineIdAnnotation:             m.Name,
		vitistackv1alpha1.ClusterWorkspaceAnnotation:      networkNamespaceName,
		vitistackv1alpha1.MachineInfrastructureAnnotation: infrastructure,
		vitistackv1alpha1.VMProviderAnnotation:            string(m.Status.Provider), //nolint:staticcheck // backward compatibility
		vitistackv1alpha1.InfrastructureAnnotation:        infrastructure,            //nolint:staticcheck // backward compatibility
	}

	if nodePool, ok := m.Annotations[vitistackv1alpha1.NodePoolAnnotation]; ok && nodePool != "" {
		annotations[vitistackv1alpha1.NodePoolAnnotation] = nodePool
	}

	return annotations
}

// talosOwnedAnnotationsKey is the annotation Talos writes on each Node listing
// the annotations it owns via machine-config nodeAnnotations. Talos's
// KubernetesNode controller reasserts any key in this list on every pass.
const talosOwnedAnnotationsKey = "talos.dev/owned-annotations"

// parseTalosOwnedAnnotations parses the JSON-encoded list of annotation keys
// Talos publishes on the Node. Returns an empty set if the value is missing
// or malformed — in that case the reconciler simply doesn't skip anything.
func parseTalosOwnedAnnotations(raw string) map[string]bool {
	owned := make(map[string]bool)
	if raw == "" {
		return owned
	}
	var keys []string
	if err := json.Unmarshal([]byte(raw), &keys); err != nil {
		return owned
	}
	for _, k := range keys {
		owned[k] = true
	}
	return owned
}

// computeAnnotationPatch computes a JSON merge-patch for annotations.
// It sets desired values that differ from current, and sets keys this
// reconciler owns to nil when they are present on the node but absent
// from the desired set — causing MergePatch to delete them. Keys in
// talosOwned are skipped entirely: Talos reasserts those from its
// machine config, so patching them causes a fight loop.
func computeAnnotationPatch(current, desired map[string]string, talosOwned map[string]bool) map[string]interface{} {
	patch := make(map[string]interface{})

	// Set or update desired annotations
	for k, v := range desired {
		if talosOwned[k] {
			continue
		}
		if current[k] != v {
			patch[k] = v
		}
	}

	// Remove reconciler-owned annotations that are no longer desired
	for _, k := range reconcilerManagedAnnotations() {
		if talosOwned[k] {
			continue
		}
		if _, isDesired := desired[k]; !isDesired {
			if _, exists := current[k]; exists {
				patch[k] = nil
			}
		}
	}

	return patch
}

// reconcilerManagedAnnotations returns the annotation keys this reconciler
// is responsible for syncing on workload-cluster Nodes. Any key in this set
// that is present on a Node but absent from the desired map will be removed.
//
// This is intentionally narrower than vitistackv1alpha1.GetAllVitistackAnnotations:
// keys like K8sEndpointAnnotation, NodeRoleAnnotation, NodeFQDNAnnotation,
// ClusterFQDNAnnotation and VMIdAnnotation are set by other actors (Talos
// machine-config nodeAnnotations, kubelet, etc.) and must not be touched
// here — otherwise this reconciler and the owner fight in a loop on every
// pass.
func reconcilerManagedAnnotations() []string {
	return []string{
		vitistackv1alpha1.ClusterIdAnnotation,
		vitistackv1alpha1.ClusterNameAnnotation,
		vitistackv1alpha1.ClusterProjectAnnotation,
		vitistackv1alpha1.EnvironmentAnnotation,
		vitistackv1alpha1.CountryAnnotation,
		vitistackv1alpha1.AzAnnotation,
		vitistackv1alpha1.RegionAnnotation,
		vitistackv1alpha1.KubernetesProviderAnnotation,
		vitistackv1alpha1.MachineProviderAnnotation,
		vitistackv1alpha1.MachineClassAnnotation,
		vitistackv1alpha1.MachineIdAnnotation,
		vitistackv1alpha1.ClusterWorkspaceAnnotation,
		vitistackv1alpha1.MachineInfrastructureAnnotation,
		vitistackv1alpha1.NodePoolAnnotation,
		// Deprecated: kept for backward compatibility during transition
		vitistackv1alpha1.VMProviderAnnotation,     //nolint:staticcheck // backward compatibility
		vitistackv1alpha1.InfrastructureAnnotation, //nolint:staticcheck // backward compatibility
	}
}

// countPatchOps counts the number of updates and removals in a patch.
func countPatchOps(patch map[string]interface{}) (updated, removed int) {
	for _, v := range patch {
		if v == nil {
			removed++
		} else {
			updated++
		}
	}
	return updated, removed
}

// resolveNetworkNamespaceName resolves the network namespace name for annotation use.
// Returns an error when the lookup fails so the caller can skip the key rather than
// clobbering a valid annotation with an empty string.
func resolveNetworkNamespaceName(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) (string, error) {
	if cluster.Spec.Cluster.NetworkNamespaceName != "" {
		return cluster.Spec.Cluster.NetworkNamespaceName, nil
	}
	ns, err := networknamespaceservice.FetchFirstNetworkNamespaceByNamespace(ctx, cluster.GetNamespace())
	if err != nil {
		vlog.Warn(fmt.Sprintf("failed to fetch NetworkNamespaces for namespace %q: %v", cluster.GetNamespace(), err))
		return "", err
	}
	if ns != nil {
		return ns.Name, nil
	}
	return "", nil
}

// resolveInfrastructure resolves the infrastructure name from the Vitistack CR.
// Returns an error when the lookup fails so the caller can skip the key rather than
// clobbering a valid annotation with an empty string.
func resolveInfrastructure(ctx context.Context) (string, error) {
	vitistack, err := vitistackservice.FetchVitistackByName(ctx, viper.GetString(consts.VITISTACK_NAME))
	if err != nil {
		vlog.Warn(fmt.Sprintf("failed to fetch Vitistack %q: %v", viper.GetString(consts.VITISTACK_NAME), err))
		return "", err
	}
	if vitistack != nil && vitistack.Spec.Infrastructure != "" {
		return vitistack.Spec.Infrastructure, nil
	}
	return "", nil
}
