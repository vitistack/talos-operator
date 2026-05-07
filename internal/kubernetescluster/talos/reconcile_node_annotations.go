package talos

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/networknamespaceservice"
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

	// Resolve shared annotation values once
	country, az := extractNodeAnnotationDatacenterInfo(cluster.Spec.Cluster.Datacenter)
	networkNamespaceName := resolveNetworkNamespaceName(ctx, cluster)
	infrastructure := resolveInfrastructure(ctx)

	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		m, exists := machineMap[node.Name]
		if !exists {
			continue
		}

		desired := buildDesiredNodeAnnotations(cluster, m, country, az, networkNamespaceName, infrastructure)
		patch := computeAnnotationPatch(node.Annotations, desired)
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

		if _, err := clientset.CoreV1().Nodes().Patch(ctx, node.Name, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{}); err != nil {
			vlog.Warn(fmt.Sprintf("Failed to patch annotations on node %s: %v", node.Name, err))
			continue
		}

		vlog.Info(fmt.Sprintf("Reconciled %d annotations on node %s", len(patch), node.Name))
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

// computeAnnotationPatch returns only the annotations that differ from current.
func computeAnnotationPatch(current, desired map[string]string) map[string]string {
	patch := make(map[string]string)
	for k, v := range desired {
		if current[k] != v {
			patch[k] = v
		}
	}
	return patch
}

// extractNodeAnnotationDatacenterInfo splits a datacenter string into country and AZ.
func extractNodeAnnotationDatacenterInfo(datacenter string) (country string, az string) {
	parts := strings.Split(datacenter, "-")
	switch {
	case len(parts) >= 3:
		country = parts[0]
		az = parts[len(parts)-1]
	case len(parts) == 2:
		country = parts[0]
		az = ""
	default:
		country = datacenter
		az = ""
	}
	return country, az
}

// resolveNetworkNamespaceName resolves the network namespace name for annotation use.
func resolveNetworkNamespaceName(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) string {
	if cluster.Spec.Cluster.NetworkNamespaceName != "" {
		return cluster.Spec.Cluster.NetworkNamespaceName
	}
	ns, err := networknamespaceservice.FetchFirstNetworkNamespaceByNamespace(ctx, cluster.GetNamespace())
	if err != nil {
		vlog.Warn(fmt.Sprintf("failed to fetch NetworkNamespaces for namespace %q: %v", cluster.GetNamespace(), err))
		return ""
	}
	if ns != nil {
		return ns.Name
	}
	return ""
}

// resolveInfrastructure resolves the infrastructure name from the Vitistack CR.
func resolveInfrastructure(ctx context.Context) string {
	vitistack, err := vitistackservice.FetchVitistackByName(ctx, viper.GetString(consts.VITISTACK_NAME))
	if err != nil {
		vlog.Warn(fmt.Sprintf("failed to fetch Vitistack %q: %v", viper.GetString(consts.VITISTACK_NAME), err))
		return ""
	}
	if vitistack != nil && vitistack.Spec.Infrastructure != "" {
		return vitistack.Spec.Infrastructure
	}
	return ""
}
