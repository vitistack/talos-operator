package talosconfigservice

import (
	"fmt"
	"maps"
	"strings"

	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

const controlPlaneRole = "control-plane"

// DesiredNodeLabelsAndTaints returns the labels and taints declared in the
// KubernetesCluster topology for the machine's role (control plane) or nodepool.
func DesiredNodeLabelsAndTaints(cluster *vitistackv1alpha1.KubernetesCluster, m *vitistackv1alpha1.Machine) (map[string]string, []corev1.Taint) {
	if m.Labels[vitistackv1alpha1.NodeRoleAnnotation] == controlPlaneRole {
		return maps.Clone(cluster.Spec.Topology.ControlPlane.Metadata.Labels), nil
	}

	poolName := m.Annotations[vitistackv1alpha1.NodePoolAnnotation]
	for i := range cluster.Spec.Topology.Workers.NodePools {
		np := &cluster.Spec.Topology.Workers.NodePools[i]
		if np.Name != poolName {
			continue
		}
		taints := make([]corev1.Taint, 0, len(np.Taint))
		for _, t := range np.Taint {
			effect := corev1.TaintEffect(t.Effect)
			switch effect {
			case corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule, corev1.TaintEffectNoExecute:
				taints = append(taints, corev1.Taint{Key: t.Key, Value: t.Value, Effect: effect})
			default:
				vlog.Warn(fmt.Sprintf("Ignoring taint %q on nodepool %q: invalid effect %q", t.Key, np.Name, t.Effect))
			}
		}
		return maps.Clone(np.Metadata.Labels), taints
	}
	return nil, nil
}

// buildRegisterWithTaintsPatch registers taints when the kubelet first creates the
// Node, so no pod can schedule before the reconciler runs. Kubelet does not own them
// afterwards, which leaves day-2 changes to the operator's reconciler.
func buildRegisterWithTaintsPatch(taints []corev1.Taint) string {
	if len(taints) == 0 {
		return ""
	}
	var patch strings.Builder
	patch.WriteString("machine:\n  kubelet:\n    extraConfig:\n      registerWithTaints:\n")
	for _, t := range taints {
		fmt.Fprintf(&patch, "        - key: %q\n          value: %q\n          effect: %s\n", t.Key, t.Value, t.Effect)
	}
	return patch.String()
}
