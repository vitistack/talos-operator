// Package clusterlog provides shared log-tagging helpers for cluster-scoped
// log lines, so that every package emits a consistent "cluster=<ns>/<id>"
// token that is easy to grep across pods and clusters.
package clusterlog

import (
	"fmt"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
)

// Tag returns a stable, grep-friendly cluster identifier for log lines:
// "cluster=<namespace>/<clusterId>". Falls back to the resource Name when
// spec.data.clusterId hasn't been set yet (very early in the lifecycle).
func Tag(cluster *vitistackv1alpha1.KubernetesCluster) string {
	if cluster == nil {
		return "cluster=<nil>"
	}
	clusterID := cluster.Spec.Cluster.ClusterId
	if clusterID == "" {
		clusterID = cluster.Name
	}
	return fmt.Sprintf("cluster=%s/%s", cluster.Namespace, clusterID)
}

// TagFromMachine returns the same "cluster=<namespace>/<clusterId>" token
// derived from a Machine resource — its namespace equals the cluster's, and
// the cluster id is carried in the ClusterIdAnnotation label. Use this from
// code paths that operate on Machine objects without holding the parent
// KubernetesCluster.
func TagFromMachine(m *vitistackv1alpha1.Machine) string {
	if m == nil {
		return "cluster=<nil>"
	}
	clusterID := m.Labels[vitistackv1alpha1.ClusterIdAnnotation]
	if clusterID == "" {
		clusterID = "<unknown>"
	}
	return fmt.Sprintf("cluster=%s/%s", m.Namespace, clusterID)
}
