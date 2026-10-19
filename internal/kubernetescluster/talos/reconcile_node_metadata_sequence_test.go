package talos

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	"github.com/vitistack/common/pkg/clients/k8sclient"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// enforceNodeResourceVersion makes Node patches behave like the API server: a stale
// metadata.resourceVersion is rejected with Conflict and every write bumps it.
func enforceNodeResourceVersion(t *testing.T, cs *kubefake.Clientset) {
	t.Helper()
	gvr := corev1.SchemeGroupVersion.WithResource("nodes")
	cs.PrependReactor("patch", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pa := action.(k8stesting.PatchAction)
		obj, err := cs.Tracker().Get(gvr, "", pa.GetName())
		if err != nil {
			return true, nil, err
		}
		cur := obj.(*corev1.Node)

		var precondition struct {
			Metadata struct {
				ResourceVersion string `json:"resourceVersion"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(pa.GetPatch(), &precondition); err != nil {
			return true, nil, err
		}
		if rv := precondition.Metadata.ResourceVersion; rv != "" && rv != cur.ResourceVersion {
			return true, nil, apierrors.NewConflict(corev1.Resource("nodes"), cur.Name, errors.New("stale resourceVersion "+rv))
		}

		original, err := json.Marshal(cur)
		if err != nil {
			return true, nil, err
		}
		patched, err := strategicpatch.StrategicMergePatch(original, pa.GetPatch(), corev1.Node{})
		if err != nil {
			return true, nil, err
		}
		out := &corev1.Node{}
		if err := json.Unmarshal(patched, out); err != nil {
			return true, nil, err
		}
		n, _ := strconv.Atoi(cur.ResourceVersion)
		out.ResourceVersion = strconv.Itoa(n + 1)
		if err := cs.Tracker().Update(gvr, out, ""); err != nil {
			return true, nil, err
		}
		return true, out, nil
	})
}

// The annotation reconciler and the label/taint reconciler share nodes.items within
// one pass; the second must not send the resourceVersion the first already advanced.
func TestNodeReconcilersShareFreshResourceVersion(t *testing.T) {
	prevDynamic := k8sclient.DynamicClient
	k8sclient.DynamicClient = dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	t.Cleanup(func() { k8sclient.DynamicClient = prevDynamic })

	cluster := &vitistackv1alpha1.KubernetesCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"}}
	cluster.Spec.Cluster.ClusterId = "cid"
	cluster.Spec.Cluster.NetworkNamespaceName = "netns"
	cluster.Spec.Topology.Workers.NodePools = []vitistackv1alpha1.KubernetesClusterNodePool{{
		Name:     "gpu",
		Metadata: vitistackv1alpha1.KubernetesClusterSpecMetadataDetails{Labels: map[string]string{"pool": "gpu"}},
		Taint:    []vitistackv1alpha1.KubernetesClusterTaint{{Key: "gpu", Value: "true", Effect: "NoSchedule"}},
	}}

	machine := &vitistackv1alpha1.Machine{ObjectMeta: metav1.ObjectMeta{
		Name:        testNameWrk0,
		Namespace:   "ns",
		Labels:      map[string]string{vitistackv1alpha1.ClusterIdAnnotation: "cid", vitistackv1alpha1.NodeRoleAnnotation: "worker"},
		Annotations: map[string]string{vitistackv1alpha1.NodePoolAnnotation: "gpu"},
	}}
	tm, _ := newStatusTestTalosManager(t, machine)

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: testNameWrk0, ResourceVersion: "1"}}
	clientset := kubefake.NewClientset(node)
	enforceNodeResourceVersion(t, clientset)
	ctx := context.Background()
	stored, err := clientset.CoreV1().Nodes().Get(ctx, testNameWrk0, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	nodes := &workloadNodes{client: clientset, items: []corev1.Node{*stored}}

	if err := tm.reconcileNodeAnnotations(ctx, cluster, nodes); err != nil {
		t.Fatal(err)
	}
	if nodes.items[0].ResourceVersion == stored.ResourceVersion {
		t.Errorf("annotation reconcile did not refresh cached resourceVersion %q", stored.ResourceVersion)
	}
	if err := tm.reconcileNodeLabelsAndTaints(ctx, cluster, nodes); err != nil {
		t.Fatal(err)
	}

	got, err := clientset.CoreV1().Nodes().Get(ctx, testNameWrk0, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Labels["pool"] != "gpu" {
		t.Errorf("label not applied after annotation reconcile: %v", got.Labels)
	}
	if len(got.Spec.Taints) != 1 || got.Spec.Taints[0].Key != "gpu" {
		t.Errorf("taint not applied after annotation reconcile: %v", got.Spec.Taints)
	}
	if got.Annotations[vitistackv1alpha1.ClusterIdAnnotation] != "cid" {
		t.Errorf("annotations lost: %v", got.Annotations)
	}
}
