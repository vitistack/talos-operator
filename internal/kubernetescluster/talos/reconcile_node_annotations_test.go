package talos

import (
	"testing"

	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/talosconfigservice"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestExtractDatacenterInfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		datacenter string
		wantCC     string
		wantAZ     string
	}{
		{name: "three parts", datacenter: "no-east-1", wantCC: "no", wantAZ: "1"},
		{name: "four parts", datacenter: "us-west-coast-2", wantCC: "us", wantAZ: "2"},
		{name: "two parts", datacenter: "de-west", wantCC: "de", wantAZ: ""},
		{name: "single part", datacenter: "local", wantCC: "local", wantAZ: ""},
		{name: "empty string", datacenter: "", wantCC: "", wantAZ: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cc, az := talosconfigservice.ExtractDatacenterInfo(tt.datacenter)
			if cc != tt.wantCC {
				t.Errorf("country = %q, want %q", cc, tt.wantCC)
			}
			if az != tt.wantAZ {
				t.Errorf("az = %q, want %q", az, tt.wantAZ)
			}
		})
	}
}

func TestBuildDesiredNodeAnnotations(t *testing.T) {
	t.Parallel()

	cluster := &vitistackv1alpha1.KubernetesCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster"},
		Spec: vitistackv1alpha1.KubernetesClusterSpec{
			Cluster: vitistackv1alpha1.KubernetesClusterSpecData{
				ClusterId:   "uuid-123",
				Project:     "myproject",
				Environment: "prod",
				Region:      "europe",
				Provider:    "talos",
			},
		},
	}

	machine := &vitistackv1alpha1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node-1",
			Annotations: map[string]string{vitistackv1alpha1.NodePoolAnnotation: "pool-a"},
		},
		Spec: vitistackv1alpha1.MachineSpec{MachineClass: "large"},
	}
	machine.Status.Provider = "kubevirt"

	result := buildDesiredNodeAnnotations(cluster, machine, "no", "1", "ws-ns", "infra-prod")

	checks := map[string]string{
		vitistackv1alpha1.ClusterIdAnnotation:             "uuid-123",
		vitistackv1alpha1.ClusterNameAnnotation:           "test-cluster",
		vitistackv1alpha1.ClusterProjectAnnotation:        "myproject",
		vitistackv1alpha1.EnvironmentAnnotation:           "prod",
		vitistackv1alpha1.CountryAnnotation:               "no",
		vitistackv1alpha1.AzAnnotation:                    "1",
		vitistackv1alpha1.RegionAnnotation:                "europe",
		vitistackv1alpha1.KubernetesProviderAnnotation:    "talos",
		vitistackv1alpha1.MachineProviderAnnotation:       "kubevirt",
		vitistackv1alpha1.MachineClassAnnotation:          "large",
		vitistackv1alpha1.MachineIdAnnotation:             "node-1",
		vitistackv1alpha1.ClusterWorkspaceAnnotation:      "ws-ns",
		vitistackv1alpha1.MachineInfrastructureAnnotation: "infra-prod",
		vitistackv1alpha1.VMProviderAnnotation:            "kubevirt",   //nolint:staticcheck // testing deprecated annotation
		vitistackv1alpha1.InfrastructureAnnotation:        "infra-prod", //nolint:staticcheck // testing deprecated annotation
		vitistackv1alpha1.NodePoolAnnotation:              "pool-a",
	}

	for k, want := range checks {
		if got := result[k]; got != want {
			t.Errorf("annotation %s = %q, want %q", k, got, want)
		}
	}
}

func TestBuildDesiredNodeAnnotations_NoNodePool(t *testing.T) {
	t.Parallel()

	cluster := &vitistackv1alpha1.KubernetesCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c"},
		Spec:       vitistackv1alpha1.KubernetesClusterSpec{},
	}
	machine := &vitistackv1alpha1.Machine{
		ObjectMeta: metav1.ObjectMeta{Name: "n"},
	}

	result := buildDesiredNodeAnnotations(cluster, machine, "", "", "", "")

	if _, exists := result[vitistackv1alpha1.NodePoolAnnotation]; exists {
		t.Error("NodePoolAnnotation should not be present when machine has no nodepool annotation")
	}
}

func TestComputeAnnotationPatch_UpdatesChanged(t *testing.T) {
	t.Parallel()

	current := map[string]string{
		vitistackv1alpha1.ClusterNameAnnotation:    "old-name",
		vitistackv1alpha1.EnvironmentAnnotation:    "dev",
		vitistackv1alpha1.MachineClassAnnotation:   "medium",
		vitistackv1alpha1.MachineIdAnnotation:      "node-1",
		vitistackv1alpha1.ClusterIdAnnotation:      "uuid-1",
		vitistackv1alpha1.ClusterProjectAnnotation: "proj",
	}

	desired := map[string]string{
		vitistackv1alpha1.ClusterNameAnnotation:    "new-name", // changed
		vitistackv1alpha1.EnvironmentAnnotation:    "dev",      // unchanged
		vitistackv1alpha1.MachineClassAnnotation:   "large",    // changed
		vitistackv1alpha1.MachineIdAnnotation:      "node-1",   // unchanged
		vitistackv1alpha1.ClusterIdAnnotation:      "uuid-1",   // unchanged
		vitistackv1alpha1.ClusterProjectAnnotation: "new-proj", // changed
	}

	patch := computeAnnotationPatch(current, desired, nil)

	// Should contain the 3 changed keys
	if v, ok := patch[vitistackv1alpha1.ClusterNameAnnotation]; !ok || v != "new-name" {
		t.Errorf("expected ClusterNameAnnotation = 'new-name', got %v", v)
	}
	if v, ok := patch[vitistackv1alpha1.MachineClassAnnotation]; !ok || v != "large" {
		t.Errorf("expected MachineClassAnnotation = 'large', got %v", v)
	}
	if v, ok := patch[vitistackv1alpha1.ClusterProjectAnnotation]; !ok || v != "new-proj" {
		t.Errorf("expected ClusterProjectAnnotation = 'new-proj', got %v", v)
	}

	// Should NOT contain unchanged keys
	if _, ok := patch[vitistackv1alpha1.EnvironmentAnnotation]; ok {
		t.Error("unchanged EnvironmentAnnotation should not be in patch")
	}
	if _, ok := patch[vitistackv1alpha1.MachineIdAnnotation]; ok {
		t.Error("unchanged MachineIdAnnotation should not be in patch")
	}
}

func TestComputeAnnotationPatch_DeletesManagedKeys(t *testing.T) {
	t.Parallel()

	current := map[string]string{
		vitistackv1alpha1.ClusterNameAnnotation: "c",
		vitistackv1alpha1.NodePoolAnnotation:    "pool-a", // managed key, not in desired
		"some.other/annotation":                 "keep",   // unmanaged, should be ignored
	}

	desired := map[string]string{
		vitistackv1alpha1.ClusterNameAnnotation: "c", // unchanged
	}

	patch := computeAnnotationPatch(current, desired, nil)

	// NodePoolAnnotation is managed and missing from desired → should be nil (delete)
	v, ok := patch[vitistackv1alpha1.NodePoolAnnotation]
	if !ok || v != nil {
		t.Errorf("expected NodePoolAnnotation = nil (delete), got ok=%v v=%v", ok, v)
	}

	// Unmanaged key should NOT appear in patch
	if _, ok := patch["some.other/annotation"]; ok {
		t.Error("unmanaged annotation should not appear in patch")
	}

	// ClusterNameAnnotation unchanged → should not appear
	if _, ok := patch[vitistackv1alpha1.ClusterNameAnnotation]; ok {
		t.Error("unchanged ClusterNameAnnotation should not be in patch")
	}
}

func TestComputeAnnotationPatch_NoPatchWhenInSync(t *testing.T) {
	t.Parallel()

	annotations := map[string]string{
		vitistackv1alpha1.ClusterNameAnnotation: "c",
		vitistackv1alpha1.MachineIdAnnotation:   "n",
	}

	patch := computeAnnotationPatch(annotations, annotations, nil)

	if len(patch) != 0 {
		t.Errorf("expected empty patch when in sync, got %d entries", len(patch))
	}
}

func TestComputeAnnotationPatch_AddsNewAnnotations(t *testing.T) {
	t.Parallel()

	current := map[string]string{}
	desired := map[string]string{
		vitistackv1alpha1.ClusterNameAnnotation: "new-cluster",
		vitistackv1alpha1.RegionAnnotation:      "europe",
	}

	patch := computeAnnotationPatch(current, desired, nil)

	if v, ok := patch[vitistackv1alpha1.ClusterNameAnnotation]; !ok || v != "new-cluster" {
		t.Errorf("expected ClusterNameAnnotation = 'new-cluster', got %v", v)
	}
	if v, ok := patch[vitistackv1alpha1.RegionAnnotation]; !ok || v != "europe" {
		t.Errorf("expected RegionAnnotation = 'europe', got %v", v)
	}
}

func TestComputeAnnotationPatch_SkipsTalosOwnedKeys(t *testing.T) {
	t.Parallel()

	current := map[string]string{
		vitistackv1alpha1.EnvironmentAnnotation:  "prod",     // Talos owns; do not touch
		vitistackv1alpha1.ClusterIdAnnotation:    "old-id",   // Talos owns; do not touch
		vitistackv1alpha1.MachineClassAnnotation: "medium",   // not Talos-owned
		vitistackv1alpha1.NodePoolAnnotation:     "leftover", // not Talos-owned, not desired → remove
	}

	desired := map[string]string{
		vitistackv1alpha1.EnvironmentAnnotation:  "production", // diverges, but Talos-owned
		vitistackv1alpha1.ClusterIdAnnotation:    "new-id",     // diverges, but Talos-owned
		vitistackv1alpha1.MachineClassAnnotation: "large",      // diverges → patch
	}

	talosOwned := map[string]bool{
		vitistackv1alpha1.EnvironmentAnnotation: true,
		vitistackv1alpha1.ClusterIdAnnotation:   true,
	}

	patch := computeAnnotationPatch(current, desired, talosOwned)

	if _, ok := patch[vitistackv1alpha1.EnvironmentAnnotation]; ok {
		t.Error("Talos-owned EnvironmentAnnotation must not appear in patch")
	}
	if _, ok := patch[vitistackv1alpha1.ClusterIdAnnotation]; ok {
		t.Error("Talos-owned ClusterIdAnnotation must not appear in patch")
	}
	if v, ok := patch[vitistackv1alpha1.MachineClassAnnotation]; !ok || v != "large" {
		t.Errorf("expected MachineClassAnnotation = 'large', got ok=%v v=%v", ok, v)
	}
	if v, ok := patch[vitistackv1alpha1.NodePoolAnnotation]; !ok || v != nil {
		t.Errorf("expected NodePoolAnnotation = nil (delete), got ok=%v v=%v", ok, v)
	}
}

func TestParseTalosOwnedAnnotations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{"empty", "", nil},
		{"malformed", "not-json", nil},
		{"single", `["vitistack.io/environment"]`, []string{"vitistack.io/environment"}},
		{"multiple", `["a","b","c"]`, []string{"a", "b", "c"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseTalosOwnedAnnotations(tc.raw)
			if len(got) != len(tc.want) {
				t.Fatalf("len mismatch: got %d, want %d", len(got), len(tc.want))
			}
			for _, k := range tc.want {
				if !got[k] {
					t.Errorf("missing key %q", k)
				}
			}
		})
	}
}

func TestCountPatchOps(t *testing.T) {
	t.Parallel()

	patch := map[string]interface{}{
		"a": "value1",
		"b": "value2",
		"c": nil,
		"d": nil,
		"e": nil,
	}

	updated, removed := countPatchOps(patch)
	if updated != 2 {
		t.Errorf("updated = %d, want 2", updated)
	}
	if removed != 3 {
		t.Errorf("removed = %d, want 3", removed)
	}
}

func TestCountPatchOps_Empty(t *testing.T) {
	t.Parallel()

	updated, removed := countPatchOps(map[string]interface{}{})
	if updated != 0 || removed != 0 {
		t.Errorf("expected 0/0 for empty patch, got %d/%d", updated, removed)
	}
}
