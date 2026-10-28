package talosclientservice

import (
	"strings"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
)

// controlPlaneConfig is a control plane machine config whose kubelet already
// runs v1.36.5 while its control plane components are still on v1.36.3: the
// state a node was left in when only the kubelet image was checked.
const controlPlaneConfig = `version: v1alpha1
machine:
  type: controlplane
  token: abcdef.0123456789abcdef
  ca:
    crt: ""
    key: ""
  kubelet:
    image: ghcr.io/siderolabs/kubelet:v1.36.5
cluster:
  controlPlane:
    endpoint: https://10.0.0.1:6443
  apiServer:
    image: registry.k8s.io/kube-apiserver:v1.36.3
  controllerManager:
    image: registry.k8s.io/kube-controller-manager:v1.36.3
  scheduler:
    image: registry.k8s.io/kube-scheduler:v1.36.3
  proxy:
    image: registry.k8s.io/kube-proxy:v1.36.3
`

func TestKubernetesImagesConfig(t *testing.T) {
	s := NewTalosClientService()
	provider, err := configloader.NewFromBytes([]byte(controlPlaneConfig))
	if err != nil {
		t.Fatal(err)
	}

	patched, changed, err := s.kubernetesImagesConfig(provider, "v1.36.5", true)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("control plane with an updated kubelet but old component images reported as already current")
	}
	for _, image := range []string{
		"kube-apiserver:v1.36.5", "kube-controller-manager:v1.36.5", "kube-scheduler:v1.36.5", "kube-proxy:v1.36.5",
	} {
		if !strings.Contains(string(patched), image) {
			t.Errorf("patched config lacks %s", image)
		}
	}

	// Once applied, the same patch must find nothing to do; otherwise every
	// pass would apply the config again.
	applied, err := configloader.NewFromBytes(patched)
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := s.kubernetesImagesConfig(applied, "v1.36.5", true); err != nil || changed {
		t.Errorf("config with every target image reported changed=%t err=%v, want nothing to apply", changed, err)
	}

	// A worker only gets the kubelet image, which this config already has.
	if _, changed, err := s.kubernetesImagesConfig(provider, "v1.36.5", false); err != nil || changed {
		t.Errorf("worker with the target kubelet reported changed=%t err=%v, want nothing to apply", changed, err)
	}
}
