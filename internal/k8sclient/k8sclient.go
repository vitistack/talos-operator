package k8sclient

import (
	"github.com/NorskHelsenett/ror/pkg/rlog"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

var (
	Kubernetes      *kubernetes.Clientset
	DiscoveryClient *discovery.DiscoveryClient

	DynamicClient dynamic.Interface
)

func Init() {
	k8sconfig, err := config.GetConfig()
	if err != nil {
		rlog.Error("Failed to get Kubernetes config", err)
		if err != nil {
			panic(err)
		}
	}

	Kubernetes, err = kubernetes.NewForConfig(k8sconfig)
	if err != nil {
		rlog.Error("Failed to create Kubernetes client", err)
		if err != nil {
			panic(err)
		}
	}

	DiscoveryClient, err = discovery.NewDiscoveryClientForConfig(k8sconfig)
	if err != nil {
		rlog.Error("Failed to create discovery client", err)
		if err != nil {
			panic(err)
		}
	}

	DynamicClient, err = dynamic.NewForConfig(k8sconfig)
	if err != nil {
		rlog.Error("Failed to create dynamic client", err)
		if err != nil {
			panic(err)
		}
	}
}
