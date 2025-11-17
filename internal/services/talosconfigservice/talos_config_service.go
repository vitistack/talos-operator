package talosconfigservice

import (
	"fmt"
	"strings"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/siderolabs/talos/pkg/machinery/constants"
	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/pkg/consts"
	yaml "gopkg.in/yaml.v3"
)

type TalosConfigService struct{}

func NewTalosConfigService() *TalosConfigService {
	return &TalosConfigService{}
}

func (s *TalosConfigService) GenerateTalosConfig(
	cluster *vitistackcrdsv1alpha1.KubernetesCluster,
	machines []*vitistackcrdsv1alpha1.Machine,
	endpointIP string) (*clientconfig.Config, []byte, []byte, error) {
	controlPlanes := filterMachinesByRole(machines, "control-plane")
	clusterId := cluster.Spec.Cluster.ClusterId
	controlPlaneEndpoint := fmt.Sprintf("https://%s:6443", endpointIP)
	kubernetesVersion := constants.DefaultKubernetesVersion
	versionContract := config.TalosVersionCurrent

	secretsBundle, err := secrets.NewBundle(secrets.NewFixedClock(time.Now()), versionContract)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to generate secrets bundle: %w", err)
	}

	endpointlist := []string{}
	for _, cp := range controlPlanes {
		if len(cp.Status.NetworkInterfaces) > 0 && len(cp.Status.NetworkInterfaces[0].IPAddresses) > 0 {
			endpointlist = append(endpointlist, cp.Status.NetworkInterfaces[0].IPAddresses[0])
		}
	}

	input, err := generate.NewInput(clusterId, controlPlaneEndpoint, kubernetesVersion,
		generate.WithVersionContract(versionContract),
		generate.WithSecretsBundle(secretsBundle),
		generate.WithEndpointList(endpointlist),
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to generate input: %w", err)
	}

	encoderOpts := []encoder.Option{encoder.WithComments(encoder.CommentsDisabled)}
	var controlPlaneYAML, workerYAML []byte

	if cpProv, err := input.Config(machine.TypeControlPlane); err == nil {
		if b, err := cpProv.EncodeBytes(encoderOpts...); err == nil {
			controlPlaneYAML = b
		} else {
			return nil, nil, nil, fmt.Errorf("failed to render control plane template: %w", err)
		}
	} else {
		return nil, nil, nil, fmt.Errorf("failed to get control plane config provider: %w", err)
	}

	if wProv, err := input.Config(machine.TypeWorker); err == nil {
		if b, err := wProv.EncodeBytes(encoderOpts...); err == nil {
			workerYAML = b
		} else {
			return nil, nil, nil, fmt.Errorf("failed to render worker template: %w", err)
		}
	} else {
		return nil, nil, nil, fmt.Errorf("failed to get worker config provider: %w", err)
	}

	clientCfg, err := input.Talosconfig()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to generate client config: %w", err)
	}

	return clientCfg, controlPlaneYAML, workerYAML, nil
}

func (s *TalosConfigService) PrepareNodeConfig(
	cluster *vitistackcrdsv1alpha1.KubernetesCluster,
	roleYAML []byte,
	installDisk string,
	m *vitistackcrdsv1alpha1.Machine) ([]byte, error) {
	patched, err := s.PatchInstallDiskYAML(roleYAML, installDisk)
	if err != nil {
		return nil, fmt.Errorf("failed to patch install disk: %w", err)
	}

	patched, err = s.PatchHostname(patched, m.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to patch hostname: %w", err)
	}

	if m.Status.Provider != "" && s.IsVirtualMachineProvider(m.Status.Provider) {
		installImage := s.GetVMInstallImage(m.Status.Provider)
		if installImage != "" {
			vlog.Info(fmt.Sprintf("Detected virtual machine provider %s for node %s, applying VM customizations with install image: %s",
				m.Status.Provider, m.Name, installImage))
		} else {
			vlog.Info(fmt.Sprintf("Detected virtual machine provider %s for node %s, applying VM customizations (no custom install image configured)",
				m.Status.Provider, m.Name))
		}
		patched, err = s.PatchVirtualMachineCustomization(patched, installImage)
		if err != nil {
			return nil, fmt.Errorf("failed to patch VM customization: %w", err)
		}
	}

	patched, err = s.PatchNodeAnnotations(cluster, patched, m)
	if err != nil {
		return nil, fmt.Errorf("failed to patch node annotations: %w", err)
	}

	return patched, nil
}

func (s *TalosConfigService) PatchInstallDiskYAML(in []byte, disk string) ([]byte, error) {
	var cfg map[string]any
	if err := yaml.Unmarshal(in, &cfg); err != nil {
		return nil, err
	}
	m, _ := cfg["machine"].(map[string]any)
	if m == nil {
		m = map[string]any{}
		cfg["machine"] = m
	}
	inst, _ := m["install"].(map[string]any)
	if inst == nil {
		inst = map[string]any{}
		m["install"] = inst
	}
	inst["disk"] = disk
	return yaml.Marshal(cfg)
}

func (s *TalosConfigService) PatchHostname(in []byte, hostname string) ([]byte, error) {
	var cfg map[string]any
	if err := yaml.Unmarshal(in, &cfg); err != nil {
		return nil, err
	}
	m, _ := cfg["machine"].(map[string]any)
	if m == nil {
		m = map[string]any{}
		cfg["machine"] = m
	}
	network, _ := m["network"].(map[string]any)
	if network == nil {
		network = map[string]any{}
		m["network"] = network
	}
	network["hostname"] = hostname
	return yaml.Marshal(cfg)
}

func (s *TalosConfigService) IsVirtualMachineProvider(provider string) bool {
	provider = strings.ToLower(provider)
	virtualProviders := []string{"proxmox", "kubevirt", "libvirt", "kvm", "qemu", "vmware", "virtualbox", "hyper-v"}
	for _, vp := range virtualProviders {
		if strings.Contains(provider, vp) {
			return true
		}
	}
	return false
}

func (s *TalosConfigService) GetVMInstallImage(provider string) string {
	provider = strings.ToLower(provider)
	var configKey string
	switch {
	case strings.Contains(provider, "proxmox"):
		configKey = consts.TALOS_VM_INSTALL_IMAGE_PROXMOX
	case strings.Contains(provider, "kubevirt"):
		configKey = consts.TALOS_VM_INSTALL_IMAGE_KUBEVIRT
	case strings.Contains(provider, "libvirt"):
		configKey = consts.TALOS_VM_INSTALL_IMAGE_LIBVIRT
	case strings.Contains(provider, "kvm"):
		configKey = consts.TALOS_VM_INSTALL_IMAGE_KVM
	case strings.Contains(provider, "vmware"):
		configKey = consts.TALOS_VM_INSTALL_IMAGE_VMWARE
	case strings.Contains(provider, "virtualbox"):
		configKey = consts.TALOS_VM_INSTALL_IMAGE_VIRTUALBOX
	case strings.Contains(provider, "hyper-v"):
		configKey = consts.TALOS_VM_INSTALL_IMAGE_HYPERV
	default:
		configKey = consts.TALOS_VM_INSTALL_IMAGE_DEFAULT
	}
	image := viper.GetString(configKey)
	if image == "" {
		image = viper.GetString(consts.TALOS_VM_INSTALL_IMAGE_DEFAULT)
	}
	return image
}

func (s *TalosConfigService) PatchVirtualMachineCustomization(in []byte, installImage string) ([]byte, error) {
	var cfg map[string]any
	if err := yaml.Unmarshal(in, &cfg); err != nil {
		return nil, err
	}
	m, _ := cfg["machine"].(map[string]any)
	if m == nil {
		m = map[string]any{}
		cfg["machine"] = m
	}
	inst, _ := m["install"].(map[string]any)
	if inst == nil {
		inst = map[string]any{}
		m["install"] = inst
	}
	if installImage != "" {
		inst["image"] = installImage
	}
	inst["extraKernelArgs"] = []any{"console=ttyS0,115200"}
	return yaml.Marshal(cfg)
}

func (s *TalosConfigService) PatchNodeAnnotations(
	cluster *vitistackcrdsv1alpha1.KubernetesCluster,
	in []byte,
	m *vitistackcrdsv1alpha1.Machine) ([]byte, error) {
	var cfg map[string]any
	if err := yaml.Unmarshal(in, &cfg); err != nil {
		return nil, err
	}
	machineSection, _ := cfg["machine"].(map[string]any)
	if machineSection == nil {
		machineSection = map[string]any{}
		cfg["machine"] = machineSection
	}
	nodeAnnotations, _ := machineSection["nodeAnnotations"].(map[string]any)
	if nodeAnnotations == nil {
		nodeAnnotations = map[string]any{}
		machineSection["nodeAnnotations"] = nodeAnnotations
	}
	country, az := extractDatacenterInfo(cluster.Spec.Cluster.Datacenter)
	nodeAnnotations["vitistack.io/workspace"] = "TODO-workspace-not-yet-available"
	nodeAnnotations["vitistack.io/clustername"] = cluster.Name
	nodeAnnotations["vitistack.io/clusterid"] = cluster.Spec.Cluster.ClusterId
	nodeAnnotations["vitistack.io/country"] = country
	nodeAnnotations["vitistack.io/az"] = az
	nodeAnnotations["vitistack.io/region"] = cluster.Spec.Cluster.Region
	nodeAnnotations["vitistack.io/vmprovider"] = m.Status.Provider
	nodeAnnotations["vitistack.io/kubernetesprovider"] = cluster.Spec.Cluster.Provider
	nodeAnnotations["vitistack.io/kubernetes-endpoint-addr"] = "TODO-endpoint-not-yet-available"
	return yaml.Marshal(cfg)
}

func (s *TalosConfigService) SelectInstallDisk(m *vitistackcrdsv1alpha1.Machine) string {
	for i := range m.Status.Disks {
		d := m.Status.Disks[i]
		if d.Device != "" {
			if d.PVCName != "" {
				return d.Device
			}
			if d.Device != "" {
				return d.Device
			}
		}
	}
	return "/dev/vda"
}

func (s *TalosConfigService) MarshalTalosClientConfig(clientCfg *clientconfig.Config) ([]byte, error) {
	if clientCfg == nil {
		return nil, nil
	}
	b, err := yaml.Marshal(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal talos client config: %w", err)
	}
	return b, nil
}

func filterMachinesByRole(machines []*vitistackcrdsv1alpha1.Machine, role string) []*vitistackcrdsv1alpha1.Machine {
	var filtered []*vitistackcrdsv1alpha1.Machine
	for _, machine := range machines {
		if machine.Labels["cluster.vitistack.io/role"] == role {
			filtered = append(filtered, machine)
		}
	}
	return filtered
}

func extractDatacenterInfo(datacenter string) (country string, az string) {
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
