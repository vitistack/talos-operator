package talosconfigservice

import (
	"context"
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
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/networknamespaceservice"
	"github.com/vitistack/talos-operator/internal/services/vitistackservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	yaml "gopkg.in/yaml.v3"
)

type TalosConfigService struct{}

func NewTalosConfigService() *TalosConfigService {
	return &TalosConfigService{}
}

func (s *TalosConfigService) GenerateTalosConfig(
	cluster *vitistackv1alpha1.KubernetesCluster,
	machines []*vitistackv1alpha1.Machine,
	endpointIPs []string) (*clientconfig.Config, []byte, []byte, error) {
	// endpointIPs contains VIP load balancer IPs for the control plane
	clusterId := cluster.Spec.Cluster.ClusterId
	controlPlaneEndpoint := fmt.Sprintf("https://%s:6443", endpointIPs[0])
	kubernetesVersion := constants.DefaultKubernetesVersion
	versionContract := config.TalosVersionCurrent

	secretsBundle, err := secrets.NewBundle(secrets.NewFixedClock(time.Now()), versionContract)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to generate secrets bundle: %w", err)
	}

	input, err := generate.NewInput(clusterId, controlPlaneEndpoint, kubernetesVersion,
		generate.WithVersionContract(versionContract),
		generate.WithSecretsBundle(secretsBundle),
		generate.WithEndpointList(endpointIPs),
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
	cluster *vitistackv1alpha1.KubernetesCluster,
	roleYAML []byte,
	installDisk string,
	m *vitistackv1alpha1.Machine,
	tenantOverrides map[string]any) ([]byte, error) {
	patched, err := s.PatchInstallDiskYAML(roleYAML, installDisk)
	if err != nil {
		return nil, fmt.Errorf("failed to patch install disk: %w", err)
	}

	patched, err = s.applyTenantOverrides(patched, tenantOverrides)
	if err != nil {
		return nil, fmt.Errorf("failed to merge tenant overrides: %w", err)
	}

	patched, err = s.PatchHostname(patched, m.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to patch hostname: %w", err)
	}

	if m.Status.Provider != "" && s.IsVirtualMachineProvider(m.Status.Provider.String()) {
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

// MergeRoleTemplateWithOverrides returns the role template combined with tenant overrides only.
// When no overrides are provided, it returns nil to signal "no tenant template".
func (s *TalosConfigService) MergeRoleTemplateWithOverrides(roleYAML []byte, tenantOverrides map[string]any) ([]byte, error) {
	if len(roleYAML) == 0 || len(tenantOverrides) == 0 {
		return nil, nil
	}

	merged, err := s.applyTenantOverrides(roleYAML, tenantOverrides)
	if err != nil {
		return nil, fmt.Errorf("failed to merge tenant overrides into role template: %w", err)
	}
	return merged, nil
}

func (s *TalosConfigService) applyTenantOverrides(in []byte, overrides map[string]any) ([]byte, error) {
	if len(overrides) == 0 {
		return in, nil
	}

	var base map[string]any
	if err := yaml.Unmarshal(in, &base); err != nil {
		return nil, err
	}
	merged := mergeTenantOverrides(base, overrides)
	return yaml.Marshal(merged)
}

func mergeTenantOverrides(base map[string]any, overrides map[string]any) map[string]any {
	if base == nil {
		base = map[string]any{}
	}
	for key, overrideVal := range overrides {
		switch typed := overrideVal.(type) {
		case map[string]any:
			existing, _ := base[key].(map[string]any)
			base[key] = mergeTenantOverrides(existing, typed)
		case []any:
			existing, _ := base[key].([]any)
			base[key] = mergeTenantOverrideSlices(existing, typed)
		default:
			base[key] = deepCopyValue(overrideVal)
		}
	}
	return base
}

func mergeTenantOverrideSlices(base, overrides []any) []any {
	if len(overrides) == 0 {
		return base
	}
	if len(base) == 0 {
		copied, _ := deepCopyValue(overrides).([]any)
		return copied
	}
	if !sliceOfNamedMaps(base) || !sliceOfNamedMaps(overrides) {
		copied, _ := deepCopyValue(overrides).([]any)
		return copied
	}

	indexByName := make(map[string]int, len(base))
	for idx, item := range base {
		if entry, ok := item.(map[string]any); ok {
			if name, _ := entry["name"].(string); name != "" {
				indexByName[name] = idx
			}
		}
	}

	for _, overrideItem := range overrides {
		overrideMap, ok := overrideItem.(map[string]any)
		if !ok {
			continue
		}
		name, _ := overrideMap["name"].(string)
		if name == "" {
			continue
		}
		if idx, exists := indexByName[name]; exists {
			existingMap, _ := base[idx].(map[string]any)
			base[idx] = mergeTenantOverrides(existingMap, overrideMap)
			continue
		}
		base = append(base, deepCopyValue(overrideMap))
	}

	return base
}

func sliceOfNamedMaps(items []any) bool {
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			return false
		}
		if _, ok := entry["name"].(string); !ok {
			return false
		}
	}
	return true
}

func deepCopyValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		cpy := make(map[string]any, len(typed))
		for k, v := range typed {
			cpy[k] = deepCopyValue(v)
		}
		return cpy
	case []any:
		cpy := make([]any, len(typed))
		for idx, v := range typed {
			cpy[idx] = deepCopyValue(v)
		}
		return cpy
	default:
		return typed
	}
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

func (s *TalosConfigService) GetVMInstallImage(machineProvider vitistackv1alpha1.MachineProviderType) string {
	var configKey string
	switch machineProvider {
	case vitistackv1alpha1.MachineProviderTypeKubevirt:
		configKey = consts.TALOS_VM_INSTALL_IMAGE_KUBEVIRT
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
	extraKernelArgs := []any{}
	if !viper.GetBool(consts.TALOS_PREDICTABLE_NETWORK_NAMES) {
		// Disable predictable network interface names, using eth0.
		extraKernelArgs = append(extraKernelArgs, "net.ifnames=0")
	}
	inst["extraKernelArgs"] = extraKernelArgs

	return yaml.Marshal(cfg)
}

func (s *TalosConfigService) PatchNodeAnnotations(
	cluster *vitistackv1alpha1.KubernetesCluster,
	in []byte,
	m *vitistackv1alpha1.Machine) ([]byte, error) {
	cfg, nodeAnnotations, err := extractNodeAnnotations(in)
	if err != nil {
		return nil, err
	}
	country, az := extractDatacenterInfo(cluster.Spec.Cluster.Datacenter)

	nodeAnnotations[vitistackv1alpha1.ClusterNameAnnotation] = cluster.Name
	nodeAnnotations[vitistackv1alpha1.ClusterIdAnnotation] = cluster.Spec.Cluster.ClusterId
	nodeAnnotations[vitistackv1alpha1.CountryAnnotation] = country
	nodeAnnotations[vitistackv1alpha1.AzAnnotation] = az
	nodeAnnotations[vitistackv1alpha1.RegionAnnotation] = cluster.Spec.Cluster.Region
	nodeAnnotations[vitistackv1alpha1.VMProviderAnnotation] = string(m.Status.Provider)
	nodeAnnotations[vitistackv1alpha1.KubernetesProviderAnnotation] = string(cluster.Spec.Cluster.Provider)

	networkNamespaceName := ""
	infrastructure := ""
	networknamespace, err := networknamespaceservice.FetchFirstNetworkNamespaceByNamespace(context.TODO(), cluster.GetNamespace())
	if err != nil {
		vlog.Warn(fmt.Sprintf("failed to fetch NetworkNamespaces for namespace %q: %v", cluster.GetNamespace(), err))
	}

	if networknamespace != nil {
		// For now, just take the first NetworkNamespace in the list
		networkNamespaceName = networknamespace.Name
	}

	vitistack, err := vitistackservice.FetchVitistackByName(context.TODO(), viper.GetString(consts.VITISTACK_NAME))
	if err != nil {
		vlog.Warn(fmt.Sprintf("failed to fetch Vitistack %q: %v", viper.GetString(consts.VITISTACK_NAME), err))
	}

	if vitistack != nil && vitistack.Spec.Infrastructure != "" {
		infrastructure = vitistack.Spec.Infrastructure
	}

	nodeAnnotations[vitistackv1alpha1.ClusterWorkspaceAnnotation] = networkNamespaceName
	nodeAnnotations[vitistackv1alpha1.InfrastructureAnnotation] = infrastructure

	return yaml.Marshal(cfg)
}

func (s *TalosConfigService) PatchNodeConfigWithEndpointIPs(nodeConfig []byte, endpointIP string) ([]byte, error) {
	cfg, nodeAnnotations, err := extractNodeAnnotations(nodeConfig)
	if err != nil {
		return nil, err
	}

	nodeAnnotations[vitistackv1alpha1.K8sEndpointAnnotation] = fmt.Sprintf("https://%s:6443", endpointIP)

	return yaml.Marshal(cfg)
}

func extractNodeAnnotations(nodeConfig []byte) (map[string]any, map[string]any, error) {
	var cfg map[string]any
	if err := yaml.Unmarshal(nodeConfig, &cfg); err != nil {
		return nil, nil, err
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
	return cfg, nodeAnnotations, nil
}

func (s *TalosConfigService) SelectInstallDisk(m *vitistackv1alpha1.Machine) string {
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
