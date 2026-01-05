package talosconfigservice

import (
	"context"
	"fmt"
	"strings"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/bundle"
	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/services/networknamespaceservice"
	"github.com/vitistack/talos-operator/internal/services/talosversion"
	"github.com/vitistack/talos-operator/internal/services/vitistackservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	yaml "gopkg.in/yaml.v3"
)

type TalosConfigService struct{}

func NewTalosConfigService() *TalosConfigService {
	return &TalosConfigService{}
}

// TalosConfigBundle holds the generated Talos configuration bundle and secrets
type TalosConfigBundle struct {
	Bundle        *bundle.Bundle
	SecretsBundle *secrets.Bundle
	ClientConfig  *clientconfig.Config
}

// getKubernetesVersion extracts the Kubernetes version from the cluster spec.
// It uses spec.topology.version if set, otherwise falls back to the configured default.
// The version is normalized to ensure it doesn't have a "v" prefix (as required by bundle.InputOptions.KubeVersion).
func getKubernetesVersion(cluster *vitistackv1alpha1.KubernetesCluster) string {
	version := cluster.Spec.Topology.Version
	source := "cluster.spec.topology.version"

	if version == "" {
		// Use configured default (do NOT fall back to library constant as it may be incompatible)
		version = viper.GetString(consts.DEFAULT_KUBERNETES_VERSION)
		source = "config DEFAULT_KUBERNETES_VERSION"
		if version == "" {
			// Use the version adapter's default for the configured Talos version
			adapter := talosversion.GetCurrentTalosVersionAdapter()
			version = adapter.DefaultKubernetesVersion()
			source = fmt.Sprintf("adapter default for Talos %s", adapter.Version())
			vlog.Warn(fmt.Sprintf("No Kubernetes version configured, using adapter default: %s", version))
		}
	}

	normalizedVersion := strings.TrimPrefix(version, "v")
	vlog.Info(fmt.Sprintf("Using Kubernetes version %s (source: %s)", normalizedVersion, source))
	return normalizedVersion
}

// getTalosVersionContract returns the version contract based on the configured Talos version.
// This ensures generated configs are compatible with the target Talos version.
func getTalosVersionContract() *config.VersionContract {
	talosVersion := viper.GetString(consts.TALOS_VERSION)
	if talosVersion == "" {
		return config.TalosVersionCurrent
	}

	contract, err := config.ParseContractFromVersion(talosVersion)
	if err != nil {
		vlog.Warn(fmt.Sprintf("Failed to parse Talos version %q, using current: %v", talosVersion, err))
		return config.TalosVersionCurrent
	}
	return contract
}

// GenerateTalosConfigBundle generates a complete Talos config bundle using the same approach as talosctl.
// This method uses bundle.NewBundle with configpatcher for proper config generation and patching.
func (s *TalosConfigService) GenerateTalosConfigBundle(
	cluster *vitistackv1alpha1.KubernetesCluster,
	endpointIPs []string,
	tenantPatches []string,
	tenantPatchesControlPlane []string,
	tenantPatchesWorker []string,
) (*TalosConfigBundle, error) {
	clusterId := cluster.Spec.Cluster.ClusterId
	controlPlaneEndpoint := fmt.Sprintf("https://%s:6443", endpointIPs[0])
	kubernetesVersion := getKubernetesVersion(cluster)
	versionContract := getTalosVersionContract()

	// Generate secrets bundle - this is the core of cluster identity
	secretsBundle, err := secrets.NewBundle(secrets.NewFixedClock(time.Now()), versionContract)
	if err != nil {
		return nil, fmt.Errorf("failed to generate secrets bundle: %w", err)
	}

	// Build generate options
	genOptions := []generate.Option{
		generate.WithVersionContract(versionContract),
		generate.WithSecretsBundle(secretsBundle),
		generate.WithEndpointList(endpointIPs),
		generate.WithAdditionalSubjectAltNames(endpointIPs),
	}

	// Build bundle options
	bundleOpts := []bundle.Option{
		bundle.WithVerbose(false),
		bundle.WithInputOptions(
			&bundle.InputOptions{
				ClusterName: clusterId,
				Endpoint:    controlPlaneEndpoint,
				KubeVersion: kubernetesVersion,
				GenOptions:  genOptions,
			},
		),
	}

	// Add tenant patches using configpatcher - this is the proper way to merge configs
	if len(tenantPatches) > 0 {
		patches, err := configpatcher.LoadPatches(tenantPatches)
		if err != nil {
			return nil, fmt.Errorf("failed to load tenant patches: %w", err)
		}
		bundleOpts = append(bundleOpts, bundle.WithPatch(patches))
	}

	if len(tenantPatchesControlPlane) > 0 {
		patches, err := configpatcher.LoadPatches(tenantPatchesControlPlane)
		if err != nil {
			return nil, fmt.Errorf("failed to load control plane tenant patches: %w", err)
		}
		bundleOpts = append(bundleOpts, bundle.WithPatchControlPlane(patches))
	}

	if len(tenantPatchesWorker) > 0 {
		patches, err := configpatcher.LoadPatches(tenantPatchesWorker)
		if err != nil {
			return nil, fmt.Errorf("failed to load worker tenant patches: %w", err)
		}
		bundleOpts = append(bundleOpts, bundle.WithPatchWorker(patches))
	}

	// Generate the config bundle
	configBundle, err := bundle.NewBundle(bundleOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to generate config bundle: %w", err)
	}

	return &TalosConfigBundle{
		Bundle:        configBundle,
		SecretsBundle: secretsBundle,
		ClientConfig:  configBundle.TalosConfig(),
	}, nil
}

// GenerateTalosConfigBundleWithSecrets generates a config bundle using an existing secrets bundle.
// This is used when re-generating configs from persisted secrets.
func (s *TalosConfigService) GenerateTalosConfigBundleWithSecrets(
	cluster *vitistackv1alpha1.KubernetesCluster,
	endpointIPs []string,
	secretsBundle *secrets.Bundle,
	tenantPatches []string,
	tenantPatchesControlPlane []string,
	tenantPatchesWorker []string,
) (*TalosConfigBundle, error) {
	clusterId := cluster.Spec.Cluster.ClusterId
	controlPlaneEndpoint := fmt.Sprintf("https://%s:6443", endpointIPs[0])
	kubernetesVersion := getKubernetesVersion(cluster)
	versionContract := getTalosVersionContract()

	// Build generate options with existing secrets
	genOptions := []generate.Option{
		generate.WithVersionContract(versionContract),
		generate.WithSecretsBundle(secretsBundle),
		generate.WithEndpointList(endpointIPs),
		generate.WithAdditionalSubjectAltNames(endpointIPs),
	}

	// Build bundle options
	bundleOpts := []bundle.Option{
		bundle.WithVerbose(false),
		bundle.WithInputOptions(
			&bundle.InputOptions{
				ClusterName: clusterId,
				Endpoint:    controlPlaneEndpoint,
				KubeVersion: kubernetesVersion,
				GenOptions:  genOptions,
			},
		),
	}

	// Add tenant patches using configpatcher
	if len(tenantPatches) > 0 {
		patches, err := configpatcher.LoadPatches(tenantPatches)
		if err != nil {
			return nil, fmt.Errorf("failed to load tenant patches: %w", err)
		}
		bundleOpts = append(bundleOpts, bundle.WithPatch(patches))
	}

	if len(tenantPatchesControlPlane) > 0 {
		patches, err := configpatcher.LoadPatches(tenantPatchesControlPlane)
		if err != nil {
			return nil, fmt.Errorf("failed to load control plane tenant patches: %w", err)
		}
		bundleOpts = append(bundleOpts, bundle.WithPatchControlPlane(patches))
	}

	if len(tenantPatchesWorker) > 0 {
		patches, err := configpatcher.LoadPatches(tenantPatchesWorker)
		if err != nil {
			return nil, fmt.Errorf("failed to load worker tenant patches: %w", err)
		}
		bundleOpts = append(bundleOpts, bundle.WithPatchWorker(patches))
	}

	configBundle, err := bundle.NewBundle(bundleOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to generate config bundle: %w", err)
	}

	return &TalosConfigBundle{
		Bundle:        configBundle,
		SecretsBundle: secretsBundle,
		ClientConfig:  configBundle.TalosConfig(),
	}, nil
}

// SerializeControlPlaneConfig serializes the control plane config from the bundle
func (s *TalosConfigService) SerializeControlPlaneConfig(configBundle *bundle.Bundle) ([]byte, error) {
	return configBundle.Serialize(encoder.CommentsDisabled, machine.TypeControlPlane)
}

// SerializeWorkerConfig serializes the worker config from the bundle
func (s *TalosConfigService) SerializeWorkerConfig(configBundle *bundle.Bundle) ([]byte, error) {
	return configBundle.Serialize(encoder.CommentsDisabled, machine.TypeWorker)
}

// SerializeSecretsBundle serializes the secrets bundle to YAML for persistence
func (s *TalosConfigService) SerializeSecretsBundle(secretsBundle *secrets.Bundle) ([]byte, error) {
	return yaml.Marshal(secretsBundle)
}

// LoadSecretsBundle loads a secrets bundle from YAML bytes
func (s *TalosConfigService) LoadSecretsBundle(data []byte) (*secrets.Bundle, error) {
	secretsBundle := &secrets.Bundle{
		Clock: secrets.NewClock(),
	}
	if err := yaml.Unmarshal(data, secretsBundle); err != nil {
		return nil, fmt.Errorf("failed to unmarshal secrets bundle: %w", err)
	}
	return secretsBundle, nil
}

// GenerateTalosConfig is the legacy method for backward compatibility.
// It generates configs using the new bundle approach internally.
//
// Deprecated: Use GenerateTalosConfigBundle instead for new code.
func (s *TalosConfigService) GenerateTalosConfig(
	cluster *vitistackv1alpha1.KubernetesCluster,
	machines []*vitistackv1alpha1.Machine,
	endpointIPs []string) (*clientconfig.Config, []byte, []byte, error) {
	configBundle, err := s.GenerateTalosConfigBundle(cluster, endpointIPs, nil, nil, nil)
	if err != nil {
		return nil, nil, nil, err
	}

	controlPlaneYAML, err := s.SerializeControlPlaneConfig(configBundle.Bundle)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to serialize control plane config: %w", err)
	}

	workerYAML, err := s.SerializeWorkerConfig(configBundle.Bundle)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to serialize worker config: %w", err)
	}

	return configBundle.ClientConfig, controlPlaneYAML, workerYAML, nil
}

// GetSecretsBundle returns the secrets bundle from a config bundle
func (s *TalosConfigService) GetSecretsBundle(configBundle *TalosConfigBundle) *secrets.Bundle {
	return configBundle.SecretsBundle
}

func (s *TalosConfigService) PrepareNodeConfig(
	cluster *vitistackv1alpha1.KubernetesCluster,
	roleYAML []byte,
	installDisk string,
	m *vitistackv1alpha1.Machine,
	tenantOverrides map[string]any) ([]byte, error) {
	// Get the version adapter for current Talos version
	adapter := talosversion.GetCurrentTalosVersionAdapter()

	// Build patches for per-node customization
	var patches []string

	// Patch install disk using version-specific format
	installDiskPatch := adapter.BuildInstallDiskPatch(installDisk)
	patches = append(patches, installDiskPatch)

	// Patch hostname using version-specific format
	hostnamePatch := adapter.BuildHostnamePatch(m.Name)
	patches = append(patches, hostnamePatch)

	// Apply VM customizations if needed
	if m.Status.Provider != "" && s.IsVirtualMachineProvider(m.Status.Provider.String()) {
		installImage := s.GetVMInstallImage(m.Status.Provider)
		vmPatch := s.buildVMCustomizationPatch(installImage)
		if vmPatch != "" {
			patches = append(patches, vmPatch)
			if installImage != "" {
				vlog.Info(fmt.Sprintf("Detected virtual machine provider %s for node %s, applying VM customizations with install image: %s",
					m.Status.Provider, m.Name, installImage))
			} else {
				vlog.Info(fmt.Sprintf("Detected virtual machine provider %s for node %s, applying VM customizations (no custom install image configured)",
					m.Status.Provider, m.Name))
			}
		}
	}

	// Build node annotations patch
	annotationsPatch := s.buildNodeAnnotationsPatch(cluster, m)
	if annotationsPatch != "" {
		patches = append(patches, annotationsPatch)
	}

	// NOTE: tenantOverrides are intentionally NOT applied here.
	// They are already applied during bundle generation in GenerateTalosConfigBundle()
	// via bundle.WithPatch(). Applying them again here would cause duplicate list entries
	// (e.g., duplicate namespaces in admissionControl.exemptions).
	// The tenantOverrides parameter is kept for backward compatibility but is ignored.
	_ = tenantOverrides // Explicitly mark as intentionally unused

	// Load all patches using configpatcher
	loadedPatches, err := configpatcher.LoadPatches(patches)
	if err != nil {
		return nil, fmt.Errorf("failed to load node config patches: %w", err)
	}

	// Apply patches to the role config using configpatcher
	patched, err := configpatcher.Apply(configpatcher.WithBytes(roleYAML), loadedPatches)
	if err != nil {
		return nil, fmt.Errorf("failed to apply node config patches: %w", err)
	}

	return patched.Bytes()
}

// buildVMCustomizationPatch builds a YAML patch for VM-specific settings
func (s *TalosConfigService) buildVMCustomizationPatch(installImage string) string {
	extraKernelArgs := []string{}
	if !viper.GetBool(consts.TALOS_PREDICTABLE_NETWORK_NAMES) {
		extraKernelArgs = append(extraKernelArgs, "net.ifnames=0")
	}

	if installImage == "" && len(extraKernelArgs) == 0 {
		return ""
	}

	var patch strings.Builder
	patch.WriteString("machine:\n  install:\n")
	if installImage != "" {
		patch.WriteString(fmt.Sprintf("    image: %s\n", installImage))
	}
	if len(extraKernelArgs) > 0 {
		patch.WriteString("    extraKernelArgs:\n")
		for _, arg := range extraKernelArgs {
			patch.WriteString(fmt.Sprintf("      - %s\n", arg))
		}
	}
	return patch.String()
}

// buildNodeAnnotationsPatch builds a YAML patch for node annotations
func (s *TalosConfigService) buildNodeAnnotationsPatch(cluster *vitistackv1alpha1.KubernetesCluster, m *vitistackv1alpha1.Machine) string {
	country, az := extractDatacenterInfo(cluster.Spec.Cluster.Datacenter)

	annotations := map[string]string{
		vitistackv1alpha1.ClusterNameAnnotation:        cluster.Name,
		vitistackv1alpha1.ClusterIdAnnotation:          cluster.Spec.Cluster.ClusterId,
		vitistackv1alpha1.CountryAnnotation:            country,
		vitistackv1alpha1.AzAnnotation:                 az,
		vitistackv1alpha1.RegionAnnotation:             cluster.Spec.Cluster.Region,
		vitistackv1alpha1.VMProviderAnnotation:         string(m.Status.Provider),
		vitistackv1alpha1.KubernetesProviderAnnotation: string(cluster.Spec.Cluster.Provider),
	}

	networkNamespaceName := ""
	infrastructure := ""

	networknamespace, err := networknamespaceservice.FetchFirstNetworkNamespaceByNamespace(context.TODO(), cluster.GetNamespace())
	if err != nil {
		vlog.Warn(fmt.Sprintf("failed to fetch NetworkNamespaces for namespace %q: %v", cluster.GetNamespace(), err))
	}
	if networknamespace != nil {
		networkNamespaceName = networknamespace.Name
	}

	vitistack, err := vitistackservice.FetchVitistackByName(context.TODO(), viper.GetString(consts.VITISTACK_NAME))
	if err != nil {
		vlog.Warn(fmt.Sprintf("failed to fetch Vitistack %q: %v", viper.GetString(consts.VITISTACK_NAME), err))
	}
	if vitistack != nil && vitistack.Spec.Infrastructure != "" {
		infrastructure = vitistack.Spec.Infrastructure
	}

	annotations[vitistackv1alpha1.ClusterWorkspaceAnnotation] = networkNamespaceName
	annotations[vitistackv1alpha1.InfrastructureAnnotation] = infrastructure

	// Add nodepool annotation if present on the machine
	if nodePool, ok := m.Annotations[vitistackv1alpha1.NodePoolAnnotation]; ok && nodePool != "" {
		annotations[vitistackv1alpha1.NodePoolAnnotation] = nodePool
	}

	// Build the patch YAML
	var patch strings.Builder
	patch.WriteString("machine:\n  nodeAnnotations:\n")
	for key, value := range annotations {
		patch.WriteString(fmt.Sprintf("    %s: %q\n", key, value))
	}
	return patch.String()
}

// MergeRoleTemplateWithOverrides applies tenant overrides to a role template using configpatcher.
// When no overrides are provided, it returns nil to signal "no tenant template".
func (s *TalosConfigService) MergeRoleTemplateWithOverrides(roleYAML []byte, tenantOverrides map[string]any) ([]byte, error) {
	if len(roleYAML) == 0 || len(tenantOverrides) == 0 {
		return nil, nil
	}

	return s.applyTenantOverrides(roleYAML, tenantOverrides)
}

// applyTenantOverrides applies tenant overrides to a config using configpatcher.
// This is the internal implementation used by MergeRoleTemplateWithOverrides and PrepareNodeConfig.
func (s *TalosConfigService) applyTenantOverrides(baseYAML []byte, tenantOverrides map[string]any) ([]byte, error) {
	if len(tenantOverrides) == 0 {
		return baseYAML, nil
	}

	// Marshal tenant overrides to YAML for patching
	overridesYAML, err := yaml.Marshal(tenantOverrides)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal tenant overrides: %w", err)
	}

	// Use configpatcher to apply the overrides
	patches, err := configpatcher.LoadPatches([]string{string(overridesYAML)})
	if err != nil {
		return nil, fmt.Errorf("failed to load tenant override patches: %w", err)
	}

	patched, err := configpatcher.Apply(configpatcher.WithBytes(baseYAML), patches)
	if err != nil {
		return nil, fmt.Errorf("failed to apply tenant overrides: %w", err)
	}

	return patched.Bytes()
}

// PatchInstallDiskYAML patches the install disk in a Talos config using configpatcher.
//
// Deprecated: Use PrepareNodeConfig which handles this internally.
func (s *TalosConfigService) PatchInstallDiskYAML(in []byte, disk string) ([]byte, error) {
	patch := fmt.Sprintf(`machine:
  install:
    disk: %s`, disk)

	patches, err := configpatcher.LoadPatches([]string{patch})
	if err != nil {
		return nil, err
	}

	patched, err := configpatcher.Apply(configpatcher.WithBytes(in), patches)
	if err != nil {
		return nil, err
	}
	return patched.Bytes()
}

// PatchHostname patches the hostname in a Talos config using configpatcher.
//
// Deprecated: Use PrepareNodeConfig which handles this internally.
func (s *TalosConfigService) PatchHostname(in []byte, hostname string) ([]byte, error) {
	// Use version adapter for hostname patch
	adapter := talosversion.GetCurrentTalosVersionAdapter()
	patch := adapter.BuildHostnamePatch(hostname)

	patches, err := configpatcher.LoadPatches([]string{patch})
	if err != nil {
		return nil, err
	}

	patched, err := configpatcher.Apply(configpatcher.WithBytes(in), patches)
	if err != nil {
		return nil, err
	}
	return patched.Bytes()
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

// PatchVirtualMachineCustomization patches VM-specific settings using configpatcher.
//
// Deprecated: Use PrepareNodeConfig which handles this internally.
func (s *TalosConfigService) PatchVirtualMachineCustomization(in []byte, installImage string) ([]byte, error) {
	vmPatch := s.buildVMCustomizationPatch(installImage)
	if vmPatch == "" {
		return in, nil
	}

	patches, err := configpatcher.LoadPatches([]string{vmPatch})
	if err != nil {
		return nil, err
	}

	patched, err := configpatcher.Apply(configpatcher.WithBytes(in), patches)
	if err != nil {
		return nil, err
	}
	return patched.Bytes()
}

// PatchNodeAnnotations patches node annotations using configpatcher.
//
// Deprecated: Use PrepareNodeConfig which handles this internally.
func (s *TalosConfigService) PatchNodeAnnotations(
	cluster *vitistackv1alpha1.KubernetesCluster,
	in []byte,
	m *vitistackv1alpha1.Machine) ([]byte, error) {
	annotationsPatch := s.buildNodeAnnotationsPatch(cluster, m)
	if annotationsPatch == "" {
		return in, nil
	}

	patches, err := configpatcher.LoadPatches([]string{annotationsPatch})
	if err != nil {
		return nil, err
	}

	patched, err := configpatcher.Apply(configpatcher.WithBytes(in), patches)
	if err != nil {
		return nil, err
	}
	return patched.Bytes()
}

// PatchNodeConfigWithEndpointIPs patches the node config to add endpoint IP annotation.
func (s *TalosConfigService) PatchNodeConfigWithEndpointIPs(nodeConfig []byte, endpointIP string) ([]byte, error) {
	patch := fmt.Sprintf(`machine:
  nodeAnnotations:
    %s: "https://%s:6443"`, vitistackv1alpha1.K8sEndpointAnnotation, endpointIP)

	patches, err := configpatcher.LoadPatches([]string{patch})
	if err != nil {
		return nil, err
	}

	patched, err := configpatcher.Apply(configpatcher.WithBytes(nodeConfig), patches)
	if err != nil {
		return nil, err
	}
	return patched.Bytes()
}

func (s *TalosConfigService) SelectInstallDisk(m *vitistackv1alpha1.Machine) string {
	// First, look for disks with "disk-" prefix (e.g., disk-0, disk-1)
	// These are the actual storage disks, not boot media like cdrom-iso

	for i := range m.Status.Disks {
		d := m.Status.Disks[i]
		if d.Device != "" && strings.HasPrefix(d.Name, "disk-") {
			return d.Device
		}
	}
	// Fallback: return first non-cdrom disk with a device path
	for i := range m.Status.Disks {
		d := m.Status.Disks[i]
		if d.Device != "" && !strings.HasPrefix(d.Name, "cdrom") {
			return d.Device
		}
	}
	// Default to /dev/vda (virtio bus used for root disk)
	// Note: Device path should normally come from Machine.Status.Disks
	// which is populated by kubevirt-operator from VMI.Status.VolumeStatus
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
