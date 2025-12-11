package consts

var (
	LOG_LEVEL                       = "LOG_LEVEL"
	LOG_JSON                        = "LOG_JSON"
	LOG_ADD_CALLER                  = "LOG_ADD_CALLER"
	LOG_DISABLE_STACKTRACE          = "LOG_DISABLE_STACKTRACE"
	LOG_UNESCAPED_MULTILINE         = "LOG_UNESCAPED_MULTILINE"
	LOG_COLORIZE_LINE               = "LOG_COLORIZE_LINE"
	SECRET_PREFIX                   = "SECRET_PREFIX"
	TENANT_CONFIGMAP_NAME           = "TENANT_CONFIGMAP_NAME"
	TENANT_CONFIGMAP_NAMESPACE      = "TENANT_CONFIGMAP_NAMESPACE"
	TENANT_CONFIGMAP_DATA_KEY       = "TENANT_CONFIGMAP_DATA_KEY"
	TALOS_VERSION                   = "TALOS_VERSION"
	DEFAULT_KUBERNETES_VERSION      = "DEFAULT_KUBERNETES_VERSION"
	TALOS_VM_INSTALL_IMAGE_KUBEVIRT = "TALOS_VM_INSTALL_IMAGE_KUBEVIRT"
	TALOS_VM_INSTALL_IMAGE_DEFAULT  = "TALOS_VM_INSTALL_IMAGE_DEFAULT"
	TALOS_PREDICTABLE_NETWORK_NAMES = "TALOS_PREDICTABLE_NETWORK_NAMES"
	VITISTACK_NAME                  = "VITISTACK_NAME"
	NAME_KUBERNETES_PROVIDER        = "NAME_KUBERNETES_PROVIDER"

	// ENDPOINT_MODE configures how the operator determines control plane endpoints.
	// Valid values: "none", "networkconfiguration", "talosvip", "custom"
	// Default: "networkconfiguration"
	ENDPOINT_MODE = "ENDPOINT_MODE"

	// CUSTOM_ENDPOINT is used when ENDPOINT_MODE is set to "custom".
	// Should be a comma-separated list of IP addresses or hostnames.
	CUSTOM_ENDPOINT = "CUSTOM_ENDPOINT"

	BOOT_IMAGE_SOURCE = "BOOT_IMAGE_SOURCE"

	BOOT_IMAGE = "BOOT_IMAGE"
)

// EndpointMode represents the mode for determining control plane endpoints
type EndpointMode string

const (
	// EndpointModeNone disables VIP/load balancer endpoint management.
	// Control plane IPs are used directly as endpoints.
	EndpointModeNone EndpointMode = "none"

	// EndpointModeNetworkConfiguration uses the NetworkNamespace's ControlPlaneVirtualSharedIP
	// to obtain load balancer IPs. This is the default mode.
	EndpointModeNetworkConfiguration EndpointMode = "networkconfiguration"

	// EndpointModeTalosVIP uses Talos' built-in VIP feature for control plane HA.
	// Requires additional Talos configuration.
	EndpointModeTalosVIP EndpointMode = "talosvip"

	// EndpointModeCustom uses user-provided endpoint addresses.
	// Requires CUSTOM_ENDPOINT to be set.
	EndpointModeCustom EndpointMode = "custom"

	// DefaultEndpointMode is the default endpoint mode
	DefaultEndpointMode = EndpointModeNetworkConfiguration
)

// IsValidEndpointMode checks if the provided mode is valid
func IsValidEndpointMode(mode string) bool {
	switch EndpointMode(mode) {
	case EndpointModeNone, EndpointModeNetworkConfiguration, EndpointModeTalosVIP, EndpointModeCustom:
		return true
	default:
		return false
	}
}

// ValidEndpointModes returns a list of valid endpoint modes
func ValidEndpointModes() []EndpointMode {
	return []EndpointMode{
		EndpointModeNone,
		EndpointModeNetworkConfiguration,
		EndpointModeTalosVIP,
		EndpointModeCustom,
	}
}

// BootImageSource represents the source for booting machines
type BootImageSource string

const (
	// BootImageSourcePXE uses PXE boot for machine provisioning.
	// This is the default mode.
	BootImageSourcePXE BootImageSource = "pxe"

	// BootImageSourceBootImage uses a boot image (ISO/disk image) for machine provisioning.
	// The imageID in the Machine spec will be used to create a DataVolume with CDI.
	// This avoids PXE boot entirely - kubevirt and Proxmox operators handle the imageID.
	BootImageSourceBootImage BootImageSource = "bootimage"

	// DefaultBootImageSource is the default boot image source
	DefaultBootImageSource = BootImageSourcePXE
)

// IsValidBootImageSource checks if the provided source is valid
func IsValidBootImageSource(source string) bool {
	switch BootImageSource(source) {
	case BootImageSourcePXE, BootImageSourceBootImage:
		return true
	default:
		return false
	}
}

// ValidBootImageSources returns a list of valid boot image sources
func ValidBootImageSources() []BootImageSource {
	return []BootImageSource{
		BootImageSourcePXE,
		BootImageSourceBootImage,
	}
}

// Machine annotation constants for signaling OS installation status
const (
	// OSInstalledAnnotation is set to "true" on a Machine CR after the OS (e.g., Talos)
	// has been successfully installed to disk. This signals to the machine provider
	// (e.g., kubevirt-provider) that it can safely eject the ISO/boot media.
	// This annotation is generic so any Kubernetes operator can use it.
	OSInstalledAnnotation = "vitistack.io/os-installed"
)
