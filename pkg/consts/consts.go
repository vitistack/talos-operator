package consts

var (
	DEVELOPMENT                     = "DEVELOPMENT"
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

// Upgrade annotation constants for KubernetesCluster resources.
// These annotations enable a provider-agnostic upgrade flow where:
// 1. Operator detects available upgrades and sets *-available annotations
// 2. User triggers upgrades by setting *-target annotations
// 3. Operator performs upgrades and updates *-current/*-status annotations
const (
	// AnnotationPrefix is the base prefix for all upgrade annotations
	UpgradeAnnotationPrefix = "upgrade.vitistack.io/"

	// === TALOS UPGRADE ANNOTATIONS ===

	// TalosAvailableAnnotation indicates a new Talos version is available for upgrade.
	// Set by operator when a newer Talos version is detected.
	// Value: version string (e.g., "1.9.0")
	TalosAvailableAnnotation = UpgradeAnnotationPrefix + "talos-available"

	// TalosCurrentAnnotation indicates the current running Talos version.
	// Set and maintained by operator.
	// Value: version string (e.g., "1.8.0")
	TalosCurrentAnnotation = UpgradeAnnotationPrefix + "talos-current"

	// TalosTargetAnnotation triggers a Talos upgrade when set by user.
	// Set by user to request an upgrade to the specified version.
	// Removed by operator after successful upgrade.
	// Value: version string (e.g., "1.9.0")
	TalosTargetAnnotation = UpgradeAnnotationPrefix + "talos-target"

	// TalosStatusAnnotation indicates the current Talos upgrade status.
	// Set by operator during upgrade process.
	// Value: one of UpgradeStatus* constants
	TalosStatusAnnotation = UpgradeAnnotationPrefix + "talos-status"

	// TalosMessageAnnotation provides human-readable upgrade progress/status.
	// Set by operator during upgrade process.
	// Value: status message (e.g., "Upgrading node cp-0 (1/5)")
	TalosMessageAnnotation = UpgradeAnnotationPrefix + "talos-message"

	// TalosProgressAnnotation tracks upgrade progress.
	// Set by operator during rolling upgrades.
	// Value: "nodesUpgraded/totalNodes" (e.g., "2/5")
	TalosProgressAnnotation = UpgradeAnnotationPrefix + "talos-progress"

	// === KUBERNETES UPGRADE ANNOTATIONS ===

	// KubernetesAvailableAnnotation indicates a new Kubernetes version is available.
	// Only set by operator when Talos version supports the new K8s version.
	// Value: version string (e.g., "1.32.0")
	KubernetesAvailableAnnotation = UpgradeAnnotationPrefix + "kubernetes-available"

	// KubernetesCurrentAnnotation indicates the current running Kubernetes version.
	// Set and maintained by operator.
	// Value: version string (e.g., "1.31.0")
	KubernetesCurrentAnnotation = UpgradeAnnotationPrefix + "kubernetes-current"

	// KubernetesTargetAnnotation triggers a Kubernetes upgrade when set by user.
	// Set by user to request an upgrade to the specified version.
	// Removed by operator after successful upgrade.
	// Blocked if Talos version is incompatible.
	// Value: version string (e.g., "1.32.0")
	KubernetesTargetAnnotation = UpgradeAnnotationPrefix + "kubernetes-target"

	// KubernetesStatusAnnotation indicates the current Kubernetes upgrade status.
	// Set by operator during upgrade process.
	// Value: one of UpgradeStatus* constants
	KubernetesStatusAnnotation = UpgradeAnnotationPrefix + "kubernetes-status"

	// KubernetesMessageAnnotation provides human-readable upgrade progress/status.
	// Set by operator during upgrade process.
	// Value: status message (e.g., "Upgrading Kubernetes components")
	KubernetesMessageAnnotation = UpgradeAnnotationPrefix + "kubernetes-message"

	// KubernetesProgressAnnotation tracks Kubernetes upgrade progress.
	// Set by operator during rolling upgrades.
	// Value: "nodesUpgraded/totalNodes" (e.g., "2/5")
	KubernetesProgressAnnotation = UpgradeAnnotationPrefix + "kubernetes-progress"

	// === UPGRADE CONTROL ANNOTATIONS ===

	// ResumeUpgradeAnnotation triggers resume of a failed/interrupted upgrade.
	// Set by user to "true" to resume an upgrade that was interrupted or partially failed.
	// Removed by operator after processing.
	// Value: "true" to resume
	ResumeUpgradeAnnotation = UpgradeAnnotationPrefix + "resume"

	// SkipFailedNodesAnnotation instructs operator to skip failed nodes and continue.
	// Set by user to "true" to skip nodes that failed upgrade and continue with remaining nodes.
	// Removed by operator after processing.
	// Value: "true" to skip failed nodes
	SkipFailedNodesAnnotation = UpgradeAnnotationPrefix + "skip-failed-nodes"

	// RetryFailedNodesAnnotation instructs operator to retry upgrading failed nodes.
	// Set by user to "true" to retry nodes that previously failed.
	// Removed by operator after processing.
	// Value: "true" to retry failed nodes
	RetryFailedNodesAnnotation = UpgradeAnnotationPrefix + "retry-failed-nodes"

	// FailedNodesAnnotation lists the nodes that failed during upgrade.
	// Set by operator when nodes fail.
	// Value: comma-separated list of node names (e.g., "worker-1,worker-2")
	FailedNodesAnnotation = UpgradeAnnotationPrefix + "failed-nodes"
)

// UpgradeStatus constants for *-status annotations
type UpgradeStatus string

const (
	// UpgradeStatusIdle indicates no upgrade is in progress
	UpgradeStatusIdle UpgradeStatus = "idle"

	// UpgradeStatusPending indicates an upgrade is queued but not started
	UpgradeStatusPending UpgradeStatus = "pending"

	// UpgradeStatusInProgress indicates an upgrade is currently running
	UpgradeStatusInProgress UpgradeStatus = "in-progress"

	// UpgradeStatusCompleted indicates an upgrade finished successfully
	UpgradeStatusCompleted UpgradeStatus = "completed"

	// UpgradeStatusFailed indicates an upgrade failed
	UpgradeStatusFailed UpgradeStatus = "failed"

	// UpgradeStatusBlocked indicates an upgrade is blocked (e.g., K8s blocked by Talos version)
	UpgradeStatusBlocked UpgradeStatus = "blocked"

	// UpgradeStatusRollingBack indicates a rollback is in progress
	UpgradeStatusRollingBack UpgradeStatus = "rolling-back"
)
