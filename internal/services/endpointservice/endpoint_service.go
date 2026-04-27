package endpointservice

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	"github.com/vitistack/common/pkg/operator/conditions"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
	"github.com/vitistack/talos-operator/pkg/consts"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Condition types surfaced onto the KubernetesCluster to reflect the Ready
// state of the dependent CRDs this controller consumes. The talos-operator
// only consumes the CRDs themselves; whichever controller provisions them
// (and whatever backend it talks to) is a deployment detail.
const (
	kcCondNetworkNamespaceReady            = "NetworkNamespaceReady"
	kcCondControlPlaneVirtualSharedIPReady = "ControlPlaneVirtualSharedIPReady"
)

const (
	statusFailed = "Failed"
	// dependentCRRequeueDelay is how long to wait before re-checking whether a
	// dependent CR (NetworkNamespace, ControlPlaneVirtualSharedIP) has reached
	// its Ready condition. Reconciliation by the upstream controller typically
	// takes a few seconds; this delay backs off enough to avoid hot-looping.
	dependentCRRequeueDelay = 15 * time.Second
)

// isResourceReady returns true when conds contains a Ready condition with
// Status=True. It also returns an error when Ready is explicitly False with
// reason "Error" (terminal — user must fix the spec), so callers can stop
// reconciling instead of looping forever. The "Error" reason is a convention
// shared by the dependent CRDs we consume.
func isResourceReady(conds []metav1.Condition) (ready bool, terminal error) {
	cond, ok := conditions.Get(conds, "Ready")
	if !ok {
		return false, nil
	}
	if cond.Status == metav1.ConditionTrue {
		return true, nil
	}
	if cond.Status == metav1.ConditionFalse && cond.Reason == "Error" {
		return false, fmt.Errorf("%s", cond.Message)
	}
	return false, nil
}

// deprecationWarned tracks namespaces for which the deprecation warning has already been logged,
// so we don't spam the logs on every reconcile loop.
var deprecationWarned sync.Map

// EndpointService handles VIP and endpoint management for Talos clusters
type EndpointService struct {
	client.Client
	StatusManager *status.StatusManager
}

// NewEndpointService creates a new EndpointService. StatusManager is optional;
// when provided, the service mirrors the Ready condition of dependent CRs
// onto the KubernetesCluster so operators can see why a reconcile is waiting
// via kubectl describe.
func NewEndpointService(c client.Client, sm *status.StatusManager) *EndpointService {
	return &EndpointService{Client: c, StatusManager: sm}
}

// mirrorReadyToKC copies the "Ready" condition of a dependent CR onto the
// KubernetesCluster as kcCondType. Called whenever the endpoint service
// inspects a dependent CR's readiness, so KC status tracks upstream state
// without the user having to describe the dependent resource directly.
//
// Safe to call with a nil StatusManager (no-op) so the service still works
// in tests or contexts without one wired in.
func (s *EndpointService) mirrorReadyToKC(ctx context.Context, kc *vitistackv1alpha1.KubernetesCluster, upstreamConds []metav1.Condition, kcCondType, displayName string) {
	if s.StatusManager == nil || kc == nil {
		return
	}
	cond, ok := conditions.Get(upstreamConds, "Ready")
	if !ok {
		_ = s.StatusManager.SetCondition(ctx, kc, kcCondType, "Unknown", "NoCondition", fmt.Sprintf("%s has not published a Ready condition yet", displayName))
		return
	}
	msg := cond.Message
	if msg == "" {
		msg = fmt.Sprintf("%s Ready=%s", displayName, cond.Status)
	}
	reason := cond.Reason
	if reason == "" {
		reason = "Unknown"
	}
	_ = s.StatusManager.SetCondition(ctx, kc, kcCondType, string(cond.Status), reason, msg)
}

// EnsureNetworkNamespaceReady verifies that the NetworkNamespace referenced by the
// cluster is Ready before the caller proceeds with downstream work (e.g. creating
// Machines). It mirrors the NetworkNamespace's Ready condition onto the cluster
// status so users can see the wait reason via kubectl describe.
//
// Returns nil when the NetworkNamespace is Ready (or when the cluster doesn't
// reference one — legacy specs are not blocked).
//
// Returns a *talosclientservice.RequeueError when the NetworkNamespace is missing
// or not yet Ready, so the caller can requeue without surfacing a hard failure.
//
// Returns a regular error when the NetworkNamespace has reached a terminal failure
// state (won't clear without a spec change), so the caller can stop reconciling.
func (s *EndpointService) EnsureNetworkNamespaceReady(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	nnName := cluster.Spec.Cluster.NetworkNamespaceName
	if nnName == "" {
		// Legacy spec without an explicit NetworkNamespace reference — no gate to apply.
		return nil
	}

	nn, err := s.findNetworkNamespace(ctx, cluster.GetNamespace(), nnName)
	if err != nil {
		return fmt.Errorf("failed to look up NetworkNamespace %q: %w", nnName, err)
	}
	if nn == nil {
		// Referenced but not yet present — wait for it to appear.
		if s.StatusManager != nil {
			_ = s.StatusManager.SetCondition(ctx, cluster, kcCondNetworkNamespaceReady, "Unknown", "NotFound",
				fmt.Sprintf("NetworkNamespace %q not found yet", nnName))
		}
		return talosclientservice.NewRequeueError(
			fmt.Sprintf("NetworkNamespace %q not found yet", nnName),
			dependentCRRequeueDelay,
		)
	}

	s.mirrorReadyToKC(ctx, cluster, nn.Status.Conditions, kcCondNetworkNamespaceReady,
		fmt.Sprintf("NetworkNamespace %s/%s", nn.Namespace, nn.Name))

	ready, terminal := isResourceReady(nn.Status.Conditions)
	if terminal != nil {
		return fmt.Errorf("NetworkNamespace %s/%s is in a terminal failure state: %w",
			nn.Namespace, nn.Name, terminal)
	}
	if !ready {
		vlog.Info(fmt.Sprintf("Waiting for NetworkNamespace %s/%s to reach Ready (phase=%s)",
			nn.Namespace, nn.Name, nn.Status.Phase))
		return talosclientservice.NewRequeueError(
			fmt.Sprintf("NetworkNamespace %s/%s not ready yet (phase=%s)", nn.Namespace, nn.Name, nn.Status.Phase),
			dependentCRRequeueDelay,
		)
	}
	return nil
}

// DetermineControlPlaneEndpoints determines the control plane endpoints based on the configured endpoint mode
// Supported modes:
// - "none": Use control plane IPs directly (no load balancing)
// - "networkconfiguration": Use ControlPlaneVirtualSharedIP from NetworkNamespace (default)
// - "talosvip": Use Talos built-in VIP (requires additional Talos configuration)
// - "custom": Use user-provided endpoint addresses from CUSTOM_ENDPOINT environment variable
func (s *EndpointService) DetermineControlPlaneEndpoints(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, controlPlaneIPs []string) ([]string, error) {
	endpointMode := consts.EndpointMode(viper.GetString(consts.ENDPOINT_MODE))

	// Validate endpoint mode
	if !consts.IsValidEndpointMode(string(endpointMode)) {
		vlog.Warn(fmt.Sprintf("Invalid endpoint mode '%s', falling back to default '%s'", endpointMode, consts.DefaultEndpointMode))
		endpointMode = consts.DefaultEndpointMode
	}

	vlog.Info(fmt.Sprintf("Using endpoint mode: %s for cluster %s", endpointMode, cluster.Name))

	switch endpointMode {
	case consts.EndpointModeNone:
		return s.getEndpointsNone(controlPlaneIPs)

	case consts.EndpointModeNetworkConfiguration:
		return s.getEndpointsNetworkConfiguration(ctx, cluster, controlPlaneIPs)

	case consts.EndpointModeTalosVIP:
		return s.getEndpointsTalosVIP(ctx, cluster, controlPlaneIPs)

	case consts.EndpointModeCustom:
		return s.getEndpointsCustom()

	default:
		// This shouldn't happen due to validation above, but handle it gracefully
		return s.getEndpointsNetworkConfiguration(ctx, cluster, controlPlaneIPs)
	}
}

// getEndpointsNone returns control plane IPs directly without any load balancing.
// This is the simplest mode but provides no HA for the control plane endpoint.
func (s *EndpointService) getEndpointsNone(controlPlaneIPs []string) ([]string, error) {
	if len(controlPlaneIPs) == 0 {
		return nil, fmt.Errorf("no control plane IPs available for endpoint mode 'none'")
	}
	vlog.Info(fmt.Sprintf("Endpoint mode 'none': using control plane IP directly: %s", controlPlaneIPs[0]))
	// Return first control plane IP as endpoint
	return []string{controlPlaneIPs[0]}, nil
}

// getEndpointsNetworkConfiguration uses the NetworkNamespace's ControlPlaneVirtualSharedIP
// to obtain load balancer IPs. This is the default and recommended mode.
func (s *EndpointService) getEndpointsNetworkConfiguration(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, controlPlaneIPs []string) ([]string, error) {
	networkNamespace, err := s.findNetworkNamespace(ctx, cluster.GetNamespace(), cluster.Spec.Cluster.NetworkNamespaceName)
	if err != nil {
		return nil, fmt.Errorf("failed to find network namespace: %w", err)
	}

	if networkNamespace == nil {
		vlog.Warn("No NetworkNamespace found, falling back to direct control plane IPs")
		return []string{controlPlaneIPs[0]}, nil
	}

	// The NetworkNamespace must be Ready before we can attach a VIP to it —
	// its Status fields (subnet, prefix, etc.) aren't populated until then.
	// Surface this as a requeue so we don't burn reconciles waiting.
	s.mirrorReadyToKC(ctx, cluster, networkNamespace.Status.Conditions, kcCondNetworkNamespaceReady, fmt.Sprintf("NetworkNamespace %s/%s", networkNamespace.Namespace, networkNamespace.Name))
	if ready, terminal := isResourceReady(networkNamespace.Status.Conditions); terminal != nil {
		vlog.Warn(fmt.Sprintf("NetworkNamespace %s/%s is in terminal failure state, spec change required: %v", networkNamespace.Namespace, networkNamespace.Name, terminal))
		return nil, fmt.Errorf("NetworkNamespace %s/%s is in a terminal failure state: %w", networkNamespace.Namespace, networkNamespace.Name, terminal)
	} else if !ready {
		vlog.Info(fmt.Sprintf("Waiting for NetworkNamespace %s/%s to reach Ready (phase=%s)", networkNamespace.Namespace, networkNamespace.Name, networkNamespace.Status.Phase))
		return nil, talosclientservice.NewRequeueError(
			fmt.Sprintf("NetworkNamespace %s/%s not ready yet", networkNamespace.Namespace, networkNamespace.Name),
			dependentCRRequeueDelay,
		)
	}

	// Create or get ControlPlaneVirtualSharedIP for load balancing
	// Initially only use the first control plane IP - more will be added after they're configured
	firstControlPlaneIP := []string{controlPlaneIPs[0]}
	vipEndpoints, err := s.EnsureControlPlaneVIPs(ctx, networkNamespace, cluster, firstControlPlaneIP)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure control plane VIP: %w", err)
	}

	vlog.Info(fmt.Sprintf("Endpoint mode 'networkconfiguration': using VIP endpoints: %v", vipEndpoints))
	return vipEndpoints, nil
}

// getEndpointsTalosVIP allocates a VIP IP from the static pool via a NetworkConfiguration
// and returns it as the control plane endpoint. The operator will also inject a
// Layer2VIPConfig patch into the control plane Talos config so that Talos advertises
// the VIP using gratuitous ARP.
func (s *EndpointService) getEndpointsTalosVIP(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, controlPlaneIPs []string) ([]string, error) {
	if len(controlPlaneIPs) == 0 {
		return nil, fmt.Errorf("no control plane IPs available for endpoint mode 'talosvip'")
	}

	networkNamespace, err := s.findNetworkNamespace(ctx, cluster.GetNamespace(), cluster.Spec.Cluster.NetworkNamespaceName)
	if err != nil {
		return nil, fmt.Errorf("failed to find NetworkNamespace for talosvip: %w", err)
	}
	if networkNamespace == nil {
		vlog.Warn("No NetworkNamespace found for talosvip mode, falling back to first control plane IP")
		return []string{controlPlaneIPs[0]}, nil
	}

	// Create or get a NetworkConfiguration to reserve a VIP IP from the static pool
	vipNCName := cluster.Spec.Cluster.ClusterId + "-vip"
	vipNC := &vitistackv1alpha1.NetworkConfiguration{}
	err = s.Get(ctx, types.NamespacedName{Name: vipNCName, Namespace: cluster.Namespace}, vipNC)

	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get VIP NetworkConfiguration: %w", err)
		}

		// Create a NetworkConfiguration with a single "vip" interface to reserve one IP
		vipNC = &vitistackv1alpha1.NetworkConfiguration{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vipNCName,
				Namespace: cluster.Namespace,
				Labels: map[string]string{
					vitistackv1alpha1.ClusterIdAnnotation: cluster.Spec.Cluster.ClusterId,
					"vitistack.io/purpose":                "control-plane-vip",
				},
			},
			Spec: vitistackv1alpha1.NetworkConfigurationSpec{
				Name:                 vipNCName,
				NetworkNamespaceName: networkNamespace.Name,
				Provider:             vitistackv1alpha1.ProviderNameStaticIP,
				NetworkInterfaces: []vitistackv1alpha1.NetworkConfigurationInterface{
					{Name: "vip"},
				},
			},
		}
		if err := s.Create(ctx, vipNC); err != nil {
			return nil, fmt.Errorf("failed to create VIP NetworkConfiguration %s: %w", vipNCName, err)
		}
		vlog.Info(fmt.Sprintf("Created VIP NetworkConfiguration %s/%s for cluster %s", cluster.Namespace, vipNCName, cluster.Name))
	}

	// Wait for the static-ip-operator to allocate an IP
	vipIP, err := s.waitForVIPIPAllocation(ctx, cluster, vipNCName, 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed waiting for VIP IP allocation: %w", err)
	}

	vlog.Info(fmt.Sprintf("Endpoint mode 'talosvip': using allocated VIP %s for cluster %s", vipIP, cluster.Name))
	return []string{vipIP}, nil
}

// waitForVIPIPAllocation polls the VIP NetworkConfiguration until an IP is allocated
func (s *EndpointService) waitForVIPIPAllocation(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, vipNCName string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("context cancelled while waiting for VIP IP allocation: %w", ctx.Err())
		case <-ticker.C:
			if time.Now().After(deadline) {
				return "", fmt.Errorf("timeout waiting for VIP IP allocation on %s/%s after %v", cluster.Namespace, vipNCName, timeout)
			}

			nc := &vitistackv1alpha1.NetworkConfiguration{}
			if err := s.Get(ctx, types.NamespacedName{Name: vipNCName, Namespace: cluster.Namespace}, nc); err != nil {
				vlog.Warn(fmt.Sprintf("Failed to get VIP NetworkConfiguration status: %v", err))
				continue
			}

			if nc.Status.Phase == "Error" || nc.Status.Status == statusFailed {
				return "", fmt.Errorf("VIP NetworkConfiguration %s/%s failed: %s", cluster.Namespace, vipNCName, nc.Status.Message)
			}

			// Check if the "vip" interface has an allocated IP
			for i := range nc.Status.NetworkInterfaces {
				if nc.Status.NetworkInterfaces[i].Name == "vip" && nc.Status.NetworkInterfaces[i].IPAllocated && len(nc.Status.NetworkInterfaces[i].IPv4Addresses) > 0 {
					return nc.Status.NetworkInterfaces[i].IPv4Addresses[0], nil
				}
			}

			vlog.Info(fmt.Sprintf("Waiting for VIP IP allocation on %s/%s... phase=%s", cluster.Namespace, vipNCName, nc.Status.Phase))
		}
	}
}

// GetAllocatedVIPIP returns the currently allocated VIP IP for a cluster, or empty string if none.
// This is used by config generation to inject the Layer2VIPConfig patch.
func (s *EndpointService) GetAllocatedVIPIP(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) string {
	vipNCName := cluster.Spec.Cluster.ClusterId + "-vip"
	nc := &vitistackv1alpha1.NetworkConfiguration{}
	if err := s.Get(ctx, types.NamespacedName{Name: vipNCName, Namespace: cluster.Namespace}, nc); err != nil {
		return ""
	}
	for i := range nc.Status.NetworkInterfaces {
		if nc.Status.NetworkInterfaces[i].Name == "vip" && nc.Status.NetworkInterfaces[i].IPAllocated && len(nc.Status.NetworkInterfaces[i].IPv4Addresses) > 0 {
			return nc.Status.NetworkInterfaces[i].IPv4Addresses[0]
		}
	}
	return ""
}

// getEndpointsCustom returns user-provided endpoint addresses from the CUSTOM_ENDPOINT environment variable.
// This allows users to specify their own load balancer or VIP addresses.
func (s *EndpointService) getEndpointsCustom() ([]string, error) {
	customEndpoint := strings.TrimSpace(viper.GetString(consts.CUSTOM_ENDPOINT))
	if customEndpoint == "" {
		return nil, fmt.Errorf("endpoint mode 'custom' requires CUSTOM_ENDPOINT environment variable to be set")
	}

	// Parse comma-separated endpoints
	endpoints := strings.Split(customEndpoint, ",")
	var validEndpoints []string
	for _, ep := range endpoints {
		ep = strings.TrimSpace(ep)
		if ep != "" {
			validEndpoints = append(validEndpoints, ep)
		}
	}

	if len(validEndpoints) == 0 {
		return nil, fmt.Errorf("no valid endpoints found in CUSTOM_ENDPOINT: %s", customEndpoint)
	}

	vlog.Info(fmt.Sprintf("Endpoint mode 'custom': using custom endpoints: %v", validEndpoints))
	return validEndpoints, nil
}

// EnsureControlPlaneVIPs creates or updates a ControlPlaneVirtualSharedIP resource and
// returns its LoadBalancerIps once it has reached Ready.
//
// Provisioning the VIP is asynchronous and driven by the controller that owns
// the ControlPlaneVirtualSharedIP CRD (deployment-specific). If the VIP isn't
// Ready yet, this returns a RequeueError so the outer reconcile loop retries
// without blocking a goroutine on a sleep loop.
func (s *EndpointService) EnsureControlPlaneVIPs(ctx context.Context, networkNamespace *vitistackv1alpha1.NetworkNamespace, cluster *vitistackv1alpha1.KubernetesCluster, controlPlaneIPs []string) ([]string, error) {
	vipName := cluster.Spec.Cluster.ClusterId

	vip := &vitistackv1alpha1.ControlPlaneVirtualSharedIP{}
	err := s.Get(ctx, types.NamespacedName{Name: vipName, Namespace: cluster.Namespace}, vip)

	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get ControlPlaneVirtualSharedIP: %w", err)
		}

		// Create new VIP
		vip = &vitistackv1alpha1.ControlPlaneVirtualSharedIP{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vipName,
				Namespace: cluster.Namespace,
				Labels: map[string]string{
					vitistackv1alpha1.ClusterIdAnnotation: cluster.Spec.Cluster.ClusterId,
					vitistackv1alpha1.NodeRoleAnnotation:  "control-plane",
				},
			},
			Spec: vitistackv1alpha1.ControlPlaneVirtualSharedIPSpec{
				DatacenterIdentifier:       networkNamespace.Spec.DatacenterIdentifier,
				ClusterIdentifier:          cluster.Spec.Cluster.ClusterId,
				SupervisorIdentifier:       networkNamespace.Spec.SupervisorIdentifier,
				Provider:                   cluster.Spec.Cluster.Provider.String(),
				Method:                     "first-alive",
				PoolMembers:                controlPlaneIPs,
				Environment:                cluster.Spec.Cluster.Environment,
				NetworkNamespaceIdentifier: networkNamespace.Name,
			},
		}

		if err := s.Create(ctx, vip); err != nil {
			return nil, fmt.Errorf("failed to create ControlPlaneVirtualSharedIP: %w", err)
		}
		vlog.Info(fmt.Sprintf("Created ControlPlaneVirtualSharedIP %s/%s, awaiting provisioning", vip.Namespace, vip.Name))
		if s.StatusManager != nil {
			_ = s.StatusManager.SetCondition(ctx, cluster, kcCondControlPlaneVirtualSharedIPReady, "False", "Provisioning",
				fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s just created, awaiting provisioning", vip.Namespace, vip.Name))
		}
		return nil, talosclientservice.NewRequeueError(
			fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s just created, awaiting provisioning", vip.Namespace, vip.Name),
			dependentCRRequeueDelay,
		)
	} else if !stringSlicesEqual(vip.Spec.PoolMembers, controlPlaneIPs) {
		// Update existing VIP if pool members changed
		vip.Spec.PoolMembers = controlPlaneIPs
		if err := s.Update(ctx, vip); err != nil {
			return nil, fmt.Errorf("failed to update ControlPlaneVirtualSharedIP: %w", err)
		}
		vlog.Info(fmt.Sprintf("Updated ControlPlaneVirtualSharedIP %s/%s pool members, awaiting reconciliation", vip.Namespace, vip.Name))
		if s.StatusManager != nil {
			_ = s.StatusManager.SetCondition(ctx, cluster, kcCondControlPlaneVirtualSharedIPReady, "False", "Updating",
				fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s pool members updated, awaiting reconciliation", vip.Namespace, vip.Name))
		}
		return nil, talosclientservice.NewRequeueError(
			fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s pool members updated, awaiting reconciliation", vip.Namespace, vip.Name),
			dependentCRRequeueDelay,
		)
	}

	// Spec is up to date — wait for Ready before returning LoadBalancerIps.
	s.mirrorReadyToKC(ctx, cluster, vip.Status.Conditions, kcCondControlPlaneVirtualSharedIPReady, fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s", vip.Namespace, vip.Name))
	if ready, terminal := isResourceReady(vip.Status.Conditions); terminal != nil {
		vlog.Warn(fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s is in terminal failure state, spec change required: %v", vip.Namespace, vip.Name, terminal))
		return nil, fmt.Errorf("ControlPlaneVirtualSharedIP %s/%s is in a terminal failure state: %w", vip.Namespace, vip.Name, terminal)
	} else if !ready {
		vlog.Info(fmt.Sprintf("Waiting for ControlPlaneVirtualSharedIP %s/%s to reach Ready (phase=%s status=%s)", vip.Namespace, vip.Name, vip.Status.Phase, vip.Status.Status))
		return nil, talosclientservice.NewRequeueError(
			fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s not ready yet (phase=%s status=%s)", vip.Namespace, vip.Name, vip.Status.Phase, vip.Status.Status),
			dependentCRRequeueDelay,
		)
	}

	if len(vip.Status.LoadBalancerIps) == 0 {
		vlog.Warn(fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s is Ready but has no LoadBalancerIps yet, waiting", vip.Namespace, vip.Name))
		return nil, talosclientservice.NewRequeueError(
			fmt.Sprintf("ControlPlaneVirtualSharedIP %s/%s is Ready but has no LoadBalancerIps yet", vip.Namespace, vip.Name),
			dependentCRRequeueDelay,
		)
	}

	vlog.Info(fmt.Sprintf("Using VIP endpoint for cluster %s: %v", cluster.Name, vip.Status.LoadBalancerIps))
	return vip.Status.LoadBalancerIps, nil
}

// UpdateVIPPoolMembers updates the pool members of an existing ControlPlaneVirtualSharedIP
func (s *EndpointService) UpdateVIPPoolMembers(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, controlPlaneIPs []string) error {
	vipName := cluster.Spec.Cluster.ClusterId

	vip := &vitistackv1alpha1.ControlPlaneVirtualSharedIP{}
	err := s.Get(ctx, types.NamespacedName{Name: vipName, Namespace: cluster.Namespace}, vip)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// VIP doesn't exist, nothing to update
			return nil
		}
		return fmt.Errorf("failed to get ControlPlaneVirtualSharedIP: %w", err)
	}

	// Only update if pool members have changed
	if !stringSlicesEqual(vip.Spec.PoolMembers, controlPlaneIPs) {
		vip.Spec.PoolMembers = controlPlaneIPs
		if err := s.Update(ctx, vip); err != nil {
			return fmt.Errorf("failed to update ControlPlaneVirtualSharedIP pool members: %w", err)
		}
		vlog.Info(fmt.Sprintf("Updated VIP %s/%s pool members: %v", vip.Namespace, vip.Name, controlPlaneIPs))
	}

	return nil
}

// findNetworkNamespace finds the NetworkNamespace in the given namespace.
// If networkNamespaceName is set, it looks up that specific NetworkNamespace by name.
// Otherwise, it falls back to listing all NetworkNamespaces and using the first one (legacy behavior).
func (s *EndpointService) findNetworkNamespace(ctx context.Context, namespace, networkNamespaceName string) (*vitistackv1alpha1.NetworkNamespace, error) {
	// If a specific NetworkNamespace name is provided, look it up directly
	if networkNamespaceName != "" {
		nn := &vitistackv1alpha1.NetworkNamespace{}
		if err := s.Get(ctx, client.ObjectKey{Name: networkNamespaceName, Namespace: namespace}, nn); err != nil {
			return nil, fmt.Errorf("failed to get NetworkNamespace %q in namespace %q: %w", networkNamespaceName, namespace, err)
		}
		return nn, nil
	}

	// Fallback: list all NetworkNamespaces and use the first one (legacy behavior)
	if _, alreadyWarned := deprecationWarned.LoadOrStore(namespace, true); !alreadyWarned {
		vlog.Warn(fmt.Sprintf("NetworkNamespaceName not set on KubernetesCluster in namespace %q, falling back to listing NetworkNamespaces. "+
			"Please set spec.data.networkNamespaceName on the KubernetesCluster resource.", namespace))
	}
	list := &vitistackv1alpha1.NetworkNamespaceList{}
	if err := s.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, nil
	}
	return &list.Items[0], nil
}

// stringSlicesEqual checks if two string slices are equal
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
