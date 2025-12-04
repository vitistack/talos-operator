package endpointservice

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/pkg/consts"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EndpointService handles VIP and endpoint management for Talos clusters
type EndpointService struct {
	client.Client
}

// NewEndpointService creates a new EndpointService
func NewEndpointService(c client.Client) *EndpointService {
	return &EndpointService{Client: c}
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
		return s.getEndpointsTalosVIP(controlPlaneIPs)

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
	networkNamespace, err := s.findNetworkNamespace(ctx, cluster.GetNamespace())
	if err != nil {
		return nil, fmt.Errorf("failed to find network namespace: %w", err)
	}

	if networkNamespace == nil {
		vlog.Warn("No NetworkNamespace found, falling back to direct control plane IPs")
		return []string{controlPlaneIPs[0]}, nil
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

// getEndpointsTalosVIP returns endpoints for Talos built-in VIP mode.
// In this mode, the operator uses the control plane IPs directly, but Talos
// handles VIP failover internally through its machine config.
// Note: The actual VIP configuration should be added to tenant config overrides.
func (s *EndpointService) getEndpointsTalosVIP(controlPlaneIPs []string) ([]string, error) {
	if len(controlPlaneIPs) == 0 {
		return nil, fmt.Errorf("no control plane IPs available for endpoint mode 'talosvip'")
	}

	// For Talos VIP mode, we need to check if a custom VIP is configured in tenant overrides
	// The VIP address should be configured in the Talos machine config via tenant overrides
	// For now, use the first control plane IP as the endpoint
	// The actual VIP will be handled by Talos' internal VIP mechanism
	vlog.Info(fmt.Sprintf("Endpoint mode 'talosvip': using first control plane IP: %s (Talos VIP should be configured in machine config)", controlPlaneIPs[0]))
	vlog.Warn("Talos VIP mode requires additional configuration in tenant overrides to set the VIP address")

	// Return first control plane IP - the actual VIP address should be in the Talos config
	return []string{controlPlaneIPs[0]}, nil
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

// EnsureControlPlaneVIPs creates or updates a ControlPlaneVirtualSharedIP resource and waits for LoadBalancerIps
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
		vlog.Info(fmt.Sprintf("Created ControlPlaneVirtualSharedIP: %s/%s", vip.Namespace, vip.Name))
	} else if !stringSlicesEqual(vip.Spec.PoolMembers, controlPlaneIPs) {
		// Update existing VIP if pool members changed
		vip.Spec.PoolMembers = controlPlaneIPs
		if err := s.Update(ctx, vip); err != nil {
			return nil, fmt.Errorf("failed to update ControlPlaneVirtualSharedIP: %w", err)
		}
		vlog.Info(fmt.Sprintf("Updated ControlPlaneVirtualSharedIP pool members: %s/%s", vip.Namespace, vip.Name))
	}

	// Wait for LoadBalancerIps to be populated
	endpoints, err := s.WaitForVIPLoadBalancerIP(ctx, cluster, vipName, 15*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed waiting for VIP LoadBalancerIps: %w", err)
	}

	vlog.Info(fmt.Sprintf("Using VIP endpoint for cluster %s: %v", cluster.Name, endpoints))
	return endpoints, nil
}

// WaitForVIPLoadBalancerIP waits for the ControlPlaneVirtualSharedIP status to have LoadBalancerIps populated
// It handles cluster deletion, VIP errors, and provides proper error context
func (s *EndpointService) WaitForVIPLoadBalancerIP(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster, vipName string, timeout time.Duration) ([]string, error) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled while waiting for VIP LoadBalancerIps: %w", ctx.Err())
		case <-ticker.C:
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("timeout waiting for VIP LoadBalancerIps after %v", timeout)
			}

			// Check if cluster is being deleted
			if err := s.checkClusterDeletion(ctx, cluster); err != nil {
				return nil, err
			}

			// Check VIP status and get LoadBalancerIps
			loadBalancerIps, shouldContinue, err := s.checkVIPStatus(ctx, cluster.Namespace, vipName)
			if err != nil {
				return nil, err
			}
			if shouldContinue {
				continue
			}
			if loadBalancerIps != nil {
				return loadBalancerIps, nil
			}
		}
	}
}

// checkClusterDeletion checks if the cluster is being deleted or has been removed
func (s *EndpointService) checkClusterDeletion(ctx context.Context, cluster *vitistackv1alpha1.KubernetesCluster) error {
	clusterCheck := &vitistackv1alpha1.KubernetesCluster{}
	if err := s.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, clusterCheck); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("cluster %s/%s was deleted while waiting for VIP", cluster.Namespace, cluster.Name)
		}
		vlog.Warn(fmt.Sprintf("Failed to check cluster status: %v", err))
		// Don't return error on transient errors, let retry continue
		return nil
	}
	if clusterCheck.GetDeletionTimestamp() != nil {
		return fmt.Errorf("cluster %s/%s is being deleted, cancelling VIP wait", cluster.Namespace, cluster.Name)
	}
	return nil
}

// checkVIPStatus checks VIP status and returns LoadBalancerIps if ready
// Returns (loadBalancerIps, shouldContinue, error)
func (s *EndpointService) checkVIPStatus(ctx context.Context, namespace, vipName string) ([]string, bool, error) {
	vip := &vitistackv1alpha1.ControlPlaneVirtualSharedIP{}
	if err := s.Get(ctx, types.NamespacedName{Name: vipName, Namespace: namespace}, vip); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, fmt.Errorf("VIP %s/%s was deleted or not found", namespace, vipName)
		}
		vlog.Warn(fmt.Sprintf("Failed to get VIP status: %v", err))
		// Continue on transient errors
		return nil, true, nil
	}

	// Check for VIP error conditions
	if vip.Status.Phase == "Failed" || vip.Status.Status == "Failed" {
		msg := vip.Status.Message
		if msg == "" {
			msg = "VIP creation failed without details"
		}
		return nil, false, fmt.Errorf("VIP %s/%s failed: %s", namespace, vipName, msg)
	}

	// Check if LoadBalancerIps are populated
	if len(vip.Status.LoadBalancerIps) > 0 {
		vlog.Info(fmt.Sprintf("VIP %s/%s ready with LoadBalancerIps: %v", namespace, vipName, vip.Status.LoadBalancerIps))
		return vip.Status.LoadBalancerIps, false, nil
	}

	// Log current status for debugging
	statusInfo := fmt.Sprintf("phase=%s, status=%s", vip.Status.Phase, vip.Status.Status)
	if vip.Status.Message != "" {
		statusInfo += fmt.Sprintf(", message=%s", vip.Status.Message)
	}
	vlog.Info(fmt.Sprintf("Waiting for VIP %s/%s to have LoadBalancerIps populated... [%s]", namespace, vipName, statusInfo))
	return nil, true, nil
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

// findNetworkNamespace finds the NetworkNamespace in the given namespace
func (s *EndpointService) findNetworkNamespace(ctx context.Context, namespace string) (*vitistackv1alpha1.NetworkNamespace, error) {
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
