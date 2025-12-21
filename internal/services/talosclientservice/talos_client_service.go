package talosclientservice

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cosi-project/runtime/pkg/safe"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
)

type TalosClientService struct{}

// RequeueError signals that the reconciler should requeue without treating it as a failure.
// This allows non-blocking waits by returning early and letting the controller retry.
type RequeueError struct {
	Reason   string
	Duration time.Duration
}

func (e *RequeueError) Error() string {
	return fmt.Sprintf("requeue needed: %s (after %v)", e.Reason, e.Duration)
}

// NewRequeueError creates a new RequeueError with the specified reason and duration.
func NewRequeueError(reason string, duration time.Duration) *RequeueError {
	return &RequeueError{Reason: reason, Duration: duration}
}

// IsRequeueError checks if the error is a RequeueError and returns it if so.
func IsRequeueError(err error) (*RequeueError, bool) {
	var requeueErr *RequeueError
	if errors.As(err, &requeueErr) {
		return requeueErr, true
	}
	return nil, false
}

func NewTalosClientService() *TalosClientService {
	return &TalosClientService{}
}

func (s *TalosClientService) CreateTalosClient(
	ctx context.Context,
	insecure bool,
	clientConfig *clientconfig.Config,
	endpointIps []string) (*talosclient.Client, error) {
	var tClient *talosclient.Client
	var err error

	if !insecure {
		tClient, err = talosclient.New(ctx,
			talosclient.WithConfig(clientConfig),
			talosclient.WithEndpoints(endpointIps...),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create secure Talos client: %w", err)
		}
	} else {
		// For insecure mode (maintenance/unconfigured nodes), do NOT use WithConfig
		// as the node doesn't have the cluster CA yet and doesn't require client auth
		tClient, err = talosclient.New(ctx,
			talosclient.WithTLSConfig(&tls.Config{
				InsecureSkipVerify: true, // #nosec G402
			}),
			talosclient.WithEndpoints(endpointIps...),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create insecure Talos client: %w", err)
		}
	}
	return tClient, nil
}

func (s *TalosClientService) ApplyConfigToNode(
	ctx context.Context,
	insecure bool,
	clientConfig *clientconfig.Config,
	nodeIP string,
	configData []byte) error {
	nodeClient, err := s.CreateTalosClient(ctx, insecure, clientConfig, []string{nodeIP})
	if err != nil {
		return fmt.Errorf("failed to create Talos client: %w", err)
	}
	defer func() { _ = nodeClient.Close() }()

	// For nodes in maintenance mode (insecure=true), the maintenance server only
	// accepts AUTO, TRY, or REBOOT modes. TryModeTimeout is only relevant for TRY mode.
	// When applying to a maintenance node, the node will install and reboot automatically.
	//
	// This matches the behavior of `talosctl apply-config --insecure`:
	// - The maintenance server receives the config
	// - Validates it
	// - Applies it (which triggers install and reboot)
	//
	// See: https://github.com/siderolabs/talos/blob/main/internal/app/maintenance/server.go
	nodeCtx := talosclient.WithNodes(ctx, nodeIP)
	resp, err := nodeClient.ApplyConfiguration(nodeCtx, &machineapi.ApplyConfigurationRequest{
		Data:   configData,
		Mode:   machineapi.ApplyConfigurationRequest_AUTO,
		DryRun: false,
		// Note: TryModeTimeout is not set as it's only applicable for TRY mode
		// and maintenance mode doesn't support NO_REBOOT or STAGED modes
	})
	if err != nil {
		return fmt.Errorf("error applying configuration to node %s: %w", nodeIP, err)
	}

	// Log response details for debugging
	for _, msg := range resp.Messages {
		details := ""
		if msg.ModeDetails != "" {
			details = fmt.Sprintf(" details=%s", msg.ModeDetails)
		}
		warnings := ""
		if len(msg.Warnings) > 0 {
			warnings = fmt.Sprintf(" warnings=%v", msg.Warnings)
		}
		vlog.Info(fmt.Sprintf("Talos apply configuration response: node=%s mode=%s%s%s",
			nodeIP, msg.Mode.String(), details, warnings))
	}
	return nil
}

// WaitForNodeRebootAfterApply waits for a node to go down and come back up after applying configuration.
// This is important for maintenance mode nodes that will install and reboot after receiving config.
// The function first waits for the API to become unreachable (node rebooting), then waits for it to become reachable again.
//
// Parameters:
//   - nodeIP: The IP address of the node
//   - initialWait: Time to wait before starting to check (allows reboot to begin)
//   - timeout: Maximum time to wait for the node to come back up
//   - interval: Time between reachability checks
//
// Returns nil if the node comes back up, or an error if timeout is exceeded.
func (s *TalosClientService) WaitForNodeRebootAfterApply(
	nodeIP string,
	initialWait time.Duration,
	timeout time.Duration,
	interval time.Duration) error {
	addr := net.JoinHostPort(nodeIP, "50000")

	// Wait for initial period to allow the node to start rebooting
	vlog.Info(fmt.Sprintf("Waiting %v for node to begin reboot: node=%s", initialWait, nodeIP))
	time.Sleep(initialWait)

	// First, optionally wait for the node to go down (become unreachable)
	// This is a quick check - we don't wait long because some nodes might already be rebooting
	downCheckDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(downCheckDeadline) {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			vlog.Info(fmt.Sprintf("Node is rebooting (API unreachable): node=%s", nodeIP))
			break
		}
		_ = conn.Close()
		time.Sleep(2 * time.Second)
	}

	// Now wait for the node to come back up
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for node %s to come back up after reboot", nodeIP)
		}

		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			vlog.Info(fmt.Sprintf("Node still rebooting: node=%s error=%s", nodeIP, err.Error()))
			time.Sleep(interval)
			continue
		}
		_ = conn.Close()

		vlog.Info(fmt.Sprintf("Node is back up after reboot: node=%s", nodeIP))
		return nil
	}
}

// IsTalosAPIReachable performs a single non-blocking check to see if a Talos API is reachable on a specific IP.
func (s *TalosClientService) IsTalosAPIReachable(nodeIP string) bool {
	addr := net.JoinHostPort(nodeIP, "50000")
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func (s *TalosClientService) WaitForTalosAPIs(
	machines []*vitistackv1alpha1.Machine,
	timeout time.Duration,
	interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	ips := []string{}
	for _, m := range machines {
		if len(m.Status.NetworkInterfaces) == 0 || len(m.Status.PublicIPAddresses) == 0 {
			continue
		}
		ips = append(ips, m.Status.PublicIPAddresses[0])
	}

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for Talos APIs to be reachable on %v", ips)
		}

		allOK := true
		for _, ip := range ips {
			addr := net.JoinHostPort(ip, "50000")
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				vlog.Warn("Talos API port not reachable yet: node=" + ip + " error=" + err.Error())
				allOK = false
				continue
			}
			_ = conn.Close()
		}

		if allOK {
			return nil
		}
		time.Sleep(interval)
	}
}

// WaitForKubernetesAPIReady waits for the Kubernetes API server to be accessible on the given endpoint.
// This should be called after bootstrap to ensure the API server is fully ready before configuring workers.
func (s *TalosClientService) WaitForKubernetesAPIReady(
	endpointIP string,
	timeout time.Duration,
	interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	addr := net.JoinHostPort(endpointIP, "6443")

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for Kubernetes API server to be reachable on %s", addr)
		}

		// Try to establish a TCP connection to port 6443
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			vlog.Info("Kubernetes API not reachable yet: endpoint=" + addr + " error=" + err.Error())
			time.Sleep(interval)
			continue
		}
		_ = conn.Close()

		vlog.Info("Kubernetes API server is reachable: endpoint=" + addr)
		return nil
	}
}

// IsKubernetesAPIReady performs a single non-blocking check to see if the Kubernetes API server is reachable.
// Returns true if reachable, false otherwise. This is the non-blocking alternative to WaitForKubernetesAPIReady.
func (s *TalosClientService) IsKubernetesAPIReady(endpointIP string) bool {
	addr := net.JoinHostPort(endpointIP, "6443")
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		vlog.Info("Kubernetes API not reachable: endpoint=" + addr + " error=" + err.Error())
		return false
	}
	_ = conn.Close()
	vlog.Info("Kubernetes API server is reachable: endpoint=" + addr)
	return true
}

// AreTalosAPIsReady performs a single non-blocking check to see if all Talos APIs are reachable.
// Returns true if all are reachable, false otherwise. This is the non-blocking alternative to WaitForTalosAPIs.
func (s *TalosClientService) AreTalosAPIsReady(machines []*vitistackv1alpha1.Machine) bool {
	for _, m := range machines {
		if m == nil || len(m.Status.PublicIPAddresses) == 0 {
			return false
		}
		ip := m.Status.PublicIPAddresses[0]
		addr := net.JoinHostPort(ip, "50000")
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			vlog.Info("Talos API not reachable: node=" + m.Name + " addr=" + addr + " error=" + err.Error())
			return false
		}
		_ = conn.Close()
	}
	vlog.Info(fmt.Sprintf("All %d Talos APIs are reachable", len(machines)))
	return true
}

func (s *TalosClientService) BootstrapTalosControlPlane(
	ctx context.Context,
	tClient *talosclient.Client,
	controlPlaneIP string) error {
	ctx = talosclient.WithNodes(ctx, controlPlaneIP)
	if err := tClient.Bootstrap(ctx, &machineapi.BootstrapRequest{}); err != nil {
		return fmt.Errorf("talos bootstrap failed: %w", err)
	}

	vlog.Info("Talos bootstrap initiated: node=" + controlPlaneIP)
	return nil
}

func (s *TalosClientService) BootstrapTalosControlPlaneWithRetry(
	ctx context.Context,
	tClient *talosclient.Client,
	controlPlaneIP string,
	timeout time.Duration,
	interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for {
		if err := s.BootstrapTalosControlPlane(ctx, tClient, controlPlaneIP); err == nil {
			return nil
		} else {
			lastErr = err
			// Check if this is a retryable error
			if !IsRetryableBootstrapError(err) {
				return err
			}
			vlog.Info("Bootstrap attempt failed (will retry): node=" + controlPlaneIP + " error=" + err.Error())
		}
		if time.Now().After(deadline) {
			return lastErr
		}
		time.Sleep(interval)
	}
}

func (s *TalosClientService) GetKubeconfigWithRetry(
	ctx context.Context,
	clientCfg *clientconfig.Config,
	nodesIp string,
	endpointIp string,
	timeout time.Duration,
	interval time.Duration) ([]byte, error) {
	deadline := time.Now().Add(timeout)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		tClient, err := talosclient.New(ctx, talosclient.WithConfig(clientCfg), talosclient.WithEndpoints(nodesIp))
		if err != nil {
			return nil, fmt.Errorf("failed to create Talos client for kubeconfig: %w", err)
		}

		// Use endpoint IP for node context as well when using VIP/load balancer
		ctxWithNode := talosclient.WithNodes(ctx, nodesIp)

		kubeconfig, err := tClient.Kubeconfig(ctxWithNode)
		if err == nil && len(kubeconfig) > 0 {
			return kubeconfig, nil
		}

		if time.Now().After(deadline) {
			if err != nil {
				return nil, fmt.Errorf("timeout waiting for kubeconfig: %w", err)
			}
			return nil, fmt.Errorf("timeout waiting for kubeconfig: received empty config")
		}

		vlog.Info("Kubeconfig not ready yet, retrying: endpoint=" + endpointIp + " node=" + nodesIp)
		time.Sleep(interval)
	}
}

func isTLSHandshakeAuthError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "x509:") || strings.Contains(s, "tls:")
}

// isRetryableBootstrapError checks if a bootstrap error is transient and should be retried.
// This includes TLS handshake errors, connection issues, and service unavailability.
// IsRetryableBootstrapError checks if a bootstrap error is transient and should be retried.
// This includes TLS handshake errors, connection issues, and service unavailability.
func IsRetryableBootstrapError(err error) bool {
	if err == nil {
		return false
	}

	// Check for TLS/auth errors first
	if isTLSHandshakeAuthError(err) {
		return true
	}

	s := err.Error()

	// Check for connection/transport errors that indicate the node is still coming up
	retryablePatterns := []string{
		"transport: authentication handshake failed",
		"connection refused",
		"connection reset",
		"i/o timeout",
		"no route to host",
		"unavailable",
		"Unavailable",
		"DeadlineExceeded",
		"deadline exceeded",
		"EOF",
		"broken pipe",
		"connection closed",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(s, pattern) {
			return true
		}
	}

	return false
}

// nodeInfo holds node IP and hostname for status checking.
type nodeInfo struct {
	ip       string
	hostname string
}

// collectNodeInfo extracts IPv4 addresses and hostnames from machines.
func collectNodeInfo(machines []*vitistackv1alpha1.Machine) []nodeInfo {
	nodes := []nodeInfo{}
	for _, m := range machines {
		if len(m.Status.PublicIPAddresses) == 0 {
			continue
		}
		ip := findFirstIPv4(m.Status.PublicIPAddresses)
		if ip != "" {
			nodes = append(nodes, nodeInfo{ip: ip, hostname: m.Name})
		}
	}
	return nodes
}

// findFirstIPv4 returns the first IPv4 address from a list of IPs.
func findFirstIPv4(ips []string) string {
	for _, ipAddr := range ips {
		if !strings.Contains(ipAddr, ":") { // Simple IPv4 check (no colons)
			return ipAddr
		}
	}
	return ""
}

// WaitForNodesReady waits for all specified nodes to report nodeReady=true via Talos NodeStatus resource.
// This queries the Talos COSI API to check the Kubernetes node status as seen by Talos.
func (s *TalosClientService) WaitForNodesReady(
	ctx context.Context,
	clientConfig *clientconfig.Config,
	machines []*vitistackv1alpha1.Machine,
	timeout time.Duration,
	interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	nodes := collectNodeInfo(machines)

	if len(nodes) == 0 {
		vlog.Info("No nodes to wait for, skipping node ready check")
		return nil
	}

	readyNodes := make(map[string]bool)

	for {
		if time.Now().After(deadline) {
			return s.buildTimeoutError(nodes, readyNodes)
		}

		if s.checkAllNodesReady(ctx, clientConfig, nodes, readyNodes) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// buildTimeoutError creates an error listing nodes that are not ready.
func (s *TalosClientService) buildTimeoutError(nodes []nodeInfo, readyNodes map[string]bool) error {
	notReady := []string{}
	for _, n := range nodes {
		if !readyNodes[n.hostname] {
			notReady = append(notReady, n.hostname+"("+n.ip+")")
		}
	}
	return fmt.Errorf("timeout waiting for nodes to be ready: %v", notReady)
}

// checkAllNodesReady checks all nodes and returns true if all are ready.
func (s *TalosClientService) checkAllNodesReady(
	ctx context.Context,
	clientConfig *clientconfig.Config,
	nodes []nodeInfo,
	readyNodes map[string]bool) bool {
	allReady := true
	for _, n := range nodes {
		if readyNodes[n.hostname] {
			continue
		}

		ready, err := s.checkNodeReady(ctx, clientConfig, n.ip, n.hostname)
		if err != nil {
			vlog.Warn(fmt.Sprintf("Error checking node status: node=%s ip=%s error=%v", n.hostname, n.ip, err))
			allReady = false
			continue
		}
		if !ready {
			vlog.Info(fmt.Sprintf("Node not ready yet: node=%s ip=%s", n.hostname, n.ip))
			allReady = false
		} else {
			vlog.Info(fmt.Sprintf("Node is ready: node=%s ip=%s", n.hostname, n.ip))
			readyNodes[n.hostname] = true
		}
	}
	return allReady
}

// checkNodeReady queries the Talos NodeStatus resource to check if the node reports Ready.
func (s *TalosClientService) checkNodeReady(
	ctx context.Context,
	clientConfig *clientconfig.Config,
	nodeIP string,
	hostname string) (bool, error) {
	tClient, err := s.CreateTalosClient(ctx, false, clientConfig, []string{nodeIP})
	if err != nil {
		return false, fmt.Errorf("failed to create Talos client: %w", err)
	}
	defer func() { _ = tClient.Close() }()

	nodeCtx := talosclient.WithNodes(ctx, nodeIP)

	// Query NodeStatus resource from the k8s namespace
	// safe.StateListAll uses the default namespace from the resource definition
	nodeStatusList, err := safe.StateListAll[*k8s.NodeStatus](nodeCtx, tClient.COSI)
	if err != nil {
		return false, fmt.Errorf("failed to list NodeStatus resources: %w", err)
	}

	// Find the node status matching the hostname
	for ns := range nodeStatusList.All() {
		spec := ns.TypedSpec()
		if spec.Nodename == hostname {
			if spec.NodeReady {
				return true, nil
			}
			return false, nil
		}
	}

	// Node status not found yet (kubelet may not have registered)
	return false, nil
}

// UpgradeNode initiates a Talos OS upgrade on a single node.
// The node will download the new image and reboot to apply the upgrade.
// This is equivalent to `talosctl upgrade --image <installerImage>`.
func (s *TalosClientService) UpgradeNode(
	ctx context.Context,
	tClient *talosclient.Client,
	nodeIP string,
	installerImage string,
) error {
	nodeCtx := talosclient.WithNodes(ctx, nodeIP)

	vlog.Info(fmt.Sprintf("Initiating Talos upgrade: node=%s image=%s", nodeIP, installerImage))

	// Call the Upgrade API
	// Parameters: image, stage (false = immediate), force (false = normal), preserveData (false)
	resp, err := tClient.Upgrade(nodeCtx, installerImage, false, false)
	if err != nil {
		return fmt.Errorf("upgrade API call failed for node %s: %w", nodeIP, err)
	}

	// Log response
	for _, msg := range resp.Messages {
		vlog.Info(fmt.Sprintf("Talos upgrade response: node=%s ack=%s", nodeIP, msg.Ack))
	}

	return nil
}

// UpgradeKubernetes initiates a Kubernetes version upgrade on the cluster.
// Note: Kubernetes upgrade is done via talosctl upgrade-k8s command externally,
// as the Talos client doesn't expose this directly. This method is a placeholder
// that documents the intended flow - the actual implementation would need to:
// 1. Use talosctl upgrade-k8s command via exec, OR
// 2. Directly manipulate the machine configs to update Kubernetes version
func (s *TalosClientService) UpgradeKubernetes(
	ctx context.Context,
	tClient *talosclient.Client,
	controlPlaneIP string,
	targetVersion string,
) error {
	vlog.Info(fmt.Sprintf("Initiating Kubernetes upgrade: endpoint=%s version=%s", controlPlaneIP, targetVersion))

	// The Kubernetes upgrade in Talos is typically done by:
	// 1. Updating the machine configuration with new Kubernetes version
	// 2. Talos automatically handles the component upgrades
	//
	// For now, we document that the upgrade-k8s functionality needs to be
	// implemented either via:
	// - Shell exec of talosctl upgrade-k8s
	// - Direct machine config patching
	//
	// TODO: Implement actual Kubernetes upgrade logic
	// This could be done by patching machine configs with new k8s version

	vlog.Warn(fmt.Sprintf("Kubernetes upgrade not yet implemented: endpoint=%s version=%s", controlPlaneIP, targetVersion))
	return fmt.Errorf("kubernetes upgrade via API not yet implemented - use talosctl upgrade-k8s manually")
}

// GetTalosVersion retrieves the current Talos version running on a node.
func (s *TalosClientService) GetTalosVersion(
	ctx context.Context,
	tClient *talosclient.Client,
	nodeIP string,
) (string, error) {
	nodeCtx := talosclient.WithNodes(ctx, nodeIP)

	resp, err := tClient.Version(nodeCtx)
	if err != nil {
		return "", fmt.Errorf("failed to get Talos version from node %s: %w", nodeIP, err)
	}

	for _, msg := range resp.Messages {
		if msg.Version != nil {
			return msg.Version.Tag, nil
		}
	}

	return "", fmt.Errorf("no version information returned from node %s", nodeIP)
}

// GetKubernetesVersion retrieves the current Kubernetes version from a node.
// This queries the Talos Version API which includes Kubernetes version info.
func (s *TalosClientService) GetKubernetesVersion(
	ctx context.Context,
	tClient *talosclient.Client,
	nodeIP string,
) (string, error) {
	nodeCtx := talosclient.WithNodes(ctx, nodeIP)

	// The Version API returns both Talos and Kubernetes versions
	resp, err := tClient.Version(nodeCtx)
	if err != nil {
		return "", fmt.Errorf("failed to get version info from node %s: %w", nodeIP, err)
	}

	// The Version API doesn't directly expose Kubernetes version
	// Kubernetes version is managed via machine config, not runtime API
	// To get the actual running K8s version, query the Kubernetes API instead
	_ = resp // suppress unused warning

	return "", fmt.Errorf("kubernetes version retrieval from Talos API needs implementation - query Kubernetes API instead")
}

// IsEtcdHealthy checks if the etcd cluster is healthy.
// This should be called before performing upgrades to ensure cluster stability.
func (s *TalosClientService) IsEtcdHealthy(
	ctx context.Context,
	tClient *talosclient.Client,
	nodeIP string,
) (bool, error) {
	nodeCtx := talosclient.WithNodes(ctx, nodeIP)

	// Use the ServiceInfo API to check etcd health
	services, err := tClient.ServiceInfo(nodeCtx, "etcd")
	if err != nil {
		return false, fmt.Errorf("failed to get etcd service info: %w", err)
	}

	for _, svc := range services {
		// svc.Service is *machineapi.ServiceInfo which has Id, State, Health fields directly
		if svc.Service != nil {
			if svc.Service.Id == "etcd" || svc.Service.GetId() == "etcd" {
				health := svc.Service.GetHealth()
				state := svc.Service.GetState()
				// Check if etcd is running and healthy
				if state == "Running" && health != nil && health.Healthy {
					return true, nil
				}
				vlog.Info(fmt.Sprintf("etcd service state: node=%s state=%s healthy=%v",
					nodeIP, state, health != nil && health.Healthy))
				return false, nil
			}
		}
	}

	return false, fmt.Errorf("etcd service not found on node %s", nodeIP)
}
