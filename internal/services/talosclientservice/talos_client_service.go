package talosclientservice

import (
	"context"
	"crypto/tls"
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
	"google.golang.org/protobuf/types/known/durationpb"
)

type TalosClientService struct{}

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
		tClient, err = talosclient.New(ctx,
			talosclient.WithConfig(clientConfig),
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

	nodeCtx := talosclient.WithNodes(ctx, nodeIP)
	resp, err := nodeClient.ApplyConfiguration(nodeCtx, &machineapi.ApplyConfigurationRequest{
		Data:           configData,
		Mode:           machineapi.ApplyConfigurationRequest_AUTO,
		DryRun:         false,
		TryModeTimeout: durationpb.New(2 * time.Minute),
	})
	if err != nil {
		return fmt.Errorf("error applying configuration: %w", err)
	}

	vlog.Info(fmt.Sprintf("Talos apply configuration response: node=%s messages=%v", nodeIP, resp.Messages))
	return nil
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
			if !isTLSHandshakeAuthError(err) && !strings.Contains(err.Error(), "transport: authentication handshake failed") {
				return err
			}
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
