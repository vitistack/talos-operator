package talosclientservice

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
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
	controlPlaneIps []string) (*talosclient.Client, error) {
	var tClient *talosclient.Client
	var err error

	if !insecure {
		tClient, err = talosclient.New(ctx,
			talosclient.WithConfig(clientConfig),
			talosclient.WithEndpoints(controlPlaneIps...),
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
			talosclient.WithEndpoints(controlPlaneIps...),
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
	machines []*vitistackcrdsv1alpha1.Machine,
	timeout time.Duration,
	interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	ips := []string{}
	for _, m := range machines {
		if len(m.Status.NetworkInterfaces) == 0 || len(m.Status.NetworkInterfaces[0].IPAddresses) == 0 {
			continue
		}
		ips = append(ips, m.Status.NetworkInterfaces[0].IPAddresses[0])
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
	endpoint string,
	timeout time.Duration,
	interval time.Duration) ([]byte, error) {
	deadline := time.Now().Add(timeout)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		tClient, err := talosclient.New(ctx, talosclient.WithConfig(clientCfg), talosclient.WithEndpoints(endpoint))
		if err != nil {
			return nil, fmt.Errorf("failed to create Talos client for kubeconfig: %w", err)
		}

		ctxWithNode := talosclient.WithNodes(ctx, endpoint)

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

		vlog.Info("Kubeconfig not ready yet, retrying: endpoint=" + endpoint)
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
