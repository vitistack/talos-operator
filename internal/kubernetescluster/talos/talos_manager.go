package talos

import (
	"context"
	"fmt"
	"time"

	"crypto/tls"

	"github.com/NorskHelsenett/ror/pkg/helpers/stringhelper"
	vitistackcrdsv1alpha1 "github.com/vitistack/crds/pkg/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/siderolabs/talos/pkg/machinery/constants"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// Machine phases
	MachinePhaseRunning  = "Running"
	MachinePhaseCreating = "Creating"
	MachinePhasePending  = "Pending"
	MachinePhaseFailed   = "Failed"

	// Default polling interval for waiting for machines
	DefaultMachineCheckInterval = 10 * time.Second
	DefaultMachineTimeout       = 2 * time.Minute
)

type TalosManager struct {
	client.Client
	statusManager *status.StatusManager
	// Store cluster configuration state
	clusterConfigurations map[string]*TalosClusterConfig
}

// TalosClusterConfig holds the generated Talos configuration state
type TalosClusterConfig struct {
	ClusterName     string
	SecretGenerated bool
	MachineConfigs  map[string][]byte // machine name -> machine config YAML
	Bootstrapped    bool
}

// NewTalosManager creates a new instance of TalosManager
func NewTalosManager(c client.Client, statusManager *status.StatusManager) *TalosManager {
	return &TalosManager{
		Client:                c,
		statusManager:         statusManager,
		clusterConfigurations: make(map[string]*TalosClusterConfig),
	}
}

// MachineInfo holds information about a machine needed for Talos cluster creation
type MachineInfo struct {
	Name    string
	Role    string // "control-plane" or "worker"
	IP      string
	Machine *vitistackcrdsv1alpha1.Machine
}

// ReconcileTalosCluster waits for machines to be ready and creates a Talos cluster
// nolint:gocyclo // Reconcile flow is linear; refactor will be done separately.
func (t *TalosManager) ReconcileTalosCluster(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster) error {
	log := ctrl.LoggerFrom(ctx)

	log.Info("Starting Talos cluster reconciliation", "cluster", cluster.Name)

	talosConfig, ok := t.clusterConfigurations[cluster.Name]
	if !ok {
		err := createTalosCluster(ctx, t, cluster)
		if err != nil {
			return err
		}
	} else {
		// todo
		// update talos cluster
		log.Info("Talos cluster already exists", "cluster", talosConfig.ClusterName, "bootstrapped", talosConfig.Bootstrapped)
	}

	err := t.statusManager.UpdateKubernetesClusterStatus(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to update Kubernetes cluster status: %w", err)
	}

	return nil
}

func createTalosCluster(ctx context.Context, t *TalosManager, cluster *vitistackcrdsv1alpha1.KubernetesCluster) error {
	log := ctrl.LoggerFrom(ctx)
	machines, err := t.getClusterMachines(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get cluster machines: %w", err)
	}

	if len(machines) == 0 {
		log.Info("No machines found for cluster, skipping Talos reconciliation", "cluster", cluster.Name)
		return nil
	}

	// Wait for all machines to be in running state
	readyMachines, err := t.waitForMachinesReady(ctx, machines)
	if err != nil {
		return fmt.Errorf("failed waiting for machines to be ready: %w", err)
	}

	// collect control plane and worker IPs from ready machines
	controlPlanes := filterMachinesByRole(readyMachines, "control-plane")

	// get other nodes except control plane
	workers := []*vitistackcrdsv1alpha1.Machine{}
	for _, machine := range readyMachines {
		role := machine.Labels["cluster.vitistack.io/role"]
		if role != "control-plane" {
			workers = append(workers, machine)
		}
	}

	var controlPlaneIPs []string
	for _, m := range controlPlanes {
		if len(m.Status.NetworkInterfaces) > 0 && len(m.Status.NetworkInterfaces[0].IPAddresses) > 0 {
			controlPlaneIPs = append(controlPlaneIPs, m.Status.NetworkInterfaces[0].IPAddresses[0])
		}
	}

	var workerIPs []string
	for _, m := range workers {
		if len(m.Status.NetworkInterfaces) > 0 && len(m.Status.NetworkInterfaces[0].IPAddresses) > 0 {
			workerIPs = append(workerIPs, m.Status.NetworkInterfaces[0].IPAddresses[0])
		}
	}

	if len(controlPlaneIPs) != len(controlPlanes) {
		return fmt.Errorf("control planes not ready quite yet, missing ip adresses before applying configuration")
	}

	if len(workerIPs) != len(workers) {
		return fmt.Errorf("workers not ready quite yet, missing ip adresses before applying configuration")
	}

	endpointIP := controlPlaneIPs[0]

	// Generate Talos configuration
	clientConfig, err := t.generateTalosConfig(ctx, cluster, readyMachines, endpointIP)
	if err != nil {
		return fmt.Errorf("failed to generate Talos config: %w", err)
	}

	log.Info("Generated Talos client config", "cluster", cluster.Name, "hasConfig", clientConfig != nil)

	talosClient, err := createTalosClient(ctx, true, clientConfig, controlPlaneIPs)
	if err != nil {
		return fmt.Errorf("failed to create Talos client: %w", err)
	}

	//err = t.applyTalosConfigurationToControlPlanes(ctx, cluster, clientConfig, controlPlaneIPs, controlPlanes, true)
	err = t.applyPerNodeConfiguration(ctx, cluster, clientConfig, talosClient, controlPlanes)
	if err != nil {
		return err
	}

	err = t.applyPerNodeConfiguration(ctx, cluster, clientConfig, talosClient, workers)
	if err != nil {
		return err
	}

	// todo registert VIP ip addresses to create loadbalancers for talos control planes
	// write to a crd, so others can handle load balancing of control planes

	// Bootstrap the cluster (bootstrap exactly one control-plane)
	if len(controlPlaneIPs) > 0 {
		if err := t.bootstrapTalosControlPlane(ctx, talosClient, endpointIP); err != nil {
			return fmt.Errorf("failed to bootstrap Talos cluster: %w", err)
		}
	}

	// Get Kubernetes access (fetch kubeconfig and store it as a Secret)
	if len(controlPlaneIPs) > 0 {
		kubeconfigBytes, err := getKubeconfigWithRetry(ctx, clientConfig, endpointIP, 5*time.Minute, 10*time.Second)
		if err != nil {
			return fmt.Errorf("failed to get kubeconfig: %w", err)
		}

		if err := t.upsertKubeconfigSecret(ctx, cluster, kubeconfigBytes); err != nil {
			return fmt.Errorf("failed to upsert kubeconfig Secret: %w", err)
		}

		log.Info("Kubeconfig stored in Secret", "secret", kubeconfigSecretName(cluster))
	}
	return nil
}

func createTalosClient(ctx context.Context, insecure bool, clientConfig *clientconfig.Config, controlPlaneIps []string) (*talosclient.Client, error) {
	var tClient *talosclient.Client
	var err error
	if !insecure {
		tClient, err = talosclient.New(ctx, talosclient.WithConfig(clientConfig), talosclient.WithEndpoints(controlPlaneIps...))
		if err != nil {
			return nil, fmt.Errorf("failed to create Talos client: %w", err)
		}
	} else {
		tClient, err = talosclient.New(ctx,
			talosclient.WithTLSConfig(&tls.Config{InsecureSkipVerify: true}), // #nosec G402, need the insecure skip verify when the cluster is totally new
			talosclient.WithEndpoints(controlPlaneIps...),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create Talos client: %w", err)
		}
	}
	return tClient, nil
}

func (t *TalosManager) applyPerNodeConfiguration(ctx context.Context,
	cluster *vitistackcrdsv1alpha1.KubernetesCluster,
	clientConfig *clientconfig.Config,
	clientTalos *talosclient.Client,
	machines []*vitistackcrdsv1alpha1.Machine) error {
	log := ctrl.LoggerFrom(ctx)

	clusterCfg := t.clusterConfigurations[cluster.Name]
	if clusterCfg == nil {
		return fmt.Errorf("no cached Talos cluster config for %s", cluster.Name)
	}

	for _, m := range machines {
		if len(m.Status.NetworkInterfaces) == 0 || len(m.Status.NetworkInterfaces[0].IPAddresses) == 0 {
			continue
		}
		ip := m.Status.NetworkInterfaces[0].IPAddresses[0]
		cfgBytes, ok := clusterCfg.MachineConfigs[m.Name]
		if !ok || len(cfgBytes) == 0 {
			return fmt.Errorf("no machine config cached for node %s", m.Name)
		}
		nodeCtx := talosclient.WithNodes(ctx, ip)
		resp, err := clientTalos.ApplyConfiguration(nodeCtx, &machineapi.ApplyConfigurationRequest{
			Data:           cfgBytes,
			Mode:           machineapi.ApplyConfigurationRequest_AUTO,
			DryRun:         false,
			TryModeTimeout: durationpb.New(2 * time.Minute),
		})
		if err != nil {
			return fmt.Errorf("error applying configuration to node %s: %w", ip, err)
		}
		log.Info("Talos apply configuration response", "node", ip, "messages", resp.Messages)
	}
	return nil
}

// isTLSError makes a best-effort check if an error likely stems from TLS handshake/verification issues.
// The Talos client returns gRPC errors with wrapped messages; inspect error string for common TLS substrings.
// func isTLSError(err error) bool {
// 	if err == nil {
// 		return false
// 	}
// 	msg := err.Error()
// 	// Common substrings for TLS handshake/verification failures across Go/grpc
// 	return strings.Contains(msg, "tls: ") ||
// 		(strings.Contains(msg, "certificate") && strings.Contains(msg, "verify")) ||
// 		strings.Contains(msg, "x509:") ||
// 		strings.Contains(msg, "handshake failure")
// }

// bootstrapTalosControlPlane bootstraps the cluster against a single control plane node.
func (t *TalosManager) bootstrapTalosControlPlane(ctx context.Context, tClient *talosclient.Client, controlPlaneIP string) error {
	log := ctrl.LoggerFrom(ctx)

	// Perform bootstrap
	if err := tClient.Bootstrap(ctx, &machineapi.BootstrapRequest{}); err != nil {
		return fmt.Errorf("talos bootstrap failed: %w", err)
	}

	log.Info("Talos bootstrap initiated", "node", controlPlaneIP)
	return nil
}

// getKubeconfigWithRetry tries to fetch kubeconfig until timeout, waiting interval between tries.
func getKubeconfigWithRetry(ctx context.Context, clientCfg *clientconfig.Config, endpoint string, timeout time.Duration, interval time.Duration) ([]byte, error) {
	log := ctrl.LoggerFrom(ctx)
	deadline := time.Now().Add(timeout)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Create client pointing to one control-plane endpoint
		tClient, err := talosclient.New(ctx, talosclient.WithConfig(clientCfg), talosclient.WithEndpoints(endpoint))
		if err != nil {
			return nil, fmt.Errorf("failed to create Talos client for kubeconfig: %w", err)
		}

		// Target the same node via context (not strictly required for kubeconfig but consistent)
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

		log.Info("Kubeconfig not ready yet, retrying", "endpoint", endpoint)
		time.Sleep(interval)
	}
}

// upsertKubeconfigSecret creates or updates a Secret with kubeconfig content.
func (t *TalosManager) upsertKubeconfigSecret(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster, kubeconfig []byte) error {
	name := kubeconfigSecretName(cluster)

	secret := &corev1.Secret{}
	err := t.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret)
	if err != nil {
		// create new secret
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: cluster.Namespace,
				Labels: map[string]string{
					"cluster.vitistack.io/cluster-name": cluster.Name,
				},
			},
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				"kubeconfig": kubeconfig,
			},
		}
		return t.Create(ctx, secret)
	}

	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	secret.Data["kubeconfig"] = kubeconfig
	return t.Update(ctx, secret)
}

func kubeconfigSecretName(cluster *vitistackcrdsv1alpha1.KubernetesCluster) string {
	return fmt.Sprintf("%s-kubeconfig", cluster.Name)
}

// func (t *TalosManager) installTalosOnDisk(ctx context.Context, machineInfos []MachineInfo) error {

// 	return nil
// }

// getClusterMachines retrieves all machines associated with the cluster
func (t *TalosManager) getClusterMachines(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster) ([]*vitistackcrdsv1alpha1.Machine, error) {
	machineList := &vitistackcrdsv1alpha1.MachineList{}
	listOpts := []client.ListOption{
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"cluster.vitistack.io/cluster-name": cluster.Name},
	}

	if err := t.List(ctx, machineList, listOpts...); err != nil {
		return nil, fmt.Errorf("failed to list machines: %w", err)
	}

	machines := make([]*vitistackcrdsv1alpha1.Machine, len(machineList.Items))
	for i := range machineList.Items {
		machines[i] = &machineList.Items[i]
	}

	return machines, nil
}

// waitForMachinesReady waits for all machines to be in running state with IP addresses
func (t *TalosManager) waitForMachinesReady(ctx context.Context, machines []*vitistackcrdsv1alpha1.Machine) ([]*vitistackcrdsv1alpha1.Machine, error) {
	log := ctrl.LoggerFrom(ctx)

	timeout := time.After(DefaultMachineTimeout)
	ticker := time.NewTicker(DefaultMachineCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			return nil, fmt.Errorf("timeout waiting for machines to be ready")
		case <-ticker.C:
			readyMachines, allReady := t.checkMachinesReady(ctx, machines)
			if allReady {
				log.Info("All machines are ready", "count", len(readyMachines))
				return readyMachines, nil
			}
			log.Info("Waiting for machines to be ready", "ready", len(readyMachines), "total", len(machines))
		}
	}
}

// checkMachinesReady checks if all machines are in running state with IP addresses
func (t *TalosManager) checkMachinesReady(ctx context.Context, machines []*vitistackcrdsv1alpha1.Machine) ([]*vitistackcrdsv1alpha1.Machine, bool) {
	var readyMachines []*vitistackcrdsv1alpha1.Machine

	for _, machine := range machines {
		// Refresh machine status
		updatedMachine := &vitistackcrdsv1alpha1.Machine{}
		if err := t.Get(ctx, types.NamespacedName{
			Name:      machine.Name,
			Namespace: machine.Namespace,
		}, updatedMachine); err != nil {
			continue // Skip this machine if we can't fetch it
		}

		// Check if machine is in running state and has IP addresses
		if t.isMachineReady(updatedMachine) {
			readyMachines = append(readyMachines, updatedMachine)
		}
	}

	return readyMachines, len(readyMachines) == len(machines)
}

// isMachineReady checks if a machine is ready for Talos cluster creation
func (t *TalosManager) isMachineReady(m *vitistackcrdsv1alpha1.Machine) bool {
	// Check if machine is in running state
	if m.Status.Phase != MachinePhaseRunning {
		return false
	}

	hasNetwork := false
	hasDisk := false
	for i := range m.Status.Disks {
		disk := &m.Status.Disks[i]
		if disk.PVCName != "" && disk.Device != "" {
			hasDisk = true
			break
		}
	}

	for i := range m.Status.NetworkInterfaces {
		iface := &m.Status.NetworkInterfaces[i]
		if iface.MACAddress != "" && (len(iface.IPAddresses) > 0 || len(iface.IPv6Addresses) > 0) {
			hasNetwork = true
			break
		}
	}

	return hasNetwork && hasDisk
}

// generateTalosConfig generates Talos configuration for the cluster
func (t *TalosManager) generateTalosConfig(ctx context.Context,
	cluster *vitistackcrdsv1alpha1.KubernetesCluster,
	machines []*vitistackcrdsv1alpha1.Machine,
	endpointIP string) (*clientconfig.Config, error) {
	log := ctrl.LoggerFrom(ctx)

	controlPlanes := filterMachinesByRole(machines, "control-plane")

	clusterName := cluster.Name
	controlPlaneEndpoint := fmt.Sprintf("https://%s:6443", endpointIP) // Using first control plane's private IP

	// * Kubernetes version to install, using the latest here
	kubernetesVersion := constants.DefaultKubernetesVersion
	versionContract := config.TalosVersionCurrent
	// generate the cluster-wide secrets once and use it for every node machine configuration
	// secrets can be stashed for future use by marshaling the structure to YAML or JSON
	secretsBundle, err := secrets.NewBundle(secrets.NewFixedClock(time.Now()), versionContract)
	if err != nil {
		log.Error(err, "failed to generate secrets bundle: %s", err)
	}

	endpointlist := []string{}
	for _, cp := range controlPlanes {
		if len(cp.Status.NetworkInterfaces) > 0 && len(cp.Status.NetworkInterfaces[0].IPAddresses) > 0 {
			endpointlist = append(endpointlist, cp.Status.NetworkInterfaces[0].IPAddresses[0])
		}
	}

	input, err := generate.NewInput(clusterName, controlPlaneEndpoint, kubernetesVersion,
		generate.WithVersionContract(versionContract),
		generate.WithSecretsBundle(secretsBundle),
		generate.WithEndpointList(
			endpointlist),
		// there are many more generate options available which allow to tweak generated config programmatically
	)
	if err != nil {
		log.Error(err, "failed to generate input: %s", err)
	}

	// Prepare or update cached cluster configuration holder
	clusterCfg := &TalosClusterConfig{
		ClusterName:     clusterName,
		SecretGenerated: true,
		MachineConfigs:  make(map[string][]byte),
	}

	// generate the machine config for each node of the cluster using the secrets and keep in-memory
	for _, node := range machines {
		var prov config.Provider
		mType := machine.TypeWorker
		if node.Labels["cluster.vitistack.io/role"] == "control-plane" {
			mType = machine.TypeControlPlane
		}
		prov, err = input.Config(mType)
		if err != nil {
			log.Error(err, "failed to generate config for node %q: %s", node.Name, err)
			continue
		}
		// TODO: Pull desired install disk from Machine spec
		prov.RawV1Alpha1().MachineConfig.MachineInstall.InstallDisk = "/dev/sdb"

		stringhelper.PrettyprintStruct(prov)

		marshaledCfg, err := prov.Bytes()
		if err != nil {
			log.Error(err, "failed to serialize config for node %q: %s", node.Name, err)
			continue
		}
		clusterCfg.MachineConfigs[node.Name] = marshaledCfg
	}

	// cache
	t.clusterConfigurations[clusterName] = clusterCfg

	// generate the client Talos configuration (for API access, e.g. talosctl)
	clientCfg, err := input.Talosconfig()
	if err != nil {
		log.Error(err, "failed to generate client config: %s", err)
	}

	// Optionally persist client config can be added later; avoid writing on controller FS.

	return clientCfg, nil
}

// (legacy commented bootstrap function removed)

// filterMachinesByRole filters machines by their role
func filterMachinesByRole(machines []*vitistackcrdsv1alpha1.Machine, role string) []*vitistackcrdsv1alpha1.Machine {
	var filtered []*vitistackcrdsv1alpha1.Machine
	for _, machine := range machines {
		if machine.Labels["cluster.vitistack.io/role"] == role {
			filtered = append(filtered, machine)
		}
	}
	return filtered
}
