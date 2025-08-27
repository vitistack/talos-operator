package talos

import (
	"context"
	"fmt"
	"os"
	"time"

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
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// Machine phases
	MachinePhaseRunning  = "Running"
	MachinePhaseCreating = "Creating"
	MachinePhasePending  = "Pending"
	MachinePhaseFailed   = "Failed"

	// Default polling interval for waiting for machines
	DefaultMachineCheckInterval = 30 * time.Second
	DefaultMachineTimeout       = 10 * time.Minute
)

type TalosManager struct {
	client.Client
	// Store cluster configuration state
	clusterConfigurations map[string]*TalosClusterConfig
}

// TalosClusterConfig holds the generated Talos configuration state
type TalosClusterConfig struct {
	ClusterName     string
	SecretGenerated bool
	MachineConfigs  map[string]string // machine name -> config (placeholder for now)
	Bootstrapped    bool
}

// NewTalosManager creates a new instance of TalosManager
func NewTalosManager(c client.Client) *TalosManager {
	return &TalosManager{
		Client:                c,
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
func (t *TalosManager) ReconcileTalosCluster(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster) error {
	log := ctrl.LoggerFrom(ctx)

	log.Info("Starting Talos cluster reconciliation", "cluster", cluster.Name)

	// Get machines associated with this cluster
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

	// Extract machine information needed for Talos
	//machineInfos := t.extractMachineInfos(readyMachines)

	// Fetch disk info and install talos on disk
	// if err := t.installTalosOnDisk(ctx, machineInfos); err != nil {
	// 	return fmt.Errorf("failed to install Talos on disk: %w", err)
	// }

	// Generate Talos configuration
	clientConfig, err := t.generateTalosConfig(ctx, cluster, readyMachines)
	if err != nil {
		return fmt.Errorf("failed to generate Talos config: %w", err)
	}

	log.Info("Generated Talos client config", "cluster", cluster.Name, "hasConfig", clientConfig != nil)

	// collect control plane and worker IPs from ready machines
	controlPlanes := filterMachinesByRole(readyMachines, "control-plane")

	// get other nodes except control plane
	workers := []*vitistackcrdsv1alpha1.Machine{}
	for _, machine := range readyMachines {
		role, ok := machine.Labels["role"]
		if !ok || role != "control-plane" {
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

	err = applyTalosConfigurationToControlPlanes(ctx, clientConfig, controlPlaneIPs)
	if err != nil {
		return err
	}

	err = applyTalosConfigurationToWorkers(ctx, clientConfig, workerIPs)
	if err != nil {
		return err
	}

	// todo registert VIP ip addresses to NAM
	// write to a crd, so others can handle load balancing of control planes

	// Bootstrap the cluster (bootstrap exactly one control-plane)
	if len(controlPlaneIPs) > 0 {
		if err := bootstrapTalosControlPlane(ctx, clientConfig, controlPlaneIPs[0]); err != nil {
			return fmt.Errorf("failed to bootstrap Talos cluster: %w", err)
		}
	}

	// Get Kubernetes access (fetch kubeconfig and store it as a Secret)
	if len(controlPlaneIPs) > 0 {
		kubeconfigBytes, err := getKubeconfigWithRetry(ctx, clientConfig, controlPlaneIPs[0], 5*time.Minute, 10*time.Second)
		if err != nil {
			return fmt.Errorf("failed to get kubeconfig: %w", err)
		}

		if err := t.upsertKubeconfigSecret(ctx, cluster, kubeconfigBytes); err != nil {
			return fmt.Errorf("failed to upsert kubeconfig Secret: %w", err)
		}

		log.Info("Kubeconfig stored in Secret", "secret", kubeconfigSecretName(cluster))
	}

	//log.Info("Successfully reconciled Talos cluster", "cluster", cluster.Name)
	return nil
}

func applyTalosConfigurationToControlPlanes(ctx context.Context, clientConfig *clientconfig.Config, controlPlaneIps []string) error {
	//log := ctrl.LoggerFrom(ctx)
	tClient, err := talosclient.New(ctx, talosclient.WithConfig(clientConfig), talosclient.WithEndpoints(controlPlaneIps...))
	if err != nil {
		return fmt.Errorf("failed to create Talos client: %w", err)
	}
	// target control plane nodes explicitly via context
	ctxWithNodes := talosclient.WithNodes(ctx, controlPlaneIps...)
	return applyConfiguration(ctxWithNodes, clientConfig, tClient)
}

func applyTalosConfigurationToWorkers(ctx context.Context, clientConfig *clientconfig.Config, workerIps []string) error {
	//log := ctrl.LoggerFrom(ctx)
	// connect directly to worker endpoints
	tClient, err := talosclient.New(ctx, talosclient.WithConfig(clientConfig), talosclient.WithEndpoints(workerIps...))
	if err != nil {
		return fmt.Errorf("failed to create Talos client: %w", err)
	}
	// and set target nodes via context
	ctxWithNodes := talosclient.WithNodes(ctx, workerIps...)
	return applyConfiguration(ctxWithNodes, clientConfig, tClient)
}

func applyConfiguration(ctx context.Context, clientConfig *clientconfig.Config, tClient *talosclient.Client) error {
	log := ctrl.LoggerFrom(ctx)
	clientConfigInBytes, err := clientConfig.Bytes()
	if err != nil {
		return fmt.Errorf("failed to serialize Talos client config: %w", err)
	}
	resp, err := tClient.ApplyConfiguration(ctx, &machineapi.ApplyConfigurationRequest{
		Data:           clientConfigInBytes,
		Mode:           machineapi.ApplyConfigurationRequest_AUTO,
		DryRun:         false,
		TryModeTimeout: durationpb.New(time.Duration(2 * time.Minute)),
	})
	if err != nil {
		return fmt.Errorf("error applying new configuration: %s", err)
	}

	log.Info("Talos apply configuration response", "messages", resp.Messages)
	return nil
}

// bootstrapTalosControlPlane bootstraps the cluster against a single control plane node.
func bootstrapTalosControlPlane(ctx context.Context, clientCfg *clientconfig.Config, controlPlaneIP string) error {
	log := ctrl.LoggerFrom(ctx)

	// Create a client pointing to the control plane endpoint
	tClient, err := talosclient.New(ctx, talosclient.WithConfig(clientCfg), talosclient.WithEndpoints(controlPlaneIP))
	if err != nil {
		return fmt.Errorf("failed to create Talos client for bootstrap: %w", err)
	}

	// Target the bootstrap node via context
	ctxWithNode := talosclient.WithNodes(ctx, controlPlaneIP)

	// Perform bootstrap
	if err := tClient.Bootstrap(ctxWithNode, &machineapi.BootstrapRequest{}); err != nil {
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
	for _, disk := range m.Status.Disks {
		if disk.PVCName != "" && disk.Device != "" {
			hasDisk = true
			break
		}
	}

	for _, iface := range m.Status.NetworkInterfaces {
		if iface.MACAddress != "" && (len(iface.IPAddresses) > 0 || len(iface.IPv6Addresses) > 0) {
			hasNetwork = true
			break
		}
	}

	return hasNetwork && hasDisk
}

// extractMachineInfos creates MachineInfo structs from ready machines
// func (t *TalosManager) extractMachineInfos(machines []*vitistackcrdsv1alpha1.Machine) []MachineInfo {
// 	var machineInfos []MachineInfo

// 	for _, machine := range machines {
// 		// Determine role from labels
// 		role := "worker"
// 		if machineRole, exists := machine.Labels["cluster.vitistack.io/role"]; exists {
// 			role = machineRole
// 		}

// 		// Get primary IP address (prefer private, fallback to public)
// 		var ip string
// 		if len(machine.Status.PrivateIPAddresses) > 0 {
// 			ip = machine.Status.PrivateIPAddresses[0]
// 		} else if len(machine.Status.PublicIPAddresses) > 0 {
// 			ip = machine.Status.PublicIPAddresses[0]
// 		}

// 		if ip != "" {
// 			machineInfos = append(machineInfos, MachineInfo{
// 				Name:    machine.Name,
// 				Role:    role,
// 				IP:      ip,
// 				Machine: machine,
// 			})
// 		}
// 	}

// 	return machineInfos
// }

// generateTalosConfig generates Talos configuration for the cluster
func (t *TalosManager) generateTalosConfig(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster, machines []*vitistackcrdsv1alpha1.Machine) (*clientconfig.Config, error) {
	log := ctrl.LoggerFrom(ctx)
	controlPlanes := filterMachinesByRole(machines, "control-plane")
	//workers := filterMachinesByRole(machines, "worker")

	firstCP := controlPlanes[0]
	if firstCP == nil {
		return nil, fmt.Errorf("no control plane found for cluster %s", cluster.Name)
	}

	clusterName := cluster.Name
	controlPlaneEndpoint := fmt.Sprintf("https://%s:6443", firstCP.Status.NetworkInterfaces[0].IPAddresses[0]) // Using first control plane's private IP

	// * Kubernetes version to install, using the latest here
	kubernetesVersion := constants.DefaultKubernetesVersion

	// * version contract defines the version of the Talos cluster configuration is generated for
	//   generate package can generate machine configuration compatible with current and previous versions of Talos
	//targetVersion := config.TalosVersionCurrent // := "v1.0"

	// parse the version contract
	// var (
	// 	versionContract = config.TalosVersionCurrent //nolint:wastedassign,ineffassign // version of the Talos machinery package
	// 	err             error
	// )

	// versionContract, err := config.ParseContractFromVersion(targetVersion)
	// if err != nil {
	// 	log.Error(err, "failed to parse version contract: %s", err)
	// }

	versionContract := config.TalosVersionCurrent
	// generate the cluster-wide secrets once and use it for every node machine configuration
	// secrets can be stashed for future use by marshaling the structure to YAML or JSON
	secretsBundle, err := secrets.NewBundle(secrets.NewFixedClock(time.Now()), versionContract)
	if err != nil {
		log.Error(err, "failed to generate secrets bundle: %s", err)
	}
	endpointlist := []string{}
	for _, cp := range controlPlanes {
		endpointlist = append(endpointlist, cp.Status.NetworkInterfaces[0].IPAddresses[0])
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

	// generate the machine config for each node of the cluster using the secrets
	for _, node := range machines {
		var cfg config.Provider

		// generate the machine config for the node, using the right machine type:
		// * machine.TypeConrolPlane for control plane nodes
		// * machine.TypeWorker for worker nodes
		cfg, err = input.Config(machine.TypeControlPlane)
		if err != nil {
			log.Error(err, "failed to generate config for node %q: %s", node, err)
		}

		// config can be tweaked at this point to add machine-specific configuration, e.g.:
		cfg.RawV1Alpha1().MachineConfig.MachineInstall.InstallDisk = "/dev/sdb"

		// marshal the config to YAML
		var marshaledCfg []byte

		marshaledCfg, err = cfg.Bytes()
		if err != nil {
			log.Error(err, "failed to generate config for node %q: %s", node, err)
		}

		// write the config to a file
		if err = os.WriteFile(clusterName+"-"+node.Name+".yaml", marshaledCfg, 0o600); err != nil {
			log.Error(err, "failed to write config for node %q: %s", node, err)
		}
	}

	// generate the client Talos configuration (for API access, e.g. talosctl)
	clientCfg, err := input.Talosconfig()
	if err != nil {
		log.Error(err, "failed to generate client config: %s", err)
	}

	if err = clientCfg.Save(clusterName + "-talosconfig"); err != nil {
		log.Error(err, "failed to save client config: %s", err)
	}

	return clientCfg, nil
}

// (legacy commented bootstrap function removed)

// filterMachinesByRole filters machines by their role
func filterMachinesByRole(machines []*vitistackcrdsv1alpha1.Machine, role string) []*vitistackcrdsv1alpha1.Machine {
	var filtered []*vitistackcrdsv1alpha1.Machine
	for _, machine := range machines {
		if machine.Labels["role"] == role {
			filtered = append(filtered, machine)
		}
	}
	return filtered
}
