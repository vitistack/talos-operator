package talos

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"crypto/tls"
	"net"

	"github.com/vitistack/common/pkg/loggers/vlog"
	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/siderolabs/talos/pkg/machinery/constants"
	"github.com/spf13/viper"
	"github.com/vitistack/talos-operator/internal/kubernetescluster/status"
	"github.com/vitistack/talos-operator/pkg/consts"
	"google.golang.org/protobuf/types/known/durationpb"
	yaml "gopkg.in/yaml.v3"
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

const trueStr = "true"
const falseStr = "false"

type TalosManager struct {
	client.Client
	statusManager *status.StatusManager
}

// NewTalosManager creates a new instance of TalosManager
func NewTalosManager(c client.Client, statusManager *status.StatusManager) *TalosManager {
	return &TalosManager{
		Client:        c,
		statusManager: statusManager,
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
	err := initializeTalosCluster(ctx, t, cluster)
	// Always update status based on current persisted flags/conditions
	if sErr := t.statusManager.UpdateKubernetesClusterStatus(ctx, cluster); sErr != nil {
		vlog.Error("failed to update Kubernetes cluster status", sErr)
	}
	return err
}

// nolint:gocognit,gocyclo,funlen // flow is linear and will be refactored later
func initializeTalosCluster(ctx context.Context, t *TalosManager, cluster *vitistackcrdsv1alpha1.KubernetesCluster) error {
	// Short-circuit: if the cluster is already initialized per persisted secret flags, skip init
	if flags, err := t.getTalosSecretFlags(ctx, cluster); err == nil {
		if flags.ControlPlaneApplied && flags.WorkerApplied && flags.Bootstrapped && flags.ClusterAccess {
			return nil
		}
	}
	machines, err := t.getClusterMachines(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to get cluster machines: %w", err)
	}

	if len(machines) == 0 {
		vlog.Info("No machines found for cluster, skipping Talos reconciliation: cluster=" + cluster.Name)
		_ = t.statusManager.SetPhase(ctx, cluster, "Pending")
		_ = t.statusManager.SetCondition(ctx, cluster, "MachinesDiscovered", "False", "NoMachines", "No machines found yet for this cluster")
		return nil
	}

	// Wait for all machines to be in running state
	// _ = t.statusManager.SetPhase(ctx, cluster, "WaitingForMachines")
	_ = t.statusManager.SetCondition(ctx, cluster, "MachinesReady", "False", "Waiting", "Waiting for machines to be running with IP addresses")
	readyMachines, err := t.waitForMachinesReady(ctx, machines)
	if err != nil {
		_ = t.statusManager.SetCondition(ctx, cluster, "MachinesReady", "False", "Timeout", err.Error())
		return fmt.Errorf("failed waiting for machines to be ready: %w", err)
	}
	_ = t.statusManager.SetCondition(ctx, cluster, "MachinesReady", "True", "Ready", "All machines are running and have IP addresses")

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
		ipv4Found := false
		if len(m.Status.NetworkInterfaces) > 0 {
			for _, ipAddr := range m.Status.NetworkInterfaces[0].IPAddresses {
				ip := net.ParseIP(ipAddr)
				if ip != nil && ip.To4() != nil {
					controlPlaneIPs = append(controlPlaneIPs, ipAddr)
					ipv4Found = true
					break
				}
			}
		}
		if !ipv4Found {
			_ = t.statusManager.SetCondition(ctx, cluster, "ControlPlaneIPsReady", "False", "NotReady", "Some control-plane nodes are missing IPv4 addresses")
			return fmt.Errorf("control planes not ready quite yet, missing IPv4 addresses before applying configuration")
		}
	}

	var workerIPs []string
	for _, m := range workers {
		ipv4Found := false
		if len(m.Status.NetworkInterfaces) > 0 {
			for _, ipAddr := range m.Status.NetworkInterfaces[0].IPAddresses {
				ip := net.ParseIP(ipAddr)
				if ip != nil && ip.To4() != nil {
					workerIPs = append(workerIPs, ipAddr)
					ipv4Found = true
					break
				}
			}
		}
		if !ipv4Found {
			_ = t.statusManager.SetCondition(ctx, cluster, "WorkerIPsReady", "False", "NotReady", "Some worker nodes are missing IPv4 addresses")
			return fmt.Errorf("workers not ready quite yet, missing IPv4 addresses before applying configuration")
		}
	}

	endpointIP := controlPlaneIPs[0]

	// Ensure the consolidated Secret exists early so flag reads/writes are consistent across restarts
	if err := t.ensureTalosSecretExists(ctx, cluster); err != nil {
		return fmt.Errorf("failed to ensure talos secret exists: %w", err)
	}

	// Prefer existing artifacts from Secret; generate only if missing
	clientConfig, fromSecret, err := t.loadTalosArtifacts(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to load talos artifacts: %w", err)
	}
	if !fromSecret {
		// Generate Talos configuration
		genClientCfg, cpYAML, wYAML, err := t.generateTalosConfig(cluster, readyMachines, endpointIP)
		if err != nil {
			return fmt.Errorf("failed to generate Talos config: %w", err)
		}
		vlog.Info(fmt.Sprintf("Generated Talos client config: cluster=%s hasConfig=%t", cluster.Name, genClientCfg != nil))
		// Update status phase: config generated
		_ = t.statusManager.SetPhase(ctx, cluster, "ConfigGenerated")
		_ = t.statusManager.SetCondition(ctx, cluster, "ConfigGenerated", "True", "Generated", "Talos client and role configs generated")
		// Persist initial Talos configs into a Secret (without kubeconfig yet)
		if err := t.upsertTalosClusterConfigSecretWithRoleYAML(ctx, cluster, genClientCfg, cpYAML, wYAML, nil /* kubeconfig */); err != nil {
			return fmt.Errorf("failed to persist Talos config secret: %w", err)
		}
		clientConfig = genClientCfg
	} else {
		vlog.Info("Loaded Talos artifacts from Secret: cluster=" + cluster.Name)
	}

	insecure := true // self signed certificates at freshly installed talos nodes
	// previously we created a shared insecure client; per-node clients are used instead for first apply

	// We'll create a secure Talos client later after nodes come back with generated certs
	// talosClientSecure will be established after readiness wait.

	// using per-node application for initial config; central apply path kept for potential future use
	flags, _ := t.getTalosSecretFlags(ctx, cluster)
	if !flags.ControlPlaneApplied {
		if err := t.applyPerNodeConfiguration(ctx, cluster, clientConfig, controlPlanes, insecure); err != nil {
			_ = t.statusManager.SetCondition(ctx, cluster, "ControlPlaneConfigApplied", "False", "ApplyError", err.Error())
			return err
		}
		_ = t.setTalosSecretFlags(ctx, cluster, map[string]bool{"controlplane_applied": true})
		_ = t.statusManager.SetCondition(ctx, cluster, "ControlPlaneConfigApplied", "True", "Applied", "Talos config applied to control planes")
	} else {
		vlog.Info("Control-plane config already applied, skipping: cluster=" + cluster.Name)
	}

	if !flags.WorkerApplied {
		if err := t.applyPerNodeConfiguration(ctx, cluster, clientConfig, workers, insecure); err != nil {
			_ = t.statusManager.SetCondition(ctx, cluster, "WorkerConfigApplied", "False", "ApplyError", err.Error())
			return err
		}
		_ = t.setTalosSecretFlags(ctx, cluster, map[string]bool{"worker_applied": true})
		_ = t.statusManager.SetCondition(ctx, cluster, "WorkerConfigApplied", "True", "Applied", "Talos config applied to workers")
	} else {
		vlog.Info("Worker config already applied, skipping: cluster=" + cluster.Name)
	}

	// Update status phase: configs applied
	_ = t.statusManager.SetPhase(ctx, cluster, "ConfigApplied")
	_ = t.statusManager.SetCondition(ctx, cluster, "ConfigApplied", "True", "Applied", "Talos configs applied to all nodes")

	// todo registert VIP ip addresses to create loadbalancers for talos control planes
	// write to a crd, so others can handle load balancing of control planes

	talosClientSecure, err := createTalosClient(ctx, false, clientConfig, controlPlaneIPs)
	if err != nil {
		return fmt.Errorf("failed to create secure Talos client: %w", err)
	}

	// Bootstrap the cluster (bootstrap exactly one control-plane)
	clusterState, _ := t.getTalosSecretFlags(ctx, cluster)
	_, hasKubeconfig, _ := t.getTalosSecretState(ctx, cluster)
	if len(controlPlaneIPs) > 0 && !clusterState.Bootstrapped {
		// Wait for control-plane nodes to be ready with their new config (Talos API reachable securely)
		_ = t.statusManager.SetPhase(ctx, cluster, "WaitingForTalosAPI")
		_ = t.statusManager.SetCondition(ctx, cluster, "WaitingForTalosAPI", "True", "Waiting", "Waiting for Talos API on control planes")
		if err := t.waitForTalosAPIs(cluster, clientConfig, controlPlanes, false, 10*time.Minute, 10*time.Second); err != nil {
			_ = t.statusManager.SetCondition(ctx, cluster, "TalosAPIReady", "False", "NotReady", err.Error())
			return fmt.Errorf("control planes not ready for bootstrap: %w", err)
		}
		_ = t.statusManager.SetPhase(ctx, cluster, "TalosAPIReady")
		_ = t.statusManager.SetCondition(ctx, cluster, "TalosAPIReady", "True", "Ready", "Talos API reachable on control planes")

		if err := t.bootstrapTalosControlPlaneWithRetry(ctx, talosClientSecure, endpointIP, 5*time.Minute, 10*time.Second); err != nil {
			_ = t.statusManager.SetCondition(ctx, cluster, "Bootstrapped", "False", "BootstrapError", err.Error())
			return fmt.Errorf("failed to bootstrap Talos cluster: %w", err)
		}
		_ = t.statusManager.SetPhase(ctx, cluster, "Bootstrapped")
		_ = t.statusManager.SetCondition(ctx, cluster, "Bootstrapped", "True", "Done", "Talos cluster bootstrapped")

		// mark bootstrapped in the persistent Secret
		_ = t.setTalosSecretFlags(ctx, cluster, map[string]bool{"bootstrapped": true})
	} else if clusterState.Bootstrapped {
		vlog.Info("Cluster already bootstrapped, skipping bootstrap step: cluster=" + cluster.Name)
	}

	// Get Kubernetes access (fetch kubeconfig and store it as a Secret)
	if len(controlPlaneIPs) > 0 && !clusterState.ClusterAccess {
		kubeconfigBytes, err := getKubeconfigWithRetry(ctx, clientConfig, endpointIP, 5*time.Minute, 10*time.Second)
		if err != nil {
			return fmt.Errorf("failed to get kubeconfig: %w", err)
		}

		// Update consolidated Talos secret with kubeconfig and bootstrapped flag
		if err := t.upsertTalosClusterConfigSecret(ctx, cluster, clientConfig, kubeconfigBytes); err != nil {
			_ = t.statusManager.SetCondition(ctx, cluster, "KubeconfigAvailable", "False", "PersistError", err.Error())
			return fmt.Errorf("failed to update Talos config secret with kubeconfig: %w", err)
		}
		_ = t.setTalosSecretFlags(ctx, cluster, map[string]bool{"cluster_access": true})
		_ = t.statusManager.SetPhase(ctx, cluster, "Ready")
		_ = t.statusManager.SetCondition(ctx, cluster, "KubeconfigAvailable", "True", "Persisted", "Kubeconfig stored in Secret")

		if !hasKubeconfig {
			vlog.Info("Kubeconfig stored in Secret: secret=" + fmt.Sprintf("k8s-%s", cluster.Name))
		}
	} else if clusterState.ClusterAccess {
		vlog.Info("Cluster access already established (kubeconfig present), skipping fetch: cluster=" + cluster.Name)
	}
	return nil
}

// ensureTalosSecretExists creates the consolidated Secret if it does not exist yet with default flags
func (t *TalosManager) ensureTalosSecretExists(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster) error {
	name := fmt.Sprintf("k8s-%s", cluster.Name)
	secret := &corev1.Secret{}
	err := t.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	// create minimal secret with default flags
	data := map[string][]byte{
		"bootstrapped":              []byte(falseStr),
		"talosconfig_present":       []byte(falseStr),
		"controlplane_yaml_present": []byte(falseStr),
		"worker_yaml_present":       []byte(falseStr),
		"kubeconfig_present":        []byte(falseStr),
		"controlplane_applied":      []byte(falseStr),
		"worker_applied":            []byte(falseStr),
		"cluster_access":            []byte(falseStr),
	}
	secret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"cluster.vitistack.io/cluster-name": cluster.Name,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}
	return t.Create(ctx, secret)
}

// getTalosSecretState returns persisted state flags from the cluster's consolidated Secret.
func (t *TalosManager) getTalosSecretState(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster) (bootstrapped bool, hasKubeconfig bool, err error) {
	name := fmt.Sprintf("k8s-%s", cluster.Name)
	secret := &corev1.Secret{}
	if e := t.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret); e != nil {
		return false, false, e
	}
	if secret.Data == nil {
		return false, false, nil
	}
	if b, ok := secret.Data["bootstrapped"]; ok && string(b) == trueStr {
		bootstrapped = true
	}
	if k, ok := secret.Data["kube.config"]; ok && len(k) > 0 {
		hasKubeconfig = true
	}
	return bootstrapped, hasKubeconfig, nil
}

// loadTalosArtifacts attempts to read talosconfig (and ensures role templates exist) from the consolidated Secret.
// Returns fromSecret=true when talosconfig and both role templates are present.
func (t *TalosManager) loadTalosArtifacts(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster) (*clientconfig.Config, bool, error) {
	name := fmt.Sprintf("k8s-%s", cluster.Name)
	secret := &corev1.Secret{}
	if err := t.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret); err != nil {
		return nil, false, nil
	}
	if secret.Data == nil {
		return nil, false, nil
	}
	var cfg *clientconfig.Config
	if b, ok := secret.Data["talosconfig"]; ok && len(b) > 0 {
		// talosclient config is YAML; unmarshal back
		c := &clientconfig.Config{}
		if err := yaml.Unmarshal(b, c); err == nil {
			cfg = c
		} else {
			return nil, false, fmt.Errorf("failed to unmarshal talosconfig from secret: %w", err)
		}
	}
	cp := secret.Data["controlplane.yaml"]
	w := secret.Data["worker.yaml"]
	if cfg != nil && len(cp) > 0 && len(w) > 0 {
		return cfg, true, nil
	}
	return nil, false, nil
}

// talosSecretFlags captures persisted boolean flags stored in the consolidated Secret
type talosSecretFlags struct {
	ControlPlaneApplied bool
	WorkerApplied       bool
	Bootstrapped        bool
	ClusterAccess       bool
}

// getTalosSecretFlags reads boolean flags from the consolidated Secret
func (t *TalosManager) getTalosSecretFlags(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster) (talosSecretFlags, error) {
	name := fmt.Sprintf("k8s-%s", cluster.Name)
	secret := &corev1.Secret{}
	if err := t.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret); err != nil {
		return talosSecretFlags{}, err
	}
	flags := talosSecretFlags{}
	if secret.Data != nil {
		if b, ok := secret.Data["controlplane_applied"]; ok && string(b) == trueStr {
			flags.ControlPlaneApplied = true
		}
		if b, ok := secret.Data["worker_applied"]; ok && string(b) == trueStr {
			flags.WorkerApplied = true
		}
		if b, ok := secret.Data["bootstrapped"]; ok && string(b) == trueStr {
			flags.Bootstrapped = true
		}
		// prefer explicit cluster_access flag; otherwise infer from kubeconfig presence
		if b, ok := secret.Data["cluster_access"]; ok && string(b) == trueStr {
			flags.ClusterAccess = true
		} else if k, ok := secret.Data["kube.config"]; ok && len(k) > 0 {
			flags.ClusterAccess = true
		} else if k2, ok := secret.Data["kubeconfig_present"]; ok && string(k2) == trueStr {
			flags.ClusterAccess = true
		}
	}
	return flags, nil
}

// setTalosSecretFlags sets provided boolean flags in the consolidated Secret
func (t *TalosManager) setTalosSecretFlags(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster, updates map[string]bool) error {
	name := fmt.Sprintf("k8s-%s", cluster.Name)
	secret := &corev1.Secret{}
	if err := t.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret); err != nil {
		return err
	}
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	for k, v := range updates {
		if v {
			secret.Data[k] = []byte(trueStr)
		} else {
			secret.Data[k] = []byte(falseStr)
		}
	}
	return t.Update(ctx, secret)
}

func createTalosClient(ctx context.Context, insecure bool, clientConfig *clientconfig.Config, controlPlaneIps []string) (*talosclient.Client, error) {
	var tClient *talosclient.Client
	var err error
	if !insecure {
		tClient, err = talosclient.New(ctx,
			talosclient.WithConfig(clientConfig),
			talosclient.WithEndpoints(controlPlaneIps...),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create Talos client: %w", err)
		}
	} else {
		tClient, err = talosclient.New(ctx,
			talosclient.WithConfig(clientConfig),
			talosclient.WithTLSConfig(&tls.Config{InsecureSkipVerify: true}), // #nosec G402
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
	machines []*vitistackcrdsv1alpha1.Machine,
	insecure bool) error {
	// Load role templates from persistent Secret
	secretName := fmt.Sprintf("k8s-%s", cluster.Name)
	secret := &corev1.Secret{}
	if err := t.Get(ctx, types.NamespacedName{Name: secretName, Namespace: cluster.Namespace}, secret); err != nil {
		return fmt.Errorf("failed to get talos secret %s: %w", secretName, err)
	}
	cpTemplate := secret.Data["controlplane.yaml"]
	wTemplate := secret.Data["worker.yaml"]

	for _, m := range machines {
		if len(m.Status.NetworkInterfaces) == 0 || len(m.Status.NetworkInterfaces[0].IPAddresses) == 0 {
			continue
		}

		// Find first IPv4 address
		var ip string
		for _, ipAddr := range m.Status.NetworkInterfaces[0].IPAddresses {
			parsedIP := net.ParseIP(ipAddr)
			if parsedIP != nil && parsedIP.To4() != nil {
				ip = ipAddr
				break
			}
		}
		if ip == "" {
			continue // Skip if no IPv4 address found
		}

		// Choose role template
		var roleYAML []byte
		if m.Labels["cluster.vitistack.io/role"] == "control-plane" {
			roleYAML = cpTemplate
		} else {
			roleYAML = wTemplate
		}
		if len(roleYAML) == 0 {
			return fmt.Errorf("missing role template for node %s in secret %s", m.Name, secretName)
		}

		// Select install disk and prepare configuration
		installDisk := selectInstallDisk(m)
		patched, err := prepareNodeConfig(roleYAML, installDisk, m)
		if err != nil {
			return fmt.Errorf("failed to prepare config for node %s: %w", m.Name, err)
		}

		// Apply configuration to node
		if err := t.applyConfigToNode(ctx, insecure, clientConfig, ip, patched); err != nil {
			return fmt.Errorf("failed to apply config to node %s: %w", ip, err)
		}
	}
	return nil
}

// applyConfigToNode applies the Talos configuration to a specific node
func (t *TalosManager) applyConfigToNode(ctx context.Context, insecure bool, clientConfig *clientconfig.Config, nodeIP string, configData []byte) error {
	nodeClient, err := createTalosClient(ctx, insecure, clientConfig, []string{nodeIP})
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

// selectInstallDisk selects the appropriate install disk from machine status
func selectInstallDisk(m *vitistackcrdsv1alpha1.Machine) string {
	for i := range m.Status.Disks {
		d := m.Status.Disks[i]
		if d.Device != "" {
			if d.PVCName != "" { // prefer PVC-backed device
				return d.Device
			}
			// fallback to first available device
			if d.Device != "" {
				return d.Device
			}
		}
	}
	return "/dev/vda" // default fallback
}

// prepareNodeConfig prepares the Talos configuration for a specific node
func prepareNodeConfig(roleYAML []byte, installDisk string, m *vitistackcrdsv1alpha1.Machine) ([]byte, error) {
	// patch install disk into YAML
	patched, err := patchInstallDiskYAML(roleYAML, installDisk)
	if err != nil {
		return nil, fmt.Errorf("failed to patch install disk: %w", err)
	}

	// Check if machine is on a virtual provider and apply VM customizations
	if m.Status.Provider != "" && isVirtualMachineProvider(m.Status.Provider) {
		installImage := getVMInstallImage(m.Status.Provider)
		if installImage != "" {
			vlog.Info(fmt.Sprintf("Detected virtual machine provider %s for node %s, applying VM customizations with install image: %s", m.Status.Provider, m.Name, installImage))
		} else {
			vlog.Info(fmt.Sprintf("Detected virtual machine provider %s for node %s, applying VM customizations (no custom install image configured)", m.Status.Provider, m.Name))
		}
		patched, err = patchVirtualMachineCustomization(patched, installImage)
		if err != nil {
			return nil, fmt.Errorf("failed to patch VM customization: %w", err)
		}
	}

	return patched, nil
}

// patchInstallDiskYAML updates machine.install.disk in the Talos config YAML.
func patchInstallDiskYAML(in []byte, disk string) ([]byte, error) {
	var cfg map[string]interface{}
	if err := yaml.Unmarshal(in, &cfg); err != nil {
		return nil, err
	}
	m, _ := cfg["machine"].(map[string]interface{})
	if m == nil {
		m = map[string]interface{}{}
		cfg["machine"] = m
	}
	inst, _ := m["install"].(map[string]interface{})
	if inst == nil {
		inst = map[string]interface{}{}
		m["install"] = inst
	}
	inst["disk"] = disk
	return yaml.Marshal(cfg)
}

// isVirtualMachineProvider checks if the provider is a virtual machine type that needs special customization
func isVirtualMachineProvider(provider string) bool {
	provider = strings.ToLower(provider)
	virtualProviders := []string{"proxmox", "kubevirt", "libvirt", "kvm", "qemu", "vmware", "virtualbox", "hyper-v"}
	for _, vp := range virtualProviders {
		if strings.Contains(provider, vp) {
			return true
		}
	}
	return false
}

// getVMInstallImage returns the appropriate Talos install image for a given VM provider
// Images are fetched from viper configuration with provider-specific keys
func getVMInstallImage(provider string) string {
	provider = strings.ToLower(provider)

	// Map provider names to their config keys
	var configKey string
	switch {
	case strings.Contains(provider, "proxmox"):
		configKey = consts.TALOS_VM_INSTALL_IMAGE_PROXMOX
	case strings.Contains(provider, "kubevirt"):
		configKey = consts.TALOS_VM_INSTALL_IMAGE_KUBEVIRT
	case strings.Contains(provider, "libvirt"):
		configKey = consts.TALOS_VM_INSTALL_IMAGE_LIBVIRT
	case strings.Contains(provider, "kvm"):
		configKey = consts.TALOS_VM_INSTALL_IMAGE_KVM
	case strings.Contains(provider, "vmware"):
		configKey = consts.TALOS_VM_INSTALL_IMAGE_VMWARE
	case strings.Contains(provider, "virtualbox"):
		configKey = consts.TALOS_VM_INSTALL_IMAGE_VIRTUALBOX
	case strings.Contains(provider, "hyper-v"):
		configKey = consts.TALOS_VM_INSTALL_IMAGE_HYPERV
	default:
		configKey = consts.TALOS_VM_INSTALL_IMAGE_DEFAULT
	}

	// Try provider-specific config first, fall back to default
	image := viper.GetString(configKey)
	if image == "" {
		image = viper.GetString(consts.TALOS_VM_INSTALL_IMAGE_DEFAULT)
	}

	return image
}

// patchVirtualMachineCustomization adds VM-specific customizations to Talos config YAML
// This adds console kernel args and custom install image
// Note: System extensions (like qemu-guest-agent) should be baked into the install image via Talos factory
func patchVirtualMachineCustomization(in []byte, installImage string) ([]byte, error) {
	var cfg map[string]any
	if err := yaml.Unmarshal(in, &cfg); err != nil {
		return nil, err
	}

	m, _ := cfg["machine"].(map[string]any)
	if m == nil {
		m = map[string]any{}
		cfg["machine"] = m
	}

	// Add machine.install section
	inst, _ := m["install"].(map[string]any)
	if inst == nil {
		inst = map[string]any{}
		m["install"] = inst
	}

	// Add custom install image if provided (should include extensions)
	if installImage != "" {
		inst["image"] = installImage
	}

	// Add extraKernelArgs for serial console (directly under machine.install)
	inst["extraKernelArgs"] = []any{
		"console=ttyS0,115200",
	}

	return yaml.Marshal(cfg)
}

// marshalTalosClientConfig serializes the Talos client config if present.
func marshalTalosClientConfig(clientCfg *clientconfig.Config) ([]byte, error) {
	if clientCfg == nil {
		return nil, nil
	}
	b, err := yaml.Marshal(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal talos client config: %w", err)
	}
	return b, nil
}

// mergeBootstrappedFlag ensures we preserve the bootstrapped flag from an existing Secret,
// and defaults it to false if not present.
func mergeBootstrappedFlag(existing *corev1.Secret, data map[string][]byte) {
	if existing != nil && existing.Data != nil {
		if v, ok := existing.Data["bootstrapped"]; ok {
			data["bootstrapped"] = v
		}
	}
	if _, ok := data["bootstrapped"]; !ok {
		data["bootstrapped"] = []byte("false")
	}
}

// computePresence inspects both new data and existing secret to determine presence flags.
func computePresence(existing *corev1.Secret, data map[string][]byte) (hasTalos, hasCP, hasW, hasK bool) {
	// helper to check key in new data or existing secret
	present := func(key string) bool {
		if len(data[key]) > 0 {
			return true
		}
		if existing != nil && existing.Data != nil && len(existing.Data[key]) > 0 {
			return true
		}
		return false
	}
	hasTalos = present("talosconfig")
	hasCP = present("controlplane.yaml")
	hasW = present("worker.yaml")
	hasK = present("kube.config")
	return
}

// setPresenceFlags writes presence booleans (and cluster_access if kubeconfig present).
func setPresenceFlags(data map[string][]byte, hasTalos, hasCP, hasW, hasK bool) {
	data["talosconfig_present"] = []byte(strconv.FormatBool(hasTalos))
	data["controlplane_yaml_present"] = []byte(strconv.FormatBool(hasCP))
	data["worker_yaml_present"] = []byte(strconv.FormatBool(hasW))
	data["kubeconfig_present"] = []byte(strconv.FormatBool(hasK))
	if hasK {
		data["cluster_access"] = []byte("true")
	}
}

// applyDataToSecret merges the provided data into the secret (creating Data map if needed),
// skipping empty kube.config to avoid accidental deletion.
func applyDataToSecret(secret *corev1.Secret, data map[string][]byte) {
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	for k, v := range data {
		if k == "kube.config" && len(v) == 0 {
			continue
		}
		secret.Data[k] = v
	}
}

// upsertTalosClusterConfigSecretWithRoleYAML creates/updates the consolidated Secret with talosconfig and role templates.
func (t *TalosManager) upsertTalosClusterConfigSecretWithRoleYAML(
	ctx context.Context,
	cluster *vitistackcrdsv1alpha1.KubernetesCluster,
	clientCfg *clientconfig.Config,
	controlPlaneYAML []byte,
	workerYAML []byte,
	kubeconfig []byte,
) error {
	name := fmt.Sprintf("k8s-%s", cluster.Name)
	secret := &corev1.Secret{}
	err := t.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret)

	data := map[string][]byte{}
	if b, mErr := marshalTalosClientConfig(clientCfg); mErr != nil {
		return mErr
	} else if len(b) > 0 {
		data["talosconfig"] = b
	}
	if len(controlPlaneYAML) > 0 {
		data["controlplane.yaml"] = controlPlaneYAML
	}
	if len(workerYAML) > 0 {
		data["worker.yaml"] = workerYAML
	}
	if len(kubeconfig) > 0 {
		data["kube.config"] = kubeconfig
	}

	// preserve bootstrapped flag and default to false when missing
	var existing *corev1.Secret
	if err == nil {
		existing = secret
	}
	mergeBootstrappedFlag(existing, data)

	// presence flags
	hasTalos, hasCP, hasW, hasK := computePresence(existing, data)
	setPresenceFlags(data, hasTalos, hasCP, hasW, hasK)

	if err != nil {
		// create
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: cluster.Namespace,
				Labels: map[string]string{
					"cluster.vitistack.io/cluster-name": cluster.Name,
				},
			},
			Type: corev1.SecretTypeOpaque,
			Data: data,
		}
		return t.Create(ctx, secret)
	}
	applyDataToSecret(secret, data)
	return t.Update(ctx, secret)
}

// setTalosSecretBootstrapped is deprecated in favor of setTalosSecretFlags

// waitForTalosAPIs waits until Talos API on the given machines is reachable.
// If insecure=true, connects with InsecureSkipVerify to handle first-boot state.
func (t *TalosManager) waitForTalosAPIs(
	_ *vitistackcrdsv1alpha1.KubernetesCluster,
	_ *clientconfig.Config,
	machines []*vitistackcrdsv1alpha1.Machine,
	_ bool,
	timeout time.Duration,
	interval time.Duration,
) error {
	deadline := time.Now().Add(timeout)

	// Collect node IPs
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

// bootstrapTalosControlPlane bootstraps the cluster against a single control plane node.
func (t *TalosManager) bootstrapTalosControlPlane(ctx context.Context, tClient *talosclient.Client, controlPlaneIP string) error {
	// Ensure we target a single node for bootstrap
	ctx = talosclient.WithNodes(ctx, controlPlaneIP)

	// Perform bootstrap
	if err := tClient.Bootstrap(ctx, &machineapi.BootstrapRequest{}); err != nil {
		return fmt.Errorf("talos bootstrap failed: %w", err)
	}

	vlog.Info("Talos bootstrap initiated: node=" + controlPlaneIP)
	return nil
}

// bootstrapTalosControlPlaneWithRetry wraps bootstrap with retries to handle TLS mTLS timing window after config apply
func (t *TalosManager) bootstrapTalosControlPlaneWithRetry(ctx context.Context, tClient *talosclient.Client, controlPlaneIP string, timeout time.Duration, interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		if err := t.bootstrapTalosControlPlane(ctx, tClient, controlPlaneIP); err == nil {
			return nil
		} else {
			lastErr = err
			// retry on TLS handshake/auth issues which typically resolve once new certs are active
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

func isTLSHandshakeAuthError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "x509:") || strings.Contains(s, "tls:")
}

// getKubeconfigWithRetry tries to fetch kubeconfig until timeout, waiting interval between tries.
func getKubeconfigWithRetry(ctx context.Context, clientCfg *clientconfig.Config, endpoint string, timeout time.Duration, interval time.Duration) ([]byte, error) {
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

		vlog.Info("Kubeconfig not ready yet, retrying: endpoint=" + endpoint)
		time.Sleep(interval)
	}
}

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
				vlog.Info(fmt.Sprintf("All machines are ready: count=%d", len(readyMachines)))
				return readyMachines, nil
			}
			vlog.Info(fmt.Sprintf("Waiting for machines to be ready: ready=%d total=%d", len(readyMachines), len(machines)))
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
func (t *TalosManager) generateTalosConfig(
	cluster *vitistackcrdsv1alpha1.KubernetesCluster,
	machines []*vitistackcrdsv1alpha1.Machine,
	endpointIP string) (*clientconfig.Config, []byte, []byte, error) {
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
		vlog.Error("failed to generate secrets bundle", err)
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
		generate.WithEndpointList(endpointlist),
		// there are many more generate options available which allow to tweak generated config programmatically
	)
	if err != nil {
		vlog.Error("failed to generate input", err)
	}

	// Role templates for control-plane and worker
	// Configure encoder to generate minimal YAML without comments and examples
	encoderOpts := []encoder.Option{
		encoder.WithComments(encoder.CommentsDisabled),
	}

	var controlPlaneYAML, workerYAML []byte
	if cpProv, err := input.Config(machine.TypeControlPlane); err == nil {
		if b, err := cpProv.EncodeBytes(encoderOpts...); err == nil {
			controlPlaneYAML = b
		} else {
			return nil, nil, nil, fmt.Errorf("failed to render control plane template: %w", err)
		}
	} else {
		return nil, nil, nil, fmt.Errorf("failed to get control plane config provider: %w", err)
	}
	if wProv, err := input.Config(machine.TypeWorker); err == nil {
		if b, err := wProv.EncodeBytes(encoderOpts...); err == nil {
			workerYAML = b
		} else {
			return nil, nil, nil, fmt.Errorf("failed to render worker template: %w", err)
		}
	} else {
		return nil, nil, nil, fmt.Errorf("failed to get worker config provider: %w", err)
	}

	// We no longer build per-node configs here; we use role templates and patch per-node fields when applying.

	// generate the client Talos configuration (for API access, e.g. talosctl)
	clientCfg, err := input.Talosconfig()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to generate client config: %w", err)
	}

	// Optionally persist client config can be added later; avoid writing on controller FS.

	return clientCfg, controlPlaneYAML, workerYAML, nil
}

// upsertTalosClusterConfigSecret stores Talos client config, role configs, kubeconfig, and bootstrapped flag in a single Secret.
// Secret name: k8s-<cluster name>
func (t *TalosManager) upsertTalosClusterConfigSecret(
	ctx context.Context,
	cluster *vitistackcrdsv1alpha1.KubernetesCluster,
	clientCfg *clientconfig.Config,
	kubeconfig []byte,
) error {
	name := fmt.Sprintf("k8s-%s", cluster.Name)
	secret := &corev1.Secret{}
	err := t.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret)

	// Prepare data payload
	data := map[string][]byte{}

	if b, mErr := marshalTalosClientConfig(clientCfg); mErr != nil {
		return mErr
	} else if len(b) > 0 {
		data["talosconfig"] = b
	}

	// preserve existing role templates
	if err == nil && secret.Data != nil {
		if v, ok := secret.Data["controlplane.yaml"]; ok {
			data["controlplane.yaml"] = v
		}
		if v, ok := secret.Data["worker.yaml"]; ok {
			data["worker.yaml"] = v
		}
	}

	// preserve bootstrapped flag and default to false
	var existing *corev1.Secret
	if err == nil {
		existing = secret
	}
	mergeBootstrappedFlag(existing, data)

	// kubeconfig (optional update)
	if len(kubeconfig) > 0 {
		data["kube.config"] = kubeconfig
	}

	// presence flags
	hasTalos, hasCP, hasW, hasK := computePresence(existing, data)
	setPresenceFlags(data, hasTalos, hasCP, hasW, hasK)

	if err != nil {
		// create
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: cluster.Namespace,
				Labels: map[string]string{
					"cluster.vitistack.io/cluster-name": cluster.Name,
				},
			},
			Type: corev1.SecretTypeOpaque,
			Data: data,
		}
		return t.Create(ctx, secret)
	}

	// update existing: merge, preserving existing kube.config if not provided now
	applyDataToSecret(secret, data)
	return t.Update(ctx, secret)
}

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
