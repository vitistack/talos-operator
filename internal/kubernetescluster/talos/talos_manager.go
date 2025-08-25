package talos

import (
	"context"
	"fmt"
	"time"

	vitistackcrdsv1alpha1 "github.com/vitistack/crds/pkg/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	machineInfos := t.extractMachineInfos(readyMachines)

	// Fetch disk info and install talos on disk
	if err := t.installTalosOnDisk(ctx, machineInfos); err != nil {
		return fmt.Errorf("failed to install Talos on disk: %w", err)
	}

	// Generate Talos configuration
	if err := t.generateTalosConfig(ctx, cluster, machineInfos); err != nil {
		return fmt.Errorf("failed to generate Talos config: %w", err)
	}

	// todo registert VIP ip addresses to NAM
	// write to a crd, so others can handle load balancing of control planes

	// Bootstrap the cluster
	if err := t.bootstrapTalosCluster(ctx, cluster, machineInfos); err != nil {
		return fmt.Errorf("failed to bootstrap Talos cluster: %w", err)
	}

	log.Info("Successfully reconciled Talos cluster", "cluster", cluster.Name)
	return nil
}

func (t *TalosManager) installTalosOnDisk(ctx context.Context, machineInfos []MachineInfo) error {
	// todo implement
	return fmt.Errorf("not implemented")
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
func (t *TalosManager) isMachineReady(machine *vitistackcrdsv1alpha1.Machine) bool {
	// Check if machine is in running state
	if machine.Status.Phase != MachinePhaseRunning {
		return false
	}

	// Check if machine has at least one IP address (private or public)
	hasIP := len(machine.Status.PrivateIPAddresses) > 0 || len(machine.Status.PublicIPAddresses) > 0
	return hasIP
}

// extractMachineInfos creates MachineInfo structs from ready machines
func (t *TalosManager) extractMachineInfos(machines []*vitistackcrdsv1alpha1.Machine) []MachineInfo {
	var machineInfos []MachineInfo

	for _, machine := range machines {
		// Determine role from labels
		role := "worker"
		if machineRole, exists := machine.Labels["cluster.vitistack.io/role"]; exists {
			role = machineRole
		}

		// Get primary IP address (prefer private, fallback to public)
		var ip string
		if len(machine.Status.PrivateIPAddresses) > 0 {
			ip = machine.Status.PrivateIPAddresses[0]
		} else if len(machine.Status.PublicIPAddresses) > 0 {
			ip = machine.Status.PublicIPAddresses[0]
		}

		if ip != "" {
			machineInfos = append(machineInfos, MachineInfo{
				Name:    machine.Name,
				Role:    role,
				IP:      ip,
				Machine: machine,
			})
		}
	}

	return machineInfos
}

// generateTalosConfig generates Talos configuration for the cluster
func (t *TalosManager) generateTalosConfig(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster, machines []MachineInfo) error {
	log := ctrl.LoggerFrom(ctx)

	log.Info("Generating Talos configuration", "cluster", cluster.Name, "machineCount", len(machines))

	// Check if we already have configuration for this cluster
	clusterKey := fmt.Sprintf("%s/%s", cluster.Namespace, cluster.Name)
	talosConfig, exists := t.clusterConfigurations[clusterKey]
	if !exists {
		talosConfig = &TalosClusterConfig{
			ClusterName:     cluster.Name,
			SecretGenerated: false,
			MachineConfigs:  make(map[string]string),
			Bootstrapped:    false,
		}
		t.clusterConfigurations[clusterKey] = talosConfig
	}

	// Separate control plane and worker machines
	controlPlanes := filterMachinesByRole(machines, "control-plane")
	workers := filterMachinesByRole(machines, "worker")

	if len(controlPlanes) == 0 {
		return fmt.Errorf("no control plane machines found for cluster %s", cluster.Name)
	}

	// Generate cluster secrets if not already done
	if !talosConfig.SecretGenerated {
		log.Info("Generating cluster secrets", "cluster", cluster.Name)
		// TODO: Implement actual Talos secret generation using talosctl Go package
		// For now, mark as generated
		talosConfig.SecretGenerated = true
	}

	// Generate machine configs
	for _, machine := range machines {
		if _, configExists := talosConfig.MachineConfigs[machine.Name]; !configExists {
			log.Info("Generating Talos config for machine",
				"machine", machine.Name,
				"role", machine.Role,
				"ip", machine.IP)

			// TODO: Generate actual machine configuration using talosctl Go package
			// For now, create a placeholder
			machineConfig := fmt.Sprintf("# Talos config for %s (%s) at %s\n", machine.Name, machine.Role, machine.IP)
			talosConfig.MachineConfigs[machine.Name] = machineConfig
		}
	}

	log.Info("Successfully generated Talos configurations",
		"cluster", cluster.Name,
		"controlPlanes", len(controlPlanes),
		"workers", len(workers),
		"totalConfigs", len(talosConfig.MachineConfigs))

	return nil
}

// bootstrapTalosCluster bootstraps the Talos cluster
func (t *TalosManager) bootstrapTalosCluster(ctx context.Context, cluster *vitistackcrdsv1alpha1.KubernetesCluster, machines []MachineInfo) error {
	log := ctrl.LoggerFrom(ctx)

	log.Info("Bootstrapping Talos cluster", "cluster", cluster.Name)

	clusterKey := fmt.Sprintf("%s/%s", cluster.Namespace, cluster.Name)
	talosConfig := t.clusterConfigurations[clusterKey]

	if talosConfig.Bootstrapped {
		log.Info("Cluster already bootstrapped", "cluster", cluster.Name)
		return nil
	}

	controlPlanes := filterMachinesByRole(machines, "control-plane")
	if len(controlPlanes) == 0 {
		return fmt.Errorf("no control plane machines found for cluster %s", cluster.Name)
	}

	// TODO: Implement actual Talos cluster bootstrap using talosctl Go package
	// This would include:
	// 1. Apply configurations to all machines via Talos API
	// 2. Bootstrap the first control plane node
	// 3. Wait for cluster to be ready
	// 4. Extract kubeconfig

	log.Info("Would bootstrap Talos cluster",
		"cluster", cluster.Name,
		"bootstrapNode", controlPlanes[0].Name,
		"bootstrapIP", controlPlanes[0].IP,
		"totalMachines", len(machines))

	// Mark as bootstrapped
	talosConfig.Bootstrapped = true

	log.Info("Successfully bootstrapped Talos cluster", "cluster", cluster.Name)
	return nil
}

// filterMachinesByRole filters machines by their role
func filterMachinesByRole(machines []MachineInfo, role string) []MachineInfo {
	var filtered []MachineInfo
	for _, machine := range machines {
		if machine.Role == role {
			filtered = append(filtered, machine)
		}
	}
	return filtered
}
