package upgradeservice

import (
	"context"
	"testing"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/spf13/viper"
	vitistackv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/talos-operator/pkg/consts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// Schematic pinned on most clusters before v1.13.10: four extensions.
	oldSchematicRef = testFactoryRef
	// Schematic the operator ships since v1.13.10: the same four plus nfs-utils.
	newSchematicRef = "factory.talos.dev/nocloud-installer/060a945412986c731a206fa0dd0b736e1363e94a38c1f3eb62dc38e63933ba8e"

	requiredExtensions = "siderolabs/iscsi-tools,siderolabs/qemu-guest-agent,siderolabs/trident-iscsi-tools,siderolabs/util-linux-tools,siderolabs/nfs-utils"
)

var (
	oldSchematicExtensions = []string{"iscsi-tools", "qemu-guest-agent", "trident-iscsi-tools", "util-linux-tools", "schematic"}
	newSchematicExtensions = []string{"iscsi-tools", "nfs-utils", "qemu-guest-agent", "trident-iscsi-tools", "util-linux-tools", "schematic"}
)

// setViper sets key for the duration of the test. viper is global, so tests
// using it must not run in parallel.
func setViper(t *testing.T, key, value string) {
	t.Helper()
	prev, had := viper.Get(key), viper.IsSet(key)
	viper.Set(key, value)
	t.Cleanup(func() {
		if had {
			viper.Set(key, prev)
		} else {
			viper.Set(key, nil)
		}
	})
}

func operatorShipsNewSchematic(t *testing.T) {
	t.Helper()
	setViper(t, consts.TALOS_REQUIRED_EXTENSIONS, requiredExtensions)
	setViper(t, consts.TALOS_VM_INSTALL_IMAGE_KUBEVIRT, newSchematicRef+":v1.13.10")
	setViper(t, consts.TALOS_VM_INSTALL_IMAGE_DEFAULT, newSchematicRef+":v1.13.10")
}

func clusterMachine(name, role string, provider vitistackv1alpha1.MachineProviderType) *vitistackv1alpha1.Machine {
	return &vitistackv1alpha1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: fixtureNamespace,
			Labels: map[string]string{
				vitistackv1alpha1.ClusterIdAnnotation: fixtureClusterID,
				vitistackv1alpha1.NodeRoleAnnotation:  role,
			},
		},
		Status: vitistackv1alpha1.MachineStatus{Provider: provider},
	}
}

// probeStub reports the same installed extensions for every machine and
// counts how often the nodes were asked.
type probeStub struct {
	installed []string
	calls     int
}

func (p *probeStub) probe(_ context.Context, _ *clientconfig.Config, machines []*vitistackv1alpha1.Machine) map[string][]string {
	p.calls++
	out := make(map[string][]string, len(machines))
	for _, m := range machines {
		if p.installed != nil {
			out[m.Name] = p.installed
		}
	}
	return out
}

func newInstallImageFixture(t *testing.T, pinned string, provider vitistackv1alpha1.MachineProviderType, installed []string) (*fixture, *probeStub) {
	t.Helper()
	f := newFixture(t,
		legacyCluster(nil),
		fixtureSecret(map[string]string{"install_image": pinned}),
		clusterMachine("d-amk-003-gsax-ctp0", "control-plane", provider),
		clusterMachine("d-amk-003-gsax-wrk3", "worker", provider),
	)
	stub := &probeStub{installed: installed}
	f.service.installedExtensions = stub.probe
	return f, stub
}

// d-amk-003 was upgraded to v1.13.10 on the four-extension schematic it was
// pinned to, although the operator had moved to a schematic that also carries
// nfs-utils. The pin must follow the required extension set.
func TestReconcileInstallImageSchematic_NodesMissingRequiredExtensionMovesToConfiguredSchematic(t *testing.T) {
	operatorShipsNewSchematic(t)
	f, _ := newInstallImageFixture(t, oldSchematicRef+":v1.13.10", vitistackv1alpha1.MachineProviderTypeKubevirt, oldSchematicExtensions)

	got, err := f.service.ReconcileInstallImageSchematic(context.Background(), f.cluster(t), nil)
	if err != nil {
		t.Fatalf("ReconcileInstallImageSchematic: %v", err)
	}

	want := newSchematicRef + ":v1.13.10"
	if got != want {
		t.Errorf("returned %q, want %q", got, want)
	}
	if pinned := f.secretData(t)["install_image"]; pinned != want {
		t.Errorf("pinned install_image = %q, want %q", pinned, want)
	}
}

// The pin keeps the version the cluster runs; only the schematic moves.
func TestReconcileInstallImageSchematic_KeepsPinnedVersion(t *testing.T) {
	operatorShipsNewSchematic(t)
	f, _ := newInstallImageFixture(t, oldSchematicRef+":v1.12.7", vitistackv1alpha1.MachineProviderTypeKubevirt, oldSchematicExtensions)

	if _, err := f.service.ReconcileInstallImageSchematic(context.Background(), f.cluster(t), nil); err != nil {
		t.Fatalf("ReconcileInstallImageSchematic: %v", err)
	}

	if pinned, want := f.secretData(t)["install_image"], newSchematicRef+":v1.12.7"; pinned != want {
		t.Errorf("pinned install_image = %q, want %q", pinned, want)
	}
}

func TestReconcileInstallImageSchematic_LeavesPinAlone(t *testing.T) {
	customRef := "factory.talos.dev/nocloud-installer/0000aaaa"

	tests := []struct {
		name      string
		pinned    string
		required  string
		provider  vitistackv1alpha1.MachineProviderType
		installed []string
		wantProbe bool
	}{
		{
			// A deliberately different schematic that already carries every
			// required extension (plus its own) must not lose its extras.
			name:      "custom schematic with every required extension",
			pinned:    customRef + ":v1.13.10",
			required:  requiredExtensions,
			provider:  vitistackv1alpha1.MachineProviderTypeKubevirt,
			installed: append([]string{"nvidia-container-toolkit"}, newSchematicExtensions...),
			wantProbe: true,
		},
		{
			// Extras beyond the required list mean the schematic was built
			// for this cluster; replacing it would drop them.
			name:      "custom schematic with extra extensions",
			pinned:    customRef + ":v1.13.10",
			required:  requiredExtensions,
			provider:  vitistackv1alpha1.MachineProviderTypeKubevirt,
			installed: append([]string{"nvidia-container-toolkit"}, oldSchematicExtensions...),
			wantProbe: true,
		},
		{
			// Without a tag there is no version to keep, and taking the
			// configured image's tag would change the version new nodes get.
			name:      "pin without a version tag",
			pinned:    oldSchematicRef,
			required:  requiredExtensions,
			provider:  vitistackv1alpha1.MachineProviderTypeKubevirt,
			installed: oldSchematicExtensions,
			wantProbe: true,
		},
		{
			name:      "already on the configured schematic",
			pinned:    newSchematicRef + ":v1.13.10",
			required:  requiredExtensions,
			provider:  vitistackv1alpha1.MachineProviderTypeKubevirt,
			installed: oldSchematicExtensions,
			wantProbe: false,
		},
		{
			name:      "no required extensions configured",
			pinned:    oldSchematicRef + ":v1.13.10",
			required:  "",
			provider:  vitistackv1alpha1.MachineProviderTypeKubevirt,
			installed: oldSchematicExtensions,
			wantProbe: false,
		},
		{
			// Without evidence from a node the pin is not changed.
			name:      "no node reachable",
			pinned:    oldSchematicRef + ":v1.13.10",
			required:  requiredExtensions,
			provider:  vitistackv1alpha1.MachineProviderTypeKubevirt,
			installed: nil,
			wantProbe: true,
		},
		{
			// The configured images are VM (nocloud) installers; a bare-metal
			// cluster's pin is not theirs to replace.
			name:      "not a virtual machine cluster",
			pinned:    oldSchematicRef + ":v1.13.10",
			required:  requiredExtensions,
			provider:  "baremetal",
			installed: oldSchematicExtensions,
			wantProbe: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operatorShipsNewSchematic(t)
			setViper(t, consts.TALOS_REQUIRED_EXTENSIONS, tt.required)
			f, stub := newInstallImageFixture(t, tt.pinned, tt.provider, tt.installed)

			got, err := f.service.ReconcileInstallImageSchematic(context.Background(), f.cluster(t), nil)
			if err != nil {
				t.Fatalf("ReconcileInstallImageSchematic: %v", err)
			}
			if got != tt.pinned {
				t.Errorf("returned %q, want %q", got, tt.pinned)
			}
			if pinned := f.secretData(t)["install_image"]; pinned != tt.pinned {
				t.Errorf("pinned install_image = %q, want %q", pinned, tt.pinned)
			}
			if probed := stub.calls > 0; probed != tt.wantProbe {
				t.Errorf("nodes probed = %t, want %t", probed, tt.wantProbe)
			}
		})
	}
}

// A rolling upgrade has already chosen its installer and pins it when it
// completes; the steady-state pass must not change the pin underneath it.
func TestReconcileInstallImageSchematic_LeavesPinAloneDuringUpgrade(t *testing.T) {
	operatorShipsNewSchematic(t)
	pinned := oldSchematicRef + ":v1.13.2"
	f := newFixture(t,
		legacyCluster(nil),
		fixtureSecret(map[string]string{"install_image": pinned, "upgrade_in_progress": "true", "upgrade_type": "talos"}),
		clusterMachine("d-amk-003-gsax-ctp0", "control-plane", vitistackv1alpha1.MachineProviderTypeKubevirt),
	)
	stub := &probeStub{installed: oldSchematicExtensions}
	f.service.installedExtensions = stub.probe

	got, err := f.service.ReconcileInstallImageSchematic(context.Background(), f.cluster(t), nil)
	if err != nil {
		t.Fatalf("ReconcileInstallImageSchematic: %v", err)
	}
	if got != pinned || f.secretData(t)["install_image"] != pinned {
		t.Errorf("pin changed during an upgrade: returned %q, stored %q", got, f.secretData(t)["install_image"])
	}
	if stub.calls != 0 {
		t.Errorf("nodes probed %d times during an upgrade, want 0", stub.calls)
	}
}

// Two clusters in the fleet are pinned to metal-installer. Only the schematic
// ID is theirs to replace: the platform decides how Talos boots.
func TestReconcileInstallImageSchematic_KeepsRegistryAndPlatform(t *testing.T) {
	operatorShipsNewSchematic(t)
	metalRef := "factory.talos.dev/metal-installer/b0f2a8b575460a3dcb1234cc081c73c88e795aaef36eda9b88a6f4dddbd49365"
	f, _ := newInstallImageFixture(t, metalRef+":v1.13.10", vitistackv1alpha1.MachineProviderTypeKubevirt, oldSchematicExtensions)

	got, err := f.service.ReconcileInstallImageSchematic(context.Background(), f.cluster(t), nil)
	if err != nil {
		t.Fatalf("ReconcileInstallImageSchematic: %v", err)
	}

	want := "factory.talos.dev/metal-installer/060a945412986c731a206fa0dd0b736e1363e94a38c1f3eb62dc38e63933ba8e:v1.13.10"
	if got != want {
		t.Errorf("returned %q, want %q", got, want)
	}
	if pinned := f.secretData(t)["install_image"]; pinned != want {
		t.Errorf("pinned install_image = %q, want %q", pinned, want)
	}
}

// A cluster whose pin is deliberately left alone must not be re-probed on
// every drift pass (every 90s, for the life of the cluster).
func TestReconcileInstallImageSchematic_DoesNotReprobeAnUnchangedPin(t *testing.T) {
	operatorShipsNewSchematic(t)
	pinned := "factory.talos.dev/nocloud-installer/0000aaaa:v1.13.10"
	f, stub := newInstallImageFixture(t, pinned, vitistackv1alpha1.MachineProviderTypeKubevirt,
		append([]string{"nvidia-container-toolkit"}, newSchematicExtensions...))

	for range 3 {
		if _, err := f.service.ReconcileInstallImageSchematic(context.Background(), f.cluster(t), nil); err != nil {
			t.Fatalf("ReconcileInstallImageSchematic: %v", err)
		}
	}

	if stub.calls != 1 {
		t.Errorf("probed the nodes %d times, want 1", stub.calls)
	}
	if got := f.secretData(t)["install_image"]; got != pinned {
		t.Errorf("pinned install_image = %q, want %q", got, pinned)
	}
}

// An upgrade must not roll nodes onto the pinned schematic when that schematic
// lacks a required extension.
func TestBuildTalosInstallerImage_UsesConfiguredSchematicWhenNodesLackExtensions(t *testing.T) {
	operatorShipsNewSchematic(t)
	f, _ := newInstallImageFixture(t, oldSchematicRef+":v1.13.2", vitistackv1alpha1.MachineProviderTypeKubevirt, oldSchematicExtensions)

	got, err := f.service.BuildTalosInstallerImage(context.Background(), f.cluster(t), nil, "v1.13.10")
	if err != nil {
		t.Fatalf("BuildTalosInstallerImage: %v", err)
	}

	if want := newSchematicRef + ":v1.13.10"; got != want {
		t.Errorf("installer image = %q, want %q", got, want)
	}
}

func TestBuildTalosInstallerImage_KeepsPinnedSchematicWhenNodesHaveExtensions(t *testing.T) {
	operatorShipsNewSchematic(t)
	f, _ := newInstallImageFixture(t, oldSchematicRef+":v1.13.2", vitistackv1alpha1.MachineProviderTypeKubevirt, newSchematicExtensions)

	got, err := f.service.BuildTalosInstallerImage(context.Background(), f.cluster(t), nil, "v1.13.10")
	if err != nil {
		t.Fatalf("BuildTalosInstallerImage: %v", err)
	}

	if want := oldSchematicRef + ":v1.13.10"; got != want {
		t.Errorf("installer image = %q, want %q", got, want)
	}
}

func TestImageRepositoryAndTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ref      string
		wantRepo string
		wantTag  string
	}{
		{ref: newSchematicRef + ":v1.13.10", wantRepo: newSchematicRef, wantTag: "v1.13.10"},
		{ref: "ghcr.io/siderolabs/installer", wantRepo: "ghcr.io/siderolabs/installer", wantTag: ""},
		{ref: "localhost:5000/foo:v1", wantRepo: "localhost:5000/foo", wantTag: "v1"},
		{ref: "localhost:5000/foo", wantRepo: "localhost:5000/foo", wantTag: ""},
		{ref: "ghcr.io/siderolabs/installer:v1.13.10@sha256:abc", wantRepo: "ghcr.io/siderolabs/installer", wantTag: "v1.13.10"},
	}
	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			t.Parallel()
			if got := imageRepository(tt.ref); got != tt.wantRepo {
				t.Errorf("imageRepository(%q) = %q, want %q", tt.ref, got, tt.wantRepo)
			}
			if got := imageTag(tt.ref); got != tt.wantTag {
				t.Errorf("imageTag(%q) = %q, want %q", tt.ref, got, tt.wantTag)
			}
		})
	}
}
