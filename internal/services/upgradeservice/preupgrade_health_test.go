package upgradeservice

import (
	"context"
	"slices"
	"strings"
	"testing"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
)

type healthStub struct {
	result         preUpgradeHealth
	controlPlaneIP string
	nodeIPs        []string
	calls          int
}

func (h *healthStub) check(_ context.Context, _ *clientconfig.Config, controlPlaneIP string, nodeIPs []string) preUpgradeHealth {
	h.calls++
	h.controlPlaneIP = controlPlaneIP
	h.nodeIPs = nodeIPs
	return h.result
}

func upgradeNodes() (controlPlanes, workers []NodeUpgradeState) {
	controlPlanes = []NodeUpgradeState{
		{NodeName: "d-amk-003-gsax-ctp0", Role: controlPlaneRole, IP: "100.64.11.245"},
		{NodeName: "d-amk-003-gsax-ctp1", Role: controlPlaneRole, IP: "100.64.11.91"},
	}
	workers = []NodeUpgradeState{{NodeName: "d-amk-003-gsax-wrk3", Role: "worker", IP: "100.64.11.170"}}
	return controlPlanes, workers
}

// The secret's health_check_* keys were seeded at cluster creation and never
// written again, so every cluster reported a failed check that never ran.
func TestRecordPreUpgradeHealth_PersistsFailedCheck(t *testing.T) {
	f := newFixture(t, legacyCluster(nil), fixtureSecret(map[string]string{
		"health_check_passed": "false",
		"health_check_at":     "",
	}))
	stub := &healthStub{result: preUpgradeHealth{
		ControlPlaneReady: true,
		NodesReady:        true,
		Issues:            []string{"etcd cluster is not healthy"},
	}}
	f.service.healthCheck = stub.check
	controlPlanes, workers := upgradeNodes()

	got := f.service.RecordPreUpgradeHealth(context.Background(), f.cluster(t), nil, controlPlanes, workers)

	if got.Passed {
		t.Error("returned Passed=true for a failed check")
	}
	if stub.controlPlaneIP != "100.64.11.245" {
		t.Errorf("checked control plane %q, want the first one", stub.controlPlaneIP)
	}
	if want := []string{"100.64.11.245", "100.64.11.91", "100.64.11.170"}; !slices.Equal(stub.nodeIPs, want) {
		t.Errorf("checked nodes %v, want %v", stub.nodeIPs, want)
	}

	data := f.secretData(t)
	for key, want := range map[string]string{
		"health_check_passed":       "false",
		"health_etcd_healthy":       "false",
		"health_nodes_ready":        "true",
		"health_controlplane_ready": "true",
	} {
		if data[key] != want {
			t.Errorf("%s = %q, want %q", key, data[key], want)
		}
	}
	if data["health_check_at"] == "" {
		t.Error("health_check_at not recorded")
	}
	if !strings.Contains(data["health_check_message"], "etcd cluster is not healthy") {
		t.Errorf("health_check_message = %q, want the failing check named", data["health_check_message"])
	}
}

func TestRecordPreUpgradeHealth_PersistsPassedCheck(t *testing.T) {
	f := newFixture(t, legacyCluster(nil), fixtureSecret(nil))
	f.service.healthCheck = (&healthStub{result: preUpgradeHealth{ControlPlaneReady: true, EtcdHealthy: true, NodesReady: true}}).check
	controlPlanes, workers := upgradeNodes()

	got := f.service.RecordPreUpgradeHealth(context.Background(), f.cluster(t), nil, controlPlanes, workers)

	if !got.Passed {
		t.Errorf("returned Passed=false: %s", got.Message)
	}
	data := f.secretData(t)
	for _, key := range []string{"health_check_passed", "health_etcd_healthy", "health_nodes_ready", "health_controlplane_ready"} {
		if data[key] != "true" {
			t.Errorf("%s = %q, want \"true\"", key, data[key])
		}
	}
}

func TestRecordPreUpgradeHealth_NoControlPlaneFailsWithoutProbing(t *testing.T) {
	f := newFixture(t, legacyCluster(nil), fixtureSecret(nil))
	stub := &healthStub{}
	f.service.healthCheck = stub.check
	_, workers := upgradeNodes()

	got := f.service.RecordPreUpgradeHealth(context.Background(), f.cluster(t), nil, nil, workers)

	if got.Passed {
		t.Error("returned Passed=true without a control plane")
	}
	if stub.calls != 0 {
		t.Errorf("nodes probed %d times, want 0", stub.calls)
	}
	if data := f.secretData(t); data["health_check_passed"] != "false" || data["health_check_at"] == "" {
		t.Errorf("failed check not recorded: passed=%q at=%q", data["health_check_passed"], data["health_check_at"])
	}
}
