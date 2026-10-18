package talosclientservice

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/vitistack/common/pkg/loggers/vlog"
)

// VersionProbeTimeout bounds one node's Talos version query.
const VersionProbeTimeout = 8 * time.Second

// maxConcurrentVersionProbes bounds how many nodes ProbeTalosVersions queries
// at once, so a few unresponsive nodes cost one timeout rather than one each.
const maxConcurrentVersionProbes = 8

// NodeEndpoint names a node and the IP its Talos API is reached on.
type NodeEndpoint struct {
	Name string
	IP   string
}

// ProbeTalosVersions asks each node's Talos API, connecting straight to the
// node's IP, which Talos version it runs (e.g. "v1.12.7"). Nodes are queried
// concurrently. Nodes that cannot be reached within timeout, or have no IP,
// are left out of the result, so callers can tell which nodes did not answer.
func (s *TalosClientService) ProbeTalosVersions(ctx context.Context, clientConfig *clientconfig.Config, nodes []NodeEndpoint, timeout time.Duration) map[string]string {
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		out = make(map[string]string, len(nodes))
		sem = make(chan struct{}, maxConcurrentVersionProbes)
	)
	for _, n := range nodes {
		if n.IP == "" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			if ver, ok := s.probeTalosVersion(ctx, clientConfig, n, timeout); ok {
				mu.Lock()
				out[n.Name] = ver
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return out
}

func (s *TalosClientService) probeTalosVersion(ctx context.Context, clientConfig *clientconfig.Config, n NodeEndpoint, timeout time.Duration) (string, bool) {
	client, err := s.CreateTalosClient(ctx, false, clientConfig, []string{n.IP})
	if err != nil {
		vlog.Debug(fmt.Sprintf("Talos version probe skipped node=%s: %v", n.Name, err))
		return "", false
	}
	defer func() { _ = client.Close() }()
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ver, err := s.GetTalosVersion(probeCtx, client, n.IP)
	if err != nil {
		vlog.Debug(fmt.Sprintf("Talos version probe failed node=%s: %v", n.Name, err))
		return "", false
	}
	return ver, true
}
