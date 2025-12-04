package etcdservice

import (
	"context"
	"fmt"
	"strings"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/vitistack/common/pkg/loggers/vlog"
	"github.com/vitistack/talos-operator/internal/services/talosclientservice"
)

// EtcdService handles etcd cluster operations via Talos API
type EtcdService struct {
	clientService *talosclientservice.TalosClientService
}

// NewEtcdService creates a new EtcdService instance
func NewEtcdService(clientService *talosclientservice.TalosClientService) *EtcdService {
	return &EtcdService{
		clientService: clientService,
	}
}

// Member represents a member of the etcd cluster.
type Member struct {
	ID         uint64
	Hostname   string
	PeerURLs   []string
	ClientURLs []string
	IsLearner  bool
}

// MemberList returns the list of etcd members in the cluster.
// The controlPlaneIP should be a healthy control plane node's IP.
func (s *EtcdService) MemberList(
	ctx context.Context,
	clientConfig *clientconfig.Config,
	controlPlaneIP string) ([]Member, error) {
	tClient, err := s.clientService.CreateTalosClient(ctx, false, clientConfig, []string{controlPlaneIP})
	if err != nil {
		return nil, fmt.Errorf("failed to create Talos client for etcd member list: %w", err)
	}
	defer func() { _ = tClient.Close() }()

	nodeCtx := talosclient.WithNodes(ctx, controlPlaneIP)

	resp, err := tClient.EtcdMemberList(nodeCtx, &machineapi.EtcdMemberListRequest{
		QueryLocal: false, // Query the actual etcd cluster, not just local
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list etcd members: %w", err)
	}

	var members []Member
	for _, msg := range resp.Messages {
		for _, m := range msg.Members {
			members = append(members, Member{
				ID:         m.Id,
				Hostname:   m.Hostname,
				PeerURLs:   m.PeerUrls,
				ClientURLs: m.ClientUrls,
				IsLearner:  m.IsLearner,
			})
		}
	}

	return members, nil
}

// ForfeitLeadership makes the etcd node at nodeIP forfeit leadership.
// This should be called before removing the leader from the cluster.
// Returns nil if the node is not the leader or if leadership was successfully transferred.
func (s *EtcdService) ForfeitLeadership(
	ctx context.Context,
	clientConfig *clientconfig.Config,
	nodeIP string) error {
	tClient, err := s.clientService.CreateTalosClient(ctx, false, clientConfig, []string{nodeIP})
	if err != nil {
		return fmt.Errorf("failed to create Talos client for etcd forfeit leadership: %w", err)
	}
	defer func() { _ = tClient.Close() }()

	nodeCtx := talosclient.WithNodes(ctx, nodeIP)

	_, err = tClient.EtcdForfeitLeadership(nodeCtx, &machineapi.EtcdForfeitLeadershipRequest{})
	if err != nil {
		// If the error indicates the node is not the leader, that's fine
		if strings.Contains(err.Error(), "not the leader") {
			vlog.Info(fmt.Sprintf("Node is not etcd leader, no need to forfeit: node=%s", nodeIP))
			return nil
		}
		return fmt.Errorf("failed to forfeit etcd leadership: %w", err)
	}

	vlog.Info(fmt.Sprintf("Successfully forfeited etcd leadership: node=%s", nodeIP))
	return nil
}

// RemoveMemberByID removes an etcd member by its member ID.
// The controlPlaneIP should be a healthy control plane node (not the one being removed).
func (s *EtcdService) RemoveMemberByID(
	ctx context.Context,
	clientConfig *clientconfig.Config,
	controlPlaneIP string,
	memberID uint64) error {
	tClient, err := s.clientService.CreateTalosClient(ctx, false, clientConfig, []string{controlPlaneIP})
	if err != nil {
		return fmt.Errorf("failed to create Talos client for etcd remove member: %w", err)
	}
	defer func() { _ = tClient.Close() }()

	nodeCtx := talosclient.WithNodes(ctx, controlPlaneIP)

	err = tClient.EtcdRemoveMemberByID(nodeCtx, &machineapi.EtcdRemoveMemberByIDRequest{
		MemberId: memberID,
	})
	if err != nil {
		return fmt.Errorf("failed to remove etcd member %d: %w", memberID, err)
	}

	vlog.Info(fmt.Sprintf("Successfully removed etcd member: memberID=%d via node=%s", memberID, controlPlaneIP))
	return nil
}

// LeaveCluster makes the etcd node at nodeIP leave the etcd cluster gracefully.
// This is an alternative to RemoveMemberByID when the node itself can perform the leave.
func (s *EtcdService) LeaveCluster(
	ctx context.Context,
	clientConfig *clientconfig.Config,
	nodeIP string) error {
	tClient, err := s.clientService.CreateTalosClient(ctx, false, clientConfig, []string{nodeIP})
	if err != nil {
		return fmt.Errorf("failed to create Talos client for etcd leave: %w", err)
	}
	defer func() { _ = tClient.Close() }()

	nodeCtx := talosclient.WithNodes(ctx, nodeIP)

	err = tClient.EtcdLeaveCluster(nodeCtx, &machineapi.EtcdLeaveClusterRequest{})
	if err != nil {
		return fmt.Errorf("failed to leave etcd cluster: %w", err)
	}

	vlog.Info(fmt.Sprintf("Node left etcd cluster: node=%s", nodeIP))
	return nil
}

// FindMemberByHostname finds an etcd member by hostname from the members list.
func FindMemberByHostname(members []Member, hostname string) *Member {
	for i := range members {
		if members[i].Hostname == hostname {
			return &members[i]
		}
	}
	return nil
}

// FindMemberByIP finds an etcd member by IP address from the members list.
// It searches through PeerURLs for a matching IP.
func FindMemberByIP(members []Member, ip string) *Member {
	for i := range members {
		for _, peerURL := range members[i].PeerURLs {
			if strings.Contains(peerURL, ip) {
				return &members[i]
			}
		}
	}
	return nil
}
