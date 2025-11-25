package memberlist

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
)

func TestNodeRoleString(t *testing.T) {
	tests := []struct {
		role NodeRole
		want string
	}{
		{NodeRoleMember, "member"},
		{NodeRoleBridge, "bridge"},
		{NodeRole(99), "unknown(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.role.String())
		})
	}
}

func TestZoneAwareRoutingConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		cfg     ZoneAwareRoutingConfig
		wantErr string
	}{
		"valid config - member role": {
			cfg: ZoneAwareRoutingConfig{
				Enabled: true,
				Zone:    "us-east-1a",
				Role:    "member",
			},
			wantErr: "",
		},
		"valid config - bridge role": {
			cfg: ZoneAwareRoutingConfig{
				Enabled: true,
				Zone:    "eu-west-1b",
				Role:    "bridge",
			},
			wantErr: "",
		},
		"valid config - max zone length": {
			cfg: ZoneAwareRoutingConfig{
				Enabled: true,
				Zone:    strings.Repeat("a", MaxZoneNameLength),
				Role:    "member",
			},
			wantErr: "",
		},
		"invalid - no zone": {
			cfg: ZoneAwareRoutingConfig{
				Enabled: true,
				Zone:    "",
				Role:    "member",
			},
			wantErr: "zone-aware routing is enabled but zone is not set",
		},
		"invalid - zone too long": {
			cfg: ZoneAwareRoutingConfig{
				Enabled: true,
				Zone:    strings.Repeat("a", MaxZoneNameLength+1),
				Role:    "member",
			},
			wantErr: "zone name too long",
		},
		"invalid - invalid role": {
			cfg: ZoneAwareRoutingConfig{
				Enabled: true,
				Zone:    "us-east-1a",
				Role:    "invalid",
			},
			wantErr: "invalid role: invalid",
		},
		"invalid - empty role": {
			cfg: ZoneAwareRoutingConfig{
				Enabled: true,
				Zone:    "us-east-1a",
				Role:    "",
			},
			wantErr: "invalid role",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestZoneAwareNodeSelectionDelegate_SelectNode(t *testing.T) {
	// Helper function to create a node with metadata.
	createNode := func(t *testing.T, name string, role NodeRole, zone string) memberlist.Node {
		meta, err := EncodeNodeMetadata(role, zone)
		require.NoError(t, err)
		return memberlist.Node{
			Name: name,
			Meta: meta,
		}
	}

	t.Run("local node is member", func(t *testing.T) {
		delegate := newZoneAwareNodeSelectionDelegate(NodeRoleMember, "zone-a", log.NewNopLogger())

		// Member in the same zone: should be selected but not preferred.
		selected, preferred := delegate.SelectNode(createNode(t, "member-zone-a", NodeRoleMember, "zone-a"))
		assert.True(t, selected)
		assert.False(t, preferred)

		// Bridge in the same zone: should be selected but not preferred.
		selected, preferred = delegate.SelectNode(createNode(t, "bridge-zone-a", NodeRoleBridge, "zone-a"))
		assert.True(t, selected)
		assert.False(t, preferred)

		// Member in a different zone: should NOT be selected.
		selected, preferred = delegate.SelectNode(createNode(t, "member-zone-b", NodeRoleMember, "zone-b"))
		assert.False(t, selected)
		assert.False(t, preferred)

		// Bridge in a different zone: should NOT be selected.
		selected, preferred = delegate.SelectNode(createNode(t, "bridge-zone-b", NodeRoleBridge, "zone-b"))
		assert.False(t, selected)
		assert.False(t, preferred)

		// Node with empty zone: should be selected but not preferred.
		selected, preferred = delegate.SelectNode(createNode(t, "member-no-zone", NodeRoleMember, ""))
		assert.True(t, selected)
		assert.False(t, preferred)
	})

	t.Run("local node is bridge", func(t *testing.T) {
		delegate := newZoneAwareNodeSelectionDelegate(NodeRoleBridge, "zone-a", log.NewNopLogger())

		// Member in the same zone: should be selected but not preferred.
		selected, preferred := delegate.SelectNode(createNode(t, "member-zone-a", NodeRoleMember, "zone-a"))
		assert.True(t, selected)
		assert.False(t, preferred)

		// Bridge in the same zone: should be selected but not preferred.
		selected, preferred = delegate.SelectNode(createNode(t, "bridge-zone-a", NodeRoleBridge, "zone-a"))
		assert.True(t, selected)
		assert.False(t, preferred)

		// Member in a different zone: should NOT be selected.
		selected, preferred = delegate.SelectNode(createNode(t, "member-zone-b", NodeRoleMember, "zone-b"))
		assert.False(t, selected)
		assert.False(t, preferred)

		// Bridge in a different zone: should be selected AND preferred.
		selected, preferred = delegate.SelectNode(createNode(t, "bridge-zone-b", NodeRoleBridge, "zone-b"))
		assert.True(t, selected)
		assert.True(t, preferred)

		// Bridge in another different zone: should be selected AND preferred.
		selected, preferred = delegate.SelectNode(createNode(t, "bridge-zone-c", NodeRoleBridge, "zone-c"))
		assert.True(t, selected)
		assert.True(t, preferred)

		// Node with empty zone: should be selected but not preferred.
		selected, preferred = delegate.SelectNode(createNode(t, "member-no-zone", NodeRoleMember, ""))
		assert.True(t, selected)
		assert.False(t, preferred)
	})

	t.Run("local node has empty zone", func(t *testing.T) {
		delegate := newZoneAwareNodeSelectionDelegate(NodeRoleMember, "", log.NewNopLogger())

		// Any node should be selected but not preferred when local zone is empty.
		selected, preferred := delegate.SelectNode(createNode(t, "member-zone-a", NodeRoleMember, "zone-a"))
		assert.True(t, selected)
		assert.False(t, preferred)

		selected, preferred = delegate.SelectNode(createNode(t, "bridge-zone-b", NodeRoleBridge, "zone-b"))
		assert.True(t, selected)
		assert.False(t, preferred)
	})

	t.Run("node with empty metadata", func(t *testing.T) {
		delegate := newZoneAwareNodeSelectionDelegate(NodeRoleMember, "zone-a", log.NewNopLogger())

		// Node with no metadata (empty Meta field).
		node := memberlist.Node{
			Name: "node-no-meta",
			Meta: nil,
		}
		selected, preferred := delegate.SelectNode(node)
		assert.True(t, selected)
		assert.False(t, preferred)
	})

	t.Run("node with invalid metadata", func(t *testing.T) {
		delegate := newZoneAwareNodeSelectionDelegate(NodeRoleMember, "zone-a", log.NewNopLogger())

		// Node with invalid metadata (too short).
		node := memberlist.Node{
			Name: "node-invalid-meta",
			Meta: []byte{1, 2}, // Too short to be valid.
		}
		selected, preferred := delegate.SelectNode(node)
		// Invalid metadata results in empty zone, so should be selected but not preferred.
		assert.True(t, selected)
		assert.False(t, preferred)
	})
}

func TestZoneAwareRouting_EndToEnd(t *testing.T) {
	const key = "test-key"

	tests := map[string]struct {
		gossipInterval   time.Duration
		pushPullInterval time.Duration
	}{
		"state sync via broadcast updates": {
			gossipInterval:   50 * time.Millisecond,
			pushPullInterval: 0,
		},
		"state sync via push/pull": {
			gossipInterval:   1 * time.Hour,
			pushPullInterval: 50 * time.Millisecond,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var (
				c      = dataCodec{}
				ctx    = context.Background()
				logger = log.NewLogfmtLogger(os.Stderr)
			)

			// Helper function to create a node configuration.
			makeConfig := func(seedNodes []string, zone, role string) KVConfig {
				var cfg KVConfig
				flagext.DefaultValues(&cfg)
				cfg.NodeName = fmt.Sprintf("%s-%s", zone, role)
				cfg.TCPTransport.BindAddrs = getLocalhostAddrs()
				cfg.TCPTransport.BindPort = 0
				cfg.Codecs = append(cfg.Codecs, c)
				cfg.GossipInterval = tc.gossipInterval
				cfg.PushPullInterval = tc.pushPullInterval
				cfg.RejoinInterval = 0 // Never rejoin, but just rely on gossiping messages or periodic push/pull operations.
				cfg.JoinMembers = seedNodes
				cfg.ZoneAwareRouting = ZoneAwareRoutingConfig{
					Enabled: true,
					Zone:    zone,
					Role:    role,
				}

				// Leave quickly. We don't care about a clean shutdown.
				cfg.LeaveTimeout = 100 * time.Millisecond
				cfg.BroadcastTimeoutForLocalUpdatesOnShutdown = 100 * time.Millisecond

				return cfg
			}

			// Create a cluster with 2 zones, each with 1 bridge and 2 members.
			// Zone A: bridge-a, member-a-1, member-a-2
			// Zone B: bridge-b, member-b-1, member-b-2

			// Zone A bridge - create and start first.
			bridgeA := NewKV(makeConfig(nil, "zone-a", "bridge"), log.WithPrefix(logger, "instance", "bridge-zone-a"), &staticDNSProviderMock{}, prometheus.NewPedanticRegistry())
			require.NoError(t, services.StartAndAwaitRunning(ctx, bridgeA))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, bridgeA))
			})

			kvBridgeA, err := NewClient(bridgeA, c)
			require.NoError(t, err)

			// Create all other nodes (but don't start them yet).
			// Get the join address for bridgeA now since it's already started.
			bridgeAAddr := []string{net.JoinHostPort(bridgeA.cfg.TCPTransport.BindAddrs[0], strconv.Itoa(bridgeA.GetListeningPort()))}

			memberA1 := NewKV(makeConfig(bridgeAAddr, "zone-a", "member"), log.WithPrefix(logger, "instance", "member-zone-a-1"), &staticDNSProviderMock{}, prometheus.NewPedanticRegistry())
			memberA2 := NewKV(makeConfig(bridgeAAddr, "zone-a", "member"), log.WithPrefix(logger, "instance", "member-zone-a-2"), &staticDNSProviderMock{}, prometheus.NewPedanticRegistry())
			bridgeB := NewKV(makeConfig(bridgeAAddr, "zone-b", "bridge"), log.WithPrefix(logger, "instance", "bridge-zone-b"), &staticDNSProviderMock{}, prometheus.NewPedanticRegistry())
			memberB1 := NewKV(makeConfig(bridgeAAddr, "zone-b", "member"), log.WithPrefix(logger, "instance", "member-zone-b-1"), &staticDNSProviderMock{}, prometheus.NewPedanticRegistry())
			memberB2 := NewKV(makeConfig(bridgeAAddr, "zone-b", "member"), log.WithPrefix(logger, "instance", "member-zone-b-2"), &staticDNSProviderMock{}, prometheus.NewPedanticRegistry())

			// Start services in what we consider the worst case scenario:
			// zone-b members first, so they don't see any zone-b bridge at startup, but they're also
			// not allowed to initiate a push/pull or gossip to any zone-a member or bridge.
			zoneBMembers, err := services.NewManager(memberB1, memberB2)
			require.NoError(t, err)
			require.NoError(t, services.StartManagerAndAwaitHealthy(ctx, zoneBMembers))
			t.Cleanup(func() {
				require.NoError(t, services.StopManagerAndAwaitStopped(ctx, zoneBMembers))
			})

			// Then start other nodes.
			otherMembers, err := services.NewManager(memberA1, memberA2, bridgeB)
			require.NoError(t, err)
			require.NoError(t, services.StartManagerAndAwaitHealthy(ctx, otherMembers))
			t.Cleanup(func() {
				require.NoError(t, services.StopManagerAndAwaitStopped(ctx, otherMembers))
			})

			// Create clients for all nodes.
			clientMemberA1, err := NewClient(memberA1, c)
			require.NoError(t, err)

			clientMemberA2, err := NewClient(memberA2, c)
			require.NoError(t, err)

			kvBridgeB, err := NewClient(bridgeB, c)
			require.NoError(t, err)

			clientMemberB1, err := NewClient(memberB1, c)
			require.NoError(t, err)

			clientMemberB2, err := NewClient(memberB2, c)
			require.NoError(t, err)

			// Create slices with all nodes for easier iteration.
			allNodes := []*KV{bridgeA, memberA1, memberA2, bridgeB, memberB1, memberB2}
			allClients := []*Client{kvBridgeA, clientMemberA1, clientMemberA2, kvBridgeB, clientMemberB1, clientMemberB2}

			// Wait for cluster to stabilize - all nodes should see all other nodes.
			for _, node := range allNodes {
				t.Logf("waiting for node %s in zone %s with role %s to see all other nodes", node.memberlist.LocalNode().Name, node.cfg.ZoneAwareRouting.Zone, node.cfg.ZoneAwareRouting.Role)
				test.Poll(t, 2*time.Second, 6, func() interface{} {
					return node.memberlist.NumMembers()
				})
			}
			t.Log("all nodes see each other")

			// Write data from a member in zone A.
			err = clientMemberA1.CAS(ctx, key, func(in interface{}) (out interface{}, retry bool, err error) {
				return &data{
					Members: map[string]member{
						"test-member": {
							Timestamp: time.Now().Unix(),
							Tokens:    []uint32{1, 2, 3},
							State:     ACTIVE,
						},
					},
				}, true, nil
			})
			require.NoError(t, err)

			// Verify that all nodes (in both zones) receive the update.
			checkValue := func(kv *Client) func() interface{} {
				return func() interface{} {
					val, err := kv.Get(ctx, key)
					if err != nil || val == nil {
						return nil
					}
					d, ok := val.(*data)
					if !ok {
						return nil
					}
					if m, exists := d.Members["test-member"]; exists && m.State == ACTIVE {
						return "ok"
					}
					return nil
				}
			}

			// Poll all nodes to ensure they all received the update.
			for idx, node := range allNodes {
				t.Logf("waiting for node %s in zone %s with role %s to receive the update", node.memberlist.LocalNode().Name, node.cfg.ZoneAwareRouting.Zone, node.cfg.ZoneAwareRouting.Role)

				client := allClients[idx]
				test.Poll(t, 2*time.Second, "ok", checkValue(client))
			}
			t.Log("all nodes received the update")
		})
	}
}
