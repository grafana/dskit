package memberlist

import (
	"context"
	"fmt"
	"math/rand"
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

func TestZoneAwareNodeSelectionDelegate_SelectNodes(t *testing.T) {
	// Helper function to create a node with metadata.
	createNode := func(t *testing.T, name string, role NodeRole, zone string) *memberlist.NodeState {
		meta, err := EncodeNodeMetadata(role, zone)
		require.NoError(t, err)
		return &memberlist.NodeState{
			Node: memberlist.Node{
				Name: name,
				Meta: meta,
			},
		}
	}

	// Helper function to check if a node is in the selected slice.
	containsNode := func(nodes []*memberlist.NodeState, name string) bool {
		for _, n := range nodes {
			if n.Name == name {
				return true
			}
		}
		return false
	}

	t.Run("local node is member", func(t *testing.T) {
		delegate := newZoneAwareNodeSelectionDelegate(NodeRoleMember, "zone-a", log.NewNopLogger())

		nodes := []*memberlist.NodeState{
			createNode(t, "member-zone-a", NodeRoleMember, "zone-a"), // Same zone member: selected, not preferred.
			createNode(t, "bridge-zone-a", NodeRoleBridge, "zone-a"), // Same zone bridge: selected, not preferred.
			createNode(t, "member-zone-b", NodeRoleMember, "zone-b"), // Different zone member: NOT selected.
			createNode(t, "bridge-zone-b", NodeRoleBridge, "zone-b"), // Different zone bridge: NOT selected.
			createNode(t, "member-no-zone", NodeRoleMember, ""),      // Empty zone: selected, not preferred.
		}

		selected, preferred := delegate.SelectNodes(nodes)

		// Should select: member-zone-a, bridge-zone-a, member-no-zone
		assert.Len(t, selected, 3)
		assert.True(t, containsNode(selected, "member-zone-a"))
		assert.True(t, containsNode(selected, "bridge-zone-a"))
		assert.True(t, containsNode(selected, "member-no-zone"))
		assert.False(t, containsNode(selected, "member-zone-b"))
		assert.False(t, containsNode(selected, "bridge-zone-b"))

		// Members never have preferred candidates.
		assert.Nil(t, preferred)
	})

	t.Run("local node is bridge", func(t *testing.T) {
		delegate := newZoneAwareNodeSelectionDelegate(NodeRoleBridge, "zone-a", log.NewNopLogger())

		nodes := []*memberlist.NodeState{
			createNode(t, "member-zone-a", NodeRoleMember, "zone-a"), // Same zone member: selected, not preferred.
			createNode(t, "bridge-zone-a", NodeRoleBridge, "zone-a"), // Same zone bridge: selected, not preferred.
			createNode(t, "member-zone-b", NodeRoleMember, "zone-b"), // Different zone member: NOT selected.
			createNode(t, "bridge-zone-b", NodeRoleBridge, "zone-b"), // Different zone bridge: selected, preferred.
			createNode(t, "bridge-zone-c", NodeRoleBridge, "zone-c"), // Different zone bridge: selected, preferred.
			createNode(t, "member-no-zone", NodeRoleMember, ""),      // Empty zone: selected, not preferred.
		}

		selected, preferred := delegate.SelectNodes(nodes)

		// Should select: member-zone-a, bridge-zone-a, bridge-zone-b, bridge-zone-c, member-no-zone
		assert.Len(t, selected, 5)
		assert.True(t, containsNode(selected, "member-zone-a"))
		assert.True(t, containsNode(selected, "bridge-zone-a"))
		assert.True(t, containsNode(selected, "bridge-zone-b"))
		assert.True(t, containsNode(selected, "bridge-zone-c"))
		assert.True(t, containsNode(selected, "member-no-zone"))
		assert.False(t, containsNode(selected, "member-zone-b"))

		// Preferred should be one of the cross-zone bridges.
		assert.NotNil(t, preferred)
		assert.True(t, preferred.Name == "bridge-zone-b" || preferred.Name == "bridge-zone-c")
	})

	t.Run("local node has empty zone", func(t *testing.T) {
		delegate := newZoneAwareNodeSelectionDelegate(NodeRoleMember, "", log.NewNopLogger())

		nodes := []*memberlist.NodeState{
			createNode(t, "member-zone-a", NodeRoleMember, "zone-a"), // Selected, not preferred.
			createNode(t, "bridge-zone-b", NodeRoleBridge, "zone-b"), // Selected, not preferred.
		}

		selected, preferred := delegate.SelectNodes(nodes)

		// All nodes should be selected but not preferred when local zone is empty.
		assert.Len(t, selected, 2)
		assert.True(t, containsNode(selected, "member-zone-a"))
		assert.True(t, containsNode(selected, "bridge-zone-b"))
		assert.Nil(t, preferred)
	})

	t.Run("node with empty metadata", func(t *testing.T) {
		delegate := newZoneAwareNodeSelectionDelegate(NodeRoleMember, "zone-a", log.NewNopLogger())

		// Node with no metadata (empty Meta field).
		nodes := []*memberlist.NodeState{
			{
				Node: memberlist.Node{
					Name: "node-no-meta",
					Meta: nil,
				},
			},
		}

		selected, preferred := delegate.SelectNodes(nodes)
		assert.Len(t, selected, 1)
		assert.True(t, containsNode(selected, "node-no-meta"))
		assert.Nil(t, preferred)
	})

	t.Run("node with invalid metadata", func(t *testing.T) {
		delegate := newZoneAwareNodeSelectionDelegate(NodeRoleMember, "zone-a", log.NewNopLogger())

		// Node with invalid metadata (too short).
		nodes := []*memberlist.NodeState{
			{
				Node: memberlist.Node{
					Name: "node-invalid-meta",
					Meta: []byte{1, 2}, // Too short to be valid.
				},
			},
		}

		selected, preferred := delegate.SelectNodes(nodes)
		// Invalid metadata results in empty zone, so should be selected but not preferred.
		assert.Len(t, selected, 1)
		assert.True(t, containsNode(selected, "node-invalid-meta"))
		assert.Nil(t, preferred)
	})

	t.Run("empty input", func(t *testing.T) {
		delegate := newZoneAwareNodeSelectionDelegate(NodeRoleBridge, "zone-a", log.NewNopLogger())

		selected, preferred := delegate.SelectNodes(nil)
		assert.Empty(t, selected)
		assert.Nil(t, preferred)

		selected, preferred = delegate.SelectNodes([]*memberlist.NodeState{})
		assert.Empty(t, selected)
		assert.Nil(t, preferred)
	})

	t.Run("skip zone-aware routing when zone has no alive bridge", func(t *testing.T) {
		delegate := newZoneAwareNodeSelectionDelegate(NodeRoleMember, "zone-a", log.NewNopLogger())

		// Zone-b has members but no alive bridge (bridge is dead).
		nodes := []*memberlist.NodeState{
			createNode(t, "member-zone-a", NodeRoleMember, "zone-a"),
			createNode(t, "bridge-zone-a", NodeRoleBridge, "zone-a"),
			createNode(t, "member-zone-b", NodeRoleMember, "zone-b"),
			createNode(t, "bridge-zone-b-dead", NodeRoleBridge, "zone-b"),
		}
		nodes[3].State = memberlist.StateDead

		selected, preferred := delegate.SelectNodes(nodes)

		// Should return all nodes (zone-aware routing skipped).
		assert.Len(t, selected, 4)
		assert.Nil(t, preferred)
	})

	t.Run("skip zone-aware routing when zone has no bridge at all", func(t *testing.T) {
		delegate := newZoneAwareNodeSelectionDelegate(NodeRoleMember, "zone-a", log.NewNopLogger())

		// Zone-b has members but no bridge.
		nodes := []*memberlist.NodeState{
			createNode(t, "member-zone-a", NodeRoleMember, "zone-a"),
			createNode(t, "bridge-zone-a", NodeRoleBridge, "zone-a"),
			createNode(t, "member-zone-b", NodeRoleMember, "zone-b"),
		}

		selected, preferred := delegate.SelectNodes(nodes)

		// Should return all nodes (zone-aware routing skipped).
		assert.Len(t, selected, 3)
		assert.Nil(t, preferred)
	})

	t.Run("skip zone-aware routing when bridge is suspect", func(t *testing.T) {
		delegate := newZoneAwareNodeSelectionDelegate(NodeRoleMember, "zone-a", log.NewNopLogger())

		// Zone-b has members but bridge is suspect.
		nodes := []*memberlist.NodeState{
			createNode(t, "member-zone-a", NodeRoleMember, "zone-a"),
			createNode(t, "bridge-zone-a", NodeRoleBridge, "zone-a"),
			createNode(t, "member-zone-b", NodeRoleMember, "zone-b"),
			createNode(t, "bridge-zone-b-suspect", NodeRoleBridge, "zone-b"),
		}
		nodes[3].State = memberlist.StateSuspect

		selected, preferred := delegate.SelectNodes(nodes)

		// Should return all nodes (zone-aware routing skipped).
		assert.Len(t, selected, 4)
		assert.Nil(t, preferred)
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

func BenchmarkZoneAwareNodeSelectionDelegate_SelectNodes(b *testing.B) {
	// Create a delegate for a bridge node in zone-a.
	delegate := newZoneAwareNodeSelectionDelegate(NodeRoleBridge, "zone-a", log.NewNopLogger())

	// Helper function to create a node with metadata.
	createNode := func(name string, role NodeRole, zone string, state memberlist.NodeStateType) *memberlist.NodeState {
		meta, _ := EncodeNodeMetadata(role, zone)
		return &memberlist.NodeState{
			Node: memberlist.Node{
				Name: name,
				Meta: meta,
			},
			State: state,
		}
	}

	// Create 10K nodes: ~5K per zone, with 3 bridges per zone.
	const (
		totalNodes     = 10000
		bridgesPerZone = 3
		membersPerZone = (totalNodes - bridgesPerZone*2) / 2
	)

	nodes := make([]*memberlist.NodeState, 0, totalNodes)

	// Add bridges for zone-a.
	for i := 0; i < bridgesPerZone; i++ {
		nodes = append(nodes, createNode(fmt.Sprintf("bridge-zone-a-%d", i), NodeRoleBridge, "zone-a", memberlist.StateAlive))
	}
	// Add bridges for zone-b.
	for i := 0; i < bridgesPerZone; i++ {
		nodes = append(nodes, createNode(fmt.Sprintf("bridge-zone-b-%d", i), NodeRoleBridge, "zone-b", memberlist.StateAlive))
	}
	// Add members for zone-a.
	for i := 0; i < membersPerZone; i++ {
		nodes = append(nodes, createNode(fmt.Sprintf("member-zone-a-%d", i), NodeRoleMember, "zone-a", memberlist.StateAlive))
	}
	// Add members for zone-b.
	for i := 0; i < membersPerZone; i++ {
		nodes = append(nodes, createNode(fmt.Sprintf("member-zone-b-%d", i), NodeRoleMember, "zone-b", memberlist.StateAlive))
	}

	// Shuffle nodes deterministically for reproducible benchmarks.
	rng := rand.New(rand.NewSource(42))
	rng.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	// Expected selected count for a bridge in zone-a:
	// - All zone-a nodes (bridges + members): bridgesPerZone + membersPerZone
	// - Cross-zone bridges (zone-b bridges): bridgesPerZone
	expectedSelectedCount := bridgesPerZone + membersPerZone + bridgesPerZone

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		selected, _ := delegate.SelectNodes(nodes)
		if len(selected) != expectedSelectedCount {
			b.Fatalf("selected count mismatch: got %d, want %d", len(selected), expectedSelectedCount)
		}
	}
}
