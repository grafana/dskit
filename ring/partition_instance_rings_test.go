package ring

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
)

func TestPartitionInstanceRings(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Each ring has a single partition owned by a distinct instance.
	writePartitionAndOwner(t, kvClient, "ring-0", 0, "instance-0")
	writePartitionAndOwner(t, kvClient, "ring-1", 0, "instance-1")

	watchers, err := NewPartitionRingWatchers(
		NewPartitionRingWatcher("ring-0", "ring-0", kvClient, log.NewNopLogger(), nil),
		NewPartitionRingWatcher("ring-1", "ring-1", kvClient, log.NewNopLogger(), nil),
	)
	require.NoError(t, err)
	startPartitionRingWatchers(t, watchers)

	now := time.Now()
	instanceRing := staticInstanceRing{instances: map[string]InstanceDesc{
		"instance-0": {Addr: "addr-0", State: ACTIVE, Timestamp: now.Unix(), Zone: "zone-a"},
		"instance-1": {Addr: "addr-1", State: ACTIVE, Timestamp: now.Unix(), Zone: "zone-a"},
	}}

	rings := NewPartitionInstanceRings(watchers, instanceRing, time.Minute)
	require.Equal(t, 2, rings.Count())

	// Each ring exposes its own partition ring.
	assert.Equal(t, 1, rings.Get(0).PartitionRing().PartitionsCount())
	assert.Equal(t, 1, rings.Get(1).PartitionRing().PartitionsCount())

	// Ring 0 resolves only ring 0's partition owner against the shared instance ring; ring 1's
	// instance is never an owner of ring 0's partitions.
	readOp := NewOp([]InstanceState{ACTIVE, PENDING}, nil)
	sets, err := rings.Get(0).GetReplicationSetsForOperation(readOp)
	require.NoError(t, err)
	require.Len(t, sets, 1)
	require.Len(t, sets[0].Instances, 1)
	assert.Equal(t, "addr-0", sets[0].Instances[0].Addr)
}

func TestPartitionInstanceRings_SingleRing(t *testing.T) {
	kvClient, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	writePartitionAndOwner(t, kvClient, "ring", 0, "instance-0")

	watchers, err := NewPartitionRingWatchers(NewPartitionRingWatcher("ring", "ring", kvClient, log.NewNopLogger(), nil))
	require.NoError(t, err)
	startPartitionRingWatchers(t, watchers)

	rings := NewPartitionInstanceRings(watchers, staticInstanceRing{}, time.Minute)
	assert.Equal(t, 1, rings.Count())
	assert.Same(t, watchers.Watcher(0).PartitionRing(), rings.Get(0).PartitionRing())
}

// staticInstanceRing is a minimal InstanceRingReader backed by a static map.
type staticInstanceRing struct {
	instances map[string]InstanceDesc
}

func (r staticInstanceRing) GetInstance(id string) (InstanceDesc, error) {
	inst, ok := r.instances[id]
	if !ok {
		return InstanceDesc{}, fmt.Errorf("instance %s not found", id)
	}
	return inst, nil
}

func (r staticInstanceRing) InstancesCount() int { return len(r.instances) }

// writePartitionAndOwner stores a partition ring desc with a single active partition owned by the given instance.
func writePartitionAndOwner(t *testing.T, kvClient kv.Client, key string, partitionID int32, ownerID string) {
	t.Helper()
	require.NoError(t, kvClient.CAS(context.Background(), key, func(interface{}) (interface{}, bool, error) {
		desc := NewPartitionRingDesc()
		desc.AddPartition(partitionID, PartitionActive, time.Now())
		desc.AddOrUpdateOwner(ownerID, OwnerActive, partitionID, time.Now())
		return desc, true, nil
	}))
}
