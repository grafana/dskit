package ring

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestPartitionRingDesc_Clone(t *testing.T) {
	for name, desc := range map[string]*PartitionRingDesc{
		"nil maps":   {},
		"empty maps": NewPartitionRingDesc(),
		"populated": {
			Partitions: map[int32]PartitionDesc{
				1: {Id: 1, Tokens: []uint32{1, 2}, State: PartitionActive, StateTimestamp: 10, StateChangeLocked: true, StateChangeLockedTimestamp: 20},
				2: {Id: 2, State: PartitionDeleted, StateTimestamp: 30},
				3: {Id: 3, Tokens: []uint32{}, State: PartitionPending},
			},
			Owners: map[string]OwnerDesc{
				"active":  {OwnedPartition: 1, State: OwnerActive, UpdatedTimestamp: 10},
				"deleted": {OwnedPartition: 2, State: OwnerDeleted, UpdatedTimestamp: 30},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			cloned := desc.Clone().(*PartitionRingDesc)
			require.Equal(t, desc, cloned)
			require.NotSame(t, desc, cloned)
			legacy := proto.Clone(desc).(*PartitionRingDesc)
			if desc.Partitions != nil && legacy.Partitions == nil {
				legacy.Partitions = map[int32]PartitionDesc{}
			}
			if desc.Owners != nil && legacy.Owners == nil {
				legacy.Owners = map[string]OwnerDesc{}
			}
			require.Equal(t, legacy, cloned)
			for id, partition := range desc.Partitions {
				if len(partition.Tokens) > 0 {
					require.Same(t, &partition.Tokens[0], &legacy.Partitions[id].Tokens[0])
					require.Same(t, &partition.Tokens[0], &cloned.Partitions[id].Tokens[0])
				}
			}
			if cloned.Partitions != nil {
				delete(cloned.Partitions, 1)
				cloned.Partitions[99] = PartitionDesc{Id: 99}
			}
			if cloned.Owners != nil {
				delete(cloned.Owners, "active")
				cloned.Owners["new"] = OwnerDesc{OwnedPartition: 99}
			}
			require.Equal(t, legacy, desc)
		})
	}
}

// Sparse maps complement the full memberlist benchmarks: tombstone removal leaves
// capacity behind, and copying that capacity would penalize rings after shrinkage.
func BenchmarkPartitionRingDescCloneAfterShrink(b *testing.B) {
	for _, remaining := range []int{0, 1, 32} {
		b.Run(fmt.Sprintf("remaining=%d", remaining), func(b *testing.B) {
			desc := NewPartitionRingDesc()
			for i := 0; i < 4096; i++ {
				desc.Partitions[int32(i)] = PartitionDesc{Id: int32(i)}
				desc.Owners[fmt.Sprint(i)] = OwnerDesc{OwnedPartition: int32(i)}
			}
			for i := remaining; i < 4096; i++ {
				delete(desc.Partitions, int32(i))
				delete(desc.Owners, fmt.Sprint(i))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cloned := desc.Clone().(*PartitionRingDesc)
				if len(cloned.Partitions) != remaining || len(cloned.Owners) != remaining {
					b.Fatal("unexpected ring size")
				}
			}
		})
	}
}
