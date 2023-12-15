package ring

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionDesc_State(t *testing.T) {
	type testcase struct {
		active, inactive, deleted int64
		expectedState             PartitionState
		expectedTimestamp         int64
	}

	for _, tc := range []testcase{
		{active: 0, inactive: 0, deleted: 0, expectedState: PartitionActive, expectedTimestamp: 0},
		{active: 10, inactive: 0, deleted: 0, expectedState: PartitionActive, expectedTimestamp: 10},
		{active: 10, inactive: 10, deleted: 0, expectedState: PartitionActive, expectedTimestamp: 10},
		{active: 10, inactive: 10, deleted: 10, expectedState: PartitionActive, expectedTimestamp: 10},
		{active: 10, inactive: 5, deleted: 10, expectedState: PartitionActive, expectedTimestamp: 10},
		{active: 10, inactive: 5, deleted: 5, expectedState: PartitionActive, expectedTimestamp: 10},
		{active: 0, inactive: 10, deleted: 0, expectedState: PartitionInactive, expectedTimestamp: 10},
		{active: 0, inactive: 10, deleted: 10, expectedState: PartitionInactive, expectedTimestamp: 10},
		{active: 5, inactive: 10, deleted: 10, expectedState: PartitionInactive, expectedTimestamp: 10},
		{active: 5, inactive: 10, deleted: 0, expectedState: PartitionInactive, expectedTimestamp: 10},
		{active: 0, inactive: 0, deleted: 10, expectedState: PartitionDeleted, expectedTimestamp: 10},
		{active: 0, inactive: 5, deleted: 10, expectedState: PartitionDeleted, expectedTimestamp: 10},
		{active: 5, inactive: 0, deleted: 10, expectedState: PartitionDeleted, expectedTimestamp: 10},
		{active: 5, inactive: 5, deleted: 10, expectedState: PartitionDeleted, expectedTimestamp: 10},
	} {
		t.Run(fmt.Sprintf("%d_%d_%d", tc.active, tc.inactive, tc.deleted), func(t *testing.T) {
			desc := PartitionDesc{
				ActiveSince:   tc.active,
				InactiveSince: tc.inactive,
				DeletedSince:  tc.deleted,
			}

			s, ts := desc.State()
			require.Equal(t, tc.expectedState, s)
			require.Equal(t, tc.expectedTimestamp, ts)
		})
	}
}
