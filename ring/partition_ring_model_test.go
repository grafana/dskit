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
	}

	for _, tc := range []testcase{
		{active: 0, inactive: 0, deleted: 0, expectedState: PartitionActive},
		{active: 10, inactive: 0, deleted: 0, expectedState: PartitionActive},
		{active: 10, inactive: 10, deleted: 0, expectedState: PartitionActive},
		{active: 10, inactive: 10, deleted: 10, expectedState: PartitionActive},
		{active: 10, inactive: 5, deleted: 10, expectedState: PartitionActive},
		{active: 10, inactive: 5, deleted: 5, expectedState: PartitionActive},
		{active: 0, inactive: 10, deleted: 0, expectedState: PartitionInactive},
		{active: 0, inactive: 10, deleted: 10, expectedState: PartitionInactive},
		{active: 5, inactive: 10, deleted: 10, expectedState: PartitionInactive},
		{active: 5, inactive: 10, deleted: 0, expectedState: PartitionInactive},
		{active: 0, inactive: 0, deleted: 10, expectedState: PartitionDeleted},
		{active: 0, inactive: 5, deleted: 10, expectedState: PartitionDeleted},
		{active: 5, inactive: 0, deleted: 10, expectedState: PartitionDeleted},
		{active: 5, inactive: 5, deleted: 10, expectedState: PartitionDeleted},
	} {
		t.Run(fmt.Sprintf("%d_%d_%d", tc.active, tc.inactive, tc.deleted), func(t *testing.T) {
			desc := PartitionDesc{
				ActiveSince:   tc.active,
				InactiveSince: tc.inactive,
				DeletedSince:  tc.deleted,
			}

			require.Equal(t, tc.expectedState, desc.State())
		})
	}
}
