package memberlist

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPropagationTrackerDesc_Merge_AddBeacon(t *testing.T) {
	tests := map[string]struct {
		local                *PropagationTrackerDesc
		incoming             *PropagationTrackerDesc
		expectedUpdatedLocal Mergeable
		expectedChange       Mergeable
	}{
		"the first beacon is added": {
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{},
			},
			incoming: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
				},
			},
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
				},
			},
			expectedChange: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
				},
			},
		},
		"a new beacon is added to existing beacons": {
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
				},
			},
			incoming: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20},
				},
			},
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20},
				},
			},
			expectedChange: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					2: {PublishedAt: 20},
				},
			},
		},
		"multiple new beacons are added": {
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
				},
			},
			incoming: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20},
					3: {PublishedAt: 30},
				},
			},
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20},
					3: {PublishedAt: 30},
				},
			},
			expectedChange: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					2: {PublishedAt: 20},
					3: {PublishedAt: 30},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, localCAS := range []bool{true, false} {
				t.Run(fmt.Sprintf("localCAS=%t", localCAS), func(t *testing.T) {
					localCopy := testData.local.Clone()
					incomingCopy := testData.incoming.Clone()

					change, err := localCopy.Merge(incomingCopy, localCAS)
					require.NoError(t, err)
					assert.Equal(t, testData.expectedUpdatedLocal, localCopy)
					assert.Equal(t, testData.expectedChange, change)
				})
			}
		})
	}
}

func TestPropagationTrackerDesc_Merge_UpdateBeacon(t *testing.T) {
	tests := map[string]struct {
		local                *PropagationTrackerDesc
		incoming             *PropagationTrackerDesc
		expectedUpdatedLocal Mergeable
		expectedChange       Mergeable
	}{
		"beacon updated with newer timestamp": {
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20},
				},
			},
			incoming: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 30}, // Updated with newer timestamp.
				},
			},
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 30},
				},
			},
			expectedChange: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					2: {PublishedAt: 30},
				},
			},
		},
		"beacon not updated with older timestamp": {
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20},
				},
			},
			incoming: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 15}, // Older timestamp, should be ignored.
				},
			},
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20},
				},
			},
			expectedChange: nil,
		},
		"beacon not updated with equal timestamp": {
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20},
				},
			},
			incoming: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20}, // Same timestamp, no change.
				},
			},
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20},
				},
			},
			expectedChange: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, localCAS := range []bool{true, false} {
				t.Run(fmt.Sprintf("localCAS=%t", localCAS), func(t *testing.T) {
					localCopy := testData.local.Clone()
					incomingCopy := testData.incoming.Clone()

					change, err := localCopy.Merge(incomingCopy, localCAS)
					require.NoError(t, err)
					assert.Equal(t, testData.expectedUpdatedLocal, localCopy)
					assert.Equal(t, testData.expectedChange, change)
				})
			}
		})
	}
}

func TestPropagationTrackerDesc_Merge_DeleteBeacon(t *testing.T) {
	now := time.Unix(10000, 0)

	tests := map[string]struct {
		localCAS             bool
		local                *PropagationTrackerDesc
		incoming             *PropagationTrackerDesc
		expectedUpdatedLocal Mergeable
		expectedChange       Mergeable
	}{
		"local change: beacon removed and local beacon is not deleted yet": {
			localCAS: true,
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20},
				},
			},
			incoming: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					// Beacon 2 removed.
				},
			},
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20, DeletedAt: now.UnixMilli()},
				},
			},
			expectedChange: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					2: {PublishedAt: 20, DeletedAt: now.UnixMilli()},
				},
			},
		},
		"local change: beacon removed and local beacon is already deleted": {
			localCAS: true,
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20, DeletedAt: 100}, // Already deleted.
				},
			},
			incoming: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					// Beacon 2 removed.
				},
			},
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20, DeletedAt: 100},
				},
			},
			expectedChange: nil,
		},
		"incoming: beacon deleted with newer timestamp": {
			localCAS: false,
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20},
				},
			},
			incoming: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 30, DeletedAt: 30}, // Deleted with newer timestamp.
				},
			},
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 30, DeletedAt: 30},
				},
			},
			expectedChange: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					2: {PublishedAt: 30, DeletedAt: 30},
				},
			},
		},
		"incoming: beacon deleted with equal timestamp, deletion should win": {
			localCAS: false,
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20},
				},
			},
			incoming: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20, DeletedAt: 20}, // Deleted with equal timestamp.
				},
			},
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20, DeletedAt: 20},
				},
			},
			expectedChange: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					2: {PublishedAt: 20, DeletedAt: 20},
				},
			},
		},
		"incoming: beacon deleted with older timestamp, should be ignored": {
			localCAS: false,
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20},
				},
			},
			incoming: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 10, DeletedAt: 10}, // Deleted with older timestamp.
				},
			},
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
					2: {PublishedAt: 20},
				},
			},
			expectedChange: nil,
		},
		"incoming: active beacon does not override tombstone with equal timestamp": {
			localCAS: false,
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 20, DeletedAt: 20}, // Local is tombstone.
				},
			},
			incoming: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 20, DeletedAt: 0}, // Incoming is active with same timestamp.
				},
			},
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 20, DeletedAt: 20}, // Tombstone preserved.
				},
			},
			expectedChange: nil,
		},
		"incoming: active beacon with newer timestamp overrides tombstone": {
			localCAS: false,
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 20, DeletedAt: 20}, // Local is tombstone.
				},
			},
			incoming: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 30, DeletedAt: 0}, // Incoming is active with newer timestamp.
				},
			},
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 30, DeletedAt: 0}, // Active wins.
				},
			},
			expectedChange: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 30, DeletedAt: 0},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			localCopy := testData.local.Clone().(*PropagationTrackerDesc)
			incomingCopy := testData.incoming.Clone()

			change, err := localCopy.mergeWithTime(incomingCopy, testData.localCAS, now)
			require.NoError(t, err)
			assert.Equal(t, testData.expectedUpdatedLocal, localCopy)
			assert.Equal(t, testData.expectedChange, change)
		})
	}
}

func TestPropagationTrackerDesc_Merge_EdgeCases(t *testing.T) {
	tests := map[string]struct {
		local                *PropagationTrackerDesc
		incoming             Mergeable
		expectedUpdatedLocal Mergeable
		expectedChange       Mergeable
		expectedError        string
	}{
		"nil incoming": {
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
				},
			},
			incoming: nil,
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
				},
			},
			expectedChange: nil,
		},
		"typed nil incoming": {
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
				},
			},
			incoming: (*PropagationTrackerDesc)(nil),
			expectedUpdatedLocal: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: 10},
				},
			},
			expectedChange: nil,
		},
		"wrong type": {
			local: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{},
			},
			incoming:      &testMergeable{},
			expectedError: "expected *PropagationTrackerDesc, got *memberlist.testMergeable",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			localCopy := testData.local.Clone()

			change, err := localCopy.Merge(testData.incoming, false)

			if testData.expectedError != "" {
				require.EqualError(t, err, testData.expectedError)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, testData.expectedUpdatedLocal, localCopy)
			assert.Equal(t, testData.expectedChange, change)
		})
	}
}

type testMergeable struct{}

func (t *testMergeable) Merge(_ Mergeable, _ bool) (Mergeable, error) { return nil, nil }
func (t *testMergeable) MergeContent() []string                       { return nil }
func (t *testMergeable) RemoveTombstones(_ time.Time) (int, int)      { return 0, 0 }
func (t *testMergeable) Clone() Mergeable                             { return &testMergeable{} }

func TestPropagationTrackerDesc_MergeContent(t *testing.T) {
	d := NewPropagationTrackerDesc()
	d.Beacons[1000] = BeaconDesc{PublishedAt: 1000}
	d.Beacons[2000] = BeaconDesc{PublishedAt: 2000}
	d.Beacons[3000] = BeaconDesc{PublishedAt: 3000}

	content := d.MergeContent()
	assert.Len(t, content, 3)
	// Content is beacon IDs as strings.
	assert.Contains(t, content, "1000")
	assert.Contains(t, content, "2000")
	assert.Contains(t, content, "3000")
}

func TestPropagationTrackerDesc_RemoveTombstones(t *testing.T) {
	now := time.Now()
	limit := now.Add(-5 * time.Minute)

	tests := map[string]struct {
		desc           *PropagationTrackerDesc
		limit          time.Time
		expectedDesc   *PropagationTrackerDesc
		expectedTotal  int
		expectedRemove int
	}{
		"zero limit removes all tombstones": {
			desc: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: now.UnixMilli(), DeletedAt: 0},                                      // Active.
					2: {PublishedAt: now.Add(-10 * time.Minute).UnixMilli(), DeletedAt: 0},               // Active (old).
					3: {PublishedAt: now.UnixMilli(), DeletedAt: now.UnixMilli()},                        // Tombstone (recent).
					4: {PublishedAt: now.UnixMilli(), DeletedAt: now.Add(-10 * time.Minute).UnixMilli()}, // Tombstone (old).
				},
			},
			limit: time.Time{}, // Zero limit.
			expectedDesc: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: now.UnixMilli(), DeletedAt: 0},
					2: {PublishedAt: now.Add(-10 * time.Minute).UnixMilli(), DeletedAt: 0},
				},
			},
			expectedTotal:  0,
			expectedRemove: 2,
		},
		"non-zero limit removes old tombstones only": {
			desc: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: now.UnixMilli(), DeletedAt: 0},                                      // Active.
					2: {PublishedAt: now.Add(-10 * time.Minute).UnixMilli(), DeletedAt: 0},               // Active (old, not removed).
					3: {PublishedAt: now.UnixMilli(), DeletedAt: now.UnixMilli()},                        // Tombstone (recent, kept).
					4: {PublishedAt: now.UnixMilli(), DeletedAt: now.Add(-10 * time.Minute).UnixMilli()}, // Tombstone (old, removed).
				},
			},
			limit: limit,
			expectedDesc: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: now.UnixMilli(), DeletedAt: 0},
					2: {PublishedAt: now.Add(-10 * time.Minute).UnixMilli(), DeletedAt: 0},
					3: {PublishedAt: now.UnixMilli(), DeletedAt: now.UnixMilli()},
				},
			},
			expectedTotal:  1, // 1 tombstone remaining (beacon 3).
			expectedRemove: 1, // 1 tombstone removed (beacon 4).
		},
		"no tombstones": {
			desc: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: now.UnixMilli(), DeletedAt: 0},
					2: {PublishedAt: now.Add(-10 * time.Minute).UnixMilli(), DeletedAt: 0},
				},
			},
			limit: limit,
			expectedDesc: &PropagationTrackerDesc{
				Beacons: map[uint64]BeaconDesc{
					1: {PublishedAt: now.UnixMilli(), DeletedAt: 0},
					2: {PublishedAt: now.Add(-10 * time.Minute).UnixMilli(), DeletedAt: 0},
				},
			},
			expectedTotal:  0,
			expectedRemove: 0,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			total, removed := testData.desc.RemoveTombstones(testData.limit)
			assert.Equal(t, testData.expectedTotal, total)
			assert.Equal(t, testData.expectedRemove, removed)
			assert.Equal(t, testData.expectedDesc, testData.desc)
		})
	}
}

func TestPropagationTrackerDesc_Clone(t *testing.T) {
	d := NewPropagationTrackerDesc()
	d.Beacons[1000] = BeaconDesc{PublishedAt: 1000}
	d.Beacons[2000] = BeaconDesc{PublishedAt: 2000, DeletedAt: 100}

	cloned := d.Clone().(*PropagationTrackerDesc)

	// Verify the clone has the same data.
	assert.Equal(t, len(d.Beacons), len(cloned.Beacons))
	assert.Equal(t, d.Beacons[1000], cloned.Beacons[1000])
	assert.Equal(t, d.Beacons[2000], cloned.Beacons[2000])

	// Modify the original and verify the clone is not affected.
	d.Beacons[1000] = BeaconDesc{PublishedAt: 9999}

	assert.NotEqual(t, d.Beacons[1000], cloned.Beacons[1000])
	assert.Equal(t, int64(1000), cloned.Beacons[1000].PublishedAt)
}

func TestGetOrCreatePropagationTrackerDesc(t *testing.T) {
	t.Run("nil input returns new desc", func(t *testing.T) {
		desc := GetOrCreatePropagationTrackerDesc(nil)
		require.NotNil(t, desc)
		assert.NotNil(t, desc.Beacons)
		assert.Len(t, desc.Beacons, 0)
	})

	t.Run("existing desc is returned", func(t *testing.T) {
		existing := NewPropagationTrackerDesc()
		existing.Beacons[1000] = BeaconDesc{PublishedAt: 1000}

		desc := GetOrCreatePropagationTrackerDesc(existing)
		assert.Same(t, existing, desc)
		assert.Len(t, desc.Beacons, 1)
	})
}

func TestGetPropagationTrackerCodec(t *testing.T) {
	codec := GetPropagationTrackerCodec()
	assert.Equal(t, PropagationTrackerCodecID, codec.CodecID())

	// Test encode/decode roundtrip.
	original := NewPropagationTrackerDesc()
	original.Beacons[1234567890] = BeaconDesc{
		PublishedAt: 1234567890,
		DeletedAt:   9876543210,
	}

	encoded, err := codec.Encode(original)
	require.NoError(t, err)

	decoded, err := codec.Decode(encoded)
	require.NoError(t, err)

	decodedDesc := decoded.(*PropagationTrackerDesc)
	assert.Equal(t, original.Beacons[1234567890], decodedDesc.Beacons[1234567890])
}
