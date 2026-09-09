package ring

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMergeWithTime_FutureTimestampGuard verifies that a ring entry whose heartbeat timestamp is
// implausibly far in the future (e.g. after clock corruption, grafana/loki#21733) can be reclaimed
// by a normal now-dated write, and can never win a merge itself.
func TestMergeWithTime_FutureTimestampGuard(t *testing.T) {
	const now = int64(1_700_000_000) // fixed reference time
	skew := int64(acceptableClockSkew / time.Second)

	corrupt := now + skew + 3600 // well beyond the acceptable skew: corrupted
	nearBoundary := now + skew   // exactly at the boundary: still acceptable
	justOver := now + skew + 1   // just past the boundary: corrupted

	entry := func(ts int64, tokens ...uint32) InstanceDesc {
		return InstanceDesc{Addr: "addr", State: ACTIVE, Timestamp: ts, Tokens: tokens}
	}

	tests := map[string]struct {
		stored   *InstanceDesc // nil => instance missing from the local ring
		incoming InstanceDesc
		wantTS   int64 // expected timestamp of the merged entry
	}{
		"reclaim: a now write beats a corrupted future entry even though it is older": {
			stored:   ptr(entry(corrupt, 1, 2, 3)),
			incoming: entry(now, 1, 2, 3),
			wantTS:   now,
		},
		"reject: a corrupted future entry cannot beat a valid stored entry": {
			stored:   ptr(entry(now, 1, 2, 3)),
			incoming: entry(corrupt, 1, 2, 3),
			wantTS:   now,
		},
		"both acceptable: newer incoming wins (unchanged behaviour)": {
			stored:   ptr(entry(now-10, 1, 2, 3)),
			incoming: entry(now, 1, 2, 3),
			wantTS:   now,
		},
		"both acceptable: older incoming loses (unchanged behaviour)": {
			stored:   ptr(entry(now, 1, 2, 3)),
			incoming: entry(now-10, 1, 2, 3),
			wantTS:   now,
		},
		"both corrupted: higher timestamp wins (documented, deterministic)": {
			stored:   ptr(entry(corrupt, 1, 2, 3)),
			incoming: entry(corrupt+10, 1, 2, 3),
			wantTS:   corrupt + 10,
		},
		"missing slot: a corrupted future entry is still admitted (keeps merge commutative)": {
			// A future-corrupt entry into an absent slot is admitted, matching the pre-existing
			// "newer than the zero value wins" behaviour, so non-CAS merges stay commutative. It is
			// healed once the owner writes a plausible timestamp (see the reclaim case above).
			stored:   nil,
			incoming: entry(corrupt, 1, 2, 3),
			wantTS:   corrupt,
		},
		"missing slot: a valid entry is admitted (unchanged behaviour)": {
			stored:   nil,
			incoming: entry(now, 1, 2, 3),
			wantTS:   now,
		},
		"boundary: an entry exactly at now+skew is still acceptable": {
			stored:   ptr(entry(corrupt, 1, 2, 3)),
			incoming: entry(nearBoundary, 1, 2, 3),
			wantTS:   nearBoundary,
		},
		"boundary: an entry one second past the boundary is corrupted": {
			stored:   ptr(entry(now, 1, 2, 3)),
			incoming: entry(justOver, 1, 2, 3),
			wantTS:   now, // justOver rejected
		},
		"reclaim heals even when the tokens are identical": {
			stored:   ptr(entry(corrupt, 1, 2, 3)),
			incoming: entry(now, 1, 2, 3),
			wantTS:   now,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			stored := &Desc{Ingesters: map[string]InstanceDesc{}}
			if tc.stored != nil {
				stored.Ingesters["instance"] = *tc.stored
			}
			incoming := &Desc{Ingesters: map[string]InstanceDesc{"instance": tc.incoming}}

			_, err := stored.mergeWithTime(incoming, false, time.Unix(now, 0))
			assert.NoError(t, err)

			got, ok := stored.Ingesters["instance"]
			require.True(t, ok)
			assert.Equal(t, tc.wantTS, got.Timestamp)
		})
	}
}

// TestMergeWithTime_FutureTimestampGuard_Converges verifies that merging in either order converges
// to the healed (valid) timestamp, so the guard does not break gossip convergence.
func TestMergeWithTime_FutureTimestampGuard_Converges(t *testing.T) {
	const now = int64(1_700_000_000)
	corrupt := now + int64(acceptableClockSkew/time.Second) + 3600

	corruptRing := func() *Desc {
		return &Desc{Ingesters: map[string]InstanceDesc{
			"instance": {Addr: "addr", State: ACTIVE, Timestamp: corrupt, Tokens: []uint32{1, 2, 3}},
		}}
	}
	healedRing := func() *Desc {
		return &Desc{Ingesters: map[string]InstanceDesc{
			"instance": {Addr: "addr", State: ACTIVE, Timestamp: now, Tokens: []uint32{1, 2, 3}},
		}}
	}

	// A node holding the corrupted entry receives the healed one: it must adopt the healed value.
	{
		d := corruptRing()
		_, err := d.mergeWithTime(healedRing(), false, time.Unix(now, 0))
		assert.NoError(t, err)
		assert.Equal(t, now, d.Ingesters["instance"].Timestamp)
	}

	// A node holding the healed entry receives the corrupted one: it must keep the healed value,
	// so the corrupted timestamp can never re-propagate.
	{
		d := healedRing()
		_, err := d.mergeWithTime(corruptRing(), false, time.Unix(now, 0))
		assert.NoError(t, err)
		assert.Equal(t, now, d.Ingesters["instance"].Timestamp)
	}
}

// TestMergeWithTime_LeftExceptionPreserved verifies the "accept LEFT even if the timestamp did not
// change" behaviour is unaffected by the future-timestamp guard.
func TestMergeWithTime_LeftExceptionPreserved(t *testing.T) {
	const now = int64(1_700_000_000)

	stored := &Desc{Ingesters: map[string]InstanceDesc{
		"instance": {Addr: "addr", State: ACTIVE, Timestamp: now, Tokens: []uint32{1, 2, 3}},
	}}
	incoming := &Desc{Ingesters: map[string]InstanceDesc{
		"instance": {Addr: "addr", State: LEFT, Timestamp: now},
	}}

	_, err := stored.mergeWithTime(incoming, false, time.Unix(now, 0))
	assert.NoError(t, err)
	assert.Equal(t, LEFT, stored.Ingesters["instance"].State)
}

// TestMergeWithTime_GuardIsTimeRelative documents that the future-timestamp guard is evaluated
// against the current time: a timestamp that is implausibly far ahead now is compared normally again
// once the wall clock has advanced past it. This is intentional — a value that is no longer
// distinguishable from a legitimate heartbeat must be treated like one — and it is why a genuinely
// corrupted (years-ahead) timestamp is healed and propagated long before it could ever be re-accepted,
// while any re-acceptance of a value only just beyond the window is bounded by acceptableClockSkew.
func TestMergeWithTime_GuardIsTimeRelative(t *testing.T) {
	const now = int64(1_700_000_000)
	skew := int64(acceptableClockSkew / time.Second)
	future := now + skew + 3600 // one hour beyond the acceptable window at `now`

	ring := func(ts int64) *Desc {
		return &Desc{Ingesters: map[string]InstanceDesc{
			"instance": {Addr: "addr", State: ACTIVE, Timestamp: ts, Tokens: []uint32{1, 2, 3}},
		}}
	}

	// At `now` the future timestamp is implausible, so a now-dated write reclaims the entry.
	stored := ring(future)
	_, err := stored.mergeWithTime(ring(now), false, time.Unix(now, 0))
	assert.NoError(t, err)
	assert.Equal(t, now, stored.Ingesters["instance"].Timestamp)

	// Once the wall clock has advanced past it, the same timestamp is within the acceptable window
	// again and is compared normally, so a lingering copy can win on merit.
	stored = ring(now)
	_, err = stored.mergeWithTime(ring(future), false, time.Unix(future, 0))
	assert.NoError(t, err)
	assert.Equal(t, future, stored.Ingesters["instance"].Timestamp)
}

func ptr(d InstanceDesc) *InstanceDesc { return &d }
