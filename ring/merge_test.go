package ring

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNormalizationAndConflictResolution(t *testing.T) {
	now := time.Now().Unix()

	first := &Desc{
		Ingesters: map[string]InstanceDesc{
			"Ing 1":   {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{50, 40, 40, 30}},
			"Ing 2":   {Addr: "addr2", Timestamp: 123456, State: LEAVING, Tokens: []uint32{100, 5, 5, 100, 100, 200, 20, 10}},
			"Ing 3":   {Addr: "addr3", Timestamp: now, State: LEFT, Tokens: []uint32{100, 200, 300}},
			"Ing 4":   {Addr: "addr4", Timestamp: now, State: LEAVING, Tokens: []uint32{30, 40, 50}},
			"Unknown": {Tokens: []uint32{100}},
		},
	}

	second := &Desc{
		Ingesters: map[string]InstanceDesc{
			"Unknown": {
				Timestamp: now + 10,
				Tokens:    []uint32{1000, 2000},
			},
		},
	}

	change, err := first.Merge(second, false)
	if err != nil {
		t.Fatal(err)
	}
	changeRing := (*Desc)(nil)
	if change != nil {
		changeRing = change.(*Desc)
	}

	assert.Equal(t, &Desc{
		Ingesters: map[string]InstanceDesc{
			"Ing 1":   {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
			"Ing 2":   {Addr: "addr2", Timestamp: 123456, State: LEAVING, Tokens: []uint32{5, 10, 20, 100, 200}},
			"Ing 3":   {Addr: "addr3", Timestamp: now, State: LEFT},
			"Ing 4":   {Addr: "addr4", Timestamp: now, State: LEAVING},
			"Unknown": {Timestamp: now + 10, Tokens: []uint32{1000, 2000}},
		},
	}, first)

	assert.Equal(t, &Desc{
		// change ring is always normalized, "Unknown" ingester has lost two tokens: 100 from first ring (because of second ring), and 1000 (conflict resolution)
		Ingesters: map[string]InstanceDesc{
			"Unknown": {Timestamp: now + 10, Tokens: []uint32{1000, 2000}},
		},
	}, changeRing)
}

func merge(ring1, ring2 *Desc) (*Desc, *Desc) {
	change, err := ring1.Merge(ring2, false)
	if err != nil {
		panic(err)
	}

	if change == nil {
		return ring1, nil
	}

	changeRing := change.(*Desc)
	return ring1, changeRing
}

func TestMerge(t *testing.T) {
	now := time.Now().Unix()

	firstRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: JOINING, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	secondRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 3": {Addr: "addr3", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{150, 250, 350}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	thirdRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: LEAVING, Tokens: []uint32{30, 40, 50}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{150, 250, 350}},
			},
		}
	}

	expectedFirstSecondMerge := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{150, 250, 350}},
			},
		}
	}

	expectedFirstSecondThirdMerge := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: LEAVING, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{150, 250, 350}},
			},
		}
	}

	fourthRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: LEFT, Tokens: []uint32{30, 40, 50}},
			},
		}
	}

	expectedFirstSecondThirdFourthMerge := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: LEFT, Tokens: nil},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{150, 250, 350}},
			},
		}
	}

	{
		our, ch := merge(firstRing(), secondRing())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		assert.Equal(t, secondRing(), ch) // entire second ring is new
	}

	{ // idempotency: (no change after applying same ring again)
		our, ch := merge(expectedFirstSecondMerge(), secondRing())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		assert.Equal(t, (*Desc)(nil), ch)
	}

	{ // commutativity: Merge(first, second) == Merge(second, first)
		our, ch := merge(secondRing(), firstRing())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		// when merging first into second ring, only "Ing 1" is new
		assert.Equal(t, &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
			},
		}, ch)
	}

	{ // associativity: Merge(Merge(first, second), third) == Merge(first, Merge(second, third))
		our1, _ := merge(firstRing(), secondRing())
		our1, _ = merge(our1, thirdRing())
		assert.Equal(t, expectedFirstSecondThirdMerge(), our1)

		our2, _ := merge(secondRing(), thirdRing())
		our2, _ = merge(our2, firstRing())
		assert.Equal(t, expectedFirstSecondThirdMerge(), our2)
	}

	{
		out, ch := merge(expectedFirstSecondThirdMerge(), fourthRing())
		assert.Equal(t, expectedFirstSecondThirdFourthMerge(), out)
		// entire fourth ring is the update -- but without tokens
		assert.Equal(t, &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: LEFT, Tokens: nil},
			},
		}, ch)
	}
}

func TestTokensTakeover(t *testing.T) {
	now := time.Now().Unix()

	first := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: JOINING, Tokens: []uint32{5, 10, 20}}, // partially migrated from Ing 3
			},
		}
	}

	second := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 5, State: LEAVING, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	merged := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 5, State: LEAVING, Tokens: []uint32{100, 200}},
			},
		}
	}

	{
		our, ch := merge(first(), second())
		assert.Equal(t, merged(), our)
		assert.Equal(t, &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 5, State: LEAVING, Tokens: []uint32{100, 200}}, // change doesn't contain conflicted tokens
			},
		}, ch)
	}

	{ // idempotency: (no change after applying same ring again)
		our, ch := merge(merged(), second())
		assert.Equal(t, merged(), our)
		assert.Equal(t, (*Desc)(nil), ch)
	}

	{ // commutativity: (Merge(first, second) == Merge(second, first)
		our, ch := merge(second(), first())
		assert.Equal(t, merged(), our)

		// change is different though
		assert.Equal(t, &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
			},
		}, ch)
	}
}

func TestMergeLeft(t *testing.T) {
	now := time.Now().Unix()

	firstRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: JOINING, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	// Not normalised because it contains duplicate and unsorted tokens.
	firstRingNotNormalised := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: JOINING, Tokens: []uint32{20, 10, 5, 10, 20, 100, 200, 100}},
			},
		}
	}

	secondRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 2": {Addr: "addr2", Timestamp: now, State: LEFT},
			},
		}
	}

	// Not normalised because it contains a LEFT ingester with tokens.
	secondRingNotNormalised := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 2": {Addr: "addr2", Timestamp: now, State: LEFT, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	expectedFirstSecondMerge := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: LEFT},
			},
		}
	}

	thirdRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: LEAVING, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: JOINING, Tokens: []uint32{5, 10, 20, 100, 200}}, // from firstRing
			},
		}
	}

	expectedFirstSecondThirdMerge := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: LEAVING, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: LEFT},
			},
		}
	}

	{
		our, ch := merge(firstRing(), secondRing())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		assert.Equal(t, &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 2": {Addr: "addr2", Timestamp: now, State: LEFT},
			},
		}, ch)
	}
	{
		// Should yield same result when RHS is not normalised.
		our, ch := merge(firstRing(), secondRingNotNormalised())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		assert.Equal(t, &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 2": {Addr: "addr2", Timestamp: now, State: LEFT},
			},
		}, ch)

	}

	{ // idempotency: (no change after applying same ring again)
		our, ch := merge(expectedFirstSecondMerge(), secondRing())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		assert.Equal(t, (*Desc)(nil), ch)
	}

	{ // commutativity: Merge(first, second) == Merge(second, first)
		our, ch := merge(secondRing(), firstRing())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		// when merging first into second ring, only "Ing 1" is new
		assert.Equal(t, &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
			},
		}, ch)
	}
	{
		// Should yield same result when RHS is not normalised.
		our, ch := merge(secondRing(), firstRingNotNormalised())
		assert.Equal(t, expectedFirstSecondMerge(), our)
		// when merging first into second ring, only "Ing 1" is new
		assert.Equal(t, &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
			},
		}, ch)

	}

	{ // associativity: Merge(Merge(first, second), third) == Merge(first, Merge(second, third))
		our1, _ := merge(firstRing(), secondRing())
		our1, _ = merge(our1, thirdRing())
		assert.Equal(t, expectedFirstSecondThirdMerge(), our1)

		our2, _ := merge(secondRing(), thirdRing())
		our2, _ = merge(our2, firstRing())
		assert.Equal(t, expectedFirstSecondThirdMerge(), our2)
	}
}

func TestMergeRemoveMissing(t *testing.T) {
	now := time.Now().Unix()

	firstRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now, State: JOINING, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now, State: LEAVING, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	secondRing := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	expectedFirstSecondMerge := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 3, State: LEFT}, // When deleting, time depends on value passed to merge function.
			},
		}
	}

	{
		our, ch := mergeLocalCAS(firstRing(), secondRing(), now+3)
		assert.Equal(t, expectedFirstSecondMerge(), our)
		assert.Equal(t, &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now + 3, State: LEFT}, // When deleting, time depends on value passed to merge function.
			},
		}, ch) // entire second ring is new
	}

	{ // idempotency: (no change after applying same ring again, even if time has advanced)
		our, ch := mergeLocalCAS(expectedFirstSecondMerge(), secondRing(), now+10)
		assert.Equal(t, expectedFirstSecondMerge(), our)
		assert.Equal(t, (*Desc)(nil), ch)
	}

	{ // commutativity is broken when deleting missing entries. But let's make sure we get reasonable results at least.
		our, ch := mergeLocalCAS(secondRing(), firstRing(), now+3)
		assert.Equal(t, &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now, State: LEAVING},
			},
		}, our)

		assert.Equal(t, &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 3": {Addr: "addr3", Timestamp: now, State: LEAVING},
			},
		}, ch)
	}
}

func TestMergeMissingIntoLeft(t *testing.T) {
	now := time.Now().Unix()

	ring1 := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 5, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now, State: LEFT},
			},
		}
	}

	ring2 := func() *Desc {
		return &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
			},
		}
	}

	{
		our, ch := mergeLocalCAS(ring1(), ring2(), now+10)
		assert.Equal(t, &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				"Ing 3": {Addr: "addr3", Timestamp: now, State: LEFT},
			},
		}, our)

		assert.Equal(t, &Desc{
			Ingesters: map[string]InstanceDesc{
				"Ing 1": {Addr: "addr1", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
				"Ing 2": {Addr: "addr2", Timestamp: now + 10, State: ACTIVE, Tokens: []uint32{5, 10, 20, 100, 200}},
				// Ing 3 is not changed, it was already LEFT
			},
		}, ch)
	}
}

func TestMergeFarFutureTimestamp_IncomingClamped(t *testing.T) {
	now := time.Now()
	nowUnix := now.Unix()
	farFuture := time.Date(2034, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	// Local ring has instance with current timestamp.
	local := &Desc{
		Ingesters: map[string]InstanceDesc{
			"Ing 1": {Addr: "addr1", Timestamp: nowUnix, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
		},
	}

	// Incoming ring has same instance with a corrupted far-future timestamp.
	incoming := &Desc{
		Ingesters: map[string]InstanceDesc{
			"Ing 1": {Addr: "addr1", Timestamp: farFuture, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
		},
	}

	change, err := local.mergeWithTime(incoming, false, now)
	assert.NoError(t, err)

	// The incoming far-future timestamp should be clamped to now.
	// Since clamped incoming (now) == local (now), no change from the incoming side.
	// But the timestamps are equal so no update from the merge loop.
	ing := local.Ingesters["Ing 1"]
	assert.Equal(t, nowUnix, ing.Timestamp, "local timestamp should remain current")
	assert.Nil(t, change, "no change expected when clamped incoming equals local")
}

func TestMergeFarFutureTimestamp_LocalClamped(t *testing.T) {
	now := time.Now()
	nowUnix := now.Unix()
	farFuture := time.Date(2034, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	// Local ring has instance with corrupted far-future timestamp.
	local := &Desc{
		Ingesters: map[string]InstanceDesc{
			"Ing 1": {Addr: "addr1", Timestamp: farFuture, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
		},
	}

	// Incoming ring has same instance with a current timestamp (slightly newer).
	incoming := &Desc{
		Ingesters: map[string]InstanceDesc{
			"Ing 1": {Addr: "addr1", Timestamp: nowUnix + 5, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
		},
	}

	change, err := local.mergeWithTime(incoming, false, now)
	assert.NoError(t, err)
	assert.NotNil(t, change, "change expected: local was clamped and incoming wins")

	// Local entry should now have the incoming's timestamp (which is nowUnix+5, after clamping local to nowUnix).
	ing := local.Ingesters["Ing 1"]
	assert.Equal(t, nowUnix+5, ing.Timestamp, "incoming should win after local is clamped")
}

func TestMergeFarFutureTimestamp_LocalClampedSelfHealing(t *testing.T) {
	now := time.Now()
	nowUnix := now.Unix()
	farFuture := time.Date(2034, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	// Local ring has instance with corrupted far-future timestamp.
	local := &Desc{
		Ingesters: map[string]InstanceDesc{
			"Ing 1": {Addr: "addr1", Timestamp: farFuture, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
		},
	}

	// Incoming ring has a DIFFERENT instance only (no mention of Ing 1).
	incoming := &Desc{
		Ingesters: map[string]InstanceDesc{
			"Ing 2": {Addr: "addr2", Timestamp: nowUnix, State: ACTIVE, Tokens: []uint32{100, 200}},
		},
	}

	change, err := local.mergeWithTime(incoming, false, now)
	assert.NoError(t, err)
	assert.NotNil(t, change, "change expected: Ing 1 was clamped, Ing 2 is new")

	// Ing 1's corrupted timestamp should be clamped to now.
	ing1 := local.Ingesters["Ing 1"]
	assert.Equal(t, nowUnix, ing1.Timestamp, "corrupted local timestamp should be clamped to now")

	// The change should include both the clamped Ing 1 and the new Ing 2.
	changeDesc := change.(*Desc)
	assert.Contains(t, changeDesc.Ingesters, "Ing 1", "clamped entry should be in change for gossip propagation")
	assert.Contains(t, changeDesc.Ingesters, "Ing 2", "new entry should be in change")
}

func TestMergeFarFutureTimestamp_LocalCASMarkLeft(t *testing.T) {
	now := time.Now()
	nowUnix := now.Unix()
	farFuture := time.Date(2034, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	// Local ring has instance with corrupted far-future timestamp.
	local := &Desc{
		Ingesters: map[string]InstanceDesc{
			"Ing 1": {Addr: "addr1", Timestamp: farFuture, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
			"Ing 2": {Addr: "addr2", Timestamp: nowUnix, State: ACTIVE, Tokens: []uint32{100, 200}},
		},
	}

	// Incoming ring does NOT have Ing 1 (it's gone), localCAS should mark it LEFT.
	incoming := &Desc{
		Ingesters: map[string]InstanceDesc{
			"Ing 2": {Addr: "addr2", Timestamp: nowUnix, State: ACTIVE, Tokens: []uint32{100, 200}},
		},
	}

	change, err := local.mergeWithTime(incoming, true, now)
	assert.NoError(t, err)
	assert.NotNil(t, change)

	// Ing 1 should be marked LEFT with a current timestamp (not the far-future one).
	ing1 := local.Ingesters["Ing 1"]
	assert.Equal(t, LEFT, ing1.State, "missing instance should be marked LEFT via localCAS")
	assert.Equal(t, nowUnix, ing1.Timestamp, "timestamp should be current, not far-future")
	assert.Nil(t, ing1.Tokens, "LEFT instance should have no tokens")
}

func TestMergeFarFutureTimestamp_LegitimateClockSkew(t *testing.T) {
	now := time.Now()
	nowUnix := now.Unix()

	// Timestamp 30 minutes in the future (within allowed drift) should NOT be clamped.
	slightlyFuture := nowUnix + 1800

	local := &Desc{
		Ingesters: map[string]InstanceDesc{
			"Ing 1": {Addr: "addr1", Timestamp: nowUnix, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
		},
	}

	incoming := &Desc{
		Ingesters: map[string]InstanceDesc{
			"Ing 1": {Addr: "addr1", Timestamp: slightlyFuture, State: ACTIVE, Tokens: []uint32{30, 40, 50}},
		},
	}

	change, err := local.mergeWithTime(incoming, false, now)
	assert.NoError(t, err)
	assert.NotNil(t, change, "incoming with slightly future timestamp should win")

	ing := local.Ingesters["Ing 1"]
	assert.Equal(t, slightlyFuture, ing.Timestamp, "legitimate clock skew should be accepted without clamping")
}

func mergeLocalCAS(ring1, ring2 *Desc, nowUnixTime int64) (*Desc, *Desc) {
	change, err := ring1.mergeWithTime(ring2, true, time.Unix(nowUnixTime, 0))
	if err != nil {
		panic(err)
	}

	if change == nil {
		return ring1, nil
	}

	changeRing := change.(*Desc)
	return ring1, changeRing
}
