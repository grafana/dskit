package ring

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type RingMock struct {
	mock.Mock
}

func (r *RingMock) Collect(_ chan<- prometheus.Metric) {}

func (r *RingMock) Describe(_ chan<- *prometheus.Desc) {}

func (r *RingMock) Get(key uint32, op Operation, bufDescs []InstanceDesc, bufHosts, bufZones []string) (ReplicationSet, error) {
	args := r.Called(key, op, bufDescs, bufHosts, bufZones)
	return args.Get(0).(ReplicationSet), args.Error(1)
}

func (r *RingMock) GetWithOptions(key uint32, op Operation, opts ...Option) (ReplicationSet, error) {
	args := r.Called(key, op, opts)
	return args.Get(0).(ReplicationSet), args.Error(1)
}

func (r *RingMock) GetAllHealthy(op Operation) (ReplicationSet, error) {
	args := r.Called(op)
	return args.Get(0).(ReplicationSet), args.Error(1)
}

func (r *RingMock) GetReplicationSetForOperation(op Operation) (ReplicationSet, error) {
	args := r.Called(op)
	return args.Get(0).(ReplicationSet), args.Error(1)
}

func (r *RingMock) GetSubringForOperationStates(op Operation) ReadRing {
	args := r.Called(op)
	return args.Get(0).(ReadRing)
}

func (r *RingMock) ReplicationFactor() int {
	return 0
}

func (r *RingMock) InstancesCount() int {
	return 0
}

func (r *RingMock) InstancesWithTokensCount() int {
	return 0
}

func (r *RingMock) ShuffleShard(identifier string, size int) ReadRing {
	args := r.Called(identifier, size)
	return args.Get(0).(ReadRing)
}

func (r *RingMock) GetInstanceState(instanceID string) (InstanceState, error) {
	args := r.Called(instanceID)
	return args.Get(0).(InstanceState), args.Error(1)
}

func (r *RingMock) ShuffleShardWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) ReadRing {
	args := r.Called(identifier, size, lookbackPeriod, now)
	return args.Get(0).(ReadRing)
}

func (r *RingMock) HasInstance(_ string) bool {
	return true
}

func (r *RingMock) CleanupShuffleShardCache(_ string) {}

func (r *RingMock) GetTokenRangesForInstance(_ string) (TokenRanges, error) {
	return []uint32{0, math.MaxUint32}, nil
}

func (r *RingMock) InstancesInZoneCount(_ string) int {
	return 0
}

func (r *RingMock) InstancesWithTokensInZoneCount(_ string) int {
	return 0
}

func (r *RingMock) WritableInstancesWithTokensCount() int {
	return 0
}

func (r *RingMock) WritableInstancesWithTokensInZoneCount(_ string) int {
	return 0
}

func (r *RingMock) ZonesCount() int {
	return 0
}

func (r *RingMock) Zones() []string {
	return nil
}

func createStartingRing() *Ring {
	// Init the ring.
	ringDesc := &Desc{Ingesters: map[string]InstanceDesc{
		"instance-1": {Id: "instance-1", Addr: "127.0.0.1", State: ACTIVE, Timestamp: time.Now().Unix()},
		"instance-2": {Id: "instance-2", Addr: "127.0.0.2", State: PENDING, Timestamp: time.Now().Unix()},
		"instance-3": {Id: "instance-3", Addr: "127.0.0.3", State: JOINING, Timestamp: time.Now().Unix()},
		"instance-4": {Id: "instance-4", Addr: "127.0.0.4", State: LEAVING, Timestamp: time.Now().Unix()},
		"instance-5": {Id: "instance-5", Addr: "127.0.0.5", State: ACTIVE, Timestamp: time.Now().Unix()},
	}}

	ring := &Ring{
		cfg:                 Config{HeartbeatTimeout: time.Minute, ReplicationFactor: 3},
		ringDesc:            ringDesc,
		ringTokens:          ringDesc.GetTokens(),
		ringTokensByZone:    ringDesc.getTokensByZone(),
		ringInstanceByToken: ringDesc.getTokensInfo(),
		ringZones:           getZones(ringDesc.getTokensByZone()),
		strategy:            NewDefaultReplicationStrategy(),
	}

	return ring
}

func TestWaitRingStability_ShouldReturnAsSoonAsMinStabilityIsReachedOnNoChanges(t *testing.T) {
	t.Parallel()

	const (
		minStability = 2 * time.Second
		maxWaiting   = 10 * time.Second
	)

	ring := createStartingRing()

	startTime := time.Now()
	require.NoError(t, WaitRingStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, minStability)
	assert.Less(t, elapsedTime, 2*minStability)
}

func TestWaitRingTokensStability_ShouldReturnAsSoonAsMinStabilityIsReachedOnNoChanges(t *testing.T) {
	t.Parallel()

	const (
		minStability = 2 * time.Second
		maxWaiting   = 10 * time.Second
	)

	ring := createStartingRing()

	startTime := time.Now()
	require.NoError(t, WaitRingTokensStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, minStability)
	assert.Less(t, elapsedTime, 2*minStability)
}

func addInstanceAfterSomeTime(ring *Ring, addInstanceAfter time.Duration) {
	go func() {
		time.Sleep(addInstanceAfter)

		ring.mtx.Lock()
		defer ring.mtx.Unlock()
		ringDesc := ring.ringDesc
		instanceID := fmt.Sprintf("127.0.0.%d", len(ringDesc.Ingesters)+1)
		ringDesc.Ingesters[instanceID] = InstanceDesc{Addr: instanceID, State: ACTIVE, Timestamp: time.Now().Unix()}
		ring.ringDesc = ringDesc
		ring.ringTokens = ringDesc.GetTokens()
		ring.ringTokensByZone = ringDesc.getTokensByZone()
		ring.ringInstanceByToken = ringDesc.getTokensInfo()
		ring.ringZones = getZones(ringDesc.getTokensByZone())
	}()
}

func TestWaitRingStability_ShouldReturnOnceMinStabilityOfInstancesHasBeenReached(t *testing.T) {
	t.Parallel()

	const (
		minStability     = 3 * time.Second
		addInstanceAfter = 2 * time.Second
		maxWaiting       = 15 * time.Second
	)

	ring := createStartingRing()

	// Add 1 new instance after some time.
	addInstanceAfterSomeTime(ring, addInstanceAfter)

	startTime := time.Now()
	require.NoError(t, WaitRingStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, minStability+addInstanceAfter)
	assert.LessOrEqual(t, elapsedTime, minStability+addInstanceAfter+3*time.Second)
}

func TestWaitRingTokensStability_ShouldReturnOnceMinStabilityOfInstancesHasBeenReached(t *testing.T) {
	t.Parallel()

	const (
		minStability     = 3 * time.Second
		addInstanceAfter = 2 * time.Second
		maxWaiting       = 15 * time.Second
	)

	ring := createStartingRing()

	// Add 1 new instance after some time.
	addInstanceAfterSomeTime(ring, addInstanceAfter)

	startTime := time.Now()
	require.NoError(t, WaitRingTokensStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, minStability+addInstanceAfter)
	assert.LessOrEqual(t, elapsedTime, minStability+addInstanceAfter+3*time.Second)
}

func addInstancesPeriodically(ring *Ring) chan struct{} {
	// Keep changing the ring.
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Second):
				ring.mtx.Lock()
				ringDesc := ring.ringDesc
				instanceID := fmt.Sprintf("127.0.0.%d", len(ringDesc.Ingesters)+1)
				ringDesc.Ingesters[instanceID] = InstanceDesc{Id: instanceID, Addr: instanceID, State: ACTIVE, Timestamp: time.Now().Unix()}
				ring.ringDesc = ringDesc
				ring.ringTokens = ringDesc.GetTokens()
				ring.ringTokensByZone = ringDesc.getTokensByZone()
				ring.ringInstanceByToken = ringDesc.getTokensInfo()
				ring.ringZones = getZones(ringDesc.getTokensByZone())

				ring.mtx.Unlock()
			}
		}
	}()
	return done
}

func TestWaitRingStability_ShouldReturnErrorIfInstancesAddedAndMaxWaitingIsReached(t *testing.T) {
	t.Parallel()

	const (
		minStability = 2 * time.Second
		maxWaiting   = 7 * time.Second
	)

	ring := createStartingRing()

	done := addInstancesPeriodically(ring)
	defer close(done)

	startTime := time.Now()
	require.Equal(t, context.DeadlineExceeded, WaitRingStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, maxWaiting)
}

func TestWaitRingTokensStability_ShouldReturnErrorIfInstancesAddedAndMaxWaitingIsReached(t *testing.T) {
	t.Parallel()

	const (
		minStability = 2 * time.Second
		maxWaiting   = 7 * time.Second
	)

	ring := createStartingRing()

	done := addInstancesPeriodically(ring)
	defer close(done)

	startTime := time.Now()
	require.Equal(t, context.DeadlineExceeded, WaitRingTokensStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, maxWaiting)
}

// Keep changing the ring in a way to avoid repeating the same set of states for at least 2 sec
func changeStatePeriodically(ring *Ring) chan struct{} {
	done := make(chan struct{})
	go func() {
		instanceToMutate := "instance-1"
		states := []InstanceState{PENDING, JOINING, ACTIVE, LEAVING}
		stateIdx := 0

		for states[stateIdx] != ring.ringDesc.Ingesters[instanceToMutate].State {
			stateIdx++
		}

		for {
			select {
			case <-done:
				return
			case <-time.After(time.Second):
				stateIdx++
				ring.mtx.Lock()
				ringDesc := ring.ringDesc
				desc := ringDesc.Ingesters[instanceToMutate]
				desc.State = states[stateIdx%len(states)]
				desc.Timestamp = time.Now().Unix()
				ringDesc.Ingesters[instanceToMutate] = desc
				ring.mtx.Unlock()
			}
		}
	}()

	return done
}

func TestWaitRingStability_ShouldReturnErrorIfInstanceStateIsChangingAndMaxWaitingIsReached(t *testing.T) {
	t.Parallel()

	const (
		minStability = 2 * time.Second
		maxWaiting   = 7 * time.Second
	)

	ring := createStartingRing()

	// Keep changing the ring.
	done := changeStatePeriodically(ring)
	defer close(done)

	startTime := time.Now()
	require.Equal(t, context.DeadlineExceeded, WaitRingStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, maxWaiting)
}

func TestWaitRingTokensStability_ShouldReturnOnceMinStabilityOfInstancesHasBeenReachedWhileStateCanChange(t *testing.T) {
	t.Parallel()

	const (
		minStability = 2 * time.Second
		maxWaiting   = 7 * time.Second
	)

	ring := createStartingRing()

	// Keep changing the ring.
	done := changeStatePeriodically(ring)
	defer close(done)

	startTime := time.Now()
	require.NoError(t, WaitRingTokensStability(context.Background(), ring, Reporting, minStability, maxWaiting))
	elapsedTime := time.Since(startTime)

	assert.GreaterOrEqual(t, elapsedTime, minStability)
	assert.Less(t, elapsedTime, 2*minStability)
}

func TestWaitInstanceState_Timeout(t *testing.T) {
	t.Parallel()

	const (
		instanceID      = "test"
		timeoutDuration = time.Second
	)

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	ring := &RingMock{}
	ring.On("GetInstanceState", mock.Anything, mock.Anything).Return(ACTIVE, nil)

	err := WaitInstanceState(ctx, ring, instanceID, PENDING)

	assert.Equal(t, context.DeadlineExceeded, err)
	ring.AssertCalled(t, "GetInstanceState", instanceID)
}

func TestWaitInstanceState_TimeoutOnError(t *testing.T) {
	t.Parallel()

	const (
		instanceID      = "test"
		timeoutDuration = time.Second
	)

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	ring := &RingMock{}
	ring.On("GetInstanceState", mock.Anything, mock.Anything).Return(PENDING, errors.New("instance not found in the ring"))

	err := WaitInstanceState(ctx, ring, instanceID, ACTIVE)

	assert.Equal(t, context.DeadlineExceeded, err)
	ring.AssertCalled(t, "GetInstanceState", instanceID)
}

func TestWaitInstanceState_ExitsAfterActualStateEqualsState(t *testing.T) {
	t.Parallel()

	const (
		instanceID      = "test"
		timeoutDuration = time.Second
	)

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	ring := &RingMock{}
	ring.On("GetInstanceState", mock.Anything, mock.Anything).Return(ACTIVE, nil)

	err := WaitInstanceState(ctx, ring, instanceID, ACTIVE)

	assert.Nil(t, err)
	ring.AssertNumberOfCalls(t, "GetInstanceState", 1)
}

func TestGetTokenDistance(t *testing.T) {
	tests := map[string]struct {
		from     uint32
		to       uint32
		expected int64
	}{
		"whole ring between token and itself": {
			from:     10,
			to:       10,
			expected: math.MaxUint32 + 1,
		},
		"10 tokens from 0 to 10": {
			from:     0,
			to:       10,
			expected: 10,
		},
		"10 tokens from math.MaxUint32 - 10 to math.MaxUint32": {
			from:     math.MaxUint32 - 10,
			to:       math.MaxUint32,
			expected: 10,
		},
		"1 token from math.MaxUint32 and 0": {
			from:     math.MaxUint32,
			to:       0,
			expected: 1,
		},
		"21 tokens from math.MaxUint32 - 10 to 10": {
			from:     math.MaxUint32 - 10,
			to:       10,
			expected: 21,
		},
	}

	for _, testData := range tests {
		distance := tokenDistance(testData.from, testData.to)
		require.Equal(t, testData.expected, distance)
	}
}

func TestSearchToken(t *testing.T) {
	tokens := []uint32{3, 5}

	assert.Equal(t, 0, searchToken(tokens, 0))
	assert.Equal(t, 1, searchToken(tokens, 3))
	assert.Equal(t, 1, searchToken(tokens, 4))
	assert.Equal(t, 0, searchToken(tokens, 5))
	assert.Equal(t, 0, searchToken(tokens, 7))
}

func BenchmarkSearchToken(b *testing.B) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	gen := initTokenGenerator(b)

	tokensPerInstance := 512
	numInstances := []int{3, 9, 27, 81, 243, 729}

	for _, instances := range numInstances {
		b.Run(fmt.Sprintf("searchToken_%d_instances", instances), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tokens := gen.GenerateTokens(instances*tokensPerInstance, []uint32{})
				hash := r.Uint32()
				b.StartTimer()
				searchToken(tokens, hash)
			}
		})
	}
}

func TestStringSet(t *testing.T) {
	t.Parallel()

	t.Run("empty set with buffer returns zero length and contains nothing", func(t *testing.T) {
		t.Parallel()
		buf := make([]string, 0, 5)
		s := newStringSet(buf)

		assert.Equal(t, 0, s.len())
		assert.False(t, s.contains("anything"))
	})

	t.Run("empty set with nil buffer returns zero length and contains nothing", func(t *testing.T) {
		t.Parallel()
		s := newStringSet(nil)

		assert.Equal(t, 0, s.len())
		assert.False(t, s.contains("anything"))
	})

	t.Run("slice mode add and contains work correctly within buffer capacity", func(t *testing.T) {
		t.Parallel()
		s := newStringSet(make([]string, 0, 5))

		// Add elements within capacity
		elements := []string{"one", "two", "three"}
		for _, elem := range elements {
			assert.False(t, s.contains(elem))
			s.add(elem)
			assert.True(t, s.contains(elem))
		}

		assert.Equal(t, 3, s.len())

		// Verify all elements are still present
		for _, elem := range elements {
			assert.True(t, s.contains(elem))
		}

		// Verify non-existent element
		assert.False(t, s.contains("four"))

		// Confirm still in slice mode (map should be nil)
		assert.Nil(t, s.setMap)
	})

	t.Run("transitions from slice to map when buffer is full", func(t *testing.T) {
		t.Parallel()
		s := newStringSet(make([]string, 0, 3))

		// Fill the slice to capacity
		s.add("one")
		s.add("two")
		s.add("three")
		assert.Nil(t, s.setMap, "should still be in slice mode")

		// Adding one more should trigger transition to map
		s.add("four")
		assert.NotNil(t, s.setMap, "should have switched to map mode")

		// Verify all elements are preserved
		assert.Equal(t, 4, s.len())
		assert.True(t, s.contains("one"))
		assert.True(t, s.contains("two"))
		assert.True(t, s.contains("three"))
		assert.True(t, s.contains("four"))
	})

	t.Run("map mode add and contains work correctly after transition", func(t *testing.T) {
		t.Parallel()
		s := newStringSet(make([]string, 0, 2))

		// Fill and transition to map
		s.add("one")
		s.add("two")
		s.add("three") // triggers map mode

		assert.NotNil(t, s.setMap)

		// Add more elements in map mode
		s.add("four")
		s.add("five")

		assert.Equal(t, 5, s.len())

		// Verify all elements
		for _, elem := range []string{"one", "two", "three", "four", "five"} {
			assert.True(t, s.contains(elem))
		}

		// Verify non-existent element
		assert.False(t, s.contains("six"))
	})

	t.Run("zero capacity buffer switches to map immediately on first add", func(t *testing.T) {
		t.Parallel()
		buf := make([]string, 0, 0)
		s := newStringSet(buf)

		// First add should immediately switch to map
		// since len(0) < cap(0) is false
		s.add("one")

		assert.NotNil(t, s.setMap, "should switch to map immediately")
		assert.Equal(t, 1, s.len())
		assert.True(t, s.contains("one"))
	})

	t.Run("nil buffer switches to map immediately on first add", func(t *testing.T) {
		t.Parallel()
		s := newStringSet(nil)

		// First add should immediately switch to map
		s.add("one")

		assert.NotNil(t, s.setMap, "should switch to map immediately")
		assert.Equal(t, 1, s.len())
		assert.True(t, s.contains("one"))
	})

	t.Run("single element buffer transitions to map on second add", func(t *testing.T) {
		t.Parallel()
		s := newStringSet(make([]string, 0, 1))

		// First element stays in slice
		s.add("one")
		assert.Nil(t, s.setMap)
		assert.Equal(t, 1, s.len())
		assert.True(t, s.contains("one"))

		// Second element triggers map
		s.add("two")
		assert.NotNil(t, s.setMap)
		assert.Equal(t, 2, s.len())
		assert.True(t, s.contains("one"))
		assert.True(t, s.contains("two"))
	})

	t.Run("handles large number of elements correctly", func(t *testing.T) {
		t.Parallel()
		buf := make([]string, 0, 10)
		s := newStringSet(buf)

		// Add many elements
		numElements := 1000
		for i := 0; i < numElements; i++ {
			elem := fmt.Sprintf("element-%d", i)
			if !s.contains(elem) {
				s.add(elem)
			}
		}

		assert.Equal(t, numElements, s.len())
		assert.NotNil(t, s.setMap, "should be in map mode for large sets")

		// Verify all elements are present
		for i := 0; i < numElements; i++ {
			elem := fmt.Sprintf("element-%d", i)
			assert.True(t, s.contains(elem), "should contain %s", elem)
		}

		// Verify non-existent element
		assert.False(t, s.contains("nonexistent"))
	})
}
