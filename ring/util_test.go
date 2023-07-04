package ring

import (
	"context"
	"fmt"
	"math"
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

func (r *RingMock) GetAllHealthy(op Operation) (ReplicationSet, error) {
	args := r.Called(op)
	return args.Get(0).(ReplicationSet), args.Error(1)
}

func (r *RingMock) GetReplicationSetForOperation(op Operation) (ReplicationSet, error) {
	args := r.Called(op)
	return args.Get(0).(ReplicationSet), args.Error(1)
}

func (r *RingMock) ReplicationFactor() int {
	return 0
}

func (r *RingMock) InstancesCount() int {
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

func createStartingRing() *Ring {
	// Init the ring.
	ringDesc := &Desc{Ingesters: map[string]InstanceDesc{
		"instance-1": {Addr: "127.0.0.1", State: ACTIVE, Timestamp: time.Now().Unix()},
		"instance-2": {Addr: "127.0.0.2", State: PENDING, Timestamp: time.Now().Unix()},
		"instance-3": {Addr: "127.0.0.3", State: JOINING, Timestamp: time.Now().Unix()},
		"instance-4": {Addr: "127.0.0.4", State: LEAVING, Timestamp: time.Now().Unix()},
		"instance-5": {Addr: "127.0.0.5", State: ACTIVE, Timestamp: time.Now().Unix()},
	}}

	ring := &Ring{
		cfg:                 Config{HeartbeatTimeout: time.Minute},
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
				ringDesc.Ingesters[instanceID] = InstanceDesc{Addr: instanceID, State: ACTIVE, Timestamp: time.Now().Unix()}
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
				desc := InstanceDesc{Addr: "127.0.0.1", State: states[stateIdx%len(states)], Timestamp: time.Now().Unix()}
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
