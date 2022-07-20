package ring

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type RingMock struct {
	mock.Mock
}

func (r *RingMock) Collect(ch chan<- prometheus.Metric) {}

func (r *RingMock) Describe(ch chan<- *prometheus.Desc) {}

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

func (r *RingMock) HasInstance(instanceID string) bool {
	return true
}

func (r *RingMock) CleanupShuffleShardCache(identifier string) {}

func TestGenerateTokens(t *testing.T) {
	tokens := GenerateTokens(1000000, nil)

	dups := make(map[uint32]int)

	for ix, v := range tokens {
		if ox, ok := dups[v]; ok {
			t.Errorf("Found duplicate token %d, tokens[%d]=%d, tokens[%d]=%d", v, ix, tokens[ix], ox, tokens[ox])
		} else {
			dups[v] = ix
		}
	}
}

func TestGenerateTokens_IgnoresOldTokens(t *testing.T) {
	first := GenerateTokens(1000000, nil)
	second := GenerateTokens(1000000, first)

	dups := make(map[uint32]bool)

	for _, v := range first {
		dups[v] = true
	}

	for _, v := range second {
		if dups[v] {
			t.Fatal("GenerateTokens returned old token")
		}
	}
}

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

func getMockInterfaceAddresses(name string) ([]netip.Addr, error) {
	toAddr := func(addr string) netip.Addr {
		prefix := netip.MustParsePrefix(addr)
		return prefix.Addr()
	}

	interfaces := map[string][]netip.Addr{
		"wlan0": {
			toAddr("172.16.16.51/24"),
			toAddr("fc16::c9a7:1d89:2dd8:f59a/64"),
			toAddr("fe80::ec62:ed39:f931:bf6d/64"),
		},
		"em0": {
			toAddr("fe80::ec62:ed39:f931:bf6d/64"),
			toAddr("fc99::9a/64"),
		},
		"lo1": {
			toAddr("fe80::ec62:ed39:f931:bf6d/64"),
		},
		"lo2": {
			toAddr("169.254.1.1/16"),
		},
		"lo9":  {},
		"lo10": {},
		"enp0s31f6": {
			toAddr("1.1.1.1/31"),
		},
		"enp0s31f7": {
			toAddr("2001::1111/120"),
		},
	}

	if val, ok := interfaces[name]; ok {
		return val, nil
	}

	return nil, fmt.Errorf("no such network interface")
}

func TestGetFirstAddressOf(t *testing.T) {
	// logger := log.NewNopLogger()
	logger := log.NewLogfmtLogger(os.Stdout)

	cases := []struct {
		names []string
		addr  string
		err   error
	}{
		{
			names: []string{"wlan0"},
			addr:  "172.16.16.51",
		},
		{
			names: []string{"em0"},
			addr:  "fc99::9a",
		},
		{
			names: []string{"lo1"},
			addr:  "fe80::ec62:ed39:f931:bf6d",
		},
		{
			names: []string{"lo2"},
			addr:  "169.254.1.1",
		},
		{
			names: []string{"lo9"},
			err:   fmt.Errorf("no useable address found for interfaces [lo9]"),
		},
		{
			names: []string{"lo9", "lo10"},
			err:   fmt.Errorf("no useable address found for interfaces [lo9 lo10]"),
		},
		{
			names: []string{"lo1", "lo2", "enp0s31f6"},
			addr:  "1.1.1.1",
		},
		{
			names: []string{"lo1", "lo2", "enp0s31f7"},
			addr:  "2001::1111",
		},
		{
			names: []string{"lo1", "lo2", "enp0s31f7", "enp0s31f6"},
			addr:  "2001::1111",
		},
		{
			names: []string{"lo1", "lo2", "enp0s31f6", "enp0s31f7"},
			addr:  "1.1.1.1",
		},
	}

	for _, tc := range cases {
		addr, err := getFirstAddressOf(tc.names, logger, getMockInterfaceAddresses)
		if tc.err != nil {
			require.Equal(t, tc.err, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, tc.addr, addr)
	}
}
