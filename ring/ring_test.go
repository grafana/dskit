package ring

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/internal/slices"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring/shard"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
)

const (
	numTokens = 512
)

func newRingForTesting(cfg Config, createCacheMaps bool) *Ring {
	ring := Ring{
		cfg:      cfg,
		strategy: NewDefaultReplicationStrategy(),
	}
	if createCacheMaps {
		ring.shuffledSubringCache = map[subringCacheKey]*Ring{}
		ring.shuffledSubringWithLookbackCache = map[subringCacheKey]cachedSubringWithLookback[*Ring]{}
	}
	return &ring
}

func BenchmarkBatch10x100(b *testing.B) {
	benchmarkBatch(b, 10, 100)
}

func BenchmarkBatch100x100(b *testing.B) {
	benchmarkBatch(b, 100, 100)
}

func BenchmarkBatch100x1000(b *testing.B) {
	benchmarkBatch(b, 100, 1000)
}

func benchmarkBatch(b *testing.B, numInstances, numKeys int) {
	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	gen := NewRandomTokenGeneratorWithSeed(seed)

	// Make a random ring with N instances, and M tokens per ingests
	desc := NewDesc()
	var takenTokens []uint32
	for i := 0; i < numInstances; i++ {
		tokens := gen.GenerateTokens(numTokens, takenTokens)
		takenTokens = append(takenTokens, tokens...)
		desc.AddIngester(fmt.Sprintf("%d", i), fmt.Sprintf("instance-%d", i), strconv.Itoa(i), tokens, InstanceState_ACTIVE, time.Now(), false, time.Time{})
	}

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.HeartbeatTimeout = time.Hour // A minute is not enough to run all benchmarks.
	r, err := NewWithStoreClientAndStrategy(cfg, testRingName, testRingKey, nil, NewDefaultReplicationStrategy(), prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(b, err)
	r.updateRingState(desc)

	ctx := context.Background()
	callback := func(InstanceDesc, []int) error { return deepStack(64) }
	keys := make([]uint32, numKeys)
	// Generate a batch of N random keys, and look them up
	b.ResetTimer()

	// run with `benchstat -col /go` to get stats on different go= options.
	// ref: https://pkg.go.dev/golang.org/x/perf/cmd/benchstat

	b.Run("go=default", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			generateKeys(rnd, numKeys, keys)
			err := DoBatchWithOptions(ctx, Write, r, keys, callback, DoBatchOptions{})
			require.NoError(b, err)
		}
	})

	b.Run("go=concurrency.ReusableGoroutinesPool", func(b *testing.B) {
		pool := concurrency.NewReusableGoroutinesPool(100)
		b.Cleanup(pool.Close)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			generateKeys(rnd, numKeys, keys)
			err := DoBatchWithOptions(ctx, Write, r, keys, callback, DoBatchOptions{Go: pool.Go})
			require.NoError(b, err)
		}
	})
}

func generateKeys(r *rand.Rand, numTokens int, dest []uint32) {
	for i := 0; i < numTokens; i++ {
		dest[i] = r.Uint32()
	}
}

//go:noinline
func deepStack(depth int) error {
	if depth == 0 {
		return nil
	}
	return deepStack(depth - 1)
}

func BenchmarkUpdateRingState(b *testing.B) {
	for _, numInstances := range []int{50, 100, 500} {
		for _, numTokens := range []int{128, 256, 512} {
			for _, updateTokens := range []bool{false, true} {
				b.Run(fmt.Sprintf("num instances = %d, num tokens = %d, update tokens = %t", numInstances, numTokens, updateTokens), func(b *testing.B) {
					benchmarkUpdateRingState(b, numInstances, numTokens, updateTokens)
				})
			}
		}
	}
}

func benchmarkUpdateRingState(b *testing.B, numInstances, numTokens int, updateTokens bool) {
	cfg := Config{
		KVStore:              kv.Config{},
		HeartbeatTimeout:     0, // get healthy stats
		ReplicationFactor:    3,
		ZoneAwarenessEnabled: true,
	}

	gen := initTokenGenerator(b)

	// create the ring to set up metrics, but do not start
	registry := prometheus.NewRegistry()
	ring, err := NewWithStoreClientAndStrategy(cfg, testRingName, testRingKey, nil, NewDefaultReplicationStrategy(), registry, log.NewNopLogger())
	require.NoError(b, err)

	// Make a random ring with N instances, and M tokens per ingests
	// Also make a copy with different timestamps and one with different tokens
	desc := NewDesc()
	otherDesc := NewDesc()
	takenTokens := []uint32{}
	otherTakenTokens := []uint32{}
	for i := 0; i < numInstances; i++ {
		tokens := gen.GenerateTokens(numTokens, takenTokens)
		takenTokens = append(takenTokens, tokens...)
		now := time.Now()
		zeroTime := time.Time{}
		id := fmt.Sprintf("%d", i)
		desc.AddIngester(id, fmt.Sprintf("instance-%d", i), strconv.Itoa(i), tokens, InstanceState_ACTIVE, now, false, zeroTime)
		if updateTokens {
			otherTokens := gen.GenerateTokens(numTokens, otherTakenTokens)
			otherTakenTokens = append(otherTakenTokens, otherTokens...)
			otherDesc.AddIngester(id, fmt.Sprintf("instance-%d", i), strconv.Itoa(i), otherTokens, InstanceState_ACTIVE, now, false, zeroTime)
		} else {
			otherDesc.AddIngester(id, fmt.Sprintf("instance-%d", i), strconv.Itoa(i), tokens, InstanceState_JOINING, now, false, zeroTime)
		}
	}

	if updateTokens {
		require.Equal(b, Different, desc.RingCompare(otherDesc))
	} else {
		require.Equal(b, EqualButStatesAndTimestamps, desc.RingCompare(otherDesc))
	}

	flipFlop := true
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if flipFlop {
			ring.updateRingState(desc)
		} else {
			ring.updateRingState(otherDesc)
		}
		flipFlop = !flipFlop
	}
}

func TestDoBatchZeroInstances(t *testing.T) {
	ctx := context.Background()
	numKeys := 10
	keys := make([]uint32, numKeys)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	generateKeys(rnd, numKeys, keys)
	callback := func(InstanceDesc, []int) error {
		return nil
	}
	cleanup := func() {
	}
	desc := NewDesc()
	r := newRingForTesting(Config{}, false)
	r.setRingStateFromDesc(desc, false, false, false)
	require.Error(t, DoBatch(ctx, Write, r, keys, callback, cleanup))
}

func TestDoBatchWithOptionsContextCancellation(t *testing.T) {
	const (
		numKeys      = 5e6
		numInstances = 100
		numZones     = 3
	)
	cancelCause := errors.New("cancel cause")

	measureDuration := func(r *Ring, keys []uint32) time.Duration {
		callback := func(InstanceDesc, []int) error { return nil }
		t0 := time.Now()
		err := DoBatchWithOptions(context.Background(), Write, r, keys, callback, DoBatchOptions{})
		duration := time.Since(t0)
		require.NoError(t, err)
		t.Logf("Call took %s", duration)
		return duration
	}

	type callbackFunc = func(InstanceDesc, []int) error
	never := func(_ InstanceDesc, _ []int) error {
		t.Errorf("should not be called.")
		return nil
	}
	tests := []struct {
		name        string
		setup       func(*Ring, []uint32) (context.Context, callbackFunc)
		expectedErr error
	}{
		{
			name: "context deadline exceeded",
			setup: func(r *Ring, keys []uint32) (context.Context, callbackFunc) {
				duration := measureDuration(r, keys)

				// Make a second call cancelling after a hundredth of duration of the first one.
				// For a 4s first call, this is 40ms: should be enough for this test to not be flaky.
				ctx, cancel := context.WithTimeout(context.Background(), duration/100)
				go func() {
					<-ctx.Done()
					cancel()
				}()
				return ctx, never
			},
			expectedErr: context.DeadlineExceeded,
		},
		{
			name: "context deadline exceeded with cause",
			setup: func(r *Ring, keys []uint32) (context.Context, callbackFunc) {
				duration := measureDuration(r, keys)

				// Make a second call cancelling after a hundredth of duration of the first one.
				// For a 4s first call, this is 40ms: should be enough for this test to not be flaky.
				ctx, cancel := context.WithTimeoutCause(context.Background(), duration/100, cancelCause)
				go func() {
					<-ctx.Done()
					cancel()
				}()
				return ctx, never
			},
			expectedErr: cancelCause,
		},
		{
			name: "context initially cancelled without cause",
			setup: func(_ *Ring, _ []uint32) (context.Context, callbackFunc) {
				ctx, cancelFunc := context.WithCancel(context.Background())
				// start batch with cancelled context
				cancelFunc()
				return ctx, never
			},
			expectedErr: context.Canceled,
		},
		{
			name: "context initially cancelled with cause",
			setup: func(_ *Ring, _ []uint32) (context.Context, callbackFunc) {
				ctx, cancelFunc := context.WithCancelCause(context.Background())
				// start batch with cancelled context
				cancelFunc(cancelCause)
				return ctx, never
			},
			expectedErr: cancelCause,
		},
		{
			name: "context cancelled during batch processing",
			setup: func(_ *Ring, _ []uint32) (context.Context, callbackFunc) {
				ctx, cancel := context.WithCancelCause(context.Background())

				wg := sync.WaitGroup{}
				wg.Add(numInstances)

				callback := func(_ InstanceDesc, _ []int) error {
					wg.Done()
					// let the call to the instance hang until context is cancelled
					<-ctx.Done()
					return nil
				}
				go func() {
					// wait until all instances hang, then cancel the context
					wg.Wait()
					cancel(cancelCause)
				}()
				return ctx, callback
			},
			expectedErr: cancelCause,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys := make([]uint32, numKeys)
			generateKeys(rand.New(rand.NewSource(0)), numKeys, keys)

			desc := &Desc{Ingesters: generateRingInstances(NewRandomTokenGeneratorWithSeed(0), numInstances, numZones, numTokens)}
			r := newRingForTesting(Config{
				HeartbeatTimeout:     time.Hour,
				ZoneAwarenessEnabled: true,
				SubringCacheDisabled: true,
				ReplicationFactor:    numZones,
			}, true)
			r.setRingStateFromDesc(desc, false, false, false)

			ctx, callback := tt.setup(r, keys)

			wg := sync.WaitGroup{}
			wg.Add(1)
			err := DoBatchWithOptions(ctx, Write, r, keys, callback, DoBatchOptions{Cleanup: wg.Done})
			require.Error(t, err)
			require.ErrorIs(t, err, tt.expectedErr)

			// Wait until cleanup to make sure that callback was never called.
			wg.Wait()
		})
	}
}

func TestDoBatch_QuorumError(t *testing.T) {
	const (
		// we should run several write request to make sure we don't have any race condition on the batchTracker code
		numberOfOperations = 10000
		replicationFactor  = 3
	)

	gen := initTokenGenerator(t)

	desc := NewDesc()
	for address := 0; address < replicationFactor; address++ {
		instTokens := gen.GenerateTokens(128, nil)
		instanceID := fmt.Sprintf("%d", address)
		desc.AddIngester(instanceID, instanceID, "", instTokens, InstanceState_ACTIVE, time.Now(), false, time.Time{})
	}
	ringConfig := Config{
		HeartbeatTimeout:  time.Hour,
		ReplicationFactor: replicationFactor,
	}
	ring, err := NewWithStoreClientAndStrategy(ringConfig, "ingester", ringKey, nil, NewDefaultReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(t, err)
	ring.updateRingState(desc)
	operationKeys := []uint32{1, 10, 100}
	ctx := context.Background()
	unfinishedDoBatchCalls := sync.WaitGroup{}
	runDoBatch := func(instanceReturnErrors [replicationFactor]error, isClientError func(error) bool) error {
		unfinishedDoBatchCalls.Add(1)
		returnInstanceError := func(i InstanceDesc, _ []int) error {
			instanceID, err := strconv.Atoi(i.Addr)
			require.NoError(t, err)
			return instanceReturnErrors[instanceID]
		}
		return DoBatchWithOptions(ctx, Write, ring, operationKeys, returnInstanceError, DoBatchOptions{
			Cleanup:       func() { unfinishedDoBatchCalls.Done() },
			IsClientError: isClientError,
		})
	}

	updateState := func(instanceIDs []string, state InstanceState) {
		for _, instanceID := range instanceIDs {
			inst := ring.ringDesc.Ingesters[instanceID]
			inst.State = state
			inst.Timestamp = time.Now().Unix()
			ring.ringDesc.Ingesters[instanceID] = inst
			ring.updateRingState(ring.ringDesc)
		}
	}

	http429Error := httpgrpc.Errorf(429, "Throttling")
	http500Error := httpgrpc.Errorf(500, "InternalServerError")
	mockClientError := mockError{isClientErr: true}
	mockServerError := mockError{isClientErr: false}

	isClientErr := func(err error) bool {
		if mockErr, ok := err.(mockError); ok {
			return mockErr.isClientError()
		}
		return false
	}

	testCases := map[string]struct {
		errors               [replicationFactor]error
		unavailableInstances []string
		isClientError        func(error) bool
		acceptableOutcomes   []error
	}{
		"no error should return no error": {
			errors:             [replicationFactor]error{nil, nil, nil},
			acceptableOutcomes: nil,
		},
		"only one HTTP error with isHTTPStatus4xx filter should return no error": {
			errors:             [replicationFactor]error{http500Error, nil, nil},
			isClientError:      isHTTPStatus4xx,
			acceptableOutcomes: nil,
		},
		"only one client error with isHTTPStatus4xx filter should return no error": {
			errors:             [replicationFactor]error{mockClientError, nil, nil},
			isClientError:      isHTTPStatus4xx,
			acceptableOutcomes: nil,
		},
		"2 HTTP 4xx and 1 HTTP 5xx errors with isHTTPStatus4xx filter should return 4xx": {
			errors:             [replicationFactor]error{http429Error, http500Error, http429Error},
			isClientError:      isHTTPStatus4xx,
			acceptableOutcomes: []error{http429Error},
		},
		"2 HTTP 5xx and 1 HTTP 4xx errors with isHTTPStatus4xx filter should return 5xx": {
			errors:             [replicationFactor]error{http500Error, http429Error, http500Error},
			isClientError:      isHTTPStatus4xx,
			acceptableOutcomes: []error{http500Error},
		},
		"1 HTTP 4xx, 1 HTTP 5xx and 1 success with isHTTPStatus4xx filter should return one of the two HTTP errors": {
			errors:             [replicationFactor]error{http429Error, http500Error, nil},
			isClientError:      isHTTPStatus4xx,
			acceptableOutcomes: []error{http429Error, http500Error},
		},
		"1 HTTP error and 1 unhealthy instance with isHTTPStatus4xx filter should return that error": {
			errors:               [replicationFactor]error{http500Error, nil, nil},
			isClientError:        isHTTPStatus4xx,
			unavailableInstances: []string{"2"},
			acceptableOutcomes:   []error{http500Error},
		},
		"2 client and 1 server errors with isClientErr filter should return the client error": {
			errors:             [replicationFactor]error{mockClientError, mockClientError, mockServerError},
			isClientError:      isClientErr,
			acceptableOutcomes: []error{mockClientError},
		},
		"2 server and 1 client errors with isClientErr filter should return the server error": {
			errors:             [replicationFactor]error{mockServerError, mockClientError, mockServerError},
			isClientError:      isClientErr,
			acceptableOutcomes: []error{mockServerError},
		},
		"1 client, 1 server error and 1 success with isClientErr filter return one of the two error": {
			errors:             [replicationFactor]error{mockServerError, mockClientError, mockServerError},
			isClientError:      isClientErr,
			acceptableOutcomes: []error{mockClientError, mockServerError},
		},
		"1 client error and 1 unhealthy instance with isClientErr filter should return that error": {
			errors:               [replicationFactor]error{mockClientError, nil, nil},
			isClientError:        isClientErr,
			unavailableInstances: []string{"2"},
			acceptableOutcomes:   []error{mockClientError},
		},
		"1 server error and 1 unhealthy instance with isClientErr filter should return that error": {
			errors:               [replicationFactor]error{mockServerError, nil, nil},
			isClientError:        isClientErr,
			unavailableInstances: []string{"2"},
			acceptableOutcomes:   []error{mockServerError},
		},
		"2 HTTP 4xx and 1 client error with isClientErr filter should return the HTTP 4xx error": {
			// isClientErr filter applied to http429Error is false, so http429Error is not treated as a client error
			errors:             [replicationFactor]error{http429Error, http429Error, mockClientError},
			isClientError:      isClientErr,
			acceptableOutcomes: []error{http429Error},
		},
		"1 HTTP 4xx, 1 HTTP 5xx and 1 client error with isClientErr filter should return either HTTP 4xx or HTTP 5xx error": {
			// isClientErr filter applied to http429Error is false, so http429Error is not treated as a client error
			errors:             [replicationFactor]error{http429Error, http500Error, mockClientError},
			isClientError:      isClientErr,
			acceptableOutcomes: []error{http429Error, http500Error},
		},
		"1 HTTP 4xx, 1 client and 1 server error with isClientErr filter should return either the HTTP 4xx or the server error": {
			// isClientErr filter applied to http429Error is false, so http429Error is not treated as a client error
			errors:             [replicationFactor]error{http429Error, mockClientError, mockServerError},
			isClientError:      isClientErr,
			acceptableOutcomes: []error{mockServerError, http429Error},
		},
	}

	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			updateState(testData.unavailableInstances, InstanceState_LEFT)
			for i := 0; i < numberOfOperations; i++ {
				err := runDoBatch(testData.errors, testData.isClientError)
				if testData.acceptableOutcomes == nil {
					require.NoError(t, err)
				} else {
					require.Contains(t, testData.acceptableOutcomes, err)
				}
			}
			unfinishedDoBatchCalls.Wait()
			updateState(testData.unavailableInstances, InstanceState_ACTIVE)
		})
	}
}

func TestAddIngester(t *testing.T) {
	r := NewDesc()

	const ingName = "ing1"

	now := time.Now()
	ing1Tokens := initTokenGenerator(t).GenerateTokens(128, nil)

	r.AddIngester(ingName, "addr", "1", ing1Tokens, InstanceState_ACTIVE, now, false, time.Time{})

	assert.Equal(t, "addr", r.Ingesters[ingName].Addr)
	assert.Equal(t, ing1Tokens, Tokens(r.Ingesters[ingName].Tokens))
	assert.InDelta(t, time.Now().Unix(), r.Ingesters[ingName].Timestamp, 2)
	assert.Equal(t, now.Unix(), r.Ingesters[ingName].RegisteredTimestamp)
	assert.False(t, r.Ingesters[ingName].ReadOnly)
	assert.Equal(t, int64(0), r.Ingesters[ingName].ReadOnlyUpdatedTimestamp)
}

func TestAddIngesterReplacesExistingTokens(t *testing.T) {
	r := NewDesc()

	const ing1Name = "ing1"

	// old tokens will be replaced
	r.Ingesters[ing1Name] = InstanceDesc{
		Tokens: []uint32{11111, 22222, 33333},
	}

	newTokens := initTokenGenerator(t).GenerateTokens(128, nil)

	r.AddIngester(ing1Name, "addr", "1", newTokens, InstanceState_ACTIVE, time.Now(), false, time.Time{})

	require.Equal(t, newTokens, Tokens(r.Ingesters[ing1Name].Tokens))
}

func TestRing_Get_ZoneAwarenessWithIngesterLeaving(t *testing.T) {
	const testCount = 10000

	tests := map[string]struct {
		replicationFactor int
		expectedInstances int
		expectedZones     int
	}{
		"should succeed if there are enough instances per zone on RF = 3": {
			replicationFactor: 3,
			expectedInstances: 3,
			expectedZones:     3,
		},
		"should succeed if there are enough instances per zone on RF = 2": {
			replicationFactor: 2,
			expectedInstances: 2,
			expectedZones:     2,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			gen := initTokenGenerator(t)

			r := NewDesc()
			instances := map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", State: InstanceState_ACTIVE},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", State: InstanceState_ACTIVE},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", State: InstanceState_ACTIVE},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", State: InstanceState_ACTIVE},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-c", State: InstanceState_LEAVING},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-c", State: InstanceState_ACTIVE},
			}
			var prevTokens []uint32
			for id, instance := range instances {
				ingTokens := gen.GenerateTokens(128, prevTokens)
				r.AddIngester(id, instance.Addr, instance.Zone, ingTokens, instance.State, time.Now(), false, time.Time{})
				prevTokens = append(prevTokens, ingTokens...)
			}
			instancesList := make([]InstanceDesc, 0, len(r.GetIngesters()))
			for _, v := range r.GetIngesters() {
				instancesList = append(instancesList, v)
			}

			ring := newRingForTesting(Config{
				HeartbeatTimeout:     time.Hour,
				ReplicationFactor:    testData.replicationFactor,
				ZoneAwarenessEnabled: true,
			}, false)
			ring.setRingStateFromDesc(r, false, false, false)

			_, bufHosts, bufZones := MakeBuffersForGet()

			// Use the GenerateTokens to get an array of random uint32 values.
			testValues := gen.GenerateTokens(testCount, nil)

			for i := 0; i < testCount; i++ {
				set, err := ring.Get(testValues[i], Write, instancesList, bufHosts, bufZones)
				require.NoError(t, err)

				distinctZones := map[string]int{}
				for _, instance := range set.Instances {
					distinctZones[instance.Zone]++
				}

				assert.Len(t, set.Instances, testData.expectedInstances)
				assert.Len(t, distinctZones, testData.expectedZones)
			}
		})
	}
}

func TestRing_Get_ZoneAwareness(t *testing.T) {
	// Number of tests to run.
	const testCount = 10000

	tests := map[string]struct {
		numInstances         int
		numZones             int
		replicationFactor    int
		zoneAwarenessEnabled bool
		expectedErr          string
		expectedInstances    int
	}{
		"should succeed if there are enough instances per zone on RF = 3": {
			numInstances:         16,
			numZones:             3,
			replicationFactor:    3,
			zoneAwarenessEnabled: true,
			expectedInstances:    3,
		},
		"should fail if there are instances in 1 zone only on RF = 3": {
			numInstances:         16,
			numZones:             1,
			replicationFactor:    3,
			zoneAwarenessEnabled: true,
			expectedErr:          "at least 2 live replicas required across different availability zones, could only find 1",
		},
		"should succeed if there are instances in 2 zones on RF = 3": {
			numInstances:         16,
			numZones:             2,
			replicationFactor:    3,
			zoneAwarenessEnabled: true,
			expectedInstances:    2,
		},
		"should succeed if there are instances in 1 zone only on RF = 3 but zone-awareness is disabled": {
			numInstances:         16,
			numZones:             1,
			replicationFactor:    3,
			zoneAwarenessEnabled: false,
			expectedInstances:    3,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			gen := initTokenGenerator(t)

			// Add instances to the ring.
			r := NewDesc()
			var prevTokens []uint32
			for i := 0; i < testData.numInstances; i++ {
				name := fmt.Sprintf("ing%v", i)
				ingTokens := gen.GenerateTokens(128, prevTokens)

				r.AddIngester(name, fmt.Sprintf("127.0.0.%d", i), fmt.Sprintf("zone-%v", i%testData.numZones), ingTokens, InstanceState_ACTIVE, time.Now(), false, time.Time{})

				prevTokens = append(prevTokens, ingTokens...)
			}

			// Create a ring with the instances
			ring := newRingForTesting(Config{
				HeartbeatTimeout:     time.Hour,
				ReplicationFactor:    testData.replicationFactor,
				ZoneAwarenessEnabled: testData.zoneAwarenessEnabled,
			}, false)
			ring.setRingStateFromDesc(r, false, false, false)

			instances := make([]InstanceDesc, 0, len(r.GetIngesters()))
			for _, v := range r.GetIngesters() {
				instances = append(instances, v)
			}

			_, bufHosts, bufZones := MakeBuffersForGet()

			// Use the GenerateTokens to get an array of random uint32 values.
			testValues := gen.GenerateTokens(testCount, nil)

			var set ReplicationSet
			var err error
			for i := 0; i < testCount; i++ {
				set, err = ring.Get(testValues[i], Write, instances, bufHosts, bufZones)
				if testData.expectedErr != "" {
					require.EqualError(t, err, testData.expectedErr)
				} else {
					require.NoError(t, err)
				}

				// Skip the rest of the assertions if we were expecting an error.
				if testData.expectedErr != "" {
					continue
				}

				// Check that we have the expected number of instances for replication.
				assert.Equal(t, testData.expectedInstances, len(set.Instances))

				// Ensure all instances are in a different zone (only if zone-awareness is enabled).
				if testData.zoneAwarenessEnabled {
					zones := make(map[string]struct{})
					for i := 0; i < len(set.Instances); i++ {
						if _, ok := zones[set.Instances[i].Zone]; ok {
							t.Fatal("found multiple instances in the same zone")
						}
						zones[set.Instances[i].Zone] = struct{}{}
					}
				}
			}
		})
	}
}

func TestRing_GetAllHealthy(t *testing.T) {
	const heartbeatTimeout = time.Minute
	now := time.Now()

	tests := map[string]struct {
		ringInstances           map[string]InstanceDesc
		expectedErrForRead      error
		expectedSetForRead      []string
		expectedErrForWrite     error
		expectedSetForWrite     []string
		expectedErrForReporting error
		expectedSetForReporting []string
	}{
		"should return error on empty ring": {
			ringInstances:           nil,
			expectedErrForRead:      ErrEmptyRing,
			expectedErrForWrite:     ErrEmptyRing,
			expectedErrForReporting: ErrEmptyRing,
		},
		"should return all healthy instances for the given operation": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: InstanceState_ACTIVE, Timestamp: now.Unix()},
				"instance-2": {Addr: "127.0.0.2", State: InstanceState_PENDING, Timestamp: now.Add(-10 * time.Second).Unix()},
				"instance-3": {Addr: "127.0.0.3", State: InstanceState_JOINING, Timestamp: now.Add(-20 * time.Second).Unix()},
				"instance-4": {Addr: "127.0.0.4", State: InstanceState_LEAVING, Timestamp: now.Add(-30 * time.Second).Unix()},
				"instance-5": {Addr: "127.0.0.5", State: InstanceState_ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix()},
			},
			expectedSetForRead:      []string{"127.0.0.1", "127.0.0.2", "127.0.0.4"},
			expectedSetForWrite:     []string{"127.0.0.1"},
			expectedSetForReporting: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Init the ring.
			ringDesc := &Desc{Ingesters: testData.ringInstances}
			for id, instance := range ringDesc.Ingesters {
				ringDesc.Ingesters[id] = instance
			}

			ring := newRingForTesting(Config{HeartbeatTimeout: heartbeatTimeout}, false)
			ring.setRingStateFromDesc(ringDesc, false, false, false)

			set, err := ring.GetAllHealthy(Read)
			require.Equal(t, testData.expectedErrForRead, err)
			assert.ElementsMatch(t, testData.expectedSetForRead, set.GetAddresses())

			set, err = ring.GetAllHealthy(Write)
			require.Equal(t, testData.expectedErrForWrite, err)
			assert.ElementsMatch(t, testData.expectedSetForWrite, set.GetAddresses())

			set, err = ring.GetAllHealthy(Reporting)
			require.Equal(t, testData.expectedErrForReporting, err)
			assert.ElementsMatch(t, testData.expectedSetForReporting, set.GetAddresses())
		})
	}
}

func TestRing_GetReplicationSetForOperation(t *testing.T) {
	now := time.Now()
	gen := initTokenGenerator(t)

	tests := map[string]struct {
		ringInstances           map[string]InstanceDesc
		ringHeartbeatTimeout    time.Duration
		ringReplicationFactor   int
		expectedErrForRead      error
		expectedSetForRead      []string
		expectedErrForWrite     error
		expectedSetForWrite     []string
		expectedErrForReporting error
		expectedSetForReporting []string
	}{
		"should return error on empty ring": {
			ringInstances:           nil,
			ringHeartbeatTimeout:    time.Minute,
			ringReplicationFactor:   1,
			expectedErrForRead:      ErrEmptyRing,
			expectedErrForWrite:     ErrEmptyRing,
			expectedErrForReporting: ErrEmptyRing,
		},
		"should succeed on all healthy instances and RF=1": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: InstanceState_ACTIVE, Timestamp: now.Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", State: InstanceState_ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", State: InstanceState_ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", State: InstanceState_ACTIVE, Timestamp: now.Add(-30 * time.Second).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", State: InstanceState_ACTIVE, Timestamp: now.Add(-40 * time.Second).Unix(), Tokens: gen.GenerateTokens(128, nil)},
			},
			ringHeartbeatTimeout:    time.Minute,
			ringReplicationFactor:   1,
			expectedSetForRead:      []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"},
			expectedSetForWrite:     []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"},
			expectedSetForReporting: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"},
		},
		"should succeed on instances with old timestamps but heartbeat timeout disabled": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: InstanceState_ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", State: InstanceState_ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", State: InstanceState_ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", State: InstanceState_ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", State: InstanceState_ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: gen.GenerateTokens(128, nil)},
			},
			ringHeartbeatTimeout:    0,
			ringReplicationFactor:   1,
			expectedSetForRead:      []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"},
			expectedSetForWrite:     []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"},
			expectedSetForReporting: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"},
		},
		"should fail on 1 unhealthy instance and RF=1": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: InstanceState_ACTIVE, Timestamp: now.Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", State: InstanceState_ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", State: InstanceState_ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", State: InstanceState_ACTIVE, Timestamp: now.Add(-30 * time.Second).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", State: InstanceState_ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: gen.GenerateTokens(128, nil)},
			},
			ringHeartbeatTimeout:    time.Minute,
			ringReplicationFactor:   1,
			expectedErrForRead:      ErrTooManyUnhealthyInstances,
			expectedErrForWrite:     ErrTooManyUnhealthyInstances,
			expectedErrForReporting: ErrTooManyUnhealthyInstances,
		},
		"should succeed on 1 unhealthy instances and RF=3": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: InstanceState_ACTIVE, Timestamp: now.Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", State: InstanceState_ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", State: InstanceState_ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", State: InstanceState_ACTIVE, Timestamp: now.Add(-30 * time.Second).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", State: InstanceState_ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: gen.GenerateTokens(128, nil)},
			},
			ringHeartbeatTimeout:    time.Minute,
			ringReplicationFactor:   3,
			expectedSetForRead:      []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
			expectedSetForWrite:     []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
			expectedSetForReporting: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
		},
		"should fail on 2 unhealthy instances and RF=3": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", State: InstanceState_ACTIVE, Timestamp: now.Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", State: InstanceState_ACTIVE, Timestamp: now.Add(-10 * time.Second).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", State: InstanceState_ACTIVE, Timestamp: now.Add(-20 * time.Second).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", State: InstanceState_ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", State: InstanceState_ACTIVE, Timestamp: now.Add(-2 * time.Minute).Unix(), Tokens: gen.GenerateTokens(128, nil)},
			},
			ringHeartbeatTimeout:    time.Minute,
			ringReplicationFactor:   3,
			expectedErrForRead:      ErrTooManyUnhealthyInstances,
			expectedErrForWrite:     ErrTooManyUnhealthyInstances,
			expectedErrForReporting: ErrTooManyUnhealthyInstances,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Init the ring.
			ringDesc := &Desc{Ingesters: testData.ringInstances}
			for id, instance := range ringDesc.Ingesters {
				ringDesc.Ingesters[id] = instance
			}

			ring := newRingForTesting(Config{
				HeartbeatTimeout:  testData.ringHeartbeatTimeout,
				ReplicationFactor: testData.ringReplicationFactor,
			}, false)
			ring.setRingStateFromDesc(ringDesc, false, false, false)

			set, err := ring.GetReplicationSetForOperation(Read)
			require.Equal(t, testData.expectedErrForRead, err)
			assert.ElementsMatch(t, testData.expectedSetForRead, set.GetAddresses())

			set, err = ring.GetReplicationSetForOperation(Write)
			require.Equal(t, testData.expectedErrForWrite, err)
			assert.ElementsMatch(t, testData.expectedSetForWrite, set.GetAddresses())

			set, err = ring.GetReplicationSetForOperation(Reporting)
			require.Equal(t, testData.expectedErrForReporting, err)
			assert.ElementsMatch(t, testData.expectedSetForReporting, set.GetAddresses())
		})
	}
}

func TestRing_GetReplicationSetForOperation_WithZoneAwarenessEnabled(t *testing.T) {
	gen := initTokenGenerator(t)

	tests := map[string]struct {
		ringInstances               map[string]InstanceDesc
		unhealthyInstances          []string
		expectedAddresses           []string
		replicationFactor           int
		expectedError               error
		expectedMaxErrors           int
		expectedMaxUnavailableZones int
	}{
		"empty ring": {
			ringInstances: nil,
			expectedError: ErrEmptyRing,
		},
		"RF=1, 1 zone": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
			},
			expectedAddresses:           []string{"127.0.0.1", "127.0.0.2"},
			replicationFactor:           1,
			expectedMaxErrors:           0,
			expectedMaxUnavailableZones: 0,
		},
		"RF=1, 1 zone, one unhealthy instance": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
			},
			unhealthyInstances: []string{"instance-2"},
			replicationFactor:  1,
			expectedError:      ErrTooManyUnhealthyInstances,
		},
		"RF=1, 3 zones, one unhealthy instance": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			unhealthyInstances: []string{"instance-3"},
			replicationFactor:  1,
			expectedError:      ErrTooManyUnhealthyInstances,
		},
		"RF=2, 2 zones": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
			},
			expectedAddresses:           []string{"127.0.0.1", "127.0.0.2"},
			replicationFactor:           2,
			expectedMaxUnavailableZones: 1,
		},
		"RF=2, 2 zones, one unhealthy instance": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
			},
			expectedAddresses:  []string{"127.0.0.1"},
			unhealthyInstances: []string{"instance-2"},
			replicationFactor:  2,
		},
		"RF=3, 3 zones, one instance per zone": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			expectedAddresses:           []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
			replicationFactor:           3,
			expectedMaxErrors:           0,
			expectedMaxUnavailableZones: 1,
		},
		"RF=3, 3 zones, one instance per zone, one instance unhealthy": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			expectedAddresses:           []string{"127.0.0.2", "127.0.0.3"},
			unhealthyInstances:          []string{"instance-1"},
			replicationFactor:           3,
			expectedMaxErrors:           0,
			expectedMaxUnavailableZones: 0,
		},
		"RF=3, 3 zones, one instance per zone, two instances unhealthy in separate zones": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			unhealthyInstances: []string{"instance-1", "instance-2"},
			replicationFactor:  3,
			expectedError:      ErrTooManyUnhealthyInstances,
		},
		"RF=3, 3 zones, one instance per zone, all instances unhealthy": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			unhealthyInstances: []string{"instance-1", "instance-2", "instance-3"},
			replicationFactor:  3,
			expectedError:      ErrTooManyUnhealthyInstances,
		},
		"RF=3, 3 zones, two instances per zone": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			expectedAddresses:           []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6"},
			replicationFactor:           3,
			expectedMaxErrors:           0,
			expectedMaxUnavailableZones: 1,
		},
		"RF=3, 3 zones, two instances per zone, two instances unhealthy in same zone": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			expectedAddresses:           []string{"127.0.0.1", "127.0.0.2", "127.0.0.5", "127.0.0.6"},
			unhealthyInstances:          []string{"instance-3", "instance-4"},
			replicationFactor:           3,
			expectedMaxErrors:           0,
			expectedMaxUnavailableZones: 0,
		},
		"RF=3, 3 zones, three instances per zone, two instances unhealthy in same zone": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-7": {Addr: "127.0.0.7", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-8": {Addr: "127.0.0.8", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-9": {Addr: "127.0.0.9", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			expectedAddresses:           []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.7", "127.0.0.8", "127.0.0.9"},
			unhealthyInstances:          []string{"instance-4", "instance-6"},
			replicationFactor:           3,
			expectedMaxErrors:           0,
			expectedMaxUnavailableZones: 0,
		},
		"RF=3, only 2 zones, two instances per zone": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
			},
			expectedAddresses:           []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
			replicationFactor:           3,
			expectedMaxErrors:           0,
			expectedMaxUnavailableZones: 1,
		},
		"RF=3, only 2 zones, two instances per zone, one instance unhealthy": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
			},
			expectedAddresses:           []string{"127.0.0.1", "127.0.0.2"},
			unhealthyInstances:          []string{"instance-4"},
			replicationFactor:           3,
			expectedMaxErrors:           0,
			expectedMaxUnavailableZones: 0,
		},
		"RF=3, only 1 zone, two instances per zone": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
			},
			expectedAddresses:           []string{"127.0.0.1", "127.0.0.2"},
			replicationFactor:           3,
			expectedMaxErrors:           0,
			expectedMaxUnavailableZones: 0,
		},
		"RF=3, only 1 zone, two instances per zone, one instance unhealthy": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
			},
			unhealthyInstances: []string{"instance-2"},
			replicationFactor:  3,
			expectedError:      ErrTooManyUnhealthyInstances,
		},
		"RF=5, 5 zones, two instances per zone except for one zone which has three": {
			ringInstances: map[string]InstanceDesc{
				"instance-1":  {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2":  {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3":  {Addr: "127.0.0.3", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4":  {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5":  {Addr: "127.0.0.5", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-6":  {Addr: "127.0.0.6", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-7":  {Addr: "127.0.0.7", Zone: "zone-d", Tokens: gen.GenerateTokens(128, nil)},
				"instance-8":  {Addr: "127.0.0.8", Zone: "zone-d", Tokens: gen.GenerateTokens(128, nil)},
				"instance-9":  {Addr: "127.0.0.9", Zone: "zone-e", Tokens: gen.GenerateTokens(128, nil)},
				"instance-10": {Addr: "127.0.0.10", Zone: "zone-e", Tokens: gen.GenerateTokens(128, nil)},
				"instance-11": {Addr: "127.0.0.11", Zone: "zone-e", Tokens: gen.GenerateTokens(128, nil)},
			},
			expectedAddresses: []string{
				"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5",
				"127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.0.11",
			},
			replicationFactor:           5,
			expectedMaxErrors:           0,
			expectedMaxUnavailableZones: 2,
		},
		"RF=5, 5 zones, two instances per zone except for one zone which has three, 2 unhealthy nodes in same zones": {
			ringInstances: map[string]InstanceDesc{
				"instance-1":  {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2":  {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3":  {Addr: "127.0.0.3", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4":  {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5":  {Addr: "127.0.0.5", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-6":  {Addr: "127.0.0.6", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-7":  {Addr: "127.0.0.7", Zone: "zone-d", Tokens: gen.GenerateTokens(128, nil)},
				"instance-8":  {Addr: "127.0.0.8", Zone: "zone-d", Tokens: gen.GenerateTokens(128, nil)},
				"instance-9":  {Addr: "127.0.0.9", Zone: "zone-e", Tokens: gen.GenerateTokens(128, nil)},
				"instance-10": {Addr: "127.0.0.10", Zone: "zone-e", Tokens: gen.GenerateTokens(128, nil)},
				"instance-11": {Addr: "127.0.0.11", Zone: "zone-e", Tokens: gen.GenerateTokens(128, nil)},
			},
			expectedAddresses:           []string{"127.0.0.1", "127.0.0.2", "127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.0.11"},
			unhealthyInstances:          []string{"instance-3", "instance-4"},
			replicationFactor:           5,
			expectedMaxErrors:           0,
			expectedMaxUnavailableZones: 1,
		},
		"RF=5, 5 zones, two instances per zone except for one zone which has three, 2 unhealthy nodes in separate zones": {
			ringInstances: map[string]InstanceDesc{
				"instance-1":  {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2":  {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3":  {Addr: "127.0.0.3", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4":  {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5":  {Addr: "127.0.0.5", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-6":  {Addr: "127.0.0.6", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-7":  {Addr: "127.0.0.7", Zone: "zone-d", Tokens: gen.GenerateTokens(128, nil)},
				"instance-8":  {Addr: "127.0.0.8", Zone: "zone-d", Tokens: gen.GenerateTokens(128, nil)},
				"instance-9":  {Addr: "127.0.0.9", Zone: "zone-e", Tokens: gen.GenerateTokens(128, nil)},
				"instance-10": {Addr: "127.0.0.10", Zone: "zone-e", Tokens: gen.GenerateTokens(128, nil)},
				"instance-11": {Addr: "127.0.0.11", Zone: "zone-e", Tokens: gen.GenerateTokens(128, nil)},
			},
			expectedAddresses:           []string{"127.0.0.1", "127.0.0.2", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.0.11"},
			unhealthyInstances:          []string{"instance-3", "instance-5"},
			replicationFactor:           5,
			expectedMaxErrors:           0,
			expectedMaxUnavailableZones: 0,
		},
		"RF=5, 5 zones, one instances per zone, three unhealthy instances": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-d", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-e", Tokens: gen.GenerateTokens(128, nil)},
			},
			unhealthyInstances: []string{"instance-2", "instance-4", "instance-5"},
			replicationFactor:  5,
			expectedError:      ErrTooManyUnhealthyInstances,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Ensure the test case has been correctly setup (max errors and max unavailable zones are
			// mutually exclusive).
			require.False(t, testData.expectedMaxErrors > 0 && testData.expectedMaxUnavailableZones > 0)

			// Init the ring.
			ringDesc := &Desc{Ingesters: testData.ringInstances}
			for id, instance := range ringDesc.Ingesters {
				instance.Timestamp = time.Now().Unix()
				instance.State = InstanceState_ACTIVE
				for _, instanceName := range testData.unhealthyInstances {
					if instanceName == id {
						instance.Timestamp = time.Now().Add(-time.Hour).Unix()
					}
				}
				ringDesc.Ingesters[id] = instance
			}

			ring := newRingForTesting(Config{
				HeartbeatTimeout:     time.Minute,
				ZoneAwarenessEnabled: true,
				ReplicationFactor:    testData.replicationFactor,
			}, false)
			ring.setRingStateFromDesc(ringDesc, false, false, false)

			// Check the replication set has the correct settings
			replicationSet, err := ring.GetReplicationSetForOperation(Read)
			if testData.expectedError != nil {
				require.Equal(t, testData.expectedError, err)
				return
			}

			require.NoError(t, err)

			assert.Equal(t, testData.expectedMaxErrors, replicationSet.MaxErrors)
			assert.Equal(t, testData.expectedMaxUnavailableZones, replicationSet.MaxUnavailableZones)
			assert.True(t, replicationSet.ZoneAwarenessEnabled)

			returnAddresses := []string{}
			for _, instance := range replicationSet.Instances {
				returnAddresses = append(returnAddresses, instance.Addr)
			}
			for _, addr := range testData.expectedAddresses {
				assert.Contains(t, returnAddresses, addr)
			}
			assert.Equal(t, len(testData.expectedAddresses), len(replicationSet.Instances))
		})
	}
}

func TestRing_GetInstancesWithTokensCounts(t *testing.T) {
	gen := initTokenGenerator(t)

	tests := map[string]struct {
		ringInstances                          map[string]InstanceDesc
		expectedInstancesWithTokensCount       int
		expectedInstancesWithTokensInZoneCount map[string]int
	}{
		"empty ring": {
			ringInstances:                          nil,
			expectedInstancesWithTokensCount:       0,
			expectedInstancesWithTokensInZoneCount: map[string]int{},
		},
		"single zone, no tokens": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", State: InstanceState_ACTIVE, Tokens: []uint32{}},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", State: InstanceState_LEAVING, Tokens: []uint32{}},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-a", State: InstanceState_PENDING, Tokens: []uint32{}},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-a", State: InstanceState_JOINING, Tokens: []uint32{}},
			},
			expectedInstancesWithTokensCount:       0,
			expectedInstancesWithTokensInZoneCount: map[string]int{"zone-a": 0},
		},
		"single zone, some tokens": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", State: InstanceState_ACTIVE, Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", State: InstanceState_ACTIVE, Tokens: []uint32{}},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-a", State: InstanceState_LEAVING, Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-a", State: InstanceState_LEAVING, Tokens: []uint32{}},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-a", State: InstanceState_PENDING, Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-a", State: InstanceState_PENDING, Tokens: []uint32{}},
				"instance-7": {Addr: "127.0.0.7", Zone: "zone-a", State: InstanceState_JOINING, Tokens: gen.GenerateTokens(128, nil)},
				"instance-8": {Addr: "127.0.0.8", Zone: "zone-a", State: InstanceState_JOINING, Tokens: []uint32{}},
			},
			expectedInstancesWithTokensCount:       4,
			expectedInstancesWithTokensInZoneCount: map[string]int{"zone-a": 4},
		},
		"multiple zones": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", State: InstanceState_ACTIVE, Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", State: InstanceState_ACTIVE, Tokens: []uint32{}},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", State: InstanceState_LEAVING, Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", State: InstanceState_LEAVING, Tokens: []uint32{}},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-c", State: InstanceState_PENDING, Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-d", State: InstanceState_PENDING, Tokens: []uint32{}},
				"instance-7": {Addr: "127.0.0.7", Zone: "zone-c", State: InstanceState_JOINING, Tokens: gen.GenerateTokens(128, nil)},
				"instance-8": {Addr: "127.0.0.8", Zone: "zone-d", State: InstanceState_JOINING, Tokens: []uint32{}},
			},
			expectedInstancesWithTokensCount:       4,
			expectedInstancesWithTokensInZoneCount: map[string]int{"zone-a": 1, "zone-b": 1, "zone-c": 2, "zone-d": 0},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Init the ring.
			ringDesc := &Desc{Ingesters: testData.ringInstances}
			for id, instance := range ringDesc.Ingesters {
				instance.Timestamp = time.Now().Unix()
				ringDesc.Ingesters[id] = instance
			}

			ring := newRingForTesting(Config{
				HeartbeatTimeout:     time.Hour,
				ZoneAwarenessEnabled: true,
			}, false)
			ring.setRingStateFromDesc(ringDesc, false, false, false)

			assert.Equal(t, testData.expectedInstancesWithTokensCount, ring.InstancesWithTokensCount())
			for z, instances := range testData.expectedInstancesWithTokensInZoneCount {
				assert.Equal(t, instances, ring.InstancesWithTokensInZoneCount(z))
			}
		})
	}
}

func TestRing_GetWritableInstancesWithTokensCounts(t *testing.T) {
	gen := initTokenGenerator(t)

	tests := map[string]struct {
		ringInstances                                   map[string]InstanceDesc
		expectedWritableInstancesWithTokensCount        int
		expectedWritableInstancesWithTokensCountPerZone map[string]int
	}{
		"empty ring": {
			ringInstances:                                   nil,
			expectedWritableInstancesWithTokensCount:        0,
			expectedWritableInstancesWithTokensCountPerZone: map[string]int{},
		},
		"single zone, no tokens": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", State: InstanceState_ACTIVE, Tokens: []uint32{}},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", State: InstanceState_LEAVING, Tokens: []uint32{}, ReadOnly: true},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-a", State: InstanceState_PENDING, Tokens: []uint32{}},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-a", State: InstanceState_JOINING, Tokens: []uint32{}},
			},
			expectedWritableInstancesWithTokensCount:        0,
			expectedWritableInstancesWithTokensCountPerZone: map[string]int{"zone-a": 0},
		},
		"single zone, some tokens": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", State: InstanceState_ACTIVE, Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", State: InstanceState_ACTIVE, Tokens: []uint32{}},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-a", State: InstanceState_LEAVING, Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-a", State: InstanceState_LEAVING, Tokens: []uint32{}},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-a", State: InstanceState_PENDING, Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-a", State: InstanceState_PENDING, Tokens: []uint32{}, ReadOnly: true},
				"instance-7": {Addr: "127.0.0.7", Zone: "zone-a", State: InstanceState_JOINING, Tokens: gen.GenerateTokens(128, nil), ReadOnly: true},
				"instance-8": {Addr: "127.0.0.8", Zone: "zone-a", State: InstanceState_JOINING, Tokens: []uint32{}, ReadOnly: true},
			},
			expectedWritableInstancesWithTokensCount:        3,
			expectedWritableInstancesWithTokensCountPerZone: map[string]int{"zone-a": 3},
		},
		"multiple zones": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", State: InstanceState_ACTIVE, Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", State: InstanceState_ACTIVE, Tokens: []uint32{}, ReadOnly: true},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", State: InstanceState_LEAVING, Tokens: gen.GenerateTokens(128, nil), ReadOnly: true},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", State: InstanceState_LEAVING, Tokens: []uint32{}},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-c", State: InstanceState_PENDING, Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-d", State: InstanceState_PENDING, Tokens: []uint32{}},
				"instance-7": {Addr: "127.0.0.7", Zone: "zone-c", State: InstanceState_JOINING, Tokens: gen.GenerateTokens(128, nil)},
				"instance-8": {Addr: "127.0.0.8", Zone: "zone-d", State: InstanceState_JOINING, Tokens: []uint32{}, ReadOnly: true},
			},
			expectedWritableInstancesWithTokensCount:        3,
			expectedWritableInstancesWithTokensCountPerZone: map[string]int{"zone-a": 1, "zone-b": 0, "zone-c": 2, "zone-d": 0},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Init the ring.
			ringDesc := &Desc{Ingesters: testData.ringInstances}
			for id, instance := range ringDesc.Ingesters {
				instance.Timestamp = time.Now().Unix()
				ringDesc.Ingesters[id] = instance
			}

			ring := newRingForTesting(Config{
				HeartbeatTimeout:     time.Hour,
				ZoneAwarenessEnabled: true,
			}, false)
			ring.setRingStateFromDesc(ringDesc, false, false, false)

			assert.Equal(t, testData.expectedWritableInstancesWithTokensCount, ring.WritableInstancesWithTokensCount())
			for z, instances := range testData.expectedWritableInstancesWithTokensCountPerZone {
				assert.Equal(t, instances, ring.WritableInstancesWithTokensInZoneCount(z))
			}
		})
	}
}

func TestRing_ShuffleShard(t *testing.T) {
	gen := initTokenGenerator(t)

	tests := map[string]struct {
		ringInstances                map[string]InstanceDesc
		shardSize                    int
		zoneAwarenessEnabled         bool
		expectedSize                 int
		expectedDistribution         []int
		expectedZoneCount            int
		expectedInstancesInZoneCount map[string]int
	}{
		"empty ring": {
			ringInstances:        nil,
			shardSize:            2,
			zoneAwarenessEnabled: true,
			expectedSize:         0,
			expectedDistribution: []int{},
		},
		"empty ring, shardSize=0": {
			ringInstances:        nil,
			shardSize:            0,
			zoneAwarenessEnabled: true,
			expectedSize:         0,
			expectedDistribution: []int{},
		},
		"single zone, shard size > num instances": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:                    3,
			zoneAwarenessEnabled:         true,
			expectedSize:                 2,
			expectedDistribution:         []int{2},
			expectedZoneCount:            1,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 2},
		},
		"single zone, shard size == 0": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:                    3,
			zoneAwarenessEnabled:         true,
			expectedSize:                 2,
			expectedDistribution:         []int{2},
			expectedZoneCount:            1,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 2},
		},
		"single zone, shard size < num instances": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:                    2,
			zoneAwarenessEnabled:         true,
			expectedSize:                 2,
			expectedDistribution:         []int{2},
			expectedZoneCount:            1,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 2},
		},
		"single zone, with read only instance, shardSize = 3": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-a", ReadOnly: true, Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:                    3,
			zoneAwarenessEnabled:         true,
			expectedSize:                 2,
			expectedDistribution:         []int{2},
			expectedZoneCount:            1,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 2},
		},
		"single zone, with read only instance, shardSize = 0": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-a", ReadOnly: true, Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:                    0,
			zoneAwarenessEnabled:         true,
			expectedSize:                 2,
			expectedDistribution:         []int{2},
			expectedZoneCount:            1,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 2},
		},
		"multiple zones, shard size < num zones": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:                    2,
			zoneAwarenessEnabled:         true,
			expectedSize:                 3,
			expectedDistribution:         []int{1, 1, 1},
			expectedZoneCount:            3,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 1, "zone-b": 1, "zone-c": 1},
		},
		"multiple zones, shard size divisible by num zones": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:                    3,
			zoneAwarenessEnabled:         true,
			expectedSize:                 3,
			expectedDistribution:         []int{1, 1, 1},
			expectedZoneCount:            3,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 1, "zone-b": 1, "zone-c": 1},
		},
		"multiple zones, shard size NOT divisible by num zones": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:                    4,
			zoneAwarenessEnabled:         true,
			expectedSize:                 6,
			expectedDistribution:         []int{2, 2, 2},
			expectedZoneCount:            3,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 2, "zone-b": 2, "zone-c": 2},
		},
		"multiple zones, shard size NOT divisible by num zones, but zone awareness is disabled": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:            4,
			zoneAwarenessEnabled: false,
			expectedSize:         4,
		},
		"multiple zones, with read only instance, shardSize=3": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-c", ReadOnly: true, Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:                    3,
			zoneAwarenessEnabled:         true,
			expectedSize:                 2,
			expectedDistribution:         []int{1, 1},
			expectedZoneCount:            2,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 1, "zone-b": 1, "zone-c": 0},
		},
		"multiple zones, with read only instance, shardSize=0": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil), ReadOnly: true},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil), ReadOnly: true},
			},
			shardSize:                    0,
			zoneAwarenessEnabled:         true,
			expectedSize:                 4,
			expectedDistribution:         []int{2, 1, 1},
			expectedZoneCount:            3,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 2, "zone-b": 1, "zone-c": 1},
		},
		"multiple zones, shard size == num instances, balanced zones": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:                    6,
			zoneAwarenessEnabled:         true,
			expectedSize:                 6,
			expectedDistribution:         []int{2, 2, 2},
			expectedZoneCount:            3,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 2, "zone-b": 2, "zone-c": 2},
		},
		"multiple zones, shard size == num instances, unbalanced zones": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:                    6,
			zoneAwarenessEnabled:         true,
			expectedSize:                 5,
			expectedDistribution:         []int{2, 2, 1},
			expectedZoneCount:            3,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 2, "zone-b": 2, "zone-c": 1},
		},
		"multiple zones, shard size > num instances, balanced zones": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:                    6,
			zoneAwarenessEnabled:         true,
			expectedSize:                 3,
			expectedDistribution:         []int{1, 1, 1},
			expectedZoneCount:            3,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 1, "zone-b": 1, "zone-c": 1},
		},
		"multiple zones, shard size > num instances, unbalanced zones": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:                    9,
			zoneAwarenessEnabled:         true,
			expectedSize:                 6,
			expectedDistribution:         []int{3, 2, 1},
			expectedZoneCount:            3,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 3, "zone-b": 2, "zone-c": 1},
		},
		"multiple zones, shard size = 0, unbalanced zones": {
			ringInstances: map[string]InstanceDesc{
				"instance-1": {Addr: "127.0.0.1", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-2": {Addr: "127.0.0.2", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-3": {Addr: "127.0.0.3", Zone: "zone-a", Tokens: gen.GenerateTokens(128, nil)},
				"instance-4": {Addr: "127.0.0.4", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-5": {Addr: "127.0.0.5", Zone: "zone-b", Tokens: gen.GenerateTokens(128, nil)},
				"instance-6": {Addr: "127.0.0.6", Zone: "zone-c", Tokens: gen.GenerateTokens(128, nil)},
			},
			shardSize:                    0,
			zoneAwarenessEnabled:         true,
			expectedSize:                 6,
			expectedDistribution:         []int{3, 2, 1},
			expectedZoneCount:            3,
			expectedInstancesInZoneCount: map[string]int{"zone-a": 3, "zone-b": 2, "zone-c": 1},
		},
	}

	for testName, testData := range tests {
		for _, updateReadOnlyInstances := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v, updateReadOnlyInstances=%v", testName, updateReadOnlyInstances), func(t *testing.T) {
				// Init the ring.
				ringDesc := &Desc{Ingesters: testData.ringInstances}
				for id, instance := range ringDesc.Ingesters {
					instance.Timestamp = time.Now().Unix()
					instance.State = InstanceState_ACTIVE
					ringDesc.Ingesters[id] = instance
				}

				ring := newRingForTesting(
					Config{
						HeartbeatTimeout:     time.Hour,
						ZoneAwarenessEnabled: testData.zoneAwarenessEnabled,
					}, false)
				ring.setRingStateFromDesc(ringDesc, false, false, updateReadOnlyInstances)

				shardRing := ring.ShuffleShard("tenant-id", testData.shardSize)
				assert.Equal(t, testData.expectedSize, shardRing.InstancesCount())

				// Compute the actual distribution of instances across zones.
				if testData.zoneAwarenessEnabled {
					assert.Equal(t, testData.expectedZoneCount, shardRing.ZonesCount())
					for z, instances := range testData.expectedInstancesInZoneCount {
						assert.Equal(t, instances, shardRing.InstancesInZoneCount(z))
					}

					var actualDistribution []int

					if shardRing.InstancesCount() > 0 {
						all, err := shardRing.GetAllHealthy(Read)
						require.NoError(t, err)

						countByZone := map[string]int{}
						for _, instance := range all.Instances {
							countByZone[instance.Zone]++
						}

						for _, count := range countByZone {
							actualDistribution = append(actualDistribution, count)
						}
					}

					assert.ElementsMatch(t, testData.expectedDistribution, actualDistribution)
				}
			})
		}
	}
}

// This test asserts on shard stability across multiple invocations and given the same input ring.
func TestRing_ShuffleShard_Stability(t *testing.T) {
	var (
		numTenants     = 100
		numInstances   = 50
		numZones       = 3
		numInvocations = 10
		shardSizes     = []int{3, 6, 9, 12, 15}
	)

	// Initialise the ring.
	ringDesc := &Desc{Ingesters: generateRingInstances(initTokenGenerator(t), numInstances, numZones, 128)}
	ring := newRingForTesting(Config{
		HeartbeatTimeout:     time.Hour,
		ZoneAwarenessEnabled: true,
	}, false)
	ring.setRingStateFromDesc(ringDesc, false, false, false)

	for i := 1; i <= numTenants; i++ {
		tenantID := fmt.Sprintf("%d", i)

		for _, size := range shardSizes {
			r := ring.ShuffleShard(tenantID, size)
			expected, err := r.GetAllHealthy(Read)
			require.NoError(t, err)

			// Assert that multiple invocations generate the same exact shard.
			for n := 0; n < numInvocations; n++ {
				r := ring.ShuffleShard(tenantID, size)
				actual, err := r.GetAllHealthy(Read)
				require.NoError(t, err)
				assert.ElementsMatch(t, expected.Instances, actual.Instances)
			}
		}
	}
}

func initTokenGenerator(t testing.TB) TokenGenerator {
	seed := time.Now().UnixNano()
	t.Log("token generator seed:", seed)
	return NewRandomTokenGeneratorWithSeed(seed)
}

func TestRing_ShuffleShard_Shuffling(t *testing.T) {
	var (
		numTenants   = 1000
		numInstances = 90
		numZones     = 3
		shardSize    = 3

		// This is the expected theoretical distribution of matching instances
		// between different shards, given the settings above. It has been computed
		// using this spreadsheet:
		// https://docs.google.com/spreadsheets/d/1FXbiWTXi6bdERtamH-IfmpgFq1fNL4GP_KX_yJvbRi4/edit
		theoreticalMatchings = map[int]float64{
			0: 90.2239,
			1: 9.55312,
			2: 0.22217,
			3: 0.00085,
		}
	)

	// Initialise the ring instances. To have stable tests we generate tokens using a linear
	// distribution. Tokens within the same zone are evenly distributed too.
	instances := make(map[string]InstanceDesc, numInstances)
	for i := 0; i < numInstances; i++ {
		id := fmt.Sprintf("instance-%d", i)
		instances[id] = InstanceDesc{
			Addr:                fmt.Sprintf("127.0.0.%d", i),
			Timestamp:           time.Now().Unix(),
			RegisteredTimestamp: time.Now().Unix(),
			State:               InstanceState_ACTIVE,
			Tokens:              generateTokensLinear(i, numInstances, 128),
			Zone:                fmt.Sprintf("zone-%d", i%numZones),
		}
	}

	// Initialise the ring.
	ringDesc := &Desc{Ingesters: instances}
	ring := newRingForTesting(Config{
		HeartbeatTimeout:     time.Hour,
		ZoneAwarenessEnabled: true,
	}, false)
	ring.setRingStateFromDesc(ringDesc, false, false, false)

	// Compute the shard for each tenant.
	shards := map[string][]string{}

	for i := 1; i <= numTenants; i++ {
		tenantID := fmt.Sprintf("%d", i)
		r := ring.ShuffleShard(tenantID, shardSize)
		set, err := r.GetAllHealthy(Read)
		require.NoError(t, err)

		instances := make([]string, 0, len(set.Instances))
		for _, instance := range set.Instances {
			instances = append(instances, instance.Addr)
		}

		shards[tenantID] = instances
	}

	// Compute the distribution of matching instances between every combination of shards.
	// The shards comparison is not optimized, but it's fine for a test.
	distribution := map[int]int{}

	for currID, currShard := range shards {
		for otherID, otherShard := range shards {
			if currID == otherID {
				continue
			}

			numMatching := 0
			for _, c := range currShard {
				if slices.Contains(otherShard, c) {
					numMatching++
				}
			}

			distribution[numMatching]++
		}
	}

	maxCombinations := int(math.Pow(float64(numTenants), 2)) - numTenants
	for numMatching, probability := range theoreticalMatchings {
		// We allow a max deviance of 10% compared to the theoretical probability,
		// clamping it between 1% and 0.2% boundaries.
		maxDeviance := math.Min(1, math.Max(0.2, probability*0.1))

		actual := (float64(distribution[numMatching]) / float64(maxCombinations)) * 100
		assert.InDelta(t, probability, actual, maxDeviance, "numMatching: %d", numMatching)
	}
}

func TestRing_ShuffleShard_Consistency(t *testing.T) {
	type change string

	type scenario struct {
		name         string
		numInstances int
		numZones     int
		shardSize    int
		ringChange   change
	}

	const (
		numTenants      = 100
		add             = change("add-instance")
		remove          = change("remove-instance")
		enableReadOnly  = change("enable-read-only")
		disableReadOnly = change("disable-read-only")
	)

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Generate all test scenarios.
	var scenarios []scenario
	for _, numInstances := range []int{20, 30, 40, 50} {
		for _, shardSize := range []int{3, 6, 9, 12, 15} {
			for _, c := range []change{add, remove, enableReadOnly, disableReadOnly} {
				scenarios = append(scenarios, scenario{
					name:         fmt.Sprintf("instances = %d, shard size = %d, ring operation = %s", numInstances, shardSize, c),
					numInstances: numInstances,
					numZones:     3,
					shardSize:    shardSize,
					ringChange:   c,
				})
			}
		}
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			gen := initTokenGenerator(t)
			// Initialise the ring.
			ringDesc := &Desc{Ingesters: generateRingInstances(gen, s.numInstances, s.numZones, 128)}

			// Mark some instances as read only
			for i := 0; i < len(ringDesc.Ingesters); i += 8 {
				instanceID := getRandomInstanceID(ringDesc.Ingesters, rnd)
				inst := ringDesc.Ingesters[instanceID]
				inst.ReadOnly = true
				inst.ReadOnlyUpdatedTimestamp = time.Now().Unix()
				ringDesc.Ingesters[instanceID] = inst
			}

			ring := newRingForTesting(Config{
				HeartbeatTimeout:     time.Hour,
				ZoneAwarenessEnabled: true,
			}, false)
			ring.setRingStateFromDesc(ringDesc, false, false, false)

			// Compute the initial shard for each tenant.
			initial := map[int]ReplicationSet{}
			for id := 0; id < numTenants; id++ {
				set, err := ring.ShuffleShard(fmt.Sprintf("%d", id), s.shardSize).GetAllHealthy(Read)
				require.NoError(t, err)
				initial[id] = set
			}

			// Update the ring.
			switch s.ringChange {
			case add:
				newID, newDesc, _ := generateRingInstance(gen, s.numInstances+1, 0, 128, nil)
				ringDesc.Ingesters[newID] = newDesc
			case remove:
				// Remove the first one.
				for id := range ringDesc.Ingesters {
					delete(ringDesc.Ingesters, id)
					break
				}
			case enableReadOnly:
				for id, desc := range ringDesc.Ingesters {
					desc.ReadOnly = true
					desc.ReadOnlyUpdatedTimestamp = time.Now().Unix()
					ringDesc.Ingesters[id] = desc
					break
				}
			case disableReadOnly:
				for id, desc := range ringDesc.Ingesters {
					if desc.ReadOnly {
						desc.ReadOnly = false
						desc.ReadOnlyUpdatedTimestamp = time.Now().Unix()
						ringDesc.Ingesters[id] = desc
					}
					break
				}
			}

			ring.setRingStateFromDesc(ringDesc, false, false, false)

			// Compute the update shard for each tenant and compare it with the initial one.
			// If the "consistency" property is guaranteed, we expect no more than 1 different instance
			// in the updated shard.
			for id := 0; id < numTenants; id++ {
				updated, err := ring.ShuffleShard(fmt.Sprintf("%d", id), s.shardSize).GetAllHealthy(Read)
				require.NoError(t, err)

				added, removed := compareReplicationSets(initial[id], updated)
				assert.LessOrEqual(t, len(added), 1)
				assert.LessOrEqual(t, len(removed), 1)
			}
		})
	}
}

func TestRing_ShuffleShard_ConsistencyOnShardSizeChanged(t *testing.T) {
	// Create 30 instances in 3 zones.
	ringInstances := map[string]InstanceDesc{}
	for i := 0; i < 30; i++ {
		name, desc, _ := generateRingInstance(initTokenGenerator(t), i, i%3, 128, nil)
		ringInstances[name] = desc
	}

	// Init the ring.
	ringDesc := &Desc{Ingesters: ringInstances}
	ring := newRingForTesting(Config{
		HeartbeatTimeout:     time.Hour,
		ZoneAwarenessEnabled: true,
	}, false)
	ring.setRingStateFromDesc(ringDesc, false, false, false)

	// Get the replication set with shard size = 3.
	firstShard := ring.ShuffleShard("tenant-id", 3)
	assert.Equal(t, 3, firstShard.InstancesCount())

	firstSet, err := firstShard.GetAllHealthy(Read)
	require.NoError(t, err)

	// Increase shard size to 6.
	secondShard := ring.ShuffleShard("tenant-id", 6)
	assert.Equal(t, 6, secondShard.InstancesCount())

	secondSet, err := secondShard.GetAllHealthy(Read)
	require.NoError(t, err)

	for _, firstInstance := range firstSet.Instances {
		assert.True(t, secondSet.Includes(firstInstance.Addr), "new replication set is expected to include previous instance %s", firstInstance.Addr)
	}

	// Increase shard size to 9.
	thirdShard := ring.ShuffleShard("tenant-id", 9)
	assert.Equal(t, 9, thirdShard.InstancesCount())

	thirdSet, err := thirdShard.GetAllHealthy(Read)
	require.NoError(t, err)

	for _, secondInstance := range secondSet.Instances {
		assert.True(t, thirdSet.Includes(secondInstance.Addr), "new replication set is expected to include previous instance %s", secondInstance.Addr)
	}

	// Decrease shard size to 6.
	fourthShard := ring.ShuffleShard("tenant-id", 6)
	assert.Equal(t, 6, fourthShard.InstancesCount())

	fourthSet, err := fourthShard.GetAllHealthy(Read)
	require.NoError(t, err)

	// We expect to have the same exact instances we had when the shard size was 6.
	for _, secondInstance := range secondSet.Instances {
		assert.True(t, fourthSet.Includes(secondInstance.Addr), "new replication set is expected to include previous instance %s", secondInstance.Addr)
	}

	// Decrease shard size to 3.
	fifthShard := ring.ShuffleShard("tenant-id", 3)
	assert.Equal(t, 3, fifthShard.InstancesCount())

	fifthSet, err := fifthShard.GetAllHealthy(Read)
	require.NoError(t, err)

	// We expect to have the same exact instances we had when the shard size was 3.
	for _, firstInstance := range firstSet.Instances {
		assert.True(t, fifthSet.Includes(firstInstance.Addr), "new replication set is expected to include previous instance %s", firstInstance.Addr)
	}
}

func TestRing_ShuffleShard_ConsistencyOnZonesChanged(t *testing.T) {
	// Create 20 instances in 2 zones.
	ringInstances := map[string]InstanceDesc{}
	for i := 0; i < 20; i++ {
		name, desc, _ := generateRingInstance(initTokenGenerator(t), i, i%2, 128, nil)
		ringInstances[name] = desc
	}

	// Init the ring.
	ringDesc := &Desc{Ingesters: ringInstances}
	ring := newRingForTesting(Config{
		HeartbeatTimeout:     time.Hour,
		ZoneAwarenessEnabled: true,
	}, false)
	ring.setRingStateFromDesc(ringDesc, false, false, false)

	// Get the replication set with shard size = 2.
	firstShard := ring.ShuffleShard("tenant-id", 2)
	assert.Equal(t, 2, firstShard.InstancesCount())

	firstSet, err := firstShard.GetAllHealthy(Read)
	require.NoError(t, err)

	// Increase shard size to 4.
	secondShard := ring.ShuffleShard("tenant-id", 4)
	assert.Equal(t, 4, secondShard.InstancesCount())

	secondSet, err := secondShard.GetAllHealthy(Read)
	require.NoError(t, err)

	for _, firstInstance := range firstSet.Instances {
		assert.True(t, secondSet.Includes(firstInstance.Addr), "new replication set is expected to include previous instance %s", firstInstance.Addr)
	}

	// Scale up cluster, adding 10 instances in 1 new zone.
	for i := 20; i < 30; i++ {
		name, desc, _ := generateRingInstance(initTokenGenerator(t), i, 2, 128, nil)
		ringInstances[name] = desc
	}

	ringDesc.Ingesters = ringInstances
	ring.setRingStateFromDesc(ringDesc, false, false, false)

	// Increase shard size to 6.
	thirdShard := ring.ShuffleShard("tenant-id", 6)
	assert.Equal(t, 6, thirdShard.InstancesCount())

	thirdSet, err := thirdShard.GetAllHealthy(Read)
	require.NoError(t, err)

	for _, secondInstance := range secondSet.Instances {
		assert.True(t, thirdSet.Includes(secondInstance.Addr), "new replication set is expected to include previous instance %s", secondInstance.Addr)
	}

	// Increase shard size to 9.
	fourthShard := ring.ShuffleShard("tenant-id", 9)
	assert.Equal(t, 9, fourthShard.InstancesCount())

	fourthSet, err := fourthShard.GetAllHealthy(Read)
	require.NoError(t, err)

	for _, thirdInstance := range thirdSet.Instances {
		assert.True(t, fourthSet.Includes(thirdInstance.Addr), "new replication set is expected to include previous instance %s", thirdInstance.Addr)
	}
}

func TestRing_ShuffleShardWithLookback(t *testing.T) {
	type eventType int

	const (
		add eventType = iota
		remove
		test

		lookbackPeriod = time.Hour
		userID         = "user-1"
	)

	now := time.Now().Truncate(time.Second)

	type event struct {
		what         eventType
		instanceID   string
		instanceDesc InstanceDesc
		shardSize    int
		expected     []string
		readOnly     bool
		readOnlyTime time.Time
	}

	tests := map[string]struct {
		timeline []event
	}{
		"single zone, shard size = 1, recently bootstrapped cluster": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-time.Minute))},
				{what: test, shardSize: 1, expected: []string{"instance-1", "instance-2", "instance-3"}},
			},
		},
		"single zone, shard size = 0, recently bootstrapped cluster": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-time.Minute))},
				{what: test, shardSize: 0, expected: []string{"instance-1", "instance-2", "instance-3"}},
			},
		},
		"single zone, shard size = 1, instances scale up": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 2}, now.Add(-10*time.Minute))},
				{what: test, shardSize: 1, expected: []string{"instance-4" /* lookback: */, "instance-1"}},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-5*time.Minute))},
				{what: test, shardSize: 1, expected: []string{"instance-5" /* lookback: */, "instance-4", "instance-1"}},
			},
		},
		"single zone, shard size = 1, instances scale down": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				{what: remove, instanceID: "instance-3"},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				{what: remove, instanceID: "instance-1"},
				{what: test, shardSize: 1, expected: []string{"instance-2"}},
			},
		},
		"single zone, shard size = 1, rollout with instances unregistered (removed and re-added one by one)": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				// Rollout instance-3.
				{what: remove, instanceID: "instance-3"},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now)},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				// Rollout instance-2.
				{what: remove, instanceID: "instance-2"},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now)},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				// Rollout instance-1.
				{what: remove, instanceID: "instance-1"},
				{what: test, shardSize: 1, expected: []string{"instance-2" /* side effect: */, "instance-3"}},
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now)},
				{what: test, shardSize: 1, expected: []string{"instance-1" /* lookback: */, "instance-2" /* side effect: */, "instance-3"}},
			},
		},
		"single zone, shard size = 2, rollout with instances unregistered (removed and re-added one by one)": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 3) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2"}},
				// Rollout instance-4.
				{what: remove, instanceID: "instance-4"},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2"}},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 3) + 1}, now)},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2"}},
				// Rollout instance-3.
				{what: remove, instanceID: "instance-3"},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2"}},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now)},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2"}},
				// Rollout instance-2.
				{what: remove, instanceID: "instance-2"},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-3" /* side effect:*/, "instance-4"}},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now)},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2" /* lookback: */, "instance-3" /* side effect:*/, "instance-4"}},
				// Rollout instance-1.
				{what: remove, instanceID: "instance-1"},
				{what: test, shardSize: 2, expected: []string{"instance-2" /* lookback: */, "instance-3" /* side effect:*/, "instance-4"}},
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now)},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2" /* lookback: */, "instance-3" /* side effect:*/, "instance-4"}},
			},
		},
		"single zone, increase shard size": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 1, expected: []string{"instance-1"}},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2"}},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3"}},
				{what: test, shardSize: 4, expected: []string{"instance-1", "instance-2", "instance-3"}},
			},
		},
		"single zone, with read only instance, within lookback, shardSize=2": {
			timeline: []event{
				// instance 2 is included in addition to another instance
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true, readOnlyTime: now.Add(lookbackPeriod / 2)},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2", "instance-3"}},
			},
		},
		"single zone, with read only instance, not within lookback, shardSize=2": {
			timeline: []event{
				// readOnlyTime is too old to matter
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true, readOnlyTime: now.Add(-2 * lookbackPeriod)},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-3"}},
			},
		},
		"single zone, with read only instance, at lookback boundary, shardSize=2": {
			timeline: []event{
				// readOnlyTime is too old to matter
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true, readOnlyTime: now.Add(lookbackPeriod)},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2", "instance-3"}},
			},
		},
		"single zone, with read only instance, with unknown ReadOnlyUpdatedTimestamp, shardSize=2": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2", "instance-3"}},
			},
		},
		"single zone, with read only instance, within lookback, shardSize=0": {
			timeline: []event{
				// instance 2 is included in addition to another instance
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true, readOnlyTime: now.Add(lookbackPeriod / 2)},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 0, expected: []string{"instance-1", "instance-2", "instance-3"}},
			},
		},
		"single zone, with read only instance, not within lookback, shardSize=0": {
			timeline: []event{
				// readOnlyTime is too old to matter
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true, readOnlyTime: now.Add(-2 * lookbackPeriod)},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 0, expected: []string{"instance-1", "instance-3"}},
			},
		},
		"single zone, with read only instance, at lookback boundary, shardSize=0": {
			timeline: []event{
				// readOnlyTime is too old to matter
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true, readOnlyTime: now.Add(lookbackPeriod)},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 0, expected: []string{"instance-1", "instance-2", "instance-3"}},
			},
		},
		"single zone, with read only instance, with unknown ReadOnlyUpdatedTimestamp, shardSize=0": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-a", []uint32{userToken(userID, "zone-a", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 2, expected: []string{"instance-1", "instance-2", "instance-3"}},
			},
		},
		"multi zone, shard size = 3, recently bootstrapped cluster": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 3) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-b", []uint32{userToken(userID, "zone-b", 4) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 5) + 1}, now.Add(-time.Minute))},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3", "instance-4", "instance-5", "instance-6"}},
			},
		},
		"multi zone, shard size = 3, instances scale up": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 2}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 2}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 2}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 3}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3"}},
				// Scale up.
				{what: add, instanceID: "instance-7", instanceDesc: generateRingInstanceWithInfo("instance-7", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now)},
				{what: test, shardSize: 3, expected: []string{"instance-7", "instance-2", "instance-3" /* lookback: */, "instance-1"}},
				{what: add, instanceID: "instance-8", instanceDesc: generateRingInstanceWithInfo("instance-8", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now)},
				{what: test, shardSize: 3, expected: []string{"instance-7", "instance-8", "instance-3" /* lookback: */, "instance-1", "instance-2"}},
				{what: add, instanceID: "instance-9", instanceDesc: generateRingInstanceWithInfo("instance-9", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 1}, now)},
				{what: test, shardSize: 3, expected: []string{"instance-7", "instance-8", "instance-9" /* lookback: */, "instance-1", "instance-2", "instance-3"}},
			},
		},
		"multi zone, shard size = 3, instances scale down": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-7", instanceDesc: generateRingInstanceWithInfo("instance-7", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 2}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-8", instanceDesc: generateRingInstanceWithInfo("instance-8", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 2}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-9", instanceDesc: generateRingInstanceWithInfo("instance-9", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 2}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3"}},
				// Scale down.
				{what: remove, instanceID: "instance-1"},
				{what: test, shardSize: 3, expected: []string{"instance-7", "instance-2", "instance-3"}},
				{what: remove, instanceID: "instance-2"},
				{what: test, shardSize: 3, expected: []string{"instance-7", "instance-8", "instance-3"}},
				{what: remove, instanceID: "instance-3"},
				{what: test, shardSize: 3, expected: []string{"instance-7", "instance-8", "instance-9"}},
			},
		},
		"multi zone, increase shard size": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 3}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-7", instanceDesc: generateRingInstanceWithInfo("instance-7", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 2}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-8", instanceDesc: generateRingInstanceWithInfo("instance-8", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 2}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-9", instanceDesc: generateRingInstanceWithInfo("instance-9", "zone-c", []uint32{userToken(userID, "zone-c", 2) + 2}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3"}},
				{what: test, shardSize: 6, expected: []string{"instance-1", "instance-2", "instance-3", "instance-7", "instance-8", "instance-9"}},
			},
		},
		"multi zone, shard size = 3, instances without registered timestamp": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, time.Time{})},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, time.Time{})},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, time.Time{})},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, time.Time{})},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, time.Time{})},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, time.Time{})},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3"}},

				// Re-add instances 1,2,3 with fresh RegisteredTimestamp
				{what: remove, instanceID: "instance-1"},
				{what: remove, instanceID: "instance-2"},
				{what: remove, instanceID: "instance-3"},
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-time.Minute))},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3", "instance-4", "instance-5", "instance-6"}},

				// Re-add 4,5,6 with RegisteredTimestamp. Now all instances have RegisteredTimestamp.
				{what: remove, instanceID: "instance-4"},
				{what: remove, instanceID: "instance-5"},
				{what: remove, instanceID: "instance-6"},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-time.Minute))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-time.Minute))},

				// All instances are registered within lookback window now.
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3", "instance-4", "instance-5", "instance-6"}},
			},
		},
		"multi zone, with read only instance, within lookback, shardSize=3": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				// instance 2 and 4 are included in addition to other instances in the same zone
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true, readOnlyTime: now.Add(lookbackPeriod / 2)},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-c", []uint32{userToken(userID, "zone-c", 3) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 4) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 5) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true, readOnlyTime: now.Add(lookbackPeriod / 2)},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3", "instance-4", "instance-6"}},
			},
		},
		"multi zone, with read only instance, at lookback boundary, shardSize=3": {
			timeline: []event{
				// readOnlyTime is too old to matter
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true, readOnlyTime: now.Add(lookbackPeriod)},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-c", []uint32{userToken(userID, "zone-c", 3) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 4) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 5) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3", "instance-6"}},
			},
		},
		"multi zone, with read only instance, not within lookback, shardSize=3": {
			timeline: []event{
				// readOnlyTime is too old to matter
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true, readOnlyTime: now.Add(-2 * lookbackPeriod)},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-c", []uint32{userToken(userID, "zone-c", 3) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 4) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 5) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-3", "instance-6"}},
			},
		},
		"multi zone, with read only instance, with unknown ReadOnlyUpdatedTimestamp, shardSize=3": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				// instance 2 and 4 are included in addition to other instances in the same zone
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-c", []uint32{userToken(userID, "zone-c", 3) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 4) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 5) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true},
				{what: test, shardSize: 3, expected: []string{"instance-1", "instance-2", "instance-3", "instance-4", "instance-6"}},
			},
		},
		"multi zone, with read only instance, within lookback, shardSize=0": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true, readOnlyTime: now.Add(lookbackPeriod / 2)},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-c", []uint32{userToken(userID, "zone-c", 3) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 4) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 5) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true, readOnlyTime: now.Add(lookbackPeriod / 2)},
				{what: test, shardSize: 0, expected: []string{"instance-1", "instance-2", "instance-3", "instance-4", "instance-5", "instance-6"}},
			},
		},
		"multi zone, with read only instance, at lookback boundary, shardSize=0": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true, readOnlyTime: now.Add(lookbackPeriod)},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-c", []uint32{userToken(userID, "zone-c", 3) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 4) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 5) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 0, expected: []string{"instance-1", "instance-2", "instance-3", "instance-4", "instance-5", "instance-6"}},
			},
		},
		"multi zone, with read only instance, not within lookback, shardSize=0": {
			timeline: []event{
				// readOnlyTime is too old to matter
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true, readOnlyTime: now.Add(-2 * lookbackPeriod)},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-c", []uint32{userToken(userID, "zone-c", 3) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 4) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 5) + 1}, now.Add(-2*lookbackPeriod))},
				{what: test, shardSize: 0, expected: []string{"instance-1", "instance-3", "instance-4", "instance-5", "instance-6"}},
			},
		},
		"multi zone, with read only instance, with unknown ReadOnlyUpdatedTimestamp, shardSize=0": {
			timeline: []event{
				{what: add, instanceID: "instance-1", instanceDesc: generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-2", instanceDesc: generateRingInstanceWithInfo("instance-2", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true},
				{what: add, instanceID: "instance-3", instanceDesc: generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 2) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-4", instanceDesc: generateRingInstanceWithInfo("instance-4", "zone-c", []uint32{userToken(userID, "zone-c", 3) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-5", instanceDesc: generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 4) + 1}, now.Add(-2*lookbackPeriod))},
				{what: add, instanceID: "instance-6", instanceDesc: generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 5) + 1}, now.Add(-2*lookbackPeriod)), readOnly: true},
				{what: test, shardSize: 0, expected: []string{"instance-1", "instance-2", "instance-3", "instance-4", "instance-5", "instance-6"}},
			},
		},
	}

	for testName, testData := range tests {
		for _, updateRegisteredTimestampCache := range []bool{false, true} {
			for _, updateReadOnlyInstances := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%v/%v", testName, updateRegisteredTimestampCache, updateReadOnlyInstances), func(t *testing.T) {
					// Initialise the ring.
					ringDesc := &Desc{Ingesters: map[string]InstanceDesc{}}
					ring := newRingForTesting(Config{
						HeartbeatTimeout:     time.Hour,
						ZoneAwarenessEnabled: true,
					}, false)
					ring.setRingStateFromDesc(ringDesc, false, updateRegisteredTimestampCache, updateReadOnlyInstances)

					// Replay the events on the timeline.
					for ix, event := range testData.timeline {
						switch event.what {
						case add:
							event.instanceDesc.ReadOnly = event.readOnly
							event.instanceDesc.ReadOnlyUpdatedTimestamp = timeToUnixSecons(event.readOnlyTime)
							ringDesc.Ingesters[event.instanceID] = event.instanceDesc

							ring.setRingStateFromDesc(ringDesc, false, updateRegisteredTimestampCache, updateReadOnlyInstances)
						case remove:
							delete(ringDesc.Ingesters, event.instanceID)

							ring.setRingStateFromDesc(ringDesc, false, updateRegisteredTimestampCache, updateReadOnlyInstances)
						case test:
							rs, err := ring.ShuffleShardWithLookback(userID, event.shardSize, lookbackPeriod, now).GetAllHealthy(Read)
							require.NoError(t, err)
							assert.ElementsMatch(t, event.expected, rs.GetAddresses(), "step %d", ix)
						}
					}
				})
			}
		}
	}
}

// This test asserts that for some randomly generated history of shuffleShard (without lookback) results,
// every subsequent ShuffleShardWithLookback will be a superset of all previously recorded shuffleShards.
func TestRing_ShuffleShardWithLookback_CorrectnessWithFuzzy(t *testing.T) {
	// The goal of this test is NOT to ensure that the minimum required number of instances
	// are returned at any given time, BUT at least all required instances are returned.
	var (
		numInitialInstances = []int{9, 30, 60, 90}
		numInitialZones     = []int{1, 3}
		numEvents           = 100
		lookbackPeriod      = time.Hour
		delayBetweenEvents  = 5 * time.Minute // 12 events / hour
		userID              = "user-1"
	)

	for _, updateOldestRegisteredTimestamp := range []bool{false, true} {
		updateOldestRegisteredTimestamp := updateOldestRegisteredTimestamp

		for _, updateReadOnlyInstances := range []bool{false, true} {
			updateReadOnlyInstances := updateReadOnlyInstances

			for _, numInstances := range numInitialInstances {
				numInstances := numInstances

				for _, numZones := range numInitialZones {
					numZones := numZones

					testName := fmt.Sprintf("num instances = %d, num zones = %d, update oldest registered timestamp = %v, update read only instances = %v", numInstances, numZones, updateOldestRegisteredTimestamp, updateReadOnlyInstances)

					t.Run(testName, func(t *testing.T) {
						t.Parallel()

						// Randomise the seed but log it in case we need to reproduce the test on failure.
						seed := time.Now().UnixNano()
						rnd := rand.New(rand.NewSource(seed))
						t.Log("random generator seed:", seed)
						gen := NewRandomTokenGeneratorWithSeed(seed)

						// Initialise the ring.
						ringDesc := &Desc{Ingesters: generateRingInstances(gen, numInstances, numZones, 128)}
						ring := newRingForTesting(Config{
							HeartbeatTimeout:     time.Hour,
							ZoneAwarenessEnabled: true,
							ReplicationFactor:    3,
						}, false)

						updateRing := func() {
							ring.setRingStateFromDesc(ringDesc, false, updateOldestRegisteredTimestamp, updateReadOnlyInstances)

							if len(ring.ringZones) != numZones {
								t.Fatalf("number of zones changed, original=%d, current zones=%v", numZones, ring.ringZones)
							}
						}
						updateRing()

						// The simulation starts with the minimum shard size. Random events can later increase it.
						shardSize := numZones

						// The simulation assumes the initial ring contains instances registered
						// since more than the lookback period.
						currTime := time.Now().Add(lookbackPeriod).Add(time.Minute)

						// Add the initial shard to the history.
						rs, err := ring.shuffleShard(userID, shardSize, 0, currTime).GetReplicationSetForOperation(Read)
						require.NoError(t, err)

						type historyEntry struct {
							ReplicationSet
							shardSize int
							time.Time
						}
						// events, indexed by event id.
						history := map[int]historyEntry{
							0: {rs, shardSize, currTime},
						}

						// Track instances that have been marked as read-only
						readOnlyInstances := make(map[string]InstanceDesc)

						// Simulate a progression of random events over the time and, at each iteration of the simulation,
						// make sure the subring includes all non-removed instances picked from previous versions of the
						// ring up until the lookback period.
						nextInstanceID := len(ringDesc.Ingesters) + 1

						for eventID := 1; eventID <= numEvents; eventID++ {
							currTime = currTime.Add(delayBetweenEvents)

							switch r := rnd.Intn(100); {
							case r < 60:
								// Scale up instances by 1.
								instanceID := fmt.Sprintf("instance-%d", nextInstanceID)
								zoneID := fmt.Sprintf("zone-%d", nextInstanceID%numZones)
								nextInstanceID++
								ringDesc.Ingesters[instanceID] = generateRingInstanceWithInfo(instanceID, zoneID, gen.GenerateTokens(128, ringDesc.GetTokens()), currTime)
								updateRing()
								t.Logf("%d (%v): added instance %s, total instances %d", eventID, currTime.Format("03:04"), instanceID, len(ringDesc.Ingesters))

							case r < 70:
								// Scale down instances by 1.
								idToRemove := getRandomInstanceID(ringDesc.Ingesters, rnd)
								zone := ringDesc.Ingesters[idToRemove].Zone
								// Don't remove instance if it is the last instance in the zone,
								// because sharding works differently for different number of zones.
								if ring.instancesCountPerZone[zone] <= 1 {
									t.Logf("%d (%v): not removing last instance %s from zone %s", eventID, currTime.Format("03:04"), idToRemove, zone)
									break
								}

								delete(ringDesc.Ingesters, idToRemove)
								updateRing()
								t.Logf("%d (%v): removed instance %s, total instances %d", eventID, currTime.Format("03:04"), idToRemove, len(ringDesc.Ingesters))

								// Remove the terminated instance from the history.
								for eid, ringState := range history {
									for idx, desc := range ringState.Instances {
										// In this simulation instance ID == instance address.
										if desc.Addr == idToRemove {
											ringState.Instances = append(ringState.Instances[:idx], ringState.Instances[idx+1:]...)
											history[eid] = ringState
											break
										}
									}
								}

								// Removed instance can't be read-only.
								delete(readOnlyInstances, idToRemove)

							case r < 80:
								// Set an instance to read only
								instanceID := getRandomInstanceID(ringDesc.Ingesters, rnd)
								instanceDesc := ringDesc.Ingesters[instanceID]
								if !instanceDesc.ReadOnly {
									instanceDesc.ReadOnly = true
									instanceDesc.ReadOnlyUpdatedTimestamp = currTime.Unix()
									ringDesc.Ingesters[instanceID] = instanceDesc
									updateRing()

									readOnlyInstances[instanceID] = instanceDesc
									t.Logf("%d (%v): switched instance %s to read-only", eventID, currTime.Format("03:04"), instanceID)
								} else {
									t.Logf("%d (%v): instance %s is already read-only, not switching", eventID, currTime.Format("03:04"), instanceID)
								}

							case r < 90:
								// Set a read-only instance back to read-write
								if len(readOnlyInstances) > 0 {
									instanceID := getRandomInstanceID(readOnlyInstances, rnd)
									instanceDesc := ringDesc.Ingesters[instanceID]
									instanceDesc.ReadOnly = false
									instanceDesc.ReadOnlyUpdatedTimestamp = currTime.Unix()
									ringDesc.Ingesters[instanceID] = instanceDesc
									updateRing()

									delete(readOnlyInstances, instanceID)
									t.Logf("%d (%v): switched instance %s to read-write", eventID, currTime.Format("03:04"), instanceID)
								} else {
									t.Logf("%d (%v): no instance to switch to read-only found", eventID, currTime.Format("03:04"))
								}
							default:
								// Scale up shard size (keeping the per-zone balance).
								shardSize += numZones
								t.Logf("%d (%v): increased shard size %d", eventID, currTime.Format("03:04"), shardSize)
							}

							// Add the current shard to the history.
							rs, err = ring.shuffleShard(userID, shardSize, 0, currTime).GetReplicationSetForOperation(Read)
							require.NoError(t, err)
							history[eventID] = historyEntry{rs, shardSize, currTime}

							// Ensure the shard with lookback includes all instances from previous states of the ring.
							rsWithLookback, err := ring.ShuffleShardWithLookback(userID, shardSize, lookbackPeriod, currTime).GetReplicationSetForOperation(Read)
							require.NoError(t, err)
							t.Logf("%d: subrings for event=%v, shardSize=%v, lookbackPeriod=%v,\nrs=%v,\nrsWithLookback=%v", eventID, currTime.Format("03:04"), shardSize, lookbackPeriod, getSortedAddresses(rs), getSortedAddresses(rsWithLookback))

							for ix, ringState := range history {
								if ringState.Time.Before(currTime.Add(-lookbackPeriod)) {
									// This entry from the history is obsolete, we can remove it.
									delete(history, ix)
									continue
								}

								for _, desc := range ringState.Instances {
									if !rsWithLookback.Includes(desc.Addr) && !desc.ReadOnly {
										t.Fatalf("%d (%v) new shuffle shard with lookback is expected to include instance %s from ring state after event %d but it's missing (actual instances are: %v)",
											eventID, currTime.Format("03:04"), desc.Addr, ix, getSortedAddresses(rsWithLookback))
									}
								}
							}
						}
					})
				}
			}
		}
	}
}

func getSortedAddresses(rs ReplicationSet) []string {
	r := rs.GetAddresses()
	sort.Strings(r)
	return r
}

func TestRing_ShuffleShardWithLookback_Caching(t *testing.T) {
	t.Parallel()

	userID := "user-1"
	otherUserID := "user-2"
	subringSize := 3
	now := time.Now()

	scenarios := map[string]struct {
		instances []InstanceDesc
		test      func(t *testing.T, ring *Ring)
	}{
		"identical request": {
			instances: []InstanceDesc{
				generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
			},
			test: func(t *testing.T, ring *Ring) {
				first := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				second := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				require.Same(t, first, second)

				rs, err := first.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-3", "instance-5"}, rs.GetAddresses())

				require.Equal(t, 3, first.ZonesCount())
				require.Equal(t, 1, first.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-c"))
				require.Equal(t, 3, second.ZonesCount())
				require.Equal(t, 1, second.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-c"))
			},
		},
		"identical request after cleaning subring cache": {
			instances: []InstanceDesc{
				generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
			},
			test: func(t *testing.T, ring *Ring) {
				first := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				ring.CleanupShuffleShardCache(userID)
				second := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				require.NotSame(t, first, second)

				firstReplicationSet, err := first.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-3", "instance-5"}, firstReplicationSet.GetAddresses())

				secondReplicationSet, err := second.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-3", "instance-5"}, secondReplicationSet.GetAddresses())

				require.Equal(t, 3, first.ZonesCount())
				require.Equal(t, 1, first.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-c"))
				require.Equal(t, 3, second.ZonesCount())
				require.Equal(t, 1, second.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-c"))
			},
		},
		"different subring sizes": {
			instances: []InstanceDesc{
				generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
			},
			test: func(t *testing.T, ring *Ring) {
				first := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				second := ring.ShuffleShardWithLookback(userID, subringSize+1, time.Hour, now)
				require.NotSame(t, first, second)

				firstReplicationSet, err := first.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-3", "instance-5"}, firstReplicationSet.GetAddresses())

				secondReplicationSet, err := second.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-2", "instance-3", "instance-4", "instance-5", "instance-6"}, secondReplicationSet.GetAddresses())

				require.Equal(t, 3, first.ZonesCount())
				require.Equal(t, 1, first.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-c"))
				require.Equal(t, 3, second.ZonesCount())
				require.Equal(t, 2, second.InstancesInZoneCount("zone-a"))
				require.Equal(t, 2, second.InstancesInZoneCount("zone-b"))
				require.Equal(t, 2, second.InstancesInZoneCount("zone-c"))
			},
		},
		"different identifiers": {
			instances: []InstanceDesc{
				generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
			},
			test: func(t *testing.T, ring *Ring) {
				first := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				second := ring.ShuffleShardWithLookback(otherUserID, subringSize, time.Hour, now)
				require.NotSame(t, first, second)

				firstReplicationSet, err := first.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-3", "instance-5"}, firstReplicationSet.GetAddresses())

				secondReplicationSet, err := second.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-2", "instance-3", "instance-6"}, secondReplicationSet.GetAddresses())

				require.Equal(t, 3, first.ZonesCount())
				require.Equal(t, 1, first.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-c"))
				require.Equal(t, 3, second.ZonesCount())
				require.Equal(t, 1, second.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-c"))
			},
		},
		"all changes before beginning of either lookback window": {
			instances: []InstanceDesc{
				generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
			},
			test: func(t *testing.T, ring *Ring) {
				first := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now.Add(-3*time.Minute))
				second := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				require.Same(t, first, second)

				rs, err := first.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-3", "instance-5"}, rs.GetAddresses())

				require.Equal(t, 3, first.ZonesCount())
				require.Equal(t, 1, first.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-c"))
				require.Equal(t, 3, second.ZonesCount())
				require.Equal(t, 1, second.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-c"))
			},
		},
		"change within both lookback windows": {
			instances: []InstanceDesc{
				generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-5*time.Minute)),
				generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
			},
			test: func(t *testing.T, ring *Ring) {
				first := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now.Add(-3*time.Minute))
				second := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				require.Same(t, first, second)

				rs, err := first.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-2", "instance-3", "instance-5"}, rs.GetAddresses())

				require.Equal(t, 3, first.ZonesCount())
				require.Equal(t, 2, first.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-c"))
				require.Equal(t, 3, second.ZonesCount())
				require.Equal(t, 2, second.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-c"))
			},
		},
		"change on threshold of second lookback window": {
			instances: []InstanceDesc{
				generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-1*time.Hour)),
				generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
			},
			test: func(t *testing.T, ring *Ring) {
				first := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now.Add(-3*time.Minute))
				second := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				require.Same(t, first, second)

				rs, err := first.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-2", "instance-3", "instance-5"}, rs.GetAddresses())

				require.Equal(t, 3, first.ZonesCount())
				require.Equal(t, 2, first.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-c"))
				require.Equal(t, 3, second.ZonesCount())
				require.Equal(t, 2, second.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-c"))
			},
		},
		"change on threshold of first lookback window": {
			instances: []InstanceDesc{
				generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-63*time.Minute)),
				generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
			},
			test: func(t *testing.T, ring *Ring) {
				first := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now.Add(-3*time.Minute))
				second := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				require.NotSame(t, first, second)

				firstReplicationSet, err := first.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-2", "instance-3", "instance-5"}, firstReplicationSet.GetAddresses())

				secondReplicationSet, err := second.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-3", "instance-5"}, secondReplicationSet.GetAddresses())

				require.Equal(t, 3, first.ZonesCount())
				require.Equal(t, 2, first.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-c"))
				require.Equal(t, 3, second.ZonesCount())
				require.Equal(t, 1, second.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-c"))
			},
		},
		"change between thresholds of first and second lookback windows": {
			instances: []InstanceDesc{
				generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-62*time.Minute)),
				generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
			},
			test: func(t *testing.T, ring *Ring) {
				first := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now.Add(-3*time.Minute))
				second := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				require.NotSame(t, first, second)

				firstReplicationSet, err := first.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-2", "instance-3", "instance-5"}, firstReplicationSet.GetAddresses())

				secondReplicationSet, err := second.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-3", "instance-5"}, secondReplicationSet.GetAddresses())

				require.Equal(t, 3, first.ZonesCount())
				require.Equal(t, 2, first.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, first.InstancesInZoneCount("zone-c"))
				require.Equal(t, 3, second.ZonesCount())
				require.Equal(t, 1, second.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, second.InstancesInZoneCount("zone-c"))
			},
		},
		"change between thresholds of first and second lookback windows with out-of-order subring calls": {
			instances: []InstanceDesc{
				generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-62*time.Minute)),
				generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
			},
			test: func(t *testing.T, ring *Ring) {
				firstEarlyThreshold := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now.Add(-3*time.Minute))
				firstLaterThreshold := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				require.NotSame(t, firstEarlyThreshold, firstLaterThreshold)

				require.Equal(t, 3, firstEarlyThreshold.ZonesCount())
				require.Equal(t, 2, firstEarlyThreshold.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, firstEarlyThreshold.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, firstEarlyThreshold.InstancesInZoneCount("zone-c"))
				require.Equal(t, 3, firstLaterThreshold.ZonesCount())
				require.Equal(t, 1, firstLaterThreshold.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, firstLaterThreshold.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, firstLaterThreshold.InstancesInZoneCount("zone-c"))

				secondEarlyThreshold := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now.Add(-3*time.Minute))
				// The subring for the later lookback window should evict the cache entry for the earlier window.
				require.NotSame(t, firstEarlyThreshold, secondEarlyThreshold)

				secondLaterThreshold := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				// The subring for the later lookback window should still be cached.
				require.Same(t, firstLaterThreshold, secondLaterThreshold)

				require.Equal(t, 3, secondEarlyThreshold.ZonesCount())
				require.Equal(t, 2, secondEarlyThreshold.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, secondEarlyThreshold.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, secondEarlyThreshold.InstancesInZoneCount("zone-c"))
				require.Equal(t, 3, secondLaterThreshold.ZonesCount())
				require.Equal(t, 1, secondLaterThreshold.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, secondLaterThreshold.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, secondLaterThreshold.InstancesInZoneCount("zone-c"))

				firstEarlyReplicationSet, err := firstEarlyThreshold.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-2", "instance-3", "instance-5"}, firstEarlyReplicationSet.GetAddresses())

				secondEarlyReplicationSet, err := secondEarlyThreshold.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-2", "instance-3", "instance-5"}, secondEarlyReplicationSet.GetAddresses())

				laterReplicationSet, err := firstLaterThreshold.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-3", "instance-5"}, laterReplicationSet.GetAddresses())
			},
		},
		"different lookback windows": {
			instances: []InstanceDesc{
				generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-1*time.Hour)),
				generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
				generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
			},
			test: func(t *testing.T, ring *Ring) {
				firstHourWindow := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				firstHalfHourWindow := ring.ShuffleShardWithLookback(userID, subringSize, 30*time.Minute, now)
				require.NotSame(t, firstHourWindow, firstHalfHourWindow, "should not reuse subring for different lookback windows when results are not equivalent")

				require.Equal(t, 3, firstHourWindow.ZonesCount())
				require.Equal(t, 2, firstHourWindow.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, firstHourWindow.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, firstHourWindow.InstancesInZoneCount("zone-c"))
				require.Equal(t, 3, firstHalfHourWindow.ZonesCount())
				require.Equal(t, 1, firstHalfHourWindow.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, firstHalfHourWindow.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, firstHalfHourWindow.InstancesInZoneCount("zone-c"))

				secondHourWindow := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
				require.Same(t, firstHourWindow, secondHourWindow, "should reuse subring for identical request")

				secondHalfHourWindow := ring.ShuffleShardWithLookback(userID, subringSize, 30*time.Minute, now)
				require.Same(t, firstHalfHourWindow, secondHalfHourWindow, "should separately cache rings for different lookback windows")

				require.Equal(t, 3, secondHourWindow.ZonesCount())
				require.Equal(t, 2, secondHourWindow.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, secondHourWindow.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, secondHourWindow.InstancesInZoneCount("zone-c"))
				require.Equal(t, 3, secondHalfHourWindow.ZonesCount())
				require.Equal(t, 1, secondHalfHourWindow.InstancesInZoneCount("zone-a"))
				require.Equal(t, 1, secondHalfHourWindow.InstancesInZoneCount("zone-b"))
				require.Equal(t, 1, secondHalfHourWindow.InstancesInZoneCount("zone-c"))

				hourReplicationSet, err := firstHourWindow.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-2", "instance-3", "instance-5"}, hourReplicationSet.GetAddresses())

				halfHourReplicationSet, err := firstHalfHourWindow.GetAllHealthy(Read)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"instance-1", "instance-3", "instance-5"}, halfHourReplicationSet.GetAddresses())

				twentyMinuteWindow := ring.ShuffleShardWithLookback(userID, subringSize, 20*time.Minute, now)
				require.NotSame(t, firstHalfHourWindow, twentyMinuteWindow, "should not reuse subring for different lookback windows even if results are currently equivalent")

			},
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			cfg := Config{KVStore: kv.Config{}, ReplicationFactor: 1, ZoneAwarenessEnabled: true}
			registry := prometheus.NewRegistry()
			ring, err := NewWithStoreClientAndStrategy(cfg, testRingName, testRingKey, nil, NewDefaultReplicationStrategy(), registry, log.NewNopLogger())
			require.NoError(t, err)

			instancesMap := make(map[string]InstanceDesc, len(scenario.instances))
			for _, instance := range scenario.instances {
				instancesMap[instance.Addr] = instance
			}

			require.Len(t, instancesMap, len(scenario.instances), "invalid test case: one or more instances share the same name")

			ringDesc := &Desc{Ingesters: instancesMap}
			ring.updateRingState(ringDesc)
			scenario.test(t, ring)
		})
	}
}

func TestRing_ShuffleShardWithLookback_CachingAfterTopologyChange(t *testing.T) {
	cfg := Config{KVStore: kv.Config{}, ReplicationFactor: 1, ZoneAwarenessEnabled: true}
	registry := prometheus.NewRegistry()
	ring, err := NewWithStoreClientAndStrategy(cfg, testRingName, testRingKey, nil, NewDefaultReplicationStrategy(), registry, log.NewNopLogger())
	require.NoError(t, err)

	userID := "user-1"
	now := time.Now()

	initialInstances := map[string]InstanceDesc{
		"instance-1": generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*time.Hour)),
		"instance-2": generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
		"instance-3": generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
		"instance-4": generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
		"instance-5": generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
		"instance-6": generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
	}

	initialRingDesc := &Desc{Ingesters: initialInstances}
	ring.updateRingState(initialRingDesc)

	subringSize := 3
	first := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
	firstReplicationSet, err := first.GetAllHealthy(Read)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"instance-1", "instance-3", "instance-5"}, firstReplicationSet.GetAddresses())

	require.Equal(t, 3, first.ZonesCount())
	require.Equal(t, 1, first.InstancesInZoneCount("zone-a"))
	require.Equal(t, 1, first.InstancesInZoneCount("zone-b"))
	require.Equal(t, 1, first.InstancesInZoneCount("zone-c"))

	updatedInstances := map[string]InstanceDesc{
		// instance-1 has unregistered
		"instance-2": generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
		"instance-3": generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
		"instance-4": generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
		"instance-5": generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
		"instance-6": generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
	}

	updatedRingDesc := &Desc{Ingesters: updatedInstances}
	ring.updateRingState(updatedRingDesc)

	second := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
	require.NotSame(t, first, second)
	secondReplicationSet, err := second.GetAllHealthy(Read)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"instance-2", "instance-3", "instance-5"}, secondReplicationSet.GetAddresses())

	require.Equal(t, 3, second.ZonesCount())
	require.Equal(t, 1, second.InstancesInZoneCount("zone-a"))
	require.Equal(t, 1, second.InstancesInZoneCount("zone-b"))
	require.Equal(t, 1, second.InstancesInZoneCount("zone-c"))
}

func makeReadOnly(desc InstanceDesc, ts time.Time) InstanceDesc {
	desc.ReadOnly = true
	desc.ReadOnlyUpdatedTimestamp = ts.Unix()
	return desc
}

func TestRing_ShuffleShardWithLookback_CachingAfterReadOnlyChange(t *testing.T) {
	cfg := Config{KVStore: kv.Config{}, ReplicationFactor: 1, ZoneAwarenessEnabled: true}
	registry := prometheus.NewRegistry()
	ring, err := NewWithStoreClientAndStrategy(cfg, testRingName, testRingKey, nil, NewDefaultReplicationStrategy(), registry, log.NewNopLogger())
	require.NoError(t, err)

	userID := "user-1"
	now := time.Now()

	makeInstances := func() map[string]InstanceDesc {
		return map[string]InstanceDesc{
			"instance-1": generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*time.Hour)),
			"instance-2": generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
			"instance-3": generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
			"instance-4": generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
			"instance-5": generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
			"instance-6": generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
		}
	}

	initialRingDesc := &Desc{Ingesters: makeInstances()}
	ring.updateRingState(initialRingDesc)

	subringSize := 3
	first := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
	firstReplicationSet, err := first.GetAllHealthy(Read)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"instance-1", "instance-3", "instance-5"}, firstReplicationSet.GetAddresses())

	require.Equal(t, 3, first.ZonesCount())
	require.Equal(t, 1, first.InstancesInZoneCount("zone-a"))
	require.Equal(t, 1, first.InstancesInZoneCount("zone-b"))
	require.Equal(t, 1, first.InstancesInZoneCount("zone-c"))

	updatedInstances := makeInstances()
	updatedInstances["instance-1"] = makeReadOnly(updatedInstances["instance-1"], now)
	updatedInstances["instance-5"] = makeReadOnly(updatedInstances["instance-5"], now)

	updatedRingDesc := &Desc{Ingesters: updatedInstances}
	ring.updateRingState(updatedRingDesc)

	second := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
	require.NotSame(t, first, second)
	secondReplicationSet, err := second.GetAllHealthy(Read)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"instance-1", "instance-2", "instance-3", "instance-5", "instance-6"}, secondReplicationSet.GetAddresses())

	require.Equal(t, 3, second.ZonesCount())
	require.Equal(t, 2, second.InstancesInZoneCount("zone-a"))
	require.Equal(t, 1, second.InstancesInZoneCount("zone-b"))
	require.Equal(t, 2, second.InstancesInZoneCount("zone-c"))
}

func TestRing_ShuffleShardWithLookback_CachingAfterHeartbeatOrStateChange(t *testing.T) {
	cfg := Config{KVStore: kv.Config{}, ReplicationFactor: 1, ZoneAwarenessEnabled: true}
	registry := prometheus.NewRegistry()
	ring, err := NewWithStoreClientAndStrategy(cfg, testRingName, testRingKey, nil, NewDefaultReplicationStrategy(), registry, log.NewNopLogger())
	require.NoError(t, err)

	userID := "user-1"
	now := time.Now()

	initialInstances := map[string]InstanceDesc{
		"instance-1": generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*time.Hour)),
		"instance-2": generateRingInstanceWithInfo("instance-2", "zone-a", []uint32{userToken(userID, "zone-a", 1) + 1}, now.Add(-2*time.Hour)),
		"instance-3": generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour)),
		"instance-4": generateRingInstanceWithInfo("instance-4", "zone-b", []uint32{userToken(userID, "zone-b", 1) + 1}, now.Add(-2*time.Hour)),
		"instance-5": generateRingInstanceWithInfo("instance-5", "zone-c", []uint32{userToken(userID, "zone-c", 0) + 1}, now.Add(-2*time.Hour)),
		"instance-6": generateRingInstanceWithInfo("instance-6", "zone-c", []uint32{userToken(userID, "zone-c", 1) + 1}, now.Add(-2*time.Hour)),
	}

	initialRingDesc := &Desc{Ingesters: initialInstances}
	ring.updateRingState(initialRingDesc)

	subringSize := 3
	first := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
	firstReplicationSet, err := first.GetAllHealthy(Read)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"instance-1", "instance-3", "instance-5"}, firstReplicationSet.GetAddresses())

	require.Equal(t, 3, first.ZonesCount())
	require.Equal(t, 1, first.InstancesInZoneCount("zone-a"))
	require.Equal(t, 1, first.InstancesInZoneCount("zone-b"))
	require.Equal(t, 1, first.InstancesInZoneCount("zone-c"))

	// Simulate an instance reporting a heartbeat.
	updatedInstance1 := generateRingInstanceWithInfo("instance-1", "zone-a", []uint32{userToken(userID, "zone-a", 0) + 1}, now.Add(-2*time.Hour))
	updatedInstance1Timestamp := time.Now().Add(32 * time.Minute).Unix()
	updatedInstance1.Timestamp = updatedInstance1Timestamp

	// Simulate an instance reporting that it's about to leave the ring.
	updatedInstance3 := generateRingInstanceWithInfo("instance-3", "zone-b", []uint32{userToken(userID, "zone-b", 0) + 1}, now.Add(-2*time.Hour))
	updatedInstance3.Timestamp = initialInstances["instance-3"].Timestamp
	updatedInstance3.State = InstanceState_LEAVING

	updatedInstances := map[string]InstanceDesc{
		"instance-1": updatedInstance1,
		"instance-2": initialInstances["instance-2"],
		"instance-3": updatedInstance3,
		"instance-4": initialInstances["instance-4"],
		"instance-5": initialInstances["instance-5"],
		"instance-6": initialInstances["instance-6"],
	}

	updatedRingDesc := &Desc{Ingesters: updatedInstances}
	ring.updateRingState(updatedRingDesc)

	second := ring.ShuffleShardWithLookback(userID, subringSize, time.Hour, now)
	require.Same(t, first, second)

	rs, err := first.GetAllHealthy(Read)
	require.NoError(t, err)

	require.ElementsMatch(t, []string{"instance-1", "instance-3", "instance-5"}, rs.GetAddresses())

	for _, instance := range rs.Instances {
		switch instance.Addr {
		case "instance-1":
			require.Equal(t, updatedInstance1Timestamp, instance.Timestamp)
		case "instance-3":
			require.Equal(t, InstanceState_LEAVING, instance.State)
		}
	}
}

func TestRing_ShuffleShardWithLookback_CachingConcurrency(t *testing.T) {
	const (
		numWorkers           = 10
		numRequestsPerWorker = 1000
	)

	now := time.Now()
	cfg := Config{KVStore: kv.Config{}, ReplicationFactor: 1, ZoneAwarenessEnabled: true}
	registry := prometheus.NewRegistry()
	ring, err := NewWithStoreClientAndStrategy(cfg, testRingName, testRingKey, nil, NewDefaultReplicationStrategy(), registry, log.NewNopLogger())
	require.NoError(t, err)

	gen := initTokenGenerator(t)

	// Add some instances to the ring.
	ringDesc := &Desc{Ingesters: map[string]InstanceDesc{
		"instance-1": generateRingInstanceWithInfo("instance-1", "zone-a", gen.GenerateTokens(128, nil), now.Add(-2*time.Hour)),
		"instance-2": makeReadOnly(generateRingInstanceWithInfo("instance-2", "zone-a", gen.GenerateTokens(128, nil), now.Add(-2*time.Hour)), now),
		"instance-3": generateRingInstanceWithInfo("instance-3", "zone-b", gen.GenerateTokens(128, nil), now.Add(-2*time.Hour)),
		"instance-4": generateRingInstanceWithInfo("instance-4", "zone-b", gen.GenerateTokens(128, nil), now.Add(-2*time.Hour)),
		"instance-5": generateRingInstanceWithInfo("instance-5", "zone-c", gen.GenerateTokens(128, nil), now.Add(-2*time.Hour)),
		"instance-6": makeReadOnly(generateRingInstanceWithInfo("instance-6", "zone-c", gen.GenerateTokens(128, nil), now.Add(-2*time.Hour)), now.Add(-2*time.Hour)),
	}}

	ring.updateRingState(ringDesc)

	for _, shardSize := range []int{3, 0} {
		t.Run(fmt.Sprintf("shardSize=%d", shardSize), func(t *testing.T) {
			// Start the workers.
			wg := sync.WaitGroup{}
			wg.Add(numWorkers)

			for w := 0; w < numWorkers; w++ {
				go func(workerID int) {
					defer wg.Done()

					// Get the subring once. This is the one expected from subsequent requests.
					userID := fmt.Sprintf("user-%d", workerID)
					expected := ring.ShuffleShardWithLookback(userID, shardSize, time.Hour, now)

					for r := 0; r < numRequestsPerWorker; r++ {
						actual := ring.ShuffleShardWithLookback(userID, shardSize, time.Hour, now)
						require.Equal(t, expected, actual)

						// Get the subring for a new user each time too, in order to stress the setter too
						// (if we only read from the cache there's no read/write concurrent access).
						ring.ShuffleShardWithLookback(fmt.Sprintf("stress-%d", r), shardSize, time.Hour, now)
					}
				}(w)
			}

			// Wait until all workers have done.
			wg.Wait()
		})
	}
}

func BenchmarkRing_ShuffleShard(b *testing.B) {
	for _, numInstances := range []int{50, 100, 1000} {
		for _, numZones := range []int{1, 3} {
			for _, shardSize := range []int{0, 3, 10, 30, 100, 1000} {
				b.Run(fmt.Sprintf("num instances = %d, num zones = %d, shard size = %d", numInstances, numZones, shardSize), func(b *testing.B) {
					benchmarkShuffleSharding(b, numInstances, numZones, 128, shardSize, false)
				})
			}
		}
	}
}

func BenchmarkRing_ShuffleShardCached(b *testing.B) {
	for _, numInstances := range []int{50, 100, 1000} {
		for _, numZones := range []int{1, 3} {
			for _, shardSize := range []int{0, 3, 10, 30, 100, 1000} {
				b.Run(fmt.Sprintf("num instances = %d, num zones = %d, shard size = %d", numInstances, numZones, shardSize), func(b *testing.B) {
					benchmarkShuffleSharding(b, numInstances, numZones, 128, shardSize, true)
				})
			}
		}
	}
}

func BenchmarkRing_ShuffleShard_512Tokens(b *testing.B) {
	const (
		numInstances = 30
		numZones     = 3
		numTokens    = 512
		shardSize    = 9
		cacheEnabled = false
	)

	benchmarkShuffleSharding(b, numInstances, numZones, numTokens, shardSize, cacheEnabled)
}

func BenchmarkRing_ShuffleShard_LargeShardSize(b *testing.B) {
	const (
		numInstances = 300 // = 100 per zone
		numZones     = 3
		numTokens    = 512
		shardSize    = 270 // = 90 per zone
		cacheEnabled = false
	)

	benchmarkShuffleSharding(b, numInstances, numZones, numTokens, shardSize, cacheEnabled)
}

func BenchmarkRing_ShuffleShard_ShardSize_0(b *testing.B) {
	const (
		numInstances = 90
		numZones     = 3
		numTokens    = 512
		shardSize    = 0
		cacheEnabled = false
	)

	benchmarkShuffleSharding(b, numInstances, numZones, numTokens, shardSize, cacheEnabled)
}

func benchmarkShuffleSharding(b *testing.B, numInstances, numZones, numTokens, shardSize int, cache bool) {
	// Initialise the ring.
	ringDesc := &Desc{Ingesters: generateRingInstances(initTokenGenerator(b), numInstances, numZones, numTokens)}
	ring := newRingForTesting(Config{HeartbeatTimeout: time.Hour, ZoneAwarenessEnabled: true, SubringCacheDisabled: !cache}, true)
	ring.setRingStateFromDesc(ringDesc, false, false, true)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ring.ShuffleShard("tenant-1", shardSize)
	}
}

func BenchmarkRing_Get(b *testing.B) {
	benchCases := map[string]struct {
		numInstances      int
		numZones          int
		replicationFactor int
	}{
		"with zone awareness": {
			numInstances:      99,
			numZones:          3,
			replicationFactor: 3,
		},
		"one excluded zone": {
			numInstances:      66,
			numZones:          2,
			replicationFactor: 3,
		},
		"without zone awareness": {
			numInstances:      3,
			numZones:          1,
			replicationFactor: 3,
		},
		"without zone awareness, not enough instances": {
			numInstances:      2,
			numZones:          1,
			replicationFactor: 3,
		},
	}

	for benchName, benchCase := range benchCases {
		// Initialise the ring.
		ringDesc := &Desc{Ingesters: generateRingInstances(initTokenGenerator(b), benchCase.numInstances, benchCase.numZones, numTokens)}
		ring := newRingForTesting(Config{
			HeartbeatTimeout:     time.Hour,
			ZoneAwarenessEnabled: benchCase.numZones > 1,
			SubringCacheDisabled: true,
			ReplicationFactor:    benchCase.replicationFactor,
		}, true)
		ring.setRingStateFromDesc(ringDesc, false, false, false)

		buf, bufHosts, bufZones := MakeBuffersForGet()
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		expectedInstances := min(benchCase.numInstances, benchCase.replicationFactor)
		if ring.cfg.ZoneAwarenessEnabled {
			expectedInstances = min(benchCase.numZones, benchCase.replicationFactor)
		}

		b.Run(benchName, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				set, err := ring.Get(r.Uint32(), Write, buf, bufHosts, bufZones)
				assert.NoError(b, err)
				assert.Equal(b, expectedInstances, len(set.Instances))
			}
		})
	}
}

func TestRing_Get_NoMemoryAllocations(t *testing.T) {
	// Initialise the ring.
	ringDesc := &Desc{Ingesters: generateRingInstances(initTokenGenerator(t), 3, 3, 128)}
	ring := newRingForTesting(Config{HeartbeatTimeout: time.Hour, ZoneAwarenessEnabled: true, SubringCacheDisabled: true, ReplicationFactor: 3}, true)
	ring.setRingStateFromDesc(ringDesc, false, false, false)

	buf, bufHosts, bufZones := MakeBuffersForGet()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	numAllocs := testing.AllocsPerRun(10, func() {
		set, err := ring.Get(r.Uint32(), Write, buf, bufHosts, bufZones)
		if err != nil || len(set.Instances) != 3 {
			t.Fail()
		}
	})

	assert.Equal(t, float64(0), numAllocs)
}

// generateTokensLinear returns tokens with a linear distribution.
func generateTokensLinear(instanceID, numInstances, numTokens int) []uint32 {
	tokens := make([]uint32, 0, numTokens)
	step := math.MaxUint32 / int64(numTokens)
	offset := (step / int64(numInstances)) * int64(instanceID)

	for t := offset; t <= math.MaxUint32; t += step {
		tokens = append(tokens, uint32(t))
	}

	return tokens
}

func generateRingInstances(gen TokenGenerator, numInstances, numZones, numTokens int) map[string]InstanceDesc {
	instances := make(map[string]InstanceDesc, numInstances)

	var allTokens []uint32

	for i := 1; i <= numInstances; i++ {
		id, desc, newTokens := generateRingInstance(gen, i, i%numZones, numTokens, allTokens)
		instances[id] = desc
		allTokens = append(allTokens, newTokens...)
	}

	return instances
}

func generateRingInstance(gen TokenGenerator, id, zone, numTokens int, usedTokens []uint32) (string, InstanceDesc, Tokens) {
	instanceID := fmt.Sprintf("instance-%d", id)
	zoneID := fmt.Sprintf("zone-%d", zone)
	newTokens := gen.GenerateTokens(numTokens, usedTokens)

	return instanceID, generateRingInstanceWithInfo(instanceID, zoneID, newTokens, time.Now()), newTokens
}

func generateRingInstanceWithInfo(addr, zone string, tokens []uint32, registeredAt time.Time) InstanceDesc {
	var regts int64
	if !registeredAt.IsZero() {
		regts = registeredAt.Unix()
	}
	return InstanceDesc{
		Addr:                addr,
		Timestamp:           time.Now().Unix(),
		RegisteredTimestamp: regts,
		State:               InstanceState_ACTIVE,
		Tokens:              tokens,
		Zone:                zone,
	}
}

// compareReplicationSets returns the list of instance addresses which differ between the two sets.
func compareReplicationSets(first, second ReplicationSet) (added, removed []string) {
	for _, instance := range first.Instances {
		if !second.Includes(instance.Addr) {
			added = append(added, instance.Addr)
		}
	}

	for _, instance := range second.Instances {
		if !first.Includes(instance.Addr) {
			removed = append(removed, instance.Addr)
		}
	}

	return
}

// This test verifies that ring is getting updates, even after extending check in the loop method.
func TestRingUpdates(t *testing.T) {
	const (
		numInstances = 3
		numZones     = 3
	)

	tests := map[string]struct {
		excludedZones                []string
		expectedInstances            int
		expectedZones                int
		expectedInstacesCountPerZone map[string]int
	}{
		"without excluded zones": {
			expectedInstances:            3,
			expectedZones:                3,
			expectedInstacesCountPerZone: map[string]int{"zone-0": 1, "zone-1": 1, "zone-2": 1},
		},
		"with excluded zones": {
			excludedZones:                []string{"zone-0"},
			expectedInstances:            2,
			expectedZones:                2,
			expectedInstacesCountPerZone: map[string]int{"zone-0": 0 /* excluded! */, "zone-1": 1, "zone-2": 1},
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			inmem, closer := consul.NewInMemoryClient(GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			cfg := Config{
				KVStore:           kv.Config{Mock: inmem},
				HeartbeatTimeout:  1 * time.Minute,
				ReplicationFactor: 3,
				ExcludedZones:     flagext.StringSliceCSV(testData.excludedZones),
			}

			ring, err := New(cfg, "test", "test", log.NewNopLogger(), nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), ring))
			t.Cleanup(func() {
				_ = services.StopAndAwaitTerminated(context.Background(), ring)
			})

			require.Equal(t, 0, ring.InstancesCount())

			// Start 1 lifecycler for each instance we want to register in the ring.
			var lifecyclers []*Lifecycler
			for instanceID := 1; instanceID <= numInstances; instanceID++ {
				lifecyclers = append(lifecyclers, startLifecycler(t, cfg, 100*time.Millisecond, instanceID, numZones))
			}

			// Ensure the ring client got updated.
			test.Poll(t, 1*time.Second, testData.expectedInstances, func() interface{} {
				return ring.InstancesCount()
			})

			for z, cnt := range testData.expectedInstacesCountPerZone {
				require.Equal(t, cnt, ring.InstancesInZoneCount(z), z)
			}

			// Ensure there's no instance in an excluded zone.
			for _, excluded := range testData.excludedZones {
				require.Equal(t, 0, ring.InstancesInZoneCount(excluded))
			}

			require.Equal(t, testData.expectedZones, ring.ZonesCount())

			// Sleep for a few seconds (ring timestamp resolution is 1 second, so to verify that ring is updated in the background,
			// sleep for 2 seconds)
			time.Sleep(2 * time.Second)

			rs, err := ring.GetAllHealthy(Read)
			require.NoError(t, err)

			now := time.Now()
			for _, ing := range rs.Instances {
				require.InDelta(t, now.UnixNano(), time.Unix(ing.Timestamp, 0).UnixNano(), float64(1500*time.Millisecond.Nanoseconds()))

				if len(testData.excludedZones) > 0 {
					assert.False(t, slices.Contains(testData.excludedZones, ing.Zone))
				}
			}

			// Stop all lifecyclers.
			for _, lc := range lifecyclers {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), lc))
			}

			// Ensure the ring client got updated.
			test.Poll(t, 1*time.Second, 0, func() interface{} {
				return ring.InstancesCount()
			})
		})
	}
}

func startLifecycler(t *testing.T, cfg Config, heartbeat time.Duration, lifecyclerID int, zones int) *Lifecycler {
	lcCfg := LifecyclerConfig{
		RingConfig:           cfg,
		NumTokens:            16,
		HeartbeatPeriod:      heartbeat,
		ObservePeriod:        0,
		JoinAfter:            0,
		Zone:                 fmt.Sprintf("zone-%d", lifecyclerID%zones),
		Addr:                 fmt.Sprintf("addr-%d", lifecyclerID),
		ID:                   fmt.Sprintf("instance-%d", lifecyclerID),
		UnregisterOnShutdown: true,
	}

	lc, err := NewLifecycler(lcCfg, &noopFlushTransferer{}, "test", "test", false, log.NewNopLogger(), nil)
	require.NoError(t, err)

	lc.AddListener(services.NewListener(nil, nil, nil, nil, func(_ services.State, failure error) {
		t.Log("lifecycler", lifecyclerID, "failed:", failure)
		t.Fail()
	}))

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), lc))

	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), lc)
	})

	return lc
}

// This test checks if shuffle-sharded ring can be reused, and whether it receives
// updates from "main" ring.
func TestRing_ShuffleShard_Caching(t *testing.T) {
	t.Parallel()

	inmem, closer := consul.NewInMemoryClientWithConfig(GetCodec(), consul.Config{
		MaxCasRetries: 20,
		CasRetryDelay: 500 * time.Millisecond,
	}, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := Config{
		KVStore:              kv.Config{Mock: inmem},
		HeartbeatTimeout:     1 * time.Minute,
		ReplicationFactor:    3,
		ZoneAwarenessEnabled: true,
	}

	ring, err := New(cfg, "test", "test", log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ring))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), ring)
	})

	// We will stop <number of zones> instances later, to see that subring is recomputed.
	const zones = 3
	const numLifecyclers = zones * 3 // 3 instances in each zone.

	lcs := []*Lifecycler(nil)
	for i := 0; i < numLifecyclers; i++ {
		lc := startLifecycler(t, cfg, 500*time.Millisecond, i, zones)
		lcs = append(lcs, lc)
	}

	// Wait until all instances in the ring are InstanceState_ACTIVE.
	test.Poll(t, 5*time.Second, numLifecyclers, func() interface{} {
		active := 0
		rs, _ := ring.GetReplicationSetForOperation(Read)
		for _, ing := range rs.Instances {
			if ing.State == InstanceState_ACTIVE {
				active++
			}
		}
		return active
	})

	// Use shardSize = zones, to get one instance from each zone.
	const shardSize = zones
	const user = "user"

	// This subring should be cached, and reused.
	subring := ring.ShuffleShard(user, shardSize)

	// Do 100 iterations over two seconds. Make sure we get the same subring.
	const iters = 100
	sleep := (2 * time.Second) / iters
	for i := 0; i < iters; i++ {
		newSubring := ring.ShuffleShard(user, shardSize)
		require.Same(t, subring, newSubring, "cached subring reused")
		require.Equal(t, shardSize, subring.InstancesCount())
		time.Sleep(sleep)
	}

	// Make sure subring has up-to-date timestamps.
	rs, err := subring.GetReplicationSetForOperation(Read)
	require.NoError(t, err)

	now := time.Now()
	for _, ing := range rs.Instances {
		// Lifecyclers use 500ms refresh, but timestamps use 1s resolution, so we better give it some extra buffer.
		assert.InDelta(t, now.UnixNano(), time.Unix(ing.Timestamp, 0).UnixNano(), float64(2*time.Second.Nanoseconds()))
	}

	// Mark instance as read only via lifecycler and wait for it to show up in the ring
	require.NoError(t, lcs[0].ChangeReadOnlyState(context.Background(), true))
	test.Poll(t, 5*time.Second, 1, func() interface{} {
		return ring.readOnlyInstanceCount()
	})

	// Cache should have been invalidated from read only change
	newSubring := ring.ShuffleShard(user, shardSize)
	require.NotSame(t, subring, newSubring)

	// Assert that shuffle shard with lookback is cached, unless it's not within the lookback
	newSubring = ring.ShuffleShardWithLookback(user, shardSize, 10*time.Minute, time.Now())
	require.NotSame(t, subring, newSubring)
	newSubring2 := ring.ShuffleShardWithLookback(user, shardSize, 10*time.Minute, time.Now())
	require.Same(t, newSubring, newSubring2)
	newSubring3 := ring.ShuffleShardWithLookback(user, shardSize, time.Nanosecond, time.Now())
	require.NotSame(t, newSubring2, newSubring3)

	// Now stop one lifecycler from each zone. Subring needs to be recomputed.
	for i := 0; i < zones; i++ {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), lcs[i]))
	}

	test.Poll(t, 5*time.Second, numLifecyclers-zones, func() interface{} {
		return ring.InstancesCount()
	})

	// Change of instances -> new subring needed.
	newSubring = ring.ShuffleShard("user", zones)
	require.NotSame(t, subring, newSubring)
	require.Equal(t, zones, subring.InstancesCount())

	// Change of shard size -> new subring needed.
	subring = newSubring
	newSubring = ring.ShuffleShard("user", 1)
	require.NotSame(t, subring, newSubring)
	// Zone-aware shuffle-shard gives all zones the same number of instances (at least one).
	require.Equal(t, zones, newSubring.InstancesCount())

	// Verify that getting the same subring uses cached instance.
	subring = newSubring
	newSubring = ring.ShuffleShard("user", 1)
	require.Same(t, subring, newSubring)

	// But after cleanup, it doesn't.
	ring.CleanupShuffleShardCache("user")
	newSubring = ring.ShuffleShard("user", 1)
	require.NotSame(t, subring, newSubring)

	// If we ask for ALL instances, we get a ring with the same instances as the original ring
	newRing := ring.ShuffleShard("user", numLifecyclers).(*Ring)
	ring.mtx.RLock()
	require.Equal(t, ring.ringDesc.Ingesters, newRing.ringDesc.Ingesters)
	ring.mtx.RUnlock()

	// If we ask for single instance, but use long lookback, we get a ring again with the same instances as the original
	newRing = ring.ShuffleShardWithLookback("user", 1, 10*time.Minute, time.Now()).(*Ring)
	ring.mtx.RLock()
	require.Equal(t, ring.ringDesc.Ingesters, newRing.ringDesc.Ingesters)
	ring.mtx.RUnlock()
}

// User shuffle shard token.
func userToken(user, zone string, skip int) uint32 {
	r := rand.New(rand.NewSource(shard.ShuffleShardSeed(user, zone)))

	for ; skip > 0; skip-- {
		_ = r.Uint32()
	}
	return r.Uint32()
}

func TestUpdateMetrics(t *testing.T) {
	cfg := Config{
		KVStore:              kv.Config{},
		HeartbeatTimeout:     0, // get healthy stats
		ReplicationFactor:    3,
		ZoneAwarenessEnabled: true,
	}

	registry := prometheus.NewRegistry()

	// create the ring to set up metrics, but do not start
	ring, err := NewWithStoreClientAndStrategy(cfg, testRingName, testRingKey, nil, NewDefaultReplicationStrategy(), registry, log.NewNopLogger())
	require.NoError(t, err)

	ringDesc := Desc{
		Ingesters: map[string]InstanceDesc{
			"A": {Addr: "127.0.0.1", Timestamp: 22, Tokens: []uint32{math.MaxUint32 / 4, (math.MaxUint32 / 4) * 3}},
			"B": {Addr: "127.0.0.2", Timestamp: 11, Tokens: []uint32{(math.MaxUint32 / 4) * 2, math.MaxUint32}},
		},
	}
	ring.updateRingState(&ringDesc)

	err = testutil.GatherAndCompare(registry, bytes.NewBufferString(`
		# HELP ring_members Number of members in the ring
		# TYPE ring_members gauge
		ring_members{name="test",state="InstanceState_ACTIVE"} 2
		ring_members{name="test",state="InstanceState_JOINING"} 0
		ring_members{name="test",state="InstanceState_LEAVING"} 0
		ring_members{name="test",state="InstanceState_PENDING"} 0
		ring_members{name="test",state="Unhealthy"} 0
		# HELP ring_oldest_member_timestamp Timestamp of the oldest member in the ring.
		# TYPE ring_oldest_member_timestamp gauge
		ring_oldest_member_timestamp{name="test",state="InstanceState_ACTIVE"} 11
		ring_oldest_member_timestamp{name="test",state="InstanceState_JOINING"} 0
		ring_oldest_member_timestamp{name="test",state="InstanceState_LEAVING"} 0
		ring_oldest_member_timestamp{name="test",state="InstanceState_PENDING"} 0
		ring_oldest_member_timestamp{name="test",state="Unhealthy"} 0
		# HELP ring_tokens_total Number of tokens in the ring
		# TYPE ring_tokens_total gauge
		ring_tokens_total{name="test"} 4
	`))
	assert.NoError(t, err)
}

func TestUpdateMetricsWithRemoval(t *testing.T) {
	cfg := Config{
		KVStore:              kv.Config{},
		HeartbeatTimeout:     0, // get healthy stats
		ReplicationFactor:    3,
		ZoneAwarenessEnabled: true,
	}

	registry := prometheus.NewRegistry()

	// create the ring to set up metrics, but do not start
	ring, err := NewWithStoreClientAndStrategy(cfg, testRingName, testRingKey, nil, NewDefaultReplicationStrategy(), registry, log.NewNopLogger())
	require.NoError(t, err)

	ringDesc := Desc{
		Ingesters: map[string]InstanceDesc{
			"A": {Addr: "127.0.0.1", Timestamp: 22, Tokens: []uint32{math.MaxUint32 / 4, (math.MaxUint32 / 4) * 3}},
			"B": {Addr: "127.0.0.2", Timestamp: 11, Tokens: []uint32{(math.MaxUint32 / 4) * 2, math.MaxUint32}},
		},
	}
	ring.updateRingState(&ringDesc)

	err = testutil.GatherAndCompare(registry, bytes.NewBufferString(`
		# HELP ring_members Number of members in the ring
		# TYPE ring_members gauge
		ring_members{name="test",state="InstanceState_ACTIVE"} 2
		ring_members{name="test",state="InstanceState_JOINING"} 0
		ring_members{name="test",state="InstanceState_LEAVING"} 0
		ring_members{name="test",state="InstanceState_PENDING"} 0
		ring_members{name="test",state="Unhealthy"} 0
		# HELP ring_oldest_member_timestamp Timestamp of the oldest member in the ring.
		# TYPE ring_oldest_member_timestamp gauge
		ring_oldest_member_timestamp{name="test",state="InstanceState_ACTIVE"} 11
		ring_oldest_member_timestamp{name="test",state="InstanceState_JOINING"} 0
		ring_oldest_member_timestamp{name="test",state="InstanceState_LEAVING"} 0
		ring_oldest_member_timestamp{name="test",state="InstanceState_PENDING"} 0
		ring_oldest_member_timestamp{name="test",state="Unhealthy"} 0
		# HELP ring_tokens_total Number of tokens in the ring
		# TYPE ring_tokens_total gauge
		ring_tokens_total{name="test"} 4
	`))
	require.NoError(t, err)

	ringDescNew := Desc{
		Ingesters: map[string]InstanceDesc{
			"A": {Addr: "127.0.0.1", Timestamp: 22, Tokens: []uint32{math.MaxUint32 / 4, (math.MaxUint32 / 4) * 3}},
		},
	}
	ring.updateRingState(&ringDescNew)

	err = testutil.GatherAndCompare(registry, bytes.NewBufferString(`
		# HELP ring_members Number of members in the ring
		# TYPE ring_members gauge
		ring_members{name="test",state="InstanceState_ACTIVE"} 1
		ring_members{name="test",state="InstanceState_JOINING"} 0
		ring_members{name="test",state="InstanceState_LEAVING"} 0
		ring_members{name="test",state="InstanceState_PENDING"} 0
		ring_members{name="test",state="Unhealthy"} 0
		# HELP ring_oldest_member_timestamp Timestamp of the oldest member in the ring.
		# TYPE ring_oldest_member_timestamp gauge
		ring_oldest_member_timestamp{name="test",state="InstanceState_ACTIVE"} 22
		ring_oldest_member_timestamp{name="test",state="InstanceState_JOINING"} 0
		ring_oldest_member_timestamp{name="test",state="InstanceState_LEAVING"} 0
		ring_oldest_member_timestamp{name="test",state="InstanceState_PENDING"} 0
		ring_oldest_member_timestamp{name="test",state="Unhealthy"} 0
		# HELP ring_tokens_total Number of tokens in the ring
		# TYPE ring_tokens_total gauge
		ring_tokens_total{name="test"} 2
	`))
	assert.NoError(t, err)
}

func TestMergeTokenGroups(t *testing.T) {
	scenarios := map[string]struct {
		groups   map[string][]uint32
		expected []uint32
	}{
		"no groups": {
			groups:   map[string][]uint32{},
			expected: []uint32{},
		},
		"empty group": {
			groups: map[string][]uint32{
				"group-1": {},
			},
			expected: []uint32{},
		},
		"single group with one value": {
			groups: map[string][]uint32{
				"group-1": {1},
			},
			expected: []uint32{1},
		},
		"single group with many values": {
			groups: map[string][]uint32{
				"group-1": {1, 2, 3},
			},
			expected: []uint32{1, 2, 3},
		},
		"single group with repeated value": {
			groups: map[string][]uint32{
				"group-1": {1, 2, 2},
			},
			expected: []uint32{1, 2, 2},
		},
		"two groups with one value each": {
			groups: map[string][]uint32{
				"group-1": {1},
				"group-2": {2},
			},
			expected: []uint32{1, 2},
		},
		"two groups with many values": {
			groups: map[string][]uint32{
				"group-1": {1, 3, 4, 7},
				"group-2": {2, 5, 6},
			},
			expected: []uint32{1, 2, 3, 4, 5, 6, 7},
		},
		"two groups with common values": {
			groups: map[string][]uint32{
				"group-1": {1, 4, 6},
				"group-2": {2, 4, 5},
			},
			expected: []uint32{1, 2, 4, 4, 5, 6},
		},
		"many groups": {
			groups: map[string][]uint32{
				"group-1": {1, 4, 7},
				"group-2": {2, 5, 8},
				"group-3": {3, 6, 9},
			},
			expected: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			actual := mergeTokenGroups(scenario.groups)
			require.Equal(t, scenario.expected, actual)
		})
	}
}

func TestCountTokensSingleZone(t *testing.T) {
	tests := map[string]struct {
		ring     *Desc
		expected map[string]int64
	}{
		"empty ring": {
			ring:     &Desc{},
			expected: map[string]int64{},
		},
		"1 instance with 1 token in the ring": {
			ring: &Desc{
				Ingesters: map[string]InstanceDesc{
					"ingester-1": {Tokens: []uint32{1000000}},
				},
			},
			expected: map[string]int64{
				"ingester-1": math.MaxUint32 + 1,
			},
		},
		"1 instance with multiple tokens in the ring": {
			ring: &Desc{
				Ingesters: map[string]InstanceDesc{
					"ingester-1": {Tokens: []uint32{1000000, 2000000, 3000000}},
				},
			},
			expected: map[string]int64{
				"ingester-1": math.MaxUint32 + 1,
			},
		},
		"multiple instances with multiple tokens in the ring": {
			ring: &Desc{
				Ingesters: map[string]InstanceDesc{
					"ingester-1": {Tokens: []uint32{1000000, 3000000, 6000000}},
					"ingester-2": {Tokens: []uint32{2000000, 4000000, 8000000}},
					"ingester-3": {Tokens: []uint32{5000000, 9000000}},
				},
			},
			expected: map[string]int64{
				"ingester-1": 3000000 + (int64(math.MaxUint32) + 1 - 9000000),
				"ingester-2": 4000000,
				"ingester-3": 2000000,
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.ring.CountTokens())
		})
	}
}

func TestCountTokensMultiZones(t *testing.T) {
	tests := map[string]struct {
		ring     *Desc
		expected map[string]int64
	}{
		"empty ring": {
			ring:     &Desc{},
			expected: map[string]int64{},
		},
		"1 instance per zone with 1 token in the ring": {
			ring: &Desc{
				Ingesters: map[string]InstanceDesc{
					"ingester-zone-a-1": {Zone: "zone-a", Tokens: []uint32{1000000}},
					"ingester-zone-b-1": {Zone: "zone-b", Tokens: []uint32{1000001}},
					"ingester-zone-c-1": {Zone: "zone-c", Tokens: []uint32{1000002}},
				},
			},
			expected: map[string]int64{
				"ingester-zone-a-1": math.MaxUint32 + 1,
				"ingester-zone-b-1": math.MaxUint32 + 1,
				"ingester-zone-c-1": math.MaxUint32 + 1,
			},
		},
		"1 instance per zone with multiple tokens in the ring": {
			ring: &Desc{
				Ingesters: map[string]InstanceDesc{
					"ingester-zone-a-1": {Zone: "zone-a", Tokens: []uint32{1000000, 2000000, 3000000}},
					"ingester-zone-b-1": {Zone: "zone-b", Tokens: []uint32{1000001, 2000001, 3000001}},
					"ingester-zone-c-1": {Zone: "zone-c", Tokens: []uint32{1000002, 2000002, 3000002}},
				},
			},
			expected: map[string]int64{
				"ingester-zone-a-1": int64(math.MaxUint32) + 1,
				"ingester-zone-b-1": int64(math.MaxUint32) + 1,
				"ingester-zone-c-1": int64(math.MaxUint32) + 1,
			},
		},
		"multiple instances in multiple zones with multiple tokens in the ring": {
			ring: &Desc{
				Ingesters: map[string]InstanceDesc{
					"ingester-zone-a-1": {Zone: "zone-a", Tokens: []uint32{1000000, 3000000, 6000000}},
					"ingester-zone-a-2": {Zone: "zone-a", Tokens: []uint32{2000000, 4000000, 8000000}},
					"ingester-zone-a-3": {Zone: "zone-a", Tokens: []uint32{5000000, 9000000}},
					"ingester-zone-b-1": {Zone: "zone-b", Tokens: []uint32{1000001, 3000001, 6000001}},
					"ingester-zone-b-2": {Zone: "zone-b", Tokens: []uint32{2000001, 4000001, 8000001}},
					"ingester-zone-b-3": {Zone: "zone-b", Tokens: []uint32{5000001, 9000001}},
				},
			},
			expected: map[string]int64{
				"ingester-zone-a-1": 3000000 + (int64(math.MaxUint32) + 1 - 9000000),
				"ingester-zone-a-2": 4000000,
				"ingester-zone-a-3": 2000000,
				"ingester-zone-b-1": 3000000 + (int64(math.MaxUint32) + 1 - 9000000),
				"ingester-zone-b-2": 4000000,
				"ingester-zone-b-3": 2000000,
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.ring.CountTokens())
		})
	}
}

// To make tests reproducible we sort the instance IDs in the map, and then get a random index via rnd.
func getRandomInstanceID(instances map[string]InstanceDesc, rnd *rand.Rand) string {
	instanceIDs := make([]string, 0, len(instances))
	for id := range instances {
		instanceIDs = append(instanceIDs, id)
	}
	sort.Strings(instanceIDs)
	return instanceIDs[rnd.Intn(len(instanceIDs))]
}

type mockError struct {
	isClientErr bool
	message     string
}

func (e mockError) isClientError() bool {
	return e.isClientErr
}

func (e mockError) Error() string {
	return e.message
}
