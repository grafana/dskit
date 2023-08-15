package ring

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/goleak"

	"github.com/grafana/dskit/internal/slices"
	"github.com/grafana/dskit/spanlogger"
)

func TestReplicationSet_GetAddresses(t *testing.T) {
	tests := map[string]struct {
		rs       ReplicationSet
		expected []string
	}{
		"should return an empty slice on empty replication set": {
			rs:       ReplicationSet{},
			expected: []string{},
		},
		"should return instances addresses (no order guaranteed)": {
			rs: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "127.0.0.1"},
					{Addr: "127.0.0.2"},
					{Addr: "127.0.0.3"},
				},
			},
			expected: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.ElementsMatch(t, testData.expected, testData.rs.GetAddresses())
		})
	}
}

func TestReplicationSet_GetAddressesWithout(t *testing.T) {
	tests := map[string]struct {
		rs       ReplicationSet
		expected []string
		exclude  string
	}{
		"should return an empty slice on empty replication set": {
			rs:       ReplicationSet{},
			expected: []string{},
			exclude:  "127.0.0.1",
		},
		"non-matching exclusion, should return all addresses": {
			rs: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "127.0.0.1"},
					{Addr: "127.0.0.2"},
					{Addr: "127.0.0.3"},
				},
			},
			expected: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
			exclude:  "127.0.0.4",
		},
		"matching exclusion, should return non-excluded addresses": {
			rs: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "127.0.0.1"},
					{Addr: "127.0.0.2"},
					{Addr: "127.0.0.3"},
				},
			},
			expected: []string{"127.0.0.1", "127.0.0.3"},
			exclude:  "127.0.0.2",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.ElementsMatch(t, testData.expected, testData.rs.GetAddressesWithout(testData.exclude))
		})
	}
}

var (
	errFailure     = errors.New("failed")
	errZoneFailure = errors.New("zone failed")
)

// Return a function that fails starting from failAfter times
func failingFunctionAfter(failAfter int32, delay time.Duration) func(context.Context, *InstanceDesc) (interface{}, error) {
	count := atomic.NewInt32(0)
	return func(context.Context, *InstanceDesc) (interface{}, error) {
		time.Sleep(delay)
		if count.Inc() > failAfter {
			return nil, errFailure
		}
		return 1, nil
	}
}

func failingFunctionOnZones(zones ...string) func(context.Context, *InstanceDesc) (interface{}, error) {
	return func(ctx context.Context, ing *InstanceDesc) (interface{}, error) {
		for _, zone := range zones {
			if ing.Zone == zone {
				return nil, errZoneFailure
			}
		}
		return 1, nil
	}
}

func TestReplicationSet_Do(t *testing.T) {
	tests := []struct {
		name                string
		instances           []InstanceDesc
		maxErrors           int
		maxUnavailableZones int
		f                   func(context.Context, *InstanceDesc) (interface{}, error)
		delay               time.Duration
		cancelContextDelay  time.Duration
		want                []interface{}
		expectedError       error
	}{
		{
			name: "max errors = 0, no errors no delay",
			instances: []InstanceDesc{
				{},
			},
			f: func(c context.Context, id *InstanceDesc) (interface{}, error) {
				return 1, nil
			},
			want: []interface{}{1},
		},
		{
			name:      "max errors = 0, should fail on 1 error out of 1 instance",
			instances: []InstanceDesc{{}},
			f: func(c context.Context, id *InstanceDesc) (interface{}, error) {
				return nil, errFailure
			},
			want:          nil,
			expectedError: errFailure,
		},
		{
			name:          "max errors = 0, should fail on 1 error out of 3 instances (last call fails)",
			instances:     []InstanceDesc{{}, {}, {}},
			f:             failingFunctionAfter(2, 10*time.Millisecond),
			want:          nil,
			expectedError: errFailure,
		},
		{
			name:          "max errors = 1, should fail on 3 errors out of 5 instances (last calls fail)",
			instances:     []InstanceDesc{{}, {}, {}, {}, {}},
			maxErrors:     1,
			f:             failingFunctionAfter(2, 10*time.Millisecond),
			delay:         100 * time.Millisecond,
			want:          nil,
			expectedError: errFailure,
		},
		{
			name:      "max errors = 1, should handle context canceled",
			instances: []InstanceDesc{{}, {}, {}},
			maxErrors: 1,
			f: func(c context.Context, id *InstanceDesc) (interface{}, error) {
				time.Sleep(300 * time.Millisecond)
				return 1, nil
			},
			cancelContextDelay: 100 * time.Millisecond,
			want:               nil,
			expectedError:      context.Canceled,
		},
		{
			name:      "max errors = 0, should succeed on all successful instances",
			instances: []InstanceDesc{{Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone3"}},
			f: func(c context.Context, id *InstanceDesc) (interface{}, error) {
				return 1, nil
			},
			want: []interface{}{1, 1, 1},
		},
		{
			name:                "max unavailable zones = 1, should succeed on instances failing in 1 out of 3 zones (3 instances)",
			instances:           []InstanceDesc{{Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone3"}},
			f:                   failingFunctionOnZones("zone1"),
			maxUnavailableZones: 1,
			want:                []interface{}{1, 1},
		},
		{
			name:                "max unavailable zones = 1, should fail on instances failing in 2 out of 3 zones (3 instances)",
			instances:           []InstanceDesc{{Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone3"}},
			f:                   failingFunctionOnZones("zone1", "zone2"),
			maxUnavailableZones: 1,
			expectedError:       errZoneFailure,
		},
		{
			name:                "max unavailable zones = 1, should succeed on instances failing in 1 out of 3 zones (6 instances)",
			instances:           []InstanceDesc{{Zone: "zone1"}, {Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone2"}, {Zone: "zone3"}, {Zone: "zone3"}},
			f:                   failingFunctionOnZones("zone1"),
			maxUnavailableZones: 1,
			want:                []interface{}{1, 1, 1, 1},
		},
		{
			name:                "max unavailable zones = 2, should fail on instances failing in 3 out of 5 zones (5 instances)",
			instances:           []InstanceDesc{{Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone3"}, {Zone: "zone4"}, {Zone: "zone5"}},
			f:                   failingFunctionOnZones("zone1", "zone2", "zone3"),
			maxUnavailableZones: 2,
			expectedError:       errZoneFailure,
		},
		{
			name:                "max unavailable zones = 2, should succeed on instances failing in 2 out of 5 zones (10 instances)",
			instances:           []InstanceDesc{{Zone: "zone1"}, {Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone2"}, {Zone: "zone3"}, {Zone: "zone3"}, {Zone: "zone4"}, {Zone: "zone4"}, {Zone: "zone5"}, {Zone: "zone5"}},
			f:                   failingFunctionOnZones("zone1", "zone5"),
			maxUnavailableZones: 2,
			want:                []interface{}{1, 1, 1, 1, 1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Ensure the test case has been correctly setup (max errors and max unavailable zones are
			// mutually exclusive).
			require.False(t, tt.maxErrors > 0 && tt.maxUnavailableZones > 0)

			r := ReplicationSet{
				Instances:           tt.instances,
				MaxErrors:           tt.maxErrors,
				MaxUnavailableZones: tt.maxUnavailableZones,
			}
			ctx := context.Background()
			if tt.cancelContextDelay > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				time.AfterFunc(tt.cancelContextDelay, func() {
					cancel()
				})
			}
			got, err := r.Do(ctx, tt.delay, tt.f)
			if tt.expectedError != nil {
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDoUntilQuorumWithoutSuccessfulContextCancellation(t *testing.T) {
	successfulF := func(ctx context.Context, desc *InstanceDesc) (string, error) {
		return desc.Addr, nil
	}

	failingF := func(ctx context.Context, desc *InstanceDesc) (string, error) {
		return "", fmt.Errorf("this is the error for %v", desc.Addr)
	}

	failingZoneB := func(ctx context.Context, desc *InstanceDesc) (string, error) {
		if desc.Zone == "zone-b" {
			return "", fmt.Errorf("this is the error for %v", desc.Addr)
		}

		return desc.Addr, nil
	}

	testCases := map[string]struct {
		replicationSet  ReplicationSet
		f               func(context.Context, *InstanceDesc) (string, error)
		expectedResults []string
		expectedError   error
		maxCleanupCalls int
	}{
		"no replicas, max errors = 0, max unavailable zones = 0": {
			replicationSet:  ReplicationSet{},
			f:               successfulF,
			expectedResults: []string{},
			expectedError:   nil,
			maxCleanupCalls: 0,
		},
		"no replicas, max errors = 1": {
			replicationSet: ReplicationSet{
				MaxErrors: 1,
			},
			f:               successfulF,
			expectedResults: []string{},
			expectedError:   nil,
			maxCleanupCalls: 0,
		},
		"no replicas, max unavailable zones = 1": {
			replicationSet: ReplicationSet{
				MaxUnavailableZones: 1,
			},
			f:               successfulF,
			expectedResults: []string{},
			expectedError:   nil,
			maxCleanupCalls: 0,
		},
		"one replica, max errors = 0, max unavailable zones = 0, call succeeds": {
			replicationSet: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "replica-1", Zone: "zone-1"},
				},
			},
			f:               successfulF,
			expectedResults: []string{"replica-1"},
			expectedError:   nil,
			maxCleanupCalls: 0,
		},
		"one replica, max errors = 0, max unavailable zones = 0, call fails": {
			replicationSet: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "replica-1", Zone: "zone-1"},
				},
			},
			f:               failingF,
			expectedResults: nil,
			expectedError:   errors.New("this is the error for replica-1"),
			maxCleanupCalls: 0,
		},
		"one replica, max errors = 1, call succeeds": {
			replicationSet: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "replica-1", Zone: "zone-1"},
				},
				MaxErrors: 1,
			},
			f:               successfulF,
			expectedResults: []string{}, // We don't need any results.
			expectedError:   nil,
			maxCleanupCalls: 1,
		},
		"one replica, max errors = 1, call fails": {
			replicationSet: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "replica-1", Zone: "zone-1"},
				},
				MaxErrors: 1,
			},
			f:               failingF,
			expectedResults: []string{}, // We don't need any results.
			expectedError:   nil,
			maxCleanupCalls: 0,
		},
		"one replica, max unavailable zones = 1, call succeeds": {
			replicationSet: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "replica-1", Zone: "zone-1"},
				},
				MaxUnavailableZones: 1,
			},
			f:               successfulF,
			expectedResults: []string{}, // We don't need any results.
			expectedError:   nil,
			maxCleanupCalls: 1,
		},
		"one replica, max unavailable zones = 1, call fails": {
			replicationSet: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "replica-1", Zone: "zone-1"},
				},
				MaxUnavailableZones: 1,
			},
			f:               failingF,
			expectedResults: []string{}, // We don't need any results.
			expectedError:   nil,
			maxCleanupCalls: 0,
		},
		"total zone failure with many replicas, max unavailable zones = 1": {
			replicationSet: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "zone-a-replica-1", Zone: "zone-a"},
					{Addr: "zone-a-replica-2", Zone: "zone-a"},
					{Addr: "zone-b-replica-1", Zone: "zone-b"},
					{Addr: "zone-b-replica-2", Zone: "zone-b"},
					{Addr: "zone-c-replica-1", Zone: "zone-c"},
					{Addr: "zone-c-replica-2", Zone: "zone-c"},
				},
				MaxUnavailableZones: 1,
			},
			f:               failingZoneB,
			expectedResults: []string{"zone-a-replica-1", "zone-a-replica-2", "zone-c-replica-1", "zone-c-replica-2"},
			expectedError:   nil,
			maxCleanupCalls: 0,
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			for _, minimizeRequests := range []bool{true, false} {
				t.Run(fmt.Sprintf("minimize requests: %v", minimizeRequests), func(t *testing.T) {
					defer goleak.VerifyNone(t)

					logger := &testLogger{}
					spanLogger, ctx := spanlogger.New(context.Background(), logger, "DoUntilQuorum test", dummyTenantResolver{})
					cleanupTracker := newCleanupTracker(t, testCase.maxCleanupCalls)
					mtx := sync.RWMutex{}
					successfulInstances := []*InstanceDesc{}

					wrappedF := func(ctx context.Context, desc *InstanceDesc, cancel context.CancelFunc) (string, error) {
						cleanupTracker.trackCall(ctx, desc, cancel)
						res, err := testCase.f(ctx, desc)

						if err == nil {
							mtx.Lock()
							defer mtx.Unlock()
							successfulInstances = append(successfulInstances, desc)
						}

						return res, err
					}

					cfg := DoUntilQuorumConfig{MinimizeRequests: minimizeRequests, Logger: spanLogger}
					actualResults, actualError := DoUntilQuorumWithoutSuccessfulContextCancellation(ctx, testCase.replicationSet, cfg, wrappedF, cleanupTracker.cleanup)
					require.ElementsMatch(t, testCase.expectedResults, actualResults)
					require.Equal(t, testCase.expectedError, actualError)

					// The list of instances expected to be cleaned up is not deterministic, even with minimizeRequests=false: there's a
					// chance we'll reach quorum before DoUntilQuorumWithoutSuccessfulContextCancellation has a chance to call f for each instance.
					var expectedCleanup []string

					mtx.Lock()
					defer mtx.Unlock()

					for _, i := range successfulInstances {
						if !slices.Contains(testCase.expectedResults, i.Addr) {
							expectedCleanup = append(expectedCleanup, i.Addr)
						}
					}

					cleanupTracker.collectCleanedUpInstancesWithExpectedCount(len(expectedCleanup))
					cleanupTracker.assertCorrectCleanup(testCase.expectedResults, expectedCleanup)

					if testCase.expectedError == nil {
						require.Contains(t, logger.messages, map[interface{}]interface{}{
							"level": level.DebugValue(),
							"msg":   "quorum reached",
						})
					} else {
						require.Contains(t, logger.messages, map[interface{}]interface{}{
							"level": level.ErrorValue(),
							"msg":   "cancelling all requests because quorum cannot be reached",
						})
					}
				})
			}
		})
	}
}

func TestDoUntilQuorumWithoutSuccessfulContextCancellation_MultipleUnavailableZones(t *testing.T) {
	replicationSet := ReplicationSet{
		Instances: []InstanceDesc{
			{Addr: "zone-a-replica-1", Zone: "zone-a"},
			{Addr: "zone-a-replica-2", Zone: "zone-a"},
			{Addr: "zone-b-replica-1", Zone: "zone-b"},
			{Addr: "zone-b-replica-2", Zone: "zone-b"},
			{Addr: "zone-c-replica-1", Zone: "zone-c"},
			{Addr: "zone-c-replica-2", Zone: "zone-c"},
		},
		MaxUnavailableZones: 1,
	}

	for _, minimizeRequests := range []bool{true, false} {
		t.Run(fmt.Sprintf("minimize requests: %v", minimizeRequests), func(t *testing.T) {
			defer goleak.VerifyNone(t)

			logger := &testLogger{}
			spanLogger, ctx := spanlogger.New(context.Background(), logger, "DoUntilQuorum test", dummyTenantResolver{})
			cleanupTracker := newCleanupTracker(t, 3)
			mtx := sync.RWMutex{}
			expectedCleanup := []string{}
			zonesCalled := []string{}

			wrappedF := func(ctx context.Context, desc *InstanceDesc, cancel context.CancelFunc) (string, error) {
				cleanupTracker.trackCall(ctx, desc, cancel)

				mtx.Lock()
				defer mtx.Unlock()

				zonesCalled = append(zonesCalled, desc.Zone)

				if strings.HasSuffix(desc.Addr, "replica-1") {
					return "", errors.New("error from a replica-1 instance")
				}

				expectedCleanup = append(expectedCleanup, desc.Addr)

				return desc.Addr, nil
			}

			cfg := DoUntilQuorumConfig{MinimizeRequests: minimizeRequests, Logger: spanLogger}
			actualResults, actualError := DoUntilQuorumWithoutSuccessfulContextCancellation(ctx, replicationSet, cfg, wrappedF, cleanupTracker.cleanup)
			require.Empty(t, actualResults)
			require.EqualError(t, actualError, "error from a replica-1 instance")

			mtx.RLock()
			defer mtx.RUnlock()
			cleanupTracker.collectCleanedUpInstancesWithExpectedCount(len(expectedCleanup))
			cleanupTracker.assertCorrectCleanup([]string{}, expectedCleanup)

			require.Contains(t, logger.messages, map[interface{}]interface{}{
				"level": level.ErrorValue(),
				"msg":   "cancelling all requests because quorum cannot be reached",
			})
		})
	}
}

func TestDoUntilQuorumWithoutSuccessfulContextCancellation_PartialZoneFailure(t *testing.T) {
	replicationSet := ReplicationSet{
		Instances: []InstanceDesc{
			{Addr: "zone-a-replica-1", Zone: "zone-a"},
			{Addr: "zone-a-replica-2", Zone: "zone-a"},
			{Addr: "zone-b-replica-1", Zone: "zone-b"},
			{Addr: "zone-b-replica-2", Zone: "zone-b"},
			{Addr: "zone-c-replica-1", Zone: "zone-c"},
			{Addr: "zone-c-replica-2", Zone: "zone-c"},
		},
		MaxUnavailableZones: 1,
	}

	expectedResults := []string{"zone-a-replica-1", "zone-a-replica-2", "zone-c-replica-1", "zone-c-replica-2"}

	for _, minimizeRequests := range []bool{true, false} {
		t.Run(fmt.Sprintf("minimize requests: %v", minimizeRequests), func(t *testing.T) {
			defer goleak.VerifyNone(t)

			ctx := context.Background()
			cleanupTracker := newCleanupTracker(t, 1)
			zoneBReplica1CleanupRequired := atomic.NewBool(false)

			f := func(ctx context.Context, desc *InstanceDesc, cancel context.CancelFunc) (string, error) {
				cleanupTracker.trackCall(ctx, desc, cancel)

				if desc.Addr == "zone-b-replica-1" {
					zoneBReplica1CleanupRequired.Store(true)
				}

				if desc.Addr == "zone-b-replica-2" {
					return "", fmt.Errorf("this is the error for %v", desc.Addr)
				}

				return desc.Addr, nil
			}

			cfg := DoUntilQuorumConfig{MinimizeRequests: minimizeRequests}
			actualResults, err := DoUntilQuorumWithoutSuccessfulContextCancellation(ctx, replicationSet, cfg, f, cleanupTracker.cleanup)
			require.ElementsMatch(t, expectedResults, actualResults)
			require.NoError(t, err)

			if zoneBReplica1CleanupRequired.Load() {
				cleanupTracker.collectCleanedUpInstances()
				cleanupTracker.assertCorrectCleanup(expectedResults, []string{"zone-b-replica-1"})
			} else {
				cleanupTracker.collectCleanedUpInstancesWithExpectedCount(0)
				cleanupTracker.assertCorrectCleanup(expectedResults, []string{})
			}
		})
	}
}

func TestDoUntilQuorumWithoutSuccessfulContextCancellation_CancelsEntireZoneImmediatelyOnSingleFailure(t *testing.T) {
	defer goleak.VerifyNone(t)

	replicationSet := ReplicationSet{
		Instances: []InstanceDesc{
			{Addr: "zone-a-replica-1", Zone: "zone-a"},
			{Addr: "zone-a-replica-2", Zone: "zone-a"},
			{Addr: "zone-b-replica-1", Zone: "zone-b"},
			{Addr: "zone-b-replica-2", Zone: "zone-b"},
			{Addr: "zone-c-replica-1", Zone: "zone-c"},
			{Addr: "zone-c-replica-2", Zone: "zone-c"},
		},
		MaxUnavailableZones: 1,
	}

	ctx := context.Background()
	cleanupTracker := newCleanupTracker(t, 1)
	failingZone := ""
	failingZoneSawCancelledContext := false
	waitForFailingZoneToSeeCancelledContext := make(chan struct{})
	mtx := sync.RWMutex{}

	f := func(ctx context.Context, instance *InstanceDesc, cancel context.CancelFunc) (string, error) {
		cleanupTracker.trackCall(ctx, instance, cancel)

		mtx.Lock()

		if failingZone == "" {
			failingZone = instance.Zone
		}

		mtx.Unlock()

		// If this instance is in the failing zone:
		// - if it's replica-1, return an error to trigger the cancellation of the zone
		// - if it's replica-2, wait for this instance's context to be cancelled before returning
		if instance.Addr == failingZone+"-replica-1" {
			if strings.HasSuffix(instance.Addr, "-replica-1") {
				return "", errors.New("this is the failing instance")
			}
		} else if instance.Addr == failingZone+"-replica-2" {
			select {
			case <-ctx.Done():
				close(waitForFailingZoneToSeeCancelledContext)
				failingZoneSawCancelledContext = true
			case <-time.After(time.Second):
				close(waitForFailingZoneToSeeCancelledContext)
				require.FailNow(t, "other instance in failing zone gave up waiting for its context to be cancelled")
			}
		} else {
			// If this instance is not in the failing zone, wait until the failing zone's context is cancelled before returning to avoid races.
			select {
			case <-waitForFailingZoneToSeeCancelledContext:
				// Nothing more to do.
			case <-time.After(2 * time.Second):
				require.FailNowf(t, "%s gave up waiting for instance in failing zone to report its context had been cancelled", instance.Addr)
			}
		}

		return instance.Addr, nil
	}

	cfg := DoUntilQuorumConfig{MinimizeRequests: true}
	actualResults, err := DoUntilQuorumWithoutSuccessfulContextCancellation(ctx, replicationSet, cfg, f, cleanupTracker.cleanup)
	require.NoError(t, err)

	require.True(t, failingZoneSawCancelledContext)

	cleanupTracker.collectCleanedUpInstances()
	cleanupTracker.assertCorrectCleanup(actualResults, []string{failingZone + "-replica-2"})
}

func TestDoUntilQuorumWithoutSuccessfulContextCancellation_RunsCallsInParallel(t *testing.T) {
	for _, minimizeRequests := range []bool{true, false} {
		t.Run(fmt.Sprintf("minimize requests: %v", minimizeRequests), func(t *testing.T) {
			defer goleak.VerifyNone(t)

			ctx := context.Background()
			replicationSet := ReplicationSet{
				Instances: []InstanceDesc{
					{
						Addr: "replica-1",
					},
					{
						Addr: "replica-2",
					},
				},
			}

			wg := sync.WaitGroup{}
			wg.Add(len(replicationSet.Instances))

			f := func(ctx context.Context, desc *InstanceDesc, _ context.CancelFunc) (string, error) {
				wg.Done()

				// Wait for the other calls to f to start. If this test hangs here, then the calls are not running in parallel.
				wg.Wait()

				return desc.Addr, nil
			}

			cfg := DoUntilQuorumConfig{MinimizeRequests: minimizeRequests}
			cleanupFunc := func(_ string) {}
			results, err := DoUntilQuorumWithoutSuccessfulContextCancellation(ctx, replicationSet, cfg, f, cleanupFunc)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"replica-1", "replica-2"}, results)
		})
	}
}

func TestDoUntilQuorumWithoutSuccessfulContextCancellation_ReturnsMinimumResultSetForZoneAwareWhenAllSucceed(t *testing.T) {
	instances := []InstanceDesc{
		{Addr: "zone-a-replica-1", Zone: "zone-a"},
		{Addr: "zone-a-replica-2", Zone: "zone-a"},
		{Addr: "zone-b-replica-1", Zone: "zone-b"},
		{Addr: "zone-b-replica-2", Zone: "zone-b"},
		{Addr: "zone-c-replica-1", Zone: "zone-c"},
		{Addr: "zone-c-replica-2", Zone: "zone-c"},
	}

	for _, minimizeRequests := range []bool{true, false} {
		t.Run(fmt.Sprintf("minimize requests: %v", minimizeRequests), func(t *testing.T) {
			defer goleak.VerifyNone(t)

			ctx := context.Background()
			replicationSet := ReplicationSet{
				Instances:           instances,
				MaxUnavailableZones: 1,
			}

			cleanupTracker := newCleanupTracker(t, 2)
			mtx := sync.RWMutex{}
			instancesCalled := []*InstanceDesc{}

			f := func(ctx context.Context, desc *InstanceDesc, cancel context.CancelFunc) (string, error) {
				cleanupTracker.trackCall(ctx, desc, cancel)

				mtx.Lock()
				defer mtx.Unlock()
				instancesCalled = append(instancesCalled, desc)

				return desc.Addr, nil
			}

			cfg := DoUntilQuorumConfig{MinimizeRequests: minimizeRequests}
			results, err := DoUntilQuorumWithoutSuccessfulContextCancellation(ctx, replicationSet, cfg, f, cleanupTracker.cleanup)
			require.NoError(t, err)
			require.Len(t, results, 4)

			zoneAReturned := slices.Contains(results, "zone-a-replica-1") && slices.Contains(results, "zone-a-replica-2")
			zoneBReturned := slices.Contains(results, "zone-b-replica-1") && slices.Contains(results, "zone-b-replica-2")
			zoneCReturned := slices.Contains(results, "zone-c-replica-1") && slices.Contains(results, "zone-c-replica-2")
			zonesReturned := 0
			var expectedResults []string

			if zoneAReturned {
				zonesReturned++
				expectedResults = append(expectedResults, "zone-a-replica-1", "zone-a-replica-2")
			}

			if zoneBReturned {
				zonesReturned++
				expectedResults = append(expectedResults, "zone-b-replica-1", "zone-b-replica-2")
			}

			if zoneCReturned {
				zonesReturned++
				expectedResults = append(expectedResults, "zone-c-replica-1", "zone-c-replica-2")
			}

			require.Equalf(t, 2, zonesReturned, "received results from %v, expected results from only two zones", results)

			mtx.Lock()
			defer mtx.Unlock()

			if minimizeRequests {
				zonesCalled := uniqueZoneCount(instancesCalled)
				require.Equal(t, 2, zonesCalled, "expected function to only be called for two zones")
			}

			// The list of instances expected to be cleaned up is not deterministic, even with minimizeRequests=false: there's a
			// chance we'll reach quorum before DoUntilQuorumWithoutSuccessfulContextCancellation has a chance to call f for each instance in the unused zone.
			var expectedCleanup []string

			for _, i := range instancesCalled {
				if !slices.Contains(expectedResults, i.Addr) {
					expectedCleanup = append(expectedCleanup, i.Addr)
				}
			}

			cleanupTracker.collectCleanedUpInstancesWithExpectedCount(len(expectedCleanup))
			cleanupTracker.assertCorrectCleanup(expectedResults, expectedCleanup)
		})
	}
}

func TestDoUntilQuorumWithoutSuccessfulContextCancellation_ReturnsMinimumResultSetForNonZoneAwareWhenAllSucceed(t *testing.T) {
	defer goleak.VerifyNone(t)

	instances := []InstanceDesc{
		{Addr: "zone-a-replica-1", Zone: "zone-a"},
		{Addr: "zone-a-replica-2", Zone: "zone-a"},
		{Addr: "zone-b-replica-1", Zone: "zone-b"},
		{Addr: "zone-b-replica-2", Zone: "zone-b"},
		{Addr: "zone-c-replica-1", Zone: "zone-c"},
		{Addr: "zone-c-replica-2", Zone: "zone-c"},
	}

	for _, minimizeRequests := range []bool{true, false} {
		t.Run(fmt.Sprintf("minimize requests: %v", minimizeRequests), func(t *testing.T) {
			defer goleak.VerifyNone(t)

			ctx := context.Background()
			replicationSet := ReplicationSet{
				Instances: instances,
				MaxErrors: 1,
			}

			expectedCleanupCount := 0

			if !minimizeRequests {
				expectedCleanupCount = 1
			}

			cleanupTracker := newCleanupTracker(t, expectedCleanupCount)
			mtx := sync.RWMutex{}
			instancesCalled := []string{}

			f := func(ctx context.Context, desc *InstanceDesc, cancel context.CancelFunc) (string, error) {
				cleanupTracker.trackCall(ctx, desc, cancel)
				mtx.Lock()
				defer mtx.Unlock()
				instancesCalled = append(instancesCalled, desc.Addr)
				return desc.Addr, nil
			}

			cfg := DoUntilQuorumConfig{MinimizeRequests: minimizeRequests}
			results, err := DoUntilQuorumWithoutSuccessfulContextCancellation(ctx, replicationSet, cfg, f, cleanupTracker.cleanup)
			require.NoError(t, err)
			require.Len(t, results, 5, "should only have results from instances required to meet quorum requirement")

			mtx.RLock()
			defer mtx.RUnlock()

			var expectedCleanup []string

			if minimizeRequests {
				require.Len(t, instancesCalled, 5, "should only call function for instances required to meet quorum requirement")
			} else {
				// The list of instances expected to be cleaned up is not deterministic, even with minimizeRequests=false: there's a
				// chance we'll reach quorum before DoUntilQuorumWithoutSuccessfulContextCancellation has a chance to call f for the unused instance.
				for _, i := range instancesCalled {
					if !slices.Contains(results, i) {
						expectedCleanup = append(expectedCleanup, i)
					}
				}

				cleanupTracker.collectCleanedUpInstancesWithExpectedCount(len(expectedCleanup))
			}

			cleanupTracker.assertCorrectCleanup(results, expectedCleanup)
		})
	}
}

func TestDoUntilQuorumWithoutSuccessfulContextCancellation_DoesNotWaitForUnnecessarySlowResponses(t *testing.T) {
	testCases := map[string]struct {
		replicationSet  ReplicationSet
		expectedResults []string
		maxCleanupCalls int
	}{
		"not zone aware": {
			replicationSet: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "instance-1"},
					{Addr: "instance-2-slow"},
					{Addr: "instance-3"},
				},
				MaxErrors: 1,
			},
			expectedResults: []string{"instance-1", "instance-3"},
			maxCleanupCalls: 1,
		},
		"zone aware": {
			replicationSet: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "zone-a-instance-1", Zone: "zone-a"},
					{Addr: "zone-a-instance-2-slow", Zone: "zone-a"},
					{Addr: "zone-b-instance-1", Zone: "zone-b"},
					{Addr: "zone-b-instance-2", Zone: "zone-b"},
					{Addr: "zone-c-instance-1", Zone: "zone-c"},
					{Addr: "zone-c-instance-2", Zone: "zone-c"},
				},
				MaxUnavailableZones: 1,
			},
			expectedResults: []string{"zone-b-instance-1", "zone-b-instance-2", "zone-c-instance-1", "zone-c-instance-2"},
			maxCleanupCalls: 2,
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			defer goleak.VerifyNone(t)

			ctx := context.Background()
			waitChan := make(chan struct{})
			cleanupTracker := newCleanupTracker(t, testCase.maxCleanupCalls)
			mtx := sync.RWMutex{}
			instancesCalled := []string{}

			f := func(ctx context.Context, desc *InstanceDesc, cancel context.CancelFunc) (string, error) {
				cleanupTracker.trackCall(ctx, desc, cancel)

				mtx.Lock()
				instancesCalled = append(instancesCalled, desc.Addr)
				mtx.Unlock()

				if strings.HasSuffix(desc.Addr, "-slow") {
					select {
					case <-waitChan:
						// Nothing more to do.
					case <-time.After(time.Second):
						require.FailNow(t, "DoUntilQuorumWithoutSuccessfulContextCancellation waited for unnecessary slow response")
					}
				}

				return desc.Addr, nil
			}

			cfg := DoUntilQuorumConfig{MinimizeRequests: false}
			actualResults, err := DoUntilQuorumWithoutSuccessfulContextCancellation(ctx, testCase.replicationSet, cfg, f, cleanupTracker.cleanup)
			require.ElementsMatch(t, testCase.expectedResults, actualResults)
			require.NoError(t, err)

			close(waitChan)

			// The list of instances expected to be cleaned up is not deterministic, even with minimizeRequests=false: there's a
			// chance we'll reach quorum before DoUntilQuorumWithoutSuccessfulContextCancellation has a chance to call f for the unused instance.
			mtx.RLock()
			defer mtx.RUnlock()

			var expectedCleanup []string

			for _, i := range instancesCalled {
				if !slices.Contains(actualResults, i) {
					expectedCleanup = append(expectedCleanup, i)
				}
			}

			cleanupTracker.collectCleanedUpInstancesWithExpectedCount(len(expectedCleanup))
			cleanupTracker.assertCorrectCleanup(testCase.expectedResults, expectedCleanup)
		})
	}
}

func TestDoUntilQuorumWithoutSuccessfulContextCancellation_ParentContextHandling_WithoutMinimizeRequests(t *testing.T) {
	defer goleak.VerifyNone(t)

	parentCtx, parentCancel := context.WithCancel(context.WithValue(context.Background(), testContextKey, "this-is-the-value-from-the-parent"))
	logger := &testLogger{}
	spanLogger, ctx := spanlogger.New(parentCtx, logger, "DoUntilQuorum test", dummyTenantResolver{})

	replicationSet := ReplicationSet{
		Instances: []InstanceDesc{
			{Addr: "instance-1"},
			{Addr: "instance-2"},
			{Addr: "instance-3"},
		},
	}

	cleanupTracker := newCleanupTracker(t, 3)

	f := func(ctx context.Context, desc *InstanceDesc, cancel context.CancelFunc) (string, error) {
		cleanupTracker.trackCall(ctx, desc, cancel)

		require.Equal(t, "this-is-the-value-from-the-parent", ctx.Value(testContextKey), "expected instance context to inherit from context passed to DoUntilQuorumWithoutSuccessfulContextCancellation")

		if desc.Addr == "instance-1" {
			go func() {
				time.Sleep(100 * time.Millisecond)
				parentCancel()
			}()
		} else {
			select {
			case <-ctx.Done():
				// Nothing more to do.
			case <-time.After(time.Second):
				require.FailNow(t, "expected instance context to be cancelled, but timed out waiting for cancellation")
			}
		}

		return desc.Addr, nil
	}

	cfg := DoUntilQuorumConfig{
		MinimizeRequests: false,
		Logger:           spanLogger,
	}

	results, err := DoUntilQuorumWithoutSuccessfulContextCancellation(ctx, replicationSet, cfg, f, cleanupTracker.cleanup)
	require.Empty(t, results)
	require.Equal(t, context.Canceled, err)

	cleanupTracker.collectCleanedUpInstances()
	cleanupTracker.assertCorrectCleanup(nil, []string{"instance-1", "instance-2", "instance-3"})

	expectedLogMessage := map[interface{}]interface{}{
		"level": level.DebugValue(),
		"msg":   "parent context done, returning",
		"err":   context.Canceled,
	}

	require.Contains(t, logger.messages, expectedLogMessage)
}

func TestDoUntilQuorumWithoutSuccessfulContextCancellation_ParentContextHandling_WithMinimizeRequests(t *testing.T) {
	testCases := map[string]ReplicationSet{
		"with zone awareness": {
			Instances: []InstanceDesc{
				{Addr: "instance-1", Zone: "zone-a"},
				{Addr: "instance-2", Zone: "zone-b"},
				{Addr: "instance-3", Zone: "zone-c"},
			},
			MaxUnavailableZones: 1,
		},
		"without zone awareness": {
			Instances: []InstanceDesc{
				{Addr: "instance-1"},
				{Addr: "instance-2"},
				{Addr: "instance-3"},
			},
			MaxErrors: 1,
		},
	}

	for name, replicationSet := range testCases {
		t.Run(name, func(t *testing.T) {
			defer goleak.VerifyNone(t)

			parentCtx, cancel := context.WithCancel(context.WithValue(context.Background(), testContextKey, "this-is-the-value-from-the-parent"))
			cleanupTracker := newCleanupTracker(t, 2)

			logger := &testLogger{}
			spanLogger, ctx := spanlogger.New(parentCtx, logger, "DoUntilQuorum test", dummyTenantResolver{})

			wg := sync.WaitGroup{}
			wg.Add(2)

			f := func(ctx context.Context, desc *InstanceDesc, cancel context.CancelFunc) (string, error) {
				cleanupTracker.trackCall(ctx, desc, cancel)

				require.Equal(t, "this-is-the-value-from-the-parent", ctx.Value(testContextKey), "expected instance context to inherit from context passed to DoUntilQuorumWithoutSuccessfulContextCancellation")

				wg.Done()

				select {
				case <-ctx.Done():
					// Nothing more to do.
				case <-time.After(time.Second):
					require.FailNow(t, "expected instance context to be cancelled, but timed out waiting for cancellation")
				}

				return desc.Addr, nil
			}

			go func() {
				// Wait until the two expected calls to f have started.
				wg.Wait()
				cancel()
			}()

			cfg := DoUntilQuorumConfig{
				MinimizeRequests: true,
				Logger:           spanLogger,
			}
			results, err := DoUntilQuorumWithoutSuccessfulContextCancellation(ctx, replicationSet, cfg, f, cleanupTracker.cleanup)
			require.Empty(t, results)
			require.Equal(t, context.Canceled, err)

			calledInstances := cleanupTracker.calledInstances()
			require.Len(t, calledInstances, 2)
			cleanupTracker.collectCleanedUpInstances()
			cleanupTracker.assertCorrectCleanup(nil, calledInstances)

			expectedLogMessage := map[interface{}]interface{}{
				"level": level.DebugValue(),
				"msg":   "parent context done, returning",
				"err":   context.Canceled,
			}

			require.Equal(t, expectedLogMessage, logger.messages[len(logger.messages)-1])
		})
	}
}

func TestDoUntilQuorumWithoutSuccessfulContextCancellation_InstanceContextHandling(t *testing.T) {
	testCases := map[string]struct {
		replicationSet      ReplicationSet
		expectedResultCount int
	}{
		"with zone awareness": {
			replicationSet: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "instance-1", Zone: "zone-a"},
					{Addr: "instance-2", Zone: "zone-a"},
					{Addr: "instance-3", Zone: "zone-b"},
					{Addr: "instance-4", Zone: "zone-b"},
					{Addr: "instance-5", Zone: "zone-c"},
					{Addr: "instance-6", Zone: "zone-c"},
				},
				MaxUnavailableZones: 1,
			},
			expectedResultCount: 4,
		},
		"without zone awareness": {
			replicationSet: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "instance-1"},
					{Addr: "instance-2"},
					{Addr: "instance-3"},
				},
				MaxErrors: 1,
			},
			expectedResultCount: 2,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			defer goleak.VerifyNone(t)

			ctx := context.Background()
			cleanupTracker := newCleanupTracker(t, 0)

			f := func(ctx context.Context, desc *InstanceDesc, cancel context.CancelFunc) (string, error) {
				cleanupTracker.trackCall(ctx, desc, cancel)

				return desc.Addr, nil
			}

			cfg := DoUntilQuorumConfig{MinimizeRequests: true}
			instancesCalled, err := DoUntilQuorumWithoutSuccessfulContextCancellation(ctx, testCase.replicationSet, cfg, f, cleanupTracker.cleanup)
			require.NoError(t, err)
			require.Len(t, instancesCalled, testCase.expectedResultCount)

			cleanupTracker.collectCleanedUpInstances()
			cleanupTracker.assertCorrectCleanup(instancesCalled, nil)

			// Use the cancel function provided to the first instance and verify that no other instance contexts are cancelled.
			instanceToCancel := instancesCalled[0]
			cancel, ok := cleanupTracker.instanceCancelFuncs.Load(instanceToCancel)
			require.True(t, ok)
			cancel.(context.CancelFunc)()

			for _, instanceAddr := range instancesCalled {
				v, ok := cleanupTracker.instanceContexts.Load(instanceAddr)
				require.True(t, ok)
				ctx := v.(context.Context)

				if instanceAddr == instanceToCancel {
					require.Equal(t, context.Canceled, ctx.Err())
				} else {
					require.NoError(t, ctx.Err(), "expected context for other instance to not be cancelled")
				}
			}
		})
	}
}

func TestDoUntilQuorumWithoutSuccessfulContextCancellation_InvalidHedgingConfig(t *testing.T) {
	replicationSet := ReplicationSet{}
	cfg := DoUntilQuorumConfig{
		MinimizeRequests: true,
		HedgingDelay:     -1 * time.Second,
	}

	f := func(ctx context.Context, desc *InstanceDesc, cancelFunc context.CancelFunc) (string, error) {
		require.FailNow(t, "should never call f")
		return "", nil
	}

	cleanup := func(_ string) {
		require.FailNow(t, "should never call cleanup")
	}

	_, err := DoUntilQuorumWithoutSuccessfulContextCancellation(context.Background(), replicationSet, cfg, f, cleanup)
	require.EqualError(t, err, "invalid DoUntilQuorumConfig: HedgingDelay must be non-negative")
}

type hedgingTestInvocation struct {
	instance     *InstanceDesc
	invokedAfter time.Duration
}

func instancesFor(results []hedgingTestInvocation) []*InstanceDesc {
	instances := make([]*InstanceDesc, 0, len(results))

	for _, result := range results {
		instances = append(instances, result.instance)
	}

	return instances
}

func TestDoUntilQuorumWithoutSuccessfulContextCancellation_Hedging_ZoneAware(t *testing.T) {
	replicationSet := ReplicationSet{
		Instances: []InstanceDesc{
			{Addr: "instance-1", Zone: "zone-a"},
			{Addr: "instance-2", Zone: "zone-a"},
			{Addr: "instance-3", Zone: "zone-b"},
			{Addr: "instance-4", Zone: "zone-b"},
			{Addr: "instance-5", Zone: "zone-c"},
			{Addr: "instance-6", Zone: "zone-c"},
			{Addr: "instance-7", Zone: "zone-d"},
			{Addr: "instance-8", Zone: "zone-d"},
		},
		MaxUnavailableZones: 2,
	}

	defer goleak.VerifyNone(t)

	ctx := context.Background()
	cleanupTracker := newCleanupTracker(t, 4)

	wg := sync.WaitGroup{}
	wg.Add(len(replicationSet.Instances))
	mtx := sync.RWMutex{}
	invocations := make([]hedgingTestInvocation, 0, len(replicationSet.Instances))
	startedAt := time.Now()

	f := func(ctx context.Context, desc *InstanceDesc, cancel context.CancelFunc) (string, error) {
		result := hedgingTestInvocation{
			instance:     desc,
			invokedAfter: time.Since(startedAt),
		}

		cleanupTracker.trackCall(ctx, desc, cancel)

		mtx.Lock()
		invocations = append(invocations, result)
		mtx.Unlock()

		wg.Done()
		wg.Wait()

		return desc.Addr, nil
	}

	cfg := DoUntilQuorumConfig{MinimizeRequests: true, HedgingDelay: time.Second}
	successfulInstances, err := DoUntilQuorumWithoutSuccessfulContextCancellation(ctx, replicationSet, cfg, f, cleanupTracker.cleanup)
	require.NoError(t, err)
	require.Len(t, successfulInstances, 4)

	cleanupTracker.collectCleanedUpInstances()
	cleanupTracker.assertCorrectCleanup(successfulInstances, cleanupTracker.cleanedUpInstances)

	require.ElementsMatch(t, append(successfulInstances, cleanupTracker.cleanedUpInstances...), []string{"instance-1", "instance-2", "instance-3", "instance-4", "instance-5", "instance-6", "instance-7", "instance-8"}, "all instances should be called")

	require.Equal(t, 2, uniqueZoneCount(instancesFor(invocations[0:4])), "should initially release instances from two zones")
	require.Equal(t, 1, uniqueZoneCount(instancesFor(invocations[4:6])), "should release one zone after first delay")
	require.Equal(t, 1, uniqueZoneCount(instancesFor(invocations[6:8])), "should release one zone after first delay")

	tolerance := float64((100 * time.Millisecond).Nanoseconds())

	for _, result := range invocations[0:4] {
		require.InDelta(t, 0, result.invokedAfter, tolerance, "expected first four requests to be released immediately")
	}

	for _, result := range invocations[4:6] {
		require.InDelta(t, cfg.HedgingDelay, result.invokedAfter, tolerance, "expected next zone to be released after hedging delay")
	}

	for _, result := range invocations[6:8] {
		require.InDelta(t, 2*cfg.HedgingDelay, result.invokedAfter, tolerance, "expected final zone to be released after another hedging delay")
	}
}

func TestDoUntilQuorumWithoutSuccessfulContextCancellation_Hedging_NonZoneAware(t *testing.T) {
	replicationSet := ReplicationSet{
		Instances: []InstanceDesc{
			{Addr: "instance-1"},
			{Addr: "instance-2"},
			{Addr: "instance-3"},
			{Addr: "instance-4"},
		},
		MaxErrors: 2,
	}

	defer goleak.VerifyNone(t)

	ctx := context.Background()
	cleanupTracker := newCleanupTracker(t, 2)

	wg := sync.WaitGroup{}
	wg.Add(len(replicationSet.Instances))
	mtx := sync.RWMutex{}
	invocations := make([]hedgingTestInvocation, 0, len(replicationSet.Instances))
	startedAt := time.Now()

	f := func(ctx context.Context, desc *InstanceDesc, cancel context.CancelFunc) (string, error) {
		result := hedgingTestInvocation{
			instance:     desc,
			invokedAfter: time.Since(startedAt),
		}

		cleanupTracker.trackCall(ctx, desc, cancel)

		mtx.Lock()
		invocations = append(invocations, result)
		mtx.Unlock()

		wg.Done()
		wg.Wait()

		return desc.Addr, nil
	}

	cfg := DoUntilQuorumConfig{MinimizeRequests: true, HedgingDelay: time.Second}
	successfulInstances, err := DoUntilQuorumWithoutSuccessfulContextCancellation(ctx, replicationSet, cfg, f, cleanupTracker.cleanup)
	require.NoError(t, err)
	require.Len(t, successfulInstances, 2)

	cleanupTracker.collectCleanedUpInstances()
	cleanupTracker.assertCorrectCleanup(successfulInstances, cleanupTracker.cleanedUpInstances)

	require.ElementsMatch(t, append(successfulInstances, cleanupTracker.cleanedUpInstances...), []string{"instance-1", "instance-2", "instance-3", "instance-4"}, "all instances should be called")

	tolerance := float64((100 * time.Millisecond).Nanoseconds())

	require.InDelta(t, 0, invocations[0].invokedAfter, tolerance, "expected first request to be released immediately")
	require.InDelta(t, 0, invocations[1].invokedAfter, tolerance, "expected second request to be released immediately")
	require.InDelta(t, cfg.HedgingDelay, invocations[2].invokedAfter, tolerance, "expected next request to be released after hedging delay")
	require.InDelta(t, 2*cfg.HedgingDelay, invocations[3].invokedAfter, tolerance, "expected final request to be released after another hedging delay")
}

func TestDoUntilQuorum_InstanceContextHandling(t *testing.T) {
	testCases := map[string]struct {
		replicationSet      ReplicationSet
		expectedResultCount int
	}{
		"with zone awareness": {
			replicationSet: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "instance-1", Zone: "zone-a"},
					{Addr: "instance-2", Zone: "zone-a"},
					{Addr: "instance-3", Zone: "zone-b"},
					{Addr: "instance-4", Zone: "zone-b"},
					{Addr: "instance-5", Zone: "zone-c"},
					{Addr: "instance-6", Zone: "zone-c"},
				},
				MaxUnavailableZones: 1,
			},
			expectedResultCount: 4,
		},
		"without zone awareness": {
			replicationSet: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "instance-1"},
					{Addr: "instance-2"},
					{Addr: "instance-3"},
				},
				MaxErrors: 1,
			},
			expectedResultCount: 2,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			defer goleak.VerifyNone(t)

			ctx := context.WithValue(context.Background(), testContextKey, "this-is-the-value-from-the-parent")
			cleanupTracker := newCleanupTracker(t, 0)
			cleanupTracker.successfulResultsShouldHaveCancelledContexts = true

			f := func(ctx context.Context, desc *InstanceDesc) (string, error) {
				cleanupTracker.trackCall(ctx, desc, nil)
				require.Equal(t, "this-is-the-value-from-the-parent", ctx.Value(testContextKey))

				return desc.Addr, nil
			}

			cfg := DoUntilQuorumConfig{MinimizeRequests: true}
			instancesCalled, err := DoUntilQuorum(ctx, testCase.replicationSet, cfg, f, cleanupTracker.cleanup)
			require.NoError(t, err)
			require.Len(t, instancesCalled, testCase.expectedResultCount)

			cleanupTracker.collectCleanedUpInstances()
			cleanupTracker.assertCorrectCleanup(instancesCalled, nil) // This will check that all request contexts are cancelled.
		})
	}
}

type cleanupTracker struct {
	t                                            *testing.T
	maximumExpectedCleanupCalls                  int
	successfulResultsShouldHaveCancelledContexts bool

	receivedCleanupCalls *atomic.Uint64
	cleanupChan          chan string
	cleanedUpInstances   []string
	instanceContexts     sync.Map
	instanceCancelFuncs  sync.Map
}

func newCleanupTracker(t *testing.T, expectedMaximumCleanupCalls int) *cleanupTracker {
	return &cleanupTracker{
		t:                           t,
		maximumExpectedCleanupCalls: expectedMaximumCleanupCalls,
		receivedCleanupCalls:        atomic.NewUint64(0),
		cleanupChan:                 make(chan string, expectedMaximumCleanupCalls),
	}
}

func (c *cleanupTracker) trackCall(ctx context.Context, instance *InstanceDesc, cancel context.CancelFunc) {
	c.instanceContexts.Store(instance.Addr, ctx)
	c.instanceCancelFuncs.Store(instance.Addr, cancel)
}

func (c *cleanupTracker) calledInstances() []string {
	instances := []string{}

	c.instanceContexts.Range(func(key, value any) bool {
		instances = append(instances, key.(string))

		return true
	})

	return instances
}

func (c *cleanupTracker) cleanup(res string) {
	cleanupCallsSoFar := c.receivedCleanupCalls.Inc()

	if cleanupCallsSoFar > uint64(c.maximumExpectedCleanupCalls) {
		require.FailNowf(c.t, "received more cleanup calls than expected", "expected at most %v, but got at least %v", c.maximumExpectedCleanupCalls, cleanupCallsSoFar)
	}

	c.cleanupChan <- res
}

func (c *cleanupTracker) collectCleanedUpInstances() {
	c.collectCleanedUpInstancesWithExpectedCount(c.maximumExpectedCleanupCalls)
}

func (c *cleanupTracker) collectCleanedUpInstancesWithExpectedCount(expected int) {
	c.cleanedUpInstances = make([]string, 0, expected)

	for len(c.cleanedUpInstances) < expected {
		select {
		case call := <-c.cleanupChan:
			c.cleanedUpInstances = append(c.cleanedUpInstances, call)
		case <-time.After(time.Second):
			require.FailNowf(c.t, "gave up waiting for expected cleanup call", "have received %v so far, expected %v", c.cleanedUpInstances, expected)
		}
	}
}

func (c *cleanupTracker) assertCorrectCleanup(successfulInstances []string, failedInstances []string) {
	for _, instance := range successfulInstances {
		require.NotContainsf(c.t, failedInstances, instance, "invalid test case: instance %v is in list of both successful and failed instances", instance)
		require.NotContainsf(c.t, c.cleanedUpInstances, instance, "result for instance %v was returned, but it was cleaned up", instance)

		instanceContext, ok := c.instanceContexts.Load(instance)
		require.True(c.t, ok)

		if c.successfulResultsShouldHaveCancelledContexts {
			require.Equalf(c.t, context.Canceled, instanceContext.(context.Context).Err(), "all returned results should have their context cancelled, but context for %v is not cancelled", instance)
		} else {
			require.NoErrorf(c.t, instanceContext.(context.Context).Err(), "all returned results should not have their context cancelled, but context for %v is cancelled", instance)
		}
	}

	for _, instance := range failedInstances {
		require.Containsf(c.t, c.cleanedUpInstances, instance, "result for instance %v was not returned, but it was not cleaned up", instance)

		instanceContext, ok := c.instanceContexts.Load(instance)
		require.True(c.t, ok)
		require.Equalf(c.t, context.Canceled, instanceContext.(context.Context).Err(), "all cleaned up results should have their context cancelled, but context for %v is not cancelled", instance)
	}

	require.ElementsMatch(c.t, c.cleanedUpInstances, failedInstances)
}

var (
	replicationSetChangesInitialState = ReplicationSet{
		Instances: []InstanceDesc{
			{Addr: "127.0.0.1"},
			{Addr: "127.0.0.2"},
			{Addr: "127.0.0.3"},
		},
	}
	replicationSetChangesTestCases = map[string]struct {
		nextState                                  ReplicationSet
		expectHasReplicationSetChanged             bool
		expectHasReplicationSetChangedWithoutState bool
	}{
		"timestamp changed": {
			ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "127.0.0.1", Timestamp: time.Hour.Microseconds()},
					{Addr: "127.0.0.2"},
					{Addr: "127.0.0.3"},
				},
			},
			false,
			false,
		},
		"state changed": {
			ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "127.0.0.1", State: PENDING},
					{Addr: "127.0.0.2"},
					{Addr: "127.0.0.3"},
				},
			},
			true,
			false,
		},
		"more instances": {
			ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "127.0.0.1"},
					{Addr: "127.0.0.2"},
					{Addr: "127.0.0.3"},
					{Addr: "127.0.0.4"},
				},
			},
			true,
			true,
		},
	}
)

func TestHasReplicationSetChanged_IgnoresTimeStamp(t *testing.T) {
	// Only testing difference to underlying Equal function
	for testName, testData := range replicationSetChangesTestCases {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectHasReplicationSetChanged, HasReplicationSetChanged(replicationSetChangesInitialState, testData.nextState), "HasReplicationSetChanged wrong result")
		})
	}
}

func TestHasReplicationSetChangedWithoutState_IgnoresTimeStampAndState(t *testing.T) {
	// Only testing difference to underlying Equal function
	for testName, testData := range replicationSetChangesTestCases {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectHasReplicationSetChangedWithoutState, HasReplicationSetChangedWithoutState(replicationSetChangesInitialState, testData.nextState), "HasReplicationSetChangedWithoutState wrong result")
		})
	}
}

func TestReplicationSet_ZoneCount(t *testing.T) {
	testCases := map[string]struct {
		instances         []InstanceDesc
		expectedZoneCount int
	}{
		"empty ring": {
			instances:         []InstanceDesc{},
			expectedZoneCount: 0,
		},
		"ring with single instance without a zone": {
			instances: []InstanceDesc{
				{Addr: "instance-1"},
			},
			expectedZoneCount: 1,
		},
		"ring with many instances without a zone": {
			instances: []InstanceDesc{
				{Addr: "instance-1"},
				{Addr: "instance-2"},
				{Addr: "instance-3"},
			},
			expectedZoneCount: 1,
		},
		"ring with single instance with a zone": {
			instances: []InstanceDesc{
				{Addr: "instance-1", Zone: "zone-a"},
			},
			expectedZoneCount: 1,
		},
		"ring with many instances in one zone": {
			instances: []InstanceDesc{
				{Addr: "instance-1", Zone: "zone-a"},
				{Addr: "instance-2", Zone: "zone-a"},
				{Addr: "instance-3", Zone: "zone-a"},
			},
			expectedZoneCount: 1,
		},
		"ring with many instances, each in their own zone": {
			instances: []InstanceDesc{
				{Addr: "instance-1", Zone: "zone-a"},
				{Addr: "instance-2", Zone: "zone-b"},
				{Addr: "instance-3", Zone: "zone-c"},
			},
			expectedZoneCount: 3,
		},
		"ring with many instances in each zone": {
			instances: []InstanceDesc{
				{Addr: "zone-a-instance-1", Zone: "zone-a"},
				{Addr: "zone-a-instance-2", Zone: "zone-a"},
				{Addr: "zone-a-instance-3", Zone: "zone-a"},
				{Addr: "zone-b-instance-1", Zone: "zone-b"},
				{Addr: "zone-b-instance-2", Zone: "zone-b"},
				{Addr: "zone-b-instance-3", Zone: "zone-b"},
				{Addr: "zone-c-instance-1", Zone: "zone-c"},
				{Addr: "zone-c-instance-2", Zone: "zone-c"},
				{Addr: "zone-c-instance-3", Zone: "zone-c"},
			},
			expectedZoneCount: 3,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			r := ReplicationSet{Instances: testCase.instances}

			actual := r.ZoneCount()
			require.Equal(t, testCase.expectedZoneCount, actual)
		})
	}
}

func BenchmarkReplicationSetZoneCount(b *testing.B) {
	for _, instancesPerZone := range []int{1, 2, 5, 10, 100, 300} {
		for _, zones := range []int{1, 2, 3} {
			instances := make([]InstanceDesc, 0, instancesPerZone*zones)

			for zoneIdx := 0; zoneIdx < zones; zoneIdx++ {
				zoneName := fmt.Sprintf("zone-%v", string(rune('a'+zoneIdx)))

				for instanceIdx := 0; instanceIdx < instancesPerZone; instanceIdx++ {
					instance := InstanceDesc{
						Addr: fmt.Sprintf("%v-instance-%v", zoneName, instanceIdx+1),
						Zone: zoneName,
					}

					instances = append(instances, instance)
				}
			}

			r := ReplicationSet{Instances: instances}

			b.Run(fmt.Sprintf("%v instances per zone, %v zones", instancesPerZone, zones), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					r.ZoneCount()
				}
			})
		}
	}
}

type dummyTenantResolver struct{}

func (d dummyTenantResolver) TenantID(context.Context) (string, error) {
	return "test-tenant-id", nil
}

func (d dummyTenantResolver) TenantIDs(context.Context) ([]string, error) {
	return []string{"test-tenant-id"}, nil
}

type testLogger struct {
	messages []map[interface{}]interface{}
}

func (l *testLogger) Log(keyvals ...interface{}) error {
	if len(keyvals)%2 != 0 {
		panic("mismatched key-value pairs logged to testLogger")
	}

	msg := map[interface{}]interface{}{}

	for i := 0; i < len(keyvals); i += 2 {
		key := keyvals[i]
		value := keyvals[i+1]

		if key == "user" || key == "method" {
			// These keys are added automatically by spanlogger, but they're not interesting for our tests.
			continue
		}

		msg[key] = value
	}

	l.messages = append(l.messages, msg)

	return nil
}
