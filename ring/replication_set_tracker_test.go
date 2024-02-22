package ring

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/cancellation"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type contextKey int

const testContextKey contextKey = iota

func TestDefaultResultTracker(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4"}

	tests := map[string]struct {
		instances []InstanceDesc
		maxErrors int
		run       func(t *testing.T, tracker *defaultResultTracker)
	}{
		"should succeed on no instances to track": {
			instances: nil,
			maxErrors: 0,
			run: func(t *testing.T, tracker *defaultResultTracker) {
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())
			},
		},
		"should succeed once all instances succeed on max errors = 0": {
			instances: []InstanceDesc{instance1, instance2, instance3, instance4},
			maxErrors: 0,
			run: func(t *testing.T, tracker *defaultResultTracker) {
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance2, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance3, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance4, nil)
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				assert.True(t, tracker.shouldIncludeResultFrom(&instance1))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance2))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance3))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance4))
			},
		},
		"should succeed with 1 failing instance on max errors = 1": {
			instances: []InstanceDesc{instance1, instance2, instance3, instance4},
			maxErrors: 1,
			run: func(t *testing.T, tracker *defaultResultTracker) {
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance2, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance3, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance4, nil)
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				assert.True(t, tracker.shouldIncludeResultFrom(&instance1))
				// Instance 2 failed, and so shouldIncludeResultFrom won't be called by DoUntilQuorumWithoutSuccessfulContextCancellation.
				assert.True(t, tracker.shouldIncludeResultFrom(&instance3))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance4))
			},
		},
		"should fail on 1st failing instance on max errors = 0": {
			instances: []InstanceDesc{instance1, instance2, instance3, instance4},
			maxErrors: 0,
			run: func(t *testing.T, tracker *defaultResultTracker) {
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance2, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.True(t, tracker.failed())
			},
		},
		"should fail on 2nd failing instance on max errors = 1": {
			instances: []InstanceDesc{instance1, instance2, instance3, instance4},
			maxErrors: 1,
			run: func(t *testing.T, tracker *defaultResultTracker) {
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance2, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance3, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.True(t, tracker.failed())
			},
		},
		"should fail on 3rd failing instance on max errors = 2": {
			instances: []InstanceDesc{instance1, instance2, instance3, instance4},
			maxErrors: 2,
			run: func(t *testing.T, tracker *defaultResultTracker) {
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance2, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance3, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance4, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.True(t, tracker.failed())
			},
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			testCase.run(t, newDefaultResultTracker(testCase.instances, testCase.maxErrors, log.NewNopLogger()))
		})
	}
}

func TestDefaultResultTracker_AwaitStart_ContextCancelled(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instances := []InstanceDesc{instance1}
	tracker := newDefaultResultTracker(instances, 0, log.NewNopLogger())

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	instance := &instances[0]
	require.Equal(t, context.Canceled, tracker.awaitStart(ctx, instance), "expected awaitStart() to abort when context cancelled")
}

func TestDefaultResultTracker_StartAllRequests(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Id: "instance-1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Id: "instance-2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Id: "instance-3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Id: "instance-4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	logger := &testLogger{}
	tracker := newDefaultResultTracker(instances, 1, logger)

	tracker.startAllRequests()
	expectedLogMessages := []map[interface{}]interface{}{}

	for i := range instances {
		instance := &instances[i]
		require.NoError(t, tracker.awaitStart(context.Background(), instance), "requests for all instances should be released immediately")

		expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
			"level":        level.DebugValue(),
			"msg":          "starting request to instance",
			"reason":       "initial requests",
			"instanceAddr": instance.Addr,
			"instanceID":   instance.Id,
		})
	}

	require.ElementsMatch(t, expectedLogMessages, logger.messages)
}

func TestDefaultResultTracker_StartMinimumRequests_NoFailingRequests(t *testing.T) {
	t.Parallel()

	instance1 := InstanceDesc{Addr: "127.0.0.1", Id: "instance-1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Id: "instance-2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Id: "instance-3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Id: "instance-4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	instanceRequestCounts := make([]atomic.Uint64, len(instances))

	for testIteration := 0; testIteration < 1000; testIteration++ {
		logger := &testLogger{}
		tracker := newDefaultResultTracker(instances, 1, logger)
		tracker.startMinimumRequests()

		mtx := sync.RWMutex{}
		instancesAwaitReleaseResults := make(map[*InstanceDesc]error, len(instances))
		countInstancesReleased := 0

		for instanceIdx := range instances {
			instanceIdx := instanceIdx
			instance := &instances[instanceIdx]
			go func() {
				err := tracker.awaitStart(context.Background(), instance)

				mtx.Lock()
				defer mtx.Unlock()
				instancesAwaitReleaseResults[instance] = err
				countInstancesReleased++

				if err == nil {
					instanceRequestCounts[instanceIdx].Inc()
				}
			}()
		}

		require.Eventually(t, func() bool {
			mtx.RLock()
			defer mtx.RUnlock()

			return countInstancesReleased == 3
		}, 1*time.Second, 10*time.Millisecond, "expected three of the four requests to be released")

		require.Equal(t, 3, nilErrorCount(instancesAwaitReleaseResults), "all requests released so far should be signalled to start immediately")

		var expectedLogMessages []map[interface{}]interface{}

		for instance := range instancesAwaitReleaseResults {
			expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
				"level":        level.DebugValue(),
				"msg":          "starting request to instance",
				"reason":       "initial requests",
				"instanceAddr": instance.Addr,
				"instanceID":   instance.Id,
			})
		}

		// Signal that the three released requests have completed successfully.
		tracker.done(nil, nil)
		tracker.done(nil, nil)
		tracker.done(nil, nil)

		require.Eventually(t, func() bool {
			mtx.RLock()
			defer mtx.RUnlock()

			return countInstancesReleased == 4
		}, 1*time.Second, 10*time.Millisecond, "expected the final request to be released")

		require.Equal(t, 3, nilErrorCount(instancesAwaitReleaseResults), "expected the final request to be released but not signalled to start")

		require.True(t, tracker.succeeded())

		require.ElementsMatch(t, expectedLogMessages, logger.messages)
	}

	// With 1000 iterations, 4 instances and max 1 error, we'd expect each instance to receive
	// 750 calls each (each instance has a 3-in-4 chance of being called in each iteration).
	const expectedAverageCallsPerInstance = 750
	const tolerancePerc = 0.2
	for _, instanceRequestCount := range instanceRequestCounts {
		require.InDeltaf(t, expectedAverageCallsPerInstance, instanceRequestCount.Load(), expectedAverageCallsPerInstance*tolerancePerc, "expected roughly even distribution of requests across all instances, but got %v", instanceRequestCounts)
	}
}

func TestDefaultResultTracker_StartMinimumRequests_FailingRequestsBelowMaximumAllowed(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Id: "instance-1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Id: "instance-2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Id: "instance-3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Id: "instance-4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	logger := &testLogger{}
	tracker := newDefaultResultTracker(instances, 2, logger)
	tracker.startMinimumRequests()

	mtx := sync.RWMutex{}
	instancesReleased := []*InstanceDesc{}
	countInstancesSignalledToStart := 0

	for instanceIdx := range instances {
		instance := &instances[instanceIdx]
		go func() {
			err := tracker.awaitStart(context.Background(), instance)

			mtx.Lock()
			defer mtx.Unlock()

			instancesReleased = append(instancesReleased, instance)
			if err == nil {
				countInstancesSignalledToStart++
			}

			// If this is the second request released, mark it as failed.
			if len(instancesReleased) == 2 {
				tracker.done(instance, errors.New("something went wrong"))
			}
		}()
	}

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return len(instancesReleased) == 3
	}, 1*time.Second, 10*time.Millisecond, "expected three requests to be released")

	require.Equal(t, 3, countInstancesSignalledToStart, "all requests released so far should be signalled to start")

	// Mark the remaining two requests as successful.
	tracker.done(nil, nil)
	tracker.done(nil, nil)

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return len(instancesReleased) == 4
	}, 1*time.Second, 10*time.Millisecond, "expected all four requests to be released")

	require.Equal(t, 3, countInstancesSignalledToStart, "final request should not be signalled to start")
	require.True(t, tracker.succeeded(), "overall request should succeed")

	var expectedLogMessages []map[interface{}]interface{}

	for _, instance := range instancesReleased[0:2] {
		expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
			"level":        level.DebugValue(),
			"msg":          "starting request to instance",
			"reason":       "initial requests",
			"instanceAddr": instance.Addr,
			"instanceID":   instance.Id,
		})
	}

	expectedLogMessages = append(expectedLogMessages,
		map[interface{}]interface{}{
			"level":        level.WarnValue(),
			"msg":          "instance failed",
			"instanceAddr": instancesReleased[1].Addr,
			"instanceID":   instancesReleased[1].Id,
			"err":          errors.New("something went wrong"),
		},
		map[interface{}]interface{}{
			"level":        level.DebugValue(),
			"msg":          "starting request to instance",
			"reason":       "failure of other instance",
			"instanceAddr": instancesReleased[2].Addr,
			"instanceID":   instancesReleased[2].Id,
		},
	)

	require.ElementsMatch(t, expectedLogMessages, logger.messages)
}

func TestDefaultResultTracker_StartMinimumRequests_FailingRequestsEqualToMaximumAllowed(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	tracker := newDefaultResultTracker(instances, 2, log.NewNopLogger())
	tracker.startMinimumRequests()

	mtx := sync.RWMutex{}
	countInstancesReleased := 0
	countInstancesSignalledToStart := 0

	for instanceIdx := range instances {
		instanceIdx := instanceIdx
		instance := &instances[instanceIdx]
		go func() {
			err := tracker.awaitStart(context.Background(), instance)

			mtx.Lock()
			defer mtx.Unlock()

			countInstancesReleased++
			if err == nil {
				countInstancesSignalledToStart++
			}

			// If this is the first or second request released, mark it as failed.
			if countInstancesReleased <= 2 {
				tracker.done(instance, errors.New("something went wrong"))
			}
		}()
	}

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return countInstancesReleased == 4
	}, 1*time.Second, 10*time.Millisecond, "expected all four requests to be released")

	// Mark the remaining requests as successful.
	tracker.done(nil, nil)
	tracker.done(nil, nil)

	require.Equal(t, 4, countInstancesSignalledToStart, "expected all four instances to be signalled to start")
	require.True(t, tracker.succeeded(), "overall request should succeed")
}

func TestDefaultResultTracker_StartMinimumRequests_MoreFailingRequestsThanMaximumAllowed(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	tracker := newDefaultResultTracker(instances, 1, log.NewNopLogger())
	tracker.startMinimumRequests()

	mtx := sync.RWMutex{}
	countInstancesReleased := 0
	countInstancesSignalledToStart := 0

	for instanceIdx := range instances {
		instanceIdx := instanceIdx
		instance := &instances[instanceIdx]
		go func() {
			err := tracker.awaitStart(context.Background(), instance)

			mtx.Lock()
			defer mtx.Unlock()

			countInstancesReleased++
			if err == nil {
				countInstancesSignalledToStart++
			}

			// If this is the first or second request released, mark it as failed.
			if countInstancesReleased <= 2 {
				tracker.done(instance, errors.New("something went wrong"))
			}
		}()
	}

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return countInstancesReleased == 4
	}, 1*time.Second, 10*time.Millisecond, "expected all four requests to be released")

	// Mark the remaining requests as successful.
	tracker.done(nil, nil)
	tracker.done(nil, nil)

	require.Equal(t, 4, countInstancesSignalledToStart, "expected all four instances to be signalled to start")
	require.True(t, tracker.failed(), "overall request should fail")
}

func TestDefaultResultTracker_StartMinimumRequests_MaxErrorsIsNumberOfInstances(t *testing.T) {
	// This scenario should never happen in the real world, but if we were to get into this situation,
	// we need to make sure we don't end up blocking forever, which could lead to leaking a goroutine in DoUntilQuorumWithoutSuccessfulContextCancellation.

	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instances := []InstanceDesc{instance1}
	tracker := newDefaultResultTracker(instances, 1, log.NewNopLogger())
	tracker.startMinimumRequests()

	var err error
	released := make(chan struct{})

	go func() {
		err = tracker.awaitStart(context.Background(), &instances[0])
		close(released)
	}()

	select {
	case <-released:
		require.Equal(t, errResultNotNeeded, err, "should not start request not required for quorum")
	case <-time.After(time.Second):
		require.Fail(t, "gave up waiting for request to be released")
	}
}

func TestDefaultResultTracker_StartAdditionalRequests(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Id: "instance-1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Id: "instance-2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Id: "instance-3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Id: "instance-4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	logger := &testLogger{}
	tracker := newDefaultResultTracker(instances, 2, logger)
	tracker.startMinimumRequests()

	mtx := sync.RWMutex{}
	instancesReleased := []*InstanceDesc{}
	countInstancesSignalledToStart := 0

	for instanceIdx := range instances {
		instanceIdx := instanceIdx
		instance := &instances[instanceIdx]
		go func() {
			err := tracker.awaitStart(context.Background(), instance)

			mtx.Lock()
			defer mtx.Unlock()

			instancesReleased = append(instancesReleased, instance)
			if err == nil {
				countInstancesSignalledToStart++
			}
		}()
	}

	waitForInstancesReleased := func(expected int, msg string) {
		require.Eventually(t, func() bool {
			mtx.RLock()
			defer mtx.RUnlock()

			return len(instancesReleased) == expected && countInstancesSignalledToStart == expected
		}, time.Second, 10*time.Millisecond, msg)
	}

	waitForInstancesReleased(2, "should initially release two requests")
	expectedLogMessages := []map[interface{}]interface{}{
		{
			"level":        level.DebugValue(),
			"msg":          "starting request to instance",
			"reason":       "initial requests",
			"instanceAddr": instancesReleased[0].Addr,
			"instanceID":   instancesReleased[0].Id,
		},
		{
			"level":        level.DebugValue(),
			"msg":          "starting request to instance",
			"reason":       "initial requests",
			"instanceAddr": instancesReleased[1].Addr,
			"instanceID":   instancesReleased[1].Id,
		},
	}
	require.ElementsMatch(t, expectedLogMessages, logger.messages)

	tracker.startAdditionalRequests()
	waitForInstancesReleased(3, "should release a third request after startAdditionalRequests()")
	expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
		"level":        level.DebugValue(),
		"msg":          "starting request to instance",
		"reason":       "hedging",
		"instanceAddr": instancesReleased[2].Addr,
		"instanceID":   instancesReleased[2].Id,
	})
	require.ElementsMatch(t, expectedLogMessages, logger.messages)

	tracker.startAdditionalRequests()
	waitForInstancesReleased(4, "should release remaining request after startAdditionalRequests()")
	expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
		"level":        level.DebugValue(),
		"msg":          "starting request to instance",
		"reason":       "hedging",
		"instanceAddr": instancesReleased[3].Addr,
		"instanceID":   instancesReleased[3].Id,
	})
	require.ElementsMatch(t, expectedLogMessages, logger.messages)
}

func TestDefaultContextTracker(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	parentCtx := context.WithValue(context.Background(), testContextKey, "this-is-the-value-from-the-parent")
	tracker := newDefaultContextTracker(parentCtx, instances)

	instance1Ctx, _ := tracker.contextFor(&instance1)
	instance2Ctx, instance2Cancel := tracker.contextFor(&instance2)
	instance3Ctx, _ := tracker.contextFor(&instance3)
	instance4Ctx, _ := tracker.contextFor(&instance4)

	for _, ctx := range []context.Context{instance1Ctx, instance2Ctx, instance3Ctx, instance4Ctx} {
		require.Equal(t, "this-is-the-value-from-the-parent", ctx.Value(testContextKey), "context for instance should inherit from provided parent context")
		require.NoError(t, ctx.Err(), "context for instance should not be cancelled")
	}

	// Cancel a context for one instance using cancelContextFor and check that the others are not cancelled.
	instance1Cause := cancellation.NewErrorf("instance 1 cancellation cause")
	tracker.cancelContextFor(&instance1, instance1Cause)
	require.Equal(t, context.Canceled, instance1Ctx.Err(), "instance context should be cancelled")
	require.Equal(t, instance1Cause, context.Cause(instance1Ctx))
	for _, ctx := range []context.Context{instance2Ctx, instance3Ctx, instance4Ctx} {
		require.NoError(t, ctx.Err(), "context for instance should not be cancelled after cancelling the context of another instance")
	}

	// Cancel another context using the cancellation function provided and check that the others are not cancelled.
	instance2Cause := cancellation.NewErrorf("instance 1 cancellation cause")
	instance2Cancel(instance2Cause)
	require.Equal(t, context.Canceled, instance2Ctx.Err(), "instance context should be cancelled")
	require.Equal(t, instance2Cause, context.Cause(instance2Ctx))
	for _, ctx := range []context.Context{instance3Ctx, instance4Ctx} {
		require.NoError(t, ctx.Err(), "context for instance should not be cancelled after cancelling the context of another instance")
	}

	remainingInstancesCause := cancellation.NewErrorf("remaining instances cancellation cause")
	tracker.cancelAllContexts(remainingInstancesCause)
	for _, ctx := range []context.Context{instance3Ctx, instance4Ctx} {
		require.Equal(t, context.Canceled, ctx.Err(), "context for instance should be cancelled after cancelling all contexts")
		require.Equal(t, remainingInstancesCause, context.Cause(ctx))
	}
}

func TestZoneAwareResultTracker(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}

	tests := map[string]struct {
		instances           []InstanceDesc
		maxUnavailableZones int
		run                 func(t *testing.T, tracker *zoneAwareResultTracker)
	}{
		"should succeed on no instances to track": {
			instances:           nil,
			maxUnavailableZones: 0,
			run: func(t *testing.T, tracker *zoneAwareResultTracker) {
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())
			},
		},
		"should succeed once all instances succeed on max unavailable zones = 0": {
			instances:           []InstanceDesc{instance1, instance2, instance3},
			maxUnavailableZones: 0,
			run: func(t *testing.T, tracker *zoneAwareResultTracker) {
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance2, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance3, nil)
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				assert.True(t, tracker.shouldIncludeResultFrom(&instance1))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance2))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance3))
			},
		},
		"should fail on 1st failing instance on max unavailable zones = 0": {
			instances:           []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6},
			maxUnavailableZones: 0,
			run: func(t *testing.T, tracker *zoneAwareResultTracker) {
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance2, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.True(t, tracker.failed())
			},
		},
		"should succeed on 2 failing instances within the same zone on max unavailable zones = 1": {
			instances:           []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6},
			maxUnavailableZones: 1,
			run: func(t *testing.T, tracker *zoneAwareResultTracker) {
				// Track failing instances.
				for _, instance := range []InstanceDesc{instance1, instance2} {
					tracker.done(&instance, errors.New("test"))
					assert.False(t, tracker.succeeded())
					assert.False(t, tracker.failed())
				}

				// Track successful instances.
				for _, instance := range []InstanceDesc{instance3, instance4, instance5} {
					tracker.done(&instance, nil)
					assert.False(t, tracker.succeeded())
					assert.False(t, tracker.failed())
				}

				tracker.done(&instance6, nil)
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				assert.False(t, tracker.shouldIncludeResultFrom(&instance1))
				assert.False(t, tracker.shouldIncludeResultFrom(&instance2))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance3))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance4))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance5))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance6))
			},
		},
		"should succeed as soon as the response has been successfully received from 'all zones - 1' on max unavailable zones = 1": {
			instances:           []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6},
			maxUnavailableZones: 1,
			run: func(t *testing.T, tracker *zoneAwareResultTracker) {
				// Track successful instances.
				for _, instance := range []InstanceDesc{instance1, instance2, instance3} {
					tracker.done(&instance, nil)
					assert.False(t, tracker.succeeded())
					assert.False(t, tracker.failed())
				}

				tracker.done(&instance4, nil)
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				assert.True(t, tracker.shouldIncludeResultFrom(&instance1))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance2))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance3))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance4))
				assert.False(t, tracker.shouldIncludeResultFrom(&instance5))
				assert.False(t, tracker.shouldIncludeResultFrom(&instance6))
			},
		},
		"should succeed on failing instances within 2 zones on max unavailable zones = 2": {
			instances:           []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6},
			maxUnavailableZones: 2,
			run: func(t *testing.T, tracker *zoneAwareResultTracker) {
				// Track failing instances.
				for _, instance := range []InstanceDesc{instance1, instance2, instance3, instance4} {
					tracker.done(&instance, errors.New("test"))
					assert.False(t, tracker.succeeded())
					assert.False(t, tracker.failed())
				}

				// Track successful instances.
				tracker.done(&instance5, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance6, nil)
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				assert.False(t, tracker.shouldIncludeResultFrom(&instance1))
				assert.False(t, tracker.shouldIncludeResultFrom(&instance2))
				assert.False(t, tracker.shouldIncludeResultFrom(&instance3))
				assert.False(t, tracker.shouldIncludeResultFrom(&instance4))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance5))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance6))
			},
		},
		"should succeed as soon as the response has been successfully received from 'all zones - 2' on max unavailable zones = 2": {
			instances:           []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6},
			maxUnavailableZones: 2,
			run: func(t *testing.T, tracker *zoneAwareResultTracker) {
				// Zone-a
				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				// Zone-b
				tracker.done(&instance3, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				// Zone-a
				tracker.done(&instance2, nil)
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				assert.True(t, tracker.shouldIncludeResultFrom(&instance1))
				assert.True(t, tracker.shouldIncludeResultFrom(&instance2))
				assert.False(t, tracker.shouldIncludeResultFrom(&instance3))
				assert.False(t, tracker.shouldIncludeResultFrom(&instance4))
				assert.False(t, tracker.shouldIncludeResultFrom(&instance5))
				assert.False(t, tracker.shouldIncludeResultFrom(&instance6))
			},
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			testCase.run(t, newZoneAwareResultTracker(testCase.instances, testCase.maxUnavailableZones, nil, log.NewNopLogger()))
		})
	}
}

func TestZoneAwareResultTracker_AwaitStart_ContextCancelled(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instances := []InstanceDesc{instance1}
	tracker := newZoneAwareResultTracker(instances, 0, nil, log.NewNopLogger())

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	instance := &instances[0]
	require.Equal(t, context.Canceled, tracker.awaitStart(ctx, instance), "expected awaitStart() to abort when context cancelled")
}

func TestZoneAwareResultTracker_StartAllRequests(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6}

	logger := &testLogger{}
	tracker := newZoneAwareResultTracker(instances, 1, nil, logger)

	tracker.startAllRequests()

	for i := range instances {
		instance := &instances[i]
		require.NoError(t, tracker.awaitStart(context.Background(), instance), "requests for all instances should be released immediately")
	}

	expectedLogMessages := []map[interface{}]interface{}{}

	for _, zone := range []string{"zone-a", "zone-b", "zone-c"} {
		expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
			"level":  level.DebugValue(),
			"msg":    "starting requests to zone",
			"reason": "initial requests",
			"zone":   zone,
		})
	}

	require.ElementsMatch(t, expectedLogMessages, logger.messages)
}

func TestZoneAwareResultTracker_StartMinimumRequests_NoFailingRequests(t *testing.T) {
	t.Parallel()

	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6}

	zoneRequestCounts := map[string]int{"zone-a": 0, "zone-b": 0, "zone-c": 0}

	for testIteration := 0; testIteration < 900; testIteration++ {
		logger := &testLogger{}
		tracker := newZoneAwareResultTracker(instances, 1, nil, logger)
		tracker.startMinimumRequests()

		mtx := sync.RWMutex{}
		instancesAwaitReleaseResults := make(map[*InstanceDesc]error, len(instances))
		instancesReleased := make([]*InstanceDesc, 0, len(instances))

		for instanceIdx := range instances {
			instanceIdx := instanceIdx
			instance := &instances[instanceIdx]
			go func() {
				released := tracker.awaitStart(context.Background(), instance)

				mtx.Lock()
				defer mtx.Unlock()
				instancesAwaitReleaseResults[instance] = released
				instancesReleased = append(instancesReleased, instance)
			}()
		}

		require.Eventually(t, func() bool {
			mtx.RLock()
			defer mtx.RUnlock()

			return len(instancesReleased) == 4
		}, 1*time.Second, 10*time.Millisecond, "expected four instances to be released")

		require.Equal(t, 4, nilErrorCount(instancesAwaitReleaseResults), "expected the four instances to be signalled to start immediately")
		require.Equal(t, 2, uniqueZoneCount(instancesReleased), "expected two zones to be released initially")

		_, zoneAReleased := instancesAwaitReleaseResults[&instances[0]]
		_, zoneBReleased := instancesAwaitReleaseResults[&instances[2]]
		_, zoneCReleased := instancesAwaitReleaseResults[&instances[4]]
		expectedLogMessages := []map[interface{}]interface{}{}

		if zoneAReleased {
			zoneRequestCounts["zone-a"]++
			tracker.done(&instance1, nil)
			tracker.done(&instance2, nil)

			expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
				"level":  level.DebugValue(),
				"msg":    "starting requests to zone",
				"reason": "initial requests",
				"zone":   "zone-a",
			})
		}

		if zoneBReleased {
			zoneRequestCounts["zone-b"]++
			tracker.done(&instance3, nil)
			tracker.done(&instance4, nil)

			expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
				"level":  level.DebugValue(),
				"msg":    "starting requests to zone",
				"reason": "initial requests",
				"zone":   "zone-b",
			})
		}

		if zoneCReleased {
			zoneRequestCounts["zone-c"]++
			tracker.done(&instance5, nil)
			tracker.done(&instance6, nil)

			expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
				"level":  level.DebugValue(),
				"msg":    "starting requests to zone",
				"reason": "initial requests",
				"zone":   "zone-c",
			})
		}

		require.True(t, tracker.succeeded())

		require.Eventually(t, func() bool {
			mtx.RLock()
			defer mtx.RUnlock()

			return len(instancesReleased) == 6
		}, 1*time.Second, 10*time.Millisecond, "expected the final requests to be released")

		require.Equal(t, 4, nilErrorCount(instancesAwaitReleaseResults), "expected the final requests to not be signalled to start")

		require.ElementsMatch(t, expectedLogMessages, logger.messages)
	}

	// With 900 iterations, 3 zones and max 1 failing zone, we'd expect each zone to receive
	// 600 calls each (each zone has a 2-in-3 chance of being called in each iteration).
	const expectedAverageCallsPerZone = 600
	const tolerancePerc = 0.2
	for _, zoneRequestCount := range zoneRequestCounts {
		require.InDeltaf(t, expectedAverageCallsPerZone, zoneRequestCount, expectedAverageCallsPerZone*tolerancePerc, "expected roughly even distribution of requests across all zones, but got %v", zoneRequestCount)
	}
}

func TestZoneAwareResultTracker_StartMinimumRequests_FailingZonesLessThanMaximumAllowed_SingleFailingRequestInZone(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instance7 := InstanceDesc{Addr: "127.0.0.7", Zone: "zone-d"}
	instance8 := InstanceDesc{Addr: "127.0.0.8", Zone: "zone-d"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6, instance7, instance8}

	logger := &testLogger{}
	tracker := newZoneAwareResultTracker(instances, 2, nil, logger)
	tracker.startMinimumRequests()

	mtx := sync.RWMutex{}
	instancesAwaitReleaseResults := make(map[*InstanceDesc]error, len(instances))
	instancesReleased := make([]*InstanceDesc, 0, len(instances))
	for instanceIdx := range instances {
		instanceIdx := instanceIdx
		instance := &instances[instanceIdx]
		go func() {
			released := tracker.awaitStart(context.Background(), instance)

			mtx.Lock()
			defer mtx.Unlock()
			instancesAwaitReleaseResults[instance] = released
			instancesReleased = append(instancesReleased, instance)
		}()
	}

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return len(instancesReleased) == 4
	}, 1*time.Second, 10*time.Millisecond, "expected four instances to be released initially")

	require.Equal(t, 4, nilErrorCount(instancesAwaitReleaseResults), "expected all four instances to be signalled to start")
	require.Equal(t, 2, uniqueZoneCount(instancesReleased), "expected two zones to be released initially")
	expectedLogMessages := []map[interface{}]interface{}{}

	for _, zone := range uniqueZones(instancesReleased) {
		expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
			"level":  level.DebugValue(),
			"msg":    "starting requests to zone",
			"reason": "initial requests",
			"zone":   zone,
		})
	}

	require.ElementsMatch(t, expectedLogMessages, logger.messages)

	// Simulate one request failing and check that another zone is released.
	tracker.done(instancesReleased[0], errors.New("something went wrong"))

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()
		return len(instancesReleased) == 6
	}, 1*time.Second, 10*time.Millisecond, "expected an additional two instances to be released after a failed request in one zone")

	require.Equal(t, 6, nilErrorCount(instancesAwaitReleaseResults), "expected all six instances to be signalled to start")
	require.Equal(t, 3, uniqueZoneCount(instancesReleased), "expected three zones to be released after one failed")

	expectedLogMessages = append(expectedLogMessages,
		map[interface{}]interface{}{
			"level":               level.WarnValue(),
			"msg":                 "request to instance has failed, zone cannot contribute to quorum",
			"zone":                instancesReleased[0].Zone,
			"failingInstanceAddr": instancesReleased[0].Addr,
			"failingInstanceID":   instancesReleased[0].Id,
			"err":                 errors.New("something went wrong"),
		},
		map[interface{}]interface{}{
			"level":  level.DebugValue(),
			"msg":    "starting requests to zone",
			"reason": "failure of other zone",
			"zone":   instancesReleased[4].Zone,
		},
	)

	require.ElementsMatch(t, expectedLogMessages, logger.messages)

	// Simulate the remaining requests succeeding and check that the last zone is signalled not to start.
	for _, i := range instancesReleased[1:] {
		tracker.done(i, nil)
	}

	require.True(t, tracker.succeeded())

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()
		return len(instancesReleased) == 8
	}, 1*time.Second, 10*time.Millisecond, "expected remaining instances to be released")

	require.Equal(t, 6, nilErrorCount(instancesAwaitReleaseResults), "expected remaining instances to not be signalled to start")
	require.ElementsMatch(t, expectedLogMessages, logger.messages)
}

func TestZoneAwareResultTracker_StartMinimumRequests_FailingZonesLessThanMaximumAllowed_MultipleFailingRequestsInSingleZone(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instance7 := InstanceDesc{Addr: "127.0.0.7", Zone: "zone-d"}
	instance8 := InstanceDesc{Addr: "127.0.0.8", Zone: "zone-d"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6, instance7, instance8}

	tracker := newZoneAwareResultTracker(instances, 2, nil, log.NewNopLogger())
	tracker.startMinimumRequests()

	mtx := sync.RWMutex{}
	instancesAwaitReleaseResults := make(map[*InstanceDesc]error, len(instances))
	instancesReleased := make([]*InstanceDesc, 0, len(instances))
	reportingSecondFailure := false
	instanceReleasedWhileReportingSecondFailure := false

	for instanceIdx := range instances {
		instanceIdx := instanceIdx
		instance := &instances[instanceIdx]
		go func() {
			released := tracker.awaitStart(context.Background(), instance)

			mtx.Lock()
			defer mtx.Unlock()
			instancesAwaitReleaseResults[instance] = released
			instancesReleased = append(instancesReleased, instance)

			if reportingSecondFailure {
				instanceReleasedWhileReportingSecondFailure = true
			}
		}()
	}

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return len(instancesReleased) == 4
	}, 1*time.Second, 10*time.Millisecond, "expected four instances to be released initially")

	require.Equal(t, 4, nilErrorCount(instancesAwaitReleaseResults), "expected all four instances to be signalled to start")
	require.Equal(t, 2, uniqueZoneCount(instancesReleased), "expected two zones to be released initially")

	// Simulate one request failing and check that another zone is released.
	failingInstance := instancesReleased[0]
	tracker.done(failingInstance, errors.New("something went wrong"))

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()
		return len(instancesReleased) == 6
	}, 1*time.Second, 10*time.Millisecond, "expected an additional two instances to be released after a failed request in one zone")

	require.Equal(t, 6, nilErrorCount(instancesAwaitReleaseResults), "expected all six instances to be signalled to start")
	require.Equal(t, 3, uniqueZoneCount(instancesReleased), "expected three zones to be released after one failed")

	// Simulate the other request in the same zone as the previous failing request also failing, and check that no further zones are released.
	var otherInstanceInFailingZone *InstanceDesc

	for _, i := range instancesReleased[1:] {
		if i.Zone == failingInstance.Zone {
			otherInstanceInFailingZone = i
			break
		}
	}

	mtx.Lock()
	reportingSecondFailure = true
	mtx.Unlock()
	tracker.done(otherInstanceInFailingZone, errors.New("something else went wrong"))

	// Wait a little to make sure no more requests were released.
	require.Never(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return instanceReleasedWhileReportingSecondFailure
	}, 500*time.Millisecond, 10*time.Millisecond, "expected no instances to be released after reporting a second failure in the same zone")

	mtx.Lock()
	reportingSecondFailure = false
	mtx.Unlock()

	// Simulate the remaining requests succeeding and check that the last zone is signalled not to start.
	for _, i := range instancesReleased {
		if i != failingInstance && i != otherInstanceInFailingZone {
			tracker.done(i, nil)
		}
	}

	require.True(t, tracker.succeeded())

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()
		return len(instancesReleased) == 8
	}, 1*time.Second, 10*time.Millisecond, "expected remaining instances to be released")

	require.Equal(t, 6, nilErrorCount(instancesAwaitReleaseResults), "expected remaining instances to not be signalled to start")
}

func TestZoneAwareResultTracker_StartMinimumRequests_FailingZonesEqualToMaximumAllowed(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instance7 := InstanceDesc{Addr: "127.0.0.7", Zone: "zone-d"}
	instance8 := InstanceDesc{Addr: "127.0.0.8", Zone: "zone-d"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6, instance7, instance8}

	tracker := newZoneAwareResultTracker(instances, 2, nil, log.NewNopLogger())
	tracker.startMinimumRequests()

	mtx := sync.RWMutex{}
	instancesAwaitReleaseResults := make(map[*InstanceDesc]error, len(instances))
	instancesReleased := make([]*InstanceDesc, 0, len(instances))
	for instanceIdx := range instances {
		instanceIdx := instanceIdx
		instance := &instances[instanceIdx]
		go func() {
			released := tracker.awaitStart(context.Background(), instance)

			mtx.Lock()
			defer mtx.Unlock()
			instancesAwaitReleaseResults[instance] = released
			instancesReleased = append(instancesReleased, instance)
		}()
	}

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return len(instancesReleased) == 4
	}, 1*time.Second, 10*time.Millisecond, "expected four instances to be released initially")

	require.Equal(t, 4, nilErrorCount(instancesAwaitReleaseResults), "expected all four instances to be signalled to start")
	require.Equal(t, 2, uniqueZoneCount(instancesReleased), "expected two zones to be released initially")

	// Simulate one request failing and check that another zone is released.
	firstFailingInstance := instancesReleased[0]
	tracker.done(firstFailingInstance, errors.New("something went wrong"))

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()
		return len(instancesReleased) == 6
	}, 1*time.Second, 10*time.Millisecond, "expected an additional two instances to be released after a failed request in one zone")

	require.Equal(t, 6, nilErrorCount(instancesAwaitReleaseResults), "expected all six instances to be signalled to start")
	require.Equal(t, 3, uniqueZoneCount(instancesReleased), "expected three zones to be released after one failed")

	// Simulate another request from another zone failing and check that another zone is released.
	for _, i := range instancesReleased {
		if i.Zone != firstFailingInstance.Zone {
			tracker.done(i, errors.New("something else went wrong"))
			break
		}
	}

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()
		return len(instancesReleased) == 8
	}, 1*time.Second, 10*time.Millisecond, "expected an additional two instances to be released after a failed request in a second zone")

	require.Equal(t, 8, nilErrorCount(instancesAwaitReleaseResults), "expected all eight instances to be signalled to start")
	require.Equal(t, 4, uniqueZoneCount(instancesReleased), "expected four zones to be released after two failed")
}

func TestZoneAwareResultTracker_StartMinimumRequests_FailingZonesGreaterThanMaximumAllowed(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instance7 := InstanceDesc{Addr: "127.0.0.7", Zone: "zone-d"}
	instance8 := InstanceDesc{Addr: "127.0.0.8", Zone: "zone-d"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6, instance7, instance8}

	tracker := newZoneAwareResultTracker(instances, 2, nil, log.NewNopLogger())
	tracker.startMinimumRequests()

	mtx := sync.RWMutex{}
	instancesAwaitReleaseResults := make(map[*InstanceDesc]error, len(instances))
	instancesReleased := make([]*InstanceDesc, 0, len(instances))
	for instanceIdx := range instances {
		instanceIdx := instanceIdx
		instance := &instances[instanceIdx]
		go func() {
			released := tracker.awaitStart(context.Background(), instance)

			mtx.Lock()
			defer mtx.Unlock()
			instancesAwaitReleaseResults[instance] = released
			instancesReleased = append(instancesReleased, instance)
		}()
	}

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return len(instancesReleased) == 4
	}, 1*time.Second, 10*time.Millisecond, "expected four instances to be released initially")

	require.Equal(t, 4, nilErrorCount(instancesAwaitReleaseResults), "expected all four instances to be signalled to start")
	require.Equal(t, 2, uniqueZoneCount(instancesReleased), "expected two zones to be released initially")

	// Simulate both initial zones failing and check that the remaining two zones are released.
	for _, i := range instancesReleased {
		tracker.done(i, errors.New("something went wrong"))
	}

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return len(instancesReleased) == 8
	}, 1*time.Second, 10*time.Millisecond, "expected all instances to be released after initial two zone failures")

	require.Equal(t, 8, nilErrorCount(instancesAwaitReleaseResults), "expected all instances to be signalled to start")

	// Simulate one more zone failing.
	tracker.done(instancesReleased[4], errors.New("something else went wrong"))

	require.True(t, tracker.failed())
}

func TestZoneAwareResultTracker_StartMinimumRequests_MaxUnavailableZonesIsNumberOfZones(t *testing.T) {
	// This scenario should never happen in the real world, but if we were to get into this situation,
	// we need to make sure we don't end up blocking forever, which could lead to leaking a goroutine in DoUntilQuorumWithoutSuccessfulContextCancellation.

	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instances := []InstanceDesc{instance1, instance2}
	tracker := newZoneAwareResultTracker(instances, 1, nil, log.NewNopLogger())
	tracker.startMinimumRequests()

	wg := sync.WaitGroup{}
	wg.Add(2)
	done := make(chan struct{})

	for i := range instances {
		i := i
		instance := &instances[i]
		go func() {
			err := tracker.awaitStart(context.Background(), instance)
			require.Equal(t, errResultNotNeeded, err)
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Nothing more to do here.
	case <-time.After(time.Second):
		require.Fail(t, "gave up waiting for requests to be released")
	}
}

func TestZoneAwareResultTracker_StartMinimumRequests_ZoneSorting(t *testing.T) {
	zoneAInstance := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	zoneBInstance := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-b"}
	zoneCInstance := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-c"}
	zoneDInstance := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-d"}
	instances := []InstanceDesc{zoneAInstance, zoneBInstance, zoneCInstance, zoneDInstance}
	mtx := sync.Mutex{}
	released := []bool{false, false, false, false}

	zoneSorter := func(zones []string) []string {
		return []string{"zone-c", "zone-a", "zone-d", "zone-b"}
	}

	tracker := newZoneAwareResultTracker(instances, 2, zoneSorter, log.NewNopLogger())
	tracker.startMinimumRequests()

	for i := range instances {
		i := i
		instance := &instances[i]
		go func() {
			err := tracker.awaitStart(context.Background(), instance)
			require.NoError(t, err)
			mtx.Lock()
			released[i] = true
			mtx.Unlock()
		}()
	}

	requireZonesReleased := func(expected []bool, msg string) {
		require.Eventually(t, func() bool {
			mtx.Lock()
			defer mtx.Unlock()

			for i, e := range expected {
				if released[i] != e {
					return false
				}
			}

			return true
		}, time.Second, 10*time.Millisecond, msg+", zones released are: ", released)
	}

	requireZonesReleased([]bool{true, false, true, false}, "first two zones (A and C) should be released immediately")

	tracker.done(&instances[0], errors.New("zone A failed"))
	requireZonesReleased([]bool{true, false, true, true}, "third zone (D) should be released after first failure")

	tracker.done(&instances[3], errors.New("zone D failed"))
	requireZonesReleased([]bool{true, true, true, true}, "final zone (C) should be released after first failure")
}

func TestZoneAwareResultTracker_StartAdditionalRequests(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instance7 := InstanceDesc{Addr: "127.0.0.7", Zone: "zone-d"}
	instance8 := InstanceDesc{Addr: "127.0.0.8", Zone: "zone-d"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6, instance7, instance8}

	logger := &testLogger{}
	tracker := newZoneAwareResultTracker(instances, 2, nil, logger)
	tracker.startMinimumRequests()

	mtx := sync.RWMutex{}
	instancesAwaitReleaseResults := make(map[*InstanceDesc]error, len(instances))
	instancesReleased := make([]*InstanceDesc, 0, len(instances))
	for instanceIdx := range instances {
		instanceIdx := instanceIdx
		instance := &instances[instanceIdx]
		go func() {
			released := tracker.awaitStart(context.Background(), instance)

			mtx.Lock()
			defer mtx.Unlock()
			instancesAwaitReleaseResults[instance] = released
			instancesReleased = append(instancesReleased, instance)
		}()
	}

	waitForZonesReleased := func(expectedZones int, msg string) {
		expectedInstances := 2 * expectedZones
		require.Eventually(t, func() bool {
			mtx.RLock()
			defer mtx.RUnlock()

			return len(instancesReleased) == expectedInstances
		}, time.Second, 10*time.Millisecond, msg)

		require.Equal(t, expectedInstances, nilErrorCount(instancesAwaitReleaseResults), "expected all released instances to be signalled to start")
		require.Equal(t, expectedZones, uniqueZoneCount(instancesReleased), msg)
	}

	waitForZonesReleased(2, "should initially release two zones")
	initialZones := uniqueZones(instancesReleased)
	expectedLogMessages := []map[interface{}]interface{}{
		{
			"level":  level.DebugValue(),
			"msg":    "starting requests to zone",
			"reason": "initial requests",
			"zone":   initialZones[0],
		},
		{
			"level":  level.DebugValue(),
			"msg":    "starting requests to zone",
			"reason": "initial requests",
			"zone":   initialZones[1],
		},
	}
	require.ElementsMatch(t, expectedLogMessages, logger.messages)

	tracker.startAdditionalRequests()
	waitForZonesReleased(3, "should release a third zone after startAdditionalRequests()")
	expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
		"level":  level.DebugValue(),
		"msg":    "starting requests to zone",
		"reason": "hedging",
		"zone":   instancesReleased[4].Zone,
	})
	require.ElementsMatch(t, expectedLogMessages, logger.messages)

	tracker.startAdditionalRequests()
	waitForZonesReleased(4, "should release remaining zone after startAdditionalRequests()")
	expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
		"level":  level.DebugValue(),
		"msg":    "starting requests to zone",
		"reason": "hedging",
		"zone":   instancesReleased[6].Zone,
	})
	require.ElementsMatch(t, expectedLogMessages, logger.messages)
}

func TestZoneAwareContextTracker(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6}

	parentCtx := context.WithValue(context.Background(), testContextKey, "this-is-the-value-from-the-parent")
	tracker := newZoneAwareContextTracker(parentCtx, instances)

	instance1Ctx, _ := tracker.contextFor(&instances[0])
	instance2Ctx, _ := tracker.contextFor(&instances[1])
	instance3Ctx, instance3Cancel := tracker.contextFor(&instances[2])
	instance4Ctx, _ := tracker.contextFor(&instances[3])
	instance5Ctx, _ := tracker.contextFor(&instances[4])
	instance6Ctx, _ := tracker.contextFor(&instances[5])

	for _, ctx := range []context.Context{instance1Ctx, instance2Ctx, instance3Ctx, instance4Ctx, instance5Ctx, instance6Ctx} {
		require.Equal(t, "this-is-the-value-from-the-parent", ctx.Value(testContextKey), "context for instance should inherit from provided parent context")
		require.NoError(t, ctx.Err(), "context for instance should not be cancelled")
	}

	// Cancel a context for one instance using cancelContextFor and check that the other context in its zone is cancelled, but others are
	// unaffected.
	instance1Cause := cancellation.NewErrorf("instance 1 cancellation cause")
	tracker.cancelContextFor(&instance1, instance1Cause)
	require.Equal(t, context.Canceled, instance1Ctx.Err(), "instance context should be cancelled")
	require.Equal(t, context.Canceled, instance2Ctx.Err(), "context for instance in same zone as cancelled instance should also be cancelled")
	require.Equal(t, instance1Cause, context.Cause(instance1Ctx))
	require.Equal(t, instance1Cause, context.Cause(instance2Ctx))
	for _, ctx := range []context.Context{instance3Ctx, instance4Ctx, instance5Ctx, instance6Ctx} {
		require.NoError(t, ctx.Err(), "context for instance should not be cancelled after cancelling the context of another instance")
	}

	// Cancel a context for one instance using the cancellation function provided and check that the other context in its zone is NOT cancelled.
	instance3Cause := cancellation.NewErrorf("instance 3 cancellation cause")
	instance3Cancel(instance3Cause)
	for _, ctx := range []context.Context{instance4Ctx, instance5Ctx, instance6Ctx} {
		require.NoError(t, ctx.Err(), "context for instance should not be cancelled after cancelling the context of another instance")
	}

	remainingInstancesCause := cancellation.NewErrorf("remaining instances cancellation cause")
	tracker.cancelAllContexts(remainingInstancesCause)
	for _, ctx := range []context.Context{instance4Ctx, instance5Ctx, instance6Ctx} {
		require.Equal(t, context.Canceled, ctx.Err(), "context for instance should be cancelled after cancelling all contexts")
		require.Equal(t, remainingInstancesCause, context.Cause(ctx))
	}
}

func TestInflightInstanceTracker(t *testing.T) {
	sets := []ReplicationSet{
		{Instances: []InstanceDesc{{Id: "instance-1"}, {Id: "instance-2"}}},
		{Instances: []InstanceDesc{{Id: "instance-3"}}},
	}

	t.Run("addInstance() should be idempotent", func(t *testing.T) {
		inflight := newInflightInstanceTracker(sets)
		inflight.addInstance(0, &(sets[0].Instances[0]))
		inflight.addInstance(0, &(sets[0].Instances[0]))

		inflight.allInstancesAdded()
		assert.False(t, inflight.allInstancesCompleted())

		inflight.removeInstance(0, &(sets[0].Instances[0]))
		assert.True(t, inflight.allInstancesCompleted())
	})

	t.Run("removeInstance() should be idempotent", func(t *testing.T) {
		inflight := newInflightInstanceTracker(sets)
		inflight.addInstance(0, &(sets[0].Instances[0]))

		inflight.allInstancesAdded()
		assert.False(t, inflight.allInstancesCompleted())

		for i := 0; i < 2; i++ {
			inflight.removeInstance(0, &(sets[0].Instances[0]))
			assert.True(t, inflight.allInstancesCompleted())
		}
	})

	t.Run("allInstancesCompleted() should return true after all instances have been removed and allInstancesAdded() has been called", func(t *testing.T) {
		inflight := newInflightInstanceTracker(sets)
		inflight.addInstance(0, &(sets[0].Instances[0]))
		inflight.addInstance(0, &(sets[0].Instances[1]))
		inflight.addInstance(0, &(sets[1].Instances[0]))

		inflight.allInstancesAdded()
		assert.False(t, inflight.allInstancesCompleted())

		inflight.removeInstance(0, &(sets[0].Instances[0]))
		assert.False(t, inflight.allInstancesCompleted())

		inflight.removeInstance(0, &(sets[0].Instances[1]))
		assert.False(t, inflight.allInstancesCompleted())

		inflight.removeInstance(0, &(sets[1].Instances[0]))
		assert.True(t, inflight.allInstancesCompleted())
	})

	t.Run("allInstancesCompleted() should return false if all instances have been removed but allInstancesAdded() has been not called yet", func(t *testing.T) {
		inflight := newInflightInstanceTracker(sets)
		inflight.addInstance(0, &(sets[0].Instances[0]))
		inflight.addInstance(0, &(sets[0].Instances[1]))
		inflight.addInstance(0, &(sets[1].Instances[0]))
		assert.False(t, inflight.allInstancesCompleted())

		inflight.removeInstance(0, &(sets[0].Instances[0]))
		assert.False(t, inflight.allInstancesCompleted())

		inflight.removeInstance(0, &(sets[0].Instances[1]))
		assert.False(t, inflight.allInstancesCompleted())

		inflight.removeInstance(0, &(sets[1].Instances[0]))
		assert.False(t, inflight.allInstancesCompleted())

		inflight.allInstancesAdded()
		assert.True(t, inflight.allInstancesCompleted())
	})
}

func BenchmarkInflightInstanceTracker(b *testing.B) {
	tests := map[string]struct {
		numReplicationSets            int
		numInstancesPerReplicationSet int
	}{
		"few replication sets, many instances per replication set": {
			numReplicationSets:            3,
			numInstancesPerReplicationSet: 100,
		},
		"many replication sets, few instances per replication set": {
			numReplicationSets:            100,
			numInstancesPerReplicationSet: 3,
		},
	}

	for testName, testData := range tests {
		b.Run(testName, func(b *testing.B) {
			// Create the sets used in the benchmark.
			sets := make([]ReplicationSet, 0, testData.numReplicationSets)
			for i := 0; i < testData.numReplicationSets; i++ {
				instances := make([]InstanceDesc, 0, testData.numInstancesPerReplicationSet)
				for j := 0; j < testData.numInstancesPerReplicationSet; j++ {
					instances = append(instances, InstanceDesc{Id: strconv.Itoa(j)})
				}

				sets = append(sets, ReplicationSet{Instances: instances})
			}

			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				inflight := newInflightInstanceTracker(sets)

				// Signal that all instances have been added even if it's not true just to
				// exercise more code in allInstancesCompleted() during the benchmark.
				inflight.allInstancesAdded()

				// First add all instances, and check if all instances completed.
				for setIdx, set := range sets {
					for instanceIdx := range set.Instances {
						inflight.addInstance(setIdx, &(set.Instances[instanceIdx]))
						inflight.allInstancesCompleted()
					}
				}

				// Then remove all instances, and check if all instances completed.
				for setIdx, set := range sets {
					for instanceIdx := range set.Instances {
						inflight.removeInstance(setIdx, &(set.Instances[instanceIdx]))
						inflight.allInstancesCompleted()
					}
				}
			}
		})
	}
}

func uniqueZones(instances []*InstanceDesc) []string {
	zones := map[string]struct{}{}

	for _, i := range instances {
		zones[i.Zone] = struct{}{}
	}

	l := make([]string, 0, len(zones))

	for zone := range zones {
		l = append(l, zone)
	}

	return l
}

func uniqueZoneCount(instances []*InstanceDesc) int {
	return len(uniqueZones(instances))
}

func nilErrorCount(l map[*InstanceDesc]error) int {
	count := 0

	for _, err := range l {
		if err == nil {
			count++
		}
	}

	return count
}
