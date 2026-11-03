package ring

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/grafana/dskit/cancellation"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			testCase.run(t, newDefaultResultTracker(testCase.instances, testCase.maxErrors, nil, log.NewNopLogger()))
		})
	}
}

// startedInstances records the instances a result tracker starts requests for, in order.
type startedInstances []*InstanceDesc

func (s *startedInstances) onStart(instance *InstanceDesc) {
	*s = append(*s, instance)
}

func TestDefaultResultTracker_StartAllRequests(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Id: "instance-1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Id: "instance-2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Id: "instance-3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Id: "instance-4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	logger := &testLogger{}
	var started startedInstances
	tracker := newDefaultResultTracker(instances, 1, started.onStart, logger)

	tracker.startAllRequests()
	expectedStarted := []*InstanceDesc{}
	expectedLogMessages := []map[interface{}]interface{}{}

	for i := range instances {
		instance := &instances[i]
		expectedStarted = append(expectedStarted, instance)
		expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
			"level":        level.DebugValue(),
			"msg":          "starting request to instance",
			"reason":       "initial requests",
			"instanceAddr": instance.Addr,
			"instanceID":   instance.Id,
		})
	}

	require.ElementsMatch(t, expectedStarted, started, "requests for all instances should be started immediately")
	require.ElementsMatch(t, expectedLogMessages, logger.messages)
}

func TestDefaultResultTracker_StartMinimumRequests_NoFailingRequests(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Id: "instance-1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Id: "instance-2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Id: "instance-3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Id: "instance-4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	instanceRequestCounts := make(map[*InstanceDesc]int, len(instances))

	const iterations = 1000
	for range iterations {
		logger := &testLogger{}
		var started startedInstances
		tracker := newDefaultResultTracker(instances, 1, started.onStart, logger)
		tracker.startMinimumRequests()

		require.Len(t, started, 3, "expected three of the four requests to be started")

		var expectedLogMessages []map[interface{}]interface{}

		for _, instance := range started {
			instanceRequestCounts[instance]++
			expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
				"level":        level.DebugValue(),
				"msg":          "starting request to instance",
				"reason":       "initial requests",
				"instanceAddr": instance.Addr,
				"instanceID":   instance.Id,
			})
		}

		// Signal that the three started requests have completed successfully.
		for _, instance := range started {
			tracker.done(instance, nil)
		}

		require.True(t, tracker.succeeded())
		require.Len(t, started, 3, "expected the final request to not be started")
		require.ElementsMatch(t, expectedLogMessages, logger.messages)
	}

	// With 1000 iterations, 4 instances and max 1 error, we'd expect each instance to receive
	// 750 calls each (each instance has a 3-in-4 chance of being called in each iteration).
	const expectedAverageCallsPerInstance = 750
	const tolerancePerc = 0.2
	for i := range instances {
		require.InDeltaf(t, expectedAverageCallsPerInstance, instanceRequestCounts[&instances[i]], expectedAverageCallsPerInstance*tolerancePerc, "expected roughly even distribution of requests across all instances, but got %v", instanceRequestCounts)
	}
}

func TestDefaultResultTracker_StartMinimumRequests_FailingRequestsBelowMaximumAllowed(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Id: "instance-1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Id: "instance-2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Id: "instance-3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Id: "instance-4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	logger := &testLogger{}
	var started startedInstances
	tracker := newDefaultResultTracker(instances, 2, started.onStart, logger)
	tracker.startMinimumRequests()

	require.Len(t, started, 2, "expected two requests to be started initially")

	// Mark the second request as failed, and check that another request is started.
	tracker.done(started[1], errors.New("something went wrong"))
	require.Len(t, started, 3, "expected a third request to be started after a failure")

	// Mark the remaining two requests as successful.
	tracker.done(started[0], nil)
	tracker.done(started[2], nil)

	require.Len(t, started, 3, "final request should not be started")
	require.True(t, tracker.succeeded(), "overall request should succeed")

	var expectedLogMessages []map[interface{}]interface{}

	for _, instance := range started[0:2] {
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
			"instanceAddr": started[1].Addr,
			"instanceID":   started[1].Id,
			"err":          errors.New("something went wrong"),
		},
		map[interface{}]interface{}{
			"level":        level.DebugValue(),
			"msg":          "starting request to instance",
			"reason":       "failure of other instance",
			"instanceAddr": started[2].Addr,
			"instanceID":   started[2].Id,
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

	var started startedInstances
	tracker := newDefaultResultTracker(instances, 2, started.onStart, log.NewNopLogger())
	tracker.startMinimumRequests()

	require.Len(t, started, 2, "expected two requests to be started initially")

	// Mark the first two requests as failed.
	tracker.done(started[0], errors.New("something went wrong"))
	tracker.done(started[1], errors.New("something went wrong"))

	require.Len(t, started, 4, "expected all four requests to be started")

	// Mark the remaining requests as successful.
	tracker.done(started[2], nil)
	tracker.done(started[3], nil)

	require.True(t, tracker.succeeded(), "overall request should succeed")
}

func TestDefaultResultTracker_StartMinimumRequests_MoreFailingRequestsThanMaximumAllowed(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	var started startedInstances
	tracker := newDefaultResultTracker(instances, 1, started.onStart, log.NewNopLogger())
	tracker.startMinimumRequests()

	require.Len(t, started, 3, "expected three requests to be started initially")

	// Mark the first two requests as failed.
	tracker.done(started[0], errors.New("something went wrong"))
	require.Len(t, started, 4, "expected the final request to be started after a failure")
	tracker.done(started[1], errors.New("something went wrong"))

	// Mark the remaining requests as successful.
	tracker.done(started[2], nil)
	tracker.done(started[3], nil)

	require.True(t, tracker.failed(), "overall request should fail")
}

func TestDefaultResultTracker_StartMinimumRequests_MaxErrorsIsNumberOfInstances(t *testing.T) {
	// This scenario should never happen in the real world, but if we were to get into this situation,
	// no request is needed to reach quorum, so none should be started.

	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instances := []InstanceDesc{instance1}
	var started startedInstances
	tracker := newDefaultResultTracker(instances, 1, started.onStart, log.NewNopLogger())
	tracker.startMinimumRequests()

	require.Empty(t, started, "should not start request not required for quorum")
	require.True(t, tracker.succeeded())
}

func TestDefaultResultTracker_StartAdditionalRequests(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Id: "instance-1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Id: "instance-2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Id: "instance-3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Id: "instance-4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	logger := &testLogger{}
	var started startedInstances
	tracker := newDefaultResultTracker(instances, 2, started.onStart, logger)
	tracker.startMinimumRequests()

	require.Len(t, started, 2, "should initially start two requests")
	expectedLogMessages := []map[interface{}]interface{}{
		{
			"level":        level.DebugValue(),
			"msg":          "starting request to instance",
			"reason":       "initial requests",
			"instanceAddr": started[0].Addr,
			"instanceID":   started[0].Id,
		},
		{
			"level":        level.DebugValue(),
			"msg":          "starting request to instance",
			"reason":       "initial requests",
			"instanceAddr": started[1].Addr,
			"instanceID":   started[1].Id,
		},
	}
	require.ElementsMatch(t, expectedLogMessages, logger.messages)

	tracker.startAdditionalRequests()
	require.Len(t, started, 3, "should start a third request after startAdditionalRequests()")
	expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
		"level":        level.DebugValue(),
		"msg":          "starting request to instance",
		"reason":       "hedging",
		"instanceAddr": started[2].Addr,
		"instanceID":   started[2].Id,
	})
	require.ElementsMatch(t, expectedLogMessages, logger.messages)

	tracker.startAdditionalRequests()
	require.Len(t, started, 4, "should start remaining request after startAdditionalRequests()")
	expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
		"level":        level.DebugValue(),
		"msg":          "starting request to instance",
		"reason":       "hedging",
		"instanceAddr": started[3].Addr,
		"instanceID":   started[3].Id,
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
			testCase.run(t, newZoneAwareResultTracker(testCase.instances, testCase.maxUnavailableZones, nil, nil, log.NewNopLogger()))
		})
	}
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
	var started startedInstances
	tracker := newZoneAwareResultTracker(instances, 1, nil, started.onStart, logger)

	tracker.startAllRequests()

	expectedStarted := []*InstanceDesc{}
	for i := range instances {
		expectedStarted = append(expectedStarted, &instances[i])
	}
	require.ElementsMatch(t, expectedStarted, started, "requests for all instances should be started immediately")

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
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6}

	zoneRequestCounts := map[string]int{}

	const iterations = 900
	for range iterations {
		logger := &testLogger{}
		var started startedInstances
		tracker := newZoneAwareResultTracker(instances, 1, nil, started.onStart, logger)
		tracker.startMinimumRequests()

		require.Len(t, started, 4, "expected four instances to be started")
		require.Equal(t, 2, uniqueZoneCount(started), "expected two zones to be started initially")

		expectedLogMessages := []map[interface{}]interface{}{}

		for _, zone := range uniqueZones(started) {
			zoneRequestCounts[zone]++
			expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
				"level":  level.DebugValue(),
				"msg":    "starting requests to zone",
				"reason": "initial requests",
				"zone":   zone,
			})
		}

		for _, instance := range started {
			tracker.done(instance, nil)
		}

		require.True(t, tracker.succeeded())
		require.Len(t, started, 4, "expected the final zone to not be started")
		require.ElementsMatch(t, expectedLogMessages, logger.messages)
	}

	// With 900 iterations, 3 zones and max 1 failing zone, we'd expect each zone to receive
	// 600 calls each (each zone has a 2-in-3 chance of being called in each iteration).
	const expectedAverageCallsPerZone = 600
	const tolerancePerc = 0.2
	for _, zone := range []string{"zone-a", "zone-b", "zone-c"} {
		require.InDeltaf(t, expectedAverageCallsPerZone, zoneRequestCounts[zone], expectedAverageCallsPerZone*tolerancePerc, "expected roughly even distribution of requests across all zones, but got %v", zoneRequestCounts)
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
	var started startedInstances
	tracker := newZoneAwareResultTracker(instances, 2, nil, started.onStart, logger)
	tracker.startMinimumRequests()

	require.Len(t, started, 4, "expected four instances to be started initially")
	require.Equal(t, 2, uniqueZoneCount(started), "expected two zones to be started initially")
	expectedLogMessages := []map[interface{}]interface{}{}

	for _, zone := range uniqueZones(started) {
		expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
			"level":  level.DebugValue(),
			"msg":    "starting requests to zone",
			"reason": "initial requests",
			"zone":   zone,
		})
	}

	require.ElementsMatch(t, expectedLogMessages, logger.messages)

	// Simulate one request failing and check that another zone is started.
	tracker.done(started[0], errors.New("something went wrong"))

	require.Len(t, started, 6, "expected an additional two instances to be started after a failed request in one zone")
	require.Equal(t, 3, uniqueZoneCount(started), "expected three zones to be started after one failed")

	expectedLogMessages = append(expectedLogMessages,
		map[interface{}]interface{}{
			"level":               level.WarnValue(),
			"msg":                 "request to instance has failed, zone cannot contribute to quorum",
			"zone":                started[0].Zone,
			"failingInstanceAddr": started[0].Addr,
			"failingInstanceID":   started[0].Id,
			"err":                 errors.New("something went wrong"),
		},
		map[interface{}]interface{}{
			"level":  level.DebugValue(),
			"msg":    "starting requests to zone",
			"reason": "failure of other zone",
			"zone":   started[4].Zone,
		},
	)

	require.ElementsMatch(t, expectedLogMessages, logger.messages)

	// Simulate the remaining requests succeeding and check that the last zone is not started.
	for _, i := range started[1:] {
		tracker.done(i, nil)
	}

	require.True(t, tracker.succeeded())
	require.Len(t, started, 6, "expected remaining instances to not be started")
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

	var started startedInstances
	tracker := newZoneAwareResultTracker(instances, 2, nil, started.onStart, log.NewNopLogger())
	tracker.startMinimumRequests()

	require.Len(t, started, 4, "expected four instances to be started initially")
	require.Equal(t, 2, uniqueZoneCount(started), "expected two zones to be started initially")

	// Simulate one request failing and check that another zone is started.
	failingInstance := started[0]
	tracker.done(failingInstance, errors.New("something went wrong"))

	require.Len(t, started, 6, "expected an additional two instances to be started after a failed request in one zone")
	require.Equal(t, 3, uniqueZoneCount(started), "expected three zones to be started after one failed")

	// Simulate the other request in the same zone as the previous failing request also failing, and check that no further zones are started.
	var otherInstanceInFailingZone *InstanceDesc

	for _, i := range started[1:] {
		if i.Zone == failingInstance.Zone {
			otherInstanceInFailingZone = i
			break
		}
	}

	tracker.done(otherInstanceInFailingZone, errors.New("something else went wrong"))
	require.Len(t, started, 6, "expected no instances to be started after reporting a second failure in the same zone")

	// Simulate the remaining requests succeeding and check that the last zone is not started.
	for _, i := range started {
		if i != failingInstance && i != otherInstanceInFailingZone {
			tracker.done(i, nil)
		}
	}

	require.True(t, tracker.succeeded())
	require.Len(t, started, 6, "expected remaining instances to not be started")
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

	var started startedInstances
	tracker := newZoneAwareResultTracker(instances, 2, nil, started.onStart, log.NewNopLogger())
	tracker.startMinimumRequests()

	require.Len(t, started, 4, "expected four instances to be started initially")
	require.Equal(t, 2, uniqueZoneCount(started), "expected two zones to be started initially")

	// Simulate one request failing and check that another zone is started.
	firstFailingInstance := started[0]
	tracker.done(firstFailingInstance, errors.New("something went wrong"))

	require.Len(t, started, 6, "expected an additional two instances to be started after a failed request in one zone")
	require.Equal(t, 3, uniqueZoneCount(started), "expected three zones to be started after one failed")

	// Simulate another request from another zone failing and check that another zone is started.
	for _, i := range started {
		if i.Zone != firstFailingInstance.Zone {
			tracker.done(i, errors.New("something else went wrong"))
			break
		}
	}

	require.Len(t, started, 8, "expected an additional two instances to be started after a failed request in a second zone")
	require.Equal(t, 4, uniqueZoneCount(started), "expected four zones to be started after two failed")
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

	var started startedInstances
	tracker := newZoneAwareResultTracker(instances, 2, nil, started.onStart, log.NewNopLogger())
	tracker.startMinimumRequests()

	require.Len(t, started, 4, "expected four instances to be started initially")
	require.Equal(t, 2, uniqueZoneCount(started), "expected two zones to be started initially")

	// Simulate both initial zones failing and check that the remaining two zones are started.
	for _, i := range slices.Clone(started) {
		tracker.done(i, errors.New("something went wrong"))
	}

	require.Len(t, started, 8, "expected all instances to be started after initial two zone failures")

	// Simulate one more zone failing.
	tracker.done(started[4], errors.New("something else went wrong"))

	require.True(t, tracker.failed())
}

func TestZoneAwareResultTracker_StartMinimumRequests_MaxUnavailableZonesIsNumberOfZones(t *testing.T) {
	// This scenario should never happen in the real world, but if we were to get into this situation,
	// no zone is needed to reach quorum, so none should be started.

	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instances := []InstanceDesc{instance1, instance2}
	var started startedInstances
	tracker := newZoneAwareResultTracker(instances, 1, nil, started.onStart, log.NewNopLogger())
	tracker.startMinimumRequests()

	require.Empty(t, started, "should not start requests not required for quorum")
	require.True(t, tracker.succeeded())
}

func TestZoneAwareResultTracker_StartMinimumRequests_ZoneSorting(t *testing.T) {
	zoneAInstance := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	zoneBInstance := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-b"}
	zoneCInstance := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-c"}
	zoneDInstance := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-d"}
	instances := []InstanceDesc{zoneAInstance, zoneBInstance, zoneCInstance, zoneDInstance}

	zoneSorter := func([]string) []string {
		return []string{"zone-c", "zone-a", "zone-d", "zone-b"}
	}

	// Each zone has one instance, so the zones started are the zones of the instances started, in order.
	var started startedInstances
	zonesInStartOrder := func(started []*InstanceDesc) []string {
		zones := make([]string, 0, len(started))
		for _, instance := range started {
			zones = append(zones, instance.Zone)
		}
		return zones
	}

	tracker := newZoneAwareResultTracker(instances, 2, zoneSorter, started.onStart, log.NewNopLogger())
	tracker.startMinimumRequests()
	require.Equal(t, []string{"zone-c", "zone-a"}, zonesInStartOrder(started), "first two zones (C and A) should be started immediately")

	tracker.done(&instances[0], errors.New("zone A failed"))
	require.Equal(t, []string{"zone-c", "zone-a", "zone-d"}, zonesInStartOrder(started), "third zone (D) should be started after first failure")

	tracker.done(&instances[3], errors.New("zone D failed"))
	require.Equal(t, []string{"zone-c", "zone-a", "zone-d", "zone-b"}, zonesInStartOrder(started), "final zone (B) should be started after second failure")
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
	var started startedInstances
	tracker := newZoneAwareResultTracker(instances, 2, nil, started.onStart, logger)
	tracker.startMinimumRequests()

	requireZonesStarted := func(expectedZones int, msg string) {
		require.Len(t, started, 2*expectedZones, msg)
		require.Equal(t, expectedZones, uniqueZoneCount(started), msg)
	}

	requireZonesStarted(2, "should initially start two zones")
	initialZones := uniqueZones(started)
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
	requireZonesStarted(3, "should start a third zone after startAdditionalRequests()")
	expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
		"level":  level.DebugValue(),
		"msg":    "starting requests to zone",
		"reason": "hedging",
		"zone":   started[4].Zone,
	})
	require.ElementsMatch(t, expectedLogMessages, logger.messages)

	tracker.startAdditionalRequests()
	requireZonesStarted(4, "should start remaining zone after startAdditionalRequests()")
	expectedLogMessages = append(expectedLogMessages, map[interface{}]interface{}{
		"level":  level.DebugValue(),
		"msg":    "starting requests to zone",
		"reason": "hedging",
		"zone":   started[6].Zone,
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
