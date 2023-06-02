package ring

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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
				// Instance 2 failed, and so shouldIncludeResultFrom won't be called by DoUntilQuorum.
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
			testCase.run(t, newDefaultResultTracker(testCase.instances, testCase.maxErrors))
		})
	}
}

func TestDefaultResultTracker_ReleaseAllRequests(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}
	tracker := newDefaultResultTracker(instances, 1)

	tracker.releaseAllRequests()

	for i := range instances {
		instance := &instances[i]
		require.True(t, tracker.awaitRelease(instance), "requests for all instances should be released immediately")
	}
}

func TestDefaultResultTracker_ReleaseMinimumRequests_NoFailingRequests(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	instanceRequestCounts := make([]atomic.Uint64, len(instances))

	for testIteration := 0; testIteration < 1000; testIteration++ {
		tracker := newDefaultResultTracker(instances, 1)
		tracker.releaseMinimumRequests()

		mtx := sync.RWMutex{}
		instancesAwaitReleaseResults := make([]bool, len(instances))
		countInstancesReleased := 0

		for instanceIdx := range instances {
			instanceIdx := instanceIdx
			instance := &instances[instanceIdx]
			go func() {
				released := tracker.awaitRelease(instance)

				mtx.Lock()
				defer mtx.Unlock()
				instancesAwaitReleaseResults[instanceIdx] = released
				countInstancesReleased++

				if released {
					instanceRequestCounts[instanceIdx].Inc()
				}
			}()
		}

		require.Eventually(t, func() bool {
			mtx.RLock()
			defer mtx.RUnlock()

			countSignalledToStart := trueCount(instancesAwaitReleaseResults)

			return countInstancesReleased == 3 && countSignalledToStart == 3
		}, 1*time.Second, 10*time.Millisecond, "expected three of the four requests to be released and signalled to start immediately")

		// Signal that the three released requests have completed successfully.
		tracker.done(nil, nil)
		tracker.done(nil, nil)
		tracker.done(nil, nil)

		require.Eventually(t, func() bool {
			mtx.RLock()
			defer mtx.RUnlock()

			countSignalledToStart := trueCount(instancesAwaitReleaseResults)

			return countInstancesReleased == 4 && countSignalledToStart == 3
		}, 1*time.Second, 10*time.Millisecond, "expected the final request to be released but not signalled to start")

		require.True(t, tracker.succeeded())
	}

	// With 1000 iterations, 4 instances and max 1 error, we'd expect each instance to receive
	// 750 calls each (each instance has a 3-in-4 chance of being called in each iteration).
	for _, instanceRequestCount := range instanceRequestCounts {
		require.InDeltaf(t, 750, instanceRequestCount.Load(), 30, "expected roughly even distribution of requests across all instances, but got %v", instanceRequestCounts)
	}
}

func TestDefaultResultTracker_ReleaseMinimumRequests_FailingRequestsBelowMaximumAllowed(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	tracker := newDefaultResultTracker(instances, 2)
	tracker.releaseMinimumRequests()

	mtx := sync.RWMutex{}
	countInstancesReleased := 0
	countInstancesSignalledToStart := 0

	for instanceIdx := range instances {
		instance := &instances[instanceIdx]
		go func() {
			released := tracker.awaitRelease(instance)

			mtx.Lock()
			defer mtx.Unlock()

			countInstancesReleased++
			if released {
				countInstancesSignalledToStart++
			}

			// If this is the first request released, mark it as failed.
			if countInstancesReleased == 1 {
				tracker.done(nil, errors.New("something went wrong"))
			}
		}()
	}

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return countInstancesReleased == 3
	}, 1*time.Second, 10*time.Millisecond, "expected three requests to be released")

	require.Equal(t, 3, countInstancesSignalledToStart, "all requests released so far should be signalled to start")

	// Mark the remaining two requests as successful.
	tracker.done(nil, nil)
	tracker.done(nil, nil)

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return countInstancesReleased == 4
	}, 1*time.Second, 10*time.Millisecond, "expected all four requests to be released")

	require.Equal(t, 3, countInstancesSignalledToStart, "final request should not be signalled to start")
	require.True(t, tracker.succeeded(), "overall request should succeed")
}

func TestDefaultResultTracker_ReleaseMinimumRequests_FailingRequestsEqualToMaximumAllowed(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	tracker := newDefaultResultTracker(instances, 2)
	tracker.releaseMinimumRequests()

	mtx := sync.RWMutex{}
	countInstancesReleased := 0
	countInstancesSignalledToStart := 0

	for instanceIdx := range instances {
		instanceIdx := instanceIdx
		instance := &instances[instanceIdx]
		go func() {
			released := tracker.awaitRelease(instance)

			mtx.Lock()
			defer mtx.Unlock()

			countInstancesReleased++
			if released {
				countInstancesSignalledToStart++
			}

			// If this is the first or second request released, mark it as failed.
			if countInstancesReleased <= 2 {
				tracker.done(nil, errors.New("something went wrong"))
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

func TestDefaultResultTracker_ReleaseMinimumRequests_MoreFailingRequestsThanMaximumAllowed(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	tracker := newDefaultResultTracker(instances, 1)
	tracker.releaseMinimumRequests()

	mtx := sync.RWMutex{}
	countInstancesReleased := 0
	countInstancesSignalledToStart := 0

	for instanceIdx := range instances {
		instanceIdx := instanceIdx
		instance := &instances[instanceIdx]
		go func() {
			released := tracker.awaitRelease(instance)

			mtx.Lock()
			defer mtx.Unlock()

			countInstancesReleased++
			if released {
				countInstancesSignalledToStart++
			}

			// If this is the first or second request released, mark it as failed.
			if countInstancesReleased <= 2 {
				tracker.done(nil, errors.New("something went wrong"))
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

func TestDefaultContextTracker(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	parentCtx := context.WithValue(context.Background(), testContextKey, "this-is-the-value-from-the-parent")
	tracker := newDefaultContextTracker(parentCtx, instances)

	instance1Ctx := tracker.contextFor(&instance1)
	instance2Ctx := tracker.contextFor(&instance2)
	instance3Ctx := tracker.contextFor(&instance3)
	instance4Ctx := tracker.contextFor(&instance4)

	for _, ctx := range []context.Context{instance1Ctx, instance2Ctx, instance3Ctx, instance4Ctx} {
		require.Equal(t, "this-is-the-value-from-the-parent", ctx.Value(testContextKey), "context for instance should inherit from provided parent context")
		require.NoError(t, ctx.Err(), "context for instance should not be cancelled")
	}

	// Cancel a context for one instance and check that the others are not cancelled.
	tracker.cancelContextFor(&instance1)
	require.Equal(t, context.Canceled, instance1Ctx.Err(), "instance context should be cancelled")
	for _, ctx := range []context.Context{instance2Ctx, instance3Ctx, instance4Ctx} {
		require.NoError(t, ctx.Err(), "context for instance should not be cancelled after cancelling the context of another instance")
	}

	tracker.cancelAllContexts()
	for _, ctx := range []context.Context{instance1Ctx, instance2Ctx, instance3Ctx, instance4Ctx} {
		require.Equal(t, context.Canceled, ctx.Err(), "context for instance should be cancelled after cancelling all contexts")
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
			testCase.run(t, newZoneAwareResultTracker(testCase.instances, testCase.maxUnavailableZones))
		})
	}
}

func TestZoneAwareResultTracker_ReleaseAllRequests(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6}
	tracker := newZoneAwareResultTracker(instances, 1)

	tracker.releaseAllRequests()

	for i := range instances {
		instance := &instances[i]
		require.True(t, tracker.awaitRelease(instance), "requests for all instances should be released immediately")
	}
}

func TestZoneAwareResultTracker_ReleaseMinimumRequests_NoFailingRequests(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6}

	zoneRequestCounts := map[string]int{"zone-a": 0, "zone-b": 0, "zone-c": 0}

	for testIteration := 0; testIteration < 900; testIteration++ {
		tracker := newZoneAwareResultTracker(instances, 1)
		tracker.releaseMinimumRequests()

		mtx := sync.RWMutex{}
		instancesAwaitReleaseResults := make([]bool, len(instances))
		countInstancesReleased := 0
		for instanceIdx := range instances {
			instanceIdx := instanceIdx
			instance := &instances[instanceIdx]
			go func() {
				released := tracker.awaitRelease(instance)

				mtx.Lock()
				defer mtx.Unlock()
				instancesAwaitReleaseResults[instanceIdx] = released
				countInstancesReleased++
			}()
		}

		require.Eventually(t, func() bool {
			mtx.RLock()
			defer mtx.RUnlock()

			countSignalledToStart := trueCount(instancesAwaitReleaseResults)

			return countInstancesReleased == 4 && countSignalledToStart == 4
		}, 1*time.Second, 10*time.Millisecond, "expected four instances to be released and signalled to start immediately")

		require.Equal(t, instancesAwaitReleaseResults[0], instancesAwaitReleaseResults[1], "expected both zone A instances to either be started or not started")
		require.Equal(t, instancesAwaitReleaseResults[2], instancesAwaitReleaseResults[3], "expected both zone B instances to either be started or not started")
		require.Equal(t, instancesAwaitReleaseResults[4], instancesAwaitReleaseResults[5], "expected both zone C instances to either be started or not started")

		zoneAReleased := instancesAwaitReleaseResults[0]
		zoneBReleased := instancesAwaitReleaseResults[2]
		zoneCReleased := instancesAwaitReleaseResults[4]

		if zoneAReleased {
			zoneRequestCounts["zone-a"]++
			tracker.done(&instance1, nil)
			tracker.done(&instance2, nil)
		}

		if zoneBReleased {
			zoneRequestCounts["zone-b"]++
			tracker.done(&instance3, nil)
			tracker.done(&instance4, nil)
		}

		if zoneCReleased {
			zoneRequestCounts["zone-c"]++
			tracker.done(&instance5, nil)
			tracker.done(&instance6, nil)
		}

		require.True(t, tracker.succeeded())

		require.Eventually(t, func() bool {
			mtx.RLock()
			defer mtx.RUnlock()

			countSignalledToStart := trueCount(instancesAwaitReleaseResults)

			return countInstancesReleased == 6 && countSignalledToStart == 4
		}, 1*time.Second, 10*time.Millisecond, "expected the final requests to be released but not signalled to start")

	}

	// With 900 iterations, 3 zones and max 1 failing zone, we'd expect each zone to receive
	// 600 calls each (each zone has a 2-in-3 chance of being called in each iteration).
	for _, zoneRequestCount := range zoneRequestCounts {
		require.InDeltaf(t, 600, zoneRequestCount, 30, "expected roughly even distribution of requests across all zones, but got %v", zoneRequestCount)
	}
}

func TestZoneAwareResultTracker_ReleaseMinimumRequests_FailingZonesLessThanMaximumAllowed_SingleFailingRequestInZone(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instance7 := InstanceDesc{Addr: "127.0.0.7", Zone: "zone-d"}
	instance8 := InstanceDesc{Addr: "127.0.0.8", Zone: "zone-d"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6, instance7, instance8}

	tracker := newZoneAwareResultTracker(instances, 2)
	tracker.releaseMinimumRequests()

	mtx := sync.RWMutex{}
	instancesAwaitReleaseResults := make([]bool, len(instances))
	instancesReleased := make([]*InstanceDesc, 0, len(instances))
	for instanceIdx := range instances {
		instanceIdx := instanceIdx
		instance := &instances[instanceIdx]
		go func() {
			released := tracker.awaitRelease(instance)

			mtx.Lock()
			defer mtx.Unlock()
			instancesAwaitReleaseResults[instanceIdx] = released
			instancesReleased = append(instancesReleased, instance)
		}()
	}

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return len(instancesReleased) == 4
	}, 1*time.Second, 10*time.Millisecond, "expected four instances to be released initially")

	require.Equal(t, 4, trueCount(instancesAwaitReleaseResults), "expected all four instances to be signalled to start")
	require.Equal(t, 2, uniqueZoneCount(instancesReleased), "expected two zones to be released initially")

	// Simulate one request failing and check that another zone is released.
	tracker.done(instancesReleased[0], errors.New("something went wrong"))

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()
		return len(instancesReleased) == 6
	}, 1*time.Second, 10*time.Millisecond, "expected an additional two instances to be released after a failed request in one zone")

	require.Equal(t, 6, trueCount(instancesAwaitReleaseResults), "expected all six instances to be signalled to start")
	require.Equal(t, 3, uniqueZoneCount(instancesReleased), "expected three zones to be released after one failed")

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

	require.Equal(t, 6, trueCount(instancesAwaitReleaseResults), "expected remaining instances to not be signalled to start")
}

func TestZoneAwareResultTracker_ReleaseMinimumRequests_FailingZonesLessThanMaximumAllowed_MultipleFailingRequestsInSingleZone(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instance7 := InstanceDesc{Addr: "127.0.0.7", Zone: "zone-d"}
	instance8 := InstanceDesc{Addr: "127.0.0.8", Zone: "zone-d"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6, instance7, instance8}

	tracker := newZoneAwareResultTracker(instances, 2)
	tracker.releaseMinimumRequests()

	mtx := sync.RWMutex{}
	instancesAwaitReleaseResults := make([]bool, len(instances))
	instancesReleased := make([]*InstanceDesc, 0, len(instances))
	reportingSecondFailure := false
	instanceReleasedWhileReportingSecondFailure := false

	for instanceIdx := range instances {
		instanceIdx := instanceIdx
		instance := &instances[instanceIdx]
		go func() {
			released := tracker.awaitRelease(instance)

			mtx.Lock()
			defer mtx.Unlock()
			instancesAwaitReleaseResults[instanceIdx] = released
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

	require.Equal(t, 4, trueCount(instancesAwaitReleaseResults), "expected all four instances to be signalled to start")
	require.Equal(t, 2, uniqueZoneCount(instancesReleased), "expected two zones to be released initially")

	// Simulate one request failing and check that another zone is released.
	failingInstance := instancesReleased[0]
	tracker.done(failingInstance, errors.New("something went wrong"))

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()
		return len(instancesReleased) == 6
	}, 1*time.Second, 10*time.Millisecond, "expected an additional two instances to be released after a failed request in one zone")

	require.Equal(t, 6, trueCount(instancesAwaitReleaseResults), "expected all six instances to be signalled to start")
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

	require.Equal(t, 6, trueCount(instancesAwaitReleaseResults), "expected remaining instances to not be signalled to start")
}

func TestZoneAwareResultTracker_ReleaseMinimumRequests_FailingZonesEqualToMaximumAllowed(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instance7 := InstanceDesc{Addr: "127.0.0.7", Zone: "zone-d"}
	instance8 := InstanceDesc{Addr: "127.0.0.8", Zone: "zone-d"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6, instance7, instance8}

	tracker := newZoneAwareResultTracker(instances, 2)
	tracker.releaseMinimumRequests()

	mtx := sync.RWMutex{}
	instancesAwaitReleaseResults := make([]bool, len(instances))
	instancesReleased := make([]*InstanceDesc, 0, len(instances))
	for instanceIdx := range instances {
		instanceIdx := instanceIdx
		instance := &instances[instanceIdx]
		go func() {
			released := tracker.awaitRelease(instance)

			mtx.Lock()
			defer mtx.Unlock()
			instancesAwaitReleaseResults[instanceIdx] = released
			instancesReleased = append(instancesReleased, instance)
		}()
	}

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return len(instancesReleased) == 4
	}, 1*time.Second, 10*time.Millisecond, "expected four instances to be released initially")

	require.Equal(t, 4, trueCount(instancesAwaitReleaseResults), "expected all four instances to be signalled to start")
	require.Equal(t, 2, uniqueZoneCount(instancesReleased), "expected two zones to be released initially")

	// Simulate one request failing and check that another zone is released.
	firstFailingInstance := instancesReleased[0]
	tracker.done(firstFailingInstance, errors.New("something went wrong"))

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()
		return len(instancesReleased) == 6
	}, 1*time.Second, 10*time.Millisecond, "expected an additional two instances to be released after a failed request in one zone")

	require.Equal(t, 6, trueCount(instancesAwaitReleaseResults), "expected all six instances to be signalled to start")
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

	require.Equal(t, 8, trueCount(instancesAwaitReleaseResults), "expected all eight instances to be signalled to start")
	require.Equal(t, 4, uniqueZoneCount(instancesReleased), "expected four zones to be released after two failed")
}

func TestZoneAwareResultTracker_ReleaseMinimumRequests_FailingZonesGreaterThanMaximumAllowed(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instance7 := InstanceDesc{Addr: "127.0.0.7", Zone: "zone-d"}
	instance8 := InstanceDesc{Addr: "127.0.0.8", Zone: "zone-d"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6, instance7, instance8}

	tracker := newZoneAwareResultTracker(instances, 2)
	tracker.releaseMinimumRequests()

	mtx := sync.RWMutex{}
	instancesAwaitReleaseResults := make([]bool, len(instances))
	instancesReleased := make([]*InstanceDesc, 0, len(instances))
	for instanceIdx := range instances {
		instanceIdx := instanceIdx
		instance := &instances[instanceIdx]
		go func() {
			released := tracker.awaitRelease(instance)

			mtx.Lock()
			defer mtx.Unlock()
			instancesAwaitReleaseResults[instanceIdx] = released
			instancesReleased = append(instancesReleased, instance)
		}()
	}

	require.Eventually(t, func() bool {
		mtx.RLock()
		defer mtx.RUnlock()

		return len(instancesReleased) == 4
	}, 1*time.Second, 10*time.Millisecond, "expected four instances to be released initially")

	require.Equal(t, 4, trueCount(instancesAwaitReleaseResults), "expected all four instances to be signalled to start")
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

	require.Equal(t, 8, trueCount(instancesAwaitReleaseResults), "expected all instances to be signalled to start")

	// Simulate one more zone failing.
	tracker.done(instancesReleased[4], errors.New("something else went wrong"))

	require.True(t, tracker.failed())
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

	instance1Ctx := tracker.contextFor(&instance1)
	instance2Ctx := tracker.contextFor(&instance2)
	instance3Ctx := tracker.contextFor(&instance3)
	instance4Ctx := tracker.contextFor(&instance4)
	instance5Ctx := tracker.contextFor(&instance5)
	instance6Ctx := tracker.contextFor(&instance6)

	for _, ctx := range []context.Context{instance1Ctx, instance2Ctx, instance3Ctx, instance4Ctx, instance5Ctx, instance6Ctx} {
		require.Equal(t, "this-is-the-value-from-the-parent", ctx.Value(testContextKey), "context for instance should inherit from provided parent context")
		require.NoError(t, ctx.Err(), "context for instance should not be cancelled")
	}

	// Cancel a context for one instance and check that the other context in its zone is cancelled, but others are
	// unaffected.
	tracker.cancelContextFor(&instance1)
	require.Equal(t, context.Canceled, instance1Ctx.Err(), "instance context should be cancelled")
	require.Equal(t, context.Canceled, instance2Ctx.Err(), "context for instance in same zone as cancelled instance should also be cancelled")
	for _, ctx := range []context.Context{instance3Ctx, instance4Ctx, instance5Ctx, instance6Ctx} {
		require.NoError(t, ctx.Err(), "context for instance should not be cancelled after cancelling the context of another instance")
	}

	tracker.cancelAllContexts()
	for _, ctx := range []context.Context{instance1Ctx, instance2Ctx, instance3Ctx, instance4Ctx, instance5Ctx, instance6Ctx} {
		require.Equal(t, context.Canceled, ctx.Err(), "context for instance should be cancelled after cancelling all contexts")
	}
}

func uniqueZoneCount(instances []*InstanceDesc) int {
	zones := map[string]struct{}{}

	for _, i := range instances {
		zones[i.Zone] = struct{}{}
	}

	return len(zones)
}

func trueCount(l []bool) int {
	count := 0

	for _, v := range l {
		if v {
			count++
		}
	}

	return count
}
