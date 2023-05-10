package ring

import (
	"context"
	"errors"
	"testing"

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
			parentCtx := context.Background()

			testCase.run(t, newDefaultResultTracker(parentCtx, testCase.instances, testCase.maxErrors))
		})
	}
}

func TestDefaultResultTracker_ContextHandling(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4}

	parentCtx := context.WithValue(context.Background(), testContextKey, "this-is-the-value-from-the-parent")
	tracker := newDefaultResultTracker(parentCtx, instances, 0)

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
			testCase.run(t, newZoneAwareResultTracker(context.Background(), testCase.instances, testCase.maxUnavailableZones))
		})
	}
}

func TestZoneAwareResultTracker_ContextHandling(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}
	instances := []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6}

	parentCtx := context.WithValue(context.Background(), testContextKey, "this-is-the-value-from-the-parent")
	tracker := newZoneAwareResultTracker(parentCtx, instances, 0)

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
}
