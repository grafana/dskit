package ring

import "context"

type replicationSetResultTracker interface {
	// Signals an instance has done the execution, either successful (no error)
	// or failed (with error).
	done(instance *InstanceDesc, err error)

	// Returns true if the minimum number of successful results have been received.
	succeeded() bool

	// Returns true if the maximum number of failed executions have been reached.
	failed() bool

	// Returns true if the result returned by instance is part of the minimal set of all results
	// required to meet the quorum requirements of this tracker.
	// This method should only be called for instances that have returned a successful result,
	// calling this method for an instance that returned an error may return unpredictable results.
	// This method should only be called after succeeded returns true for the first time and before
	// calling done any further times.
	shouldIncludeResultFrom(instance *InstanceDesc) bool

	// Returns a context.Context for instance.
	contextFor(instance *InstanceDesc) context.Context

	// Cancels the context for instance previously obtained with contextFor.
	// This method may cancel the context for other instances if those other instances are part of
	// the same zone and this tracker is zone-aware.
	cancelContextFor(instance *InstanceDesc)
}

type defaultResultTracker struct {
	ctx                        context.Context
	minSucceeded               int
	numSucceeded               int
	numErrors                  int
	maxErrors                  int
	instanceContextCancelFuncs map[*InstanceDesc]context.CancelFunc
}

func newDefaultResultTracker(ctx context.Context, instances []InstanceDesc, maxErrors int) *defaultResultTracker {
	return &defaultResultTracker{
		ctx:                        ctx,
		minSucceeded:               len(instances) - maxErrors,
		numSucceeded:               0,
		numErrors:                  0,
		maxErrors:                  maxErrors,
		instanceContextCancelFuncs: make(map[*InstanceDesc]context.CancelFunc, len(instances)),
	}
}

func (t *defaultResultTracker) done(_ *InstanceDesc, err error) {
	if err == nil {
		t.numSucceeded++
	} else {
		t.numErrors++
	}
}

func (t *defaultResultTracker) succeeded() bool {
	return t.numSucceeded >= t.minSucceeded
}

func (t *defaultResultTracker) failed() bool {
	return t.numErrors > t.maxErrors
}

func (t *defaultResultTracker) shouldIncludeResultFrom(_ *InstanceDesc) bool {
	// This method should only be called for instances that succeeded, and immediately
	// after we've returned succeeded()==true for the first time.
	return true
}

func (t *defaultResultTracker) contextFor(instance *InstanceDesc) context.Context {
	ctx, cancel := context.WithCancel(t.ctx)
	t.instanceContextCancelFuncs[instance] = cancel
	return ctx
}

func (t *defaultResultTracker) cancelContextFor(instance *InstanceDesc) {
	t.instanceContextCancelFuncs[instance]()
}

// zoneAwareResultTracker tracks the results per zone.
// All instances in a zone must succeed in order for the zone to succeed.
type zoneAwareResultTracker struct {
	waitingByZone          map[string]int
	failuresByZone         map[string]int
	minSuccessfulZones     int
	maxUnavailableZones    int
	zoneContexts           map[string]context.Context
	zoneContextCancelFuncs map[string]context.CancelFunc
}

func newZoneAwareResultTracker(ctx context.Context, instances []InstanceDesc, maxUnavailableZones int) *zoneAwareResultTracker {
	t := &zoneAwareResultTracker{
		waitingByZone:       make(map[string]int),
		failuresByZone:      make(map[string]int),
		maxUnavailableZones: maxUnavailableZones,
	}

	for _, instance := range instances {
		t.waitingByZone[instance.Zone]++
	}

	t.minSuccessfulZones = len(t.waitingByZone) - maxUnavailableZones

	t.zoneContexts = make(map[string]context.Context, len(t.waitingByZone))
	t.zoneContextCancelFuncs = make(map[string]context.CancelFunc, len(t.waitingByZone))

	for zone := range t.waitingByZone {
		zoneCtx, cancel := context.WithCancel(ctx)
		t.zoneContexts[zone] = zoneCtx
		t.zoneContextCancelFuncs[zone] = cancel
	}

	return t
}

func (t *zoneAwareResultTracker) done(instance *InstanceDesc, err error) {
	t.waitingByZone[instance.Zone]--

	if err != nil {
		t.failuresByZone[instance.Zone]++
	}
}

func (t *zoneAwareResultTracker) succeeded() bool {
	successfulZones := 0

	// The execution succeeded once we successfully received a successful result
	// from "all zones - max unavailable zones".
	for zone, numWaiting := range t.waitingByZone {
		if numWaiting == 0 && t.failuresByZone[zone] == 0 {
			successfulZones++
		}
	}

	return successfulZones >= t.minSuccessfulZones
}

func (t *zoneAwareResultTracker) failed() bool {
	failedZones := len(t.failuresByZone)
	return failedZones > t.maxUnavailableZones
}

func (t *zoneAwareResultTracker) shouldIncludeResultFrom(instance *InstanceDesc) bool {
	return t.failuresByZone[instance.Zone] == 0 && t.waitingByZone[instance.Zone] == 0
}

func (t *zoneAwareResultTracker) contextFor(instance *InstanceDesc) context.Context {
	return t.zoneContexts[instance.Zone]
}

func (t *zoneAwareResultTracker) cancelContextFor(instance *InstanceDesc) {
	t.zoneContextCancelFuncs[instance.Zone]()
}
