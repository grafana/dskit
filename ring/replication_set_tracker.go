package ring

import (
	"context"
	"math/rand"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

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

	// Starts an initial set of requests sufficient to meet the quorum requirements of this tracker,
	// by calling the tracker's onStart function for each instance whose request should start.
	// Further requests will be started if necessary when done is called with a non-nil error.
	// Calling this method multiple times may lead to unpredictable behaviour.
	// Calling both this method and releaseAllRequests may lead to unpredictable behaviour.
	// This method must only be called before calling done.
	startMinimumRequests()

	// Starts additional request(s) as defined by the quorum requirements of this tracker.
	// For example, a zone-aware tracker would start requests for another zone, whereas a
	// non-zone-aware tracker would start a request for another instance.
	// This method must only be called after calling startMinimumRequests or startAllRequests.
	// If requests for all instances have already been started, this method does nothing.
	// This method must only be called before calling done.
	startAdditionalRequests()

	// Starts requests for all instances.
	// Calling this method multiple times may lead to unpredictable behaviour.
	// Calling both this method and releaseMinimumRequests may lead to unpredictable behaviour.
	// This method must only be called before calling done.
	startAllRequests()
}

// startFunc is called by a replicationSetResultTracker, synchronously, for each instance whose request
// should start. Requests that are never needed are never started.
type startFunc func(instance *InstanceDesc)

type replicationSetContextTracker interface {
	// Returns a context.Context and context.CancelFunc for instance.
	// The context.CancelFunc will only cancel the context for this instance (ie. if this tracker
	// is zone-aware, calling the context.CancelFunc should not cancel contexts for other instances
	// in the same zone).
	contextFor(instance *InstanceDesc) (context.Context, context.CancelCauseFunc)

	// Cancels the context for instance previously obtained with contextFor.
	// This method may cancel the context for other instances if those other instances are part of
	// the same zone and this tracker is zone-aware.
	cancelContextFor(instance *InstanceDesc, cause error)

	// Cancels all contexts previously obtained with contextFor.
	cancelAllContexts(cause error)
}

type defaultResultTracker struct {
	minSucceeded     int
	numSucceeded     int
	numErrors        int
	maxErrors        int
	instances        []InstanceDesc
	pendingInstances []*InstanceDesc
	onStart          startFunc
	logger           log.Logger
}

func newDefaultResultTracker(instances []InstanceDesc, maxErrors int, onStart startFunc, logger log.Logger) *defaultResultTracker {
	return &defaultResultTracker{
		minSucceeded: len(instances) - maxErrors,
		numSucceeded: 0,
		numErrors:    0,
		maxErrors:    maxErrors,
		instances:    instances,
		onStart:      onStart,
		logger:       logger,
	}
}

func (t *defaultResultTracker) done(instance *InstanceDesc, err error) {
	if err == nil {
		t.numSucceeded++

		if t.succeeded() {
			t.onSucceeded()
		}
	} else {
		level.Warn(t.logger).Log(
			"msg", "instance failed",
			"instanceAddr", instance.Addr,
			"instanceID", instance.Id,
			"err", err,
		)

		t.numErrors++
		t.startAdditionalRequestsDueTo("failure of other instance")
	}
}

func (t *defaultResultTracker) succeeded() bool {
	return t.numSucceeded >= t.minSucceeded
}

func (t *defaultResultTracker) onSucceeded() {
	// We don't need any of the requests that haven't started yet.
	t.pendingInstances = nil
}

func (t *defaultResultTracker) failed() bool {
	return t.numErrors > t.maxErrors
}

func (t *defaultResultTracker) shouldIncludeResultFrom(_ *InstanceDesc) bool {
	return true
}

func (t *defaultResultTracker) startMinimumRequests() {
	releaseOrder := rand.Perm(len(t.instances))
	t.pendingInstances = make([]*InstanceDesc, 0, t.maxErrors)

	for _, instanceIdx := range releaseOrder {
		instance := &t.instances[instanceIdx]

		if len(t.pendingInstances) < t.maxErrors {
			t.pendingInstances = append(t.pendingInstances, instance)
		} else {
			level.Debug(t.logger).Log("msg", "starting request to instance", "reason", "initial requests", "instanceAddr", instance.Addr, "instanceID", instance.Id)
			t.onStart(instance)
		}
	}

	// If we've already succeeded (which should only happen if the replica set is misconfigured with MaxErrors >= the number of instances),
	// then we don't need any of the pending requests.
	if t.succeeded() {
		t.onSucceeded()
	}
}

func (t *defaultResultTracker) startAdditionalRequests() {
	t.startAdditionalRequestsDueTo("hedging")
}

func (t *defaultResultTracker) startAdditionalRequestsDueTo(reason string) {
	if len(t.pendingInstances) > 0 {
		// There are some outstanding requests we could make before we reach maxErrors. Release the next one.
		i := t.pendingInstances[0]
		level.Debug(t.logger).Log("msg", "starting request to instance", "reason", reason, "instanceAddr", i.Addr, "instanceID", i.Id)
		t.onStart(i)
		t.pendingInstances = t.pendingInstances[1:]
	}
}

func (t *defaultResultTracker) startAllRequests() {
	for i := range t.instances {
		instance := &t.instances[i]
		level.Debug(t.logger).Log("msg", "starting request to instance", "reason", "initial requests", "instanceAddr", instance.Addr, "instanceID", instance.Id)
		t.onStart(instance)
	}
}

type defaultContextTracker struct {
	ctx         context.Context
	cancelFuncs map[*InstanceDesc]context.CancelCauseFunc
}

func newDefaultContextTracker(ctx context.Context, instances []InstanceDesc) *defaultContextTracker {
	return &defaultContextTracker{
		ctx:         ctx,
		cancelFuncs: make(map[*InstanceDesc]context.CancelCauseFunc, len(instances)),
	}
}

func (t *defaultContextTracker) contextFor(instance *InstanceDesc) (context.Context, context.CancelCauseFunc) {
	ctx, cancel := context.WithCancelCause(t.ctx)
	t.cancelFuncs[instance] = cancel
	return ctx, cancel
}

func (t *defaultContextTracker) cancelContextFor(instance *InstanceDesc, cause error) {
	if cancel, ok := t.cancelFuncs[instance]; ok {
		cancel(cause)
		delete(t.cancelFuncs, instance)
	}
}

func (t *defaultContextTracker) cancelAllContexts(cause error) {
	for instance, cancel := range t.cancelFuncs {
		cancel(cause)
		delete(t.cancelFuncs, instance)
	}
}

// zoneAwareResultTracker tracks the results per zone.
// All instances in a zone must succeed in order for the zone to succeed.
type zoneAwareResultTracker struct {
	waitingByZone       map[string]int
	failuresByZone      map[string]int
	minSuccessfulZones  int
	maxUnavailableZones int
	instances           []InstanceDesc
	pendingZones        []string
	zoneSorter          ZoneSorter
	onStart             startFunc
	logger              log.Logger
}

type ZoneSorter func(zones []string) []string

func newZoneAwareResultTracker(instances []InstanceDesc, maxUnavailableZones int, zoneSorter ZoneSorter, onStart startFunc, logger log.Logger) *zoneAwareResultTracker {
	t := &zoneAwareResultTracker{
		waitingByZone:       make(map[string]int),
		failuresByZone:      make(map[string]int),
		maxUnavailableZones: maxUnavailableZones,
		instances:           instances,
		zoneSorter:          zoneSorter,
		onStart:             onStart,
		logger:              logger,
	}

	for _, instance := range instances {
		t.waitingByZone[instance.Zone]++
	}

	t.minSuccessfulZones = len(t.waitingByZone) - maxUnavailableZones

	if t.minSuccessfulZones < 0 {
		t.minSuccessfulZones = 0
	}

	if t.zoneSorter == nil {
		t.zoneSorter = defaultZoneSorter
	}

	return t
}

func defaultZoneSorter(zones []string) []string {
	rand.Shuffle(len(zones), func(i, j int) {
		zones[i], zones[j] = zones[j], zones[i]
	})

	return zones
}

func (t *zoneAwareResultTracker) done(instance *InstanceDesc, err error) {
	t.waitingByZone[instance.Zone]--

	if err == nil {
		if t.succeeded() {
			t.onSucceeded()
		}
	} else {
		t.failuresByZone[instance.Zone]++

		if t.failuresByZone[instance.Zone] == 1 {
			level.Warn(t.logger).Log(
				"msg", "request to instance has failed, zone cannot contribute to quorum",
				"zone", instance.Zone,
				"failingInstanceAddr", instance.Addr,
				"failingInstanceID", instance.Id,
				"err", err,
			)

			// If this was the first failure for this zone, release another zone's requests and signal they should start.
			t.startAdditionalRequestsDueTo("failure of other zone")
		}
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

func (t *zoneAwareResultTracker) onSucceeded() {
	// We don't need any of the zones that haven't started yet.
	t.pendingZones = nil
}

func (t *zoneAwareResultTracker) failed() bool {
	failedZones := len(t.failuresByZone)
	return failedZones > t.maxUnavailableZones
}

func (t *zoneAwareResultTracker) shouldIncludeResultFrom(instance *InstanceDesc) bool {
	return t.failuresByZone[instance.Zone] == 0 && t.waitingByZone[instance.Zone] == 0
}

func (t *zoneAwareResultTracker) startMinimumRequests() {
	allZones := make([]string, 0, len(t.waitingByZone))

	for zone := range t.waitingByZone {
		allZones = append(allZones, zone)
	}

	allZones = t.zoneSorter(allZones)

	for i := 0; i < t.minSuccessfulZones; i++ {
		level.Debug(t.logger).Log("msg", "starting requests to zone", "reason", "initial requests", "zone", allZones[i])
		t.startZone(allZones[i])
	}

	t.pendingZones = allZones[t.minSuccessfulZones:]

	// If we've already succeeded (which should only happen if the replica set is misconfigured with MaxUnavailableZones >= the number of zones),
	// then we don't need any of the pending zones.
	if t.succeeded() {
		t.onSucceeded()
	}
}

func (t *zoneAwareResultTracker) startAdditionalRequests() {
	t.startAdditionalRequestsDueTo("hedging")
}

func (t *zoneAwareResultTracker) startAdditionalRequestsDueTo(reason string) {
	if len(t.pendingZones) > 0 {
		// If there are more zones we could try before reaching maxUnavailableZones, release another zone's requests and signal they should start.
		level.Debug(t.logger).Log("msg", "starting requests to zone", "reason", reason, "zone", t.pendingZones[0])
		t.startZone(t.pendingZones[0])
		t.pendingZones = t.pendingZones[1:]
	}
}

func (t *zoneAwareResultTracker) startAllRequests() {
	for zone := range t.waitingByZone {
		level.Debug(t.logger).Log("msg", "starting requests to zone", "reason", "initial requests", "zone", zone)
		t.startZone(zone)
	}
}

func (t *zoneAwareResultTracker) startZone(zone string) {
	for i := range t.instances {
		if t.instances[i].Zone == zone {
			t.onStart(&t.instances[i])
		}
	}
}

type zoneAwareContextTracker struct {
	ctx         context.Context
	contexts    map[*InstanceDesc]context.Context
	cancelFuncs map[*InstanceDesc]context.CancelCauseFunc
}

// newZoneAwareContextTracker returns a tracker that creates each instance's context the first time
// contextFor is called for it, so instances whose requests never start don't cost a context.
func newZoneAwareContextTracker(ctx context.Context, instances []InstanceDesc) *zoneAwareContextTracker {
	return &zoneAwareContextTracker{
		ctx:         ctx,
		contexts:    make(map[*InstanceDesc]context.Context, len(instances)),
		cancelFuncs: make(map[*InstanceDesc]context.CancelCauseFunc, len(instances)),
	}
}

func (t *zoneAwareContextTracker) contextFor(instance *InstanceDesc) (context.Context, context.CancelCauseFunc) {
	if ctx, ok := t.contexts[instance]; ok {
		return ctx, t.cancelFuncs[instance]
	}

	ctx, cancel := context.WithCancelCause(t.ctx)
	t.contexts[instance] = ctx
	t.cancelFuncs[instance] = cancel
	return ctx, cancel
}

func (t *zoneAwareContextTracker) cancelContextFor(instance *InstanceDesc, cause error) {
	// Why not create a per-zone parent context to make this easier?
	// If we create a per-zone parent context, we'd need to have some way to cancel the per-zone context when the last of the individual
	// contexts in a zone are cancelled using the context.CancelFunc returned from contextFor.
	for i, cancel := range t.cancelFuncs {
		if i.Zone == instance.Zone {
			cancel(cause)
			delete(t.contexts, i)
			delete(t.cancelFuncs, i)
		}
	}
}

func (t *zoneAwareContextTracker) cancelAllContexts(cause error) {
	for instance, cancel := range t.cancelFuncs {
		cancel(cause)
		delete(t.contexts, instance)
		delete(t.cancelFuncs, instance)
	}
}

type inflightInstanceTracker struct {
	mx       sync.Mutex
	inflight [][]*InstanceDesc

	// expectMoreInstances is true if more instances are expected to be added to the tracker.
	expectMoreInstances bool
}

func newInflightInstanceTracker(sets []ReplicationSet) *inflightInstanceTracker {
	// Init the inflight tracker.
	inflight := make([][]*InstanceDesc, len(sets))
	for idx, set := range sets {
		inflight[idx] = make([]*InstanceDesc, 0, len(set.Instances))
	}

	return &inflightInstanceTracker{
		inflight:            inflight,
		expectMoreInstances: true,
	}
}

// addInstance adds the instance for replicationSetIdx to the tracker.
//
// addInstance is idempotent.
func (t *inflightInstanceTracker) addInstance(replicationSetIdx int, instance *InstanceDesc) {
	t.mx.Lock()
	defer t.mx.Unlock()

	// Check if the instance has already been added.
	for _, curr := range t.inflight[replicationSetIdx] {
		if curr == instance {
			return
		}
	}

	t.inflight[replicationSetIdx] = append(t.inflight[replicationSetIdx], instance)
}

// removeInstance removes the instance for replicationSetIdx from the tracker.
//
// removeInstance is idempotent.
func (t *inflightInstanceTracker) removeInstance(replicationSetIdx int, instance *InstanceDesc) {
	t.mx.Lock()
	defer t.mx.Unlock()

	for i, curr := range t.inflight[replicationSetIdx] {
		if curr == instance {
			instances := t.inflight[replicationSetIdx]
			t.inflight[replicationSetIdx] = append(instances[:i], instances[i+1:]...)

			// We can safely break the loop because we don't expect multiple occurrences of the same instance.
			return
		}
	}
}

// allInstancesAdded signals the tracker that all expected instances have been added.
//
// allInstancesAdded is idempotent.
func (t *inflightInstanceTracker) allInstancesAdded() {
	t.mx.Lock()
	defer t.mx.Unlock()

	t.expectMoreInstances = false
}

// allInstancesCompleted returns true if and only if no more instances are expected to be
// added to the tracker and all previously tracked instances have been removed calling removeInstance().
func (t *inflightInstanceTracker) allInstancesCompleted() bool {
	t.mx.Lock()
	defer t.mx.Unlock()

	// We can't assert all instances have completed if it's still possible
	// to add new ones to the tracker.
	if t.expectMoreInstances {
		return false
	}

	// Ensure there are no inflight instances for any replication set.
	for _, instances := range t.inflight {
		if len(instances) > 0 {
			return false
		}
	}

	return true
}
