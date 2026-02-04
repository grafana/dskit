package memberlist

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/timeutil"
)

const (
	// propagationTrackerKey is the KV store key used for storing propagation tracker state.
	propagationTrackerKey = "memberlist-propagation-tracker"
)

// PropagationTracker is a service that tracks gossip propagation delay across
// the memberlist cluster by periodically publishing beacons and measuring
// how long it takes for beacons to propagate via WatchKey.
type PropagationTracker struct {
	services.Service

	beaconInterval time.Duration
	beaconLifetime time.Duration
	kv             *KV
	codec          codec.Codec
	logger         log.Logger

	// seenBeacons tracks beacon IDs that have already been processed to avoid
	// measuring the same beacon twice.
	seenBeaconsMu sync.RWMutex
	seenBeacons   map[uint64]struct{}

	// initialSyncDone indicates whether the first WatchKey callback has completed.
	// On the first callback, we mark all beacons as seen but skip delay recording
	// since pre-existing beacons may have been published long ago.
	initialSyncDone atomic.Bool

	// Metrics
	propagationDelay      prometheus.Histogram
	beaconsPublishedTotal prometheus.Counter
	beaconsReceivedTotal  prometheus.Counter
}

// NewPropagationTracker creates a new PropagationTracker service.
//
// Parameters:
//   - kv: The memberlist KV store. The propagation tracker codec must be registered before calling this.
//   - beaconInterval: How often to check for and potentially publish beacons.
//   - beaconLifetime: How long a beacon lives before being marked as a tombstone.
//   - logger: Logger for the service.
//   - registerer: Prometheus registerer for metrics.
func NewPropagationTracker(
	kv *KV,
	beaconInterval time.Duration,
	beaconLifetime time.Duration,
	logger log.Logger,
	registerer prometheus.Registerer,
) *PropagationTracker {
	t := &PropagationTracker{
		beaconInterval: beaconInterval,
		beaconLifetime: beaconLifetime,
		kv:             kv,
		codec:          GetPropagationTrackerCodec(),
		logger:         log.With(logger, "component", "memberlist-propagation-tracker"),
		seenBeacons:    make(map[uint64]struct{}),
		propagationDelay: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Name:                            "memberlist_propagation_tracker_delay_seconds",
			Help:                            "Time from beacon publish to receive.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
		beaconsPublishedTotal: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "memberlist_propagation_tracker_beacons_published_total",
			Help: "Total number of beacons published by this node.",
		}),
		beaconsReceivedTotal: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "memberlist_propagation_tracker_beacons_received_total",
			Help: "Total number of unique beacons received.",
		}),
	}

	t.Service = services.NewBasicService(nil, t.running, nil).WithName("propagation-tracker")

	return t
}

func (t *PropagationTracker) running(ctx context.Context) error {
	level.Info(t.logger).Log("msg", "propagation tracker started", "beacon_interval", t.beaconInterval, "beacon_lifetime", t.beaconLifetime)

	// Start the WatchKey goroutine to track beacon arrivals in real-time
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()

	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		t.watchBeacons(watchCtx)
	}()
	defer func() {
		select {
		case <-watchDone:
		case <-time.After(10 * time.Second):
			level.Warn(t.logger).Log("msg", "timed out waiting for watch goroutine to finish")
		}
	}()

	// Start the beacon publish ticker. The first tick uses a random delay between 1ns and
	// beaconInterval to distribute beacon publishing over time when multiple processes
	// start simultaneously (e.g., during a rollout).
	initialDelay := 1 + time.Duration(rand.Int63n(int64(t.beaconInterval)))
	stopTicker, tickerChan := timeutil.NewVariableTicker(initialDelay, t.beaconInterval)
	defer stopTicker()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tickerChan:
			t.onBeaconInterval(ctx)
		}
	}
}

// watchBeacons uses WatchKey to receive beacon updates in real-time and measure delay.
func (t *PropagationTracker) watchBeacons(ctx context.Context) {
	t.kv.WatchKey(ctx, propagationTrackerKey, t.codec, func(val interface{}) bool {
		if val == nil {
			return true // continue watching
		}

		desc, ok := val.(*PropagationTrackerDesc)
		if !ok {
			level.Warn(t.logger).Log("msg", "unexpected value type in watch callback", "type", fmt.Sprintf("%T", val))
			return true
		}

		t.onBeaconsReceived(desc)
		return true // continue watching
	})
}

// onBeaconsReceived processes received beacons and records delay for unseen beacons.
func (t *PropagationTracker) onBeaconsReceived(desc *PropagationTrackerDesc) {
	now := time.Now()
	receivedCount := 0

	for beaconID, beacon := range desc.Beacons {
		// Skip deleted beacons (tombstones)
		if beacon.DeletedAt != 0 {
			continue
		}

		if t.alreadySeen(beaconID) {
			continue
		}
		t.markAsSeen(beaconID)

		// Skip delay recording on initial sync (pre-existing beacons)
		if !t.initialSyncDone.Load() {
			continue
		}

		receivedCount++
		delay := now.Sub(beacon.GetPublishedAtTime())
		if delay >= 0 {
			t.propagationDelay.Observe(delay.Seconds())
		}
	}

	if !t.initialSyncDone.Load() {
		t.initialSyncDone.Store(true)
	}

	if receivedCount > 0 {
		t.beaconsReceivedTotal.Add(float64(receivedCount))
	}

	// Clean up seenBeacons that are no longer in the KV store (or are deleted tombstones).
	t.cleanupSeenBeacons(desc)
}

func (t *PropagationTracker) alreadySeen(beaconID uint64) bool {
	t.seenBeaconsMu.RLock()
	defer t.seenBeaconsMu.RUnlock()
	_, seen := t.seenBeacons[beaconID]
	return seen
}

func (t *PropagationTracker) markAsSeen(beaconID uint64) {
	t.seenBeaconsMu.Lock()
	defer t.seenBeaconsMu.Unlock()
	t.seenBeacons[beaconID] = struct{}{}
}

// cleanupSeenBeacons removes entries from seenBeacons that are no longer active
// in the KV store. Tombstone handling is done at the model level (BeaconDesc.DeletedAt),
// so we just need to keep seenBeacons in sync with active beacons.
func (t *PropagationTracker) cleanupSeenBeacons(desc *PropagationTrackerDesc) {
	t.seenBeaconsMu.Lock()
	defer t.seenBeaconsMu.Unlock()

	for beaconID := range t.seenBeacons {
		beacon, exists := desc.Beacons[beaconID]
		// Remove from seenBeacons if not in KV store or if it's a tombstone
		if !exists || beacon.DeletedAt != 0 {
			delete(t.seenBeacons, beaconID)
		}
	}
}

// onBeaconInterval is called on each beacon interval tick to:
// 1. Mark beacons older than beaconLifetime as tombstones for garbage collection
// 2. Publish a new beacon if no recent beacon exists
func (t *PropagationTracker) onBeaconInterval(ctx context.Context) {
	val, err := t.kv.Get(propagationTrackerKey, t.codec)
	if err != nil {
		level.Warn(t.logger).Log("msg", "failed to get beacons", "err", err)
		return
	}

	desc := GetOrCreatePropagationTrackerDesc(val)
	now := time.Now()

	// Collect old beacons that should be marked as tombstones
	var beaconsToDelete []uint64
	hasRecentBeacon := false

	for beaconID, beacon := range desc.Beacons {
		if beacon.DeletedAt != 0 {
			continue
		}
		age := now.Sub(beacon.GetPublishedAtTime())
		if age >= t.beaconLifetime {
			beaconsToDelete = append(beaconsToDelete, beaconID)
		} else if age < t.beaconInterval {
			hasRecentBeacon = true
		}
	}

	// Mark old beacons as tombstones
	if len(beaconsToDelete) > 0 {
		t.deleteBeacons(ctx, beaconsToDelete)
	}

	// Publish new beacon if no recent one exists
	if !hasRecentBeacon {
		t.publishBeacon(ctx)
	}
}

func (t *PropagationTracker) publishBeacon(ctx context.Context) {
	beaconID := rand.Uint64()
	now := time.Now()

	// Mark as seen before publishing to avoid measuring our own beacon
	t.markAsSeen(beaconID)

	err := t.kv.CAS(ctx, propagationTrackerKey, t.codec, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePropagationTrackerDesc(in)

		desc.Beacons[beaconID] = BeaconDesc{
			PublishedAt: now.UnixMilli(),
		}

		return desc, true, nil
	})

	if err != nil {
		level.Warn(t.logger).Log("msg", "failed to publish beacon", "err", err)
		return
	}

	t.beaconsPublishedTotal.Inc()
	level.Debug(t.logger).Log("msg", "published beacon", "beacon_id", beaconID)
}

// deleteBeacons marks the specified beacons as tombstones by setting their DeletedAt timestamp.
func (t *PropagationTracker) deleteBeacons(ctx context.Context, beaconIDs []uint64) {
	err := t.kv.CAS(ctx, propagationTrackerKey, t.codec, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePropagationTrackerDesc(in)
		now := time.Now()

		changed := false
		for _, beaconID := range beaconIDs {
			beacon, exists := desc.Beacons[beaconID]
			if exists && beacon.DeletedAt == 0 {
				beacon.DeletedAt = now.UnixMilli()
				desc.Beacons[beaconID] = beacon
				changed = true
			}
		}

		if !changed {
			return nil, false, nil
		}
		return desc, true, nil
	})

	if err != nil {
		level.Warn(t.logger).Log("msg", "failed to mark beacons as deleted", "err", err)
		return
	}

	level.Debug(t.logger).Log("msg", "marked beacons as deleted", "count", len(beaconIDs))
}
