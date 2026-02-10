package memberlist

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/metrics"
	"github.com/grafana/dskit/services"
)

// TestPropagationTracker_TracksPropagationDelay verifies that when multiple trackers are running,
// they receive beacons from each other and record the propagation delay in the histogram.
func TestPropagationTracker_TracksPropagationDelay(t *testing.T) {
	mkv := createPropagationTrackerTestKV(t)

	const beaconInterval = 100 * time.Millisecond

	// Start two trackers with separate registries so we can check their metrics independently.
	// Each tracker will publish beacons and receive beacons from the other.
	registry1 := prometheus.NewPedanticRegistry()
	tracker1 := NewPropagationTracker(mkv, beaconInterval, time.Hour, log.NewNopLogger(), registry1)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), tracker1))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), tracker1))
	})

	registry2 := prometheus.NewPedanticRegistry()
	tracker2 := NewPropagationTracker(mkv, beaconInterval, time.Hour, log.NewNopLogger(), registry2)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), tracker2))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), tracker2))
	})

	// Wait for trackers to exchange beacons and record delay.
	// We need to wait long enough for:
	// 1. Initial sync to complete on both trackers (beacons seen during initial sync are not measured)
	// 2. At least one tracker to publish a beacon AFTER both trackers have completed initial sync
	// 3. The other tracker to receive it via WatchKey and record delay
	//
	// Due to random initial delays, one tracker might "win" several beacon publishing cycles
	// before the other tracker gets a chance to measure any beacons. A 10-second timeout
	// provides enough margin for both trackers to eventually receive beacons from each other.
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		metricFamilies1, err := metrics.NewMetricFamilyMapFromGatherer(registry1)
		require.NoError(t, err)
		metricFamilies2, err := metrics.NewMetricFamilyMapFromGatherer(registry2)
		require.NoError(t, err)

		histogram1, err := metrics.FindHistogramWithNameAndLabels(metricFamilies1, "memberlist_propagation_tracker_delay_seconds")
		require.NoError(t, err)
		require.NotNil(t, histogram1)

		histogram2, err := metrics.FindHistogramWithNameAndLabels(metricFamilies2, "memberlist_propagation_tracker_delay_seconds")
		require.NoError(t, err)
		require.NotNil(t, histogram2)

		count1 := histogram1.GetSampleCount()
		count2 := histogram2.GetSampleCount()

		// Each tracker should have received at least one beacon from the other.
		require.Greater(t, count1, uint64(0), "tracker1 should have received beacons")
		require.Greater(t, count2, uint64(0), "tracker2 should have received beacons")

		// Verify the average delay is reasonable (should be very low for in-memory communication).
		// We allow up to 5 seconds to account for test environment variability.
		avgDelay1 := histogram1.GetSampleSum() / float64(count1)
		avgDelay2 := histogram2.GetSampleSum() / float64(count2)
		assert.Less(t, avgDelay1, 5.0, "tracker1 average delay should be less than 5 seconds")
		assert.Less(t, avgDelay2, 5.0, "tracker2 average delay should be less than 5 seconds")
	}, 10*time.Second, 50*time.Millisecond)
}

// TestPropagationTracker_SkipsTrackingPropagationDelayOnInitialSync verifies that:
// 1. Pre-existing beacons in the KV store do not have their delay recorded when the tracker starts
// 2. A tracker's own published beacons are marked as seen before publishing, so they don't count as "received"
func TestPropagationTracker_SkipsTrackingPropagationDelayOnInitialSync(t *testing.T) {
	mkv := createPropagationTrackerTestKV(t)

	const beaconInterval = 100 * time.Millisecond

	// Start a "publisher" tracker to pre-populate the KV store with beacons.
	// This simulates beacons that were published by other nodes before our test tracker starts.
	publisher := NewPropagationTracker(mkv, beaconInterval, time.Hour, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), publisher))

	// Wait for the publisher to create some beacons
	require.Eventually(t, func() bool {
		val, err := mkv.Get(propagationTrackerKey, GetPropagationTrackerCodec())
		if err != nil || val == nil {
			return false
		}
		desc := val.(*PropagationTrackerDesc)
		return len(desc.Beacons) >= 2
	}, 5*time.Second, 50*time.Millisecond, "expected publisher to create beacons")

	// Stop the publisher - we now have pre-existing beacons in the KV store
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), publisher))

	// Record how many beacons exist before starting the test tracker
	val, err := mkv.Get(propagationTrackerKey, GetPropagationTrackerCodec())
	require.NoError(t, err)
	preExistingBeaconCount := len(val.(*PropagationTrackerDesc).Beacons)

	// Create test tracker with its own registry so we can check metrics
	registry := prometheus.NewPedanticRegistry()
	tracker := NewPropagationTracker(mkv, beaconInterval, time.Hour, log.NewNopLogger(), registry)

	// Start tracker - it will receive pre-existing beacons via WatchKey during initial sync
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), tracker))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), tracker))
	})

	// Wait for the tracker to publish at least one new beacon
	require.Eventually(t, func() bool {
		val, err := mkv.Get(propagationTrackerKey, GetPropagationTrackerCodec())
		if err != nil || val == nil {
			return false
		}
		desc := val.(*PropagationTrackerDesc)
		return len(desc.Beacons) > preExistingBeaconCount
	}, 5*time.Second, 50*time.Millisecond, "expected new beacon to be published")

	// Verify no delay was recorded in the histogram:
	// - Pre-existing beacons are skipped during initial sync (no delay recorded)
	// - The tracker's own beacons are marked as seen BEFORE publishing, so they're never "received"
	metricFamilies, err := metrics.NewMetricFamilyMapFromGatherer(registry)
	require.NoError(t, err)
	histogram, err := metrics.FindHistogramWithNameAndLabels(metricFamilies, "memberlist_propagation_tracker_delay_seconds")
	require.NoError(t, err)
	assert.Zero(t, histogram.GetSampleCount(), "pre-existing beacons and tracker's own beacons should not have delay recorded")
}

// TestPropagationTracker_PublishesFirstBeacon verifies that the tracker correctly
// publishes a beacon when the propagationTrackerKey doesn't exist in the KV store yet.
func TestPropagationTracker_PublishesFirstBeacon(t *testing.T) {
	mkv := createPropagationTrackerTestKV(t)

	// Verify the key doesn't exist initially
	val, err := mkv.Get(propagationTrackerKey, GetPropagationTrackerCodec())
	require.NoError(t, err)
	require.Nil(t, val, "expected key to not exist initially")

	// Create PropagationTracker with short beacon interval
	tracker := NewPropagationTracker(mkv, 100*time.Millisecond, time.Hour, log.NewNopLogger(), prometheus.NewPedanticRegistry())

	// Start tracker
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), tracker))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), tracker))
	})

	// Wait for first beacon to be published
	require.Eventually(t, func() bool {
		val, err := mkv.Get(propagationTrackerKey, GetPropagationTrackerCodec())
		if err != nil || val == nil {
			return false
		}
		desc, ok := val.(*PropagationTrackerDesc)
		if !ok {
			return false
		}
		return len(desc.Beacons) > 0
	}, 5*time.Second, 50*time.Millisecond, "expected beacon to be published")

	// Verify the beacon has a valid timestamp close to now
	val, err = mkv.Get(propagationTrackerKey, GetPropagationTrackerCodec())
	require.NoError(t, err)
	desc := val.(*PropagationTrackerDesc)
	for _, beacon := range desc.Beacons {
		require.InDelta(t, time.Now().UnixMilli(), beacon.PublishedAt, float64(10*time.Second.Milliseconds()))
	}
}

// TestPropagationTracker_OneBeaconPerIntervalPerCluster verifies that the beacon publishing rate is
// close to one beacon per interval across the entire cluster, even when multiple trackers
// are running. Due to the propagation delays, some duplicate beacons are expected when
// trackers race, but the rate should be much lower than if every tracker publishes a beacon
// every interval.
func TestPropagationTracker_OneBeaconPerIntervalPerCluster(t *testing.T) {
	mkv := createPropagationTrackerTestKV(t)

	const beaconInterval = 100 * time.Millisecond
	const numTrackers = 10

	// Start multiple trackers (simulating multiple nodes in a cluster)
	var trackers []*PropagationTracker
	for i := 0; i < numTrackers; i++ {
		tracker := NewPropagationTracker(mkv, beaconInterval, time.Hour, log.NewNopLogger(), prometheus.NewPedanticRegistry())
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), tracker))
		trackers = append(trackers, tracker)
	}

	// Let trackers run for some time
	const numIntervals = 10
	time.Sleep(numIntervals * beaconInterval)

	// Stop all trackers before checking to ensure no more beacons are added during verification
	for _, tracker := range trackers {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), tracker))
	}

	// Get the beacons and compute the actual time range covered
	val, err := mkv.Get(propagationTrackerKey, GetPropagationTrackerCodec())
	require.NoError(t, err)
	require.NotNil(t, val)

	desc := val.(*PropagationTrackerDesc)
	beaconCount := len(desc.Beacons)
	require.NotZero(t, beaconCount, "expected at least one beacon")

	// Find min and max timestamps to compute the actual time range
	var minTimestamp, maxTimestamp int64
	for _, beacon := range desc.Beacons {
		if minTimestamp == 0 || beacon.PublishedAt < minTimestamp {
			minTimestamp = beacon.PublishedAt
		}
		if beacon.PublishedAt > maxTimestamp {
			maxTimestamp = beacon.PublishedAt
		}
	}

	// Compute expected beacons based on actual time range
	actualDuration := time.Duration(maxTimestamp-minTimestamp) * time.Millisecond
	expectedBeacons := int(actualDuration/beaconInterval) + 1 // +1 for the first beacon

	// The ideal case is expectedBeacons (1 per interval).
	// The worst case is expectedBeacons * numTrackers (every tracker publishes every interval).
	// We allow up to 2x the ideal rate to account for propagation delays causing some duplicates.
	maxExpected := expectedBeacons * 2
	require.LessOrEqual(t, beaconCount, maxExpected,
		"expected at most %d beacons (2x ideal for %v duration), got %d", maxExpected, actualDuration, beaconCount)
}

// TestPropagationTracker_BeaconLifetimeGarbageCollection verifies that beacons older than
// beaconLifetime are automatically marked as tombstones, and then removed by the KV store's
// tombstone removal mechanism after LeftIngestersTimeout.
//
// Note: The KV store's Get() method removes all tombstones before returning, so we cannot
// directly observe the tombstone state via Get(). Instead, we verify that old beacons
// eventually disappear from the store (which requires them to first become tombstones).
func TestPropagationTracker_BeaconLifetimeGarbageCollection(t *testing.T) {
	// Create KV with short LeftIngestersTimeout for faster tombstone removal
	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	cfg.TCPTransport.BindAddrs = getLocalhostAddrs()
	cfg.Codecs = []codec.Codec{GetPropagationTrackerCodec()}
	cfg.LeftIngestersTimeout = 100 * time.Millisecond

	mkv := NewKV(cfg, log.NewNopLogger(), &staticDNSProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), mkv))
	})

	const beaconInterval = 50 * time.Millisecond
	const beaconLifetime = 200 * time.Millisecond

	registry := prometheus.NewPedanticRegistry()
	tracker := NewPropagationTracker(mkv, beaconInterval, beaconLifetime, log.NewNopLogger(), registry)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), tracker))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), tracker))
	})

	trackerCodec := GetPropagationTrackerCodec()

	// Wait for a beacon to be published
	var initialBeaconID uint64
	require.Eventually(t, func() bool {
		val, _ := mkv.Get(propagationTrackerKey, trackerCodec)
		if val == nil {
			return false
		}
		desc := val.(*PropagationTrackerDesc)
		for beaconID, beacon := range desc.Beacons {
			if beacon.DeletedAt == 0 {
				initialBeaconID = beaconID
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "beacon should be published")

	// Wait for the initial beacon to be garbage collected (marked as tombstone and then removed).
	// The beacon should be marked as tombstone after beaconLifetime (200ms), and then removed
	// after LeftIngestersTimeout (100ms), for a total of ~300ms minimum.
	// We can't observe the intermediate tombstone state via Get() since it strips tombstones.
	require.Eventually(t, func() bool {
		val, _ := mkv.Get(propagationTrackerKey, trackerCodec)
		if val == nil {
			return false
		}
		desc := val.(*PropagationTrackerDesc)
		_, exists := desc.Beacons[initialBeaconID]
		return !exists
	}, 5*time.Second, 10*time.Millisecond, "beacon should be garbage collected")
}

// TestPropagationTracker_SkipsNegativeDelay verifies that beacons with future timestamps
// (which can happen due to clock skew between nodes) do not have their delay recorded,
// since a negative delay would be meaningless.
func TestPropagationTracker_SkipsNegativeDelay(t *testing.T) {
	mkv := createPropagationTrackerTestKV(t)
	trackerCodec := GetPropagationTrackerCodec()

	// Start a tracker that will receive beacons via WatchKey.
	registry := prometheus.NewPedanticRegistry()
	tracker := NewPropagationTracker(mkv, time.Hour, time.Hour, log.NewNopLogger(), registry)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), tracker))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), tracker))
	})

	// Publish an initial beacon to trigger the tracker's initial sync.
	err := mkv.CAS(context.Background(), propagationTrackerKey, trackerCodec, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePropagationTrackerDesc(in)
		desc.Beacons[1] = BeaconDesc{PublishedAt: time.Now().UnixMilli()}
		return desc, true, nil
	})
	require.NoError(t, err)

	// Wait for the tracker to complete initial sync.
	require.Eventually(t, func() bool {
		return tracker.initialSyncDone.Load()
	}, 5*time.Second, 10*time.Millisecond, "tracker should complete initial sync")

	// Publish a beacon with a future timestamp (simulating clock skew from another node).
	futureTime := time.Now().Add(10 * time.Second)
	err = mkv.CAS(context.Background(), propagationTrackerKey, trackerCodec, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePropagationTrackerDesc(in)
		desc.Beacons[2] = BeaconDesc{PublishedAt: futureTime.UnixMilli()}
		return desc, true, nil
	})
	require.NoError(t, err)

	// Wait for the tracker to receive the beacon.
	time.Sleep(100 * time.Millisecond)

	// Verify no delay was recorded (negative delay should be skipped).
	metricFamilies, err := metrics.NewMetricFamilyMapFromGatherer(registry)
	require.NoError(t, err)
	histogram, err := metrics.FindHistogramWithNameAndLabels(metricFamilies, "memberlist_propagation_tracker_delay_seconds")
	require.NoError(t, err)
	assert.Zero(t, histogram.GetSampleCount(), "negative delay (future timestamp) should not be recorded")
}

// createPropagationTrackerTestKV creates and starts a KV store configured with the PropagationTracker codec.
// The KV store is automatically stopped when the test completes.
func createPropagationTrackerTestKV(t *testing.T) *KV {
	t.Helper()

	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	cfg.TCPTransport.BindAddrs = getLocalhostAddrs()
	cfg.Codecs = []codec.Codec{GetPropagationTrackerCodec()}

	mkv := NewKV(cfg, log.NewNopLogger(), &staticDNSProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), mkv))
	})

	return mkv
}
