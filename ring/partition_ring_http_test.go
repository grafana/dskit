package ring

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/services"
)

func TestPartitionRingPageHandler_ViewPage(t *testing.T) {
	handler := NewPartitionRingPageHandler(
		newStaticPartitionRingReader(
			NewPartitionRing(PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {
						State:          PartitionActive,
						StateTimestamp: time.Now().Unix(),
						Tokens:         []uint32{1000000, 3000000, 6000000},
					},
					2: {
						State:          PartitionInactive,
						StateTimestamp: time.Now().Unix(),
						Tokens:         []uint32{2000000, 4000000, 5000000, 7000000},
					},
				},
				Owners: map[string]OwnerDesc{
					"ingester-zone-a-0": {
						OwnedPartition: 1,
					},
					"ingester-zone-a-1": {
						OwnedPartition: 2,
					},
					"ingester-zone-b-0": {
						OwnedPartition: 1,
					},
					"ingester-zone-b-1": {
						OwnedPartition: 2,
					},

					// Simulate a corrupted partition, with a dangling owner but no partition.
					"ingester-zone-b-2": {
						OwnedPartition: 3,
					},
				},
			}),
		),
		nil,
	)

	t.Run("displays expected partition info", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/partition-ring", nil))

		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<td>", "1", "</td>",
			"<td>", "Active", "</td>",
			"<td>", "[^<]+", "</td>",
			"<td>", "ingester-zone-a-0", "<br />", "ingester-zone-b-0", "<br />", "</td>",
			"<td>", "3", "</td>",
			"<td>", "99.9%", "</td>",
		}, `\s*`))), recorder.Body.String())

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<td>", "2", "</td>",
			"<td>", "Inactive", "</td>",
			"<td>", "[^<]+", "</td>",
			"<td>", "ingester-zone-a-1", "<br />", "ingester-zone-b-1", "<br />", "</td>",
			"<td>", "4", "</td>",
			"<td>", "0.0931%", "</td>",
		}, `\s*`))), recorder.Body.String())

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<td>", "3", "</td>",
			"<td>", "Corrupt", "</td>",
			"<td>", "N/A", "</td>",
			"<td>", "ingester-zone-b-2", "<br />", "</td>",
			"<td>", "0", "</td>",
			"<td>", "0%", "</td>",
		}, `\s*`))), recorder.Body.String())
	})

	t.Run("displays Show Tokens button by default", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/partition-ring", nil))

		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			`<input type="button" value="Show Tokens" onclick="window.location.href = '\?tokens=true'"/>`,
		}, `\s*`))), recorder.Body.String())
	})

	t.Run("displays tokens when Show Tokens is enabled", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/partition-ring?tokens=true", nil))

		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			`<input type="button" value="Hide Tokens" onclick="window.location.href = '\?tokens=false'"/>`,
		}, `\s*`))), recorder.Body.String())

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<h2>", "Partition: 1", "</h2>",
			"<p>", "Tokens:<br/>", "1000000", "3000000", "6000000", "</p>",
		}, `\s*`))), recorder.Body.String())

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<h2>", "Partition: 2", "</h2>",
			"<p>", "Tokens:<br/>", "2000000", "4000000", "5000000", "7000000", "</p>",
		}, `\s*`))), recorder.Body.String())

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<h2>", "Partition: 3", "</h2>",
			"<p>", "Tokens:<br/>", "</p>",
		}, `\s*`))), recorder.Body.String())
	})
}

func TestPartitionRingPageHandler_ChangePartitionState(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Init the ring.
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.AddPartition(1, PartitionActive, time.Now())
		desc.AddPartition(2, PartitionActive, time.Now())
		return desc, true, nil
	}))

	// Init the watcher.
	watcher := NewPartitionRingWatcher(testRingName, ringKey, store, logger, nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, watcher))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, watcher))
	})

	// Init the handler.
	editor := NewPartitionRingEditor(ringKey, store)
	handler := NewPartitionRingPageHandler(watcher, editor)

	t.Run("should fail if the partition ID is invalid", func(t *testing.T) {
		data := url.Values{}
		data.Set("action", "change_state")
		data.Set("partition_id", "xxx")
		data.Set("partition_state", PartitionActive.String())

		req := httptest.NewRequest(http.MethodPost, "/partition-ring", strings.NewReader(data.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)

		assert.Equal(t, http.StatusBadRequest, recorder.Code)
		assert.Contains(t, recorder.Body.String(), "invalid partition ID")
	})

	t.Run("should fail if the partition does not exist", func(t *testing.T) {
		data := url.Values{}
		data.Set("action", "change_state")
		data.Set("partition_id", "100")
		data.Set("partition_state", PartitionActive.String())

		req := httptest.NewRequest(http.MethodPost, "/partition-ring", strings.NewReader(data.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)

		assert.Equal(t, http.StatusBadRequest, recorder.Code)
		assert.Contains(t, recorder.Body.String(), ErrPartitionDoesNotExist.Error())
	})

	t.Run("should fail if the state is invalid", func(t *testing.T) {
		data := url.Values{}
		data.Set("action", "change_state")
		data.Set("partition_id", "1")
		data.Set("partition_state", "xxx")

		req := httptest.NewRequest(http.MethodPost, "/partition-ring", strings.NewReader(data.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)

		assert.Equal(t, http.StatusBadRequest, recorder.Code)
		assert.Contains(t, recorder.Body.String(), "invalid partition state")
	})

	t.Run("should fail if the state change is not allowed", func(t *testing.T) {
		data := url.Values{}
		data.Set("action", "change_state")
		data.Set("partition_id", "1")
		data.Set("partition_state", PartitionPending.String())

		req := httptest.NewRequest(http.MethodPost, "/partition-ring", strings.NewReader(data.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)

		assert.Equal(t, http.StatusBadRequest, recorder.Code)
		assert.Contains(t, recorder.Body.String(), ErrPartitionStateChangeNotAllowed.Error())
	})

	t.Run("should succeed if the state change is allowed", func(t *testing.T) {
		data := url.Values{}
		data.Set("action", "change_state")
		data.Set("partition_id", "1")
		data.Set("partition_state", PartitionInactive.String())

		req := httptest.NewRequest(http.MethodPost, "/partition-ring", strings.NewReader(data.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		recorder := httptest.NewRecorder()

		// Pre-condition check.
		require.Equal(t, PartitionActive, getPartitionStateFromStore(t, store, ringKey, 1))
		require.Equal(t, PartitionActive, getPartitionStateFromStore(t, store, ringKey, 2))

		handler.ServeHTTP(recorder, req)
		assert.Equal(t, http.StatusFound, recorder.Code)

		require.Equal(t, PartitionInactive, getPartitionStateFromStore(t, store, ringKey, 1))
		require.Equal(t, PartitionActive, getPartitionStateFromStore(t, store, ringKey, 2))
	})

	t.Run("should also lock the state change if 'change_lock' is used", func(t *testing.T) {
		// Reset partition 1 to Active and state_change=unlocked
		require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
			desc := GetOrCreatePartitionRingDesc(in)
			desc.Partitions[1] = PartitionDesc{
				Id:             1,
				State:          PartitionActive,
				StateTimestamp: time.Now().Unix(),
				Tokens:         desc.Partitions[1].Tokens,
			}
			return desc, true, nil
		}))

		reqTest := func(lock bool) (*httptest.ResponseRecorder, *http.Request) {
			data := url.Values{}
			data.Set("action", "change_lock")
			data.Set("partition_id", "1")
			data.Set("locked", strconv.FormatBool(lock))

			req := httptest.NewRequest(http.MethodPost, "/partition-ring", strings.NewReader(data.Encode()))
			req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

			return httptest.NewRecorder(), req
		}

		// Test lock
		recorder, reqLock := reqTest(true)

		handler.ServeHTTP(recorder, reqLock)
		assert.Equal(t, http.StatusFound, recorder.Code)

		desc := getPartitionRingFromStore(t, store, ringKey)
		require.True(t, desc.Partitions[1].StateChangeLocked)

		// Test unlock
		recorder, reqUnlock := reqTest(false)

		handler.ServeHTTP(recorder, reqUnlock)
		assert.Equal(t, http.StatusFound, recorder.Code)

		desc = getPartitionRingFromStore(t, store, ringKey)
		require.False(t, desc.Partitions[1].StateChangeLocked)

	})

	t.Run("should also lock the state change if 'change_state_and_lock' is used", func(t *testing.T) {
		// Reset partition 1 to Active and state_change=unlocked
		require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
			desc := GetOrCreatePartitionRingDesc(in)
			desc.Partitions[1] = PartitionDesc{
				Id:             1,
				State:          PartitionActive,
				StateTimestamp: time.Now().Unix(),
				Tokens:         desc.Partitions[1].Tokens,
			}
			return desc, true, nil
		}))

		data := url.Values{}
		data.Set("action", "change_state_and_lock")
		data.Set("partition_id", "1")
		data.Set("partition_state", PartitionInactive.String())

		req := httptest.NewRequest(http.MethodPost, "/partition-ring", strings.NewReader(data.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		recorder := httptest.NewRecorder()

		// Pre-condition check.
		require.Equal(t, PartitionActive, getPartitionStateFromStore(t, store, ringKey, 1))

		handler.ServeHTTP(recorder, req)
		assert.Equal(t, http.StatusFound, recorder.Code)

		require.Equal(t, PartitionInactive, getPartitionStateFromStore(t, store, ringKey, 1))

		// Check if locked
		desc := getPartitionRingFromStore(t, store, ringKey)
		require.True(t, desc.Partitions[1].StateChangeLocked)
	})
}
