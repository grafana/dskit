package ring

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
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

		// Check partition 1 - just verify the basic structure and that owners are present
		assert.Contains(t, recorder.Body.String(), "<td>1</td>")
		assert.Contains(t, recorder.Body.String(), "Active")
		assert.Contains(t, recorder.Body.String(), "ingester-zone-a-0")
		assert.Contains(t, recorder.Body.String(), "ingester-zone-b-0")
		assert.Contains(t, recorder.Body.String(), "<td>3</td>")
		assert.Contains(t, recorder.Body.String(), "<td>99.9%</td>")

		// Check partition 2
		assert.Contains(t, recorder.Body.String(), "<td>2</td>")
		assert.Contains(t, recorder.Body.String(), "Inactive")
		assert.Contains(t, recorder.Body.String(), "ingester-zone-a-1")
		assert.Contains(t, recorder.Body.String(), "ingester-zone-b-1")
		assert.Contains(t, recorder.Body.String(), "<td>4</td>")
		assert.Contains(t, recorder.Body.String(), "<td>0.0931%</td>")

		// Check partition 3 (corrupted)
		assert.Contains(t, recorder.Body.String(), "<td>3</td>")
		assert.Contains(t, recorder.Body.String(), "Corrupt")
		assert.Contains(t, recorder.Body.String(), "ingester-zone-b-2")
		assert.Contains(t, recorder.Body.String(), "<td>0</td>")
		assert.Contains(t, recorder.Body.String(), "<td>0%</td>")
	})

	t.Run("displays Remove buttons for each owner", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/partition-ring", nil))

		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))

		// Check that Remove buttons are present for owners of partition 1
		assert.Regexp(t, regexp.MustCompile(`(?s)ingester-zone-a-0.*?<button name="action" value="remove_owner" type="submit">Remove</button>`), recorder.Body.String())
		assert.Regexp(t, regexp.MustCompile(`(?s)ingester-zone-b-0.*?<button name="action" value="remove_owner" type="submit">Remove</button>`), recorder.Body.String())

		// Check that Remove buttons are present for owners of partition 2
		assert.Regexp(t, regexp.MustCompile(`(?s)ingester-zone-a-1.*?<button name="action" value="remove_owner" type="submit">Remove</button>`), recorder.Body.String())
		assert.Regexp(t, regexp.MustCompile(`(?s)ingester-zone-b-1.*?<button name="action" value="remove_owner" type="submit">Remove</button>`), recorder.Body.String())
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
}

func TestPartitionRingPageHandler_RemovePartitionOwner(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()

	store, closer := consul.NewInMemoryClient(GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Init the ring with partitions and owners.
	require.NoError(t, store.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := GetOrCreatePartitionRingDesc(in)
		desc.AddPartition(1, PartitionActive, time.Now())
		desc.AddPartition(2, PartitionActive, time.Now())
		desc.AddOrUpdateOwner("ingester-zone-a-0", OwnerActive, 1, time.Now())
		desc.AddOrUpdateOwner("ingester-zone-a-1", OwnerActive, 1, time.Now())
		desc.AddOrUpdateOwner("ingester-zone-b-0", OwnerActive, 2, time.Now())
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
		data.Set("action", "remove_owner")
		data.Set("partition_id", "xxx")
		data.Set("owner_id", "ingester-zone-a-0")

		req := httptest.NewRequest(http.MethodPost, "/partition-ring", strings.NewReader(data.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)

		assert.Equal(t, http.StatusBadRequest, recorder.Code)
		assert.Contains(t, recorder.Body.String(), "invalid partition ID")
	})

	t.Run("should fail if the owner ID is missing", func(t *testing.T) {
		data := url.Values{}
		data.Set("action", "remove_owner")
		data.Set("partition_id", "1")
		data.Set("owner_id", "")

		req := httptest.NewRequest(http.MethodPost, "/partition-ring", strings.NewReader(data.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)

		assert.Equal(t, http.StatusBadRequest, recorder.Code)
		assert.Contains(t, recorder.Body.String(), "missing owner ID")
	})

	t.Run("should succeed removing an existing owner", func(t *testing.T) {
		data := url.Values{}
		data.Set("action", "remove_owner")
		data.Set("partition_id", "1")
		data.Set("owner_id", "ingester-zone-a-0")

		req := httptest.NewRequest(http.MethodPost, "/partition-ring", strings.NewReader(data.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		recorder := httptest.NewRecorder()

		// Pre-condition check: owner should exist.
		require.True(t, getPartitionRingFromStore(t, store, ringKey).HasOwner("ingester-zone-a-0"))
		require.True(t, getPartitionRingFromStore(t, store, ringKey).HasOwner("ingester-zone-a-1"))
		require.True(t, getPartitionRingFromStore(t, store, ringKey).HasOwner("ingester-zone-b-0"))

		handler.ServeHTTP(recorder, req)
		assert.Equal(t, http.StatusFound, recorder.Code)

		// Post-condition check: owner should be removed.
		require.False(t, getPartitionRingFromStore(t, store, ringKey).HasOwner("ingester-zone-a-0"))
		require.True(t, getPartitionRingFromStore(t, store, ringKey).HasOwner("ingester-zone-a-1"))
		require.True(t, getPartitionRingFromStore(t, store, ringKey).HasOwner("ingester-zone-b-0"))
	})

	t.Run("should succeed even if the owner doesn't exist", func(t *testing.T) {
		data := url.Values{}
		data.Set("action", "remove_owner")
		data.Set("partition_id", "1")
		data.Set("owner_id", "non-existent-owner")

		req := httptest.NewRequest(http.MethodPost, "/partition-ring", strings.NewReader(data.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		recorder := httptest.NewRecorder()

		// Pre-condition check: owner should not exist.
		require.False(t, getPartitionRingFromStore(t, store, ringKey).HasOwner("non-existent-owner"))

		handler.ServeHTTP(recorder, req)
		assert.Equal(t, http.StatusFound, recorder.Code)

		// Post-condition check: still doesn't exist, and it's okay.
		require.False(t, getPartitionRingFromStore(t, store, ringKey).HasOwner("non-existent-owner"))
	})
}
