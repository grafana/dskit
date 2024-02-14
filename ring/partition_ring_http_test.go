package ring

import (
	_ "embed"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionRingPageHandler(t *testing.T) {
	handler := NewPartitionRingPageHandler(
		newStaticPartitionRingReader(
			NewPartitionRing(PartitionRingDesc{
				Partitions: map[int32]PartitionDesc{
					1: {
						State:          PartitionActive,
						StateTimestamp: time.Now().Unix(),
					},
					2: {
						State:          PartitionInactive,
						StateTimestamp: time.Now().Unix(),
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
	)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/partition-ring", nil))

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))

	doc, err := goquery.NewDocumentFromReader(recorder.Body)
	require.NoError(t, err)

	rows := doc.Find("table tbody tr")
	require.Equal(t, 3, rows.Length())

	row := doc.Find("table tbody tr:nth-child(1)")
	require.Equal(t, 1, row.Length())
	assert.Equal(t, "1", strings.TrimSpace(row.Find("td:nth-child(1)").Text()))
	assert.Equal(t, "Active", strings.TrimSpace(row.Find("td:nth-child(2)").Text()))
	assert.Contains(t, strings.TrimSpace(row.Find("td:nth-child(4)").Text()), "ingester-zone-a-0")
	assert.Contains(t, strings.TrimSpace(row.Find("td:nth-child(4)").Text()), "ingester-zone-b-0")

	row = doc.Find("table tbody tr:nth-child(2)")
	require.Equal(t, 1, row.Length())
	assert.Equal(t, "2", strings.TrimSpace(row.Find("td:nth-child(1)").Text()))
	assert.Equal(t, "Inactive", strings.TrimSpace(row.Find("td:nth-child(2)").Text()))
	assert.Contains(t, strings.TrimSpace(row.Find("td:nth-child(4)").Text()), "ingester-zone-a-1")
	assert.Contains(t, strings.TrimSpace(row.Find("td:nth-child(4)").Text()), "ingester-zone-b-1")

	row = doc.Find("table tbody tr:nth-child(3)")
	require.Equal(t, 1, row.Length())
	assert.Equal(t, "3", strings.TrimSpace(row.Find("td:nth-child(1)").Text()))
	assert.Equal(t, "Corrupted", strings.TrimSpace(row.Find("td:nth-child(2)").Text()))
	assert.Contains(t, strings.TrimSpace(row.Find("td:nth-child(4)").Text()), "ingester-zone-b-2")
}
