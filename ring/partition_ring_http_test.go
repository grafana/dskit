package ring

import (
	_ "embed"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

	assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
		"<td>", "1", "</td>",
		"<td>", "Active", "</td>",
		"<td>", "[^<]+", "</td>",
		"<td>", "ingester-zone-a-0", "<br />", "ingester-zone-b-0", "<br />", "</td>",
	}, `\s*`))), recorder.Body.String())

	assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
		"<td>", "2", "</td>",
		"<td>", "Inactive", "</td>",
		"<td>", "[^<]+", "</td>",
		"<td>", "ingester-zone-a-1", "<br />", "ingester-zone-b-1", "<br />", "</td>",
	}, `\s*`))), recorder.Body.String())

	assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
		"<td>", "3", "</td>",
		"<td>", "Corrupt", "</td>",
		"<td>", "N/A", "</td>",
		"<td>", "ingester-zone-b-2", "<br />", "</td>",
	}, `\s*`))), recorder.Body.String())
}
