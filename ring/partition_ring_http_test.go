package ring

import (
	_ "embed"
	"net/http"
	"net/http/httptest"
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
				},
			}),
		),
	)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/partition-ring", nil))

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))
}

type staticPartitionRingReader struct {
	ring *PartitionRing
}

func newStaticPartitionRingReader(ring *PartitionRing) *staticPartitionRingReader {
	return &staticPartitionRingReader{
		ring: ring,
	}
}

func (r *staticPartitionRingReader) PartitionRing() *PartitionRing {
	return r.ring
}
