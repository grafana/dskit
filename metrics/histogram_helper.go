package metrics

import (
	dto "github.com/prometheus/client_model/go"
)

// convertSpansAndDeltasToBuckets converts native histogram spans and deltas to a bucket index map
func convertSpansAndDeltasToBuckets(spans []*dto.BucketSpan, deltas []int64) map[int]uint64 {
	buckets := make(map[int]uint64)

	if len(spans) == 0 || len(deltas) == 0 {
		return buckets
	}

	deltaIndex := 0
	currentBucketIndex := 0
	var currentCount uint64 = 0

	for _, span := range spans {
		if span == nil {
			continue
		}

		// Move to the start of this span
		currentBucketIndex += int(span.GetOffset())

		// Process buckets in this span
		for i := uint32(0); i < span.GetLength() && deltaIndex < len(deltas); i++ {
			delta := deltas[deltaIndex]

			// Convert delta to absolute count
			if delta >= 0 {
				currentCount += uint64(delta)
			} else {
				if uint64(-delta) <= currentCount {
					currentCount -= uint64(-delta)
				} else {
					currentCount = 0 // Prevent underflow
				}
			}

			// Store the count for this bucket index (but only if non-zero)
			if currentCount > 0 {
				buckets[currentBucketIndex] = currentCount
			}

			currentBucketIndex++
			deltaIndex++
		}
	}

	return buckets
}
