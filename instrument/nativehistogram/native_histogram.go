// SPDX-License-Identifier: Apache-2.0

package nativehistogram

import "time"

const (
	// Define the default values for latency related native histogram configuration.
	LatencyBucketFactor     = 1.1
	LatencyMaxBucketNumber  = 100
	LatencyMinResetDuration = time.Hour
)
