package ring

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
)

// TestPartitionDescComparison compares serialization sizes between PartitionDesc and PartitionDescNoMetadata
func TestPartitionDescComparison(t *testing.T) {
	fmt.Println("\n=== Comparison: PartitionDesc vs PartitionDescNoMetadata ===")

	// Test configurations
	testCases := []struct {
		name     string
		metadata map[string]string
	}{
		{
			name:     "No metadata",
			metadata: nil,
		},
		{
			name:     "Empty metadata",
			metadata: map[string]string{},
		},
		{
			name: "Single entry (partition_deactivated_via_debug_endpoint => true)",
			metadata: map[string]string{
				"partition_deactivated_via_debug_endpoint": "true",
			},
		},
		{
			name: "Multiple entries",
			metadata: map[string]string{
				"partition_deactivated_via_debug_endpoint": "true",
				"owner":    "team-a",
				"priority": "high",
			},
		},
	}

	// Common fields for both message types
	id := int32(1)
	state := PartitionActive
	stateTimestamp := int64(1700000000)
	tokens := []uint32{100, 200, 300}

	for _, tc := range testCases {
		fmt.Printf("=== Case: %s ===\n", tc.name)

		// Create PartitionDesc with metadata
		descWithMeta := &PartitionDesc{
			Id:             id,
			State:          state,
			StateTimestamp: stateTimestamp,
			Tokens:         tokens,
			Metadata:       tc.metadata,
		}

		// Create PartitionDescNoMetadata (no metadata field at all)
		descNoMeta := &PartitionDescNoMetadata{
			Id:             id,
			State:          state,
			StateTimestamp: stateTimestamp,
			Tokens:         tokens,
		}

		// Serialize both
		dataWithMeta, err := proto.Marshal(descWithMeta)
		if err != nil {
			t.Errorf("Failed to marshal PartitionDesc: %v", err)
			continue
		}

		dataNoMeta, err := proto.Marshal(descNoMeta)
		if err != nil {
			t.Errorf("Failed to marshal PartitionDescNoMetadata: %v", err)
			continue
		}

		// Compare sizes
		sizeWithMeta := len(dataWithMeta)
		sizeNoMeta := len(dataNoMeta)
		difference := sizeWithMeta - sizeNoMeta

		fmt.Printf("  PartitionDesc size:           %d bytes\n", sizeWithMeta)
		fmt.Printf("  PartitionDescNoMetadata size: %d bytes\n", sizeNoMeta)
		fmt.Printf("  Difference:                   %d bytes\n", difference)

		if difference > 0 {
			fmt.Printf("  → PartitionDesc is %d bytes larger\n", difference)
		} else if difference < 0 {
			fmt.Printf("  → PartitionDescNoMetadata is %d bytes larger\n", -difference)
		} else {
			fmt.Printf("  → Both messages are the same size\n")
		}

		// Show hex for small messages
		if sizeWithMeta <= 100 {
			fmt.Printf("  PartitionDesc hex:           %x\n", dataWithMeta)
		}
		if sizeNoMeta <= 100 {
			fmt.Printf("  PartitionDescNoMetadata hex: %x\n", dataNoMeta)
		}

		fmt.Println()
	}
}
