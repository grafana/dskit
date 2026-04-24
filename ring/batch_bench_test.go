package ring

import (
	"context"
	"fmt"
	"testing"
)

// mockDoBatchRing implements DoBatchRing for benchmarking purposes.
type mockDoBatchRing struct {
	instances     []InstanceDesc
	replicationFactor int
	numTokens     int
}

func newMockDoBatchRing(numInstances, replicationFactor, numTokens int) *mockDoBatchRing {
	instances := make([]InstanceDesc, numInstances)
	for i := range instances {
		instances[i] = InstanceDesc{
			Addr:  fmt.Sprintf("instance-%d", i),
			State: ACTIVE,
		}
	}
	return &mockDoBatchRing{
		instances:         instances,
		replicationFactor: replicationFactor,
		numTokens:         numTokens,
	}
}

func (r *mockDoBatchRing) Get(key uint32, _ Operation, bufInstances []InstanceDesc, _, _ []string) (ReplicationSet, error) {
	// Deterministic selection: pick replicationFactor instances based on key
	start := int(key) % len(r.instances)
	instances := bufInstances[:0]
	for j := 0; j < r.replicationFactor; j++ {
		idx := (start + j) % len(r.instances)
		instances = append(instances, r.instances[idx])
	}
	return ReplicationSet{
		Instances: instances,
		MaxErrors: r.replicationFactor / 2, // quorum
	}, nil
}

func (r *mockDoBatchRing) ReplicationFactor() int {
	return r.replicationFactor
}

func (r *mockDoBatchRing) InstancesCount() int {
	return len(r.instances)
}

func BenchmarkDoBatchWithOptions(b *testing.B) {
	// Production parameters based on metrics:
	// - cortex-prod-10: ~388 ingesters, RF=3
	// - Keys per request (series count) ranges from small (10) to large (1000+)
	type benchCase struct {
		name          string
		numKeys       int
		numInstances  int
		replicationFactor int
	}

	cases := []benchCase{
		// Small request, small ring
		{"keys=10/instances=30/rf=3", 10, 30, 3},
		// Small request, production-sized ring
		{"keys=10/instances=400/rf=3", 10, 400, 3},
		// Medium request, production-sized ring (common case)
		{"keys=100/instances=400/rf=3", 100, 400, 3},
		// Large request, production-sized ring
		{"keys=1000/instances=400/rf=3", 1000, 400, 3},
		// Very large request
		{"keys=5000/instances=400/rf=3", 5000, 400, 3},
		// Small ring with many keys
		{"keys=1000/instances=30/rf=3", 1000, 30, 3},
		// Large ring with few keys (many instances unused)
		{"keys=5/instances=400/rf=3", 5, 400, 3},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			ring := newMockDoBatchRing(tc.numInstances, tc.replicationFactor, 128)
			keys := make([]uint32, tc.numKeys)
			for i := range keys {
				keys[i] = uint32(i * 7) // spread keys around
			}
			ctx := context.Background()
			callback := func(_ InstanceDesc, _ []int) error {
				return nil
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := DoBatchWithOptions(ctx, WriteNoExtend, ring, keys, callback, DoBatchOptions{
					Cleanup:       func() {},
					IsClientError: isHTTPStatus4xx,
					Go:            func(f func()) { f() }, // synchronous execution to reduce noise
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
