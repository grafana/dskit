package user

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestExtractFromGRPCRequest(t *testing.T) {
	t.Run("should return error if no org ID is set in the gRPC request context", func(t *testing.T) {
		inputCtx := context.Background()

		_, returnedCtx, err := ExtractFromGRPCRequest(inputCtx)
		assert.Equal(t, inputCtx, returnedCtx)
		assert.Equal(t, ErrNoOrgID, err)
	})

	t.Run("should return a context with org ID injected if org ID is set in the gRPC request context", func(t *testing.T) {
		inputCtx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
			lowerOrgIDHeaderName: "user-1",
		}))

		// Pre-condition check: no org ID should be set in the input context.
		_, err := ExtractOrgID(inputCtx)
		assert.Equal(t, ErrNoOrgID, err)

		actualOrgID, returnedCtx, err := ExtractFromGRPCRequest(inputCtx)
		assert.NotEqual(t, inputCtx, returnedCtx)
		assert.NoError(t, err)
		assert.Equal(t, "user-1", actualOrgID)

		// Org ID should be set in the returned context.
		actualOrgID, err = ExtractOrgID(returnedCtx)
		assert.NoError(t, err)
		assert.Equal(t, "user-1", actualOrgID)
	})
}

func BenchmarkExtractFromGRPCRequest(b *testing.B) {
	// The following fixture has been generated looking at the actual metadata received by Grafana Mimir.
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		"authority":            "1.1.1.1",
		"content-type":         "application/grpc",
		"grpc-accept-encoding": "snappy,gzip",
		"uber-trace-id":        "xxx",
		"user-agent":           "grpc-go/1.61.1",
		lowerOrgIDHeaderName:   "user-1",
	}))

	for n := 0; n < b.N; n++ {
		orgID, _, err := ExtractFromGRPCRequest(ctx)
		if orgID != "user-1" {
			b.Fatalf("unexpected org ID: %s", orgID)
		}
		if err != nil {
			b.Fatal(err)
		}
	}
}
