package grpcutil

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/dskit/httpgrpc"
)

func TestAppendMessageSizeToOutgoingContext(t *testing.T) {
	ctx := context.Background()

	req := &httpgrpc.HTTPRequest{
		Method: "GET",
		Url:    "/test",
	}

	ctx = AppendMessageSizeToOutgoingContext(ctx, req)

	md, exists := metadata.FromOutgoingContext(ctx)
	require.True(t, exists)

	vals := md.Get(MetadataMessageSize)
	require.Len(t, vals, 1)
	require.Equal(t, strconv.Itoa(req.Size()), vals[0])
}
