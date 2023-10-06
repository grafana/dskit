package httpgrpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestAppendMessageSizeToOutgoingContext(t *testing.T) {
	ctx := context.Background()

	req := &HTTPRequest{
		Method: "GET",
		Url:    "/test",
	}

	ctx = AppendRequestMetadataToContext(ctx, req)

	md, exists := metadata.FromOutgoingContext(ctx)
	require.True(t, exists)

	require.Equal(t, []string{"GET"}, md.Get(MetadataMethod))
	require.Equal(t, []string{"/test"}, md.Get(MetadataURL))
}
