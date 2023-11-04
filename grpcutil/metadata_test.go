package grpcutil

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestAppendMessageSizeToOutgoingContext(t *testing.T) {
	ctx := context.Background()

	sizer := fakeSizer(5)

	ctx = AppendMessageSizeToOutgoingContext(ctx, sizer)

	md, exists := metadata.FromOutgoingContext(ctx)
	require.True(t, exists)

	vals := md.Get(MetadataMessageSize)
	require.Len(t, vals, 1)
	require.Equal(t, strconv.Itoa(sizer.Size()), vals[0])
}

type fakeSizer int

func (f fakeSizer) Size() int {
	return int(f)
}
