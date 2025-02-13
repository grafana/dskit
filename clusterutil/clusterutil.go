package clusterutil

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"
)

const (
	// ClusterVerificationLabelHeader is the name of the cluster verification label HTTP header.
	ClusterVerificationLabelHeader = "X-Cluster"

	// MetadataClusterVerificationLabelKey is the key of the cluster verification label gRPC metadata.
	MetadataClusterVerificationLabelKey = "x-cluster"
)

var (
	ErrNoClusterVerificationLabel               = errors.New("no cluster verification label in context")
	ErrDifferentClusterVerificationLabelPresent = errors.New("different cluster verification label already present in header")
)

type clusterContextKey string

func NewIncomingContext(containsRequestCluster bool, requestCluster string) context.Context {
	ctx := context.Background()
	if !containsRequestCluster {
		return ctx
	}
	md := map[string][]string{
		MetadataClusterVerificationLabelKey: {requestCluster},
	}
	return metadata.NewIncomingContext(ctx, md)
}

// PutClusterIntoOutgoingContext returns a new context with the provided value
// for MetadataClusterVerificationLabelKey merged with any existing metadata in the context.
func PutClusterIntoOutgoingContext(ctx context.Context, cluster string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, MetadataClusterVerificationLabelKey, cluster)
}

// GetClusterFromIncomingContext returns the metadata value corresponding to the metadata
// key MetadataClusterVerificationLabelKey from the incoming metadata if it exists.
func GetClusterFromIncomingContext(ctx context.Context, logger log.Logger) (string, bool) {
	clusterIDs := metadata.ValueFromIncomingContext(ctx, MetadataClusterVerificationLabelKey)
	if len(clusterIDs) != 1 {
		if logger != nil {
			msg := fmt.Sprintf("gRPC metadata should contain exactly 1 value for key %q, but the current set of values is %v. Returning an empty string.", MetadataClusterVerificationLabelKey, clusterIDs)
			level.Warn(logger).Log("msg", msg)
		}
		return "", false
	}
	return clusterIDs[0], true
}
