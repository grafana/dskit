package middleware

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/dskit/clusterutil"
	"github.com/grafana/dskit/grpcutil"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var (
	errNoClusterProvided = grpcutil.Status(codes.Internal, "no cluster provided").Err()
)

// ClusterUnaryClientInterceptor propagates the given cluster label to gRPC metadata,
// before calling the next invoker.
// If no cluster label is provided, an errNoClusterProvided is returned.
// In case of an error related to the cluster label validation, the given
// non-nil prometheus.CounterVec is incremented.
func ClusterUnaryClientInterceptor(cluster string, invalidCluster *prometheus.CounterVec, logger log.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// We skip the gRPC health check.
		if method == healthpb.Health_Check_FullMethodName {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		if cluster == "" {
			level.Warn(logger).Log("msg", "no cluster provided", "method", method)
			invalidCluster.WithLabelValues(method, clusterutil.ReasonEmptyClusterLabel).Inc()
			return errNoClusterProvided
		}

		msgs, err := getClusterFromIncomingContext(ctx, method, cluster, false)
		if err != nil {
			if msgs != nil {
				level.Warn(logger).Log(msgs...)
			}
			invalidCluster.WithLabelValues(method, clusterutil.ReasonClient).Inc()
			return grpcutil.Status(codes.Internal, err.Error()).Err()
		}
		// The incoming context either contains no cluster verification label,
		// or it already contains one which is equal to the given cluster parameter.
		// In both cases we propagate the latter to the outgoing context.
		ctx = clusterutil.PutClusterIntoOutgoingContext(ctx, cluster)
		return handleError(invoker(ctx, method, req, reply, cc, opts...), cluster, method, invalidCluster, logger)
	}
}

func handleError(err error, cluster string, method string, invalidCluster *prometheus.CounterVec, logger log.Logger) error {
	if err == nil {
		return nil
	}
	if stat, ok := grpcutil.ErrorToStatus(err); ok {
		details := stat.Details()
		if len(details) == 1 {
			if errDetails, ok := details[0].(*grpcutil.ErrorDetails); ok {
				if errDetails.GetCause() == grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL {
					msg := fmt.Sprintf("request rejected by the server: %s", stat.Message())
					level.Warn(logger).Log("msg", msg, "method", method, "clusterVerificationLabel", cluster)
					invalidCluster.WithLabelValues(method, clusterutil.ReasonServer).Inc()
					return grpcutil.Status(codes.Internal, msg).Err()
				}
			}
		}
	}
	return err

}

// ClusterUnaryServerInterceptor checks if the incoming gRPC metadata contains any cluster label and if so, checks if
// the latter corresponds to the given cluster label. If it is the case, the request is further propagated.
// Otherwise, an error is returned.
func ClusterUnaryServerInterceptor(cluster string, logger log.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// We skip the gRPC health check.
		if _, ok := info.Server.(healthpb.HealthServer); ok {
			return handler(ctx, req)
		}

		if cluster == "" {
			if logger != nil {
				level.Warn(logger).Log("msg", "no cluster verification label sent to the interceptor", "method", info.FullMethod)
			}
			return nil, errNoClusterProvided
		}

		msgs, err := getClusterFromIncomingContext(ctx, info.FullMethod, cluster, true)
		if err == nil {
			return handler(ctx, req)
		}
		if msgs != nil {
			level.Warn(logger).Log(msgs...)
		}
		stat := grpcutil.Status(codes.FailedPrecondition, err.Error(), &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL})
		return nil, stat.Err()
	}
}

func getClusterFromIncomingContext(ctx context.Context, method string, expectedCluster string, failOnEmpty bool) ([]any, error) {
	reqCluster, err := clusterutil.GetClusterFromIncomingContext(ctx)
	if err == nil {
		if reqCluster == expectedCluster {
			return nil, nil
		}
		return []any{"msg", "rejecting request with wrong cluster verification label", "method", method, "clusterVerificationLabel", expectedCluster, "requestClusterVerificationLabel", reqCluster}, fmt.Errorf("rejected request with wrong cluster verification label %q - it should be %q", reqCluster, expectedCluster)
	}
	if errors.Is(err, clusterutil.ErrNoClusterVerificationLabel) {
		if !failOnEmpty {
			return nil, nil
		}
		return []any{"msg", "rejecting request with no cluster verification label", "method", method, "clusterVerificationLabel", expectedCluster}, fmt.Errorf("rejected request with empty cluster verification label - it should be %q", expectedCluster)
	}
	return []any{"msg", "rejecting request due to an error during cluster verification label extraction", "method", method, "clusterVerificationLabel", expectedCluster, "err", err}, fmt.Errorf("rejected request: %w", err)
}
