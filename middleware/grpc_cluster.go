package middleware

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/dskit/clusterutil"
	"github.com/grafana/dskit/grpcutil"
)

const (
	failureClient = "client"
	failureServer = "server"
)

// ClusterUnaryClientInterceptor propagates the given cluster info to gRPC metadata.
func ClusterUnaryClientInterceptor(cluster string, invalidCluster *prometheus.CounterVec, logger log.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		reqCluster, ok := clusterutil.GetClusterFromIncomingContext(ctx, logger)
		if ok && reqCluster != cluster {
			// If the incoming context already contains a cluster verification label,
			// but it is different from the expected one, we increase the metrics,
			// and return an error.
			msg := fmt.Sprintf("wrong cluster verification label in the incoming context: %s, expected: %s", reqCluster, cluster)
			if logger != nil {
				level.Warn(logger).Log("msg", msg, "clusterVerificationLabel", cluster, "requestClusterVerificationLabel", reqCluster)
			}
			if invalidCluster != nil {
				invalidCluster.WithLabelValues("grpc", method, cluster, failureClient).Inc()
			}
			return grpcutil.Status(codes.InvalidArgument, msg).Err()
		}

		// We include the cluster verification label in the outgoing context.
		if cluster != "" {
			ctx = clusterutil.PutClusterIntoOutgoingContext(ctx, cluster)
		}

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
					if logger != nil {
						level.Warn(logger).Log("msg", msg, "cluster", cluster, "method", method)
					}
					if invalidCluster != nil {
						invalidCluster.WithLabelValues("grpc", method, cluster, failureServer).Inc()
					}
					return grpcutil.Status(codes.InvalidArgument, msg).Err()
				}
			}
		}
	}
	return err

}

// ClusterUnaryServerInterceptor checks if the incoming gRPC metadata contains any cluster information and if so,
// checks if the latter corresponds to the given cluster. If it is the case, the request is further propagated.
// Otherwise, an error is returned. In that case, non-nil invalidClusters counter is increased.
func ClusterUnaryServerInterceptor(cluster string, invalidClusters *prometheus.CounterVec, logger log.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if _, ok := info.Server.(healthpb.HealthServer); ok {
			return handler(ctx, req)
		}
		reqCluster, _ := clusterutil.GetClusterFromIncomingContext(ctx, logger)
		if cluster != reqCluster {
			if logger != nil {
				level.Warn(logger).Log("msg", "rejecting request with wrong cluster verification label", "clusterVerificationLabel", cluster, "requestClusterVerificationLabel", reqCluster, "method", info.FullMethod)
			}
			if invalidClusters != nil {
				invalidClusters.WithLabelValues("grpc", info.FullMethod, reqCluster).Inc()
			}
			msg := fmt.Sprintf("request intended for cluster %q - this is cluster %q", reqCluster, cluster)
			stat := grpcutil.Status(codes.FailedPrecondition, msg, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL})
			return nil, stat.Err()
		}
		return handler(ctx, req)
	}
}
