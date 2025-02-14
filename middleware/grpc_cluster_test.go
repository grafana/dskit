package middleware

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/dskit/clusterutil"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
)

func TestClusterUnaryClientInterceptor(t *testing.T) {
	genericErr := errors.New("generic error")
	testCases := map[string]struct {
		incomingContext            context.Context
		invokerError               error
		cluster                    string
		expectedClusterFromContext string
		expectedError              error
	}{
		"if no cluster label is set, an errNoClusterProvided error is returned": {
			incomingContext: context.Background(),
			cluster:         "",
			expectedError:   grpcutil.Status(codes.InvalidArgument, "no cluster provided").Err(),
		},
		"if cluster label is set, and the incoming context contains no cluster label, the former should be propagated to invoker": {
			incomingContext:            context.Background(),
			cluster:                    "cluster",
			expectedClusterFromContext: "cluster",
			expectedError:              nil,
		},
		"if the incoming context contains cluster label different from the set cluster label, an error is returned": {
			incomingContext: clusterutil.NewIncomingContext(true, "cached-cluster"),
			cluster:         "cluster",
			expectedError:   grpcutil.Status(codes.InvalidArgument, "wrong cluster verification label in the incoming context: cached-cluster, expected: cluster").Err(),
		},
		"if the incoming context contains cluster label, it must be equal to the set cluster label": {
			incomingContext:            clusterutil.NewIncomingContext(true, "cluster"),
			cluster:                    "cluster",
			expectedClusterFromContext: "cluster",
			expectedError:              nil,
		},
		"if the incoming context contains more than one cluster labels, ErrDifferentClusterVerificationLabelPresent error is returned": {
			incomingContext: metadata.NewIncomingContext(context.Background(), map[string][]string{clusterutil.MetadataClusterVerificationLabelKey: {"cluster", "another-cluster"}}),
			cluster:         "cluster",
			expectedError:   clusterutil.ErrDifferentClusterVerificationLabelPresent,
		},
		"if invoker returns a wrong cluster error, it is handled by the interceptor": {
			incomingContext: context.Background(),
			cluster:         "cluster",
			invokerError:    grpcutil.Status(codes.FailedPrecondition, "request intended for cluster cluster - this is cluster another-cluster", &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL}).Err(),
			expectedError:   grpcutil.Status(codes.InvalidArgument, "request rejected by the server: request intended for cluster cluster - this is cluster another-cluster").Err(),
		},
		"if invoker returns a generic error, the error is propagated": {
			incomingContext: context.Background(),
			cluster:         "cluster",
			invokerError:    genericErr,
			expectedError:   genericErr,
		},
	}
	verify := func(ctx context.Context, expectedCluster string) {
		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		clusterIDs, ok := md[clusterutil.MetadataClusterVerificationLabelKey]
		require.True(t, ok)
		require.Len(t, clusterIDs, 1)
		require.Equal(t, expectedCluster, clusterIDs[0])
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			interceptor := ClusterUnaryClientInterceptor(testCase.cluster, nil, nil)
			invoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				if testCase.expectedClusterFromContext != "" {
					verify(ctx, testCase.expectedClusterFromContext)
				}
				return testCase.invokerError
			}

			err := interceptor(testCase.incomingContext, "GET", createRequest(t), nil, nil, invoker)
			if testCase.expectedError == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, testCase.expectedError, err)
			}
		})
	}
}

func TestClusterUnaryServerInterceptor(t *testing.T) {
	testCases := map[string]struct {
		incomingContext context.Context
		serverCluster   string
		expectedError   error
	}{
		"empty server cluster gives an errNoClusterProvided error": {
			incomingContext: clusterutil.NewIncomingContext(false, ""),
			serverCluster:   "",
			expectedError:   errNoClusterProvided,
		},
		"equal request and server clusters give no error": {
			incomingContext: clusterutil.NewIncomingContext(true, "cluster"),
			serverCluster:   "cluster",
			expectedError:   nil,
		},
		"different request and server clusters give rise to an error": {
			incomingContext: clusterutil.NewIncomingContext(true, "wrong-cluster"),
			serverCluster:   "cluster",
			expectedError:   grpcutil.Status(codes.FailedPrecondition, `server from cluster "cluster" rejected request intended for cluster "wrong-cluster"`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL}).Err(),
		},
		"empty request cluster and non-empty server cluster give rise to an error": {
			incomingContext: clusterutil.NewIncomingContext(true, ""),
			serverCluster:   "cluster",
			expectedError:   grpcutil.Status(codes.FailedPrecondition, `server from cluster "cluster" rejected request intended for cluster ""`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL}).Err(),
		},
		"no request cluster and non-empty server cluster give rise to an error": {
			incomingContext: clusterutil.NewIncomingContext(false, ""),
			serverCluster:   "cluster",
			expectedError:   grpcutil.Status(codes.FailedPrecondition, `server from cluster "cluster" rejected request with no cluster verification label`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL}).Err(),
		},
		"if the incoming context contains more than one cluster labels, ErrDifferentClusterVerificationLabelPresent error is returned": {
			incomingContext: metadata.NewIncomingContext(context.Background(), map[string][]string{clusterutil.MetadataClusterVerificationLabelKey: {"cluster", "another-cluster"}}),
			serverCluster:   "cluster",
			expectedError:   clusterutil.ErrDifferentClusterVerificationLabelPresent,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			interceptor := ClusterUnaryServerInterceptor(testCase.serverCluster, nil, log.NewNopLogger())
			handler := func(context.Context, interface{}) (interface{}, error) {
				return nil, nil
			}
			info := &grpc.UnaryServerInfo{FullMethod: "/Test/Me"}
			req := createRequest(t)
			_, err := interceptor(testCase.incomingContext, req, info, handler)
			if testCase.expectedError == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, testCase.expectedError, err)
			}
		})
	}
}

func TestClusterUnaryServerInterceptorWithHealthServer(t *testing.T) {
	const goodCluster = "good-cluster"
	const badCluster = "bad-cluster"

	testCases := map[string]struct {
		serverInfo      *grpc.UnaryServerInfo
		incomingContext context.Context
		expectedError   error
	}{
		"UnaryServerInfo with healthpb.HealthServer does no cluster check": {
			// We create a UnaryServerInfo with grpc health server.
			serverInfo: &grpc.UnaryServerInfo{Server: health.NewServer(), FullMethod: "/Test/Me"},
			// We create a context with a bad cluster.
			incomingContext: clusterutil.NewIncomingContext(true, badCluster),
			// Since UnaryServerInfo contains the grpc health server, no check is done, and we expect no errors.
			expectedError: nil,
		},
		"UnaryServerInfo without healthpb.HealthServer does cluster check": {
			// We create a UnaryServerInfo with grpc health server.
			serverInfo: &grpc.UnaryServerInfo{Server: nil, FullMethod: "/Test/Me"},
			// We create a context with a bad cluster.
			incomingContext: clusterutil.NewIncomingContext(true, badCluster),
			// Since UnaryServerInfo doesn't contain the grpc health server, the check is done, and we expect an error.
			expectedError: grpcutil.Status(codes.FailedPrecondition, `server from cluster "good-cluster" rejected request intended for cluster "bad-cluster"`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL}).Err(),
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			interceptor := ClusterUnaryServerInterceptor(goodCluster, nil, log.NewNopLogger())
			handler := func(context.Context, interface{}) (interface{}, error) {
				return nil, nil
			}
			req := createRequest(t)
			_, err := interceptor(testCase.incomingContext, req, testCase.serverInfo, handler)
			if testCase.expectedError == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, testCase.expectedError, err)
			}
		})
	}
}

func createRequest(t *testing.T) *httpgrpc.HTTPRequest {
	r, err := http.NewRequest("POST", "/i/am/calling/you", strings.NewReader("some body"))
	require.NoError(t, err)
	req, err := httpgrpc.FromHTTPRequest(r)
	require.NoError(t, err)
	return req
}
