package middleware

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"google.golang.org/grpc/health"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestClusterUnaryClientInterceptor(t *testing.T) {
	testCases := map[string]struct {
		cluster                    string
		expectedClusterFromContext string
	}{
		"no cluster info sets no cluster info in context": {
			cluster:                    "",
			expectedClusterFromContext: "",
		},
		"if cluster info is set, it should be propagated to invoker": {
			cluster:                    "cluster",
			expectedClusterFromContext: "cluster",
		},
	}
	verify := func(ctx context.Context, expectedCluster string) {
		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		clusterIDs, ok := md[MetadataClusterKey]
		require.True(t, ok)
		require.Len(t, clusterIDs, 1)
		require.Equal(t, expectedCluster, clusterIDs[0])
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			interceptor := ClusterUnaryClientInterceptor(testCase.cluster)
			invoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				if testCase.expectedClusterFromContext != "" {
					verify(ctx, testCase.expectedClusterFromContext)
				}
				return nil
			}

			err := interceptor(context.Background(), "GET", createRequest(t), nil, nil, invoker)
			require.NoError(t, err)
		})
	}
}

func TestClusterUnaryServerInterceptor(t *testing.T) {
	testCases := map[string]struct {
		incomingContext context.Context
		serverCluster   string
		expectedError   error
	}{
		"equal request and server clusters give no error": {
			incomingContext: createIncomingContext(true, "cluster"),
			serverCluster:   "cluster",
			expectedError:   nil,
		},
		"different request and server clusters give rise to an error": {
			incomingContext: createIncomingContext(true, "wrong-cluster"),
			serverCluster:   "cluster",
			expectedError:   grpcutil.Status(codes.FailedPrecondition, "request intended for cluster \"wrong-cluster\" - this is cluster \"cluster\"", &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_NAME}).Err(),
		},
		"empty request cluster and non-empty server cluster give rise to an error": {
			incomingContext: createIncomingContext(true, ""),
			serverCluster:   "cluster",
			expectedError:   grpcutil.Status(codes.FailedPrecondition, "request intended for cluster \"\" - this is cluster \"cluster\"", &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_NAME}).Err(),
		},
		"no request cluster and non-empty server cluster give rise to an error": {
			incomingContext: createIncomingContext(false, ""),
			serverCluster:   "cluster",
			expectedError:   grpcutil.Status(codes.FailedPrecondition, "request intended for cluster \"\" - this is cluster \"cluster\"", &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_NAME}).Err(),
		},
		"empty request cluster and empty server cluster give no error": {
			incomingContext: createIncomingContext(true, ""),
			serverCluster:   "",
			expectedError:   nil,
		},
		"no request cluster and empty server cluster give no error": {
			incomingContext: createIncomingContext(false, ""),
			serverCluster:   "",
			expectedError:   nil,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			logger := log.NewLogfmtLogger(os.Stdin)
			interceptor := ClusterUnaryServerInterceptor(testCase.serverCluster, nil, logger)
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

func TestHeTestClusterUnaryServerInterceptorWithHealthServer(t *testing.T) {
	goodCluster := "good-cluster"
	badCluster := "bad-cluster"

	testCases := map[string]struct {
		serverInfo      *grpc.UnaryServerInfo
		incomingContext context.Context
		expectedError   error
	}{
		"UnaryServerInfo with healthpb.HealthServer does no cluster check": {
			// We create a UnaryServerInfo with grpc health server.
			serverInfo: &grpc.UnaryServerInfo{Server: health.NewServer(), FullMethod: "/Test/Me"},
			// We create a context with a bad cluster.
			incomingContext: createIncomingContext(true, badCluster),
			// Since UnaryServerInfo contains the grpc health server, no check is done, and we expect no errors.
			expectedError: nil,
		},
		"UnaryServerInfo without healthpb.HealthServer does cluster check": {
			// We create a UnaryServerInfo with grpc health server.
			serverInfo: &grpc.UnaryServerInfo{Server: nil, FullMethod: "/Test/Me"},
			// We create a context with a bad cluster.
			incomingContext: createIncomingContext(true, badCluster),
			// Since UnaryServerInfo doesn't contain the grpc health server, the check is done, and we expect an error.
			expectedError: grpcutil.Status(codes.FailedPrecondition, "request intended for cluster \"bad-cluster\" - this is cluster \"good-cluster\"", &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_NAME}).Err(),
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			logger := log.NewLogfmtLogger(os.Stdin)
			interceptor := ClusterUnaryServerInterceptor(goodCluster, nil, logger)
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

func createIncomingContext(containsRequestCluster bool, requestCluster string) context.Context {
	ctx := context.Background()
	if !containsRequestCluster {
		return ctx
	}
	md := map[string][]string{
		MetadataClusterKey: {requestCluster},
	}
	return metadata.NewIncomingContext(ctx, md)
}

func createRequest(t *testing.T) *httpgrpc.HTTPRequest {
	r, err := http.NewRequest("POST", "/i/am/calling/you", strings.NewReader("some body"))
	require.NoError(t, err)
	req, err := httpgrpc.FromHTTPRequest(r)
	require.NoError(t, err)
	return req
}
