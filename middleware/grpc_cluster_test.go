package middleware

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/gogo/status"
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
			invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
				if testCase.expectedClusterFromContext != "" {
					verify(ctx, testCase.expectedClusterFromContext)
				}
				return nil
			}

			interceptor(context.Background(), "GET", createRequest(t), nil, nil, invoker)
		})
	}
}

func TestClusterUnaryServerInterceptor(t *testing.T) {
	testCases := map[string]struct {
		requestCluster string
		serverCluster  string
		expectedError  error
	}{
		"equal request and server clusters give no error": {
			requestCluster: "cluster",
			serverCluster:  "cluster",
			expectedError:  nil,
		},
		"different request and server clusters give rise to an error": {
			requestCluster: "wrong-cluster",
			serverCluster:  "cluster",
			expectedError:  status.Error(codes.FailedPrecondition, "request intended for cluster \"wrong-cluster\" - this is cluster \"cluster\""),
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			logger := log.NewLogfmtLogger(os.Stdin)
			interceptor := ClusterUnaryServerInterceptor(testCase.serverCluster, logger)
			handler := func(context.Context, interface{}) (interface{}, error) {
				return nil, nil
			}

			md := map[string][]string{
				MetadataClusterKey: {testCase.requestCluster},
			}
			ctx := metadata.NewIncomingContext(context.Background(), md)
			info := &grpc.UnaryServerInfo{FullMethod: "/Test/Me"}
			req := createRequest(t)
			_, err := interceptor(ctx, req, info, handler)
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
