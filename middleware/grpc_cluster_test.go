package middleware

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/dskit/clusterutil"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	dskitlog "github.com/grafana/dskit/log"
)

func TestClusterUnaryClientInterceptor(t *testing.T) {
	genericErr := errors.New("generic error")
	testCases := map[string]struct {
		incomingContext            context.Context
		invokerError               error
		cluster                    string
		expectedClusterFromContext string
		expectedError              error
		expectedMetrics            string
		expectedLogs               string
		shouldPanic                bool
	}{
		"if no cluster label is set, ClusterUnaryClientInterceptor panics": {
			incomingContext: context.Background(),
			cluster:         "",
			shouldPanic:     true,
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
			expectedError:   grpcutil.Status(codes.Internal, `rejected request with wrong cluster verification label "cached-cluster" - it should be "cluster"`).Err(),
			expectedMetrics: `
				# HELP test_request_invalid_cluster_verification_labels_total Number of requests with invalid cluster verification label.
				# TYPE test_request_invalid_cluster_verification_labels_total counter
				test_request_invalid_cluster_verification_labels_total{method="GET",reason="client_check_failed"} 1
			`,
			expectedLogs: `level=warn msg="rejecting request with wrong cluster verification label" method=GET clusterVerificationLabel=cluster requestClusterVerificationLabel=cached-cluster`,
		},
		"if the incoming context contains cluster label, it must be equal to the set cluster label": {
			incomingContext:            clusterutil.NewIncomingContext(true, "cluster"),
			cluster:                    "cluster",
			expectedClusterFromContext: "cluster",
			expectedError:              nil,
		},
		"if the incoming context contains more than one cluster labels, an error is returned": {
			incomingContext: metadata.NewIncomingContext(context.Background(), map[string][]string{clusterutil.MetadataClusterVerificationLabelKey: {"cluster", "another-cluster"}}),
			cluster:         "cluster",
			expectedError:   grpcutil.Status(codes.Internal, `rejected request: gRPC metadata should contain exactly 1 value for key "x-cluster", but it contains [cluster another-cluster]`).Err(),
			expectedMetrics: `
				# HELP test_request_invalid_cluster_verification_labels_total Number of requests with invalid cluster verification label.
				# TYPE test_request_invalid_cluster_verification_labels_total counter
				test_request_invalid_cluster_verification_labels_total{method="GET",reason="client_check_failed"} 1
			`,
			expectedLogs: `level=warn msg="rejecting request due to an error during cluster verification label extraction" method=GET clusterVerificationLabel=cluster err="gRPC metadata should contain exactly 1 value for key \"x-cluster\", but it contains [cluster another-cluster]"`,
		},
		"if invoker returns a wrong cluster error, it is handled by the interceptor": {
			incomingContext: context.Background(),
			cluster:         "cluster",
			invokerError:    grpcutil.Status(codes.FailedPrecondition, `request intended for cluster "cluster" - this is cluster "another-cluster"`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL}).Err(),
			expectedError:   grpcutil.Status(codes.Internal, `request rejected by the server: request intended for cluster "cluster" - this is cluster "another-cluster"`).Err(),
			expectedMetrics: `
				# HELP test_request_invalid_cluster_verification_labels_total Number of requests with invalid cluster verification label.
				# TYPE test_request_invalid_cluster_verification_labels_total counter
				test_request_invalid_cluster_verification_labels_total{method="GET",reason="server_check_failed"} 1
			`,
			expectedLogs: `level=warn msg="request rejected by the server: request intended for cluster \"cluster\" - this is cluster \"another-cluster\"" method=GET clusterVerificationLabel=cluster`,
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
			defer func() {
				r := recover()
				require.Equal(t, testCase.shouldPanic, r != nil)
			}()
			buf := bytes.NewBuffer(nil)
			logger := createLogger(t, buf)
			reg := prometheus.NewRegistry()
			interceptor := ClusterUnaryClientInterceptor(testCase.cluster, newRequestInvalidClusterVerficationLabelsTotalCounter(reg), logger)
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
			// Check tracked Prometheus metrics
			err = testutil.GatherAndCompare(reg, strings.NewReader(testCase.expectedMetrics), "test_request_invalid_cluster_verification_labels_total")
			assert.NoError(t, err)
			if testCase.expectedLogs == "" {
				require.Empty(t, buf.Bytes())
			} else {
				require.True(t, bytes.Contains(buf.Bytes(), []byte(testCase.expectedLogs)))
			}
		})
	}
}

func TestClusterUnaryClientInterceptorWithHealthServer(t *testing.T) {
	err := fmt.Errorf("this is an error")
	failingInvoker := func(_ context.Context, method string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		if method != healthpb.Health_Check_FullMethodName {
			return err
		}
		return nil
	}
	testCases := map[string]struct {
		incomingContext context.Context
		method          string
		expectedError   error
	}{
		"calls to healthpb.Health_Check_FullMethodName are ignored": {
			// We create a context with no cluster label.
			incomingContext: context.Background(),
			method:          healthpb.Health_Check_FullMethodName,
			// Since we call healthpb.Health_Check_FullMethodName, the failing invoker doesn't fail, and we expect no errors.
			expectedError: nil,
		},
		"calls to endpoints different from healthpb.Health_Check_FullMethodName are executed": {
			// We create a context with no cluster label.
			incomingContext: context.Background(),
			method:          "/Test/Me",
			// Since we don't call healthpb.Health_Check_FullMethodName, the failing invoker fails, and we expect an error.
			expectedError: err,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			interceptor := ClusterUnaryClientInterceptor("cluster", newRequestInvalidClusterVerficationLabelsTotalCounter(prometheus.NewRegistry()), log.NewNopLogger())
			err := interceptor(testCase.incomingContext, testCase.method, createRequest(t), nil, nil, failingInvoker)
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
		expectedLogs    string
		shouldPanic     bool
	}{
		"empty server cluster make ClusterUnaryServerInterceptor panic": {
			incomingContext: clusterutil.NewIncomingContext(false, ""),
			serverCluster:   "",
			shouldPanic:     true,
		},
		"equal request and server clusters give no error": {
			incomingContext: clusterutil.NewIncomingContext(true, "cluster"),
			serverCluster:   "cluster",
			expectedError:   nil,
		},
		"different request and server clusters give rise to an error with grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL cause": {
			incomingContext: clusterutil.NewIncomingContext(true, "wrong-cluster"),
			serverCluster:   "cluster",
			expectedLogs:    "level=warn msg=\"rejecting request with wrong cluster verification label\" method=/Test/Me clusterVerificationLabel=cluster requestClusterVerificationLabel=wrong-cluster",
			expectedError:   grpcutil.Status(codes.FailedPrecondition, `rejected request with wrong cluster verification label "wrong-cluster" - it should be "cluster"`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL}).Err(),
		},
		"empty request cluster and non-empty server cluster give no error": {
			incomingContext: clusterutil.NewIncomingContext(true, ""),
			serverCluster:   "cluster",
			expectedLogs:    "level=warn msg=\"processing request with no cluster verification label\" method=/Test/Me clusterVerificationLabel=cluster",
		},
		"no request cluster and non-empty server cluster give no error": {
			incomingContext: clusterutil.NewIncomingContext(false, ""),
			serverCluster:   "cluster",
			expectedLogs:    "level=warn msg=\"processing request with no cluster verification label\" method=/Test/Me clusterVerificationLabel=cluster",
		},
		"if the incoming context contains more than one cluster labels, an error is returned": {
			incomingContext: metadata.NewIncomingContext(context.Background(), map[string][]string{clusterutil.MetadataClusterVerificationLabelKey: {"cluster", "another-cluster"}}),
			serverCluster:   "cluster",
			expectedError:   grpcutil.Status(codes.FailedPrecondition, `rejected request: gRPC metadata should contain exactly 1 value for key "x-cluster", but it contains [cluster another-cluster]`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL}).Err(),
			expectedLogs:    "level=warn msg=\"rejecting request due to an error during cluster verification label extraction\" method=/Test/Me clusterVerificationLabel=cluster err=\"gRPC metadata should contain exactly 1 value for key \\\"x-cluster\\\", but it contains [cluster another-cluster]\"",
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			defer func() {
				r := recover()
				require.Equal(t, testCase.shouldPanic, r != nil)
			}()
			buf := bytes.NewBuffer(nil)
			logger := createLogger(t, buf)
			interceptor := ClusterUnaryServerInterceptor(testCase.serverCluster, logger)
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
			if testCase.expectedLogs == "" {
				require.Empty(t, buf.Bytes())
			} else {
				require.True(t, bytes.Contains(buf.Bytes(), []byte(testCase.expectedLogs)))
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
			expectedError: grpcutil.Status(codes.FailedPrecondition, `rejected request with wrong cluster verification label "bad-cluster" - it should be "good-cluster"`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL}).Err(),
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			interceptor := ClusterUnaryServerInterceptor(goodCluster, log.NewNopLogger())
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

func createLogger(t *testing.T, buf *bytes.Buffer) log.Logger {
	var lvl dskitlog.Level
	require.NoError(t, lvl.Set("warn"))
	return dskitlog.NewGoKitWithWriter(dskitlog.LogfmtFormat, buf)
}

func createRequest(t *testing.T) *httpgrpc.HTTPRequest {
	r, err := http.NewRequest("POST", "/i/am/calling/you", strings.NewReader("some body"))
	require.NoError(t, err)
	req, err := httpgrpc.FromHTTPRequest(r)
	require.NoError(t, err)
	return req
}

func newRequestInvalidClusterVerficationLabelsTotalCounter(reg prometheus.Registerer) *prometheus.CounterVec {
	return promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name:        "test_request_invalid_cluster_verification_labels_total",
		Help:        "Number of requests with invalid cluster verification label.",
		ConstLabels: nil,
	}, []string{"method", "reason"})
}
