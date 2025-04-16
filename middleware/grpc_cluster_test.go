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
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/dskit/clusterutil"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	dskitlog "github.com/grafana/dskit/log"
)

func TestClusterUnaryClientInterceptor(t *testing.T) {
	genericErr := errors.New("generic error")
	testCases := map[string]struct {
		invokerError    error
		cluster         string
		expectedErr     error
		expectedMetrics string
		expectedLogs    string
		shouldPanic     bool
	}{
		"if no cluster label is set ClusterUnaryClientInterceptor panics": {
			cluster:     "",
			shouldPanic: true,
		},
		"if cluster label is set it should be propagated to invoker": {
			cluster: "cluster",
		},
		"if invoker returns a wrong cluster error it is handled by the interceptor": {
			cluster:      "cluster",
			invokerError: grpcutil.Status(codes.FailedPrecondition, `request intended for cluster "cluster" - this is cluster "another-cluster"`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VALIDATION_LABEL}).Err(),
			expectedErr:  grpcutil.Status(codes.Internal, `request rejected by the server: request intended for cluster "cluster" - this is cluster "another-cluster"`).Err(),
			expectedMetrics: `
				# HELP test_request_invalid_cluster_validation_labels_total Number of requests with invalid cluster validation label.
				# TYPE test_request_invalid_cluster_validation_labels_total counter
				test_request_invalid_cluster_validation_labels_total{method="GET"} 1
			`,
			expectedLogs: `level=warn msg="request rejected by the server: request intended for cluster \"cluster\" - this is cluster \"another-cluster\"" method=GET cluster_validation_label=cluster`,
		},
		"if invoker returns a generic error the error is propagated": {
			cluster:      "cluster",
			invokerError: genericErr,
			expectedErr:  genericErr,
		},
	}
	verifyClusterPropagation := func(ctx context.Context, expectedCluster string) {
		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		clusterIDs, ok := md[clusterutil.MetadataClusterValidationLabelKey]
		require.True(t, ok)
		require.Len(t, clusterIDs, 1)
		require.Equal(t, expectedCluster, clusterIDs[0])
	}
	invalidClusterValidationReporter := func(cluster string, logger log.Logger, invalidClusterRequests *prometheus.CounterVec) InvalidClusterValidationReporter {
		return func(msg string, method string) {
			level.Warn(logger).Log("msg", msg, "method", method, "cluster_validation_label", cluster)
			invalidClusterRequests.WithLabelValues(method).Inc()
		}
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
			interceptor := ClusterUnaryClientInterceptor(testCase.cluster, invalidClusterValidationReporter(testCase.cluster, logger, newRequestInvalidClusterValidationLabelsTotalCounter(reg)))
			invoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				verifyClusterPropagation(ctx, testCase.cluster)
				return testCase.invokerError
			}

			err := interceptor(context.Background(), "GET", createRequest(t), nil, nil, invoker)
			if testCase.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, testCase.expectedErr, err)
			}
			// Check tracked Prometheus metrics
			err = testutil.GatherAndCompare(reg, strings.NewReader(testCase.expectedMetrics), "test_request_invalid_cluster_validation_labels_total")
			require.NoError(t, err)
			if testCase.expectedLogs == "" {
				require.Empty(t, buf.Bytes())
			} else {
				require.True(t, bytes.Contains(buf.Bytes(), []byte(testCase.expectedLogs)))
			}
		})
	}
}

func TestClusterUnaryServerInterceptor(t *testing.T) {
	testCases := map[string]struct {
		incomingContext context.Context
		serverCluster   string
		verifyErr       func(err error, softValidation bool)
		expectedLogs    string
		expectedMetrics string
		shouldPanic     bool
	}{
		"empty server cluster makes ClusterUnaryServerInterceptor panic": {
			incomingContext: newIncomingContext(false, ""),
			serverCluster:   "",
			shouldPanic:     true,
		},
		"equal request and server clusters give no error": {
			incomingContext: newIncomingContext(true, "cluster"),
			serverCluster:   "cluster",
		},
		"different request and server clusters give rise to an error with grpcutil.WRONG_CLUSTER_VALIDATION_LABEL cause if soft validation disabled": {
			incomingContext: newIncomingContext(true, "wrong-cluster"),
			serverCluster:   "cluster",
			expectedLogs:    `level=warn msg="request with wrong cluster validation label" method=/Test/Me cluster_validation_label=cluster request_cluster_validation_label=wrong-cluster soft_validation=%v`,
			expectedMetrics: `
                                # HELP server_invalid_cluster_validation_label_requests_total Number of requests received by server with invalid cluster validation label.
                                # TYPE server_invalid_cluster_validation_label_requests_total counter
                                server_invalid_cluster_validation_label_requests_total{cluster_validation_label="cluster", method="/Test/Me",protocol="grpc",request_cluster_validation_label="wrong-cluster"} 1
				`,
			verifyErr: func(err error, softValidation bool) {
				if !softValidation {
					require.Equal(t, grpcutil.Status(codes.FailedPrecondition, `rejected request with wrong cluster validation label "wrong-cluster" - it should be "cluster"`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VALIDATION_LABEL}).Err(), err)
				}
			},
		},
		"empty request cluster and non-empty server cluster give an error with grpcutil.WRONG_CLUSTER_VALIDATION_LABEL cause if soft validation disabled": {
			incomingContext: newIncomingContext(true, ""),
			serverCluster:   "cluster",
			expectedLogs:    `level=warn msg="request with no cluster validation label" method=/Test/Me cluster_validation_label=cluster soft_validation=%v`,
			expectedMetrics: `
                                # HELP server_invalid_cluster_validation_label_requests_total Number of requests received by server with invalid cluster validation label.
                                # TYPE server_invalid_cluster_validation_label_requests_total counter
                                server_invalid_cluster_validation_label_requests_total{cluster_validation_label="cluster",method="/Test/Me",protocol="grpc",request_cluster_validation_label=""} 1
				`,
			verifyErr: func(err error, softValidation bool) {
				if !softValidation {
					require.Equal(t, grpcutil.Status(codes.FailedPrecondition, `rejected request with empty cluster validation label - it should be "cluster"`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VALIDATION_LABEL}).Err(), err)
				}
			},
		},
		"no request cluster and non-empty server cluster give an error with grpcutil.WRONG_CLUSTER_VALIDATION_LABEL cause if soft validation disabled": {
			incomingContext: newIncomingContext(false, ""),
			serverCluster:   "cluster",
			expectedLogs:    `level=warn msg="request with no cluster validation label" method=/Test/Me cluster_validation_label=cluster soft_validation=%v`,
			expectedMetrics: `
                                # HELP server_invalid_cluster_validation_label_requests_total Number of requests received by server with invalid cluster validation label.
                                # TYPE server_invalid_cluster_validation_label_requests_total counter
                                server_invalid_cluster_validation_label_requests_total{cluster_validation_label="cluster",method="/Test/Me",protocol="grpc",request_cluster_validation_label=""} 1
				`,
			verifyErr: func(err error, softValidation bool) {
				if !softValidation {
					require.Equal(t, grpcutil.Status(codes.FailedPrecondition, `rejected request with empty cluster validation label - it should be "cluster"`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VALIDATION_LABEL}).Err(), err)
				}
			},
		},
		"if the incoming context contains more than one cluster label an error is returned if soft validation disabled": {
			incomingContext: metadata.NewIncomingContext(context.Background(), map[string][]string{clusterutil.MetadataClusterValidationLabelKey: {"cluster", "another-cluster"}}),
			serverCluster:   "cluster",
			expectedLogs:    `level=warn msg="detected error during cluster validation label extraction" method=/Test/Me cluster_validation_label=cluster soft_validation=%v err="gRPC metadata should contain exactly 1 value for key \"x-cluster\", but it contains [cluster another-cluster]"`,
			expectedMetrics: `
                                # HELP server_invalid_cluster_validation_label_requests_total Number of requests received by server with invalid cluster validation label.
                                # TYPE server_invalid_cluster_validation_label_requests_total counter
                                server_invalid_cluster_validation_label_requests_total{cluster_validation_label="cluster",method="/Test/Me",protocol="grpc",request_cluster_validation_label=""} 1
				`,
			verifyErr: func(err error, softValidation bool) {
				if !softValidation {
					require.Equal(t, grpcutil.Status(codes.FailedPrecondition, `rejected request: gRPC metadata should contain exactly 1 value for key "x-cluster", but it contains [cluster another-cluster]`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VALIDATION_LABEL}).Err(), err)
				}
			},
		},
	}
	for testName, testCase := range testCases {
		for _, softValidation := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s softValidation=%v", testName, softValidation), func(t *testing.T) {
				defer func() {
					r := recover()
					require.Equal(t, testCase.shouldPanic, r != nil, r)
				}()
				buf := bytes.NewBuffer(nil)
				logger := createLogger(t, buf)
				reg := prometheus.NewPedanticRegistry()
				interceptor := ClusterUnaryServerInterceptor(
					testCase.serverCluster, softValidation, NewInvalidClusterRequests(reg), logger,
				)
				handler := func(context.Context, interface{}) (interface{}, error) {
					return nil, nil
				}
				info := &grpc.UnaryServerInfo{FullMethod: "/Test/Me"}
				req := createRequest(t)
				_, err := interceptor(testCase.incomingContext, req, info, handler)
				if testCase.verifyErr != nil {
					testCase.verifyErr(err, softValidation)
				}
				if testCase.expectedLogs == "" {
					require.Empty(t, buf.Bytes())
				} else {
					require.True(t, bytes.Contains(buf.Bytes(), []byte(fmt.Sprintf(testCase.expectedLogs, softValidation))))
				}
				err = testutil.GatherAndCompare(reg, strings.NewReader(testCase.expectedMetrics), "server_invalid_cluster_validation_label_requests_total")
				require.NoError(t, err)
			})
		}
	}
}

func TestClusterUnaryServerInterceptorWithHealthServer(t *testing.T) {
	const goodCluster = "good-cluster"
	const badCluster = "bad-cluster"

	testCases := map[string]struct {
		serverInfo      *grpc.UnaryServerInfo
		incomingContext context.Context
		expectedMetrics string
		expectedError   error
	}{
		"UnaryServerInfo with healthpb.HealthServer does no cluster check": {
			// We create a UnaryServerInfo with grpc health server.
			serverInfo: &grpc.UnaryServerInfo{Server: health.NewServer(), FullMethod: "/Test/Me"},
			// We create a context with a bad cluster.
			incomingContext: newIncomingContext(true, badCluster),
			// Since UnaryServerInfo contains the grpc health server, no check is done, and we expect no errors.
			expectedError: nil,
		},
		"UnaryServerInfo without healthpb.HealthServer does cluster check": {
			// We create a UnaryServerInfo with grpc health server.
			serverInfo: &grpc.UnaryServerInfo{Server: nil, FullMethod: "/Test/Me"},
			// We create a context with a bad cluster.
			incomingContext: newIncomingContext(true, badCluster),
			expectedMetrics: `
                                # HELP server_invalid_cluster_validation_label_requests_total Number of requests received by server with invalid cluster validation label.
                                # TYPE server_invalid_cluster_validation_label_requests_total counter
                                server_invalid_cluster_validation_label_requests_total{cluster_validation_label="good-cluster",method="/Test/Me",protocol="grpc",request_cluster_validation_label="bad-cluster"} 1
			`,
			// Since UnaryServerInfo doesn't contain the grpc health server, the check is done, and we expect an error.
			expectedError: grpcutil.Status(codes.FailedPrecondition, `rejected request with wrong cluster validation label "bad-cluster" - it should be "good-cluster"`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VALIDATION_LABEL}).Err(),
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			invalidClusterRequests := NewInvalidClusterRequests(reg)
			interceptor := ClusterUnaryServerInterceptor(goodCluster, false, invalidClusterRequests, log.NewNopLogger())
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
			err = testutil.GatherAndCompare(reg, strings.NewReader(testCase.expectedMetrics), "server_invalid_cluster_validation_label_requests_total")
			require.NoError(t, err)
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

func newRequestInvalidClusterValidationLabelsTotalCounter(reg prometheus.Registerer) *prometheus.CounterVec {
	return promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name:        "test_request_invalid_cluster_validation_labels_total",
		Help:        "Number of requests with invalid cluster validation label.",
		ConstLabels: nil,
	}, []string{"method"})
}

func newIncomingContext(containsRequestCluster bool, requestCluster string) context.Context {
	ctx := context.Background()
	if !containsRequestCluster {
		return ctx
	}
	md := map[string][]string{
		clusterutil.MetadataClusterValidationLabelKey: {requestCluster},
	}
	return metadata.NewIncomingContext(ctx, md)
}
