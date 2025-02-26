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
		incomingContext context.Context
		invokerError    error
		cluster         string
		expectedErr     error
		expectedMetrics string
		expectedLogs    string
		shouldPanic     bool
	}{
		"if no cluster label is set ClusterUnaryClientInterceptor panics": {
			incomingContext: context.Background(),
			cluster:         "",
			shouldPanic:     true,
		},
		"if cluster label is set it should be propagated to invoker": {
			incomingContext: context.Background(),
			cluster:         "cluster",
		},
		"if invoker returns a wrong cluster error it is handled by the interceptor": {
			incomingContext: context.Background(),
			cluster:         "cluster",
			invokerError:    grpcutil.Status(codes.FailedPrecondition, `request intended for cluster "cluster" - this is cluster "another-cluster"`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL}).Err(),
			expectedErr:     grpcutil.Status(codes.Internal, `request rejected by the server: request intended for cluster "cluster" - this is cluster "another-cluster"`).Err(),
			expectedMetrics: `
				# HELP test_request_invalid_cluster_verification_labels_total Number of requests with invalid cluster verification label.
				# TYPE test_request_invalid_cluster_verification_labels_total counter
				test_request_invalid_cluster_verification_labels_total{method="GET",reason="server_check_failed"} 1
			`,
			expectedLogs: `level=warn msg="request rejected by the server: request intended for cluster \"cluster\" - this is cluster \"another-cluster\"" method=GET clusterVerificationLabel=cluster`,
		},
		"if invoker returns a generic error the error is propagated": {
			incomingContext: context.Background(),
			cluster:         "cluster",
			invokerError:    genericErr,
			expectedErr:     genericErr,
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
			reg := prometheus.NewRegistry()
			interceptor := ClusterUnaryClientInterceptor(testCase.cluster, newRequestInvalidClusterVerficationLabelsTotalCounter(reg), logger)
			invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				return testCase.invokerError
			}

			err := interceptor(testCase.incomingContext, "GET", createRequest(t), nil, nil, invoker)
			if testCase.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, testCase.expectedErr, err)
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
		verifyErr       func(err error, softValidation bool)
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
		},
		"different request and server clusters give rise to an error with grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL cause if soft validation disabled": {
			incomingContext: clusterutil.NewIncomingContext(true, "wrong-cluster"),
			serverCluster:   "cluster",
			expectedLogs:    `level=warn msg="request with wrong cluster verification label" method=/Test/Me clusterVerificationLabel=cluster requestClusterVerificationLabel=wrong-cluster softValidation=%v`,
			verifyErr: func(err error, softValidation bool) {
				if !softValidation {
					require.Equal(t, grpcutil.Status(codes.FailedPrecondition, `rejected request with wrong cluster verification label "wrong-cluster" - it should be "cluster"`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL}).Err(), err)
				}
			},
		},
		"empty request cluster and non-empty server cluster give an error with grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL cause if soft validation disabled": {
			incomingContext: clusterutil.NewIncomingContext(true, ""),
			serverCluster:   "cluster",
			expectedLogs:    `level=warn msg="request with no cluster verification label" method=/Test/Me clusterVerificationLabel=cluster softValidation=%v`,
			verifyErr: func(err error, softValidation bool) {
				if !softValidation {
					require.Equal(t, grpcutil.Status(codes.FailedPrecondition, `rejected request with empty cluster verification label - it should be "cluster"`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL}).Err(), err)
				}
			},
		},
		"no request cluster and non-empty server cluster give an error with grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL cause if soft validation disabled": {
			incomingContext: clusterutil.NewIncomingContext(false, ""),
			serverCluster:   "cluster",
			expectedLogs:    `level=warn msg="request with no cluster verification label" method=/Test/Me clusterVerificationLabel=cluster softValidation=%v`,
			verifyErr: func(err error, softValidation bool) {
				if !softValidation {
					require.Equal(t, grpcutil.Status(codes.FailedPrecondition, `rejected request with empty cluster verification label - it should be "cluster"`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL}).Err(), err)
				}
			},
		},
		"if the incoming context contains more than one cluster labels an error is returned if soft validation disabled": {
			incomingContext: metadata.NewIncomingContext(context.Background(), map[string][]string{clusterutil.MetadataClusterVerificationLabelKey: {"cluster", "another-cluster"}}),
			serverCluster:   "cluster",
			expectedLogs:    `level=warn msg="detected error during cluster verification label extraction" method=/Test/Me clusterVerificationLabel=cluster softValidation=%v err="gRPC metadata should contain exactly 1 value for key \"x-cluster\", but it contains [cluster another-cluster]"`,
			verifyErr: func(err error, softValidation bool) {
				if !softValidation {
					require.Equal(t, grpcutil.Status(codes.FailedPrecondition, `rejected request: gRPC metadata should contain exactly 1 value for key "x-cluster", but it contains [cluster another-cluster]`, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL}).Err(), err)
				}
			},
		},
	}
	for testName, testCase := range testCases {
		for _, softValidation := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s softValidation=%v", testName, softValidation), func(t *testing.T) {
				defer func() {
					r := recover()
					require.Equal(t, testCase.shouldPanic, r != nil)
				}()
				buf := bytes.NewBuffer(nil)
				logger := createLogger(t, buf)
				interceptor := ClusterUnaryServerInterceptor(testCase.serverCluster, softValidation, logger)
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
			interceptor := ClusterUnaryServerInterceptor(goodCluster, false, log.NewNopLogger())
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
