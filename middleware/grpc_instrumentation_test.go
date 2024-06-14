// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/middleware/grpc_instrumentation_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package middleware

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/dskit/metrics"
)

const (
	errMsg = "fail"
)

func TestInstrumentationLabel_StatusCodeToString(t *testing.T) {
	testCases := map[string]struct {
		statusCode                        codes.Code
		reportGRPCCodes                   bool
		expectedStatusWithMaskingEnabled  string
		expectedStatusWithMaskingDisabled string
	}{
		"when reportGRPCCodes is enabled, codes.OK always returns codes.OK": {
			reportGRPCCodes:                   true,
			statusCode:                        codes.OK,
			expectedStatusWithMaskingEnabled:  codes.OK.String(),
			expectedStatusWithMaskingDisabled: codes.OK.String(),
		},
		"when reportGRPCCodes is disabled, codes.OK  returns 2xx with masking enabled, and success with masking disabled": {
			reportGRPCCodes:                   false,
			statusCode:                        codes.OK,
			expectedStatusWithMaskingEnabled:  "2xx",
			expectedStatusWithMaskingDisabled: "success",
		},
		"when reportGRPCCodes is enabled, HTTP status 500 returns 5xx with masking enabled and 500 with masking disabled": {
			reportGRPCCodes:                   true,
			statusCode:                        http.StatusInternalServerError,
			expectedStatusWithMaskingEnabled:  "5xx",
			expectedStatusWithMaskingDisabled: "500",
		},
		"when reportGRPCCodes is disabled, HTTP status 500 returns 5xx with masking enabled and 500 with masking disabled": {
			reportGRPCCodes:                   false,
			statusCode:                        http.StatusInternalServerError,
			expectedStatusWithMaskingEnabled:  "5xx",
			expectedStatusWithMaskingDisabled: "500",
		},
		"when reportGRPCCodes is enabled, HTTP status 503 returns 5xx with masking enabled and 503 with masking disabled": {
			reportGRPCCodes:                   true,
			statusCode:                        http.StatusServiceUnavailable,
			expectedStatusWithMaskingEnabled:  "5xx",
			expectedStatusWithMaskingDisabled: "503",
		},
		"when reportGRPCCodes is disabled, HTTP status 503 returns 5xx with masking enabled and 503 with masking disabled": {
			reportGRPCCodes:                   false,
			statusCode:                        http.StatusServiceUnavailable,
			expectedStatusWithMaskingEnabled:  "5xx",
			expectedStatusWithMaskingDisabled: "503",
		},
		"when reportGRPCCodes is enabled, HTTP status 400 returns 4xx with masking enabled and 400 with masking disabled": {
			reportGRPCCodes:                   true,
			statusCode:                        http.StatusBadRequest,
			expectedStatusWithMaskingEnabled:  "4xx",
			expectedStatusWithMaskingDisabled: "400",
		},
		"when reportGRPCCodes is disabled, HTTP status 400 returns 4xx with masking enabled and 400 with masking disabled": {
			reportGRPCCodes:                   false,
			statusCode:                        http.StatusBadRequest,
			expectedStatusWithMaskingEnabled:  "4xx",
			expectedStatusWithMaskingDisabled: "400",
		},
		"when reportGRPCCodes is enabled, HTTP status 429 returns 4xx with masking enabled and 420 with masking disabled": {
			reportGRPCCodes:                   true,
			statusCode:                        http.StatusTooManyRequests,
			expectedStatusWithMaskingEnabled:  "4xx",
			expectedStatusWithMaskingDisabled: "429",
		},
		"when reportGRPCCodes is disabled, HTTP status 429 returns 4xx with masking enabled and 420 with masking disabled": {
			reportGRPCCodes:                   false,
			statusCode:                        http.StatusTooManyRequests,
			expectedStatusWithMaskingEnabled:  "4xx",
			expectedStatusWithMaskingDisabled: "429",
		},
		"when reportGRPCCodes is enabled, gRPC status codes.Internal always returns codes.Internal": {
			reportGRPCCodes:                   true,
			statusCode:                        codes.Internal,
			expectedStatusWithMaskingEnabled:  codes.Internal.String(),
			expectedStatusWithMaskingDisabled: codes.Internal.String(),
		},
		"when reportGRPCCodes is disabled, gRPC status codes.Internal always returns error": {
			reportGRPCCodes:                   false,
			statusCode:                        codes.Internal,
			expectedStatusWithMaskingEnabled:  "error",
			expectedStatusWithMaskingDisabled: "error",
		},
		"when reportGRPCCodes is enabled, gRPC status codes.Unavailable always returns codes.Unavailable": {
			reportGRPCCodes:                   true,
			statusCode:                        codes.Unavailable,
			expectedStatusWithMaskingEnabled:  codes.Unavailable.String(),
			expectedStatusWithMaskingDisabled: codes.Unavailable.String(),
		},
		"when reportGRPCCodes is disabled, gRPC status codes.Unavailable always returns error": {
			reportGRPCCodes:                   false,
			statusCode:                        codes.Unavailable,
			expectedStatusWithMaskingEnabled:  "error",
			expectedStatusWithMaskingDisabled: "error",
		},
		"when reportGRPCCodes is enabled, gRPC status codes.FailedPrecondition always returns codes.FailedPrecondition": {
			reportGRPCCodes:                   true,
			statusCode:                        codes.FailedPrecondition,
			expectedStatusWithMaskingEnabled:  codes.FailedPrecondition.String(),
			expectedStatusWithMaskingDisabled: codes.FailedPrecondition.String(),
		},
		"when reportGRPCCodes is disabled, gRPC status codes.FailedPrecondition always returns error": {
			reportGRPCCodes:                   false,
			statusCode:                        codes.FailedPrecondition,
			expectedStatusWithMaskingEnabled:  "error",
			expectedStatusWithMaskingDisabled: "error",
		},
		"when reportGRPCCodes is enabled, gRPC status codes.ResourceExhausted always returns codes.ResourceExhausted": {
			reportGRPCCodes:                   true,
			statusCode:                        codes.ResourceExhausted,
			expectedStatusWithMaskingEnabled:  codes.ResourceExhausted.String(),
			expectedStatusWithMaskingDisabled: codes.ResourceExhausted.String(),
		},
		"when reportGRPCCodes is disabled, gRPC status codes.ResourceExhausted always returns error": {
			reportGRPCCodes:                   false,
			statusCode:                        codes.ResourceExhausted,
			expectedStatusWithMaskingEnabled:  "error",
			expectedStatusWithMaskingDisabled: "error",
		},
		"when reportGRPCCodes is enabled, gRPC status Codes.Canceled always returns codes.Canceled": {
			reportGRPCCodes:                   true,
			statusCode:                        codes.Canceled,
			expectedStatusWithMaskingEnabled:  codes.Canceled.String(),
			expectedStatusWithMaskingDisabled: codes.Canceled.String(),
		},
		"when reportGRPCCodes is disabled, gRPC status Codes.Canceled always returns cancel": {
			reportGRPCCodes:                   false,
			statusCode:                        codes.Canceled,
			expectedStatusWithMaskingEnabled:  "cancel",
			expectedStatusWithMaskingDisabled: "cancel",
		},
		"when reportGRPCCodes is enabled, gRPC status codes.Unknown always codes.Unknown": {
			reportGRPCCodes:                   true,
			statusCode:                        codes.Unknown,
			expectedStatusWithMaskingEnabled:  codes.Unknown.String(),
			expectedStatusWithMaskingDisabled: codes.Unknown.String(),
		},
		"when reportGRPCCodes is disabled, gRPC status codes.Unknown always returns error": {
			reportGRPCCodes:                   false,
			statusCode:                        codes.Unknown,
			expectedStatusWithMaskingEnabled:  "error",
			expectedStatusWithMaskingDisabled: "error",
		},
	}
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			instrumentationLabel := instrumentationLabel{reportGRPCStatus: testData.reportGRPCCodes}

			instrumentationLabel.maskHTTPStatus = true
			statusCode := instrumentationLabel.statusCodeToString(testData.statusCode)
			require.Equal(t, testData.expectedStatusWithMaskingEnabled, statusCode)

			instrumentationLabel.maskHTTPStatus = false
			statusCode = instrumentationLabel.statusCodeToString(testData.statusCode)
			require.Equal(t, testData.expectedStatusWithMaskingDisabled, statusCode)
		})
	}
}

func TestInstrumentationLabel_ErrorToStatusCode(t *testing.T) {
	testCases := map[string]struct {
		err                     error
		expectedGRPCStatueCodes codes.Code
	}{
		"no error returns codes.OK": {
			err:                     nil,
			expectedGRPCStatueCodes: codes.OK,
		},
		"a gRPC error with status 5xx returns 5xx": {
			err:                     status.Errorf(http.StatusInternalServerError, errMsg),
			expectedGRPCStatueCodes: http.StatusInternalServerError,
		},
		"a wrapped gRPC error with status 5xx returns 5xx": {
			err:                     fmt.Errorf("wrapped: %w", status.Errorf(http.StatusServiceUnavailable, errMsg)),
			expectedGRPCStatueCodes: http.StatusServiceUnavailable,
		},
		"a gRPC error with status 4xx return 4xx": {
			err:                     status.Errorf(http.StatusBadRequest, errMsg),
			expectedGRPCStatueCodes: http.StatusBadRequest,
		},
		"a wrapped gRPC error with status 4xx returns 4xx": {
			err:                     fmt.Errorf("wrapped: %w", status.Errorf(http.StatusTooManyRequests, errMsg)),
			expectedGRPCStatueCodes: http.StatusTooManyRequests,
		},
		"a gRPC error with status codes.Internal returns codes.Internal": {
			err:                     status.Errorf(codes.Internal, errMsg),
			expectedGRPCStatueCodes: codes.Internal,
		},
		"a wrapped gRPC error with status codes.FailedPrecondition returns codes.FailedPrecondition ": {
			err:                     fmt.Errorf("wrapped: %w", status.Errorf(codes.FailedPrecondition, errMsg)),
			expectedGRPCStatueCodes: codes.FailedPrecondition,
		},
		"a gRPC error with status codes.Canceled returns codes.Canceled": {
			err:                     status.Errorf(codes.Canceled, context.Canceled.Error()),
			expectedGRPCStatueCodes: codes.Canceled,
		},
		"a wrapped gRPC error with status codes.Canceled returns codes.Canceled": {
			err:                     fmt.Errorf("wrapped: %w", status.Errorf(codes.Canceled, context.Canceled.Error())),
			expectedGRPCStatueCodes: codes.Canceled,
		},
		"context.Canceled returns codes.Canceled": {
			err:                     context.Canceled,
			expectedGRPCStatueCodes: codes.Canceled,
		},
		"a gRPC error with status Codes.Unknown returns codes.Unknown": {
			err:                     status.Errorf(codes.Unknown, errMsg),
			expectedGRPCStatueCodes: codes.Unknown,
		},
		"a wrapped gRPC error with status Codes.Unknown returns codes.Unknown": {
			err:                     fmt.Errorf("wrapped: %w", status.Errorf(codes.Unknown, errMsg)),
			expectedGRPCStatueCodes: codes.Unknown,
		},
		"a non-gRPC error returns codes.Unknown": {
			err:                     fmt.Errorf(errMsg),
			expectedGRPCStatueCodes: codes.Unknown,
		},
	}
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			statusCode := errorToStatusCode(testData.err)
			require.Equal(t, testData.expectedGRPCStatueCodes, statusCode)
		})
	}
}

func TestApplyInstrumentationLabelOptions(t *testing.T) {
	testCases := map[string]struct {
		instrumentationOptions   []InstrumentationOption
		expectedReportGRPCStatus bool
	}{
		"Applying no InstrumentationOption sets acceptGRPCStatus to false": {
			instrumentationOptions:   nil,
			expectedReportGRPCStatus: false,
		},
		"Applying ReportGRPCStatusOption sets acceptGRPCStatus to true": {
			instrumentationOptions:   []InstrumentationOption{ReportGRPCStatusOption},
			expectedReportGRPCStatus: true,
		},
	}
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			instrumentationLabel := applyInstrumentationOptions(false, testData.instrumentationOptions...)
			require.Equal(t, testData.expectedReportGRPCStatus, instrumentationLabel.reportGRPCStatus)
			require.False(t, instrumentationLabel.maskHTTPStatus)

			instrumentationLabel = applyInstrumentationOptions(true, testData.instrumentationOptions...)
			require.Equal(t, testData.expectedReportGRPCStatus, instrumentationLabel.reportGRPCStatus)
			require.True(t, instrumentationLabel.maskHTTPStatus)
		})
	}
}

func setUpStreamClientInstrumentInterceptorTest(t *testing.T, serverStreams bool) (grpc.ClientStream, *mockGrpcStream, context.CancelFunc, *prometheus.Registry) {
	reg := prometheus.NewPedanticRegistry()
	metric := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name: "client_request_duration_seconds",
	}, []string{"method", "result"})

	interceptor := StreamClientInstrumentInterceptor(metric)
	ctx, cancelCtx := context.WithCancel(context.Background())
	t.Cleanup(cancelCtx)

	mockStream := &mockGrpcStream{}
	streamer := func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, ...grpc.CallOption) (grpc.ClientStream, error) {
		return mockStream, nil
	}
	desc := &grpc.StreamDesc{ServerStreams: serverStreams}
	stream, err := interceptor(ctx, desc, nil, "/thing.Server/DoThing", streamer)
	require.NoError(t, err)

	return stream, mockStream, cancelCtx, reg
}

func TestStreamClientInstrumentInterceptor_SendMsg(t *testing.T) {
	stream, mockStream, cancelCtx, reg := setUpStreamClientInstrumentInterceptorTest(t, true)

	err := stream.SendMsg("message 1")
	require.NoError(t, err)
	requireNoRequestMetrics(t, reg)

	mockStream.sendMsgError = io.EOF
	err = stream.SendMsg("message 2")
	require.Equal(t, io.EOF, err)
	requireNoRequestMetrics(t, reg) // If SendMsg returns io.EOF, the true error is available from RecvMsg, so we shouldn't consider the stream failed at this point.

	mockStream.sendMsgError = errors.New("something went wrong")
	err = stream.SendMsg("message 3")
	require.Equal(t, mockStream.sendMsgError, err)
	requireRequestMetrics(t, reg, "error", 1)

	requireDoesNotIncrementMetricsAfterCompletion(t, stream, mockStream, cancelCtx, reg, "error")
}

func TestStreamClientInstrumentInterceptor_RecvMsg(t *testing.T) {
	t.Run("server is streaming", func(t *testing.T) {
		t.Run("RecvMsg returns an EOF", func(t *testing.T) {
			stream, mockStream, cancelCtx, reg := setUpStreamClientInstrumentInterceptorTest(t, true)

			err := stream.RecvMsg(nil)
			require.NoError(t, err)
			requireNoRequestMetrics(t, reg)

			mockStream.recvMsgError = io.EOF
			err = stream.RecvMsg(nil)
			require.Equal(t, mockStream.recvMsgError, err)
			requireRequestMetrics(t, reg, "2xx", 1)

			requireDoesNotIncrementMetricsAfterCompletion(t, stream, mockStream, cancelCtx, reg, "2xx")
		})

		t.Run("RecvMsg returns an error", func(t *testing.T) {
			stream, mockStream, cancelCtx, reg := setUpStreamClientInstrumentInterceptorTest(t, true)

			err := stream.RecvMsg(nil)
			require.NoError(t, err)
			requireNoRequestMetrics(t, reg)

			mockStream.recvMsgError = errors.New("something went wrong")
			err = stream.RecvMsg(nil)
			require.Equal(t, mockStream.recvMsgError, err)
			requireRequestMetrics(t, reg, "error", 1)

			requireDoesNotIncrementMetricsAfterCompletion(t, stream, mockStream, cancelCtx, reg, "error")
		})
	})

	t.Run("server is unary", func(t *testing.T) {
		t.Run("RecvMsg does not return an error", func(t *testing.T) {
			stream, mockStream, cancelCtx, reg := setUpStreamClientInstrumentInterceptorTest(t, false)

			err := stream.RecvMsg(nil)
			require.NoError(t, err)
			requireRequestMetrics(t, reg, "2xx", 1)

			requireDoesNotIncrementMetricsAfterCompletion(t, stream, mockStream, cancelCtx, reg, "2xx")
		})

		t.Run("RecvMsg returns an error", func(t *testing.T) {
			stream, mockStream, cancelCtx, reg := setUpStreamClientInstrumentInterceptorTest(t, false)

			mockStream.recvMsgError = errors.New("something went wrong")
			err := stream.RecvMsg(nil)
			require.Equal(t, mockStream.recvMsgError, err)
			requireRequestMetrics(t, reg, "error", 1)

			requireDoesNotIncrementMetricsAfterCompletion(t, stream, mockStream, cancelCtx, reg, "error")
		})
	})
}

func TestStreamClientInstrumentInterceptor_Header(t *testing.T) {
	stream, mockStream, cancelCtx, reg := setUpStreamClientInstrumentInterceptorTest(t, true)

	_, err := stream.Header()
	require.NoError(t, err)
	requireNoRequestMetrics(t, reg)

	mockStream.headerError = errors.New("something went wrong")
	_, err = stream.Header()
	require.Equal(t, mockStream.headerError, err)
	requireRequestMetrics(t, reg, "error", 1)

	requireDoesNotIncrementMetricsAfterCompletion(t, stream, mockStream, cancelCtx, reg, "error")
}

func TestStreamClientInstrumentInterceptor_CloseSend(t *testing.T) {
	stream, mockStream, cancelCtx, reg := setUpStreamClientInstrumentInterceptorTest(t, true)

	err := stream.CloseSend()
	require.NoError(t, err)
	requireNoRequestMetrics(t, reg)

	mockStream.closeSendError = errors.New("something went wrong")
	err = stream.CloseSend()
	require.Equal(t, mockStream.closeSendError, err)
	requireRequestMetrics(t, reg, "error", 1)

	requireDoesNotIncrementMetricsAfterCompletion(t, stream, mockStream, cancelCtx, reg, "error")
}

func TestStreamClientInstrumentInterceptor_ContextCancelled(t *testing.T) {
	stream, mockStream, cancelCtx, reg := setUpStreamClientInstrumentInterceptorTest(t, true)

	cancelCtx()

	require.Eventually(t, func() bool {
		return gatherRequestMetrics(t, reg) != ""
	}, time.Second, 10*time.Millisecond, "gave up waiting for the context cancellation to report metrics")

	requireRequestMetrics(t, reg, "cancel", 1)
	requireDoesNotIncrementMetricsAfterCompletion(t, stream, mockStream, cancelCtx, reg, "cancel")
}

func requireDoesNotIncrementMetricsAfterCompletion(t *testing.T, stream grpc.ClientStream, mockStream *mockGrpcStream, cancelCtx context.CancelFunc, reg *prometheus.Registry, result string) {
	// Should not increment metric, even if Header now returns an error.
	mockStream.headerError = errors.New("calling Header() failed")
	_, err := stream.Header()
	require.Equal(t, mockStream.headerError, err)
	requireRequestMetrics(t, reg, result, 1)

	// Should not increment metric, even if SendMsg now returns an error
	mockStream.sendMsgError = errors.New("calling SendMsg() failed")
	err = stream.SendMsg("one last message")
	require.Equal(t, mockStream.sendMsgError, err)
	requireRequestMetrics(t, reg, result, 1)

	// Should not increment metric, even if RecvMsg now returns an error
	mockStream.recvMsgError = errors.New("calling RecvMsg() failed")
	err = stream.RecvMsg(nil)
	require.Equal(t, mockStream.recvMsgError, err)
	requireRequestMetrics(t, reg, result, 1)

	// Should not increment metric, even if RecvMsg now returns EOF
	mockStream.recvMsgError = io.EOF
	err = stream.RecvMsg(nil)
	require.Equal(t, mockStream.recvMsgError, err)
	requireRequestMetrics(t, reg, result, 1)

	// Should not increment metric, even if CloseSend now returns an error
	mockStream.closeSendError = errors.New("calling CloseSend() failed")
	err = stream.CloseSend()
	require.Equal(t, mockStream.closeSendError, err)
	requireRequestMetrics(t, reg, result, 1)

	// Should not increment metric, even if the context is cancelled
	cancelCtx()
	requireRequestMetrics(t, reg, result, 1)
}

func requireNoRequestMetrics(t *testing.T, g prometheus.Gatherer) {
	actual := gatherRequestMetrics(t, g)
	require.Empty(t, actual, "expected no request metrics to be reported")
}

func requireRequestMetrics(t *testing.T, g prometheus.Gatherer, result string, count int) {
	expected := fmt.Sprintf(`client_request_duration_seconds_count{method="/thing.Server/DoThing", result="%v"} %v`, result, count)
	actual := gatherRequestMetrics(t, g)

	require.Equalf(t, expected, actual, "expected count of %v for requests with result '%v'", count, result)
}

func gatherRequestMetrics(t *testing.T, g prometheus.Gatherer) string {
	metricMap, err := metrics.NewMetricFamilyMapFromGatherer(g)
	require.NoError(t, err)

	mf := metricMap["client_request_duration_seconds"]
	if mf == nil {
		return ""
	}

	actual := ""

	for _, m := range mf.Metric {
		require.NotNil(t, m.Histogram)
		actual += fmt.Sprintf(`%v_count%v %v`, *mf.Name, formatLabelPairs(m.Label), *m.Histogram.SampleCount)
	}

	return actual
}

func formatLabelPairs(pairs []*io_prometheus_client.LabelPair) string {
	if len(pairs) == 0 {
		return ""
	}

	b := strings.Builder{}
	b.WriteString("{")

	for i, p := range pairs {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString(*p.Name)
		b.WriteString(`="`)
		b.WriteString(*p.Value)
		b.WriteString(`"`)
	}

	b.WriteString("}")

	return b.String()
}

type mockGrpcStream struct {
	recvMsgError   error
	sendMsgError   error
	headerError    error
	closeSendError error
}

func (m *mockGrpcStream) Header() (metadata.MD, error) {
	return nil, m.headerError
}

func (m *mockGrpcStream) Trailer() metadata.MD {
	return nil
}

func (m *mockGrpcStream) CloseSend() error {
	return m.closeSendError
}

func (m *mockGrpcStream) Context() context.Context {
	return nil
}

func (m *mockGrpcStream) SendMsg(interface{}) error {
	return m.sendMsgError
}

func (m *mockGrpcStream) RecvMsg(interface{}) error {
	return m.recvMsgError
}
