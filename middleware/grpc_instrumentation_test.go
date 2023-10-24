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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/metrics"
)

func TestErrorCode(t *testing.T) {
	testCases := []struct {
		name           string
		err            error
		expected       string
		expectedHeader *httpgrpc.Header
	}{
		{
			name:           "No Error",
			err:            nil,
			expected:       "2xx",
			expectedHeader: nil, // No header expected for this case
		},
		{
			name:           "Any 5xx",
			err:            httpgrpc.Errorf(http.StatusNotImplemented, "Fail"),
			expected:       "5xx",
			expectedHeader: nil,
		},
		{
			name:           "Any 4xx",
			err:            httpgrpc.Errorf(http.StatusConflict, "Fail"),
			expected:       "4xx",
			expectedHeader: nil,
		},
		{
			name:           "Canceled",
			err:            status.Errorf(codes.Canceled, "Fail"),
			expected:       "cancel",
			expectedHeader: nil,
		},
		{
			name:           "Unknown",
			err:            status.Errorf(codes.Unknown, "Fail"),
			expected:       "error",
			expectedHeader: nil,
		},
		{
			name:           "5xx WithHeaders",
			err:            httpgrpc.ErrorfWithHeaders(http.StatusNotImplemented, []*httpgrpc.Header{{Key: "X-Test", Values: []string{"test"}}}, "Fail"),
			expected:       "5xx",
			expectedHeader: &httpgrpc.Header{Key: "X-Test", Values: []string{"test"}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := errorCode(tc.err)
			assert.Equal(t, tc.expected, a)

			if tc.expectedHeader != nil {
				resp, ok := httpgrpc.HTTPResponseFromError(tc.err)
				assert.True(t, ok)
				assert.Contains(t, resp.Headers, tc.expectedHeader)
			}
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
	streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
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
