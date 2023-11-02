// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/middleware/grpc_instrumentation.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package middleware

import (
	"context"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/instrument"
)

func observe(ctx context.Context, hist *prometheus.HistogramVec, method string, err error, duration time.Duration, instrumentationLabelOptions ...InstrumentationLabelOption) {
	instrumentationLabel := ApplyInstrumentationLabelOptions(false, instrumentationLabelOptions...)
	instrument.ObserveWithExemplar(ctx, hist.WithLabelValues(gRPC, method, instrumentationLabel.GetInstrumentationLabel(err), "false"), duration.Seconds())
}

// UnaryServerInstrumentInterceptor instruments gRPC requests for errors and latency.
func UnaryServerInstrumentInterceptor(hist *prometheus.HistogramVec, instrumentationLabelOptions ...InstrumentationLabelOption) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		begin := time.Now()
		resp, err := handler(ctx, req)
		observe(ctx, hist, info.FullMethod, err, time.Since(begin), instrumentationLabelOptions...)
		return resp, err
	}
}

// StreamServerInstrumentInterceptor instruments gRPC requests for errors and latency.
func StreamServerInstrumentInterceptor(hist *prometheus.HistogramVec, instrumentationLabelOptions ...InstrumentationLabelOption) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		begin := time.Now()
		err := handler(srv, ss)
		observe(ss.Context(), hist, info.FullMethod, err, time.Since(begin), instrumentationLabelOptions...)
		return err
	}
}

// UnaryClientInstrumentInterceptor records duration of gRPC requests client side.
func UnaryClientInstrumentInterceptor(metric *prometheus.HistogramVec, instrumentationLabelOptions ...InstrumentationLabelOption) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, resp interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		err := invoker(ctx, method, req, resp, cc, opts...)
		// we enforce masking of HTTP statuses.
		instrumentationLabel := ApplyInstrumentationLabelOptions(true, instrumentationLabelOptions...)
		metric.WithLabelValues(method, instrumentationLabel.GetInstrumentationLabel(err)).Observe(time.Since(start).Seconds())
		return err
	}
}

// StreamClientInstrumentInterceptor records duration of streaming gRPC requests client side.
func StreamClientInstrumentInterceptor(metric *prometheus.HistogramVec, instrumentationLabelOptions ...InstrumentationLabelOption) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
		streamer grpc.Streamer, opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		start := time.Now()
		stream, err := streamer(ctx, desc, cc, method, opts...)
		// we enforce masking of HTTP statuses.
		instrumentationLabel := ApplyInstrumentationLabelOptions(true, instrumentationLabelOptions...)
		s := &instrumentedClientStream{
			metric:               metric,
			start:                start,
			method:               method,
			serverStreams:        desc.ServerStreams,
			finished:             atomic.NewBool(false),
			finishedChan:         make(chan struct{}),
			stream:               stream,
			instrumentationLabel: instrumentationLabel,
		}
		s.awaitCompletion(ctx)
		return s, err
	}
}

// This implementation is heavily inspired by github.com/opentracing-contrib/go-grpc's openTracingClientStream.
type instrumentedClientStream struct {
	metric               *prometheus.HistogramVec
	start                time.Time
	method               string
	serverStreams        bool
	finished             *atomic.Bool
	finishedChan         chan struct{}
	stream               grpc.ClientStream
	instrumentationLabel InstrumentationLabel
}

func (s *instrumentedClientStream) Trailer() metadata.MD {
	return s.stream.Trailer()
}

func (s *instrumentedClientStream) Context() context.Context {
	return s.stream.Context()
}

func (s *instrumentedClientStream) awaitCompletion(ctx context.Context) {
	go func() {
		select {
		case <-s.finishedChan:
			// Stream has finished for another reason, nothing more to do.
		case <-ctx.Done():
			s.finish(ctx.Err())
		}
	}()
}

func (s *instrumentedClientStream) finish(err error) {
	if !s.finished.CompareAndSwap(false, true) {
		return
	}

	close(s.finishedChan)

	s.metric.WithLabelValues(s.method, s.instrumentationLabel.GetInstrumentationLabel(err)).Observe(time.Since(s.start).Seconds())
}

func (s *instrumentedClientStream) SendMsg(m interface{}) error {
	err := s.stream.SendMsg(m)
	if err == nil || err == io.EOF {
		// If SendMsg returns io.EOF, the true error is available from RecvMsg, so we shouldn't consider the stream failed at this point.
		return err
	}

	s.finish(err)
	return err
}

func (s *instrumentedClientStream) RecvMsg(m interface{}) error {
	err := s.stream.RecvMsg(m)
	if !s.serverStreams {
		// Unary server: this is the only message we'll receive, so the stream has ended.
		s.finish(err)
		return err
	}

	if err == nil {
		return nil
	}

	if err == io.EOF {
		s.finish(nil)
	} else {
		s.finish(err)
	}

	return err
}

func (s *instrumentedClientStream) Header() (metadata.MD, error) {
	md, err := s.stream.Header()
	if err != nil {
		s.finish(err)
	}
	return md, err
}

func (s *instrumentedClientStream) CloseSend() error {
	err := s.stream.CloseSend()
	if err != nil {
		s.finish(err)
	}
	return err
}

type InstrumentationLabelOption func(*InstrumentationLabel)

var (
	AcceptGRPCStatusesOption InstrumentationLabelOption = func(instrumentationLabel *InstrumentationLabel) {
		instrumentationLabel.AcceptGRPCStatuses(true)
	}

	DoNotAcceptGRPCStatusesOption InstrumentationLabelOption = func(instrumentationLabel *InstrumentationLabel) {
		instrumentationLabel.AcceptGRPCStatuses(false)
	}
)

func ApplyInstrumentationLabelOptions(maskHTTPStatuses bool, options ...InstrumentationLabelOption) InstrumentationLabel {
	instrumentationLabel := InstrumentationLabel{maskHTTPStatuses: maskHTTPStatuses}
	for _, opt := range options {
		opt(&instrumentationLabel)
	}
	return instrumentationLabel
}

type InstrumentationLabel struct {
	acceptGRPCStatuses bool
	maskHTTPStatuses   bool
}

func (i *InstrumentationLabel) AcceptGRPCStatuses(acceptance bool) {
	i.acceptGRPCStatuses = acceptance
}

// GetInstrumentationLabel converts an error into an error code string by applying the configurations
// contained in this InstrumentationLabel object.
func (i *InstrumentationLabel) GetInstrumentationLabel(err error) string {
	statusCode := i.errorToStatusCode(err)
	return i.statusCodeToString(statusCode)
}

// statusCodeToString converts a given status code in its string representation.
//
//   - If the given status code is codes.OK, statusCodeToString returns "success" or "2xx",
//     depending on whether maskHTTPStatuses is set to false or true respectively.
//   - If the given status code is codes.Canceled, statusCodeToString returns "cancel"
//     independently of maskHTTPStatuses.
//   - If the given status code ia a valid HTTP status code, statusCodeToString returns the
//     string representation of the HTTP status code or the first digit of the HTTP status
//     code followed by "xx", depending on whether maskHTTPStatuses is set to false or true
//     respectively.
//   - If the given status code is a gRPC status code different from codes.Unknown, statusCodeToString
//     returns the string representation of the status code, independently of maskHTTPStatuses.
//   - If the given status code is codes.Unknown, statusCodeToString returns "error".
func (i *InstrumentationLabel) statusCodeToString(statusCode codes.Code) string {
	if statusCode == codes.OK {
		if i.maskHTTPStatuses {
			return "2xx"
		}
		return "success"
	}

	if statusCode == codes.Canceled {
		return "cancel"
	}

	if statusCode == codes.Unknown {
		return "error"
	}

	if isHTTPStatusCode(statusCode) {
		statusFamily := int(statusCode / 100)
		if i.maskHTTPStatuses {
			return strconv.Itoa(statusFamily) + "xx"
		}
		return strconv.Itoa(int(statusCode))
	}

	return statusCode.String()
}

// errorToStatusCode extracts a status code from the given error, and does the following:
//
//   - If the error corresponds to context.Canceled, codes.Canceled is returned.
//   - If the extracted status code is a valid HTTP status code, it is returned.
//   - If the extracted status code is a gRPC code, and acceptGRPCStatusCodes is
//     true, the gRPC status code is returned. Otherwise, codes.Unknown is returned.
func (i *InstrumentationLabel) errorToStatusCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}

	if errors.Is(err, context.Canceled) {
		return codes.Canceled
	}

	statusCode := grpcutil.ErrorToStatusCode(err)

	if statusCode == codes.Canceled {
		return statusCode
	}

	if isHTTPStatusCode(statusCode) {
		return statusCode
	}
	if i.acceptGRPCStatuses {
		return statusCode
	}
	return codes.Unknown
}

// isHTTPStatusCode checks whether the given gRPC status code corresponds to a valid
// HTTP status code. A status code is considered a valid HTTP status codes if it is
// higher or equal than 100 and lower than 600.
func isHTTPStatusCode(statusCode codes.Code) bool {
	return int(statusCode) >= 100 && int(statusCode) < 600
}
