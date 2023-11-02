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

func observe(ctx context.Context, hist *prometheus.HistogramVec, method string, err error, duration time.Duration) {
	respStatus := errorCode(err, false)
	instrument.ObserveWithExemplar(ctx, hist.WithLabelValues(gRPC, method, respStatus, "false"), duration.Seconds())
}

// UnaryServerInstrumentInterceptor instruments gRPC requests for errors and latency.
func UnaryServerInstrumentInterceptor(hist *prometheus.HistogramVec) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		begin := time.Now()
		resp, err := handler(ctx, req)
		observe(ctx, hist, info.FullMethod, err, time.Since(begin))
		return resp, err
	}
}

// StreamServerInstrumentInterceptor instruments gRPC requests for errors and latency.
func StreamServerInstrumentInterceptor(hist *prometheus.HistogramVec) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		begin := time.Now()
		err := handler(srv, ss)
		observe(ss.Context(), hist, info.FullMethod, err, time.Since(begin))
		return err
	}
}

// UnaryClientInstrumentInterceptor records duration of gRPC requests client side.
func UnaryClientInstrumentInterceptor(metric *prometheus.HistogramVec) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, resp interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		err := invoker(ctx, method, req, resp, cc, opts...)
		metric.WithLabelValues(method, errorCode(err, true)).Observe(time.Since(start).Seconds())
		return err
	}
}

// StreamClientInstrumentInterceptor records duration of streaming gRPC requests client side.
func StreamClientInstrumentInterceptor(metric *prometheus.HistogramVec) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
		streamer grpc.Streamer, opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		start := time.Now()
		stream, err := streamer(ctx, desc, cc, method, opts...)
		s := &instrumentedClientStream{
			metric:        metric,
			start:         start,
			method:        method,
			serverStreams: desc.ServerStreams,
			finished:      atomic.NewBool(false),
			finishedChan:  make(chan struct{}),
			stream:        stream,
		}
		s.awaitCompletion(ctx)
		return s, err
	}
}

// This implementation is heavily inspired by github.com/opentracing-contrib/go-grpc's openTracingClientStream.
type instrumentedClientStream struct {
	metric        *prometheus.HistogramVec
	start         time.Time
	method        string
	serverStreams bool
	finished      *atomic.Bool
	finishedChan  chan struct{}
	stream        grpc.ClientStream
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

	s.metric.WithLabelValues(s.method, errorCode(err, true)).Observe(time.Since(s.start).Seconds())
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

// errorCode converts an error into an error code string.
// If the given error is nil, errorCode returns "success" or "2xx", depending on whether maskHTTPStatuses
// is set to false or true respectively.
// If the given error corresponds to context.Canceled, errorCode returns "cancel", independently of maskHTTPStatuses.
// If the given error is a gRPC error with an HTTP status code, errorCode returns the string representation of the
// HTTP status code or the first digit of the HTTP status code followed by "xx", depending on whether maskHTTPStatuses
// is set to false or true respectively.
// If the given error is a gRPC error with a gRPC status code different from codes.Unknown, errorCode returns the string
// representation of the status code, independently of maskHTTPStatuses.
// If the given error is a gRPC error with gRPC status code codes.Unknown, or if it is a non-gRPC error, errorCode
// returns "error".
func errorCode(err error, maskHTTPStatuses bool) string {
	statusCode := grpcutil.ErrorToStatusCode(err)
	if statusCode == codes.OK {
		if maskHTTPStatuses {
			return "2xx"
		}
		return "success"
	}

	// In order to understand whether this error corresponds to context.Canceled, we don't call
	// grpcutil.IsCanceled to avoid an additional call to grpcutil.ErrorToStatusCode. Instead,
	// we check whether statusCodes is codes.Canceled, or whether it is codes.Unknown with the
	// error being context.Canceled. In that case we return "cancel".
	// If statusCode is codes.Unknown but the error is not context.Canceled, we return "error".
	if statusCode == codes.Canceled {
		return "cancel"
	}

	if statusCode == codes.Unknown {
		if errors.Is(err, context.Canceled) {
			return "cancel"
		}
		return "error"
	}

	// At this point err is a gRPC error with a gRPC or an HTTP status code.
	// HTTP status codes are higher than 200, and when divided by 100 give a
	// positive value. If maskHTTPStatuses is true, we mask those status codes.
	statusFamily := int(statusCode / 100)
	if statusFamily > 0 {
		if maskHTTPStatuses {
			return strconv.Itoa(statusFamily) + "xx"
		}
		return strconv.Itoa(int(statusCode))
	}

	return statusCode.String()
}
