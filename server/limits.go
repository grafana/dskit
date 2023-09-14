package server

import (
	"context"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

type GrpcMethodLimiter interface {
	// RpcCallStarting is called before request has been read into memory.
	// All that's known about the request at this point is grpc method name.
	// Returned error should be convertible to gRPC Status via status.FromError,
	// otherwise gRPC-server implementation-specific error will be returned to the client (codes.PermissionDenied in grpc@v1.55.0).
	RpcCallStarting(methodName string) error
	RpcCallFinished(methodName string)
}

// Custom type to hide it from other packages.
type grpcLimitCheckContextKey int

// Presence of this key in the context indicates that inflight request counter was increased for this request, and needs to be decreased when request ends.
const (
	requestFullMethod grpcLimitCheckContextKey = 1

	errTooManyInflightRequestsMsg = "too many inflight requests"
)

var errTooManyInflightRequests = status.Error(codes.Unavailable, errTooManyInflightRequestsMsg)

func newGrpcLimitCheck(maxInflight int, methodLimiter GrpcMethodLimiter, namespace string, reg prometheus.Registerer) *grpcLimitCheck {
	c := &grpcLimitCheck{
		methodLimiter: methodLimiter,
	}
	c.maxInflight.Store(int64(maxInflight))

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "grpc_inflight_requests",
		Help:      "Current number of inflight requests handled by gRPC server.",
	}, func() float64 {
		return float64(c.inflight.Load())
	})

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "grpc_max_inflight_requests",
		Help:      "Current limit of inflight requests that gRPC server can handle. 0 means no limit.",
	}, func() float64 {
		return float64(c.maxInflight.Load())
	})

	return c
}

// grpcLimitCheck implements gRPC TapHandle and gRPC stats.Handler.
// grpcLimitCheck can track inflight requests, and reject requests before even reading them into memory.
type grpcLimitCheck struct {
	methodLimiter GrpcMethodLimiter

	inflight    atomic.Int64
	maxInflight atomic.Int64
}

// TapHandle is called after receiving grpc request and headers, but before reading any request data yet.
// If we reject request here, it won't be counted towards any metrics (eg. in middleware.grpcStatsHandler).
// If we accept request (not return error), eventually HandleRPC with stats.End notification will be called.
func (g *grpcLimitCheck) TapHandle(ctx context.Context, info *tap.Info) (context.Context, error) {
	v := g.inflight.Inc()
	decreaseInflightInDefer := true
	defer func() {
		if decreaseInflightInDefer {
			g.inflight.Dec()
		}
	}()

	if !isMethodNameValid(info.FullMethodName) {
		// If method name is not valid, we let the request continue, but decrease inflight requests.
		// Otherwise, we would not have an option to decrease it later, because in this case grpc server will not call stat handler when request finishes.
		return ctx, nil
	}

	l := g.maxInflight.Load()
	if l > 0 && v > l {
		return ctx, errTooManyInflightRequests
	}

	if g.methodLimiter != nil {
		if err := g.methodLimiter.RpcCallStarting(info.FullMethodName); err != nil {
			return ctx, err
		}
	}

	decreaseInflightInDefer = false
	ctx = context.WithValue(ctx, requestFullMethod, info.FullMethodName)
	return ctx, nil
}

func (g *grpcLimitCheck) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (g *grpcLimitCheck) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
	// when request ends, and we started "inflight" request tracking for it, finish it.
	if _, ok := rpcStats.(*stats.End); !ok {
		return
	}

	if name, ok := ctx.Value(requestFullMethod).(string); ok {
		g.inflight.Dec()
		if g.methodLimiter != nil {
			g.methodLimiter.RpcCallFinished(name)
		}
	}
}

func (g *grpcLimitCheck) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (g *grpcLimitCheck) HandleConn(_ context.Context, _ stats.ConnStats) {
	// Not interested.
}

// This function mimics the check in grpc library, server.go, handleStream method. handleStream method can stop processing early,
// without calling stat handler if the method name is invalid.
func isMethodNameValid(method string) bool {
	if method != "" && method[0] == '/' {
		method = method[1:]
	}
	pos := strings.LastIndex(method, "/")
	if pos == -1 {
		return false
	}
	return true
}
