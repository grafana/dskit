// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/tracing/tracing.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package tracing

import (
	"context"
	"fmt"
	"io"
	"reflect"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	jaeger "github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerprom "github.com/uber/jaeger-lib/metrics/prometheus"
)

// ErrInvalidConfiguration is an error to notify client to provide valid trace report agent or config server
var (
	ErrBlankTraceConfiguration = errors.New("no trace report agent, config server, or collector endpoint specified")
)

// installJaeger registers Jaeger as the OpenTracing implementation.
func installJaeger(serviceName string, cfg *jaegercfg.Configuration, options ...jaegercfg.Option) (io.Closer, error) {
	metricsFactory := jaegerprom.New()

	// put the metricsFactory earlier so provided options can override it
	opts := append([]jaegercfg.Option{jaegercfg.Metrics(metricsFactory)}, options...)

	closer, err := cfg.InitGlobalTracer(serviceName, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "could not initialize jaeger tracer")
	}
	return closer, nil
}

// NewFromEnv is a convenience function to allow tracing configuration
// via environment variables
//
// Tracing will be enabled if one (or more) of the following environment variables is used to configure trace reporting:
// - JAEGER_AGENT_HOST
// - JAEGER_SAMPLER_MANAGER_HOST_PORT
func NewFromEnv(serviceName string, options ...jaegercfg.Option) (io.Closer, error) {
	cfg, err := jaegercfg.FromEnv()
	if err != nil {
		return nil, errors.Wrap(err, "could not load jaeger tracer configuration")
	}

	if cfg.Sampler.SamplingServerURL == "" && cfg.Reporter.LocalAgentHostPort == "" && cfg.Reporter.CollectorEndpoint == "" {
		return nil, ErrBlankTraceConfiguration
	}

	return installJaeger(serviceName, cfg, options...)
}

// ExtractTraceID extracts the trace id, if any from the context.
func ExtractTraceID(ctx context.Context) (string, bool) {
	if tid, _, ok := extractJaegerContext(ctx); ok {
		return tid.String(), true
	}
	return "", false
}

// ExtractTraceSpanID extracts the trace id, span id if any from the context.
func ExtractTraceSpanID(ctx context.Context) (string, string, bool) {
	if tid, sid, ok := extractJaegerContext(ctx); ok {
		return tid.String(), sid.String(), true
	}
	return "", "", false
}

func extractJaegerContext(ctx context.Context) (tid jaeger.TraceID, sid jaeger.SpanID, success bool) {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return
	}
	jsp, ok := sp.Context().(jaeger.SpanContext)
	if !ok {
		return
	}
	return jsp.TraceID(), jsp.SpanID(), true
}

// ExtractSampledTraceID works like ExtractTraceID but the returned bool is only
// true if the returned trace id is sampled.
func ExtractSampledTraceID(ctx context.Context) (string, bool) {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return "", false
	}

	sctx := sp.Context()

	jctx, ok := sctx.(jaeger.SpanContext)
	if ok {
		return jctx.TraceID().String(), jctx.IsSampled()
	}

	return extractOtelSampledTraceID(sctx)
}

// extractOtelSampledTraceID from an OTEL compatible span context. This also
// works when using the open tracing bridge.  The interface for the
// span context is:
//
//	type SpanContext interface {
//		TraceID() TraceID
//		IsSampled() bool
//	}
//
// See the notes below on why this is using reflection.
func extractOtelSampledTraceID(ctx opentracing.SpanContext) (string, bool) {
	// Fast path to check sampling
	octx, ok := ctx.(otelCompatibleSpanContext)
	if !ok || !octx.IsSampled() {
		return "", false
	}

	// Slow path using reflection to get the TraceID.
	m := reflect.ValueOf(octx).MethodByName("TraceID")
	if !m.IsValid() {
		return "", false
	}

	tid := m.Call(nil)
	if len(tid) == 0 {
		return "", false
	}

	str, ok := tid[0].Interface().(fmt.Stringer)
	if !ok {
		return "", false
	}

	return str.String(), true
}

type otelCompatibleSpanContext interface {
	IsSampled() bool

	// Ideally either of these methods are added to the interface but
	// couldn't find a solution. The TraceID() method returns a
	// `TraceID` struct alias of [16]byte with a String() method.
	// TraceID() [16]byte
	// TraceID() fmt.Stringer
}
