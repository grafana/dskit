package spanlogger

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/weaveworks/common/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type loggerCtxMarker struct{}

// TenantResolver provides methods for extracting tenant IDs from a context.
type TenantResolver interface {
	// TenantID tries to extract a tenant ID from a context.
	TenantID(context.Context) (string, error)
	// TenantIDs tries to extract tenant IDs from a context.
	TenantIDs(context.Context) ([]string, error)
}

const (
	// TenantIDsTagName is the tenant IDs tag name.
	TenantIDsTagName = "tenant_ids"
)

var (
	loggerCtxKey = &loggerCtxMarker{}
)

// SpanLogger unifies tracing and logging, to reduce repetition.
func withContext(ctx context.Context, logger log.Logger, resolver TenantResolver) (log.Logger, bool) {
	userID, err := resolver.TenantID(ctx)
	if err == nil && userID != "" {
		logger = log.With(logger, "user", userID)
	}
	traceID, ok := tracing.ExtractSampledTraceID(ctx)
	if !ok {
		return logger, false
	}

	return log.With(logger, "traceID", traceID), true
}

type SpanLogger struct {
	log.Logger
	trace.Span
	sampled bool
}

// New makes a new SpanLogger with a log.Logger to send logs to. The provided context will have the logger attached
// to it and can be retrieved with FromContext.
func New(ctx context.Context, logger log.Logger, method string, resolver TenantResolver, kvps ...interface{}) (*SpanLogger, context.Context) {
	ctx, sp := otel.Tracer("").Start(ctx, method)
	defer sp.End()
	if ids, err := resolver.TenantIDs(ctx); err == nil && len(ids) > 0 {
		sp.SetAttributes(attribute.StringSlice(TenantIDsTagName, ids))
	}
	lwc, sampled := withContext(ctx, logger, resolver)
	l := &SpanLogger{
		Logger:  log.With(lwc, "method", method),
		Span:    sp,
		sampled: sampled,
	}
	if len(kvps) > 0 {
		level.Debug(l).Log(kvps...)
	}

	ctx = context.WithValue(ctx, loggerCtxKey, logger)
	return l, ctx
}

// FromContext returns a span logger using the current parent span.
// If there is no parent span, the SpanLogger will only log to the logger
// within the context. If the context doesn't have a logger, the fallback
// logger is used.
func FromContext(ctx context.Context, fallback log.Logger, resolver TenantResolver) *SpanLogger {
	logger, ok := ctx.Value(loggerCtxKey).(log.Logger)
	if !ok {
		logger = fallback
	}
	sp := trace.SpanFromContext(ctx)
	lwc, sampled := withContext(ctx, logger, resolver)
	return &SpanLogger{
		Logger:  lwc,
		Span:    sp,
		sampled: sampled,
	}
}

// Log implements gokit's Logger interface; sends logs to underlying logger and
// also puts the on the spans.
func (s *SpanLogger) Log(kvps ...interface{}) error {
	s.Logger.Log(kvps...)
	if !s.sampled {
		return nil
	}
	fields, err := convertKVToAttributes(kvps...)
	if err != nil {
		return err
	}
	s.AddEvent("log", trace.WithAttributes(fields...))
	return nil
}

// Error sets error flag and logs the error on the span, if non-nil. Returns the err passed in.
func (s *SpanLogger) Error(err error) error {
	if err == nil || !s.sampled {
		return err
	}
	s.Span.SetStatus(codes.Error, "")
	s.Span.RecordError(err)
	return err
}

// convertKVToAttributes converts keyValues to a slice of attribute.KeyValue
func convertKVToAttributes(keyValues ...interface{}) ([]attribute.KeyValue, error) {
	if len(keyValues)%2 != 0 {
		return nil, fmt.Errorf("non-even keyValues len: %d", len(keyValues))
	}
	fields := make([]attribute.KeyValue, len(keyValues)/2)
	for i := 0; i*2 < len(keyValues); i++ {
		key, ok := keyValues[i*2].(string)
		if !ok {
			return nil, fmt.Errorf("non-string key (pair #%d): %T", i, keyValues[i*2])
		}
		value := keyValues[i*2+1]
		typedVal := reflect.ValueOf(value)

		switch typedVal.Kind() {
		case reflect.Bool:
			fields[i] = attribute.Bool(key, typedVal.Bool())
		case reflect.String:
			fields[i] = attribute.String(key, typedVal.String())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fields[i] = attribute.Int(key, int(typedVal.Int()))
		case reflect.Float32, reflect.Float64:
			fields[i] = attribute.Float64(key, typedVal.Float())
		default:
			if typedVal.Kind() == reflect.Ptr && typedVal.IsNil() {
				fields[i] = attribute.String(key, "nil")
				continue
			}
			// When in doubt, coerce to a string
			fields[i] = attribute.String(key, fmt.Sprintf("%v", value))
		}
	}
	return fields, nil
}
