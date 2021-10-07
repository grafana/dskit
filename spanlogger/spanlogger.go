package spanlogger

import (
	"context"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/weaveworks/common/tracing"
	"github.com/weaveworks/common/user"
)

type loggerCtxMarker struct{}

const (
	// TenantIDsTagName is the tenant IDs tag name.
	TenantIDsTagName = "tenant_ids"
)

var (
	loggerCtxKey = &loggerCtxMarker{}
)

// SpanLogger unifies tracing and logging, to reduce repetition.
type SpanLogger struct {
	log.Logger
	opentracing.Span
}

// New makes a new SpanLogger with a log.Logger to send logs to. The provided context will have the logger attached
// to it and can be retrieved with FromContext.
func New(ctx context.Context, logger log.Logger, method string, kvps ...interface{}) (*SpanLogger, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, method)
	if id := tenantID(ctx); id != "" {
		span.SetTag(TenantIDsTagName, []string{id})
	}
	l := &SpanLogger{
		Logger: log.With(withContext(ctx, logger), "method", method),
		Span:   span,
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
func FromContext(ctx context.Context, fallback log.Logger) *SpanLogger {
	logger, ok := ctx.Value(loggerCtxKey).(log.Logger)
	if !ok {
		logger = fallback
	}
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		sp = defaultNoopSpan
	}
	return &SpanLogger{
		Logger: withContext(ctx, logger),
		Span:   sp,
	}
}

// Log implements gokit's Logger interface; sends logs to underlying logger and
// also puts the on the spans.
func (s *SpanLogger) Log(kvps ...interface{}) error {
	s.Logger.Log(kvps...)
	fields, err := otlog.InterleavedKVToFields(kvps...)
	if err != nil {
		return err
	}
	s.Span.LogFields(fields...)
	return nil
}

// Error sets error flag and logs the error on the span, if non-nil. Returns the err passed in.
func (s *SpanLogger) Error(err error) error {
	if err == nil {
		return nil
	}
	ext.Error.Set(s.Span, true)
	s.Span.LogFields(otlog.Error(err))
	return err
}

// tenantID tries to extract the tenant ID from ctx.
func tenantID(ctx context.Context) string {
	//lint:ignore faillint wrapper around upstream method
	id, err := user.ExtractOrgID(ctx)
	if err != nil {
		return ""
	}

	// handle the relative reference to current and parent path.
	if id == "." || id == ".." || strings.ContainsAny(id, `\/`) {
		return ""
	}

	return id
}

func withContext(ctx context.Context, l log.Logger) log.Logger {
	// Weaveworks uses "orgs" and "orgID" to represent Cortex users,
	// even though the code-base generally uses `userID` to refer to the same thing.
	userID := tenantID(ctx)
	if userID != "" {
		l = log.With(l, "org_id", userID)
	}

	traceID, ok := tracing.ExtractSampledTraceID(ctx)
	if !ok {
		return l
	}

	return log.With(l, "traceID", traceID)
}
