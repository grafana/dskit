package middleware

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestTracer_HeadersNotAddedAsSpanAttributes(t *testing.T) {
	const headersSent = maxHeadersToAddAsSpanAttributes + 50

	recorder := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	handler := NewTracer(nil, true, nil, nil).Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	for i := range headersSent {
		req.Header.Set(fmt.Sprintf("X-Test-Header-%03d", i), "value")
	}
	handler.ServeHTTP(httptest.NewRecorder(), req)

	spans := recorder.Ended()
	require.NotEmpty(t, spans)

	var reported []string
	var found bool
	for _, span := range spans {
		for _, event := range span.Events() {
			for _, attr := range event.Attributes {
				if attr.Key == "headers_not_added_as_span_attributes" {
					reported = attr.Value.AsStringSlice()
					found = true
				}
			}
		}
	}
	require.True(t, found, "expected a span event listing the headers that were not added")
	require.Len(t, reported, headersSent-maxHeadersToAddAsSpanAttributes)
}
