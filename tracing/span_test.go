package tracing

import (
	"context"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func TestKeyValueToOTelAttribute(t *testing.T) {
	type kv struct {
		k string
		v any
	}
	tests := []struct {
		name     string
		kv       kv
		expected attribute.KeyValue
	}{
		{
			name:     "string",
			kv:       kv{"key", "value"},
			expected: attribute.String("key", "value"),
		},
		{
			name:     "int",
			kv:       kv{"key", 42},
			expected: attribute.Int("key", 42),
		},
		{
			name:     "int64",
			kv:       kv{"key", int64(42)},
			expected: attribute.Int64("key", 42),
		},
		{
			name:     "float64",
			kv:       kv{"key", 42.0},
			expected: attribute.Float64("key", 42.0),
		},
		{
			name:     "bool",
			kv:       kv{"key", true},
			expected: attribute.Bool("key", true),
		},
		{
			name:     "string slice",
			kv:       kv{"key", []string{"value1", "value2"}},
			expected: attribute.StringSlice("key", []string{"value1", "value2"}),
		},
		{
			name:     "int slice",
			kv:       kv{"key", []int{1, 2, 3}},
			expected: attribute.IntSlice("key", []int{1, 2, 3}),
		},
		{
			name:     "int64 slice",
			kv:       kv{"key", []int64{1, 2, 3}},
			expected: attribute.Int64Slice("key", []int64{1, 2, 3}),
		},
		{
			name:     "stringer",
			kv:       kv{"key", testStringer{}},
			expected: attribute.Stringer("key", testStringer{}),
		},
		{
			name:     "[]byte",
			kv:       kv{"key", []byte("value")},
			expected: attribute.String("key", "value"),
		},
		{
			name:     "fallback",
			kv:       kv{"key", map[string]string{"key": "value"}},
			expected: attribute.String("key", "map[key:value]"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, KeyValueToOTelAttribute(test.kv.k, test.kv.v))
		})
	}
}

func TestSpanFromContext(t *testing.T) {
	tests := []struct {
		name           string
		context        context.Context
		expectOTelSpan bool
		expectOTSpan   bool
		expectSampled  bool
	}{
		{
			name:           "no active spans",
			context:        context.Background(),
			expectOTelSpan: false,
			expectOTSpan:   true, // falls back to OpenTracing NoopTracer by default
			expectSampled:  false,
		},
		{
			name:           "OpenTracing span in context",
			context:        opentracing.ContextWithSpan(context.Background(), opentracing.NoopTracer{}.StartSpan("test")),
			expectOTelSpan: false,
			expectOTSpan:   true,
			expectSampled:  false,
		},
		{
			name: "OpenTelemetry valid span context",
			context: trace.ContextWithSpan(
				context.Background(),
				&mockSpan{validSpanContext: true, isSampled: true},
			),
			expectOTelSpan: true,
			expectOTSpan:   false,
			expectSampled:  true,
		},
		{
			name: "OpenTelemetry invalid span context",
			context: trace.ContextWithSpan(
				context.Background(),
				&mockSpan{validSpanContext: false, isSampled: false},
			),
			expectOTelSpan: false,
			expectOTSpan:   true, // falls back to OpenTracing NoopTracer
			expectSampled:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			otelSpan, opentracingSpan, sampled := SpanFromContext(test.context)
			if test.expectOTelSpan {
				require.NotNil(t, otelSpan)
			} else {
				require.Nil(t, otelSpan)
			}

			if test.expectOTSpan {
				require.NotNil(t, opentracingSpan)
			} else {
				require.Nil(t, opentracingSpan)
			}

			require.Equal(t, test.expectSampled, sampled)
		})
	}
}

type mockSpan struct {
	trace.Span
	validSpanContext bool
	isSampled        bool
}

func (m *mockSpan) SpanContext() trace.SpanContext {
	if !m.validSpanContext {
		return trace.SpanContext{}
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1},
		SpanID:  trace.SpanID{1},
		TraceFlags: func() trace.TraceFlags {
			if m.isSampled {
				return trace.FlagsSampled
			}
			return 0
		}(),
	})
}

type testStringer struct{}

func (testStringer) String() string {
	return "test"
}
