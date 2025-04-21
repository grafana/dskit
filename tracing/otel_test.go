package tracing

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestParseAttributes(t *testing.T) {
	os.Setenv("EXISTENT_ENV_KEY", "env_value")
	defer os.Unsetenv("EXISTENT_ENV_KEY")
	t.Run("ValidAttributes", func(t *testing.T) {
		tests := []struct {
			input          string
			expectedOutput []attribute.KeyValue
			expectedError  error
		}{
			{
				input: "key1=value1,key2=value2",
				expectedOutput: []attribute.KeyValue{
					attribute.String("key1", "value1"),
					attribute.String("key2", "value2"),
				},
				expectedError: nil,
			},
			{
				input: "key1=${EXISTENT_ENV_KEY},key2=${NON_EXISTENT_ENV_KEY:default_value}",
				expectedOutput: []attribute.KeyValue{
					attribute.String("key1", os.Getenv("EXISTENT_ENV_KEY")),
					attribute.String("key2", "default_value"),
				},
				expectedError: nil,
			},
		}

		for _, test := range tests {
			output, err := parseJaegerTags(test.input)
			assert.Equal(t, test.expectedOutput, output)
			assert.Equal(t, test.expectedError, err)
		}
	})

	t.Run("InvalidAttributes", func(t *testing.T) {
		tests := []struct {
			input         string
			expectedError string
		}{
			{
				input:         "key1=value1,key2",
				expectedError: fmt.Sprintf("invalid tag \"%s\", expected key=value", "key2"),
			},
			{
				input:         "key1=value1,key2=",
				expectedError: fmt.Sprintf("invalid tag \"%s\", expected key=value", "key2="),
			},
		}

		for _, test := range tests {
			_, err := parseJaegerTags(test.input)
			assert.Error(t, err, test.expectedError)
		}
	})
}

func TestExtractSampledTraceID(t *testing.T) {
	cases := []struct {
		desc  string
		ctx   func(*testing.T) (context.Context, func())
		empty bool
	}{
		{
			desc: "OpenTelemetry",
			ctx:  getContextWithOpenTelemetry,
		},
		{
			desc: "No tracer",
			ctx: func(_ *testing.T) (context.Context, func()) {
				return context.Background(), func() {}
			},
			empty: true,
		},
		{
			desc:  "OpenTelemetry with noop",
			ctx:   getContextWithOpenTelemetryNoop,
			empty: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, closer := tc.ctx(t)
			defer closer()
			sampledTraceID, sampled := ExtractSampledTraceID(ctx)
			traceID, ok := ExtractTraceID(ctx)

			assert.Equal(t, sampledTraceID, traceID, "Expected sampledTraceID to equal traceID")
			if tc.empty {
				assert.Empty(t, traceID, "Expected traceID to be empty")
				assert.False(t, sampled, "Expected sampled to be false")
				assert.False(t, ok, "Expected ok to be false")
			} else {
				assert.NotEmpty(t, traceID, "Expected traceID to be non-empty")
				assert.True(t, sampled, "Expected sampled to be true")
				assert.True(t, ok, "Expected ok to be true")
			}
		})
	}
}

func getContextWithOpenTelemetry(_ *testing.T) (context.Context, func()) {
	originTracerProvider := otel.GetTracerProvider()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	tr := tp.Tracer("test")
	ctx, sp := tr.Start(context.Background(), "test")
	return ctx, func() {
		sp.End()
		otel.SetTracerProvider(originTracerProvider)
	}
}

func getContextWithOpenTelemetryNoop(t *testing.T) (context.Context, func()) {
	ctx, sp := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "test")
	// sanity check
	require.False(t, sp.SpanContext().TraceID().IsValid())
	return ctx, func() {
		sp.End()
	}
}

func TestNewResource(t *testing.T) {
	res, err := NewResource("test-service", []attribute.KeyValue{
		attribute.String("test.key", "test.value"),
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Contains(t, res.Attributes(), attribute.String("service.name", "test-service"))
	require.Contains(t, res.Attributes(), attribute.String("test.key", "test.value"))
}
