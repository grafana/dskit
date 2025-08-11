package tracing

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

func TestParseJaegerTags(t *testing.T) {
	os.Setenv("EXISTENT_ENV_KEY", "env_value")
	defer os.Unsetenv("EXISTENT_ENV_KEY")
	t.Run("valid tags", func(t *testing.T) {
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
					attribute.String("key1", "env_value"),
					attribute.String("key2", "default_value"),
				},
				expectedError: nil,
			},
		}

		for _, test := range tests {
			output, err := parseJaegerTags(test.input)
			require.Equal(t, test.expectedOutput, output)
			require.Equal(t, test.expectedError, err)
		}
	})

	t.Run("invalid tags", func(t *testing.T) {
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
			require.Error(t, err, test.expectedError)
		}
	})
}

func TestJaegerDebugIDHandling(t *testing.T) {
	resultFromInnerPropagator := tracesdk.SamplingResult{
		Decision:   tracesdk.RecordOnly,
		Attributes: []attribute.KeyValue{attribute.String("some.key", "some_value")},
	}

	resultWithDebugID := tracesdk.SamplingResult{
		Decision: tracesdk.RecordAndSample,
		Attributes: []attribute.KeyValue{
			attribute.String("some.key", "some_value"),
			attribute.String("jaeger-debug-id", "the-debug-id"),
		},
	}

	testCases := map[string]struct {
		headers  http.Header
		expected tracesdk.SamplingResult
	}{
		"header not present": {
			headers: http.Header{
				"Content-Type": []string{"application/json"},
			},
			expected: resultFromInnerPropagator,
		},
		"header present": {
			headers: http.Header{
				"Content-Type":    []string{"application/json"},
				"Jaeger-Debug-Id": []string{"the-debug-id"},
			},
			expected: resultWithDebugID,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			propagator := JaegerDebuggingPropagator{}
			inner := mockSampler{resultFromInnerPropagator}
			sampler := JaegerDebuggingSampler{inner}

			ctx := context.Background()
			carrier := propagation.HeaderCarrier(testCase.headers)
			ctx = propagator.Extract(ctx, carrier)

			params := tracesdk.SamplingParameters{
				ParentContext: ctx,
			}
			result := sampler.ShouldSample(params)
			require.Equal(t, testCase.expected, result)
		})
	}
}

type mockSampler struct {
	result tracesdk.SamplingResult
}

func (m mockSampler) ShouldSample(parameters tracesdk.SamplingParameters) tracesdk.SamplingResult {
	// Clone the result so that we don't share the attributes slice between invocations.
	return tracesdk.SamplingResult{
		Decision:   m.result.Decision,
		Attributes: slices.Clone(m.result.Attributes),
	}
}

func (m mockSampler) Description() string {
	return "mockSampler"
}
