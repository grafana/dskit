package tracing

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
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
