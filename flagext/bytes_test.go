package flagext

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func Test_Bytes_String(t *testing.T) {
	for _, tcase := range []struct {
		input    uint64
		expected string
	}{
		{
			input:    1024 * 1024 * 1024,
			expected: "1GiB",
		},
		{
			input:    (1024 + 512) * 1024 * 1024,
			expected: "1GiB512MiB",
		},
		{
			input:    1024 * 1024,
			expected: "1MiB",
		},
		{
			input:    1024,
			expected: "1KiB",
		},
	} {
		b := Bytes(tcase.input)
		require.Equal(t, tcase.expected, b.String())
	}
}

func Test_Bytes_Set(t *testing.T) {
	for _, tcase := range []struct {
		input    string
		expected uint64
	}{
		{
			input:    "1.5GiB",
			expected: (1024 + 512) * 1024 * 1024,
		},
		{
			input:    "1.5GB",
			expected: (1024 + 512) * 1024 * 1024,
		},
		{
			input:    "1536MiB",
			expected: (1024 + 512) * 1024 * 1024,
		},
		{
			input:    "1536MB",
			expected: (1024 + 512) * 1024 * 1024,
		},

		{
			input:    "1GiB",
			expected: 1024 * 1024 * 1024,
		},
		{
			input:    "1GB",
			expected: 1024 * 1024 * 1024,
		},
		{
			input:    "1MiB",
			expected: 1024 * 1024,
		},
		{
			input:    "1MB",
			expected: 1024 * 1024,
		},
		{
			input:    "1KiB",
			expected: 1024,
		},
		{
			input:    "1KB",
			expected: 1024,
		},
	} {
		var b Bytes
		require.NoError(t, b.Set(tcase.input))
		require.Equal(t, b, Bytes(tcase.expected))
	}
}
func Test_Bytes_MarshalYAML(t *testing.T) {
	for _, tcase := range []struct {
		input    uint64
		expected string
	}{
		{
			input:    (1024 + 512) * 1024 * 1024,
			expected: "1GiB512MiB\n",
		},
		{
			input:    1024 * 1024 * 1024,
			expected: "1GiB\n",
		},
		{
			input:    1024 * 1024,
			expected: "1MiB\n",
		},
		{
			input:    1024,
			expected: "1KiB\n",
		},
	} {
		b := Bytes(tcase.input)
		y, err := yaml.Marshal(b)
		require.NoError(t, err)
		require.Equal(t, []byte(tcase.expected), y)
	}
}

func Test_Bytes_UnmarshalYAML(t *testing.T) {
	for _, tcase := range []struct {
		input    string
		expected uint64
	}{
		{
			input:    "1.5GiB\n",
			expected: (1024 + 512) * 1024 * 1024,
		},
		{
			input:    "1.5GB\n",
			expected: (1024 + 512) * 1024 * 1024,
		},
		{
			input:    "1536MiB\n",
			expected: (1024 + 512) * 1024 * 1024,
		},
		{
			input:    "1536MB\n",
			expected: (1024 + 512) * 1024 * 1024,
		},
		{
			input:    "1GiB\n",
			expected: 1024 * 1024 * 1024,
		},
		{
			input:    "1GB\n",
			expected: 1024 * 1024 * 1024,
		},
		{
			input:    "1MiB\n",
			expected: 1024 * 1024,
		},
		{
			input:    "1MB\n",
			expected: 1024 * 1024,
		},
		{
			input:    "1KiB\n",
			expected: 1024,
		},
		{
			input:    "1KB\n",
			expected: 1024,
		},
	} {
		var b Bytes
		require.NoError(t, yaml.Unmarshal([]byte(tcase.input), &b))
		require.Equal(t, Bytes(tcase.expected), b)
	}
}
