package flagext

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func Test_IntString_String(t *testing.T) {
	for _, tcase := range []struct {
		input    int
		expected string
	}{
		{
			input:    1024,
			expected: "1024",
		},
		{
			input:    2048000,
			expected: "2048000",
		},
		{
			input:    0,
			expected: "0",
		},
	} {
		t.Run(fmt.Sprintf("when input is %d", tcase.input), func(t *testing.T) {
			b := IntString(tcase.input)
			require.Equal(t, tcase.expected, b.String())
		})
	}
}

func Test_IntString_Set(t *testing.T) {
	for _, tcase := range []struct {
		input       string
		expected    int
		expectedErr bool
	}{
		{
			input:    "2048000",
			expected: 2048000,
		},
		{
			input:    "512",
			expected: 512,
		},
		{
			input:    "40960000",
			expected: 40960000,
		},
		{
			expected: 0,
		},
		{
			input:       "invalid",
			expectedErr: true,
		},
	} {
		t.Run(fmt.Sprintf("when input is %s", tcase.input), func(t *testing.T) {
			var b IntString
			require.True(t, (b.Set(tcase.input) != nil) == tcase.expectedErr)
			require.Equal(t, b, IntString(tcase.expected))
		})
	}
}

func Test_IntString_UnmarshalYAML(t *testing.T) {
	for _, tcase := range []struct {
		input       []byte
		expected    int
		expectedErr bool
	}{
		{
			input:    []byte(strconv.Itoa(0)),
			expected: 0,
		},
		{
			input:    []byte(strconv.Itoa(2048000)),
			expected: 2048000,
		},
		{
			input:    []byte("2048000"),
			expected: 2048000,
		},
		{
			input:    []byte(strconv.Itoa(40960000)),
			expected: 40960000,
		},
		{
			input:    []byte("40960000"),
			expected: 40960000,
		},
		{
			input:    []byte(""),
			expected: 0,
		},
		{
			input:       []byte("invalid"),
			expectedErr: true,
		},
	} {
		t.Run(fmt.Sprintf("when input is %s", tcase.input), func(t *testing.T) {
			var b IntString
			require.True(t, (yaml.Unmarshal(tcase.input, &b) != nil) == tcase.expectedErr)
			require.Equal(t, IntString(tcase.expected), b)
		})
	}
}

func Test_IntString_UnmarshalJSON(t *testing.T) {
	for _, tcase := range []struct {
		input       []byte
		expected    int
		expectedErr bool
	}{
		{
			input:    []byte(strconv.Itoa(0)),
			expected: 0,
		},
		{
			input:    []byte(strconv.Itoa(2048000)),
			expected: 2048000,
		},
		{
			input:    []byte("2048000"),
			expected: 2048000,
		},
		{
			input:    []byte(strconv.Itoa(40960000)),
			expected: 40960000,
		},
		{
			input:    []byte("40960000"),
			expected: 40960000,
		},
		{
			input:    []byte(""),
			expected: 0,
		},
		{
			input:       []byte("invalid"),
			expectedErr: true,
		},
	} {
		t.Run(fmt.Sprintf("when input is %s", tcase.input), func(t *testing.T) {
			var b IntString

			require.True(t, (b.UnmarshalJSON(tcase.input) != nil) == tcase.expectedErr)
			// err := json.Unmarshal(tcase.input, &b)
			// require.Nil(t, err)

			require.Equal(t, IntString(tcase.expected), b)
		})
	}
}
