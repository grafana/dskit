package runtimeconfig

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestUnmarshalYAMLCompatibleJSONMatchesYAML(t *testing.T) {
	data := []byte(`{
		"small": 42,
		"large": 9007199254740993,
		"maxInt64": 9223372036854775807,
		"aboveMaxInt64": 9223372036854775808,
		"maxUint64": 18446744073709551615,
		"fraction": 1.25,
		"exponent": 1e3,
		"negativeZero": -0,
		"nested": [{"value": 7}, null, true, "text"],
		"escaped": "\u2028"
	}`)

	var expected map[string]any
	require.NoError(t, yaml.Unmarshal(data, &expected))

	actual, err := unmarshalYAMLCompatibleJSON(data)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestUnmarshalJSONOrYAMLPreservesYAMLValidation(t *testing.T) {
	tests := map[string][]byte{
		"duplicate key":         []byte(`{"value": 1, "value": 2}`),
		"escaped duplicate key": []byte(`{"value": 1, "\u0076alue": 2}`),
		"nested duplicate key":  []byte(`{"values": [{"value": 1, "value": 2}]}`),
		"invalid UTF-8":         {'{', '"', 'v', '"', ':', '"', 0xff, '"', '}'},
		"escaped solidus":       []byte(`{"value": "a\/b"}`),
		"surrogate escape":      []byte(`{"value": "\uD83D\uDE00"}`),
	}

	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			var expected map[string]any
			expectedErr := yaml.Unmarshal(data, &expected)
			require.Error(t, expectedErr)

			_, err := unmarshalYAMLCompatibleJSON(data)
			require.Error(t, err)

			_, err = unmarshalJSONOrYAML(data)
			require.Error(t, err)
		})
	}
}

func TestUnmarshalJSONOrYAMLPreservesYAMLRawCharacterValidation(t *testing.T) {
	for name, r := range map[string]rune{
		"delete":                  '\u007f',
		"control":                 '\u0080',
		"application program cmd": '\u009f',
		"noncharacter U+FFFE":     '\ufffe',
		"noncharacter U+FFFF":     '\uffff',
	} {
		t.Run(name, func(t *testing.T) {
			data := []byte(`{"value": "before` + string(r) + `after"}`)

			var expected map[string]any
			require.Error(t, yaml.Unmarshal(data, &expected))

			_, err := unmarshalYAMLCompatibleJSON(data)
			require.Error(t, err)
			_, err = unmarshalJSONOrYAML(data)
			require.Error(t, err)
		})
	}
}

func TestUnmarshalJSONOrYAMLPreservesYAMLRawLineBreaks(t *testing.T) {
	for name, r := range map[string]rune{
		"next line":           '\u0085',
		"line separator":      '\u2028',
		"paragraph separator": '\u2029',
	} {
		t.Run(name, func(t *testing.T) {
			data := []byte(`{"value": "before` + string(r) + `  after"}`)

			var expected map[string]any
			require.NoError(t, yaml.Unmarshal(data, &expected))

			_, err := unmarshalYAMLCompatibleJSON(data)
			require.Error(t, err)

			actual, err := unmarshalJSONOrYAML(data)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}

func TestUnmarshalYAMLCompatibleJSONAcceptsEscapedLineBreaksAndNoncharacters(t *testing.T) {
	data := []byte(`{
		"nextLine": "\u0085",
		"lineSeparator": "\u2028",
		"paragraphSeparator": "\u2029",
		"noncharacter": "\uFFFF"
	}`)

	var expected map[string]any
	require.NoError(t, yaml.Unmarshal(data, &expected))

	actual, err := unmarshalYAMLCompatibleJSON(data)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestValidateYAMLCompatibleJSONRejectsExcessiveNesting(t *testing.T) {
	data := []byte(`{"value":` +
		strings.Repeat("[", maxJSONNestingDepth) +
		`0` +
		strings.Repeat("]", maxJSONNestingDepth) +
		`}`)
	require.ErrorContains(t, validateYAMLCompatibleJSON(data), "JSON nesting depth exceeds")
}

func TestUnmarshalJSONOrYAMLFallsBackToYAML(t *testing.T) {
	for _, data := range [][]byte{
		[]byte(`{value: 1e400}`),
		[]byte(`{"value": 1e400}`),
		[]byte(`{"value": 1} {"ignored": 2}`),
	} {
		var expected map[string]any
		require.NoError(t, yaml.Unmarshal(data, &expected))

		actual, err := unmarshalJSONOrYAML(data)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
}

func TestUnmarshalMaybeGzippedJSONMatchesUncompressed(t *testing.T) {
	data := []byte(`{"limits":{"large":9007199254740993,"fraction":1.25}}`)

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	manager := &Manager{}
	uncompressed, err := manager.unmarshalMaybeGzipped("config.json", data)
	require.NoError(t, err)
	gzipped, err := manager.unmarshalMaybeGzipped("config.json.gz", compressed.Bytes())
	require.NoError(t, err)
	require.Equal(t, uncompressed, gzipped)

	duplicate := []byte(`{"limits":{"value":1,"value":2}}`)
	compressed.Reset()
	writer = gzip.NewWriter(&compressed)
	_, err = writer.Write(duplicate)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	_, plainErr := manager.unmarshalMaybeGzipped("config.json", duplicate)
	_, gzipErr := manager.unmarshalMaybeGzipped("config.json.gz", compressed.Bytes())
	require.Error(t, plainErr)
	require.Error(t, gzipErr)

	rawLineBreak := []byte(`{"limits":{"value":"before` + string('\u2028') + `  after"}}`)
	compressed.Reset()
	writer = gzip.NewWriter(&compressed)
	_, err = writer.Write(rawLineBreak)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	uncompressed, err = manager.unmarshalMaybeGzipped("config.json", rawLineBreak)
	require.NoError(t, err)
	gzipped, err = manager.unmarshalMaybeGzipped("config.json.gz", compressed.Bytes())
	require.NoError(t, err)
	require.Equal(t, uncompressed, gzipped)

	rawControl := []byte(`{"limits":{"value":"before` + string('\u0080') + `after"}}`)
	compressed.Reset()
	writer = gzip.NewWriter(&compressed)
	_, err = writer.Write(rawControl)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	_, plainErr = manager.unmarshalMaybeGzipped("config.json", rawControl)
	_, gzipErr = manager.unmarshalMaybeGzipped("config.json.gz", compressed.Bytes())
	require.Error(t, plainErr)
	require.Error(t, gzipErr)
}
