package flagext

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestStringSliceCSVQuoted(t *testing.T) {
	type TestStruct struct {
		CSV StringSliceCSVQuoted `yaml:"csv"`
	}

	var testStruct TestStruct
	s := "a,b,c,d"
	require.NoError(t, testStruct.CSV.Set(s))

	assert.Equal(t, []string{"a", "b", "c", "d"}, []string(testStruct.CSV))
	assert.Equal(t, s, testStruct.CSV.String())

	expected := []byte(`csv: a,b,c,d
`)

	actual, err := yaml.Marshal(testStruct)
	require.NoError(t, err)
	assert.Equal(t, expected, actual)

	var testStruct2 TestStruct

	err = yaml.Unmarshal(expected, &testStruct2)
	require.NoError(t, err)
	assert.Equal(t, testStruct, testStruct2)
}

func TestStringSliceCSVQuoted_Set(t *testing.T) {
	tests := map[string]struct {
		input    string
		expected []string
	}{
		"empty string":                     {input: "", expected: nil},
		"single value":                     {input: "a", expected: []string{"a"}},
		"multiple values":                  {input: "a,b,c", expected: []string{"a", "b", "c"}},
		"empty values":                     {input: "a,,b,", expected: []string{"a", "", "b", ""}},
		"whitespace is preserved":          {input: " a , b ", expected: []string{" a ", " b "}},
		"quoted value with comma":          {input: `a,"b,c",d`, expected: []string{"a", "b,c", "d"}},
		"only a quoted value":              {input: `"a,b"`, expected: []string{"a,b"}},
		"quoted empty value":               {input: `"",a`, expected: []string{"", "a"}},
		"quoted value with trailing comma": {input: `"a,b",`, expected: []string{"a,b", ""}},
		"escaped quotes in quoted value":   {input: `"a"",b""","c"`, expected: []string{`a",b"`, "c"}},
		"quoted value holding only quote":  {input: `""""`, expected: []string{`"`}},
		"quoted value with newline":        {input: "a,\"b\nc\"", expected: []string{"a", "b\nc"}},
		"trailing newline is ignored":      {input: "a,b\n", expected: []string{"a", "b"}},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var v StringSliceCSVQuoted
			require.NoError(t, v.Set(test.input))
			assert.Equal(t, test.expected, []string(v))

			// Only values that need quoting are quoted, so the round-trip must yield the
			// same values but is not required to reproduce the input.
			var reparsed StringSliceCSVQuoted
			require.NoError(t, reparsed.Set(v.String()))
			assert.Equal(t, test.expected, []string(reparsed))
		})
	}
}

func TestStringSliceCSVQuoted_SetMalformed(t *testing.T) {
	tests := map[string]string{
		"bare quote in unquoted value":    `a"b,c`,
		"missing closing quote":           `"a,b`,
		"unquoted text after value":       `"a"b,c`,
		"missing separator after value":   `"a" ,b`,
		"unquoted newline":                "a\nb,c",
		"unquoted newline in later value": "a,b\nc",
	}

	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			v := StringSliceCSVQuoted{"unchanged"}
			assert.Error(t, v.Set(input))
			assert.Equal(t, []string{"unchanged"}, []string(v))
		})
	}
}

func TestStringSliceCSVQuoted_String(t *testing.T) {
	tests := map[string]struct {
		input    StringSliceCSVQuoted
		expected string
	}{
		"nil":                        {input: nil, expected: ""},
		"values without quoting":     {input: StringSliceCSVQuoted{"a", "b"}, expected: "a,b"},
		"empty values":               {input: StringSliceCSVQuoted{"a", "", "b"}, expected: "a,,b"},
		"value with comma":           {input: StringSliceCSVQuoted{"a,b", "c"}, expected: `"a,b",c`},
		"value with quote":           {input: StringSliceCSVQuoted{`a"b`}, expected: `"a""b"`},
		"value with comma and quote": {input: StringSliceCSVQuoted{`a",b`}, expected: `"a"",b"`},
		"value with newline":         {input: StringSliceCSVQuoted{"a\nb"}, expected: "\"a\nb\""},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.input.String())
		})
	}
}

func TestStringSliceCSVQuoted_YamlRoundTrip(t *testing.T) {
	type TestStruct struct {
		CSV StringSliceCSVQuoted `yaml:"csv"`
	}

	testStruct := TestStruct{CSV: StringSliceCSVQuoted{"a", "b,c", `d"e`, "f\ng"}}

	actual, err := yaml.Marshal(testStruct)
	require.NoError(t, err)

	var testStruct2 TestStruct
	require.NoError(t, yaml.Unmarshal(actual, &testStruct2))
	assert.Equal(t, testStruct, testStruct2)
}

func TestStringSliceCSVQuoted_EmptyYaml(t *testing.T) {
	type TestStruct struct {
		CSV StringSliceCSVQuoted `yaml:"csv"`
	}

	var testStructEmpty = TestStruct{CSV: nil}

	assert.Empty(t, testStructEmpty.CSV)
	expected := []byte(`csv: ""
`)
	actual, err := yaml.Marshal(testStructEmpty)
	require.NoError(t, err)
	assert.Equal(t, expected, actual)

	var testStruct2 TestStruct

	err = yaml.Unmarshal(actual, &testStruct2)
	require.NoError(t, err)
	assert.Equal(t, testStructEmpty, testStruct2)
}
