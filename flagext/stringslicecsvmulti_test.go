package flagext

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestStringSliceCSVMulti(t *testing.T) {
	type TestStruct struct {
		CSV StringSliceCSVMulti `yaml:"csv"`
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

func TestStringSliceCSVMulti_MultipleSet(t *testing.T) {
	var v StringSliceCSVMulti

	require.NoError(t, v.Set("a,b"))
	require.NoError(t, v.Set("c"))
	require.NoError(t, v.Set("d,e,f"))

	assert.Equal(t, []string{"a", "b", "c", "d", "e", "f"}, []string(v))
	assert.Equal(t, "a,b,c,d,e,f", v.String())
}

func TestStringSliceCSVMulti_EmptyString(t *testing.T) {
	type TestStruct struct {
		CSV StringSliceCSVMulti `yaml:"csv"`
	}

	var testStructEmpty = TestStruct{CSV: nil}

	assert.Len(t, testStructEmpty.CSV, 0)
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

func TestStringSliceCSVMulti_EmptyStringPreservesExisting(t *testing.T) {
	var v StringSliceCSVMulti
	require.NoError(t, v.Set("a,b"))
	require.NoError(t, v.Set(""))
	require.NoError(t, v.Set("c"))

	assert.Equal(t, []string{"a", "b", "c"}, []string(v))
}
