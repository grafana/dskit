package flagext

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v2"
)

func TestSecretdYAML(t *testing.T) {
	// Test embedding of Secret.
	{
		type TestStruct struct {
			Secret Secret `yaml:"secret"`
		}

		var testStruct TestStruct
		require.NoError(t, testStruct.Secret.Set("pa55w0rd"))
		expected := []byte(`secret: '********'
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		yamlSecret := []byte(`secret: pa55w0rd
`)
		err = yaml.Unmarshal(yamlSecret, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}

	// Test pointers of Secret.
	{
		type TestStruct struct {
			Secret *Secret `yaml:"secret"`
		}

		var testStruct TestStruct
		testStruct.Secret = &Secret{}
		require.NoError(t, testStruct.Secret.Set("pa55w0rd"))
		expected := []byte(`secret: '********'
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		yamlSecret := []byte(`secret: pa55w0rd
`)
		err = yaml.Unmarshal(yamlSecret, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}

	// Test no value set in Secret.
	{
		type TestStruct struct {
			Secret Secret `yaml:"secret"`
		}
		var testStruct TestStruct
		expected := []byte(`secret: ""
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		err = yaml.Unmarshal(expected, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}
}

func TestSecret_Equals(t *testing.T) {
	t.Run("equal", func(t *testing.T) {
		tc := []struct {
			name string
			s1   Secret
			s2   Secret
		}{
			{
				name: "same value",
				s1:   Secret{value: "somesecret"},
				s2:   Secret{value: "somesecret"},
			},
			{
				name: "empty value",
				s1:   Secret{value: ""},
				s2:   Secret{value: ""},
			},
		}

		for _, tt := range tc {
			require.True(t, tt.s1.Equal(tt.s2), cmp.Diff(tt.s1, tt.s2))
			require.True(t, cmp.Equal(tt.s1, tt.s2), cmp.Diff(tt.s1, tt.s2))
		}
	})

	t.Run("not equal", func(t *testing.T) {
		tc := []struct {
			name string
			s1   Secret
			s2   Secret
		}{
			{
				name: "different value",
				s1:   Secret{value: "somesecret"},
				s2:   Secret{value: "anothersecret"},
			},
			{
				name: "MarshalYAMLs to same value but different",
				s1:   Secret{value: "secretone"},
				s2:   Secret{value: "secrettwo"},
			},
			{
				name: "one empty value",
				s1:   Secret{value: "somesecret"},
				s2:   Secret{value: ""},
			},
		}

		for _, tt := range tc {
			require.False(t, tt.s1.Equal(tt.s2), cmp.Diff(tt.s1, tt.s2))
			require.False(t, cmp.Equal(tt.s1, tt.s2), cmp.Diff(tt.s1, tt.s2))
		}
	})
}
