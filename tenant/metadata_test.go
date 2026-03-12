package tenant

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadata_WithTenant(t *testing.T) {
	tests := []struct {
		name     string
		metadata *Metadata
		tenant   string
		expected string
	}{
		{
			name: "single pair",
			metadata: &Metadata{data: map[string]string{
				"key": "value",
			}},
			tenant:   "my-tenant",
			expected: "my-tenant:key=value",
		},
		{
			name: "multiple pairs sorted by key",
			metadata: &Metadata{data: map[string]string{
				"product": "k6",
				"env":     "prod",
			}},
			tenant:   "123456",
			expected: "123456:env=prod:product=k6",
		},
		{
			name:     "empty metadata",
			metadata: &Metadata{data: map[string]string{}},
			tenant:   "tenant-a",
			expected: "tenant-a",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.metadata.WithTenant(tc.tenant))
		})
	}
}

func TestMetadata_Has(t *testing.T) {
	md := NewMetadata()
	md.Set("key", "value")
	assert.True(t, md.Has("key"))
	assert.False(t, md.Has("missing"))
}

func TestMetadata_Get(t *testing.T) {
	var md Metadata
	md.Set("key", "value")
	val, ok := md.Get("key")
	assert.True(t, ok)
	assert.Equal(t, "value", val)

	val, ok = md.Get("missing")
	assert.False(t, ok)
	assert.Equal(t, "", val)
}

func Test_ParseMetadata(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Metadata
		errMsg   string
	}{
		{
			name:  "empty input",
			input: "",
		},
		{
			name:  "single pair",
			input: "key=value",
			expected: Metadata{data: map[string]string{
				"key": "value",
			}},
		},
		{
			name:  "multiple pairs",
			input: "product=k6:env=prod",
			expected: Metadata{data: map[string]string{
				"product": "k6",
				"env":     "prod",
			}},
		},
		{
			name:   "missing equals sign",
			input:  "noequalssign",
			errMsg: "invalid key value pair",
		},
		{
			name:   "one valid one invalid pair",
			input:  "key=value:bad",
			errMsg: "invalid key value pair",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			md, err := ParseMetadata(tc.input)
			if tc.errMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, md)
		})
	}
}
