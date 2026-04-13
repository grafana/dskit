package tenant

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadata_WithTenant(t *testing.T) {
	tests := []struct {
		name     string
		metadata Metadata
		tenant   string
		expected string
	}{
		{
			name:     "single pair",
			metadata: Metadata{}.With("key", "value"),
			tenant:   "my-tenant",
			expected: "my-tenant:key=value",
		},
		{
			name:     "multiple pairs sorted by key",
			metadata: Metadata{}.With("product", "k6").With("env", "prod"),
			tenant:   "123456",
			expected: "123456:env=prod:product=k6",
		},
		{
			name:     "empty metadata",
			metadata: Metadata{},
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

func TestMetadata_IsEmpty(t *testing.T) {
	testCases := []struct {
		name  string
		input Metadata
		want  bool
	}{
		{
			name:  "empty string",
			input: NewMetadata(""),
			want:  true,
		},
		{
			name:  "one kv pair",
			input: NewMetadata(":a=b"),
			want:  false,
		},
		{
			name:  "multiple kv pairs",
			input: NewMetadata(":a=b:b=c"),
			want:  false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.input.IsEmpty())
		})
	}
}

func TestMetadata_DivideIter(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected [][2]string
	}{
		{
			name:  "empty input",
			input: "",
		},
		{
			name:     "multiple pairs",
			input:    ":env=prod:product=k6",
			expected: [][2]string{{"env", "prod"}, {"product", "k6"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			md := NewMetadata(tc.input)

			var got [][2]string
			for k, v := range md.Iter() {
				got = append(got, [2]string{k, v})
			}
			require.Equal(t, tc.expected, got)

			var expectedSubs, gotSubs []Metadata
			for _, kv := range tc.expected {
				expectedSubs = append(expectedSubs, Metadata{}.With(kv[0], kv[1]))
			}
			for sub := range md.Divide() {
				gotSubs = append(gotSubs, sub)
			}
			require.Equal(t, expectedSubs, gotSubs)
		})
	}
}

func TestMetadata_Set(t *testing.T) {
	var md Metadata

	// Insert new
	md.Set("product", "k6")
	require.Equal(t, ":product=k6", md.Encode())

	// Insert before
	md.Set("env", "prod")
	require.Equal(t, ":env=prod:product=k6", md.Encode())

	// Replace
	md.Set("env", "dev")
	require.Equal(t, ":env=dev:product=k6", md.Encode())

	// Insert between
	md.Set("foo", "bar")
	require.Equal(t, md.Encode(), ":env=dev:foo=bar:product=k6")
}
