package tenant

import (
	"context"
	"strings"
	"testing"

	"github.com/grafana/dskit/user"

	"github.com/stretchr/testify/assert"
)

func TestValidTenantIDs(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  *string
	}{
		{
			name: "tenant-a",
		},
		{
			name: "ABCDEFGHIJKLMNOPQRSTUVWXYZ-abcdefghijklmnopqrstuvwxyz_0987654321!.*'()",
		},
		{
			name: "invalid|",
			err:  strptr("tenant ID 'invalid|' contains unsupported character '|'"),
		},
		{
			name: strings.Repeat("a", 150),
		},
		{
			name: strings.Repeat("a", 151),
			err:  strptr("tenant ID is too long: max 150 characters"),
		},
		{
			name: ".",
			err:  strptr("tenant ID is '.' or '..'"),
		},
		{
			name: "..",
			err:  strptr("tenant ID is '.' or '..'"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidTenantID(tc.name)
			if tc.err == nil {
				assert.Nil(t, err)
			} else {
				assert.EqualError(t, err, *tc.err)
			}
		})
	}
}

func BenchmarkTenantID(b *testing.B) {
	singleCtx := context.Background()
	singleCtx = user.InjectOrgID(singleCtx, "tenant-a")
	multiCtx := context.Background()
	multiCtx = user.InjectOrgID(multiCtx, "tenant-a|tenant-b|tenant-c")

	b.ResetTimer()
	b.ReportAllocs()
	b.Run("single", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = TenantID(singleCtx)
		}
	})
	b.Run("multi", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = TenantID(multiCtx)
		}
	})
}

func BenchmarkSubtenantID(b *testing.B) {
	singleCtx := context.Background()
	singleCtx = user.InjectOrgID(singleCtx, "tenant-a:k6")
	singleNoSubCtx := context.Background()
	singleNoSubCtx = user.InjectOrgID(singleNoSubCtx, "tenant-a")
	multiCtx := context.Background()
	multiCtx = user.InjectOrgID(multiCtx, "tenant-a:k6|tenant-a:k6")

	b.ResetTimer()
	b.ReportAllocs()
	b.Run("single-with-subtenant", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = SubtenantID(singleCtx)
		}
	})
	b.Run("single-no-subtenant", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = SubtenantID(singleNoSubCtx)
		}
	})
	b.Run("multi-same-tenant", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = SubtenantID(multiCtx)
		}
	})
}

func BenchmarkStringSearch(b *testing.B) {
	// Benchmark comparing strings.IndexByte vs strings.Contains for single char search
	testStrings := []struct {
		name string
		s    string
	}{
		{"short-no-match", "tenant-a"},
		{"short-match", "tenant:k6"},
		{"medium-no-match", "tenant-abcdefghijklmnop"},
		{"medium-match-end", "tenant-abcdefghijklmnop:k6"},
		{"long-no-match", "tenant-abcdefghijklmnopqrstuvwxyz0123456789"},
		{"long-match-end", "tenant-abcdefghijklmnopqrstuvwxyz0123456789:k6"},
	}

	for _, ts := range testStrings {
		b.Run("IndexByte/"+ts.name, func(b *testing.B) {
			s := ts.s
			for i := 0; i < b.N; i++ {
				_ = strings.IndexByte(s, ':')
			}
		})
		b.Run("Contains/"+ts.name, func(b *testing.B) {
			s := ts.s
			for i := 0; i < b.N; i++ {
				_ = strings.Contains(s, ":")
			}
		})
	}
}
