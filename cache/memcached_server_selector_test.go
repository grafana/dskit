package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkMemcachedJumpHashSelector_PickServer(b *testing.B) {
	servers := []string{
		"localhost:11211",
		"localhost:11212",
		"localhost:11213",
		"localhost:11214",
		"localhost:11215",
		"localhost:11216",
		"localhost:11217",
		"localhost:11218",
		"localhost:11219",
		"localhost:11220",
		"localhost:11221",
		"localhost:11222",
		"localhost:11223",
		"localhost:11224",
		"localhost:11225",
		"localhost:11226",
		"localhost:11227",
		"localhost:11228",
		"localhost:11229",
		"localhost:11230",
	}

	selector := &MemcachedJumpHashSelector{}
	require.NoError(b, selector.SetServers(servers...))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := selector.PickServer("some-key")
		if err != nil {
			require.NoError(b, err)
		}
	}
}
