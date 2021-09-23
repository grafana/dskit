package testutils

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
)

// NumTokens determines the number of tokens owned by the specified
// address
func NumTokens(c kv.Client, name, ringKey string) int {
	ringDesc, err := c.Get(context.Background(), ringKey)

	// The ringDesc may be null if the lifecycler hasn't stored the ring
	// to the KVStore yet.
	if ringDesc == nil || err != nil {
		level.Error(log.NewNopLogger()).Log("msg", "error reading consul", "err", err)
		return 0
	}
	rd := ringDesc.(*ring.Desc)
	return len(rd.Ingesters[name].Tokens)
}
