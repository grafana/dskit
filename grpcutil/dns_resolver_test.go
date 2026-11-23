package grpcutil

import (
	"context"
	"errors"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestDNSWatcher_LookupFailureKeepsKnownAddresses(t *testing.T) {
	origLookupHost := lookupHost
	t.Cleanup(func() { lookupHost = origLookupHost })

	var (
		addrs   = []string{"10.0.0.1", "10.0.0.2"}
		lookErr error
	)
	lookupHost = func(context.Context, string) ([]string, error) {
		return addrs, lookErr
	}

	r, err := NewDNSResolver(log.NewNopLogger())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := &dnsWatcher{r: r, logger: log.NewNopLogger(), host: "example.local", port: "9095", ctx: ctx, cancel: cancel}

	updates := w.lookup()
	require.Len(t, updates, 2)
	for _, u := range updates {
		require.Equal(t, Add, u.Op)
	}

	// A transient lookup failure must not be reported as the removal of every known address.
	lookErr = errors.New("temporary DNS failure")
	require.Empty(t, w.lookup())

	// Once DNS recovers with an unchanged answer there is nothing to report either.
	lookErr = nil
	require.Empty(t, w.lookup())

	// A successful lookup that returns fewer addresses still removes the missing ones.
	addrs = []string{"10.0.0.1"}
	updates = w.lookup()
	require.Len(t, updates, 1)
	require.Equal(t, Delete, updates[0].Op)
	require.Equal(t, "10.0.0.2:9095", updates[0].Addr)
}
