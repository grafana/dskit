// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dns

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestProvider(t *testing.T) {
	ips := []string{
		"127.0.0.1:19091",
		"127.0.0.2:19092",
		"127.0.0.3:19093",
		"127.0.0.4:19094",
		"127.0.0.5:19095",
	}

	prv := NewProvider(log.NewNopLogger(), nil, "")
	prv.resolver = &mockResolver{
		res: map[string][]string{
			"a": ips[:2],
			"b": ips[2:4],
			"c": {ips[4]},
		},
	}
	ctx := context.TODO()

	checkMetrics := func(metrics string) {
		const metadata = `
		# HELP dns_provider_results The number of resolved endpoints for each configured address
		# TYPE dns_provider_results gauge
		`
		expected := strings.NewReader(metadata + metrics + "\n")
		assert.NoError(t, testutil.CollectAndCompare(prv, expected))
	}
	err := prv.Resolve(ctx, []string{"any+x"})
	assert.NoError(t, err)
	result := prv.Addresses()
	sort.Strings(result)
	assert.Equal(t, []string(nil), result)
	checkMetrics(`dns_provider_results{addr="any+x"} 0`)

	err = prv.Resolve(ctx, []string{"any+a", "any+b", "any+c"})
	assert.NoError(t, err)
	result = prv.Addresses()
	sort.Strings(result)
	assert.Equal(t, ips, result)
	checkMetrics(`
		dns_provider_results{addr="any+a"} 2
		dns_provider_results{addr="any+b"} 2
		dns_provider_results{addr="any+c"} 1`)

	err = prv.Resolve(ctx, []string{"any+b", "any+c"})
	assert.NoError(t, err)
	result = prv.Addresses()
	sort.Strings(result)
	assert.Equal(t, ips[2:], result)
	checkMetrics(`
		dns_provider_results{addr="any+b"} 2
		dns_provider_results{addr="any+c"} 1`)

	err = prv.Resolve(ctx, []string{"any+x"})
	assert.NoError(t, err)
	result = prv.Addresses()
	sort.Strings(result)
	assert.Equal(t, []string(nil), result)
	checkMetrics(`dns_provider_results{addr="any+x"} 0`)

	err = prv.Resolve(ctx, []string{"any+a", "any+b", "any+c"})
	assert.NoError(t, err)
	result = prv.Addresses()
	sort.Strings(result)
	assert.Equal(t, ips, result)
	checkMetrics(`
		dns_provider_results{addr="any+a"} 2
		dns_provider_results{addr="any+b"} 2
		dns_provider_results{addr="any+c"} 1`)

	err = prv.Resolve(ctx, []string{"any+b", "example.com:90", "any+c"})
	assert.NoError(t, err)
	result = prv.Addresses()
	sort.Strings(result)
	assert.Equal(t, append(ips[2:], "example.com:90"), result)
	checkMetrics(`
		dns_provider_results{addr="any+b"} 2
		dns_provider_results{addr="example.com:90"} 1
		dns_provider_results{addr="any+c"} 1`)
	err = prv.Resolve(ctx, []string{"any+b", "any+c"})
	assert.NoError(t, err)
	result = prv.Addresses()
	sort.Strings(result)
	assert.Equal(t, ips[2:], result)
	checkMetrics(`
		dns_provider_results{addr="any+b"} 2
		dns_provider_results{addr="any+c"} 1`)
}

type mockResolver struct {
	res map[string][]string
	err error
}

func (d *mockResolver) Resolve(_ context.Context, name string, _ QType) ([]string, error) {
	if d.err != nil {
		return nil, d.err
	}
	return d.res[name], nil
}

// TestIsDynamicNode tests whether we properly catch dynamically defined nodes.
func TestIsDynamicNode(t *testing.T) {
	for _, tcase := range []struct {
		node      string
		isDynamic bool
	}{
		{
			node:      "1.2.3.4",
			isDynamic: false,
		},
		{
			node:      "gibberish+1.1.1.1+noa",
			isDynamic: true,
		},
		{
			node:      "",
			isDynamic: false,
		},
		{
			node:      "dns+aaa",
			isDynamic: true,
		},
		{
			node:      "dnssrv+asdasdsa",
			isDynamic: true,
		},
	} {
		isDynamic := IsDynamicNode(tcase.node)
		assert.Equal(t, tcase.isDynamic, isDynamic, "mismatch between results")
	}
}
