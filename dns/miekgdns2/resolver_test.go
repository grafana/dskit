package miekgdns2

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

func TestResolver_LookupSRV(t *testing.T) {

}

func TestResolver_LookupIP(t *testing.T) {

}

func TestPoolingClient(t *testing.T) {
	conf := getClientConfig(t)
	server := net.JoinHostPort(conf.Servers[0], conf.Port)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	msg := new(dns.Msg).SetQuestion(dns.Fqdn("localhost"), dns.TypeA)
	client := NewPoolingClient(defaultMaxConnsPerHost)

	resp, _, err := client.Exchange(ctx, msg, server)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Answer)
	require.Len(t, client.pools, 1)

	client.Clean([]string{})
	require.Empty(t, client.pools)
}

func TestPool(t *testing.T) {
	conf := getClientConfig(t)
	server := net.JoinHostPort(conf.Servers[0], conf.Port)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Create a connection, return it to the pool, then shutdown the pool.
	pool := NewPool(defaultMaxConnsPerHost)
	conn, err := pool.Get(ctx, "tcp", server)
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.NoError(t, pool.Put(conn))
	pool.Close()

	_, err = pool.Get(ctx, "tcp", server)
	require.ErrorIs(t, err, ErrPoolClosed)
}

func getClientConfig(t *testing.T) *dns.ClientConfig {
	conf, err := dns.ClientConfigFromFile(DefaultResolvConfPath)
	require.NoError(t, err)
	return conf
}
