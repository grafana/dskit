package miekgdns2

import (
	"context"
	"errors"
	"net"
	"os"
	"path"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

var resolvConfContents = `
nameserver 127.0.0.1
nameserver 127.0.0.2
option attempts:2
option timeout:1
`

func TestResolver_LookupSRV(t *testing.T) {
	t.Run("bad resolv.conf", func(t *testing.T) {
		cfgPath := path.Join(t.TempDir(), "resolv.conf")
		client := newMockClient()

		resolver := NewResolverWithClient(cfgPath, client)
		_, _, err := resolver.LookupSRV(context.Background(), "cache", "tcp", "example.com")

		require.Error(t, err)
	})

	t.Run("multiple timeouts", func(t *testing.T) {
		cfgPath := writeResovConf(t, resolvConfContents)
		client := newMockClient()
		client.err["127.0.0.1:53"] = errors.New("timeout 1 in test")
		client.err["127.0.0.2:53"] = errors.New("timeout 2 in test")

		resolver := NewResolverWithClient(cfgPath, client)
		_, _, err := resolver.LookupSRV(context.Background(), "cache", "tcp", "example.com")

		require.Error(t, err)
	})

	t.Run("one timeout and one success", func(t *testing.T) {
		cfgPath := writeResovConf(t, resolvConfContents)
		client := newMockClient()
		client.err["127.0.0.1:53"] = errors.New("timeout 1 in test")
		client.res["127.0.0.2:53"] = []*dns.Msg{newSrvDNSResponse("_cache._tcp.example.com.", "cache01.example.com.")}

		resolver := NewResolverWithClient(cfgPath, client)
		_, res, err := resolver.LookupSRV(context.Background(), "cache", "tcp", "example.com")

		require.Equal(t, []*net.SRV{
			{
				Target:   "cache01.example.com.",
				Port:     11211,
				Priority: 10,
				Weight:   100,
			},
		}, res)
		require.NoError(t, err)
	})

	t.Run("name error", func(t *testing.T) {
		response := new(dns.Msg).SetQuestion("_cache._tcp.example.com.", dns.TypeSRV)
		response.Rcode = dns.RcodeNameError
		response.Response = true

		cfgPath := writeResovConf(t, resolvConfContents)
		client := newMockClient()
		client.err["127.0.0.1:53"] = errors.New("timeout 1 in test")
		client.res["127.0.0.2:53"] = []*dns.Msg{response}

		resolver := NewResolverWithClient(cfgPath, client)
		_, res, err := resolver.LookupSRV(context.Background(), "cache", "tcp", "example.com")

		require.Empty(t, res)
		require.NoError(t, err)
	})

	t.Run("truncated", func(t *testing.T) {
		response := newSrvDNSResponse("_cache._tcp.example.com.", "cache01.example.com.")
		response.Truncated = true

		cfgPath := writeResovConf(t, resolvConfContents)
		client := newMockClient()
		// Include two responses, one for each attempt made since truncation triggers retries
		client.res["127.0.0.1:53"] = []*dns.Msg{response, response}
		client.err["127.0.0.2:53"] = errors.New("timeout 2 in test")

		resolver := NewResolverWithClient(cfgPath, client)
		_, res, err := resolver.LookupSRV(context.Background(), "cache", "tcp", "example.com")

		require.Nil(t, res)
		require.Error(t, err)
	})
}

func TestResolver_LookupIP(t *testing.T) {
	t.Run("bad resolv.conf", func(t *testing.T) {
		cfgPath := path.Join(t.TempDir(), "resolv.conf")
		client := newMockClient()

		resolver := NewResolverWithClient(cfgPath, client)
		_, err := resolver.LookupIPAddr(context.Background(), "cache01.example.com.")

		require.Error(t, err)
	})

	t.Run("multiple timeouts", func(t *testing.T) {
		cfgPath := writeResovConf(t, resolvConfContents)
		client := newMockClient()
		client.err["127.0.0.1:53"] = errors.New("timeout 1 in test")
		client.err["127.0.0.2:53"] = errors.New("timeout 2 in test")

		resolver := NewResolverWithClient(cfgPath, client)
		_, err := resolver.LookupIPAddr(context.Background(), "cache01.example.com.")

		require.Error(t, err)
	})

	t.Run("one timeout and one success", func(t *testing.T) {
		cfgPath := writeResovConf(t, resolvConfContents)
		client := newMockClient()
		client.err["127.0.0.1:53"] = errors.New("timeout 1 in test")
		client.res["127.0.0.2:53"] = []*dns.Msg{newIPDNSResponse("cache01.example.com.", net.IPv4(10, 0, 0, 1))}

		resolver := NewResolverWithClient(cfgPath, client)
		res, err := resolver.LookupIPAddr(context.Background(), "cache01.example.com")

		require.Equal(t, []net.IPAddr{{IP: net.IPv4(10, 0, 0, 1)}}, res)
		require.NoError(t, err)
	})

	t.Run("name error", func(t *testing.T) {
		response1 := new(dns.Msg).SetQuestion("cache01.example.com.", dns.TypeAAAA)
		response1.Rcode = dns.RcodeNameError
		response1.Response = true

		response2 := new(dns.Msg).SetQuestion("cache01.example.com.", dns.TypeA)
		response2.Rcode = dns.RcodeNameError
		response2.Response = true

		cfgPath := writeResovConf(t, resolvConfContents)
		client := newMockClient()
		client.err["127.0.0.1:53"] = errors.New("timeout 1 in test")
		// Include two responses since a failed AAAA lookup will result in an A fallback
		client.res["127.0.0.2:53"] = []*dns.Msg{response1, response2}

		resolver := NewResolverWithClient(cfgPath, client)
		res, err := resolver.LookupIPAddr(context.Background(), "cache01.example.com")

		require.Empty(t, res)
		require.NoError(t, err)
	})

	t.Run("one level of CNAME", func(t *testing.T) {
		cfgPath := writeResovConf(t, resolvConfContents)
		client := newMockClient()
		client.err["127.0.0.1:53"] = errors.New("timeout 1 in test")
		client.res["127.0.0.2:53"] = []*dns.Msg{
			newCnameDNSResponse("cache01.example.com.", "cache01.east.example.com."),
			newIPDNSResponse("cache01.east.example.com.", net.IPv4(10, 0, 0, 1)),
		}

		resolver := NewResolverWithClient(cfgPath, client)
		res, err := resolver.LookupIPAddr(context.Background(), "cache01.example.com")

		require.Equal(t, []net.IPAddr{{IP: net.IPv4(10, 0, 0, 1)}}, res)
		require.NoError(t, err)
	})
}

func TestPoolingClient(t *testing.T) {
	// NOTE: This test talks to the local DNS server over the network

	conf := getClientConfig(t)
	server := net.JoinHostPort(conf.Servers[0], conf.Port)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	msg := new(dns.Msg).SetQuestion(dns.Fqdn("localhost"), dns.TypeA)
	client := NewPoolingClient(defaultMaxConnsPerHost)

	t.Run("exchange", func(t *testing.T) {
		resp, _, err := client.Exchange(ctx, msg, server)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Answer)
		require.Len(t, client.pools, 1)
	})

	t.Run("clean", func(t *testing.T) {
		client.Clean([]string{})
		require.Empty(t, client.pools)
	})
}

func TestPool(t *testing.T) {
	// NOTE: This test talks to the local DNS server over the network

	conf := getClientConfig(t)
	server := net.JoinHostPort(conf.Servers[0], conf.Port)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pool := NewPool(defaultMaxConnsPerHost)
	var conn *dns.Conn
	var err error

	t.Run("get connection", func(t *testing.T) {
		conn, err = pool.Get(ctx, "tcp", server)
		require.NoError(t, err)
		require.NotNil(t, conn)
	})

	t.Run("put connection", func(t *testing.T) {
		require.NoError(t, pool.Put(conn))
	})

	t.Run("close", func(t *testing.T) {
		pool.Close()
		_, err = pool.Get(ctx, "tcp", server)
		require.Error(t, err)
	})
}

type mockClient struct {
	err     map[string]error
	res     map[string][]*dns.Msg
	cleaned atomic.Uint64
}

func newMockClient() *mockClient {
	return &mockClient{
		err: make(map[string]error),
		res: make(map[string][]*dns.Msg),
	}
}

func (m *mockClient) Exchange(_ context.Context, _ *dns.Msg, server string) (*dns.Msg, time.Duration, error) {
	if err, ok := m.err[server]; ok {
		return nil, 0, err
	}

	// Shift the first element off the front of the slice
	response := m.res[server][0]
	m.res[server] = m.res[server][1:]

	return response, 0, nil
}

func (m *mockClient) Clean([]string) {
	m.cleaned.Add(1)
}

func writeResovConf(t *testing.T, contents string) string {
	p := path.Join(t.TempDir(), "resolv.conf")
	require.NoError(t, os.WriteFile(p, []byte(contents), 0777))
	return p
}

func getClientConfig(t *testing.T) *dns.ClientConfig {
	conf, err := dns.ClientConfigFromFile(DefaultResolvConfPath)
	require.NoError(t, err)
	return conf
}

func newSrvDNSResponse(host string, target string) *dns.Msg {
	request := new(dns.Msg).SetQuestion(host, dns.TypeSRV)
	response := new(dns.Msg).SetReply(request)
	response.Answer = append(response.Answer, &dns.SRV{
		Hdr: dns.RR_Header{
			Name:     host,
			Rrtype:   dns.TypeSRV,
			Class:    dns.ClassINET,
			Ttl:      30,
			Rdlength: 0, // our client ignores the header
		},
		Priority: 10,
		Weight:   100,
		Port:     11211,
		Target:   target,
	})

	return response
}

func newIPDNSResponse(host string, addr net.IP) *dns.Msg {
	request := new(dns.Msg).SetQuestion(host, dns.TypeA)
	response := new(dns.Msg).SetReply(request)
	response.Answer = append(response.Answer, &dns.A{
		Hdr: dns.RR_Header{
			Name:     host,
			Rrtype:   dns.TypeA,
			Class:    dns.ClassINET,
			Ttl:      30,
			Rdlength: 4,
		},
		A: addr,
	})

	return response
}

func newCnameDNSResponse(host string, target string) *dns.Msg {
	request := new(dns.Msg).SetQuestion(host, dns.TypeCNAME)
	response := new(dns.Msg).SetReply(request)
	response.Answer = append(response.Answer, &dns.CNAME{
		Hdr: dns.RR_Header{
			Name:     host,
			Rrtype:   dns.TypeCNAME,
			Class:    dns.ClassINET,
			Ttl:      30,
			Rdlength: 4,
		},
		Target: target,
	})

	return response
}
