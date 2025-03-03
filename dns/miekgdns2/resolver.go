// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/discovery/dns/miekgdns/resolver.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package miekgdns2

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/miekg/dns"

	"github.com/grafana/dskit/multierror"
)

const (
	DefaultResolvConfPath  = "/etc/resolv.conf"
	defaultMaxConnsPerHost = 2
)

type Client interface {
	Exchange(ctx context.Context, msg *dns.Msg, server string) (*dns.Msg, time.Duration, error)
	Clean(known []string)
}

// Resolver is a DNS service discovery backend that retries on errors, only uses TCP,
// and pools connections to nameservers to increase reliability.
//
// This backend:
// * Does _not_ use search domains, all names are assumed to be fully qualified
// * Only uses TCP connections to the nameservers
// * Keeps several connections to each nameserver open
// * Reads resolv.conf on every query
// * Closes connections to unknown nameservers on every query
type Resolver struct {
	confPath string
	client   Client
}

// NewResolver creates a new Resolver that uses the provided resolv.conf configuration
// to perform DNS queries.
func NewResolver(resolvConf string) *Resolver {
	return NewResolverWithClient(resolvConf, NewPoolingClient(defaultMaxConnsPerHost))
}

// NewResolverWithClient creates a new Resolver that uses the provided resolv.conf configuration
// and Client implementation to perform DNS queries.
func NewResolverWithClient(resolvConf string, client Client) *Resolver {
	return &Resolver{
		confPath: resolvConf,
		client:   client,
	}
}

func (r *Resolver) IsNotFound(error) bool {
	// We don't return an error when there are no hosts found so this is
	// always false. Instead, we return the empty DNS response with the
	// appropriate return code set.
	return false
}

func (r *Resolver) LookupSRV(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error) {
	servers, conf, err := r.loadConfiguration()
	if err != nil {
		return "", nil, err
	}

	// Close connections to nameservers no longer configured in resolv.conf
	r.client.Clean(servers)
	return r.lookupSRV(ctx, servers, conf, service, proto, name)
}

func (r *Resolver) lookupSRV(ctx context.Context, servers []string, conf *dns.ClientConfig, service, proto, name string) (cname string, addrs []*net.SRV, err error) {
	var target string
	if service == "" && proto == "" {
		target = name
	} else {
		target = "_" + service + "._" + proto + "." + name
	}

	response, err := r.query(ctx, servers, conf.Attempts, target, dns.Type(dns.TypeSRV))
	if err != nil {
		return "", nil, err
	}

	for _, record := range response.Answer {
		switch addr := record.(type) {
		case *dns.SRV:
			addrs = append(addrs, &net.SRV{
				Weight:   addr.Weight,
				Target:   addr.Target,
				Priority: addr.Priority,
				Port:     addr.Port,
			})
		default:
			return "", nil, fmt.Errorf("invalid SRV response record %s", record)
		}
	}

	return "", addrs, err
}

func (r *Resolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	servers, conf, err := r.loadConfiguration()
	if err != nil {
		return nil, err
	}

	// Close connections to nameservers no longer configured in resolv.conf
	r.client.Clean(servers)
	return r.lookupIPAddr(ctx, servers, conf, host, 1, 8)
}

func (r *Resolver) lookupIPAddr(ctx context.Context, servers []string, conf *dns.ClientConfig, host string, currIteration, maxIterations int) ([]net.IPAddr, error) {
	// We want to protect from infinite loops when resolving DNS records recursively.
	if currIteration > maxIterations {
		return nil, fmt.Errorf("maximum number of recursive iterations reached (%d)", maxIterations)
	}

	response, err := r.query(ctx, servers, conf.Attempts, host, dns.Type(dns.TypeAAAA))
	if err != nil || len(response.Answer) == 0 {
		// Ugly fallback to A lookup.
		response, err = r.query(ctx, servers, conf.Attempts, host, dns.Type(dns.TypeA))
		if err != nil {
			return nil, err
		}
	}

	var resp []net.IPAddr
	for _, record := range response.Answer {
		switch addr := record.(type) {
		case *dns.A:
			resp = append(resp, net.IPAddr{IP: addr.A})
		case *dns.AAAA:
			resp = append(resp, net.IPAddr{IP: addr.AAAA})
		case *dns.CNAME:
			// Recursively resolve it.
			addrs, err := r.lookupIPAddr(ctx, servers, conf, addr.Target, currIteration+1, maxIterations)
			if err != nil {
				return nil, fmt.Errorf("%w: recursively resolve %s", err, addr.Target)
			}
			resp = append(resp, addrs...)
		default:
			return nil, fmt.Errorf("invalid A, AAAA or CNAME response record %s", record)
		}
	}
	return resp, nil
}

func (r *Resolver) query(ctx context.Context, servers []string, attempts int, name string, qType dns.Type) (*dns.Msg, error) {
	// We don't support search domains, all names are assumed to be fully qualified already.
	msg := new(dns.Msg).SetQuestion(dns.Fqdn(name), uint16(qType))

	merr := multierror.New()
	// `man 5 resolv.conf` says that we should try each server, continuing to the next if
	// there is a timeout. We should repeat this process up to "attempt" times trying to get
	// a viable response.
	//
	// > (The algorithm used is to try a name server, and if the query times out, try the next,
	// > until out of name servers, then repeat trying all the name servers until a maximum number
	// > of retries are made.)
	for i := 0; i < attempts; i++ {
		for _, server := range servers {
			response, _, err := r.client.Exchange(ctx, msg, server)
			if err != nil {
				merr.Add(fmt.Errorf("resolution against server %s for %s: %w", server, name, err))
				continue
			}

			if response.Truncated {
				merr.Add(fmt.Errorf("resolution against server %s for %s: response truncated", server, name))
				continue
			}

			if response.Rcode == dns.RcodeSuccess || response.Rcode == dns.RcodeNameError {
				return response, nil
			}
		}
	}

	return nil, fmt.Errorf("could not resolve %s: no servers returned a viable answer. Errs %s", name, merr.Err())
}

// loadConfiguration parses and returns a resolv.conf configuration and builds a list of
// nameservers of the form "ip:port" for convenience. Returns an error if the file cannot
// be loaded or is not syntactically valid.
func (r *Resolver) loadConfiguration() ([]string, *dns.ClientConfig, error) {
	conf, err := dns.ClientConfigFromFile(r.confPath)
	if err != nil {
		return nil, nil, fmt.Errorf("could not load resolv.conf: %w", err)
	}

	servers := make([]string, len(conf.Servers))
	for i, nameserver := range conf.Servers {
		servers[i] = net.JoinHostPort(nameserver, conf.Port)
	}

	return servers, conf, nil
}

// PoolingClient is a DNS client that pools TCP connections to each nameserver.
type PoolingClient struct {
	network string
	maxOpen int

	mtx   sync.Mutex
	pools map[string]*Pool
}

// NewPoolingClient creates a new PoolingClient instance that keeps up to maxOpen connections to each nameserver.
func NewPoolingClient(maxOpen int) *PoolingClient {
	return &PoolingClient{
		network: "tcp",
		maxOpen: maxOpen,
		pools:   make(map[string]*Pool),
	}
}

// Exchange sends the DNS msg to the nameserver using a pooled connection. The nameserver must be
// of the form "ip:port".
func (c *PoolingClient) Exchange(ctx context.Context, msg *dns.Msg, server string) (*dns.Msg, time.Duration, error) {
	pool := c.getPool(server)
	conn, err := pool.Get(ctx, c.network, server)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to create connection to %s via %s: %w", server, c.network, err)
	}

	connOk := true
	defer func() {
		if connOk {
			_ = pool.Put(conn)
		} else {
			pool.Discard(conn)
		}
	}()

	client := &dns.Client{Net: c.network}
	response, rtt, err := client.ExchangeWithConnContext(ctx, msg, conn)
	if err != nil {
		connOk = false
	}

	return response, rtt, err
}

// Clean closes connections to any nameservers that are _not_ part of list of known
// nameservers. The nameservers must be of the form "ip:port", the same format as the
// Exchange method.
func (c *PoolingClient) Clean(known []string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for server, pool := range c.pools {
		if slices.Contains(known, server) {
			continue
		}

		pool.Close()
		delete(c.pools, server)
	}
}

func (c *PoolingClient) getPool(server string) *Pool {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	p, ok := c.pools[server]
	if !ok {
		p = NewPool(c.maxOpen)
		c.pools[server] = p
	}

	return p
}

// Pool is a pool of DNS connections for a single DNS server.
type Pool struct {
	mtx    sync.RWMutex
	conns  chan *dns.Conn
	closed bool
}

// NewPool creates a new DNS connection Pool, keeping up to maxConns open.
func NewPool(maxConns int) *Pool {
	return &Pool{
		conns: make(chan *dns.Conn, maxConns),
	}
}

// Get gets an existing connection from the pool or creates a new one if there are no
// pooled connections available. If the pool has been closed, an error is returned.
func (p *Pool) Get(ctx context.Context, network string, server string) (*dns.Conn, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	if p.closed {
		return nil, fmt.Errorf("connection pool for %s %s is closed", network, server)
	}

	select {
	case conn := <-p.conns:
		return conn, nil
	default:
		return p.newConn(ctx, network, server)
	}
}

// Put returns a healthy connection to the pool, potentially closing it if the pool is
// already at capacity. If the pool has been closed, the connection will be closed immediately.
func (p *Pool) Put(conn *dns.Conn) error {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	if p.closed {
		return conn.Close()
	}

	select {
	case p.conns <- conn:
		return nil
	default:
		return conn.Close()
	}
}

// Discard closes and does not return the given broken connection to the pool.
func (p *Pool) Discard(conn *dns.Conn) {
	_ = conn.Close()
}

// Close shuts down this pool, closing all existing connections and preventing new connections
// from being created. Any attempts to get a connection from this pool after it is closed will
// result in an error.
func (p *Pool) Close() {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.closed = true
	for {
		select {
		case c := <-p.conns:
			_ = c.Close()
		default:
			return
		}
	}
}

func (p *Pool) newConn(ctx context.Context, network string, server string) (*dns.Conn, error) {
	client := &dns.Client{Net: network}
	return client.DialContext(ctx, server)
}
