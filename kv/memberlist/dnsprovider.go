package memberlist

import (
	"context"
	"net"
	"sync"
)

// DNSProvider supports storing or resolving a list of addresses.
type DNSProvider interface {
	// Resolve stores a list of provided addresses or their DNS records if requested.
	// Implementations may have specific ways of interpreting addresses.
	Resolve(ctx context.Context, addrs []string) error

	// Addresses returns the latest addresses present in the DNSProvider.
	Addresses() []string
}

type dnsProvider struct {
	sync.Mutex
	addr []string
}

func NewDNSProvider() DNSProvider { return &dnsProvider{} }

func (d *dnsProvider) Resolve(ctx context.Context, addrs []string) error {
	d.Lock()
	defer d.Unlock()
	for _, a := range addrs {
		ips, err := net.LookupIP(a)
		if err != nil {
			return err
		}
		for _, ip := range ips {
			d.addr = append(d.addr, ip.String())
		}
	}
	return nil
}

func (d *dnsProvider) Addresses() []string {
	d.Lock()
	defer d.Unlock()
	return d.addr
}
