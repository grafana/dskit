package memberlist

import (
	"context"
)

// DNSProvider supports storing or resolving a list of addresses.
type DNSProvider interface {
	// Resolve stores a list of provided addresses or their DNS records if requested.
	// Addresses prefixed with `dns+` or `dnssrv+` will be resolved through respective DNS lookup (A/AAAA or SRV).
	// For non-SRV records, it will return an error if a port is not supplied.
	//
	// Example: By providing the address of "dns+grafana.com:80", it should resolve into something like []string{"34.120.177.193:80", "[2600:1901:0:b3ea::]:80"}.
	Resolve(ctx context.Context, addrs []string) error

	// Addresses returns the latest addresses present in the DNSProvider.
	Addresses() []string
}
