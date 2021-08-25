package memberlist

import (
	"context"
)

// DNSProvider supports storing or resolving a list of addresses.
type DNSProvider interface {
	// Resolve stores a list of provided addresses or their DNS records if requested.
	Resolve(ctx context.Context, addrs []string) error

	// Addresses returns the latest addresses present in the DNSProvider.
	Addresses() []string
}
