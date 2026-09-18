package memberlist

import (
	"context"
	"testing"
)

func TestDNSProvider(t *testing.T) {
	dns := &dnsProvider{}
	if err := dns.Resolve(context.Background(), []string{"localhost"}); err != nil {
		t.Fatal(err)
	}
	has127_0_0_1 := false
	for _, addr := range dns.Addresses() {
		if addr == "127.0.0.1" {
			has127_0_0_1 = true
		}
	}
	if !has127_0_0_1 {
		t.Error("resolving localhost must result in 127.0.0.1 address", dns.Addresses())
	}
	if err := dns.Resolve(context.Background(), []string{"invalid dns"}); err == nil {
		t.Error("resolving and invalid address must result in an error")
	}
	if len(dns.Addresses()) == 0 {
		t.Error("DNSProvider must keep recent addresses on failure")
	}
}
