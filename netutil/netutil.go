package netutil

import (
	"fmt"
	"net"
	"net/netip"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
)

var (
	getInterfaceAddrs = (*net.Interface).Addrs
)

// PrivateNetworkInterfaces lists network interfaces and returns those having an address conformant to RFC1918
func PrivateNetworkInterfaces(logger log.Logger) []string {
	ints, err := net.Interfaces()
	if err != nil {
		level.Warn(logger).Log("msg", "error getting network interfaces", "err", err)
	}
	return privateNetworkInterfaces(ints, []string{}, logger)
}

func PrivateNetworkInterfacesWithFallback(fallback []string, logger log.Logger) []string {
	ints, err := net.Interfaces()
	if err != nil {
		level.Warn(logger).Log("msg", "error getting network interfaces", "err", err)
	}
	return privateNetworkInterfaces(ints, fallback, logger)
}

// private testable function that checks each given interface
func privateNetworkInterfaces(all []net.Interface, fallback []string, logger log.Logger) []string {
	var privInts []string
	for _, i := range all {
		addrs, err := getInterfaceAddrs(&i)
		if err != nil {
			level.Warn(logger).Log("msg", "error getting addresses from network interface", "interface", i.Name, "err", err)
		}
		for _, a := range addrs {
			s := a.String()
			ip, _, err := net.ParseCIDR(s)
			if err != nil {
				level.Warn(logger).Log("msg", "error parsing network interface IP address", "interface", i.Name, "addr", s, "err", err)
				continue
			}
			if ip.IsPrivate() {
				privInts = append(privInts, i.Name)
				break
			}
		}
	}
	if len(privInts) == 0 {
		return fallback
	}
	return privInts
}

// GetFirstAddressOf returns the first IPv4/IPV6 address of the supplied interface names, omitting any link-local addresses.
func GetFirstAddressOf(names []string, logger log.Logger) (string, error) {
	return getFirstAddressOf(names, logger, getInterfaceAddresses, false)
}

// GetPrivateInet6Address returns the first IPv6 address found on any interface, omitting link-local addresses.
func GetPrivateInet6Address(logger log.Logger) (string, error) {
	return getFirstInet6AddressOf([]string{}, logger, getInterfaceAddresses)
}

func getFirstInet6AddressOf(names []string, logger log.Logger, interfaceAddrs NetworkInterfaceAddressGetter) (string, error) {
	addr, err := getFirstAddressOf(names, logger, interfaceAddrs, true)
	if err != nil {
		return "", fmt.Errorf("failed to get valid address: %w", err)
	}

	a, err := netip.ParseAddr(addr)
	if err != nil {
		return "", fmt.Errorf("faild to parse address: %w", err)
	}

	if !a.Is6() || !a.IsValid() {
		return "", fmt.Errorf("no inet6 address available")
	}

	return addr, nil
}

// NetworkInterfaceGetter matches the signature of net.InterfaceByName() to allow for test mocks.
type NetworkInterfaceAddressGetter func(name string) ([]netip.Addr, error)

// getFirstAddressOf returns the first IPv4/IPV6 address of the supplied interface names, omitting any link-local addresses.
func getFirstAddressOf(names []string, logger log.Logger, interfaceAddrsFunc NetworkInterfaceAddressGetter, preferInet6 bool) (string, error) {
	var ipAddr netip.Addr

	// When passing an empty list of interface names, we select all interfaces.
	if len(names) == 0 {
		infs, err := net.Interfaces()
		if err != nil {
			return "", fmt.Errorf("failed to get interface list and no interface names supplied: %w", err)
		}
		ifNames := make([]string, len(infs))
		for i, v := range infs {
			ifNames[i] = v.Name
		}
	}

	// Replace a nil func with the standard approach.
	if interfaceAddrsFunc == nil {
		interfaceAddrsFunc = getInterfaceAddresses
	}

	for _, name := range names {
		addrs, err := interfaceAddrsFunc(name)
		if err != nil {
			level.Warn(logger).Log("msg", "error getting addresses for interface", "inf", name, "err", err)
			continue
		}
		level.Debug(logger).Log("msg", "addresses for interface", "addrs", fmt.Sprintf("%+v", addrs), "inf", name, "inet6pref", preferInet6)

		if len(addrs) <= 0 {
			level.Warn(logger).Log("msg", "no addresses found for interface", "inf", name, "err", err)
			continue
		}
		if ip := filterIPs(addrs, preferInet6); ip.IsValid() {
			ipAddr = ip
		}

		level.Debug(logger).Log("msg", "filtered", "ipAddr", ipAddr.String(), "inf", name)

		if ipAddr.IsLinkLocalUnicast() || !ipAddr.IsValid() {
			continue
		}
		if preferInet6 && !ipAddr.Is6() {
			continue
		}
		return ipAddr.String(), nil
	}
	level.Debug(logger).Log("msg", "ipAddr", "addrs", ipAddr)
	if !ipAddr.IsValid() {
		return "", fmt.Errorf("no useable address found for interfaces %s", names)
	}
	if ipAddr.IsLinkLocalUnicast() {
		level.Warn(logger).Log("msg", "using link-local address", "address", ipAddr.String())
	}
	return ipAddr.String(), nil
}

// getInterfaceAddresses is the standard approach to collecting []net.Addr from a network interface by name.
func getInterfaceAddresses(name string) ([]netip.Addr, error) {
	inf, err := net.InterfaceByName(name)
	if err != nil {
		return nil, err
	}

	addrs, err := inf.Addrs()
	if err != nil {
		return nil, err
	}

	// Using netip.Addr to allow for easier and consistent address parsing.
	// Without this, the net.ParseCIDR() that we might like to use in a test does
	// not have the same net.Addr implementation that we get from calling
	// interface.Addrs() as above.  Here we normalize on netip.Addr.
	netaddrs := make([]netip.Addr, len(addrs))
	for i, a := range addrs {
		prefix, err := netip.ParsePrefix(a.String())
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse netip.Prefix")
		}
		netaddrs[i] = prefix.Addr()
	}

	return netaddrs, nil
}

// filterIPs attempts to return the first non automatic private IP (APIPA / 169.254.x.x / link-local) if possible, only returning APIPA if available and no other valid IP is found.
func filterIPs(addrs []netip.Addr, preferInet6 bool) netip.Addr {
	var ipAddr netip.Addr
	for _, addr := range addrs {
		if addr.IsValid() {
			ipAddr = addr
		}

		if preferInet6 && !addr.Is6() {
			continue
		}

		if !addr.IsLinkLocalUnicast() {
			return addr
		}
	}
	return ipAddr
}
