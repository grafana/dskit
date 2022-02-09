package netutil

import (
	"net"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
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
	return privateNetworkInterfaces(ints, logger)
}

// private testable function that checks each given interface
func privateNetworkInterfaces(all []net.Interface, logger log.Logger) []string {
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
				level.Warn(logger).Log("msg", "error parsing network interface IP address", "inf", i.Name, "addr", s, "err", err)
			}
			if ip.IsPrivate() {
				privInts = append(privInts, i.Name)
				break
			}
		}
	}
	if len(privInts) == 0 {
		return []string{"eth0", "en0"}
	}
	level.Debug(logger).Log("msg", "found network interfaces with private IP addresses assigned", "interfaces", privInts)
	return privInts
}
