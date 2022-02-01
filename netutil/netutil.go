package netutil

import (
	"net"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

var (
	getInterfaceAddrs = (*net.Interface).Addrs
)

// Parses network interfaces and returns those having an address conformant to RFC1918
func PrivateNetworkInterfaces(logger log.Logger) []string {
	ints, err := net.Interfaces()
	if err != nil {
		level.Warn(logger).Log("msg", "error getting interfaces", "err", err)
	}
	return privateNetworkInterfaces(ints, logger)
}

// private testable function that checks each given interface
func privateNetworkInterfaces(all []net.Interface, logger log.Logger) []string {
	var privInts []string
	for _, i := range all {
		addrs, err := getInterfaceAddrs(&i)
		if err != nil {
			level.Warn(logger).Log("msg", "error getting addresses", "inf", i.Name, "err", err)
		}
		for _, a := range addrs {
			s := a.String()
			ip, _, err := net.ParseCIDR(s)
			if err != nil {
				level.Warn(logger).Log("msg", "error getting ip address", "inf", i.Name, "addr", s, "err", err)
			}
			if ip.IsPrivate() {
				level.Info(logger).Log(i.Name, "is private with address", s)
				privInts = append(privInts, i.Name)
				break
			}
		}
	}
	if len(privInts) == 0 {
		return []string{"eth0", "en0"}
	}
	level.Info(logger).Log("msg", "found interfaces on private networks:", "["+strings.Join(privInts, ", ")+"]")
	return privInts
}
