package netutil

import (
	"fmt"
	"net"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func ipForAddr(addr net.Addr) (net.IP, bool) {
	switch a := addr.(type) {
	case *net.IPAddr:
		return a.IP, true
	case *net.IPNet:
		return a.IP, true
	default:
		return net.IPv4zero, false
	}
}

func PrivateNetworkInterfaces() []string {
	ifaces := []string{}

	all, err := net.Interfaces()
	if err != nil {
		return ifaces
	}

IFACES:
	for _, iface := range all {
		if iface.Flags&net.FlagLoopback == 0 && iface.Flags&net.FlagUp != 0 {
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				ip, ok := ipForAddr(addr)
				if !ok {
					continue
				}
				if !IsPrivate(ip) {
					continue IFACES
				}
			}
			ifaces = append(ifaces, iface.Name)
		}
	}
	return ifaces
}

// FirstAddressOf returns the first IPv4 address of the supplied interface
// names, omitting any 169.254.x.x automatic private IPs if possible.
func FirstAddressOf(names []string, logger log.Logger) (string, error) {
	var ipAddr net.IP
	for _, name := range names {
		inf, err := net.InterfaceByName(name)
		if err != nil {
			level.Warn(logger).Log("msg", "error getting interface", "inf", name, "err", err)
			continue
		}
		addrs, err := inf.Addrs()
		if err != nil {
			level.Warn(logger).Log("msg", "error getting addresses for interface", "inf", name, "err", err)
			continue
		}
		if len(addrs) <= 0 {
			level.Warn(logger).Log("msg", "no addresses found for interface", "inf", name, "err", err)
			continue
		}
		if ip := filterIPs(addrs); !ip.IsUnspecified() {
			ipAddr = ip
		}
		if isAPIPA(ipAddr) || ipAddr.IsUnspecified() {
			continue
		}
		return ipAddr.String(), nil
	}
	if ipAddr.IsUnspecified() {
		return "", fmt.Errorf("no address found for %s", names)
	}
	if isAPIPA(ipAddr) {
		level.Warn(logger).Log("msg", "using automatic private ip", "address", ipAddr)
	}
	return ipAddr.String(), nil
}

func isAPIPA(ip4 net.IP) bool {
	return ip4[0] == 169 && ip4[1] == 254
}

// filterIPs attempts to return the first non automatic private IP (APIPA /
// 169.254.x.x) if possible, only returning APIPA if available and no other
// valid IP is found.
func filterIPs(addrs []net.Addr) net.IP {
	ipAddr := net.IPv4zero
	for _, addr := range addrs {
		if ip, ok := ipForAddr(addr); ok {
			if ip4 := ip.To4(); ip4 != nil {
				ipAddr = ip4
				if isAPIPA(ip4) {
					return ipAddr
				}
			}
		}
	}
	return ipAddr
}

// IsPrivate reports whether ip is a private address, according to
// RFC 1918 (IPv4 addresses) and RFC 4193 (IPv6 addresses).
// Copied form net package of the Go 1.17 stdlib.
// So, it can be removed once dskit is updated to Go 1.17
func IsPrivate(ip net.IP) bool {
	if ip4 := ip.To4(); ip4 != nil {
		// Following RFC 1918, Section 3. Private Address Space which says:
		//   The Internet Assigned Numbers Authority (IANA) has reserved the
		//   following three blocks of the IP address space for private internets:
		//     10.0.0.0        -   10.255.255.255  (10/8 prefix)
		//     172.16.0.0      -   172.31.255.255  (172.16/12 prefix)
		//     192.168.0.0     -   192.168.255.255 (192.168/16 prefix)
		return ip4[0] == 10 ||
			(ip4[0] == 172 && ip4[1]&0xf0 == 16) ||
			(ip4[0] == 192 && ip4[1] == 168)
	}
	// Following RFC 4193, Section 8. IANA Considerations which says:
	//   The IANA has assigned the FC00::/7 prefix to "Unique Local Unicast".
	return len(ip) == net.IPv6len && ip[0]&0xfe == 0xfc
}
