package netutil

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A type that implements the net.Addr interface
// Only String() is called by netutil logic
type mockAddr struct {
	netAddr string
}

func (ma mockAddr) Network() string {
	return "tcp"
}

func (ma mockAddr) String() string {
	return ma.netAddr
}

// Helper function to test a list of interfaces
func generateTestInterfaces(names []string) []net.Interface {
	testInts := []net.Interface{}
	for i, j := range names {
		k := net.Interface{
			Index:        i + 1,
			MTU:          1500,
			Name:         j,
			HardwareAddr: []byte{},
			Flags:        0,
		}
		testInts = append(testInts, k)
	}
	return testInts
}

func TestPrivateInterface(t *testing.T) {
	testIntsAddrs := map[string][]string{
		"privNetA":  {"10.6.19.34/8"},
		"privNetB":  {"172.16.0.7/12"},
		"privNetC":  {"192.168.3.29/24"},
		"pubNet":    {"34.120.177.193/24"},
		"multiPriv": {"10.6.19.34/8", "172.16.0.7/12"},
		"multiMix":  {"1.1.1.1/24", "192.168.0.42/24"},
		"multiPub":  {"1.1.1.1/24", "34.120.177.193/24"},
	}
	defaultOutput := []string{"eth0", "en0"}
	type testCases struct {
		description    string
		interfaces     []string
		expectedOutput []string
	}
	for _, scenario := range []testCases{
		{
			description:    "empty interface list",
			interfaces:     []string{},
			expectedOutput: defaultOutput,
		},
		{
			description:    "single private interface",
			interfaces:     []string{"privNetA"},
			expectedOutput: []string{"privNetA"},
		},
		{
			description:    "single public interface",
			interfaces:     []string{"pubNet"},
			expectedOutput: defaultOutput,
		},
		{
			description:    "single interface multi address private",
			interfaces:     []string{"multiPriv"},
			expectedOutput: []string{"multiPriv"},
		},
		{
			description:    "single interface multi address mix",
			interfaces:     []string{"multiMix"},
			expectedOutput: []string{"multiMix"},
		},
		{
			description:    "single interface multi address public",
			interfaces:     []string{"multiPub"},
			expectedOutput: defaultOutput,
		},
		{
			description:    "all private interfaces",
			interfaces:     []string{"privNetA", "privNetB", "privNetC"},
			expectedOutput: []string{"privNetA", "privNetB", "privNetC"},
		},
		{
			description:    "mix of public and private interfaces",
			interfaces:     []string{"pubNet", "privNetA", "privNetB", "privNetC", "multiPriv", "multiMix", "multiPub"},
			expectedOutput: []string{"privNetA", "privNetB", "privNetC", "multiPriv", "multiMix"},
		},
	} {
		getInterfaceAddrs = func(i *net.Interface) ([]net.Addr, error) {
			addrs := []net.Addr{}
			for _, ip := range testIntsAddrs[i.Name] {
				addrs = append(addrs, mockAddr{netAddr: ip})
			}
			return addrs, nil
		}
		t.Run(scenario.description, func(t *testing.T) {
			privInts := privateNetworkInterfaces(
				generateTestInterfaces(scenario.interfaces),
				defaultOutput,
				log.NewNopLogger(),
			)
			assert.Equal(t, privInts, scenario.expectedOutput)
		})
	}
}

func TestPrivateInterfaceError(t *testing.T) {
	interfaces := generateTestInterfaces([]string{"eth9"})
	ipaddr := "not_a_parseable_ip_string"
	getInterfaceAddrs = func(i *net.Interface) ([]net.Addr, error) {
		return []net.Addr{mockAddr{netAddr: ipaddr}}, nil
	}
	logger := log.NewLogfmtLogger(os.Stdout)
	privInts := privateNetworkInterfaces(interfaces, []string{}, logger)
	assert.Equal(t, privInts, []string{})
}

func getMockInterfaceAddresses(name string) ([]netip.Addr, error) {
	toAddr := func(addr string) netip.Addr {
		prefix := netip.MustParsePrefix(addr)
		return prefix.Addr()
	}

	interfaces := map[string][]netip.Addr{
		"wlan0": {
			toAddr("172.16.16.51/24"),
			toAddr("fc16::c9a7:1d89:2dd8:f59a/64"),
			toAddr("fe80::ec62:ed39:f931:bf6d/64"),
		},
		"em0": {
			toAddr("fe80::ec62:ed39:f931:bf6d/64"),
			toAddr("fc99::9a/64"),
		},
		"em1": {
			toAddr("10.54.99.53/24"),
			toAddr("fcca::e6:9274:f580:7a1b:c335/64"),
			toAddr("fe80::38f8:7eff:fe7c:6e5d/64"),
		},
		"em2": {
			toAddr("10.54.99.53/24"),
			toAddr("fe80::38f8:7eff:fe7c:6e56/64"),
			toAddr("fe80::38f8:7eff:fe7c:6e5d/64"),
		},
		"lo1": {
			toAddr("fe80::ec62:ed39:f931:bf6d/64"),
		},
		"lo2": {
			toAddr("169.254.1.1/16"),
		},
		"lo9":  {},
		"lo10": {},
		"enp0s31f6": {
			toAddr("1.1.1.1/31"),
		},
		"enp0s31f7": {
			toAddr("2001::1111/120"),
		},
	}

	if val, ok := interfaces[name]; ok {
		return val, nil
	}

	return nil, fmt.Errorf("no such network interface")
}

func TestGetFirstAddressOf(t *testing.T) {
	// logger := log.NewNopLogger()
	logger := log.NewLogfmtLogger(os.Stdout)

	cases := []struct {
		names       []string
		addr        string
		err         error
		enableInet6 bool
	}{
		{
			names: []string{"wlan0"},
			addr:  "172.16.16.51",
		},
		{
			names: []string{"em0"},
			addr:  "fc99::9a",
		},
		{
			names: []string{"lo1"},
			addr:  "fe80::ec62:ed39:f931:bf6d",
		},
		{
			names: []string{"lo2"},
			addr:  "169.254.1.1",
		},
		{
			names: []string{"lo9"},
			err:   fmt.Errorf("no useable address found for interfaces [lo9]"),
		},
		{
			names: []string{"lo9", "lo10"},
			err:   fmt.Errorf("no useable address found for interfaces [lo9 lo10]"),
		},
		{
			names: []string{"lo1", "lo2", "enp0s31f6"},
			addr:  "1.1.1.1",
		},
		{
			names: []string{"lo1", "lo2", "enp0s31f7"},
			addr:  "2001::1111",
		},
		{
			names: []string{"lo1", "lo2", "enp0s31f7", "enp0s31f6"},
			addr:  "2001::1111",
		},
		{
			names: []string{"lo1", "lo2", "enp0s31f6", "enp0s31f7"},
			addr:  "1.1.1.1",
		},
		{
			names:       []string{"lo1", "lo2", "enp0s31f6", "enp0s31f7"},
			addr:        "2001::1111",
			enableInet6: true,
		},
		{
			names:       []string{"em0", "em1"},
			addr:        "fc99::9a",
			enableInet6: true,
		},
		{
			names:       []string{"em1"},
			addr:        "fcca::e6:9274:f580:7a1b:c335",
			enableInet6: true,
		},
		{
			names:       []string{"lo1", "enp0s31f6"},
			addr:        "1.1.1.1",
			enableInet6: true,
		},
		{
			names:       []string{"lo2", "enp0s31f6"},
			addr:        "1.1.1.1",
			enableInet6: true,
		},
		{
			names:       []string{"em2"},
			addr:        "10.54.99.53",
			enableInet6: true,
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s", tc.names), func(t *testing.T) {
			addr, err := getFirstAddressOf(tc.names, logger, getMockInterfaceAddresses, tc.enableInet6)
			if tc.err != nil {
				require.Equal(t, tc.err, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.addr, addr)
		})
	}
}

func TestFilterBestIP(t *testing.T) {
	// logger := log.NewLogfmtLogger(os.Stdout)
	toAddr := func(addr string) netip.Addr {
		a, err := netip.ParseAddr(addr)
		require.NoError(t, err)
		return a
	}

	cases := []struct {
		addrs       []netip.Addr
		addr        string
		enableInet6 bool
	}{
		{
			addrs: []netip.Addr{
				toAddr("169.254.1.1"),
				toAddr("127.0.0.1"),
			},
			addr: "169.254.1.1",
		},
		{
			addrs: []netip.Addr{
				toAddr("1.1.1.1"),
			},
			addr: "1.1.1.1",
		},
		{
			addrs: []netip.Addr{
				toAddr("10.54.99.53"),
				toAddr("169.254.1.1"),
			},
			addr: "10.54.99.53",
		},
		{
			addrs: []netip.Addr{
				toAddr("169.254.1.1"),
				toAddr("fe80::1"),
			},
			addr: "169.254.1.1",
		},
		{
			addrs: []netip.Addr{
				toAddr("169.254.1.1"),
				toAddr("fe80::1"),
			},
			addr:        "fe80::1",
			enableInet6: true,
		},
		{
			addrs: []netip.Addr{
				toAddr("169.254.1.1"),
				toAddr("fe80::1"),
			},
			addr:        "fe80::1",
			enableInet6: true,
		},
		{
			addrs: []netip.Addr{
				toAddr("fc99::1"),
				toAddr("fe80::1"),
			},
			addr: "fc99::1",
		},
		{
			addrs: []netip.Addr{
				toAddr("169.254.1.1"),
				toAddr("10.54.99.53"),
				toAddr("fc99::1"),
				toAddr("fe80::1"),
			},
			addr:        "fc99::1",
			enableInet6: true,
		},
		{
			addrs: []netip.Addr{
				toAddr("169.254.1.1"),
				toAddr("10.54.99.53"),
				toAddr("fc99::1"),
				toAddr("fe80::1"),
			},
			addr: "10.54.99.53",
		},
	}

	for _, tc := range cases {
		addr := filterBestIP(tc.addrs, tc.enableInet6)
		require.Equal(t, tc.addr, addr.String())
	}

}
