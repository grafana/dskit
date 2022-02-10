package netutil

import (
	"net"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
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
