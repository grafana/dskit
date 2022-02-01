package netutil

import (
	"net"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
)

// Setup required logger and example interface names and addresses
var (
	logger        log.Logger = log.NewLogfmtLogger(os.Stdout)
	testIntsAddrs            = map[string]string{
		"privNetA": "10.6.19.34/8",
		"privNetB": "172.16.0.7/12",
		"privNetC": "192.168.3.29/24",
		"pubNet":   "34.120.177.193/24",
	}
)

// A type that implements the net.Addr interface
// Only String() is called by netutil logic
type MockAddr struct {
	netAddr string
}

func (ma MockAddr) Network() string {
	return "tcp"
}

func (ma MockAddr) String() string {
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

func TestEmptyInterface(t *testing.T) {
	ints := []net.Interface{}
	getInterfaceAddrs = func(i *net.Interface) ([]net.Addr, error) {
		return []net.Addr{}, nil
	}
	privInts := privateNetworkInterfaces(ints, logger)
	assert.Equal(t, privInts, []string{"eth0", "en0"})
}

func TestSinglePrivateInterface(t *testing.T) {
	ifname := "privNetA"
	ints := []net.Interface{{
		Index:        1,
		MTU:          1500,
		Name:         ifname,
		HardwareAddr: []byte{},
		Flags:        0,
	}}
	getInterfaceAddrs = func(i *net.Interface) ([]net.Addr, error) {
		return []net.Addr{MockAddr{netAddr: testIntsAddrs[ifname]}}, nil
	}
	privInts := privateNetworkInterfaces(ints, logger)
	assert.Equal(t, privInts, []string{ifname})
}

func TestSinglePublicInterface(t *testing.T) {
	ifname := "pubNet"
	ints := []net.Interface{{
		Index:        1,
		MTU:          1500,
		Name:         ifname,
		HardwareAddr: []byte{},
		Flags:        0,
	}}
	getInterfaceAddrs = func(i *net.Interface) ([]net.Addr, error) {
		return []net.Addr{MockAddr{netAddr: testIntsAddrs[ifname]}}, nil
	}
	privInts := privateNetworkInterfaces(ints, logger)
	assert.Equal(t, privInts, []string{"eth0", "en0"})
}

func TestListAllPrivate(t *testing.T) {
	intNames := []string{"privNetA", "privNetB", "privNetC"}
	ints := generateTestInterfaces(intNames)
	getInterfaceAddrs = func(i *net.Interface) ([]net.Addr, error) {
		return []net.Addr{
			MockAddr{netAddr: testIntsAddrs[i.Name]},
		}, nil
	}
	privInts := privateNetworkInterfaces(ints, logger)
	assert.Equal(t, privInts, intNames)
}

func TestMixPrivatePublic(t *testing.T) {
	intNames := []string{"pubNet", "privNetA", "privNetB", "privNetC"}
	ints := generateTestInterfaces(intNames)
	getInterfaceAddrs = func(i *net.Interface) ([]net.Addr, error) {
		return []net.Addr{
			MockAddr{netAddr: testIntsAddrs[i.Name]},
		}, nil
	}
	privInts := privateNetworkInterfaces(ints, logger)
	assert.Equal(t, privInts, []string{"privNetA", "privNetB", "privNetC"})
}
