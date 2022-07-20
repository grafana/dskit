package ring

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/netip"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/grafana/dskit/backoff"
)

// GenerateTokens make numTokens unique random tokens, none of which clash
// with takenTokens. Generated tokens are sorted.
func GenerateTokens(numTokens int, takenTokens []uint32) []uint32 {
	if numTokens <= 0 {
		return []uint32{}
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	used := make(map[uint32]bool, len(takenTokens))
	for _, v := range takenTokens {
		used[v] = true
	}

	tokens := make([]uint32, 0, numTokens)
	for i := 0; i < numTokens; {
		candidate := r.Uint32()
		if used[candidate] {
			continue
		}
		used[candidate] = true
		tokens = append(tokens, candidate)
		i++
	}

	// Ensure returned tokens are sorted.
	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i] < tokens[j]
	})

	return tokens
}

// GetInstanceAddr returns the address to use to register the instance
// in the ring.
func GetInstanceAddr(configAddr string, netInterfaces []string, logger log.Logger) (string, error) {
	if configAddr != "" {
		return configAddr, nil
	}

	addr, err := GetFirstAddressOf(netInterfaces, logger)
	if err != nil {
		return "", err
	}

	return addr, nil
}

// GetInstancePort returns the port to use to register the instance
// in the ring.
func GetInstancePort(configPort, listenPort int) int {
	if configPort > 0 {
		return configPort
	}

	return listenPort
}

// WaitInstanceState waits until the input instanceID is registered within the
// ring matching the provided state. A timeout should be provided within the context.
func WaitInstanceState(ctx context.Context, r ReadRing, instanceID string, state InstanceState) error {
	backoff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 0,
	})

	for backoff.Ongoing() {
		if actualState, err := r.GetInstanceState(instanceID); err == nil && actualState == state {
			return nil
		}

		backoff.Wait()
	}

	return backoff.Err()
}

// WaitRingStability monitors the ring topology for the provided operation and waits until it
// keeps stable for at least minStability.
func WaitRingStability(ctx context.Context, r ReadRing, op Operation, minStability, maxWaiting time.Duration) error {
	return waitStability(ctx, r, op, minStability, maxWaiting, HasReplicationSetChanged)
}

// WaitRingTokensStability waits for the Ring to be unchanged at
// least for minStability time period, excluding transitioning between
// allowed states (e.g. JOINING->ACTIVE if allowed by op).
// This can be used to avoid wasting resources on moving data around
// due to multiple changes in the Ring.
func WaitRingTokensStability(ctx context.Context, r ReadRing, op Operation, minStability, maxWaiting time.Duration) error {
	return waitStability(ctx, r, op, minStability, maxWaiting, HasReplicationSetChangedWithoutState)
}

func waitStability(ctx context.Context, r ReadRing, op Operation, minStability, maxWaiting time.Duration, isChanged func(ReplicationSet, ReplicationSet) bool) error {
	// Configure the max waiting time as a context deadline.
	ctx, cancel := context.WithTimeout(ctx, maxWaiting)
	defer cancel()

	// Get the initial ring state.
	ringLastState, _ := r.GetAllHealthy(op) // nolint:errcheck
	ringLastStateTs := time.Now()

	const pollingFrequency = time.Second
	pollingTicker := time.NewTicker(pollingFrequency)
	defer pollingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pollingTicker.C:
			// We ignore the error because in case of error it will return an empty
			// replication set which we use to compare with the previous state.
			currRingState, _ := r.GetAllHealthy(op) // nolint:errcheck

			if isChanged(ringLastState, currRingState) {
				ringLastState = currRingState
				ringLastStateTs = time.Now()
			} else if time.Since(ringLastStateTs) >= minStability {
				return nil
			}
		}
	}
}

// MakeBuffersForGet returns buffers to use with Ring.Get().
func MakeBuffersForGet() (bufDescs []InstanceDesc, bufHosts, bufZones []string) {
	bufDescs = make([]InstanceDesc, 0, GetBufferSize)
	bufHosts = make([]string, 0, GetBufferSize)
	bufZones = make([]string, 0, GetBufferSize)
	return
}

// getZones return the list zones from the provided tokens. The returned list
// is guaranteed to be sorted.
func getZones(tokens map[string][]uint32) []string {
	var zones []string

	for zone := range tokens {
		zones = append(zones, zone)
	}

	sort.Strings(zones)
	return zones
}

// searchToken returns the offset of the tokens entry holding the range for the provided key.
func searchToken(tokens []uint32, key uint32) int {
	i := sort.Search(len(tokens), func(x int) bool {
		return tokens[x] > key
	})
	if i >= len(tokens) {
		i = 0
	}
	return i
}

// GetFirstAddressOf returns the first IPv4/IPV6 address of the supplied interface names, omitting any link-local addresses.
func GetFirstAddressOf(names []string, logger log.Logger) (string, error) {
	return getFirstAddressOf(names, logger, getInterfaceAddresses)
}

// NetworkInterfaceGetter matches the signature of net.InterfaceByName() to allow for test mocks.
type NetworkInterfaceAddressGetter func(name string) ([]netip.Addr, error)

// GetFirstAddressOf returns the first IPv4/IPV6 address of the supplied interface names, omitting any link-local addresses.
func getFirstAddressOf(names []string, logger log.Logger, interfaceAddrs NetworkInterfaceAddressGetter) (string, error) {
	var ipAddr netip.Addr
	for _, name := range names {
		addrs, err := interfaceAddrs(name)
		if err != nil {
			level.Warn(logger).Log("msg", "error getting addresses for interface", "inf", name, "err", err)
			continue
		}
		if len(addrs) <= 0 {
			level.Warn(logger).Log("msg", "no addresses found for interface", "inf", name, "err", err)
			continue
		}
		if ip := filterIPs(addrs); ip.IsValid() {
			ipAddr = ip
		}
		if ipAddr.IsLinkLocalUnicast() || !ipAddr.IsValid() {
			continue
		}
		return ipAddr.String(), nil
	}
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
		netaddrs[i], err = netip.ParseAddr(a.String())
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse netip.Addr")
		}
	}

	return netaddrs, nil
}

// filterIPs attempts to return the first non automatic private IP (APIPA / 169.254.x.x) if possible, only returning APIPA if available and no other valid IP is found.
func filterIPs(addrs []netip.Addr) netip.Addr {
	var ipAddr netip.Addr
	for _, addr := range addrs {
		if addr.String() != "" {
			ipAddr = addr
		}
		if !addr.IsLinkLocalUnicast() {
			return addr
		}
	}
	return ipAddr
}
