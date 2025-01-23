package memberlist

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/services"
)

const ACTIVE = 1
const JOINING = 2
const LEFT = 3

// Simple mergeable data structure, used for gossiping
type member struct {
	Timestamp int64
	Tokens    []uint32
	State     int
}

type data struct {
	Members map[string]member
}

func (d *data) Merge(mergeable Mergeable, localCAS bool) (Mergeable, error) {
	if mergeable == nil {
		return nil, nil
	}

	od, ok := mergeable.(*data)
	if !ok || od == nil {
		return nil, fmt.Errorf("invalid thing to merge: %T", od)
	}

	updated := map[string]member{}

	for k, v := range od.Members {
		if v.Timestamp > d.Members[k].Timestamp {
			d.Members[k] = v
			updated[k] = v
		}
	}

	if localCAS {
		for k, v := range d.Members {
			if _, ok := od.Members[k]; !ok && v.State != LEFT {
				v.State = LEFT
				v.Tokens = nil
				d.Members[k] = v
				updated[k] = v
			}
		}
	}

	if len(updated) == 0 {
		return nil, nil
	}
	return &data{Members: updated}, nil
}

func (d *data) MergeContent() []string {
	// return list of keys
	out := []string(nil)
	for k := range d.Members {
		out = append(out, k)
	}
	return out
}

// This method deliberately ignores zero limit, so that tests can observe LEFT state as well.
func (d *data) RemoveTombstones(limit time.Time) (total, removed int) {
	for n, m := range d.Members {
		if m.State == LEFT {
			if time.Unix(m.Timestamp, 0).Before(limit) {
				// remove it
				delete(d.Members, n)
				removed++
			} else {
				total++
			}
		}
	}
	return
}

func (m member) clone() member {
	out := member{
		Timestamp: m.Timestamp,
		Tokens:    make([]uint32, len(m.Tokens)),
		State:     m.State,
	}
	copy(out.Tokens, m.Tokens)
	return out
}

func (d *data) Clone() Mergeable {
	out := &data{
		Members: make(map[string]member, len(d.Members)),
	}
	for k, v := range d.Members {
		out.Members[k] = v.clone()
	}
	return out
}

func (d *data) getAllTokens() []uint32 {
	out := []uint32(nil)
	for _, m := range d.Members {
		out = append(out, m.Tokens...)
	}

	sort.Sort(sortableUint32(out))
	return out
}

type dataCodec struct{}

func (d dataCodec) CodecID() string {
	return "testDataCodec"
}

func (d dataCodec) Decode(b []byte) (interface{}, error) {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	out := &data{}
	err := dec.Decode(out)
	return out, err
}

func (d dataCodec) Encode(val interface{}) ([]byte, error) {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
	return buf.Bytes(), err
}

var _ codec.Codec = &dataCodec{}

type sortableUint32 []uint32

func (ts sortableUint32) Len() int           { return len(ts) }
func (ts sortableUint32) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts sortableUint32) Less(i, j int) bool { return ts[i] < ts[j] }

const key = "test"

func updateFn(name string) func(*data) (*data, bool, error) {
	return func(in *data) (out *data, retry bool, err error) {
		// Modify value that was passed as a parameter.
		// Client takes care of concurrent modifications.
		r := in
		if r == nil {
			r = &data{Members: map[string]member{}}
		}

		m, ok := r.Members[name]
		if !ok {
			r.Members[name] = member{
				Timestamp: time.Now().Unix(),
				Tokens:    generateTokens(128),
				State:     JOINING,
			}
		} else {
			// We need to update timestamp, otherwise CAS will fail
			m.Timestamp = time.Now().Unix()
			m.State = ACTIVE
			r.Members[name] = m
		}

		return r, true, nil
	}
}

func get(t *testing.T, kv *Client, key string) interface{} {
	val, err := kv.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Failed to get value for key %s: %v", key, err)
	}
	return val
}

func getData(t *testing.T, kv *Client, key string) *data {
	t.Helper()
	val := get(t, kv, key)
	if val == nil {
		return nil
	}
	if r, ok := val.(*data); ok {
		return r
	}
	t.Fatalf("Expected ring, got: %T", val)
	return nil
}

func cas(kv *Client, key string, updateFn func(*data) (*data, bool, error)) error {
	return casWithErr(context.Background(), kv, key, updateFn)
}

func casWithErr(ctx context.Context, kv *Client, key string, updateFn func(*data) (*data, bool, error)) error {
	fn := func(in interface{}) (out interface{}, retry bool, err error) {
		var r *data
		if in != nil {
			r = in.(*data)
		}

		d, rt, e := updateFn(r)
		if d == nil {
			// translate nil pointer to nil interface value
			return nil, rt, e
		}
		return d, rt, e
	}

	return kv.CAS(ctx, key, fn)
}

func getLocalhostAddr() string {
	return getLocalhostAddrs()[0]
}

var addrsOnce sync.Once
var localhostIP string

func getLocalhostAddrs() []string {
	addrsOnce.Do(func() {
		ip, err := net.ResolveIPAddr("ip4", "localhost")
		if err != nil {
			localhostIP = "127.0.0.1" // this is the most common answer, try it
		}
		localhostIP = ip.String()
	})
	return []string{localhostIP}
}

func TestBasicGetAndCas(t *testing.T) {
	c := dataCodec{}

	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	cfg.TCPTransport = TCPTransportConfig{
		BindAddrs: getLocalhostAddrs(),
	}
	cfg.Codecs = []codec.Codec{c}

	mkv := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv))
	defer services.StopAndAwaitTerminated(context.Background(), mkv) //nolint:errcheck

	kv, err := NewClient(mkv, c)
	require.NoError(t, err)

	const key = "test"

	val := get(t, kv, key)
	if val != nil {
		t.Error("Expected nil, got:", val)
	}

	// Create member in PENDING state, with some tokens
	name := "Ing 1"
	err = cas(kv, key, updateFn(name))
	require.NoError(t, err)

	r := getData(t, kv, key)
	if r == nil || r.Members[name].Timestamp == 0 || len(r.Members[name].Tokens) <= 0 {
		t.Fatalf("Expected ring with tokens, got %v", r)
	}

	val = get(t, kv, "other key")
	if val != nil {
		t.Errorf("Expected nil, got: %v", val)
	}

	// Update member into ACTIVE state
	err = cas(kv, key, updateFn(name))
	require.NoError(t, err)

	r = getData(t, kv, key)
	if r.Members[name].State != ACTIVE {
		t.Errorf("Expected member to be active after second update, got %v", r)
	}

	// Delete member
	err = cas(kv, key, func(r *data) (*data, bool, error) {
		delete(r.Members, name)
		return r, true, nil
	})
	require.NoError(t, err)

	r = getData(t, kv, key)
	if r.Members[name].State != LEFT {
		t.Errorf("Expected member to be LEFT, got %v", r)
	}
}

func withFixtures(t *testing.T, testFN func(t *testing.T, kv *Client)) {
	t.Helper()

	c := dataCodec{}

	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	cfg.TCPTransport = TCPTransportConfig{
		BindAddrs: getLocalhostAddrs(),
	}
	cfg.Codecs = []codec.Codec{c}

	mkv := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv))
	defer services.StopAndAwaitTerminated(context.Background(), mkv) //nolint:errcheck

	kv, err := NewClient(mkv, c)
	require.NoError(t, err)

	testFN(t, kv)
}

func TestCASNoOutput(t *testing.T) {
	withFixtures(t, func(t *testing.T, kv *Client) {
		// should succeed with single call
		calls := 0
		err := cas(kv, key, func(*data) (*data, bool, error) {
			calls++
			return nil, true, nil
		})
		require.NoError(t, err)

		require.Equal(t, 1, calls)
	})
}

func TestCASErrorNoRetry(t *testing.T) {
	withFixtures(t, func(t *testing.T, kv *Client) {
		calls := 0
		err := casWithErr(context.Background(), kv, key, func(*data) (*data, bool, error) {
			calls++
			return nil, false, errors.New("don't worry, be happy")
		})
		require.EqualError(t, err, "failed to CAS-update key test: fn returned error: don't worry, be happy")
		require.Equal(t, 1, calls)
	})
}

func TestCASErrorWithRetries(t *testing.T) {
	withFixtures(t, func(t *testing.T, kv *Client) {
		calls := 0
		err := casWithErr(context.Background(), kv, key, func(*data) (*data, bool, error) {
			calls++
			return nil, true, errors.New("don't worry, be happy")
		})
		require.EqualError(t, err, "failed to CAS-update key test: fn returned error: don't worry, be happy")
		require.Equal(t, 10, calls) // hard-coded in CAS function.
	})
}

func TestCASNoChange(t *testing.T) {
	t.Parallel()

	withFixtures(t, func(t *testing.T, kv *Client) {
		err := cas(kv, key, func(in *data) (*data, bool, error) {
			if in == nil {
				in = &data{Members: map[string]member{}}
			}

			in.Members["hello"] = member{
				Timestamp: time.Now().Unix(),
				Tokens:    generateTokens(128),
				State:     JOINING,
			}

			return in, true, nil
		})
		require.NoError(t, err)

		startTime := time.Now()
		calls := 0
		err = casWithErr(context.Background(), kv, key, func(d *data) (*data, bool, error) {
			calls++
			return d, true, nil
		})
		require.EqualError(t, err, "failed to CAS-update key test: no change detected")
		require.Equal(t, maxCasRetries, calls)
		// if there was no change, CAS sleeps before every retry
		require.True(t, time.Since(startTime) >= (maxCasRetries-1)*noChangeDetectedRetrySleep)
	})
}

func TestCASNoChangeShortTimeout(t *testing.T) {
	withFixtures(t, func(t *testing.T, kv *Client) {
		err := cas(kv, key, func(in *data) (*data, bool, error) {
			if in == nil {
				in = &data{Members: map[string]member{}}
			}

			in.Members["hello"] = member{
				Timestamp: time.Now().Unix(),
				Tokens:    generateTokens(128),
				State:     JOINING,
			}

			return in, true, nil
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		calls := 0
		err = casWithErr(ctx, kv, key, func(d *data) (*data, bool, error) {
			calls++
			return d, true, nil
		})
		require.EqualError(t, err, "failed to CAS-update key test: context deadline exceeded")
		require.Equal(t, 1, calls) // hard-coded in CAS function.
	})
}

func TestCASFailedBecauseOfVersionChanges(t *testing.T) {
	withFixtures(t, func(t *testing.T, kv *Client) {
		err := cas(kv, key, func(*data) (*data, bool, error) {
			return &data{Members: map[string]member{"nonempty": {Timestamp: time.Now().Unix()}}}, true, nil
		})
		require.NoError(t, err)

		calls := 0
		// outer cas
		err = casWithErr(context.Background(), kv, key, func(d *data) (*data, bool, error) {
			// outer CAS logic
			calls++

			// run inner-CAS that succeeds, and that will make outer cas to fail
			err = cas(kv, key, func(d *data) (*data, bool, error) {
				// to avoid delays due to merging, we update different ingester each time.
				d.Members[fmt.Sprintf("%d", calls)] = member{
					Timestamp: time.Now().Unix(),
				}
				return d, true, nil
			})
			require.NoError(t, err)

			d.Members["world"] = member{
				Timestamp: time.Now().Unix(),
			}
			return d, true, nil
		})

		require.EqualError(t, err, "failed to CAS-update key test: too many retries")
		require.Equal(t, maxCasRetries, calls)
	})
}

func TestMultipleCAS(t *testing.T) {
	t.Parallel()

	c := dataCodec{}

	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	cfg.TCPTransport = TCPTransportConfig{
		BindAddrs: getLocalhostAddrs(),
	}
	cfg.Codecs = []codec.Codec{c}

	mkv := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	mkv.maxCasRetries = 20
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv))
	defer services.StopAndAwaitTerminated(context.Background(), mkv) //nolint:errcheck

	kv, err := NewClient(mkv, c)
	require.NoError(t, err)

	wg := &sync.WaitGroup{}
	start := make(chan struct{})

	const members = 10
	const namePattern = "Member-%d"

	for i := 0; i < members; i++ {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			<-start
			up := updateFn(name)
			err := cas(kv, "test", up) // JOINING state
			require.NoError(t, err)
			err = cas(kv, "test", up) // ACTIVE state
			require.NoError(t, err)
		}(fmt.Sprintf(namePattern, i))
	}

	close(start) // start all CAS updates
	wg.Wait()    // wait until all CAS updates are finished

	// Now let's test that all members are in ACTIVE state
	r := getData(t, kv, "test")
	require.True(t, r != nil, "nil ring")

	for i := 0; i < members; i++ {
		n := fmt.Sprintf(namePattern, i)

		if r.Members[n].State != ACTIVE {
			t.Errorf("Expected member %s to be ACTIVE got %v", n, r.Members[n].State)
		}
	}

	// Make all members leave
	start = make(chan struct{})

	for i := 0; i < members; i++ {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()

			<-start
			up := func(in *data) (out *data, retry bool, err error) {
				delete(in.Members, name)
				return in, true, nil
			}
			err := cas(kv, "test", up) // PENDING state
			require.NoError(t, err)
		}(fmt.Sprintf(namePattern, i))
	}

	close(start) // start all CAS updates
	wg.Wait()    // wait until all CAS updates are finished

	r = getData(t, kv, "test")
	require.True(t, r != nil, "nil ring")

	for i := 0; i < members; i++ {
		n := fmt.Sprintf(namePattern, i)

		if r.Members[n].State != LEFT {
			t.Errorf("Expected member %s to be ACTIVE got %v", n, r.Members[n].State)
		}
	}
}

func defaultKVConfig(i int) KVConfig {
	id := fmt.Sprintf("Member-%d", i)
	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	cfg.NodeName = id

	cfg.GossipInterval = 100 * time.Millisecond
	cfg.GossipNodes = 10
	cfg.PushPullInterval = 5 * time.Second
	cfg.ObsoleteEntriesTimeout = 5 * time.Second

	cfg.TCPTransport = TCPTransportConfig{
		BindAddrs: getLocalhostAddrs(),
		BindPort:  0, // randomize ports
	}

	return cfg
}

func TestDelete(t *testing.T) {
	t.Parallel()

	c := dataCodec{}

	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	cfg.TCPTransport = TCPTransportConfig{
		BindAddrs: getLocalhostAddrs(),
		BindPort:  0, // randomize ports
	}
	cfg.GossipNodes = 1
	cfg.GossipInterval = 100 * time.Millisecond
	cfg.ObsoleteEntriesTimeout = 1 * time.Second
	cfg.ClusterLabelVerificationDisabled = true
	cfg.Codecs = []codec.Codec{c}

	mkv := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv))
	defer services.StopAndAwaitTerminated(context.Background(), mkv) //nolint:errcheck

	kv, err := NewClient(mkv, c)
	require.NoError(t, err)

	const key = "test"

	val := get(t, kv, key)
	if val != nil {
		t.Error("Expected nil, got:", val)
	}

	err = cas(kv, key, updateFn("test"))
	require.NoError(t, err)

	err = kv.Delete(context.Background(), key)
	if err != nil {
		t.Fatalf("Failed to delete key %s: %v", key, err)
	}

	time.Sleep(2 * time.Second) // wait for obsolete entries to be removed
	val = get(t, kv, key)

	if val != nil {
		t.Errorf("Expected nil, got: %v", val)
	}
}

func TestDeleteMultipleClients(t *testing.T) {
	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	cfg.TCPTransport = TCPTransportConfig{
		BindAddrs: getLocalhostAddrs(),
		BindPort:  0, // randomize
	}

	cfg.Codecs = []codec.Codec{
		dataCodec{},
	}

	cfg.PushPullInterval = 1 * time.Second
	cfg.ObsoleteEntriesTimeout = 1 * time.Second

	mkv1 := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv1))
	defer services.StopAndAwaitTerminated(context.Background(), mkv1) //nolint:errcheck

	kv1, err := NewClient(mkv1, dataCodec{})
	require.NoError(t, err)

	const memberKey = "entry"

	// Calling updateFn once creates single entry, in JOINING state.
	err = cas(kv1, key, updateFn(memberKey))
	require.NoError(t, err)

	// We will read values from second KV, which will join the first one
	cfg.JoinMembers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(mkv1.GetListeningPort()))}

	mkv2 := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	go func() {
		// Wait a bit, and then start mkv2.
		time.Sleep(500 * time.Millisecond)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv2))
	}()

	defer services.StopAndAwaitTerminated(context.Background(), mkv2) //nolint:errcheck

	// While waiting for mkv2 to start, we can already create a client for it.
	// Any client operations will block until mkv2 transitioned to Running state.
	kv2, err := NewClient(mkv2, dataCodec{})
	require.NoError(t, err)

	val, err := kv2.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, val)

	err = kv1.Delete(context.Background(), key)
	if err != nil {
		t.Fatalf("Failed to delete key %s: %v", key, err)
	}

	time.Sleep(5 * time.Second) // wait for obsolete entries to be removed

	val, err = kv1.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, val)
	val, err = kv2.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, val)
}

func TestMultipleClients(t *testing.T) {
	t.Parallel()

	members := 10

	err := testMultipleClientsWithConfigGenerator(t, members, defaultKVConfig)
	require.NoError(t, err)
}

func TestMultipleClientsWithMixedLabelsAndExpectFailure(t *testing.T) {
	t.Parallel()

	// We want 3 members, they will be configured with the following labels:
	// 1) ""
	// 2) "label1"
	// 3) "label2"
	// 4) "label3"
	// 5) "label4"
	//
	// We expect that it won't be possible to build a memberlist cluster with mixed labels.
	var membersLabel = []string{
		"",
		"label1",
		"label2",
		"label3",
		"label4",
	}

	configGen := func(i int) KVConfig {
		cfg := defaultKVConfig(i)

		cfg.ClusterLabelVerificationDisabled = false
		cfg.ClusterLabel = membersLabel[i]

		return cfg
	}

	err := testMultipleClientsWithConfigGenerator(t, len(membersLabel), configGen)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected to see at least 2 members, got 1")
}

func TestMultipleClientsWithMixedLabelsAndClusterLabelVerificationDisabled(t *testing.T) {
	t.Parallel()

	// We want 3 members, all will have the cluster label verification disabled.
	// They will be configured with mixed labels, and some without any labels.
	//
	// If the disabled verification works as expected then these members
	// will be able to form a cluster together.
	var membersLabel = []string{
		"",
		"label1",
		"label2",
	}

	configGen := func(i int) KVConfig {
		cfg := defaultKVConfig(i)

		cfg.ClusterLabelVerificationDisabled = true
		cfg.ClusterLabel = membersLabel[i]

		return cfg
	}

	err := testMultipleClientsWithConfigGenerator(t, len(membersLabel), configGen)
	require.NoError(t, err)
}

func TestMultipleClientsWithSameLabelWithClusterLabelVerification(t *testing.T) {
	t.Parallel()

	members := 10
	label := "myTestLabel"

	configGen := func(i int) KVConfig {
		cfg := defaultKVConfig(i)

		cfg.ClusterLabel = label
		return cfg
	}

	err := testMultipleClientsWithConfigGenerator(t, members, configGen)
	require.NoError(t, err)
}

func testMultipleClientsWithConfigGenerator(t *testing.T, members int, configGen func(memberId int) KVConfig) error {
	t.Helper()

	c := dataCodec{}
	const key = "ring"
	var clients []*Client
	port := 0
	casInterval := time.Second

	var clientWg sync.WaitGroup
	clientWg.Add(members)
	clientErrCh := make(chan error, members)
	getClientErr := func() error {
		select {
		case err := <-clientErrCh:
			return err
		default:
			return nil
		}
	}

	stop := make(chan struct{})
	defer func() {
		close(stop)
		clientWg.Wait()
	}()
	start := make(chan struct{})

	for i := 0; i < members; i++ {
		cfg := configGen(i)
		cfg.Codecs = []codec.Codec{c}

		mkv := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv))

		kv, err := NewClient(mkv, c)
		require.NoError(t, err)
		clients = append(clients, kv)

		go func(port int) {
			defer clientWg.Done()

			if err := runClient(kv, cfg.NodeName, key, port, casInterval, start, stop); err != nil {
				clientErrCh <- err
			}
		}(port) // Must copy value, otherwise the next iteration might update it before runClient() gets it.

		// next KV will connect to this one
		port = kv.kv.GetListeningPort()
	}

	t.Log("Waiting before start")
	time.Sleep(2 * time.Second)
	close(start)

	if err := getClientErr(); err != nil {
		return err
	}

	t.Log("Observing ring ...")

	startTime := time.Now()
	firstKv := clients[0]
	ctx, cancel := context.WithTimeout(context.Background(), casInterval*3) // Watch for 3x cas intervals.
	joinedMembers := 0
	firstKv.WatchKey(ctx, key, func(in interface{}) bool {
		r := in.(*data)
		joinedMembers = len(r.Members)

		minTimestamp, maxTimestamp, avgTimestamp := getTimestamps(r.Members)

		now := time.Now()
		t.Log("Update", now.Sub(startTime).String(), ": Ring has", len(r.Members), "members, and", len(r.getAllTokens()),
			"tokens, oldest timestamp:", now.Sub(time.Unix(minTimestamp, 0)).String(),
			"avg timestamp:", now.Sub(time.Unix(avgTimestamp, 0)).String(),
			"youngest timestamp:", now.Sub(time.Unix(maxTimestamp, 0)).String())
		return true // yes, keep watching
	})
	cancel() // make linter happy

	if joinedMembers <= 1 {
		// expect at least 2 members. Otherwise, this means that the ring has failed to sync.
		return fmt.Errorf("expected to see at least 2 members, got %d", joinedMembers)
	}

	if err := getClientErr(); err != nil {
		return err
	}

	// Let's check all the clients to see if they have relatively up-to-date information
	// All of them should at least have all the clients
	// And same tokens.
	check := func() error {
		allTokens := []uint32(nil)

		for i := 0; i < members; i++ {
			kv := clients[i]

			r := getData(t, kv, key)
			t.Logf("KV %d: number of known members: %d\n", i, len(r.Members))
			if len(r.Members) != members {
				return fmt.Errorf("Member %d has only %d members in the ring", i, len(r.Members))
			}

			minTimestamp, maxTimestamp, avgTimestamp := getTimestamps(r.Members)
			for n, ing := range r.Members {
				if ing.State != ACTIVE {
					stateStr := "UNKNOWN"
					switch ing.State {
					case JOINING:
						stateStr = "JOINING"
					case LEFT:
						stateStr = "LEFT"
					}
					return fmt.Errorf("Member %d: invalid state of member %s in the ring: %s (%v) ", i, n, stateStr, ing.State)
				}
			}
			now := time.Now()
			t.Logf("Member %d: oldest: %v, avg: %v, youngest: %v", i,
				now.Sub(time.Unix(minTimestamp, 0)).String(),
				now.Sub(time.Unix(avgTimestamp, 0)).String(),
				now.Sub(time.Unix(maxTimestamp, 0)).String())

			tokens := r.getAllTokens()
			if allTokens == nil {
				allTokens = tokens
				t.Logf("Found tokens: %d", len(allTokens))
			} else {
				if len(allTokens) != len(tokens) {
					return fmt.Errorf("Member %d: Expected %d tokens, got %d", i, len(allTokens), len(tokens))
				}

				for ix, tok := range allTokens {
					if tok != tokens[ix] {
						return fmt.Errorf("Member %d: Tokens at position %d differ: %v, %v", i, ix, tok, tokens[ix])
					}
				}
			}
		}

		return getClientErr()
	}

	// Try this for ~10 seconds. memberlist is eventually consistent, so we may need to wait a bit, especially with `-race`.
	for timeout := time.After(10 * time.Second); ; {
		select {
		case <-timeout:
			return check() // return last error
		default:
			if err := check(); err == nil {
				return nil // it passed
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func TestJoinMembersWithRetryBackoff(t *testing.T) {

	c := dataCodec{}

	const members = 3
	const key = "ring"

	tests := map[string]struct {
		dnsDelayForFirstKV int
		startDelayForRest  time.Duration
		queryType          string
	}{
		"Test late start of members with fast join": {
			dnsDelayForFirstKV: 0,
			startDelayForRest:  1 * time.Second,
			queryType:          "",
		},
		"Test late start of members with DNS lookup": {
			dnsDelayForFirstKV: 0,
			startDelayForRest:  1 * time.Second,
			queryType:          "dns+",
		},
		"Test late start of DNS service": {
			dnsDelayForFirstKV: 5, // fail DNS lookup for 5 times (fast join and first couple of normal join DNS lookups)
			startDelayForRest:  0,
			queryType:          "dns+",
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			var clients []*Client

			stop := make(chan struct{})
			start := make(chan struct{})

			ports, err := getFreePorts(members)
			require.NoError(t, err)

			watcher := services.NewFailureWatcher()
			go func() {
				for {
					select {
					case err := <-watcher.Chan():
						t.Errorf("service reported error: %v", err)
					case <-stop:
						return
					}
				}
			}()

			kvsStarted := &sync.WaitGroup{}
			for i, port := range ports {
				id := fmt.Sprintf("Member-%d", i)
				var cfg KVConfig
				flagext.DefaultValues(&cfg)
				cfg.NodeName = id

				cfg.GossipInterval = 100 * time.Millisecond
				cfg.GossipNodes = 3
				cfg.PushPullInterval = 5 * time.Second

				cfg.MinJoinBackoff = 100 * time.Millisecond
				cfg.MaxJoinBackoff = 1 * time.Minute
				cfg.MaxJoinRetries = 10
				cfg.AbortIfJoinFails = true

				cfg.TCPTransport = TCPTransportConfig{
					BindAddrs: getLocalhostAddrs(),
					BindPort:  port,
				}

				cfg.Codecs = []codec.Codec{c}
				dnsDelay := 0
				if i == 0 {
					// Add members to first KV config to join immediately on initialization.
					// This will enforce backoff as each next members listener is not open yet.
					cfg.JoinMembers = []string{fmt.Sprintf("%slocalhost:%d", testData.queryType, ports[1])}
					dnsDelay = testData.dnsDelayForFirstKV
				} else {
					// Add possible delay to each next member to force backoff
					time.Sleep(testData.startDelayForRest)
				}

				mkv := NewKV(cfg, log.NewNopLogger(), &delayedDNSProviderMock{delay: dnsDelay}, prometheus.NewPedanticRegistry()) // Not started yet.
				watcher.WatchService(mkv)

				kv, err := NewClient(mkv, c)
				require.NoError(t, err)

				clients = append(clients, kv)

				startKVAndRunClient := func(kv *Client, id string, port int) {
					err := services.StartAndAwaitRunning(context.Background(), mkv)
					kvsStarted.Done()
					require.NoError(t, err, "failed to start KV: %v")
					err = runClient(kv, id, key, port, time.Second, start, stop)
					require.NoError(t, err)
				}

				kvsStarted.Add(1)
				if i < 2 {
					go startKVAndRunClient(kv, id, 0)
				} else {
					go startKVAndRunClient(kv, id, ports[i-1])
				}
			}

			t.Log("Waiting for all members to join memberlist cluster")
			kvsStarted.Wait() // We want the KVs to not wait for each other in startup, but our test clients expect that each KV is started.
			close(start)

			t.Log("Observing ring ...")

			startTime := time.Now()
			firstKv := clients[0]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			observedMembers := 0
			firstKv.WatchKey(ctx, key, func(in interface{}) bool {
				r := in.(*data)
				observedMembers = len(r.Members)

				now := time.Now()
				t.Log("Update", now.Sub(startTime).String(), ": Ring has", len(r.Members), "members, and", len(r.getAllTokens()),
					"tokens")
				return observedMembers != members // wait until expected members reached
			})
			cancel() // make linter happy

			// Let clients exchange messages for a while
			close(stop)

			if observedMembers < members {
				t.Errorf("expected to see at least %d but saw %d", members, observedMembers)
			}
		})
	}
}

func TestMemberlistFailsToJoin(t *testing.T) {
	c := dataCodec{}

	ports, err := getFreePorts(1)
	require.NoError(t, err)

	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	cfg.MinJoinBackoff = 100 * time.Millisecond
	cfg.MaxJoinBackoff = 100 * time.Millisecond
	cfg.MaxJoinRetries = 2
	cfg.AbortIfJoinFails = true

	cfg.TCPTransport = TCPTransportConfig{
		BindAddrs: getLocalhostAddrs(),
		BindPort:  0,
	}

	cfg.JoinMembers = []string{net.JoinHostPort(getLocalhostAddr(), strconv.Itoa(ports[0]))}

	cfg.Codecs = []codec.Codec{c}

	mkv := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv))

	ctxTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Service should fail soon after starting, since it cannot join the cluster.
	_ = mkv.AwaitTerminated(ctxTimeout)

	// We verify service state here.
	require.Equal(t, mkv.FailureCase(), errFailedToJoinCluster)
}

func getFreePorts(count int) ([]int, error) {
	var ports []int
	for i := 0; i < count; i++ {
		addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(getLocalhostAddr(), "0"))
		if err != nil {
			return nil, err
		}

		l, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return nil, err
		}
		defer l.Close()
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}

func getTimestamps(members map[string]member) (min int64, max int64, avg int64) {
	min = int64(math.MaxInt64)

	for _, ing := range members {
		if ing.Timestamp < min {
			min = ing.Timestamp
		}

		if ing.Timestamp > max {
			max = ing.Timestamp
		}

		avg += ing.Timestamp
	}
	if len(members) > 0 {
		avg /= int64(len(members))
	}
	return
}

func runClient(kv *Client, name string, ringKey string, portToConnect int, casInterval time.Duration, start <-chan struct{}, stop <-chan struct{}) error {
	// stop gossipping about the ring(s)
	defer services.StopAndAwaitTerminated(context.Background(), kv.kv) //nolint:errcheck

	for {
		select {
		case <-start:
			start = nil

			// let's join the first member
			if portToConnect > 0 {
				_, err := kv.kv.JoinMembers([]string{net.JoinHostPort(getLocalhostAddr(), strconv.Itoa(portToConnect))})
				if err != nil {
					return fmt.Errorf("%s failed to join the cluster: %f", name, err)
				}
			}
		case <-stop:
			return nil
		case <-time.After(casInterval):
			err := cas(kv, ringKey, updateFn(name))
			if err != nil {
				return fmt.Errorf("failed to cas the ring: %f", err)
			}
		}
	}
}

// avoid dependency on ring package
func generateTokens(numTokens int) []uint32 {
	var tokens []uint32
	for i := 0; i < numTokens; {
		candidate := rand.Uint32()
		tokens = append(tokens, candidate)
		i++
	}
	return tokens
}

type distributedCounter map[string]int

func (dc distributedCounter) Merge(mergeable Mergeable, _ bool) (Mergeable, error) {
	if mergeable == nil {
		return nil, nil
	}

	odc, ok := mergeable.(distributedCounter)
	if !ok || odc == nil {
		return nil, fmt.Errorf("invalid thing to merge: %T", mergeable)
	}

	updated := distributedCounter{}

	for k, v := range odc {
		if v > dc[k] {
			dc[k] = v
			updated[k] = v
		}
	}

	if len(updated) == 0 {
		return nil, nil
	}
	return updated, nil
}

func (dc distributedCounter) MergeContent() []string {
	// return list of keys
	out := []string(nil)
	for k := range dc {
		out = append(out, k)
	}
	return out
}

func (dc distributedCounter) RemoveTombstones(_ time.Time) (_, _ int) {
	// nothing to do
	return
}

func (dc distributedCounter) Clone() Mergeable {
	out := make(distributedCounter, len(dc))
	for k, v := range dc {
		out[k] = v
	}
	return out
}

type distributedCounterCodec struct{}

func (d distributedCounterCodec) CodecID() string {
	return "distributedCounter"
}

func (d distributedCounterCodec) Decode(b []byte) (interface{}, error) {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	out := &distributedCounter{}
	err := dec.Decode(out)
	return *out, err
}

func (d distributedCounterCodec) Encode(val interface{}) ([]byte, error) {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
	return buf.Bytes(), err
}

var _ codec.Codec = &distributedCounterCodec{}

func TestMultipleCodecs(t *testing.T) {
	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	cfg.TCPTransport = TCPTransportConfig{
		BindAddrs: getLocalhostAddrs(),
		BindPort:  0, // randomize
	}

	cfg.Codecs = []codec.Codec{
		dataCodec{},
		distributedCounterCodec{},
	}

	mkv1 := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv1))
	defer services.StopAndAwaitTerminated(context.Background(), mkv1) //nolint:errcheck

	kv1, err := NewClient(mkv1, dataCodec{})
	require.NoError(t, err)

	kv2, err := NewClient(mkv1, distributedCounterCodec{})
	require.NoError(t, err)

	err = kv1.CAS(context.Background(), "data", func(in interface{}) (out interface{}, retry bool, err error) {
		var d *data
		if in != nil {
			d = in.(*data)
		}
		if d == nil {
			d = &data{}
		}
		if d.Members == nil {
			d.Members = map[string]member{}
		}
		d.Members["test"] = member{
			Timestamp: time.Now().Unix(),
			State:     ACTIVE,
		}
		return d, true, nil
	})
	require.NoError(t, err)

	err = kv2.CAS(context.Background(), "counter", func(in interface{}) (out interface{}, retry bool, err error) {
		var dc distributedCounter
		if in != nil {
			dc = in.(distributedCounter)
		}
		if dc == nil {
			dc = distributedCounter{}
		}
		dc["test"] = 5
		return dc, true, err
	})
	require.NoError(t, err)

	// We will read values from second KV, which will join the first one
	mkv2 := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv2))
	defer services.StopAndAwaitTerminated(context.Background(), mkv2) //nolint:errcheck

	// Join second KV to first one. That will also trigger state transfer.
	_, err = mkv2.JoinMembers([]string{net.JoinHostPort("127.0.0.1", strconv.Itoa(mkv1.GetListeningPort()))})
	require.NoError(t, err)

	// Now read both values from second KV. It should have both values by now.

	// fetch directly from single KV, to see that both are stored in the same one
	val, err := mkv2.Get("data", dataCodec{})
	require.NoError(t, err)
	require.NotNil(t, val)
	require.NotZero(t, val.(*data).Members["test"].Timestamp)
	require.Equal(t, ACTIVE, val.(*data).Members["test"].State)

	val, err = mkv2.Get("counter", distributedCounterCodec{})
	require.NoError(t, err)
	require.NotNil(t, val)
	require.Equal(t, 5, val.(distributedCounter)["test"])
}

func TestGenerateRandomSuffix(t *testing.T) {
	h1 := generateRandomSuffix(testLogger{})
	h2 := generateRandomSuffix(testLogger{})
	h3 := generateRandomSuffix(testLogger{})

	require.NotEqual(t, h1, h2)
	require.NotEqual(t, h2, h3)
}

func TestRejoin(t *testing.T) {
	ports, err := getFreePorts(2)
	require.NoError(t, err)

	var cfg1 KVConfig
	flagext.DefaultValues(&cfg1)
	cfg1.TCPTransport = TCPTransportConfig{
		BindAddrs: getLocalhostAddrs(),
		BindPort:  ports[0],
	}

	cfg1.RandomizeNodeName = true
	cfg1.Codecs = []codec.Codec{dataCodec{}}
	cfg1.AbortIfJoinFails = false

	cfg2 := cfg1
	cfg2.TCPTransport.BindPort = ports[1]
	cfg2.JoinMembers = []string{net.JoinHostPort("localhost", strconv.Itoa(ports[0]))}
	cfg2.RejoinInterval = 1 * time.Second

	mkv1 := NewKV(cfg1, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv1))
	defer services.StopAndAwaitTerminated(context.Background(), mkv1) //nolint:errcheck

	mkv2 := NewKV(cfg2, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv2))
	defer services.StopAndAwaitTerminated(context.Background(), mkv2) //nolint:errcheck

	expectMembers := func(expected int) func() bool {
		return func() bool { return mkv2.memberlist.NumMembers() == expected }
	}

	require.Eventually(t, expectMembers(2), 10*time.Second, 100*time.Millisecond, "expected 2 members in the cluster")

	// Shutdown first KV
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), mkv1))

	// Second KV should see single member now.
	require.Eventually(t, expectMembers(1), 10*time.Second, 100*time.Millisecond, "expected 1 member in the cluster")

	// Let's start first KV again. It is not configured to join the cluster, but KV2 is rejoining.
	mkv1 = NewKV(cfg1, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv1))
	defer services.StopAndAwaitTerminated(context.Background(), mkv1) //nolint:errcheck

	require.Eventually(t, expectMembers(2), 10*time.Second, 100*time.Millisecond, "expected 2 member in the cluster")
}

func TestMessageBuffer(t *testing.T) {
	buf := []Message(nil)
	size := 0

	buf, size = addMessageToBuffer(buf, size, 100, Message{Size: 50})
	assert.Len(t, buf, 1)
	assert.Equal(t, size, 50)

	buf, size = addMessageToBuffer(buf, size, 100, Message{Size: 50})
	assert.Len(t, buf, 2)
	assert.Equal(t, size, 100)

	buf, size = addMessageToBuffer(buf, size, 100, Message{Size: 25})
	assert.Len(t, buf, 2)
	assert.Equal(t, size, 75)
}

func TestNotifyMsgResendsOnlyChanges(t *testing.T) {
	codec := dataCodec{}

	cfg := KVConfig{
		TCPTransport: TCPTransportConfig{
			BindAddrs: getLocalhostAddrs(),
		},
	}
	// We will be checking for number of messages in the broadcast queue, so make sure to use known retransmit factor.
	cfg.RetransmitMult = 1
	cfg.Codecs = append(cfg.Codecs, codec)

	kv := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), kv))
	defer services.StopAndAwaitTerminated(context.Background(), kv) //nolint:errcheck

	client, err := NewClient(kv, codec)
	require.NoError(t, err)

	// No broadcast messages from KV at the beginning.
	require.Equal(t, 0, len(kv.GetBroadcasts(0, math.MaxInt32)))

	now := time.Now()

	require.NoError(t, client.CAS(context.Background(), key, func(in interface{}) (out interface{}, retry bool, err error) {
		d := getOrCreateData(in)
		d.Members["a"] = member{Timestamp: now.Unix(), State: JOINING}
		d.Members["b"] = member{Timestamp: now.Unix(), State: JOINING}
		return d, true, nil
	}))

	// Check that new instance is broadcasted about just once.
	assert.Equal(t, 1, len(kv.GetBroadcasts(0, math.MaxInt32)))
	require.Equal(t, 0, len(kv.GetBroadcasts(0, math.MaxInt32)))

	kv.NotifyMsg(marshalKeyValuePair(t, key, codec, &data{
		Members: map[string]member{
			"a": {Timestamp: now.Unix() - 5, State: ACTIVE},
			"b": {Timestamp: now.Unix() + 5, State: ACTIVE, Tokens: []uint32{1, 2, 3}},
			"c": {Timestamp: now.Unix(), State: ACTIVE},
		}}))

	// Wait until KV update has been processed.
	time.Sleep(time.Millisecond * 100)

	// Check two things here:
	// 1) state of value in KV store
	// 2) broadcast message only has changed members

	d := getData(t, client, key)
	assert.Equal(t, &data{
		Members: map[string]member{
			"a": {Timestamp: now.Unix(), State: JOINING, Tokens: []uint32{}}, // unchanged, timestamp too old
			"b": {Timestamp: now.Unix() + 5, State: ACTIVE, Tokens: []uint32{1, 2, 3}},
			"c": {Timestamp: now.Unix(), State: ACTIVE, Tokens: []uint32{}},
		}}, d)

	bs := kv.GetBroadcasts(0, math.MaxInt32)
	require.Equal(t, 1, len(bs))

	d = decodeDataFromMarshalledKeyValuePair(t, bs[0], key, codec)
	assert.Equal(t, &data{
		Members: map[string]member{
			// "a" is not here, because it wasn't changed by the message.
			"b": {Timestamp: now.Unix() + 5, State: ACTIVE, Tokens: []uint32{1, 2, 3}},
			"c": {Timestamp: now.Unix(), State: ACTIVE},
		}}, d)
}

func TestSendingOldTombstoneShouldNotForwardMessage(t *testing.T) {
	codec := dataCodec{}

	cfg := KVConfig{
		TCPTransport: TCPTransportConfig{
			BindAddrs: getLocalhostAddrs(),
		},
	}
	// We will be checking for number of messages in the broadcast queue, so make sure to use known retransmit factor.
	cfg.RetransmitMult = 1
	cfg.LeftIngestersTimeout = 5 * time.Minute
	cfg.Codecs = append(cfg.Codecs, codec)

	kv := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), kv))
	defer services.StopAndAwaitTerminated(context.Background(), kv) //nolint:errcheck

	client, err := NewClient(kv, codec)
	require.NoError(t, err)

	now := time.Now()

	// No broadcast messages from KV at the beginning.
	require.Equal(t, 0, len(kv.GetBroadcasts(0, math.MaxInt32)))

	for _, tc := range []struct {
		name             string
		valueBeforeSend  *data // value in KV store before sending messsage
		msgToSend        *data
		valueAfterSend   *data // value in KV store after sending message
		broadcastMessage *data // broadcasted change, if not nil
	}{
		// These tests follow each other (end state of KV in state is starting point in the next state).
		{
			name:             "old tombstone, empty KV",
			valueBeforeSend:  nil,
			msgToSend:        &data{Members: map[string]member{"instance": {Timestamp: now.Unix() - int64(2*cfg.LeftIngestersTimeout.Seconds()), State: LEFT}}},
			valueAfterSend:   nil, // no change to KV
			broadcastMessage: nil, // no new message
		},

		{
			name:             "recent tombstone, empty KV",
			valueBeforeSend:  nil,
			msgToSend:        &data{Members: map[string]member{"instance": {Timestamp: now.Unix(), State: LEFT}}},
			broadcastMessage: &data{Members: map[string]member{"instance": {Timestamp: now.Unix(), State: LEFT}}},
			valueAfterSend:   &data{Members: map[string]member{"instance": {Timestamp: now.Unix(), State: LEFT, Tokens: []uint32{}}}},
		},

		{
			name:             "old tombstone, KV contains tombstone already",
			valueBeforeSend:  &data{Members: map[string]member{"instance": {Timestamp: now.Unix(), State: LEFT, Tokens: []uint32{}}}},
			msgToSend:        &data{Members: map[string]member{"instance": {Timestamp: now.Unix() - 10, State: LEFT}}},
			broadcastMessage: nil,
			valueAfterSend:   &data{Members: map[string]member{"instance": {Timestamp: now.Unix(), State: LEFT, Tokens: []uint32{}}}},
		},

		{
			name:             "fresh tombstone, KV contains tombstone already",
			valueBeforeSend:  &data{Members: map[string]member{"instance": {Timestamp: now.Unix(), State: LEFT, Tokens: []uint32{}}}},
			msgToSend:        &data{Members: map[string]member{"instance": {Timestamp: now.Unix() + 10, State: LEFT}}},
			broadcastMessage: &data{Members: map[string]member{"instance": {Timestamp: now.Unix() + 10, State: LEFT}}},
			valueAfterSend:   &data{Members: map[string]member{"instance": {Timestamp: now.Unix() + 10, State: LEFT, Tokens: []uint32{}}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := getData(t, client, key)
			if tc.valueBeforeSend == nil {
				require.True(t, d == nil || len(d.Members) == 0)
			} else {
				require.Equal(t, tc.valueBeforeSend, d, "valueBeforeSend")
			}

			kv.NotifyMsg(marshalKeyValuePair(t, key, codec, tc.msgToSend))

			// Wait until KV update has been processed.
			time.Sleep(time.Millisecond * 100)

			bs := kv.GetBroadcasts(0, math.MaxInt32)
			if tc.broadcastMessage == nil {
				require.Equal(t, 0, len(bs), "expected no broadcast message")
			} else {
				require.Equal(t, 1, len(bs), "expected broadcast message")
				require.Equal(t, tc.broadcastMessage, decodeDataFromMarshalledKeyValuePair(t, bs[0], key, codec))
			}

			d = getData(t, client, key)
			if tc.valueAfterSend == nil {
				require.True(t, d == nil || len(d.Members) == 0)
			} else {
				require.Equal(t, tc.valueAfterSend, d, "valueAfterSend")
			}
		})
	}
}

func TestFastJoin(t *testing.T) {
	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	cfg.TCPTransport = TCPTransportConfig{
		BindAddrs: getLocalhostAddrs(),
		BindPort:  0, // randomize
	}

	cfg.Codecs = []codec.Codec{
		dataCodec{},
	}

	mkv1 := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv1))
	defer services.StopAndAwaitTerminated(context.Background(), mkv1) //nolint:errcheck

	kv1, err := NewClient(mkv1, dataCodec{})
	require.NoError(t, err)

	const memberKey = "entry"

	// Calling updateFn once creates single entry, in JOINING state.
	err = cas(kv1, key, updateFn(memberKey))
	require.NoError(t, err)

	// We will read values from second KV, which will join the first one
	cfg.JoinMembers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(mkv1.GetListeningPort()))}

	mkv2 := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	go func() {
		// Wait a bit, and then start mkv2.
		time.Sleep(500 * time.Millisecond)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv2))
	}()

	defer services.StopAndAwaitTerminated(context.Background(), mkv2) //nolint:errcheck

	// While waiting for mkv2 to start, we can already create a client for it.
	// Any client operations will block until mkv2 transitioned to Running state.
	kv2, err := NewClient(mkv2, dataCodec{})
	require.NoError(t, err)

	val, err := kv2.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, val)
	require.NotZero(t, val.(*data).Members[memberKey].Timestamp)
	require.Equal(t, JOINING, val.(*data).Members[memberKey].State)
}

func TestDelegateMethodsDontCrashBeforeKVStarts(t *testing.T) {
	codec := dataCodec{}

	cfg := KVConfig{}
	cfg.Codecs = append(cfg.Codecs, codec)
	cfg.TCPTransport = TCPTransportConfig{
		BindAddrs: getLocalhostAddrs(),
	}

	kv := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())

	// Make sure we can call delegate methods on unstarted service, and they don't crash nor block.
	kv.LocalState(true)
	kv.MergeRemoteState(nil, true)
	kv.GetBroadcasts(100, 100)

	now := time.Now()
	msg := &data{
		Members: map[string]member{
			"a": {Timestamp: now.Unix() - 5, State: ACTIVE, Tokens: []uint32{}},
			"b": {Timestamp: now.Unix() + 5, State: ACTIVE, Tokens: []uint32{1, 2, 3}},
			"c": {Timestamp: now.Unix(), State: ACTIVE, Tokens: []uint32{}},
		}}

	kv.NotifyMsg(marshalKeyValuePair(t, key, codec, msg))

	// Verify that message was not added to KV.
	time.Sleep(time.Millisecond * 100)
	val, err := kv.Get(key, codec)
	require.NoError(t, err)
	require.Nil(t, val)

	// Now start the service, and try NotifyMsg again
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), kv))
	defer services.StopAndAwaitTerminated(context.Background(), kv) //nolint:errcheck

	kv.NotifyMsg(marshalKeyValuePair(t, key, codec, msg))

	// Wait until processing finished, and check the message again.
	time.Sleep(time.Millisecond * 100)

	val, err = kv.Get(key, codec)
	require.NoError(t, err)
	assert.Equal(t, msg, val)
}

func TestMetricsRegistration(t *testing.T) {
	c := dataCodec{}

	cfg := KVConfig{}
	cfg.Codecs = append(cfg.Codecs, c)

	reg := prometheus.NewPedanticRegistry()
	kv := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, reg)
	err := kv.CAS(context.Background(), "test", c, func(interface{}) (out interface{}, retry bool, err error) {
		return &data{Members: map[string]member{
			"member": {},
		}}, true, nil
	})
	require.NoError(t, err)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP memberlist_client_kv_store_count Number of values in KV Store
			# TYPE memberlist_client_kv_store_count gauge
			memberlist_client_kv_store_count 1
	`), "memberlist_client_kv_store_count"))

}

func decodeDataFromMarshalledKeyValuePair(t *testing.T, marshalledKVP []byte, key string, codec dataCodec) *data {
	kvp := KeyValuePair{}
	require.NoError(t, kvp.Unmarshal(marshalledKVP))
	require.Equal(t, key, kvp.Key)

	val, err := codec.Decode(kvp.Value)
	require.NoError(t, err)
	d, ok := val.(*data)
	require.True(t, ok)
	return d
}

func keyValuePair(t *testing.T, key string, codec codec.Codec, value interface{}) *KeyValuePair {
	data, err := codec.Encode(value)
	require.NoError(t, err)

	return &KeyValuePair{Key: key, Codec: codec.CodecID(), Value: data}

}

func marshalKeyValuePair(t *testing.T, key string, codec codec.Codec, value interface{}) []byte {
	kvp := keyValuePair(t, key, codec, value)

	data, err := kvp.Marshal()
	require.NoError(t, err)
	return data
}

func getOrCreateData(in interface{}) *data {
	// Modify value that was passed as a parameter.
	// Client takes care of concurrent modifications.
	r, ok := in.(*data)
	if !ok || r == nil {
		return &data{Members: map[string]member{}}
	}
	return r
}

type testLogger struct {
}

func (l testLogger) Log(_ ...interface{}) error {
	return nil
}

type dnsProviderMock struct {
	resolved []string
}

func (p *dnsProviderMock) Resolve(_ context.Context, addrs []string) error {
	p.resolved = addrs
	return nil
}

func (p dnsProviderMock) Addresses() []string {
	return p.resolved
}

type delayedDNSProviderMock struct {
	resolved []string
	delay    int
}

func (p *delayedDNSProviderMock) Resolve(_ context.Context, addrs []string) error {
	if p.delay == 0 {
		p.resolved = make([]string, len(addrs))
		for _, addr := range addrs {
			p.resolved = append(p.resolved, addr[strings.Index(addr, "+")+1:])
		}
		return nil
	}
	p.delay--
	return nil
}

func (p delayedDNSProviderMock) Addresses() []string {
	return p.resolved
}

func TestGetBroadcastsPrefersLocalUpdates(t *testing.T) {
	codec := dataCodec{}

	cfg := KVConfig{
		TCPTransport: TCPTransportConfig{
			BindAddrs: getLocalhostAddrs(),
		},
	}

	// We will be checking for number of messages in the broadcast queue, so make sure to use known retransmit factor.
	cfg.RetransmitMult = 1
	cfg.Codecs = append(cfg.Codecs, codec)

	reg := prometheus.NewRegistry()
	kv := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, reg)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), kv))
	defer services.StopAndAwaitTerminated(context.Background(), kv) //nolint:errcheck

	now := time.Now()

	smallUpdate := &data{Members: map[string]member{"a": {Timestamp: now.Unix(), State: JOINING}}}
	bigUpdate := &data{Members: map[string]member{"b": {Timestamp: now.Unix(), State: JOINING}, "c": {Timestamp: now.Unix(), State: JOINING}, "d": {Timestamp: now.Unix(), State: JOINING}}}
	mediumUpdate := &data{Members: map[string]member{"d": {Timestamp: now.Unix(), State: JOINING}, "e": {Timestamp: now.Unix(), State: JOINING}}}

	// No broadcast messages from KV at the beginning.
	require.Equal(t, 0, len(kv.GetBroadcasts(0, math.MaxInt32)))

	// Check that locally-generated broadcast messages will be prioritized and sent out first, even if they are enqueued later or are smaller than other messages in the queue.
	kv.broadcastNewValue("non-local", smallUpdate, 1, codec, false, false, time.Now())
	kv.broadcastNewValue("non-local", bigUpdate, 2, codec, false, false, time.Now())
	kv.broadcastNewValue("local", smallUpdate, 1, codec, true, false, time.Now())
	kv.broadcastNewValue("local", bigUpdate, 2, codec, true, false, time.Now())
	kv.broadcastNewValue("local", mediumUpdate, 3, codec, true, false, time.Now())

	err := testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP memberlist_client_messages_in_broadcast_queue Number of user messages in the broadcast queue
		# TYPE memberlist_client_messages_in_broadcast_queue gauge
		memberlist_client_messages_in_broadcast_queue{queue="gossip"} 2
		memberlist_client_messages_in_broadcast_queue{queue="local"} 3
	`), "memberlist_client_messages_in_broadcast_queue")
	require.NoError(t, err)

	msgs := kv.GetBroadcasts(0, 10000)
	require.Len(t, msgs, 5) // we get all 4 messages
	require.Equal(t, "local", getKey(t, msgs[0]))
	require.Equal(t, "local", getKey(t, msgs[1]))
	require.Equal(t, "local", getKey(t, msgs[2]))
	require.Equal(t, "non-local", getKey(t, msgs[3]))
	require.Equal(t, "non-local", getKey(t, msgs[4]))

	// Check that TransmitLimitedQueue.GetBroadcasts preferred larger messages (it does that).
	require.True(t, len(msgs[0]) > len(msgs[1])) // Bigger local message is returned before medium one.
	require.True(t, len(msgs[1]) > len(msgs[2])) // Medium local message is returned before small one.
	require.True(t, len(msgs[3]) > len(msgs[4])) // Bigger non-local message is returned before smaller one
}

func getKey(t *testing.T, msg []byte) string {
	kvPair := KeyValuePair{}
	err := kvPair.Unmarshal(msg)
	require.NoError(t, err)
	return kvPair.Key
}

func TestRaceBetweenStoringNewValueForKeyAndUpdatingIt(t *testing.T) {
	codec := dataCodec{}

	cfg := KVConfig{}
	cfg.Codecs = append(cfg.Codecs, codec)
	cfg.TCPTransport = TCPTransportConfig{
		BindAddrs: getLocalhostAddrs(),
	}

	kv := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), kv))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), kv))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	vals := make(chan int64, 10000)

	go func() {
		d := &data{Members: map[string]member{}}
		for i := 0; i < 100; i++ {
			d.Members[fmt.Sprintf("member_%d", i)] = member{Timestamp: time.Now().Unix(), State: i % 3}
		}

		err := kv.CAS(context.Background(), key, codec, func(_ interface{}) (out interface{}, retry bool, err error) {
			return d, true, nil
		})
		require.NoError(t, err)

		// keep iterating over d.Members. If other goroutine modifies same ring descriptor, we will see a race error.
		for ctx.Err() == nil {
			sum := int64(0)
			for n, m := range d.Members {
				sum += int64(len(n))
				sum += m.Timestamp
				sum += int64(len(m.Tokens))
			}
			vals <- sum
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Wait until CAS and iteration finishes before pushing remote state.
	<-vals

	s := 0
	for ctx.Err() == nil {
		s++
		d := &data{Members: map[string]member{}}
		for i := 0; i < 100; i++ {
			d.Members[fmt.Sprintf("member_%d", i)] = member{Timestamp: time.Now().Unix(), State: (i + s) % 3}
		}

		kv.MergeRemoteState(marshalState(t, keyValuePair(t, key, codec, d)), false)
		time.Sleep(10 * time.Millisecond)

	drain:
		select {
		case <-vals:
			goto drain
		default:
			// stop draining.
		}
	}
}

func marshalState(t *testing.T, kvps ...*KeyValuePair) []byte {
	buf := bytes.Buffer{}

	for _, kvp := range kvps {
		d, err := kvp.Marshal()
		require.NoError(t, err)
		err = binary.Write(&buf, binary.BigEndian, uint32(len(d)))
		require.NoError(t, err)
		buf.Write(d)
	}

	return buf.Bytes()
}

func TestNotificationDelay(t *testing.T) {
	cfg := KVConfig{}
	// We're going to trigger sends manually, so effectively disable the automatic send interval.
	const hundredYears = 100 * 365 * 24 * time.Hour
	cfg.NotifyInterval = hundredYears
	kv := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())

	watchChan := make(chan string, 16)

	// Add ourselves as a watcher.
	kv.watchersMu.Lock()
	kv.watchers["foo_123"] = append(kv.watchers["foo_123"], watchChan)
	kv.watchers["foo_124"] = append(kv.watchers["foo_124"], watchChan)
	kv.watchersMu.Unlock()

	defer func() {
		kv.watchersMu.Lock()
		removeWatcherChannel("foo_123", watchChan, kv.watchers)
		removeWatcherChannel("foo_124", watchChan, kv.watchers)
		kv.watchersMu.Unlock()
	}()

	verifyNotifs := func(expected map[string]int, comment string) {
		observed := make(map[string]int, len(expected))
		for kk := range expected {
			observed[kk] = 0
		}
	loop:
		for {
			select {
			case k := <-watchChan:
				observed[k]++
			default:
				break loop
			}
		}
		require.Equal(t, expected, observed, comment)
	}

	drainChan := func() {
		for {
			select {
			case <-watchChan:
			default:
				return
			}
		}
	}

	kv.notifyWatchers("foo_123")
	kv.sendKeyNotifications()
	verifyNotifs(map[string]int{"foo_123": 1}, "1 change 1 notification")

	// Test coalescing of updates.
	drainChan()
	verifyNotifs(map[string]int{"foo_123": 0}, "chan drained")
	kv.notifyWatchers("foo_123")
	verifyNotifs(map[string]int{"foo_123": 0}, "no flush -> no watcher notification")
	kv.notifyWatchers("foo_123")
	verifyNotifs(map[string]int{"foo_123": 0}, "no flush -> no watcher notification")
	kv.notifyWatchers("foo_123")
	verifyNotifs(map[string]int{"foo_123": 0}, "no flush -> no watcher notification")
	kv.notifyWatchers("foo_123")
	verifyNotifs(map[string]int{"foo_123": 0}, "no flush -> no watcher notification")
	kv.notifyWatchers("foo_123")
	verifyNotifs(map[string]int{"foo_123": 0}, "no flush -> no watcher notification")
	kv.notifyWatchers("foo_123")
	verifyNotifs(map[string]int{"foo_123": 0}, "no flush -> no watcher notification")
	kv.sendKeyNotifications()
	verifyNotifs(map[string]int{"foo_123": 1}, "flush should coalesce updates")

	// multiple buffered updates
	drainChan()
	verifyNotifs(map[string]int{"foo_123": 0}, "chan drained")
	kv.notifyWatchers("foo_123")
	kv.sendKeyNotifications()
	kv.notifyWatchers("foo_123")
	kv.sendKeyNotifications()
	verifyNotifs(map[string]int{"foo_123": 2}, "two buffered updates")

	// multiple keys
	drainChan()
	kv.notifyWatchers("foo_123")
	kv.notifyWatchers("foo_124")
	kv.sendKeyNotifications()
	verifyNotifs(map[string]int{"foo_123": 1, "foo_124": 1}, "2 changes 2 notifications")
	kv.sendKeyNotifications()
	verifyNotifs(map[string]int{"foo_123": 0, "foo_124": 0}, "no new notifications")

	// sendKeyNotifications can be called repeatedly without new updates.
	kv.sendKeyNotifications()
	kv.sendKeyNotifications()
	kv.sendKeyNotifications()
	kv.sendKeyNotifications()

	// Finally, exercise the monitor method.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tick := make(chan time.Time)
	go kv.monitorKeyNotifications(ctx, tick)
	kv.notifyWatchers("foo_123")
	tick <- time.Now()

	require.Eventually(t, func() bool {
		select {
		case k := <-watchChan:
			if k != "foo_123" {
				panic(fmt.Sprintf("unexpected key: %s", k))
			}
			return true
		default: // nothing yet.
			return false
		}
	}, 20*time.Second, 100*time.Millisecond)
}
