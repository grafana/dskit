package memberlist

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
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

func TestBasicGetAndCas(t *testing.T) {
	c := dataCodec{}

	name := "Ing 1"
	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	cfg.TCPTransport = TCPTransportConfig{
		BindAddrs: []string{"localhost"},
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
	cfg.TCPTransport = TCPTransportConfig{}
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
		err := cas(kv, key, func(d *data) (*data, bool, error) {
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
		err := casWithErr(context.Background(), kv, key, func(d *data) (*data, bool, error) {
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
		err := casWithErr(context.Background(), kv, key, func(d *data) (*data, bool, error) {
			calls++
			return nil, true, errors.New("don't worry, be happy")
		})
		require.EqualError(t, err, "failed to CAS-update key test: fn returned error: don't worry, be happy")
		require.Equal(t, 10, calls) // hard-coded in CAS function.
	})
}

func TestCASNoChange(t *testing.T) {
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
		err := cas(kv, key, func(in *data) (*data, bool, error) {
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
	c := dataCodec{}

	var cfg KVConfig
	flagext.DefaultValues(&cfg)
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

	cfg.TCPTransport = TCPTransportConfig{
		BindAddrs: []string{"localhost"},
		BindPort:  0, // randomize ports
	}

	return cfg
}

func TestMultipleClients(t *testing.T) {
	members := 10

	err := testMultipleClientsWithConfigGenerator(t, members, defaultKVConfig)
	require.NoError(t, err)
}

func TestMultipleClientsWithMixedLabelsAndExpectFailure(t *testing.T) {
	// We want 3 members, they will be configured with the following labels:
	// 1) ""
	// 2) "label1"
	// 3) "label2"
	//
	// We expect that it won't be possible to build a memberlist cluster with mixed labels.
	var membersLabel = []string{
		"",
		"label1",
		"label2",
	}

	configGen := func(i int) KVConfig {
		cfg := defaultKVConfig(i)

		cfg.ClusterLabelVerificationDisabled = false
		cfg.ClusterLabel = membersLabel[i]

		return cfg
	}

	err := testMultipleClientsWithConfigGenerator(t, len(membersLabel), configGen)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("expected to see at least %d updates, got", len(membersLabel)))
}

func TestMultipleClientsWithMixedLabelsAndClusterLabelVerificationDisabled(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), casInterval*3/2) // Watch for 1.5 cas intervals.
	updates := 0
	firstKv.WatchKey(ctx, key, func(in interface{}) bool {
		updates++

		r := in.(*data)

		minTimestamp, maxTimestamp, avgTimestamp := getTimestamps(r.Members)

		now := time.Now()
		t.Log("Update", now.Sub(startTime).String(), ": Ring has", len(r.Members), "members, and", len(r.getAllTokens()),
			"tokens, oldest timestamp:", now.Sub(time.Unix(minTimestamp, 0)).String(),
			"avg timestamp:", now.Sub(time.Unix(avgTimestamp, 0)).String(),
			"youngest timestamp:", now.Sub(time.Unix(maxTimestamp, 0)).String())
		return true // yes, keep watching
	})
	cancel() // make linter happy

	t.Logf("Ring updates observed: %d", updates)

	if updates < members {
		// in general, at least one update from each node. (although that's not necessarily true...
		// but typically we get more updates than that anyway)
		return fmt.Errorf("expected to see at least %d updates, got %d", members, updates)
	}

	if err := getClientErr(); err != nil {
		return err
	}

	// Let's check all the clients to see if they have relatively up-to-date information
	// All of them should at least have all the clients
	// And same tokens.
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
				return fmt.Errorf("Member %d: invalid state of member %s in the ring: %v ", i, n, ing.State)
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

func TestJoinMembersWithRetryBackoff(t *testing.T) {
	c := dataCodec{}

	const members = 3
	const key = "ring"

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
			BindAddrs: []string{"localhost"},
			BindPort:  port,
		}

		cfg.Codecs = []codec.Codec{c}

		if i == 0 {
			// Add members to first KV config to join immediately on initialization.
			// This will enforce backoff as each next members listener is not open yet.
			cfg.JoinMembers = []string{fmt.Sprintf("localhost:%d", ports[1])}
		} else {
			// Add delay to each next member to force backoff
			time.Sleep(1 * time.Second)
		}

		mkv := NewKV(cfg, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry()) // Not started yet.
		watcher.WatchService(mkv)

		kv, err := NewClient(mkv, c)
		require.NoError(t, err)

		clients = append(clients, kv)

		startKVAndRunClient := func(kv *Client, id string, port int) {
			err = services.StartAndAwaitRunning(context.Background(), mkv)
			if err != nil {
				t.Errorf("failed to start KV: %v", err)
			}
			err = runClient(kv, id, key, port, time.Second, start, stop)
			require.NoError(t, err)
		}

		if i == 0 {
			go startKVAndRunClient(kv, id, 0)
		} else {
			go startKVAndRunClient(kv, id, ports[i-1])
		}
	}

	t.Log("Waiting for all members to join memberlist cluster")
	close(start)
	time.Sleep(2 * time.Second)

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
		return true // yes, keep watching
	})
	cancel() // make linter happy

	// Let clients exchange messages for a while
	close(stop)

	if observedMembers < members {
		t.Errorf("expected to see at least %d but saw %d", members, observedMembers)
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
		BindAddrs: []string{"localhost"},
		BindPort:  0,
	}

	cfg.JoinMembers = []string{fmt.Sprintf("127.0.0.1:%d", ports[0])}

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
		addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
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
				_, err := kv.kv.JoinMembers([]string{fmt.Sprintf("127.0.0.1:%d", portToConnect)})
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

func (dc distributedCounter) Merge(mergeable Mergeable, localCAS bool) (Mergeable, error) {
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

func (dc distributedCounter) RemoveTombstones(limit time.Time) (_, _ int) {
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
		BindAddrs: []string{"localhost"},
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
	_, err = mkv2.JoinMembers([]string{fmt.Sprintf("127.0.0.1:%d", mkv1.GetListeningPort())})
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
		BindAddrs: []string{"localhost"},
		BindPort:  ports[0],
	}

	cfg1.RandomizeNodeName = true
	cfg1.Codecs = []codec.Codec{dataCodec{}}
	cfg1.AbortIfJoinFails = false

	cfg2 := cfg1
	cfg2.TCPTransport.BindPort = ports[1]
	cfg2.JoinMembers = []string{fmt.Sprintf("localhost:%d", ports[0])}
	cfg2.RejoinInterval = 1 * time.Second

	mkv1 := NewKV(cfg1, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv1))
	defer services.StopAndAwaitTerminated(context.Background(), mkv1) //nolint:errcheck

	mkv2 := NewKV(cfg2, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv2))
	defer services.StopAndAwaitTerminated(context.Background(), mkv2) //nolint:errcheck

	membersFunc := func() interface{} {
		return mkv2.memberlist.NumMembers()
	}

	poll(t, 5*time.Second, 2, membersFunc)

	// Shutdown first KV
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), mkv1))

	// Second KV should see single member now.
	poll(t, 5*time.Second, 1, membersFunc)

	// Let's start first KV again. It is not configured to join the cluster, but KV2 is rejoining.
	mkv1 = NewKV(cfg1, log.NewNopLogger(), &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv1))
	defer services.StopAndAwaitTerminated(context.Background(), mkv1) //nolint:errcheck

	poll(t, 5*time.Second, 2, membersFunc)
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

	cfg := KVConfig{}
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

	cfg := KVConfig{}
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
		BindAddrs: []string{"localhost"},
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
	cfg.JoinMembers = []string{fmt.Sprintf("127.0.0.1:%d", mkv1.GetListeningPort())}

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

func marshalKeyValuePair(t *testing.T, key string, codec codec.Codec, value interface{}) []byte {
	data, err := codec.Encode(value)
	require.NoError(t, err)

	kvp := KeyValuePair{Key: key, Codec: codec.CodecID(), Value: data}
	data, err = kvp.Marshal()
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

// poll repeatedly evaluates condition until we either timeout, or it succeeds.
func poll(t testing.TB, d time.Duration, want interface{}, have func() interface{}) {
	t.Helper()

	deadline := time.Now().Add(d)
	for {
		if time.Now().After(deadline) {
			break
		}
		if reflect.DeepEqual(want, have()) {
			return
		}
		time.Sleep(d / 100)
	}
	h := have()
	if !reflect.DeepEqual(want, h) {
		t.Fatalf("expected %v, got %v", want, h)
	}
}

type testLogger struct {
}

func (l testLogger) Log(keyvals ...interface{}) error {
	return nil
}

type dnsProviderMock struct {
	resolved []string
}

func (p *dnsProviderMock) Resolve(ctx context.Context, addrs []string) error {
	p.resolved = addrs
	return nil
}

func (p dnsProviderMock) Addresses() []string {
	return p.resolved
}
