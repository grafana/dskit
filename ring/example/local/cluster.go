//

package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"

	"github.com/go-kit/log"
)

var (
	bindaddr    string
	bindport    int
	joinmembers string
)

type Config struct {
	Lifecycler ring.LifecyclerConfig `yaml:"lifecycler"`
}

type Server struct {
	*services.BasicService
	lifecycler *ring.Lifecycler
}

func (s *Server) Flush() {
	// no-op
}

func (s *Server) TransferOut(ctx context.Context) error {
	// no-op
	return nil
}

func New(cfg *Config) (*Server, error) {
	s := &Server{}
	ctx := context.Background()

	lfc, err := ring.NewLifecycler(cfg.Lifecycler, s, "cluster", "ring", true, log.NewNopLogger(), prometheus.DefaultRegisterer)
	if err != nil {
		return nil, fmt.Errorf("error creating lifecycler %w", err)
	}

	if err := lfc.StartAsync(ctx); err != nil {
		return nil, fmt.Errorf("error starting lifecycler %w", err)
	}

	if err := lfc.AwaitRunning(ctx); err != nil {
		return nil, fmt.Errorf("error running lifecycler %w", err)
	}

	s.lifecycler = lfc

	return s, nil
}

func main() {
	flag.StringVar(&bindaddr, "bindaddr", "127.0.0.1", "bindaddr for this specific peer")
	flag.IntVar(&bindport, "bindport", 7946, "bindport for this specific peer")
	flag.StringVar(&joinmembers, "join-members", "", "comma separated peers list from existing cluster to join")

	flag.Parse()

	var joinmemberslice []string

	if joinmembers != "" {
		joinmemberslice = strings.FieldsFunc(joinmembers, func(r rune) bool {
			return r == ','
		})
	}

	cfg := &Config{
		Lifecycler: defaultConfig(bindaddr, bindport, joinmemberslice),
	}

	svr, err := New(cfg)
	if err != nil {
		panic(err)
	}

	svr.Flush()

	listener, err := net.Listen("tcp", bindaddr+":8100")
	if err != nil {
		panic(err)
	}

	fmt.Println("listening on ", listener.Addr())
	panic(http.Serve(listener, svr.lifecycler))
}

func defaultConfig(bindaddr string, bindport int, joinmembers []string) ring.LifecyclerConfig {
	lc := ring.LifecyclerConfig{}
	flagext.DefaultValues(&lc)

	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	kvconfig := memberlist.KVConfig{
		Codecs: []codec.Codec{ring.GetCodec()},
		TCPTransport: memberlist.TCPTransportConfig{
			BindPort:  bindport,
			BindAddrs: []string{bindaddr},
		},
		JoinMembers: joinmembers,
		NodeName:    bindaddr,
		// ClusterLabelVerificationDisabled: true,
	}
	// flagext.DefaultValues(&kvconfig)

	dnsProvider := dns.NewProvider(log.NewLogfmtLogger(os.Stdout), nil, dns.GolangResolverType)
	MemberlistKV := memberlist.NewKVInitService(&kvconfig, log.NewLogfmtLogger(os.Stdout), dnsProvider, nil)

	kv := kv.Config{Store: "memberlist"}
	kv.StoreConfig.MemberlistKV = MemberlistKV.GetMemberlistKV
	kv.RegisterFlagsWithPrefix("cluster", "collectors/", flag.NewFlagSet("", flag.PanicOnError))

	rc.KVStore = kv
	lc.RingConfig = rc

	data, err := yaml.Marshal(lc)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v\n", string(data))

	return lc
}
