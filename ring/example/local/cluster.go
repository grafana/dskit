//

package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"

	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
)

var (
	bindaddr   string
	bindport   int
	joinmember string
)

func main() {
	flag.StringVar(&bindaddr, "bindaddr", "127.0.0.1", "bindaddr for this specific peer")
	flag.IntVar(&bindport, "bindport", 7946, "bindport for this specific peer")
	flag.StringVar(&joinmember, "join-member", "", "peer addr that is part of existing cluster")

	flag.Parse()

	ctx := context.Background()

	joinmembers := make([]string, 0)
	if joinmember != "" {
		joinmembers = append(joinmembers, joinmember)
	}

	// start memberlist service.
	memberlistsvc := SimpleMemberlistKV(bindaddr, bindport, joinmembers)
	if err := services.StartAndAwaitRunning(ctx, memberlistsvc); err != nil {
		panic(err)
	}
	defer services.StopAndAwaitTerminated(ctx, memberlistsvc)

	store, err := memberlistsvc.GetMemberlistKV()
	if err != nil {
		panic(err)
	}

	client, err := memberlist.NewClient(store, ring.GetCodec())
	if err != nil {
		panic(err)
	}

	lfc, err := SimpleRingLifecycler(client, bindaddr, bindport)
	if err != nil {
		panic(err)
	}

	// start lifecycler service
	if err := services.StartAndAwaitRunning(ctx, lfc); err != nil {
		panic(err)
	}
	defer services.StopAndAwaitTerminated(ctx, lfc)

	listener, err := net.Listen("tcp", bindaddr+":8100")
	if err != nil {
		panic(err)
	}

	fmt.Println("listening on ", listener.Addr())

	mux := http.NewServeMux()
	mux.Handle("/ring", lfc)
	mux.Handle("/kv", memberlistsvc)

	panic(http.Serve(listener, mux))
}
