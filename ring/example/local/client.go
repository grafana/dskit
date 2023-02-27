package main

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
)

func main() {
	ctx := context.Background()

	// start memberlist service.
	memberlistsvc := SimpleMemberlistKV("127.0.0.3", 0, []string{"127.0.0.1"})
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

	ringsvc, err := SimpleRing(client)
	if err != nil {
		panic(err)
	}

	// start the ring service
	if err := services.StartAndAwaitRunning(ctx, ringsvc); err != nil {
		panic(err)
	}
	defer services.StopAndAwaitTerminated(ctx, ringsvc)

	var rr ring.ReadRing
	rr = ringsvc

	for {

		time.Sleep(1 * time.Second)

		replicas, err := rr.GetAllHealthy(ring.Read)
		if err != nil {
			fmt.Println("error when getting healthy instances", err)
			continue
		}

		fmt.Println("Peers:")
		for _, v := range replicas.Instances {
			fmt.Println("Addr", v.Addr, "Token count", len(v.Tokens))
		}
	}
}
