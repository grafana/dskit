package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
)

const (
	ringKey = "ring-load-test"
)

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)

	// CLI flags.
	consulAddr := flag.String("consul.address", "localhost:8500", "Consul host and port.")
	numClients := flag.Int("test.num-clients", 1, "Number of ring clients to run. Each client watches the ring key on Consul.")
	numLifecyclers := flag.Int("test.num-lifecyclers", 1, "Number of ring lifecyclers to run.")
	numLifecyclersHeartbeating := flag.Int("test.num-lifecyclers-heartbeating", 0, "Number of ring lifecyclers heartbeating the ring. If 0, all lifecyclers do.")
	numTokens := flag.Int("test.num-tokens", 512, "Number of tokens each lifecycler registers.")
	heartbeatPeriod := flag.Duration("test.heartbeat-period", 5*time.Second, "How frequently each lifecycler heartbeats the ring.")
	deleteRingAtStartup := flag.Bool("test.delete-ring-at-startup", false, "True to delete the ring key on Consul at startup.")

	flag.Parse()

	// Get the hostname (used to compute unique instance IDs).
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(logger).Log("msg", "unable to get hostname", "err", err)
		os.Exit(1)
	}

	// If 0 means all lifecyclers are heartbeating the ring.
	if *numLifecyclersHeartbeating == 0 {
		*numLifecyclersHeartbeating = *numLifecyclers
	}

	if *deleteRingAtStartup {
		cfg := consul.Config{Host: *consulAddr}
		client, err := consul.NewClient(cfg, nil, logger, nil)
		if err != nil {
			level.Error(logger).Log("msg", "failed to create Consul client", "err", err)
			os.Exit(1)
		}

		if err := client.Delete(context.Background(), ringKey); err != nil {
			if err != nil {
				level.Error(logger).Log("msg", "failed to delete ring key at startup", "err", err)
				os.Exit(1)
			}
		}

		level.Info(logger).Log("msg", "deleted ring key")
	}

	// Run lifecyclers.
	level.Info(logger).Log("msg", "starting lifecyclers")
	var lifecyclers []services.Service

	for i := 0; i < *numLifecyclers; i++ {
		cfg := ring.LifecyclerConfig{}
		flagext.DefaultValues(&cfg)
		cfg.ID = fmt.Sprintf("lifecycler-%s-%d", hostname, i)
		cfg.Addr = fmt.Sprintf("127.0.0.%d", i)
		cfg.JoinAfter = 0
		cfg.FinalSleep = 0
		cfg.NumTokens = *numTokens
		cfg.RingConfig.KVStore.Consul.Host = *consulAddr

		if i < *numLifecyclersHeartbeating {
			cfg.HeartbeatPeriod = *heartbeatPeriod
		} else {
			cfg.HeartbeatPeriod = 0
		}

		// TODO can't be configured via CLI.
		cfg.RingConfig.KVStore.Consul.CasRetryDelay = time.Second
		cfg.RingConfig.KVStore.Consul.MaxCasRetries = 100

		lifecycler, err := ring.NewLifecycler(cfg, nil, ringKey, ringKey, false, log.With(logger, "lifecycler", i), nil)
		if err != nil {
			level.Error(logger).Log("msg", "failed to create lifecycler", "err", err)
			os.Exit(1)
		}

		if err := services.StartAndAwaitRunning(context.Background(), lifecycler); err != nil {
			level.Error(logger).Log("msg", "failed to start lifecycler", "err", err)
			os.Exit(1)
		}

		lifecyclers = append(lifecyclers, lifecycler)

		// Slow down the startup.
		time.Sleep(*heartbeatPeriod / time.Duration(*numLifecyclers))
	}

	// Run clients.
	level.Info(logger).Log("msg", "starting clients")
	var clients []services.Service

	for i := 0; i < *numClients; i++ {
		cfg := ring.Config{}
		flagext.DefaultValues(&cfg)

		client, err := ring.New(cfg, ringKey, ringKey, log.With(logger, "client", i), nil)
		if err != nil {
			level.Error(logger).Log("msg", "failed to create client", "err", err)
			os.Exit(1)
		}

		if err := services.StartAndAwaitRunning(context.Background(), client); err != nil {
			level.Error(logger).Log("msg", "failed to start client", "err", err)
			os.Exit(1)
		}

		clients = append(clients, client)

		// Slow down the startup.
		time.Sleep(*heartbeatPeriod / time.Duration(*numClients))
	}

	level.Info(logger).Log("msg", fmt.Sprintf("started %d lifecyclers (%d of which heartbeating the ring) and %d clients", *numLifecyclers, *numLifecyclersHeartbeating, *numClients))

	// Periodically run a health check and print statistics.
	for {
		// Ensure all lifecyclers and clients are up and running.
		if err := checkServicesHealth(lifecyclers); err != nil {
			level.Error(logger).Log("msg", "found an unhealthy lifecycler", "err", err)
		}
		if err := checkServicesHealth(clients); err != nil {
			level.Error(logger).Log("msg", "found an unhealthy client", "err", err)
		}

		printCASStatistics(logger)

		time.Sleep(*heartbeatPeriod)
	}
}

func printCASStatistics(logger log.Logger) {
	ops := consul.GetCASStats()
	if len(ops) == 0 {
		return
	}

	minClientDuration := time.Duration(math.MaxInt64)
	maxClientDuration := time.Duration(math.MinInt64)
	sumClientDuration := time.Duration(0)
	minConsulDuration := time.Duration(math.MaxInt64)
	maxConsulDuration := time.Duration(math.MinInt64)
	sumConsulDuration := time.Duration(0)
	maxDataSize := 0

	for _, op := range ops {
		sumClientDuration += op.ClientCASDuration
		sumConsulDuration += op.ConsulCASDuration

		if op.ClientCASDuration < minClientDuration {
			minClientDuration = op.ClientCASDuration
		}
		if op.ClientCASDuration > maxClientDuration {
			maxClientDuration = op.ClientCASDuration
		}
		if op.ConsulCASDuration < minConsulDuration {
			minConsulDuration = op.ConsulCASDuration
		}
		if op.ConsulCASDuration > maxConsulDuration {
			maxConsulDuration = op.ConsulCASDuration
		}
		if op.DataSize > maxDataSize {
			maxDataSize = op.DataSize
		}
	}

	level.Info(logger).Log("msg", "operations", "CAS()", len(ops), "data size (bytes)", maxDataSize)
	level.Info(logger).Log("msg", "consul.CAS()", "avg", sumConsulDuration / time.Duration(len(ops)), "min", minConsulDuration, "max", maxConsulDuration)
	level.Info(logger).Log("msg", "client.CAS()", "avg", sumClientDuration / time.Duration(len(ops)), "min", minClientDuration, "max", maxClientDuration)
}

func checkServicesHealth(list []services.Service) error {
	numUnhealthy := 0

	for _, service := range list {
		if service.State() != services.Running {
			numUnhealthy++
		}
	}

	if numUnhealthy > 0 {
		fmt.Errorf("%d unhealthy", numUnhealthy)
	}

	return nil
}