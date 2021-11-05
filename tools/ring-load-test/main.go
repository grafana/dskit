package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
)

const (
	ringKey  = "ring-load-test"
	noPrefix = ""
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
	testDuration := flag.Duration("test.duration", time.Minute, "How long the test should run.")

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
		cfg.RingConfig.KVStore.Prefix = noPrefix

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
		cfg.KVStore.Consul.Host = *consulAddr
		cfg.KVStore.Prefix = noPrefix

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

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Keep track of all ops and print stats on shutdown.
	var allOps []consul.CasStats

	// Periodically run a health check and print statistics.
	startTime := time.Now()
	for ctx.Err() == nil && time.Since(startTime) < *testDuration {
		// Ensure all lifecyclers and clients are up and running.
		if err := checkServicesHealth(lifecyclers); err != nil {
			level.Error(logger).Log("msg", "found an unhealthy lifecycler", "err", err)
		}
		if err := checkServicesHealth(clients); err != nil {
			level.Error(logger).Log("msg", "found an unhealthy client", "err", err)
		}

		ops := consul.GetCASStats()
		if len(ops) >= 0 {
			printCASStatistics(ops, logger)
		}

		// Keep track of all ops.
		allOps = append(allOps, ops...)

		select {
			case <-time.After(*heartbeatPeriod):
			case <-ctx.Done():
		}
	}

	// Print summary stats.
	level.Info(logger).Log("msg", "summary")
	printCASStatistics(allOps, logger)
}

func printCASStatistics(ops[]consul.CasStats, logger log.Logger) {
	minConsulGetDuration := time.Duration(math.MaxInt64)
	maxConsulGetDuration := time.Duration(math.MinInt64)
	sumConsulGetDuration := time.Duration(0)
	minConsulCasDuration := time.Duration(math.MaxInt64)
	maxConsulCasDuration := time.Duration(math.MinInt64)
	sumConsulCasDuration := time.Duration(0)
	minClientDuration := time.Duration(math.MaxInt64)
	maxClientDuration := time.Duration(math.MinInt64)
	sumClientDuration := time.Duration(0)
	maxDataSize := 0
	numRetries := 0

	for _, op := range ops {
		sumConsulGetDuration += op.ConsulGetDuration
		sumConsulCasDuration += op.ConsulCASDuration
		sumClientDuration += op.ClientCASDuration

		if op.ConsulCASDuration < minConsulCasDuration {
			minConsulCasDuration = op.ConsulCASDuration
		}
		if op.ConsulCASDuration > maxConsulCasDuration {
			maxConsulCasDuration = op.ConsulCASDuration
		}

		if op.ConsulGetDuration < minConsulGetDuration {
			minConsulGetDuration = op.ConsulGetDuration
		}
		if op.ConsulGetDuration > maxConsulGetDuration {
			maxConsulGetDuration = op.ConsulGetDuration
		}

		if op.ClientCASDuration < minClientDuration {
			minClientDuration = op.ClientCASDuration
		}
		if op.ClientCASDuration > maxClientDuration {
			maxClientDuration = op.ClientCASDuration
		}

		if op.DataSize > maxDataSize {
			maxDataSize = op.DataSize
		}

		if op.Retry > 0 {
			numRetries++
		}
	}

	level.Info(logger).Log("msg", "operations", "CAS()", len(ops), "data size (bytes)", maxDataSize)
	level.Info(logger).Log("msg", "consul.Get()", "avg", sumConsulGetDuration/ time.Duration(len(ops)), "min", minConsulGetDuration, "max", maxConsulGetDuration)
	level.Info(logger).Log("msg", "consul.CAS()", "avg", sumConsulCasDuration/ time.Duration(len(ops)), "min", minConsulCasDuration, "max", maxConsulCasDuration)
	level.Info(logger).Log("msg", "client.CAS()", "avg", sumClientDuration / time.Duration(len(ops)), "min", minClientDuration, "max", maxClientDuration, "retries", numRetries)
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