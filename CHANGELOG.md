## Changelog

* [CHANGE] Updated the minimum required Go version to 1.19 to ensure compatibility with OpenTelemetry version 1.14.0. #350
* [CHANGE] Change `WaitRingStability` and `WaitInstanceState` methods signature to rely on `ReadRing` instead. #251
* [CHANGE] Added new `-consul.cas-retry-delay` flag. It has a default value of `1s`, while previously there was no delay between retries. #178
* [CHANGE] Flagext: `DayValue` now always uses UTC when parsing or displaying dates. #71
* [CHANGE] Closer: remove the closer package since it's trivial to just copy/paste. #70
* [CHANGE] Memberlist: allow specifying address and port advertised to the memberlist cluster members by setting the following configuration: #37
  * `-memberlist.advertise_addr`
  * `-memberlist.advertise_port`
* [CHANGE] Removed global metrics for KV package. Making a KV object will now require a prometheus registerer that will be used to register all relevant KV class metrics. #22
* [CHANGE] Added CHANGELOG.md and Pull Request template to reference the changelog
* [CHANGE] Remove `cortex_` prefix for metrics registered in the ring. #46
* [CHANGE] Rename `kv/kvtls` to `crypto/tls`. #39
* [CHANGE] spanlogger: Take interface implementation for extracting tenant ID. #59
* [CHANGE] The `status_code` label on gRPC client metrics has changed from '200' and '500' to '2xx', '5xx', '4xx', 'cancel' or 'error'. #68
* [CHANGE] Memberlist: changed probe interval from `1s` to `5s` and probe timeout from `500ms` to `2s`. #90
* [CHANGE] Remove package `math`. #104
* [CHANGE] time: Remove time package. #103
* [CHANGE] grpcutil: Convert Resolver into concrete type. #105
* [CHANGE] grpcutil.Resolver.Resolve: Take a service parameter. #102
* [CHANGE] grpcutil.Update: Remove gRPC LB related metadata. #102
* [CHANGE] concurrency.ForEach: deprecated and reimplemented by new `concurrency.ForEachJob`. #113
* [CHANGE] grpcclient: Bump default `grpc-max-send-msg-size` flag to 100 Mb. #123
* [CHANGE] ring/client: It's now possible to set different value than `consul` as default KV store. #120
* [CHANGE] Lifecycler: Default value of lifecycler's `final-sleep` is now `0s` (i.e. no sleep). #121
* [CHANGE] Minor cosmetic changes in ring and memberlist HTTP status templates. #149
* [CHANGE] flagext.Secret: `value` field is no longer exported. Value can be read using `String()` method and set using `Set` method. #154
* [CHANGE] spanlogger.SpanLogger: Log the user ID from the context with the `user` label instead of `org_id`. #156
* [CHANGE] ring: removed the following metrics from ring client and lifecycler: #161
  * `member_ring_tokens_owned`
  * `member_ring_tokens_to_own`
  * `ring_tokens_owned`
  * `ring_member_ownership_percent`
* [CHANGE] Memberlist: `-memberlist.abort-if-join-fails` option now defaults to false.
* [CHANGE] Remove middleware package. #182
* [CHANGE] Memberlist: disabled TCP-based ping fallback, because dskit already uses a custom transport based on TCP. #194
* [CHANGE] Memberlist: KV store now fast-joins memberlist cluster before serving any KV requests. #195
* [CHANGE] Ring: remove duplicate state in NewOp func #203
* [CHANGE] Memberlist: Increase the leave timeout to 10x the connection timeout, so that we can communicate the leave to at least 1 node, if the first 9 we try to contact times out.
* [CHANGE] Netutil: removed debug log message "found network interfaces with private IP addresses assigned". #216
* [CHANGE] Runtimeconfig: Listener channels created by Manager no longer receive current value on every reload period, but only if any configuration file has changed. #218
* [CHANGE] Services: `FailureWatcher.WatchService()` and `FailureWatcher.WatchManager()` now panic if `FailureWatcher` is `nil`. #219
* [CHANGE] Memberlist: cluster label verification feature (`-memberlist.cluster-label` and `-memberlist.cluster-label-verification-disabled`) is now marked as stable. #222
* [CHANGE] Cache: Switch Memcached backend to use `github.com/grafana/gomemcache` repository instead of `github.com/bradfitz/gomemcache`. #248
* [CHANGE] Multierror: Implement `Is(error) bool`. This allows to use `multierror.MultiError` with `errors.Is()`. `MultiError` will in turn call `errors.Is()` on every error value. #254
* [CHANGE] Cache: Remove the `context.Context` argument from the `Cache.Store` method and rename the method to `Cache.StoreAsync`. #273
* [CHANGE] ring: make it harder to leak contexts when using `DoUntilQuorum`. #319
* [CHANGE] ring: `CountTokens()`, used for the calculation of ownership in the ring page has been changed in such a way that when zone-awareness is enabled, it calculates the ownership per-zone. Previously zones were not taken into account. #325
* [CHANGE] memberlist: `MetricsRegisterer` field has been removed from `memberlist.KVConfig` in favor of registrerer passed via into `NewKV` function. #327
* [CHANGE] gRPC client: use default connect timeout of 5s, and therefore enable default connect backoff max delay of 5s. #332
* [CHANGE] Remove `github.com/grafana/dskit/errors` in favour of Go's `errors` package. #357
* [CHANGE] Remove `grpcutil.IsGRPCContextCanceled()` in favour of `grpcutil.IsCanceled()`. #357
* [CHANGE] Remove `logrus` and `log.Interface`. #359
* [CHANGE] Remove jaeger-specific opentracing usage in `middleware.HTTPGRPCTracer`. #372
* [FEATURE] Cache: Add support for configuring a Redis cache backend. #268 #271 #276
* [FEATURE] Add support for waiting on the rate limiter using the new `WaitN` method. #279
* [FEATURE] Add `log.BufferedLogger` type. #338
* [FEATURE] Add `flagext.ParseFlagsAndArguments()` and `flagext.ParseFlagsWithoutArguments()` utilities. #341
* [FEATURE] Add `log.RateLimitedLogger` for limiting the rate of logging. The `logger_rate_limit_discarded_log_lines_total` metrics traces the total number of discarded log lines per level. #352
* [FEATURE] Add `middleware.HTTPGRPCTracer` for more detailed server-side tracing spans and tags on `httpgrpc.HTTP/Handle` requests
* [ENHANCEMENT] Add configuration to customize backoff for the gRPC clients.
* [ENHANCEMENT] Use `SecretReader` interface to fetch secrets when configuring TLS. #274
* [ENHANCEMENT] Add middleware package. #38
* [ENHANCEMENT] Add the ring package #45
* [ENHANCEMENT] Add limiter package. #41
* [ENHANCEMENT] Add grpcclient, grpcencoding and grpcutil packages. #39
* [ENHANCEMENT] Replace go-kit/kit/log with go-kit/log. #52
* [ENHANCEMENT] Add spanlogger package. #42
* [ENHANCEMENT] Add runutil.CloseWithLogOnErr function. #58
* [ENHANCEMENT] Add cache, gate and stringsutil packages. #239
* [ENHANCEMENT] Cache: add Redis support. #245
* [ENHANCEMENT] Optimise memberlist receive path when used as a backing store for rings with a large number of members. #76 #77 #84 #91 #93
* [ENHANCEMENT] Memberlist: prepare the data to send on the write before starting counting the elapsed time for `-memberlist.packet-write-timeout`, in order to reduce chances we hit the timeout when sending a packet to other node. #89
* [ENHANCEMENT] Memberlist: parallelize processing of messages received by memberlist. #110
* [ENHANCEMENT] flagext: for cases such as `DeprecatedFlag()` that need a logger, add RegisterFlagsWithLogger. #80
* [ENHANCEMENT] Added option to BasicLifecycler to keep instance in the ring when stopping. #97
* [ENHANCEMENT] Add WaitRingTokensStability function to ring, to be able to wait on ring stability excluding allowed state transitions. #95
* [ENHANCEMENT] Trigger metrics update on ring changes instead of doing it periodically to speed up tests that wait for certain metrics. #107
* [ENHANCEMENT] Add an HTTP hedging library. #115
* [ENHANCEMENT] Ring: Add ring page handler to BasicLifecycler and Lifecycler. #112
* [ENHANCEMENT] Lifecycler: It's now possible to change default value of lifecycler's `final-sleep`. #121
* [ENHANCEMENT] Memberlist: Update to latest fork of memberlist. #160
* [ENHANCEMENT] Memberlist: extracted HTTP status page handler to `memberlist.HTTPStatusHandler` which now can be instantiated with a custom template. #163
* [ENHANCEMENT] Lifecycler: add flag to clear tokens on shutdown. #167
* [ENHANCEMENT] ring: Added InstanceRegisterDelegate. #177
* [ENHANCEMENT] ring: optimize shuffle-shard computation when lookback is used, and all instances have registered timestamp within the lookback window. In that case we can immediately return origial ring, because we would select all instances anyway. #181
* [ENHANCEMENT] Runtimeconfig: Allow providing multiple runtime config yaml files as comma separated list file paths. #183
* [ENHANCEMENT] Memberlist: Add cluster label support to memberlist client. #187
* [ENHANCEMENT] Runtimeconfig: Don't unmarshal and merge runtime config yaml files if they haven't changed since last check. #218
* [ENHANCEMENT] ring: DoBatch now differentiates between 4xx and 5xx GRPC errors and keeps track of them separately. It only returns when there is a quorum of either error class. If your errors do not implement `GRPCStatus() *Status` from google.golang.org/grpc/status, then this change does not affect you. #201
* [ENHANCEMENT] Added `<prefix>.tls-min-version` and `<prefix>.tls-cipher-suites` flags to client configurations. #217
* [ENHANCEMENT] Concurrency: Add LimitedConcurrencySingleFlight to run jobs concurrently and with in-flight deduplication. #214
* [ENHANCEMENT] Add the ability to define custom gRPC health checks. #227
* [ENHANCEMENT] Import Bytes type, DeleteAll function and DNS package from Thanos. #228
* [ENHANCEMENT] Execute health checks in ring client pool concurrently. #237
* [ENHANCEMENT] Cache: Add the ability to use a custom memory allocator for cache results. #249
* [ENHANCEMENT] Add the metrics package to support per tenant metric aggregation. #258
* [ENHANCEMENT] Cache: Add Delete method to cache.Cache interface. #255
* [ENHANCEMENT] Cache: Add backward compatible metrics exposition for cache. #255
  * `<prefix>_cache_client_info{backend="[memcached|redis]",...}`
  * `<prefix>_cache_dns_failures_total{backend="[memcached|redis]",...}`
  * `<prefix>_cache_dns_lookups_total{backend="[memcached|redis]",...}`
  * `<prefix>_cache_dns_provider_results{backend="[memcached|redis]",...}`
  * `<prefix>_cache_getmulti_gate_duration_seconds_bucket{backend="[memcached|redis]",...}`
  * `<prefix>_cache_getmulti_gate_duration_seconds_count{backend="[memcached|redis]",...}`
  * `<prefix>_cache_getmulti_gate_duration_seconds_sum{backend="[memcached|redis]",...}`
  * `<prefix>_cache_getmulti_gate_queries_concurrent_max{backend="[memcached|redis]",...}`
  * `<prefix>_cache_getmulti_gate_queries_in_flight{backend="[memcached|redis]",...}`
  * `<prefix>_cache_hits_total{backend="[memcached|redis]",...}`
  * `<prefix>_cache_operation_data_size_bytes_bucket{backend="[memcached|redis]",...}`
  * `<prefix>_cache_operation_data_size_bytes_count{backend="[memcached|redis]",...}`
  * `<prefix>_cache_operation_data_size_bytes_sum{backend="[memcached|redis]",...}`
  * `<prefix>_cache_operation_duration_seconds_bucket{backend="[memcached|redis]",...}`
  * `<prefix>_cache_operation_duration_seconds_count{backend="[memcached|redis]",...}`
  * `<prefix>_cache_operation_duration_seconds_sum{backend="[memcached|redis]",...}`
  * `<prefix>_cache_operation_failures_total{backend="[memcached|redis]",...}`
  * `<prefix>_cache_operation_skipped_total{backend="[memcached|redis]",...}`
  * `<prefix>_cache_operations_total{backend="[memcached|redis]",...}`
  * `<prefix>_cache_requests_total{backend="[memcached|redis]",...}`
* [ENHANCEMENT] Lifecycler: Added `InstancesInZoneCount` and `InstancesCount` functions returning respectively the total number of instances in the ring and the number of instances in the ring that are registered in lifecycler's zone, updated during the last heartbeat period. #270
* [ENHANCEMENT] Memcached: add `MinIdleConnectionsHeadroomPercentage` support. It configures the minimum number of idle connections to keep open as a percentage of the number of recently used idle connections. If negative (default), idle connections are kept open indefinitely. #269
* [ENHANCEMENT] Memcached: Add support for using TLS or mTLS with Memcached based caching. #278
* [ENHANCEMENT] Ring: improve performance of shuffle sharding computation. #281
* [ENHANCEMENT] Add option to enable IPv6 address detection in ring and memberlist handling. #185
* [ENHANCEMENT] Ring: cache results of shuffle sharding with lookback where possible. #283 #295
* [ENHANCEMENT] BasicLifecycler: functions `ShouldKeepInstanceInTheRingOnShutdown` and `SetKeepInstanceInTheRingOnShutdown` for checking and setting whether instances should be kept in the ring or unregistered on shutdown have been added. #290
* [ENHANCEMENT] Ring: add `DoUntilQuorum` method. #293
* [ENHANCEMENT] Ring: add `ReplicationSet.ZoneCount()` method. #298
* [ENHANCEMENT] Ring: add request minimization to `DoUntilQuorum` method. #306
* [ENHANCEMENT] grpcclient: add `<prefix>.initial-stream-window-size` and `<prefix>.initial-connection-window-size` configuration flags to alter HTTP flow control options for a gRPC client. #312
* [ENHANCEMENT] Added `TokenGenerator` interface with `RandomTokenGenerator` (generating random tokens), and `SpreadMinimizingTokenGenerator` (generating tokens with almost even distribution) implementation. By default `RandomTokenGenerator` is used. #321
* [ENHANCEMENT] Lifecycler: Added `RingTokenGenerator` configuration that specifies the `TokenGenerator` implementation that is used for token generation. Default value is nil, meaning that `RandomTokenGenerator` is used. #323
* [ENHANCEMENT] BasicLifecycler: Added `RingTokenGenerator` configuration that specifies the `TokenGenerator` implementation that is used for token generation. Default value is nil, meaning that `RandomTokenGenerator` is used. #323
* [ENHANCEMENT] Ring: add support for hedging to `DoUntilQuorum` when request minimization is enabled. #330
* [ENHANCEMENT] Lifecycler: allow instances to register in ascending order of ids in case of spread minimizing token generation strategy. #326
* [ENHANCEMENT] Remove dependency on `github.com/weaveworks/common` package by migrating code to a corresponding package in `github.com/grafana/dskit`. #342
* [ENHANCEMENT] Add ability to pass TLS certificates and keys inline when configuring server-side TLS. #349 #363
* [ENHANCEMENT] Migrate `github.com/weaveworks/common/aws` and `github.com/weaveworks/common/test` packages to corresponding packages in `github.com/grafana/dskit`. #356
* [ENHANCEMENT] Ring: optionally emit logs and trace events in `DoUntilQuorum`. #361
* [ENHANCEMENT] metrics: Add `MetricFamilyMap.MinGauges` and `MetricFamiliesPerTenant.SendMinOfGauges` methods. #366
* [ENHANCEMENT] metrics: add `MatchesLabels`, `FindMetricsInFamilyMatchingLabels` and `FindHistogramWithNameAndLabels` test helper methods. #365
* [ENHANCEMENT] SpanLogger: much more efficient debug logging. #367
* [ENHANCMENT] server: clarify documentation for `-server.grpc-max-concurrent-streams` CLI flag. #369
* [BUGFIX] spanlogger: Support multiple tenant IDs. #59
* [BUGFIX] Memberlist: fixed corrupted packets when sending compound messages with more than 255 messages or messages bigger than 64KB. #85
* [BUGFIX] Ring: `ring_member_ownership_percent` and `ring_tokens_owned` metrics are not updated on scale down. #109
* [BUGFIX] Allow in-memory kv-client to support multiple codec #132
* [BUGFIX] Modules: fix a module waiting for other modules depending on it before stopping. #141
* [BUGFIX] Multi KV: fix panic when using function to modify primary KV store in runtime. #153
* [BUGFIX] Lifecycler: if the ring backend storage is reset, the instance adds itself back to the ring with an updated registration timestamp set to current time. #165
* [BUGFIX] Ring: fix bug where hash ring instances may appear unhealthy in the web UI even though they are not. #172
* [BUGFIX] Lifecycler: if existing ring entry is reused, ring is updated immediately, and not on next heartbeat. #175
* [BUGFIX] stringslicecsv: handle unmarshalling empty yaml string #206
* [BUGFIX] Memberlist: retry joining memberlist cluster on startup when no nodes are resolved. #215
* [BUGFIX] Ring status page: display 100% ownership as "100%", rather than "1e+02%". #231
* [BUGFIX] Memberlist: fix crash when methods from `memberlist.Delegate` interface are called on `*memberlist.KV` before the service is fully started. #244
* [BUGFIX] runtimeconfig: Fix `runtime_config_last_reload_successful` metric after recovery from bad config with exact same config hash as before. #262
* [BUGFIX] Ring status page: fixed the owned tokens percentage value displayed. #282
* [BUGFIX] Ring: prevent iterating the whole ring when using `ExcludedZones`. #285
* [BUGFIX] grpcclient: fix missing `.` in flag name for initial connection window size flag. #314
* [BUGFIX] ring.Lifecycler: Handle when previous ring state is leaving and the number of tokens has changed. #79
* [BUGFIX] memberlist metrics: fix registration of `memberlist_client_kv_store_count` metric and disable expiration of metrics exposed by memberlist library. #327
* [BUGFIX] middleware: fix issue where successful gRPC streaming requests are not always captured in metrics. #344
* [BUGFIX] Ring: `DoUntilQuorum` and `DoUntilQuorumWithoutSuccessfulContextCancellation` will now cancel the context for a zone as soon as first failure for that zone is encountered. #364
* [BUGFIX] `MetricFamiliesPerTenant.SendMinOfGauges` will no longer return 0 if some tenant doesn't have the gauge in their registry, and other tenants have positive values only. #368
* [BUGFIX] `MetricFamiliesPerTenant.SendMaxOfGauges` will no longer return 0 if some tenant doesn't have the gauge in their registry, and other tenants have negative values only. #368
* [BUGFIX] `MetricFamiliesPerTenant.SendMaxOfGaugesPerTenant` no longer includes tenants, which do not have specified gauge. #368
