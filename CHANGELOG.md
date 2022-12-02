## Changelog

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
* [ENHANCEMENT] Add middleware package. #38
* [ENHANCEMENT] Add the ring package #45
* [ENHANCEMENT] Add limiter package. #41
* [ENHANCEMENT] Add grpcclient, grpcencoding and grpcutil packages. #39
* [ENHANCEMENT] Replace go-kit/kit/log with go-kit/log. #52
* [ENHANCEMENT] Add spanlogger package. #42
* [ENHANCEMENT] Add runutil.CloseWithLogOnErr function. #58
* [ENHANCEMENT] Add cache, gate and stringsutil packages. #239
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
