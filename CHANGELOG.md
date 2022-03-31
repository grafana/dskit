## Changelog

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
* [CHANGE] Lifecycler: It's now possible to change default value of lifecycler's `final-sleep`. #121
* [CHANGE] Minor cosmetic changes in ring and memberlist HTTP status templates. #149
* [CHANGE] flagext.Secret: `value` field is no longer exported. Value can be read using `String()` method and set using `Set` method. #154
* [ENHANCEMENT] Add middleware package. #38
* [ENHANCEMENT] Add the ring package #45
* [ENHANCEMENT] Add limiter package. #41
* [ENHANCEMENT] Add grpcclient, grpcencoding and grpcutil packages. #39
* [ENHANCEMENT] Replace go-kit/kit/log with go-kit/log. #52
* [ENHANCEMENT] Add spanlogger package. #42
* [ENHANCEMENT] Add runutil.CloseWithLogOnErr function. #58
* [ENHANCEMENT] Optimise memberlist receive path when used as a backing store for rings with a large number of members. #76 #77 #84 #91 #93
* [ENHANCEMENT] Memberlist: prepare the data to send on the write before starting counting the elapsed time for `-memberlist.packet-write-timeout`, in order to reduce chances we hit the timeout when sending a packet to other node. #89
* [ENHANCEMENT] flagext: for cases such as `DeprecatedFlag()` that need a logger, add RegisterFlagsWithLogger. #80
* [ENHANCEMENT] Added option to BasicLifecycler to keep instance in the ring when stopping. #97
* [ENHANCEMENT] Add WaitRingTokensStability function to ring, to be able to wait on ring stability excluding allowed state transitions. #95
* [ENHANCEMENT] Trigger metrics update on ring changes instead of doing it periodically to speed up tests that wait for certain metrics. #107
* [ENHANCEMENT] Add an HTTP hedging library. #115
* [ENHANCEMENT] Ring: Add ring page handler to BasicLifecycler and Lifecycler. #112
* [BUGFIX] spanlogger: Support multiple tenant IDs. #59
* [BUGFIX] Memberlist: fixed corrupted packets when sending compound messages with more than 255 messages or messages bigger than 64KB. #85
* [BUGFIX] Ring: `ring_member_ownership_percent` and `ring_tokens_owned` metrics are not updated on scale down. #109
* [BUGFIX] Allow in-memory kv-client to support multiple codec #132
* [BUGFIX] Modules: fix a module waiting for other modules depending on it before stopping. #141
* [BUGFIX] Multi KV: fix panic when using function to modify primary KV store in runtime. #153