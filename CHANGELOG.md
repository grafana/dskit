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
* [ENHANCEMENT] Add middleware package. #38
* [ENHANCEMENT] Add the ring package #45
* [ENHANCEMENT] Add limiter package. #41
* [ENHANCEMENT] Add grpcclient, grpcencoding and grpcutil packages. #39
* [ENHANCEMENT] Replace go-kit/kit/log with go-kit/log. #52
* [ENHANCEMENT] Add spanlogger package. #42
* [ENHANCEMENT] Add runutil.CloseWithLogOnErr function. #58
* [ENHANCEMENT] Optimise memberlist receive path when used as a backing store for rings with a large number of members. #76 #77 #84 #91
* [ENHANCEMENT] Memberlist: prepare the data to send on the write before starting counting the elapsed time for `-memberlist.packet-write-timeout`, in order to reduce chances we hit the timeout when sending a packet to other node. #89
* [ENHANCEMENT] flagext: for cases such as `DeprecatedFlag()` that need a logger, add RegisterFlagsWithLogger. #80
* [ENHANCEMENT] Implement kubernetes KV store client. #94
* [BUGFIX] spanlogger: Support multiple tenant IDs. #59
* [BUGFIX] Memberlist: fixed corrupted packets when sending compound messages with more than 255 messages or messages bigger than 64KB. #85
