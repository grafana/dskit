# Grafana Dskit

This library contains utilities that are useful for building distributed
services, including:
 - Exponential [backoff](https://github.com/grafana/dskit/tree/main/backoff) for retries.
 - A common [cache](https://github.com/grafana/dskit/tree/main/cache) API, implemented for Memcached and Redis.
 - [Hedging](https://github.com/grafana/dskit/tree/main/hedging), sending extra duplicate requests to improve the chance that one succeeds.
 - A common [key-value](https://github.com/grafana/dskit/tree/main/kv) API, implemented for Consul, Etcd and Memberlist.
 - RPC [middlewares](https://github.com/grafana/dskit/tree/main/middleware), for metrics, logging, etc.
 - A [services model](https://github.com/grafana/dskit/tree/main/services), to manage start-up and shut-down.

## Current state

This library is used at scale in production at Grafana Labs.
A number of packages were collected here from database-related projects:

- [Mimir]
- [Loki]
- [Tempo]
- [Pyroscope]

[Mimir]: https://github.com/grafana/mimir
[Loki]: https://github.com/grafana/loki
[Tempo]: https://github.com/grafana/tempo
[Pyroscope]: https://github.com/grafana/pyroscope

## Go version compatibility

This library aims to support at least the two latest Go minor releases.

## Contributing

If you're interested in contributing to this project:

- Start by reading the [Contributing guide](/CONTRIBUTING.md).

## License

[Apache 2.0 License](https://github.com/grafana/dskit/blob/main/LICENSE)
