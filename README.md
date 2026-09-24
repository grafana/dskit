# Grafana Dskit

This library contains utilities that are useful for building distributed
services, including:
 - Exponential [backoff](https://github.com/grafana/dskit/tree/main/backoff) for retries.
 - A common [cache](https://github.com/grafana/dskit/tree/main/cache) API and Memcached implementation.
 - [Hedging](https://github.com/grafana/dskit/tree/main/hedging), sending extra duplicate requests to improve the chance that one succeeds.
 - A common [key-value](https://github.com/grafana/dskit/tree/main/kv) API, implemented for Consul, Etcd and Memberlist.
 - RPC [middlewares](https://github.com/grafana/dskit/tree/main/middleware), for metrics, logging, etc.
 - A [services model](https://github.com/grafana/dskit/tree/main/services), to manage start-up and shut-down.

## Packages

Some of the most commonly used packages are:
 - [ring](https://github.com/grafana/dskit/tree/main/ring): consistent hashing rings shared between instances via a key-value store, with lifecyclers to join and heartbeat them.
 - [kv](https://github.com/grafana/dskit/tree/main/kv): a key-value client API with CAS and Watch operations, implemented for Consul, Etcd and Memberlist.
 - [services](https://github.com/grafana/dskit/tree/main/services): a service lifecycle model (New, Starting, Running, Stopping, Terminated, Failed) inspired by Google Guava.
 - [server](https://github.com/grafana/dskit/tree/main/server): an instrumented HTTP and gRPC server with common initialization, including TLS support.
 - [grpcclient](https://github.com/grafana/dskit/tree/main/grpcclient): gRPC client configuration with retries, backoff, rate limiting, compression and TLS.

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
- Pull request titles must follow [Conventional Commits](https://www.conventionalcommits.org/) format.

## Release History

This project uses conventional commit messages to maintain a clear history of changes. No separate changelog is maintained - please refer to the [commit history](https://github.com/grafana/dskit/commits/main) for information about releases and changes.

## License

[Apache 2.0 License](https://github.com/grafana/dskit/blob/main/LICENSE)
