# AGENTS.md

Guidance for coding agents working in this repository.

## Engineering guidance

- Before modifying code or tests, inspect nearby examples for naming, structure, and style. Follow explicit task and repository instructions; use local conventions for choices they leave open. During refactors, follow the conventions appropriate to the resulting code.
- Review the final diff for correctness, consistency with surrounding code, and compliance with the user's corrections. Check other code touched by the task for the same issue.
- Prefer compact doc comments focused on the essential contract, invariants, and non-obvious behavior. Keep implementation details in local comments near the code they explain, and avoid duplicating them in declaration comments.

## Testing

- Select checks according to the change and applicable repository requirements. Prefer affected package paths for focused validation; `./...` covers the root module.
- Run Go tests without `-count=1` by default so Go can reuse cached results. `make test` passes `-count 1`, which disables caching. Use uncached execution only when specifically required, and state why.
- For structural test refactors, preserve coverage, assertion strength, and required setup. Keep independent scenarios isolated.
- When using a filter to validate specific tests, verify that the intended cases were selected and produced the expected results, using verbose or structured output when needed. Accept valid cached results and flag unintended empty matches or skips; a successful exit status alone is insufficient.

## Commands

**Testing and Quality:**

```bash
go test -mod=readonly -tags netgo -race -timeout 30m ./... # Root-module tests, permitting cached results
make test              # Uncached root-module tests with race detection (30m timeout)
make test-benchmarks   # One-iteration benchmark smoke checks
make build-submodules  # Vet nested modules excluded by root ./... checks
make lint              # Run misspell and golangci-lint
make mod-check         # Download, verify, tidy, and check module files for diffs
```

`make mod-check` runs `go mod tidy` and can rewrite `go.mod` and `go.sum` files.

**Protocol Buffers:**

```bash
make protos           # Generate protobuf code
make check-protos     # Remove and regenerate protobuf outputs, then check for diffs
make clean-protos     # Remove generated proto files
```

**Requirements:** Use the Go version and toolchain declared in `go.mod`.

## Architecture

**Grafana Dskit** is a Go library of utilities for building distributed services, used in production by Mimir, Loki, Tempo, and Pyroscope.

### Core Components

**Service Infrastructure:**
- `services/` - Service lifecycle management (Google Guava-inspired)
- `modules/` - Module dependency system
- `server/` - HTTP/gRPC server with TLS support

**Distributed Primitives:**
- `ring/` - Consistent hashing ring for service discovery and sharding
- `kv/` - Unified key-value store (Consul, etcd, memberlist backends)
- `cache/` - Common cache API (Memcached, Redis, LRU)

**Reliability:**
- `backoff/` - Exponential backoff
- `hedging/` - Request hedging for improved latency
- `limiter/` - Rate limiting
- `gate/` - Circuit breaker
- `concurrency/` - Concurrency control

**RPC & Middleware:**
- `grpcclient/` - gRPC client with retry, rate limiting, instrumentation
- `middleware/` - HTTP/gRPC middleware for metrics, logging, auth, tracing
- `httpgrpc/` - HTTP over gRPC protocol

**Observability:**
- `tracing/` - OpenTracing + OpenTelemetry support
- `spanlogger/` - Structured logging with tracing
- `metrics/` - Prometheus helpers
- `log/` - Logging utilities

### Design Patterns

- **Service-oriented:** Everything follows explicit lifecycle states
- **Interface-driven:** Common interfaces with multiple implementations
- **Composable:** Mix and match components as needed
- **Import grouping:** stdlib → 3rd party → `github.com/grafana/dskit`

### Code Quality

- **golangci-lint** with custom rules in `.golangci.yml`
- **depguard** in golangci-lint enforces import restrictions
- Race detection enabled in tests
- Protocol buffers use gogo/protobuf for efficiency
