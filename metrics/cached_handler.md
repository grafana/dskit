# Cached Prometheus metrics handler

Use `CachedHandlerFor` with the same gatherer and options as
`promhttp.HandlerFor`, plus a cache duration:

```go
handler := metrics.CachedHandlerFor(registry, promhttp.HandlerOpts{
    EnableOpenMetrics: true,
}, 15*time.Second)
```

The dskit server exposes the same behavior through
`-server.metrics-cache-duration=15s`, or YAML:

```yaml
server:
  metrics_cache_duration: 15s
```

The default is `0s` (disabled). The server rejects negative durations. The
standalone handler disables caching for any non-positive duration.

Successful GET responses are cached separately for up to eight distinct
`Accept`/`Accept-Encoding` combinations using an LRU. Content negotiation,
compression, escaping, created samples, error handling and process start headers
remain delegated to `promhttp`. Requests with query parameters (including
`name[]` filtering) and methods other than GET bypass the cache. Collection and
encoding errors are not cached, including partial responses with status 200,
except with `ContinueOnError`: its partial responses are what every uncached
scrape would return, so they are cached too.

Refreshes run on demand, without a background timer. Requests for the same
representation that arrive during a refresh share its result, even when the
refresh outlives the TTL; different representations refresh independently. A
refresh continues when the request that started it is canceled. Timeout options
apply only to waiting for a refresh; cache hits are written directly. The cached
path does not apply `MaxRequestsInFlight`, since at most one refresh runs per
representation. The TTL starts before gathering, so a slow collection does not
extend the age of the cached data. Failed refreshes do not serve expired data.

Choose the duration with scrape intervals, short-window rates and alerting in
mind: the cache may repeat a metric value for up to the configured duration.
Different representations have separate snapshots and refresh schedules. A
single representation scraped 20 times per minute with a 15-second TTL requires
four collections per minute in the evenly spaced steady state.

The cache retains encoded bodies (including compression), so memory grows with
payload size and the number of variants. Buffer capacities are trimmed before
retention; refreshes temporarily require both the old snapshot and the new
collection/encoding working set. Cached responses still consume network
bandwidth on every scrape.

## Benchmark

The synthetic benchmark populates 1,000 or 8,000 tenants with one histogram and
15 counter vectors. The 8,000-tenant fixture exposes 144,000 text samples. It
measures gathering, encoding, compression and response handling; writes go to a
discard writer, so network transfer and scraper parsing are excluded. Setup and
cache warmup are excluded from the measurements. A simulated clock uses a 3-second
interval with uniform ±3-second jitter (0–6-second gaps), averaging 20 scrapes per
minute. Cached and uncached cases replay the same fixed-seed request schedule,
generated before timing starts. Cache refreshes are included, and their frequency
depends on the jittered arrivals rather than a fixed number of requests. With
`-benchtime=20x`, the fixed schedule spans approximately 57 seconds and includes
three measured refreshes after untimed warmup.

```sh
go test ./metrics -run '^$' -bench '^BenchmarkCachedHandlerFor$' -benchmem -benchtime=20x -count=6
```

Measured cached-versus-uncached results are included in [PR #1128](https://github.com/grafana/dskit/pull/1128).
