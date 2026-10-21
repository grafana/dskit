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
encoding errors are not cached, including partial responses with status 200.

Refreshes are coalesced and run on demand, without a background timer. Timeout
options also cover waiting for a refresh. Cache hits do not wait for an unrelated
refresh. The TTL starts before gathering, so a slow collection does not extend
the age of the cached data. Failed refreshes do not serve expired data.

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
cache warmup are excluded from the measurements. A simulated clock includes one
refresh per five requests rather than timing cache hits alone.

```sh
go test ./metrics -run '^$' -bench '^BenchmarkCachedHandlerFor$' -benchmem -benchtime=20x -count=5
```

Local results on Apple M3 Pro, darwin/arm64, Go 1.27.2, GOMAXPROCS 11.
Medians of five 20-request samples; uncached → 15-second cache:

- 1,000 tenants, identity: 13.39 → 2.69 ms/request (79.9% less elapsed time); 5.54 → 2.17 MB allocated/request; 68,635 → 13,732 allocations/request.
- 1,000 tenants, gzip: 17.26 → 3.94 ms/request (77.2% less elapsed time); 6.57 → 1.40 MB allocated/request; 68,652 → 13,736 allocations/request.
- 8,000 tenants, identity: 163.79 → 33.92 ms/request (79.3% less elapsed time); 46.57 → 17.81 MB allocated/request; 545,726 → 109,151 allocations/request.
- 8,000 tenants, gzip: 229.41 → 43.29 ms/request (81.1% less elapsed time); 47.49 → 10.10 MB allocated/request; 545,742 → 109,156 allocations/request.

Raw output: [cached_handler_benchmark.txt](cached_handler_benchmark.txt).
Allocated bytes measure churn, not retained heap. These are synthetic local
handler measurements, not estimates of whole-service CPU savings.
