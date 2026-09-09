# retry

Run an operation again until:
 - it succeeds
 - the decider gives up on a failure
 - the attempt budget is spent
 - the context is done.

The package owns the *mechanism*: an executor, a source of delays, a convention
for marking errors, and metrics. Deciding what to retry is left to the user.

```go
err := retry.Retry(ctx, client.Sync, retry.WithPredicate(isTransient))
```

Runnable examples can be found in [`example_test.go`](example_test.go).

## Deciding what to retry

Every failed attempt is handed to a decider, which says whether another attempt
has a chance. There are multiple ways to implement that policy, to be chosen by the user.

One way would be to mark the error where it is produced:

```go
err := retry.Retry(ctx, func(ctx context.Context) error {
	resp, err := c.send(ctx)
	if err != nil {
		return retry.MarkRetryable(err) // retry send errors
	}
	// ...

	if resp.StatusCode < 400 {
		return nil // success
	}

	statusErr := fmt.Errorf("sync: unexpected status %d", resp.StatusCode)
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return retry.MarkRetryableAfter(statusErr, parseRetryAfter(resp.Header))
	case resp.StatusCode >= 500:
		return retry.MarkRetryable(statusErr)
	default:
		return statusErr // 4xx: the request itself is wrong: don't retry, leave unmarked
	}
})
```

The knowledge of whether a failure is transient lives at the point of failure.

The default decider is `retry.Marked`, which reports whether an error is marked as
retryable; unmarked errors are non-retryable, so they get exactly one attempt.

`MarkRetryableAfter` is for the cases where the server sets a lower bound on the next
delay: the loop then waits `max(backoff delay, after)`. To guard against a very long
`Retry-After`, use a bounded context.

Another way would be to use a `func(error) bool` that can be wired straight in:

```go
retry.WithPredicate(isRetryableAPIError)
retry.WithPredicate(func(error) bool { return true }) // retry everything; the budget and ctx are the bounds
```

The last and more customizable way is using `WithDecider`. It returns `Decision{Retryable, After}` so
that policy can also raise the delay before the next attempt, typically a server's
`Retry-After` (check `ExampleWithDecider`).

The zero `Decision` gives up, so a decider that does not recognize an error
fails closed. That is the safe choice for work that is not idempotent. Reach for
a permissive predicate on read paths and start-up loops where not retrying is the
worse failure mode.

Marks and a call-site decider combine with a plain `if` (`ExampleMarked`), so a
call site that mixes its own marked errors with errors from a library it does not
own needs no combinator.

The package ships no built-in decider for HTTP or gRPC for now, but adding them in
the future could be considered.

## Backoffs and the budget

A `Backoff` says how long to wait after a given attempt; `WithMaxAttempts` says
how many attempts there are.

```go
type Backoff func(attempts int) time.Duration
```

`attempts` starts at `1`, after the first failure. Four backoffs are
built in, and `Jittered` spreads any of them:

```go
retry.Exponential(time.Second, 2, time.Minute) // Each attempt increases exponentially
retry.Linear(time.Second, 1, time.Minute)      // Each attempt increases linearly
retry.Uniform(0, 500*time.Millisecond)         // [min, max)
retry.Constant(500 * time.Millisecond)         // Each attempt waits for the same duration

retry.Jittered(retry.Exponential(time.Second, 2, time.Minute), 0.5)
```

`Jittered` shortens each delay by up to `fraction` of itself, drawn uniformly, so
that callers that failed together do not retry together:

| `fraction` | delay once the backoff reaches `max` |
|---|---|
| `0` | exactly `max` |
| `0.5` | uniform over `[max/2, max]` |
| `1` | uniform over `[0, max]` |

The default backoff is
`Jittered(Exponential(100*time.Millisecond, 2, 10*time.Second), 0.5)`.

The budget is how many attempts an operation gets in total, the first one included:

```go
retry.WithMaxAttempts(3)      // three attempts in total, initial + 2 retries
retry.WithUnlimitedAttempts() // until success or ctx error
```

The default budget is `10`, and no value of `n` turns it off by accident:
an explicit `WithUnlimitedAttempts` is the only way to do that.

## Reusing a Retrier

`retry.New` binds a set of options once, and the returned `*Retrier` runs any
number of concurrent operations under them (`ExampleNew`). Prefer it to the
package-level `retry.Retry(ctx, fn, opts...)` whenever the same options serve more
than one call: `retry.Retry` is shorthand for `retry.New(opts...).Retry(ctx, fn)`,
so it rebuilds the options every time.

## Options and defaults

Options apply in order, so a later one overrides an earlier one, and passing `nil` to
an option restores that option's default. The one worth knowing is `WithBackoff(nil)`:
it gives you the default jittered exponential backoff rather than no waits at all,
because retrying with no delay is a hot loop against a dependency that is already
failing. Ask for that explicitly with `retry.Constant(0)`.

This package registers no flags and reads no YAML for now. Turning the knobs a component
exposes into `retry.Option`s is that component's job.

## Metrics

By default, metrics are turned off, so passing no `WithMetrics`, or a nil `*Metrics`, records
nothing. Since Prometheus panics on duplicate registration it is recommended to build them once
per registry and pass them to every `Retrier` reporting to it:

```go
metrics := retry.NewMetrics(prometheus.WrapRegistererWithPrefix("mycomponent_", reg))
retry.Retry(ctx, func(_ context.Context) error { return nil }, retry.WithMetrics(metrics))
```

Names are unprefixed, so the caller owns the prefix, as above.

| Metric | Type | Labels | Answers |
|---|---|---|---|
| `retry_attempts_total` | counter | `operation` | are we retrying more than usual |
| `retry_duration_seconds` | histogram | `operation`, `outcome` | **is retrying helping**, and what it costs |

Every call reports exactly one `outcome`: `first_attempt`, `after_retry`,
`terminal`, `exhausted`, `deadline` (the next delay would have outlasted the
deadline) or `cancelled` (the context ended). Exactly one observation goes in per
completed operation, so `retry_duration_seconds_count` counts operations by how
they ended.

That makes the ratio `after_retry / (after_retry + exhausted)` the answer to whether
retrying is helping or only adding load, and the buckets the answer to what it costs.
Both are what to read when tuning a backoff or a budget.

The span measured is the whole of the caller's wait on `Retry`.

Weigh the cost before instrumenting a component with many operations: 13
buckets plus `_sum` and `_count` is 15 series per `outcome`, and `outcome`
has 6 values, so each operation is on the order of ~90 series.

## Exhaustion and nesting

The context is passed to your function unchanged and bounds the whole loop, waits
included. A wait that would outlast its deadline is not served out, because no
attempt could follow it. A context that is already done gets no attempt at all.

When the loop stops for a reason of its own, the returned error wraps two things, why
it stopped and the last failure, and both are reachable with `errors.Is`. Only a
terminal decision hands the failure back untouched:

```go
if errors.Is(err, retry.ErrExhausted) { ... }
```

| How the loop ended        | Returned error wraps                                            |
|---------------------------|-----------------------------------------------------------------|
| a spent budget            | `retry.ErrExhausted`, and the last failure                       |
| an unfulfillable deadline | `context.DeadlineExceeded`, and the last failure                 |
| an ended context          | `context.Cause(ctx)`, and the last failure if there was one      |
| the decider gave up       | nothing: the error comes back as `fn` left it (still `Marked`)    |

The retrier never marks or unmarks an error on its own, so a failure marked retryable
still reports as retryable once the loop has given up on it. How the loop ended is
what the table above reports, not the mark.

An outer loop has to look at the returned error before asking for another round.

Nesting is multiplicative: an inner budget of 3 inside an outer
budget of 5 is up to 15 calls, and the product of the delays. This package does
not police that.
