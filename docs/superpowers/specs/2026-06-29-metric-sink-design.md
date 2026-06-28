# Metric Sink Design Spec

## Goal

Add optional push-style metric reporting to `pkg/timewheel` and `pkg/executor`
without adding observability dependencies or moving user callbacks into hot
paths.

The existing `Metrics()` methods remain the primary pull API. `MetricSink`
extends that API for applications that prefer periodic snapshots delivered to
their own Prometheus, OpenTelemetry, logging, or in-house adapters.

## Motivation

`timewheel.go` already records useful counters:

- timer scheduling, expiration, cancellation, rejection, panic, pending size,
  command queue depth, bucket offers, bucket expirations, and max bucket delay.
- executor submission, completion, rejection, panic, queue depth, and worker
  count.

These values are important in production because they show saturation and
latency pressure before delayed tasks visibly fail. A pull-only API is correct
but pushes repetitive ticker setup into every application. A sink option gives
users a standard integration point while keeping the library dependency-free.

## Chosen Approach

Use a snapshot-level optional sink.

```go
// package timewheel
type MetricSink interface {
    ObserveTimerMetrics(Metrics)
}

func WithMetricSink(sink MetricSink) Option
func WithMetricReportInterval(interval time.Duration) Option
```

```go
// package executor
type MetricSink interface {
    ObservePoolMetrics(Metrics)
}

func WithMetricSink(sink MetricSink) Option
func WithMetricReportInterval(interval time.Duration) Option
```

Each package owns its own sink interface because each package owns its own
`Metrics` type. Method names include the observed component so one concrete
application sink can implement both interfaces without Go method-overload
conflicts.

## Lifecycle

No reporter goroutine is started unless `WithMetricSink` is configured.

When a sink is configured:

1. The constructor validates sink and interval options.
2. The component starts one reporter goroutine after construction succeeds.
3. The reporter immediately emits one snapshot.
4. The reporter emits later snapshots on a `time.Ticker`.
5. The reporter exits when the component shutdown channel closes.

`WithMetricReportInterval` defaults to `10s` when a sink is present and the user
does not override it. `WithMetricSink(nil)` and non-positive intervals return
`ErrInvalid`.

## Safety And Performance

Sink callbacks must never run from scheduling, bucket movement, worker dispatch,
or worker execution hot paths. The reporter goroutine reads `Metrics()` snapshots
using atomic loads and invokes the sink outside the core execution path.

Sink panics are recovered and ignored. Observability adapters must not stop
timers or executor pools.

Sink implementations must be fast and concurrency-safe. The same sink instance
may receive snapshots from a timer, boss pool, and worker pool concurrently.

## Non-Goals

- Do not add Prometheus, OpenTelemetry, or logging dependencies.
- Do not emit event-level callbacks for every counter increment.
- Do not change existing metric counters from snapshot counters into deltas.
- Do not make sink failures affect scheduling or task execution.
- Do not start reporter goroutines when no sink is configured.

## Testing

Tests must cover:

- nil sink validation.
- non-positive report interval validation.
- executor sink receives pool snapshots.
- timer sink receives timer snapshots.
- sink panic does not stop the reporter.
- reporter goroutines stop after component shutdown.
- existing `goleak`, race, and CI checks remain clean.

## Documentation

Documentation must state that metric sinks are optional, snapshot-level,
dependency-free, panic-isolated, and expected to be fast and concurrency-safe.

README and examples should include a small metrics example showing one sink type
implementing both `ObserveTimerMetrics` and `ObservePoolMetrics`.
