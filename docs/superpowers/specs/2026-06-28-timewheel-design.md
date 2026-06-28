# timewheel.go Design Spec

## Purpose

`timewheel.go` is a Go library for delayed task scheduling based on a
Kafka-style hierarchical timing wheel.

The library exposes two public packages:

- `pkg/timewheel`: delayed scheduling API.
- `pkg/executor`: bounded goroutine pool used by `timewheel`.

`timewheel.Timer` uses two executor pools:

- `bossPool`: advances the timing wheel, flushes buckets, cascades entries,
  and dispatches expired tasks.
- `workerPool`: executes user tasks.

## Layout

```text
.
├── go.mod
├── README.md
├── docs
│   ├── design.md
│   └── superpowers
│       └── specs
│           └── 2026-06-28-timewheel-design.md
├── pkg
│   ├── executor
│   │   ├── doc.go
│   │   ├── pool.go
│   │   ├── worker.go
│   │   ├── queue.go
│   │   ├── task.go
│   │   ├── option.go
│   │   ├── metrics.go
│   │   ├── reject_policy.go
│   │   ├── panic_handler.go
│   │   └── errors.go
│   └── timewheel
│       ├── doc.go
│       ├── timer.go
│       ├── scheduler.go
│       ├── command.go
│       ├── option.go
│       ├── task.go
│       ├── timeout.go
│       ├── metrics.go
│       ├── backpressure.go
│       └── errors.go
└── internal
    └── timingwheel
        ├── timing_wheel.go
        ├── bucket.go
        ├── bucket_delay_queue.go
        ├── task_entry.go
        ├── task_state.go
        └── clock.go
```

Package names are `timewheel`, `executor`, and `timingwheel`. File names are
lowercase, use underscores when needed, and describe responsibility.

## Public API

### Timer

```go
type userTask struct{}

func (userTask) Run(ctx context.Context) {
    // user task
}

func shutdownTimer(timer *timewheel.Timer) {
    _ = timer.Shutdown(context.Background())
}

timer, err := timewheel.New()
if err != nil {
    return err
}
defer shutdownTimer(timer)

timeout, err := timer.Schedule(ctx, time.Second, userTask{})
if err != nil {
    return err
}

cancelled := timeout.Cancel()
_ = cancelled
```

```go
func New(opts ...Option) (*Timer, error)

func (t *Timer) Schedule(
    ctx context.Context,
    delay time.Duration,
    task Task,
) (*Timeout, error)

func (t *Timer) Shutdown(ctx context.Context) error
func (t *Timer) Metrics() Metrics
func (t *Timer) Size() int64
```

`Schedule` uses `ctx` for command enqueue and backpressure waiting. Timeout
cancellation is controlled by `Timeout.Cancel`. The scheduled task receives the
context captured by `Schedule`.

### Task

```go
type Task interface {
    Run(ctx context.Context)
}

// TaskFunc is a named adapter for existing functions. Prefer a concrete Task
// type for reusable production tasks.
type TaskFunc func(ctx context.Context)

func (fn TaskFunc) Run(ctx context.Context)
```

Task failure is represented by panic recovery metrics.

### Timeout

```go
type Timeout struct {
    // unexported
}

func (t *Timeout) Cancel() bool
func (t *Timeout) IsCancelled() bool
func (t *Timeout) IsExpired() bool
```

`Cancel` is logical cancellation. Physical removal from a bucket is handled by
the scheduler loop. `IsExpired` reports that the timeout has been accepted for
worker execution.

### Options

```go
timewheel.New(
    timewheel.WithTick(time.Millisecond),
    timewheel.WithBucketCount(512),
    timewheel.WithCommandCapacity(65_536),
    timewheel.WithMaxPending(1_000_000),
    timewheel.WithBackpressurePolicy(timewheel.BackpressureReject),
    timewheel.WithExpiredTaskPolicy(timewheel.ExpiredTaskReject),
    timewheel.WithExpiredTaskRetryDelay(time.Millisecond),
    timewheel.WithBoss(
        executor.WithWorkers(1),
        executor.WithQueueCapacity(16),
    ),
    timewheel.WithWorker(
        executor.WithWorkers(runtime.GOMAXPROCS(0)),
        executor.WithQueueCapacity(100_000),
    ),
)
```

External pools:

```go
timewheel.New(
    timewheel.WithBossPool(bossPool),
    timewheel.WithWorkerPool(workerPool),
)
```

`WithBoss` and `WithWorker` configure timer-owned pools. `WithBossPool` and
`WithWorkerPool` inject caller-owned pools.

## Configuration

Default timer settings:

```text
tick                  = 1 millisecond
bucketCount           = 512
commandCapacity       = 65_536
maxPending            = 1_000_000
backpressurePolicy    = BackpressureReject
expiredTaskPolicy     = ExpiredTaskReject
expiredTaskRetryDelay = tick
```

Default boss pool:

```text
name          = timewheel-boss
workers       = 1
queueCapacity = 16
rejectPolicy  = RejectPolicyReject
```

Default worker pool:

```text
name          = timewheel-worker
workers       = runtime.GOMAXPROCS(0)
queueCapacity = 100_000
rejectPolicy  = RejectPolicyReject
```

Pool ownership:

- Timer-owned pools are closed by `Timer.Shutdown`.
- Caller-owned pools remain running after `Timer.Shutdown`.
- A missing boss pool or worker pool is created internally.

Validation:

- `context.Context` parameters require non-nil values.
- `tick` must be greater than zero.
- `bucketCount` must be greater than zero.
- `commandCapacity` must be greater than zero.
- `maxPending` must be greater than zero.
- `expiredTaskRetryDelay` must be greater than zero when retry is enabled.
- `WithBoss` and `WithBossPool` are mutually exclusive.
- `WithWorker` and `WithWorkerPool` are mutually exclusive.

Startup:

- `New` creates missing internal pools.
- `New` submits one long-lived scheduler task to the boss pool.
- Scheduler submission failure closes timer-owned pools and returns an error.
- Caller-owned boss pools need capacity for the scheduler task.

## Executor

`pkg/executor` provides a bounded goroutine pool.

```go
pool, err := executor.New(
    executor.WithName("worker"),
    executor.WithWorkers(8),
    executor.WithQueueCapacity(100_000),
    executor.WithRejectPolicy(executor.RejectPolicyReject),
)
```

```go
func New(opts ...Option) (*Pool, error)

func (p *Pool) Execute(ctx context.Context, task Task) error
func (p *Pool) TryExecute(task Task) error
func (p *Pool) Shutdown(ctx context.Context) error
func (p *Pool) Metrics() Metrics
```

`Execute` follows the configured reject policy. `TryExecute` returns
`ErrSaturated` when the queue is full. The `ctx` passed to `Execute` controls
submission waiting.

Executor task API:

```go
type Task interface {
    Run()
}

// TaskFunc is a named adapter for existing functions. Prefer a concrete Task
// type for reusable production tasks.
type TaskFunc func()

func (fn TaskFunc) Run()
```

Executor validation:

- `context.Context` parameters require non-nil values.
- `workers` must be greater than zero.
- `queueCapacity` must be zero or greater.
- `RejectPolicyBlock` requires a context with cancellation or deadline.

Executor queue:

- Implemented by an unexported bounded channel wrapper in `queue.go`.
- Tracks queue depth, submissions, completions, rejections, and panics.
- Recovers panics at worker goroutine boundaries.

## Timing Wheel Internals

`internal/timingwheel` implements the Kafka-style timing-wheel algorithm.

Core types:

```go
type TimingWheel struct {
    // unexported
}

type Bucket struct {
    // intrusive task list
}

type BucketDelayQueue struct {
    // bucket-level delay queue
}

type TaskEntry struct {
    // task, deadline, state, list membership
}
```

Timing wheel fields:

```text
tick
bucketCount
interval = tick * bucketCount
currentTime
buckets
overflow
```

Scheduling rules:

- `delay <= 0`: due now.
- `deadline < currentTime + tick`: due now.
- `deadline < currentTime + interval`: add to current wheel bucket.
- otherwise: add to overflow timing wheel.

Delay queue rules:

- The delay queue stores buckets.
- A bucket is offered when its expiration changes.
- Expired buckets advance the timing-wheel clock.
- Bucket flush re-adds entries to the root timing wheel.
- Due entries are returned to the scheduler for worker dispatch.

Time source:

- Runtime stores deadlines and bucket expirations as Unix milliseconds derived from `time.Time`.
- `internal/timingwheel` provides `ManualClock` for focused internal tests; the public timer uses `RealClock`.

Single-writer invariant:

- The scheduler loop mutates the timing wheel.
- Public methods communicate through commands and timeout state atomics.
- Buckets, bucket lists, and delay queues are owned by the scheduler loop.

## Scheduler

The scheduler loop runs as one long-lived task in `bossPool`.

Schedule path:

```text
Timer.Schedule
  -> validate arguments
  -> reserve pending quota
  -> create TaskEntry and Timeout
  -> enqueue schedule command
  -> return Timeout
```

If command enqueue fails after quota reservation, the quota is released before
returning the error. Non-positive delays are normalized to due-now schedules.

Scheduler loop:

```text
loop
  -> drain schedule commands
  -> add scheduled entries to TimingWheel
  -> compute delay until next bucket expiration
  -> wait for command, shutdown, or bucket expiration
  -> poll expired bucket
  -> advance timing-wheel clock
  -> flush bucket
  -> re-add entries or collect due entries
  -> dispatch due entries with workerPool.TryExecute
```

The loop uses reusable timers for bucket waiting.

Worker dispatch:

```text
due entry
  -> claim scheduled entry as dispatching
  -> workerPool.TryExecute(wrapper task)
  -> success: mark expired and release pending quota
  -> ErrSaturated + ExpiredTaskReject: mark rejected and release pending quota
  -> ErrSaturated + ExpiredTaskRetry: restore scheduled state and re-add
```

Worker saturation is handled by reject or retry policy while the scheduler
continues clock advancement and bucket flushing.

The wrapper task invokes the user task with the context captured by `Schedule`.
If user code panics, the wrapper records `PanickedTimeouts` and re-panics for
executor recovery.

Cancellation:

```text
Timeout.Cancel
  -> CAS scheduled to cancelled
  -> update metrics and pending count
```

`Cancel` returns false when the timeout is cancelled, rejected, expired, or
claimed for worker dispatch. A full command queue preserves logical
cancellation; bucket cleanup is performed during bucket flush.

## Backpressure

Backpressure boundaries:

- command queue
- pending timeout quota
- worker dispatch queue

### Command Queue

The command queue bounds schedule commands waiting for the scheduler.

```go
timewheel.WithCommandCapacity(65_536)
timewheel.WithBackpressurePolicy(timewheel.BackpressureReject)
```

Policies:

- `BackpressureReject`: return `ErrSaturated`.
- `BackpressureBlock`: wait for queue capacity, `ctx.Done()`, or timer shutdown.

### Pending Timeouts

`maxPending` bounds accepted but incomplete timeouts.

```go
timewheel.WithMaxPending(1_000_000)
```

Pending quota is released when a timeout becomes `cancelled`, `expired`, or
`rejected`. Retry keeps the timeout pending.

### Worker Dispatch

Expired tasks are submitted to `workerPool` with `TryExecute`.

Policies:

- `ExpiredTaskReject`: record rejection.
- `ExpiredTaskRetry`: update deadline to `now + expiredTaskRetryDelay` and
  re-add to the root timing wheel.

## Task State

Valid state transitions:

```text
scheduled -> cancelled
scheduled -> dispatching
dispatching -> scheduled   // retry
dispatching -> expired
dispatching -> rejected
```

State transitions use atomic compare-and-swap.

Metrics updates:

```text
schedule accepted  -> scheduled +1, pending +1
cancel success     -> cancelled +1, pending -1
expire success     -> expired +1, pending -1
worker rejected    -> rejected +1, pending -1
task panic         -> panicked +1
```

Cancellation:

- `Cancel` succeeds from `scheduled`.
- `Cancel` updates the state immediately.
- Scheduler cleanup removes cancelled entries from buckets.
- Bucket flush skips cancelled entries.

Expiration:

- `expired` is set after successful worker submission.
- Cancel after `dispatching`, `expired`, or `rejected` returns false.
- Worker rejection marks the task rejected or restores `scheduled` for retry.

## Shutdown

`Timer.Shutdown(ctx)`:

1. Mark the timer closed.
2. Reject new schedules.
3. Close the scheduler shutdown signal.
4. Drain accepted schedule commands.
5. Mark undispatched scheduled entries as cancelled and release pending quota.
6. Wait for the scheduler loop to exit.
7. Shut down timer-owned pools.
8. Leave caller-owned pools running.

`Pool.Shutdown(ctx)`:

1. Stop accepting tasks.
2. Close the task queue from the producer side.
3. Let workers drain accepted tasks.
4. Wait for workers to exit or return `ctx.Err()`.

Shutdown is idempotent.

## Errors

`pkg/timewheel`:

```go
var (
    ErrClosed    = errors.New("timewheel: closed")
    ErrSaturated = errors.New("timewheel: saturated")
    ErrInvalid   = errors.New("timewheel: invalid")
)
```

`pkg/executor`:

```go
var (
    ErrClosed    = errors.New("executor: closed")
    ErrSaturated = errors.New("executor: saturated")
    ErrInvalid   = errors.New("executor: invalid")
)
```

Returned errors wrap sentinel errors with operation context.

## Metrics

Timer metrics:

```text
ScheduledTimeouts
ExpiredTimeouts
CancelledTimeouts
RejectedSchedules
RejectedDispatches
PanickedTimeouts
PendingTimeouts
CommandQueueDepth
BucketOffers
BucketExpirations
MaxBucketDelay
```

Executor metrics:

```text
SubmittedTasks
CompletedTasks
RejectedTasks
PanickedTasks
QueueDepth
Workers
```

Metrics return immutable point-in-time snapshots.

## Testing

Executor tests:

- task execution
- saturated queue rejection
- blocking submission with context cancellation
- panic recovery metrics
- idempotent shutdown
- goroutine lifecycle

Timewheel tests:

- zero-delay schedule
- delayed schedule
- captured task context
- cancel before scheduler add
- cancel after bucket add
- cancel after expiration
- overflow cascade
- command queue saturation
- pending quota saturation
- worker saturation with scheduler progress
- retry policy dispatch
- task panic metrics
- caller-owned pools remain running
- idempotent shutdown
- shutdown cancellation of pending timeouts
- conflicting owned-pool and caller-owned-pool options
- goroutine lifecycle

Internal timing-wheel tests:

- bucket expiration update
- bucket delay queue ordering
- stale bucket offer skip
- entry bucket movement
- bucket flush reset
- overflow cascade

Verification commands:

```bash
go test ./...
go test -race ./...
```

Packages that start goroutines use `go.uber.org/goleak` in tests.

## Implementation Phases

1. Project scaffolding and package docs.
2. `pkg/executor`.
3. `internal/timingwheel`.
4. `pkg/timewheel` scheduler and public API.
5. Examples and benchmarks.
6. Race and leak verification.
