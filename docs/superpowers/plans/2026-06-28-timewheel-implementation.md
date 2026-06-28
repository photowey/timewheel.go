# Timewheel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `pkg/executor`, `internal/timingwheel`, and `pkg/timewheel` according to `docs/design.md`.

**Architecture:** `executor.Pool` is a generic bounded goroutine pool. `timewheel.Timer` owns one scheduler loop submitted to `bossPool` and dispatches expired tasks to `workerPool`. `internal/timingwheel` is single-writer and is mutated only by the scheduler loop.

**Tech Stack:** Go 1.26.4, standard library channels, `sync`, typed atomics, `container/heap`, table-driven tests, race detector.

---

## Execution Prerequisites

- Current branch is `main`; implementation requires an isolated worktree or explicit consent to work on `main`.
- `go.mod` is present and untracked; implementation commits should include it.
- `go.uber.org/goleak` is the planned test-only goroutine leak dependency. Adding it requires user approval before `go get`.

## File Structure

Create:

- `README.md`: package overview and minimal usage.
- `.gitignore`: local temporary files and worktree directories.
- `pkg/executor/doc.go`: package documentation.
- `pkg/executor/errors.go`: sentinel errors.
- `pkg/executor/task.go`: `Task`, `TaskFunc`.
- `pkg/executor/reject_policy.go`: reject policy enum.
- `pkg/executor/option.go`: pool options and validation.
- `pkg/executor/metrics.go`: atomic metrics and snapshot type.
- `pkg/executor/panic_handler.go`: panic callback.
- `pkg/executor/queue.go`: bounded task queue.
- `pkg/executor/worker.go`: worker loop.
- `pkg/executor/pool.go`: public pool API.
- `pkg/executor/pool_test.go`: executor behavior tests.
- `internal/timingwheel/clock.go`: clock abstraction and manual clock.
- `internal/timingwheel/task_state.go`: task state enum.
- `internal/timingwheel/task_entry.go`: task entry and state transitions.
- `internal/timingwheel/bucket.go`: intrusive bucket list.
- `internal/timingwheel/bucket_delay_queue.go`: bucket heap.
- `internal/timingwheel/timing_wheel.go`: hierarchical timing wheel.
- `internal/timingwheel/timing_wheel_test.go`: timing-wheel tests.
- `pkg/timewheel/doc.go`: package documentation.
- `pkg/timewheel/errors.go`: sentinel errors.
- `pkg/timewheel/task.go`: user task API.
- `pkg/timewheel/backpressure.go`: policy enums.
- `pkg/timewheel/metrics.go`: timer metrics.
- `pkg/timewheel/timeout.go`: timeout handle.
- `pkg/timewheel/option.go`: timer options and defaults.
- `pkg/timewheel/command.go`: scheduler command types.
- `pkg/timewheel/scheduler.go`: scheduler loop.
- `pkg/timewheel/timer.go`: public timer API.
- `pkg/timewheel/timer_test.go`: timer behavior tests.
- `pkg/timewheel/example_test.go`: executable example.

## Task 1: Project Scaffold

**Files:**
- Create: `.gitignore`
- Modify: `go.mod`
- Create: `README.md`

- [ ] **Step 1: Add scaffold files**

Create `.gitignore`:

```gitignore
.tmp/
.worktrees/
worktrees/
coverage.out
*.test
*.prof
```

Create `README.md`:

```markdown
# timewheel.go

`timewheel.go` provides a Kafka-style hierarchical timing wheel for delayed
task scheduling in Go.

## Packages

- `pkg/timewheel`: delayed scheduling API
- `pkg/executor`: bounded goroutine pool
```

- [ ] **Step 2: Verify module metadata**

Run:

```bash
go env GOVERSION GOMOD
go test ./...
```

Expected:

- `GOMOD` points at this repository's `go.mod`.
- `go test ./...` may report no packages before package files are added.

- [ ] **Step 3: Commit scaffold**

```bash
git add .gitignore go.mod README.md
git commit -m "chore: add project scaffold"
```

## Task 2: Executor Public API and Options

**Files:**
- Create: `pkg/executor/doc.go`
- Create: `pkg/executor/errors.go`
- Create: `pkg/executor/task.go`
- Create: `pkg/executor/reject_policy.go`
- Create: `pkg/executor/option.go`
- Create: `pkg/executor/metrics.go`
- Test: `pkg/executor/pool_test.go`

- [ ] **Step 1: Write failing option and task tests**

Add tests:

```go
func TestNewRejectsInvalidOptions(t *testing.T)
func TestTaskFuncRuns(t *testing.T)
func TestPoolMetricsSnapshotIsImmutable(t *testing.T)
```

Assertions:

- `executor.New(executor.WithWorkers(0))` returns an error matching `executor.ErrInvalid`.
- `executor.New(executor.WithQueueCapacity(-1))` returns an error matching `executor.ErrInvalid`.
- `TaskFunc` invokes the wrapped function exactly once.
- Mutating a returned `Metrics` value does not mutate pool internals.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
go test ./pkg/executor -run 'Test(NewRejectsInvalidOptions|TaskFuncRuns|PoolMetricsSnapshotIsImmutable)' -count=1
```

Expected: FAIL because the package does not exist.

- [ ] **Step 3: Implement minimal API**

Implement:

- `type Task interface { Run() }`
- `type TaskFunc func()`
- `func (fn TaskFunc) Run()`
- `type RejectPolicy int`
- `RejectPolicyReject`, `RejectPolicyBlock`
- `type Metrics struct`
- `type Option interface { apply(*options) error }`
- `WithName`, `WithWorkers`, `WithQueueCapacity`, `WithRejectPolicy`, `WithPanicHandler`
- `New(opts ...Option) (*Pool, error)` with validation and default values

- [ ] **Step 4: Run tests and verify GREEN**

Run:

```bash
go test ./pkg/executor -run 'Test(NewRejectsInvalidOptions|TaskFuncRuns|PoolMetricsSnapshotIsImmutable)' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/executor
git commit -m "feat(executor): add pool options and task API"
```

## Task 3: Executor Worker Pool

**Files:**
- Create: `pkg/executor/queue.go`
- Create: `pkg/executor/worker.go`
- Modify: `pkg/executor/pool.go`
- Modify: `pkg/executor/metrics.go`
- Test: `pkg/executor/pool_test.go`

- [ ] **Step 1: Write failing worker pool tests**

Add tests:

```go
func TestPoolExecuteRunsTask(t *testing.T)
func TestPoolTryExecuteRejectsWhenSaturated(t *testing.T)
func TestPoolExecuteBlocksUntilContextCancelled(t *testing.T)
func TestPoolRecoversPanicAndUpdatesMetrics(t *testing.T)
func TestPoolShutdownIsIdempotent(t *testing.T)
```

Key assertions:

- `Execute` runs submitted tasks.
- `TryExecute` returns `ErrSaturated` when queue capacity is exhausted.
- `Execute` with `RejectPolicyBlock` returns `context.Canceled` or `context.DeadlineExceeded`.
- Worker panic increments `PanickedTasks` and does not stop the pool.
- Repeated `Shutdown` calls return nil.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
go test ./pkg/executor -run 'TestPool' -count=1
```

Expected: FAIL because worker execution is not implemented.

- [ ] **Step 3: Implement bounded pool**

Implement:

- bounded channel queue
- `Execute(ctx, task)` with reject/block policy
- `TryExecute(task)`
- worker goroutine startup in `New`
- panic recovery in worker boundary
- atomic metrics
- `Shutdown(ctx)` with idempotent close and drain

- [ ] **Step 4: Run tests and verify GREEN**

Run:

```bash
go test ./pkg/executor -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/executor
git commit -m "feat(executor): implement bounded worker pool"
```

## Task 4: Timing Wheel Core

**Files:**
- Create: `internal/timingwheel/clock.go`
- Create: `internal/timingwheel/task_state.go`
- Create: `internal/timingwheel/task_entry.go`
- Create: `internal/timingwheel/bucket.go`
- Create: `internal/timingwheel/bucket_delay_queue.go`
- Create: `internal/timingwheel/timing_wheel.go`
- Test: `internal/timingwheel/timing_wheel_test.go`

- [ ] **Step 1: Write failing timing-wheel tests**

Add tests:

```go
func TestBucketDelayQueueOrdersByExpiration(t *testing.T)
func TestBucketDelayQueueSkipsStaleOffers(t *testing.T)
func TestBucketFlushResetsExpiration(t *testing.T)
func TestTimingWheelAddReturnsDueForExpiredDeadline(t *testing.T)
func TestTimingWheelAddsEntryToBucket(t *testing.T)
func TestTimingWheelCascadesOverflow(t *testing.T)
func TestTaskEntryStateTransitions(t *testing.T)
```

Key assertions:

- bucket heap returns earliest expiration first.
- stale bucket heap offers are ignored.
- bucket flush removes all entries and resets expiration.
- due entries are returned without bucket insertion.
- overflow wheel entries become due after clock advancement and flush.
- state transitions match `scheduled -> cancelled`, `scheduled -> dispatching`, `dispatching -> expired`, and retry path.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
go test ./internal/timingwheel -count=1
```

Expected: FAIL because package does not exist.

- [ ] **Step 3: Implement timing-wheel internals**

Implement:

- `Clock`, real clock, manual clock
- `TaskEntry` with intrusive list pointers and atomic state
- `Bucket` with add/remove/flush/set expiration
- typed heap-backed `BucketDelayQueue`
- hierarchical `TimingWheel` with overflow creation and `AdvanceClock`

- [ ] **Step 4: Run tests and verify GREEN**

Run:

```bash
go test ./internal/timingwheel -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/timingwheel
git commit -m "feat(timingwheel): implement hierarchical timing wheel"
```

## Task 5: Timer Options and Lifecycle

**Files:**
- Create: `pkg/timewheel/doc.go`
- Create: `pkg/timewheel/errors.go`
- Create: `pkg/timewheel/task.go`
- Create: `pkg/timewheel/backpressure.go`
- Create: `pkg/timewheel/metrics.go`
- Create: `pkg/timewheel/timeout.go`
- Create: `pkg/timewheel/option.go`
- Create: `pkg/timewheel/command.go`
- Create: `pkg/timewheel/timer.go`
- Test: `pkg/timewheel/timer_test.go`

- [ ] **Step 1: Write failing lifecycle tests**

Add tests:

```go
func TestNewUsesDefaultPools(t *testing.T)
func TestNewRejectsInvalidOptions(t *testing.T)
func TestShutdownIsIdempotent(t *testing.T)
func TestTimerDoesNotCloseCallerOwnedPools(t *testing.T)
```

Key assertions:

- `timewheel.New()` succeeds with internal pools.
- invalid tick, bucket count, command capacity, and max pending return `ErrInvalid`.
- repeated shutdown returns nil.
- injected worker pool accepts tasks after timer shutdown.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
go test ./pkg/timewheel -run 'Test(New|Shutdown|TimerDoesNotCloseCallerOwnedPools)' -count=1
```

Expected: FAIL because package does not exist.

- [ ] **Step 3: Implement timer construction**

Implement:

- public errors
- `Task`, `TaskFunc`
- backpressure enums
- timeout handle
- timer options and defaults
- internal and external pool ownership tracking
- command queue
- scheduler startup stub
- idempotent shutdown

- [ ] **Step 4: Run tests and verify GREEN**

Run:

```bash
go test ./pkg/timewheel -run 'Test(New|Shutdown|TimerDoesNotCloseCallerOwnedPools)' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/timewheel
git commit -m "feat(timewheel): add timer lifecycle"
```

## Task 6: Timer Scheduling, Cancellation, and Dispatch

**Files:**
- Modify: `pkg/timewheel/scheduler.go`
- Modify: `pkg/timewheel/timer.go`
- Modify: `pkg/timewheel/timeout.go`
- Modify: `pkg/timewheel/metrics.go`
- Test: `pkg/timewheel/timer_test.go`

- [ ] **Step 1: Write failing scheduling tests**

Add tests:

```go
func TestScheduleZeroDelayRunsTask(t *testing.T)
func TestScheduleDelayedRunsTask(t *testing.T)
func TestSchedulePassesCapturedContext(t *testing.T)
func TestCancelBeforeSchedulerAdd(t *testing.T)
func TestCancelAfterBucketAdd(t *testing.T)
func TestCancelAfterExpirationReturnsFalse(t *testing.T)
func TestOverflowCascadeRunsTask(t *testing.T)
```

Key assertions:

- zero and delayed tasks execute exactly once.
- task receives the context passed to `Schedule`.
- cancel returns true before dispatch and prevents execution.
- cancel returns false after dispatch.
- overflow deadlines cascade and execute.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
go test ./pkg/timewheel -run 'Test(Schedule|Cancel|Overflow)' -count=1
```

Expected: FAIL because scheduler behavior is incomplete.

- [ ] **Step 3: Implement scheduler loop**

Implement:

- schedule command enqueue with pending quota reservation
- backpressure reject/block behavior
- cancel cleanup command
- reusable timer wait for next bucket expiration
- bucket polling and timing-wheel advancement
- dispatch wrapper submission through `workerPool.TryExecute`
- timeout state and metrics updates

- [ ] **Step 4: Run tests and verify GREEN**

Run:

```bash
go test ./pkg/timewheel -run 'Test(Schedule|Cancel|Overflow)' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/timewheel
git commit -m "feat(timewheel): implement scheduling and cancellation"
```

## Task 7: Timer Backpressure, Retry, Panic Metrics

**Files:**
- Modify: `pkg/timewheel/scheduler.go`
- Modify: `pkg/timewheel/timer.go`
- Modify: `pkg/timewheel/metrics.go`
- Test: `pkg/timewheel/timer_test.go`

- [ ] **Step 1: Write failing backpressure tests**

Add tests:

```go
func TestScheduleRejectsWhenCommandQueueSaturated(t *testing.T)
func TestScheduleRejectsWhenMaxPendingReached(t *testing.T)
func TestWorkerSaturationDoesNotBlockScheduler(t *testing.T)
func TestExpiredTaskRetry(t *testing.T)
func TestTaskPanicUpdatesMetrics(t *testing.T)
```

Key assertions:

- command queue saturation returns `ErrSaturated`.
- max pending saturation returns `ErrSaturated`.
- saturated worker pool increments rejected dispatch metrics and scheduler keeps running.
- retry policy re-adds a dispatch-saturated task.
- user task panic increments `PanickedTimeouts`.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
go test ./pkg/timewheel -run 'Test(ScheduleRejects|WorkerSaturation|ExpiredTaskRetry|TaskPanic)' -count=1
```

Expected: FAIL because advanced policies are incomplete.

- [ ] **Step 3: Implement policies**

Implement:

- command queue saturation metrics
- pending quota boundary
- expired reject policy
- expired retry delay
- panic wrapper metrics
- snapshot metric fields

- [ ] **Step 4: Run tests and verify GREEN**

Run:

```bash
go test ./pkg/timewheel -run 'Test(ScheduleRejects|WorkerSaturation|ExpiredTaskRetry|TaskPanic)' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/timewheel
git commit -m "feat(timewheel): add backpressure policies"
```

## Task 8: Examples, Benchmarks, and Full Verification

**Files:**
- Create: `pkg/timewheel/example_test.go`
- Create: `pkg/timewheel/timer_bench_test.go`
- Create: `pkg/executor/pool_bench_test.go`
- Modify: `README.md`

- [ ] **Step 1: Add executable example**

Add:

```go
func ExampleTimer_Schedule()
```

The example creates a timer, schedules a zero-delay task, waits for completion,
and shuts down the timer.

- [ ] **Step 2: Add focused benchmarks**

Add:

```go
func BenchmarkPoolExecute(b *testing.B)
func BenchmarkTimerScheduleCancel(b *testing.B)
```

Benchmarks must call `b.ReportAllocs()` and use `b.Loop()`.

- [ ] **Step 3: Run package tests**

Run:

```bash
go test ./pkg/executor ./internal/timingwheel ./pkg/timewheel -count=1
```

Expected: PASS.

- [ ] **Step 4: Run full verification**

Run:

```bash
go test ./...
go test -race ./...
go test -bench=. -benchmem ./pkg/executor ./pkg/timewheel
```

Expected: tests pass; benchmarks report allocation metrics.

- [ ] **Step 5: Commit**

```bash
git add README.md pkg
git commit -m "test: add examples and benchmarks"
```

## Self-Review Checklist

- [ ] Public API matches `docs/design.md`.
- [ ] No user task runs in the scheduler loop.
- [ ] `executor.Pool` contains no timer-specific boss/worker concepts.
- [ ] Timer-owned pools close during `Timer.Shutdown`.
- [ ] Caller-owned pools remain open after `Timer.Shutdown`.
- [ ] Backpressure exists at command queue, pending quota, and worker dispatch.
- [ ] Timing-wheel internals remain under `internal/timingwheel`.
- [ ] All goroutines have an exit path and are waited on.
- [ ] All tests pass with `go test ./...`.
- [ ] Race detector passes with `go test -race ./...`.
