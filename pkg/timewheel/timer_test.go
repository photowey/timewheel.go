// Copyright © 2026-present The Timewheel.go Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package timewheel

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/photowey/timewheel.go/pkg/executor"
)

type timerInvalidOptionCase struct {
	name string
	opts []Option
}

func (tc timerInvalidOptionCase) run(t *testing.T) {
	timer, err := New(tc.opts...)
	if err == nil {
		shutdownTimer(t, timer)
		t.Fatal("expected error, got nil")
	}
	if timer != nil {
		t.Fatalf("expected nil timer, got %#v", timer)
	}
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got %v", err)
	}
}

type timerExecutorOptionCase struct {
	name string
	opts []Option
}

func (tc timerExecutorOptionCase) run(t *testing.T) {
	timer, err := New(tc.opts...)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if timer != nil {
		t.Fatalf("expected nil timer, got %#v", timer)
	}
	if !errors.Is(err, executor.ErrInvalid) {
		t.Fatalf("expected executor.ErrInvalid, got %v", err)
	}
}

type recordingTimerMetricSink struct {
	observed  atomic.Int64
	scheduled atomic.Int64
}

func (sink *recordingTimerMetricSink) ObserveTimerMetrics(metrics Metrics) {
	sink.observed.Add(1)
	sink.scheduled.Store(metrics.ScheduledTimeouts)
}

type panickingTimerMetricSink struct {
	observed atomic.Int64
}

func (sink *panickingTimerMetricSink) ObserveTimerMetrics(Metrics) {
	sink.observed.Add(1)
	panic("timer metric sink panic")
}

type timerMetricReleaseSignal struct {
	done       chan struct{}
	isReleased atomic.Bool
}

func newTimerMetricReleaseSignal() *timerMetricReleaseSignal {
	return &timerMetricReleaseSignal{
		done: make(chan struct{}),
	}
}

func (signal *timerMetricReleaseSignal) Release() {
	if signal.isReleased.CompareAndSwap(false, true) {
		close(signal.done)
	}
}

type blockingTimerMetricSink struct {
	entered     chan<- struct{}
	release     <-chan struct{}
	enteredOnce atomic.Bool
}

func (sink *blockingTimerMetricSink) ObserveTimerMetrics(Metrics) {
	if sink.enteredOnce.CompareAndSwap(false, true) {
		close(sink.entered)
	}
	<-sink.release
}

type noopTimerTask struct{}

func (noopTimerTask) Run(context.Context) {}

type countTimerTask struct {
	runs *atomic.Int64
}

func (task countTimerTask) Run(context.Context) {
	task.runs.Add(1)
}

type closeTimerTask struct {
	done chan<- struct{}
}

func (task closeTimerTask) Run(context.Context) {
	close(task.done)
}

type panicTimerTask struct {
	done chan<- struct{}
}

func (task panicTimerTask) Run(context.Context) {
	defer close(task.done)
	panic("boom")
}

type contextKey struct{}

type contextValueTask struct {
	t    *testing.T
	done chan<- struct{}
	want string
}

func (task contextValueTask) Run(ctx context.Context) {
	if got := ctx.Value(contextKey{}); got != task.want {
		task.t.Errorf("context value = %v, want %s", got, task.want)
	}
	close(task.done)
}

type closeExecutorTask struct {
	done chan<- struct{}
}

func (task closeExecutorTask) Run() {
	close(task.done)
}

type blockingExecutorTask struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (task blockingExecutorTask) Run() {
	close(task.started)
	<-task.release
}

type scheduleAsync struct {
	timer *Timer
	ctx   context.Context
	delay time.Duration
	task  Task
	errc  chan<- error
}

func (op scheduleAsync) run() {
	_, err := op.timer.Schedule(op.ctx, op.delay, op.task)
	op.errc <- err
}

type shutdownAsync struct {
	timer *Timer
	ctx   context.Context
	errc  chan<- error
}

func (op shutdownAsync) run() {
	op.errc <- op.timer.Shutdown(op.ctx)
}

type newTimerResult struct {
	timer *Timer
	err   error
}

type newTimerAsync struct {
	opts    []Option
	resultc chan<- newTimerResult
}

func (op newTimerAsync) run() {
	timer, err := New(op.opts...)
	op.resultc <- newTimerResult{
		timer: timer,
		err:   err,
	}
}

type condition interface {
	ready() bool
}

type rejectedDispatchesEqualCheck struct {
	timer *Timer
	want  int64
}

func (check rejectedDispatchesEqualCheck) ready() bool {
	return check.timer.Metrics().RejectedDispatches == check.want
}

type rejectedDispatchesAtLeastCheck struct {
	timer *Timer
	min   int64
}

func (check rejectedDispatchesAtLeastCheck) ready() bool {
	return check.timer.Metrics().RejectedDispatches >= check.min
}

type panickedTimeoutsEqualCheck struct {
	timer *Timer
	want  int64
}

func (check panickedTimeoutsEqualCheck) ready() bool {
	return check.timer.Metrics().PanickedTimeouts == check.want
}

type timerMetricScheduledCheck struct {
	sink *recordingTimerMetricSink
	min  int64
}

func (check timerMetricScheduledCheck) ready() bool {
	return check.sink.scheduled.Load() >= check.min
}

type timerMetricObservedCheck struct {
	sink *panickingTimerMetricSink
	min  int64
}

func (check timerMetricObservedCheck) ready() bool {
	return check.sink.observed.Load() >= check.min
}

type timerMetricReporterDoneCheck struct {
	timer *Timer
}

func (check timerMetricReporterDoneCheck) ready() bool {
	select {
	case <-check.timer.metricReporterDone:
		return true
	default:
		return false
	}
}

type timerOwnedPoolsClosedCheck struct {
	timer *Timer
}

func (check timerOwnedPoolsClosedCheck) ready() bool {
	bossErr := check.timer.bossPool.TryExecute(closeExecutorTask{done: make(chan struct{})})
	workerErr := check.timer.workerPool.TryExecute(closeExecutorTask{done: make(chan struct{})})

	return errors.Is(bossErr, executor.ErrClosed) &&
		errors.Is(workerErr, executor.ErrClosed)
}

type blockedPoolControl struct {
	t            *testing.T
	pool         *executor.Pool
	release      chan struct{}
	releasedOnce atomic.Bool
}

func (control *blockedPoolControl) Release() {
	if control.releasedOnce.CompareAndSwap(false, true) {
		close(control.release)
	}
}

func (control *blockedPoolControl) Shutdown() {
	control.Release()
	if err := control.pool.Shutdown(control.t.Context()); err != nil {
		control.t.Fatalf("blocked pool Shutdown() error = %v", err)
	}
}

func (control *blockedPoolControl) ShutdownAfterTimer(timer *Timer) {
	control.Release()
	shutdownTimer(control.t, timer)
	control.Shutdown()
}

func shutdownExecutorPool(t *testing.T, pool *executor.Pool) {
	t.Helper()

	if err := pool.Shutdown(t.Context()); err != nil {
		t.Fatalf("executor pool Shutdown() error = %v", err)
	}
}

func TestNewUsesDefaultPools(t *testing.T) {
	timer, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if timer.bossPool == nil {
		t.Fatal("bossPool is nil")
	}
	if timer.workerPool == nil {
		t.Fatal("workerPool is nil")
	}

	if err := timer.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	tests := []timerInvalidOptionCase{
		{
			name: "zero tick",
			opts: []Option{WithTick(0)},
		},
		{
			name: "zero bucket count",
			opts: []Option{WithBucketCount(0)},
		},
		{
			name: "zero command capacity",
			opts: []Option{WithCommandCapacity(0)},
		},
		{
			name: "zero max pending",
			opts: []Option{WithMaxPending(0)},
		},
		{
			name: "unknown backpressure policy",
			opts: []Option{WithBackpressurePolicy(BackpressurePolicyUnknown)},
		},
		{
			name: "unknown expired task policy",
			opts: []Option{WithExpiredTaskPolicy(ExpiredTaskPolicyUnknown)},
		},
		{
			name: "zero retry delay",
			opts: []Option{WithExpiredTaskRetryDelay(0)},
		},
		{
			name: "nil metric sink",
			opts: []Option{WithMetricSink(nil)},
		},
		{
			name: "zero metric report interval",
			opts: []Option{WithMetricReportInterval(0)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestNewRejectsConflictingPoolOptions(t *testing.T) {
	bossPool, err := executor.New(
		executor.WithWorkers(1),
		executor.WithQueueCapacity(1),
	)
	if err != nil {
		t.Fatalf("executor.New() boss error = %v", err)
	}
	defer shutdownExecutorPool(t, bossPool)

	workerPool, err := executor.New(
		executor.WithWorkers(1),
		executor.WithQueueCapacity(1),
	)
	if err != nil {
		t.Fatalf("executor.New() worker error = %v", err)
	}
	defer shutdownExecutorPool(t, workerPool)

	tests := []timerInvalidOptionCase{
		{
			name: "boss options then boss pool",
			opts: []Option{
				WithBoss(executor.WithWorkers(1)),
				WithBossPool(bossPool),
			},
		},
		{
			name: "boss pool then boss options",
			opts: []Option{
				WithBossPool(bossPool),
				WithBoss(executor.WithWorkers(1)),
			},
		},
		{
			name: "worker options then worker pool",
			opts: []Option{
				WithWorker(executor.WithWorkers(1)),
				WithWorkerPool(workerPool),
			},
		},
		{
			name: "worker pool then worker options",
			opts: []Option{
				WithWorkerPool(workerPool),
				WithWorker(executor.WithWorkers(1)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestNewReturnsExecutorOptionErrors(t *testing.T) {
	tests := []timerExecutorOptionCase{
		{
			name: "invalid boss option",
			opts: []Option{WithBoss(executor.WithWorkers(0))},
		},
		{
			name: "invalid worker option",
			opts: []Option{WithWorker(executor.WithWorkers(0))},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestNewAppliesOwnedPoolOptions(t *testing.T) {
	timer, err := New(
		WithBoss(executor.WithWorkers(1), executor.WithQueueCapacity(2)),
		WithWorker(executor.WithWorkers(2), executor.WithQueueCapacity(3)),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	if !timer.ownBossPool {
		t.Fatal("ownBossPool = false, want true")
	}
	if !timer.ownWorkerPool {
		t.Fatal("ownWorkerPool = false, want true")
	}
	if got := timer.bossPool.Metrics().Workers; got != 1 {
		t.Fatalf("boss workers = %d, want 1", got)
	}
	if got := timer.workerPool.Metrics().Workers; got != 2 {
		t.Fatalf("worker workers = %d, want 2", got)
	}
}

func TestNewStartsOwnedBossPoolWithDirectHandoff(t *testing.T) {
	timer, err := New(
		WithBoss(
			executor.WithWorkers(1),
			executor.WithQueueCapacity(0),
			executor.WithRejectPolicy(executor.RejectPolicyBlock),
		),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)
}

func TestNewRejectsSaturatedCallerOwnedBossPool(t *testing.T) {
	bossPool, bossControl := blockedPool(t, 0)
	defer bossControl.Shutdown()

	resultc := make(chan newTimerResult, 1)
	newOp := newTimerAsync{
		opts:    []Option{WithBossPool(bossPool)},
		resultc: resultc,
	}
	go newOp.run()

	select {
	case result := <-resultc:
		if result.timer != nil {
			shutdownTimer(t, result.timer)
		}
		if !errors.Is(result.err, executor.ErrSaturated) {
			t.Fatalf("New() error = %v, want executor.ErrSaturated", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("New() blocked when caller-owned boss pool was saturated")
	}
}

func TestScheduleRejectsInvalidInputs(t *testing.T) {
	timer, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	_, err = timer.Schedule(nil, 0, noopTimerTask{}) //nolint:staticcheck // Intentionally verifies nil context rejection.
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Schedule(nil ctx) error = %v, want ErrInvalid", err)
	}

	_, err = timer.Schedule(t.Context(), 0, nil)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Schedule(nil task) error = %v, want ErrInvalid", err)
	}

	err = timer.Shutdown(nil) //nolint:staticcheck // Intentionally verifies nil context rejection.
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Shutdown(nil ctx) error = %v, want ErrInvalid", err)
	}
}

func TestTimerMetricsSnapshotIsImmutable(t *testing.T) {
	timer, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	metrics := timer.Metrics()
	metrics.ScheduledTimeouts = 42
	metrics.ExpiredTimeouts = 42
	metrics.CancelledTimeouts = 42
	metrics.RejectedSchedules = 42
	metrics.RejectedDispatches = 42
	metrics.PanickedTimeouts = 42
	metrics.PendingTimeouts = 42
	metrics.CommandQueueDepth = 42
	metrics.BucketOffers = 42
	metrics.BucketExpirations = 42
	metrics.MaxBucketDelay = 42

	again := timer.Metrics()
	if again.ScheduledTimeouts == 42 ||
		again.ExpiredTimeouts == 42 ||
		again.CancelledTimeouts == 42 ||
		again.RejectedSchedules == 42 ||
		again.RejectedDispatches == 42 ||
		again.PanickedTimeouts == 42 ||
		again.PendingTimeouts == 42 ||
		again.CommandQueueDepth == 42 ||
		again.BucketOffers == 42 ||
		again.BucketExpirations == 42 ||
		again.MaxBucketDelay == 42 {
		t.Fatalf("metrics snapshot mutated timer state: %#v", again)
	}
}

func TestTimerMetricSinkObservesSnapshots(t *testing.T) {
	sink := &recordingTimerMetricSink{}
	timer, err := New(
		WithMetricSink(sink),
		WithMetricReportInterval(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	done := make(chan struct{})
	if _, err := timer.Schedule(t.Context(), 0, closeTimerTask{done: done}); err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}
	waitForClose(t, done, "task did not run")

	check := timerMetricScheduledCheck{
		sink: sink,
		min:  1,
	}
	waitUntil(t, check, "metric sink did not observe scheduled timeout")
}

func TestTimerMetricSinkPanicDoesNotStopReporter(t *testing.T) {
	sink := &panickingTimerMetricSink{}
	timer, err := New(
		WithMetricSink(sink),
		WithMetricReportInterval(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	check := timerMetricObservedCheck{
		sink: sink,
		min:  2,
	}
	waitUntil(t, check, "metric sink panic stopped the reporter")
}

func TestTimerMetricSinkReporterStopsOnShutdown(t *testing.T) {
	sink := &recordingTimerMetricSink{}
	timer, err := New(
		WithMetricSink(sink),
		WithMetricReportInterval(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if timer.metricReporterDone == nil {
		t.Fatal("metricReporterDone is nil")
	}
	if err := timer.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	check := timerMetricReporterDoneCheck{timer: timer}
	waitUntil(t, check, "metric reporter did not stop after shutdown")
}

func TestTimerShutdownWaitsForMetricSink(t *testing.T) {
	release := newTimerMetricReleaseSignal()
	defer release.Release()

	entered := make(chan struct{})
	sink := &blockingTimerMetricSink{
		entered: entered,
		release: release.done,
	}
	timer, err := New(
		WithMetricSink(sink),
		WithMetricReportInterval(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	waitForClose(t, entered, "metric sink was not entered")

	errc := make(chan error, 1)
	shutdownOp := shutdownAsync{
		timer: timer,
		ctx:   t.Context(),
		errc:  errc,
	}
	go shutdownOp.run()

	select {
	case err := <-errc:
		release.Release()
		t.Fatalf("Shutdown() returned before metric sink released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	release.Release()
	if err := <-errc; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestTimerShutdownClosesOwnedPoolsWhenMetricSinkWaitTimesOut(t *testing.T) {
	release := newTimerMetricReleaseSignal()
	defer release.Release()

	entered := make(chan struct{})
	sink := &blockingTimerMetricSink{
		entered: entered,
		release: release.done,
	}
	timer, err := New(
		WithMetricSink(sink),
		WithMetricReportInterval(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	waitForClose(t, entered, "metric sink was not entered")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	err = timer.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want context.DeadlineExceeded", err)
	}

	check := timerOwnedPoolsClosedCheck{timer: timer}
	waitUntil(t, check, "owned pools were not closed after metric sink wait timeout")
}

func TestTimerDoesNotCloseCallerOwnedBossPool(t *testing.T) {
	bossPool, err := executor.New(
		executor.WithWorkers(1),
		executor.WithQueueCapacity(1),
	)
	if err != nil {
		t.Fatalf("executor.New() error = %v", err)
	}
	defer shutdownExecutorPool(t, bossPool)

	timer, err := New(WithBossPool(bossPool))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := timer.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	done := make(chan struct{})
	if err := bossPool.Execute(t.Context(), closeExecutorTask{done: done}); err != nil {
		t.Fatalf("external boss pool Execute() error = %v", err)
	}
	waitForClose(t, done, "external boss pool did not run task")
}

func TestShutdownIsIdempotent(t *testing.T) {
	timer, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := timer.Shutdown(t.Context()); err != nil {
		t.Fatalf("first Shutdown() error = %v", err)
	}
	if err := timer.Shutdown(t.Context()); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}
}

func TestScheduleAfterShutdownReturnsClosed(t *testing.T) {
	timer, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := timer.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	_, err = timer.Schedule(t.Context(), 0, noopTimerTask{})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Schedule() after shutdown error = %v, want ErrClosed", err)
	}
	if got := timer.Metrics().RejectedSchedules; got != 1 {
		t.Fatalf("RejectedSchedules = %d, want 1", got)
	}
}

func TestShutdownCancelsPendingTimeouts(t *testing.T) {
	timer, err := New(WithTick(time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var runs atomic.Int64
	timeout, err := timer.Schedule(t.Context(), time.Hour, countTimerTask{runs: &runs})
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}
	if got := timer.Size(); got != 1 {
		t.Fatalf("Size() before shutdown = %d, want 1", got)
	}

	if err := timer.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got := timer.Size(); got != 0 {
		t.Fatalf("Size() after shutdown = %d, want 0", got)
	}
	if !timeout.IsCancelled() {
		t.Fatal("timeout is not cancelled")
	}
	if got := timer.Metrics().CancelledTimeouts; got != 1 {
		t.Fatalf("CancelledTimeouts = %d, want 1", got)
	}
	if got := runs.Load(); got != 0 {
		t.Fatalf("runs = %d, want 0", got)
	}
}

func TestTimeoutNilMethodsReturnFalse(t *testing.T) {
	var timeout *Timeout
	if timeout.Cancel() {
		t.Fatal("nil Timeout Cancel() = true, want false")
	}
	if timeout.IsCancelled() {
		t.Fatal("nil Timeout IsCancelled() = true, want false")
	}
	if timeout.IsExpired() {
		t.Fatal("nil Timeout IsExpired() = true, want false")
	}
}

func TestTimerDoesNotCloseCallerOwnedPools(t *testing.T) {
	workerPool, err := executor.New(
		executor.WithWorkers(1),
		executor.WithQueueCapacity(1),
	)
	if err != nil {
		t.Fatalf("executor.New() error = %v", err)
	}
	defer shutdownExecutorPool(t, workerPool)

	timer, err := New(WithWorkerPool(workerPool))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := timer.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	done := make(chan struct{})
	if err := workerPool.Execute(t.Context(), closeExecutorTask{done: done}); err != nil {
		t.Fatalf("external worker pool Execute() error = %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("external worker pool did not run task")
	}
}

func TestScheduleNegativeDelayRunsTask(t *testing.T) {
	timer, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	done := make(chan struct{})
	timeout, err := timer.Schedule(t.Context(), -time.Second, closeTimerTask{done: done})
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}

	waitForClose(t, done, "negative-delay task did not run")
	if !timeout.IsExpired() {
		t.Fatal("timeout is not expired")
	}
	metrics := timer.Metrics()
	if metrics.ScheduledTimeouts != 1 {
		t.Fatalf("ScheduledTimeouts = %d, want 1", metrics.ScheduledTimeouts)
	}
	if metrics.ExpiredTimeouts != 1 {
		t.Fatalf("ExpiredTimeouts = %d, want 1", metrics.ExpiredTimeouts)
	}
}

func TestScheduleZeroDelayRunsTask(t *testing.T) {
	timer, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	done := make(chan struct{})
	timeout, err := timer.Schedule(t.Context(), 0, closeTimerTask{done: done})
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}

	waitForClose(t, done, "zero-delay task did not run")
	if !timeout.IsExpired() {
		t.Fatal("timeout is not expired")
	}
	if got := timer.Size(); got != 0 {
		t.Fatalf("Size() = %d, want 0", got)
	}
}

func TestScheduleDelayedRunsTask(t *testing.T) {
	timer, err := New(WithTick(time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	done := make(chan struct{})
	_, err = timer.Schedule(t.Context(), 5*time.Millisecond, closeTimerTask{done: done})
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}

	waitForClose(t, done, "delayed task did not run")
	if got := timer.Metrics().ExpiredTimeouts; got != 1 {
		t.Fatalf("ExpiredTimeouts = %d, want 1", got)
	}
}

func TestSchedulePassesCapturedContext(t *testing.T) {
	timer, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	ctx := context.WithValue(t.Context(), contextKey{}, "value")
	done := make(chan struct{})
	task := contextValueTask{
		t:    t,
		done: done,
		want: "value",
	}
	_, err = timer.Schedule(ctx, 0, task)
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}

	waitForClose(t, done, "context task did not run")
}

func TestCancelBeforeSchedulerAdd(t *testing.T) {
	bossPool, bossControl := blockedPool(t, 1)

	timer, err := New(
		WithBossPool(bossPool),
		WithCommandCapacity(1),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer bossControl.ShutdownAfterTimer(timer)

	var runs atomic.Int64
	timeout, err := timer.Schedule(t.Context(), time.Hour, countTimerTask{runs: &runs})
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}
	if !timeout.Cancel() {
		t.Fatal("Cancel() = false, want true")
	}

	time.Sleep(20 * time.Millisecond)
	if got := runs.Load(); got != 0 {
		t.Fatalf("runs = %d, want 0", got)
	}
	if !timeout.IsCancelled() {
		t.Fatal("timeout is not cancelled")
	}
	if got := timer.Size(); got != 0 {
		t.Fatalf("Size() = %d, want 0", got)
	}
	if got := timer.Metrics().CancelledTimeouts; got != 1 {
		t.Fatalf("CancelledTimeouts = %d, want 1", got)
	}
}

func TestCancelAfterBucketAdd(t *testing.T) {
	timer, err := New(WithTick(time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	var runs atomic.Int64
	timeout, err := timer.Schedule(t.Context(), 50*time.Millisecond, countTimerTask{runs: &runs})
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}

	time.Sleep(5 * time.Millisecond)
	if !timeout.Cancel() {
		t.Fatal("Cancel() = false, want true")
	}
	time.Sleep(70 * time.Millisecond)
	if got := runs.Load(); got != 0 {
		t.Fatalf("runs = %d, want 0", got)
	}
	if got := timer.Size(); got != 0 {
		t.Fatalf("Size() = %d, want 0", got)
	}
	if got := timer.Metrics().CancelledTimeouts; got != 1 {
		t.Fatalf("CancelledTimeouts = %d, want 1", got)
	}
}

func TestCancelAfterExpirationReturnsFalse(t *testing.T) {
	timer, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	done := make(chan struct{})
	timeout, err := timer.Schedule(t.Context(), 0, closeTimerTask{done: done})
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}
	waitForClose(t, done, "task did not run")

	if timeout.Cancel() {
		t.Fatal("Cancel() after expiration = true, want false")
	}
}

func TestOverflowCascadeRunsTask(t *testing.T) {
	timer, err := New(
		WithTick(time.Millisecond),
		WithBucketCount(8),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	done := make(chan struct{})
	_, err = timer.Schedule(t.Context(), 25*time.Millisecond, closeTimerTask{done: done})
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}

	waitForClose(t, done, "overflow task did not run")
}

func TestScheduleRejectsWhenCommandQueueIsFull(t *testing.T) {
	bossPool, bossControl := blockedPool(t, 1)

	timer, err := New(
		WithBossPool(bossPool),
		WithCommandCapacity(1),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer bossControl.ShutdownAfterTimer(timer)

	if _, err := timer.Schedule(t.Context(), time.Hour, noopTimerTask{}); err != nil {
		t.Fatalf("first Schedule() error = %v", err)
	}

	_, err = timer.Schedule(t.Context(), time.Hour, noopTimerTask{})
	if !errors.Is(err, ErrSaturated) {
		t.Fatalf("second Schedule() error = %v, want ErrSaturated", err)
	}
	if got := timer.Metrics().RejectedSchedules; got != 1 {
		t.Fatalf("RejectedSchedules = %d, want 1", got)
	}
	if got := timer.Metrics().CommandQueueDepth; got != 1 {
		t.Fatalf("CommandQueueDepth = %d, want 1", got)
	}
	if got := timer.Size(); got != 1 {
		t.Fatalf("Size() = %d, want 1", got)
	}
}

func TestScheduleBlocksWhenCommandQueueIsFull(t *testing.T) {
	bossPool, bossControl := blockedPool(t, 1)

	timer, err := New(
		WithBossPool(bossPool),
		WithCommandCapacity(1),
		WithBackpressurePolicy(BackpressureBlock),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer bossControl.ShutdownAfterTimer(timer)

	if _, err := timer.Schedule(t.Context(), time.Hour, noopTimerTask{}); err != nil {
		t.Fatalf("first Schedule() error = %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	scheduleOp := scheduleAsync{
		timer: timer,
		ctx:   ctx,
		delay: time.Hour,
		task:  noopTimerTask{},
		errc:  errc,
	}
	go scheduleOp.run()

	select {
	case err := <-errc:
		t.Fatalf("Schedule() returned before context cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	err = <-errc
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Schedule() error = %v, want context.Canceled", err)
	}
	if got := timer.Metrics().RejectedSchedules; got != 1 {
		t.Fatalf("RejectedSchedules = %d, want 1", got)
	}
	if got := timer.Size(); got != 1 {
		t.Fatalf("Size() = %d, want 1", got)
	}
}

func TestScheduleReturnsClosedWhenShutdownWhileCommandQueueIsFull(t *testing.T) {
	bossPool, bossControl := blockedPool(t, 1)

	timer, err := New(
		WithBossPool(bossPool),
		WithCommandCapacity(1),
		WithBackpressurePolicy(BackpressureBlock),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer bossControl.ShutdownAfterTimer(timer)

	if _, err := timer.Schedule(t.Context(), time.Hour, noopTimerTask{}); err != nil {
		t.Fatalf("first Schedule() error = %v", err)
	}

	errc := make(chan error, 1)
	scheduleOp := scheduleAsync{
		timer: timer,
		ctx:   t.Context(),
		delay: time.Hour,
		task:  noopTimerTask{},
		errc:  errc,
	}
	go scheduleOp.run()

	select {
	case err := <-errc:
		t.Fatalf("Schedule() returned before shutdown: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := timer.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want context.DeadlineExceeded", err)
	}

	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Schedule() error = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Schedule() did not return after shutdown")
	}

	bossControl.Release()
	if err := timer.Shutdown(t.Context()); err != nil {
		t.Fatalf("final Shutdown() error = %v", err)
	}
	if got := timer.Metrics().RejectedSchedules; got != 1 {
		t.Fatalf("RejectedSchedules = %d, want 1", got)
	}
}

func TestScheduleRejectsWhenMaxPendingReached(t *testing.T) {
	bossPool, bossControl := blockedPool(t, 1)

	timer, err := New(
		WithBossPool(bossPool),
		WithCommandCapacity(2),
		WithMaxPending(1),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer bossControl.ShutdownAfterTimer(timer)

	if _, err := timer.Schedule(t.Context(), time.Hour, noopTimerTask{}); err != nil {
		t.Fatalf("first Schedule() error = %v", err)
	}

	_, err = timer.Schedule(t.Context(), time.Hour, noopTimerTask{})
	if !errors.Is(err, ErrSaturated) {
		t.Fatalf("second Schedule() error = %v, want ErrSaturated", err)
	}
	if got := timer.Metrics().RejectedSchedules; got != 1 {
		t.Fatalf("RejectedSchedules = %d, want 1", got)
	}
	if got := timer.Size(); got != 1 {
		t.Fatalf("Size() = %d, want 1", got)
	}
}

func TestShutdownDoesNotBlockWhenCommandQueueIsFull(t *testing.T) {
	bossPool, bossControl := blockedPool(t, 1)
	defer bossControl.Shutdown()

	timer, err := New(
		WithBossPool(bossPool),
		WithCommandCapacity(1),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := timer.Schedule(t.Context(), time.Hour, noopTimerTask{}); err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	errc := make(chan error, 1)
	shutdownOp := shutdownAsync{
		timer: timer,
		ctx:   ctx,
		errc:  errc,
	}
	go shutdownOp.run()

	select {
	case err := <-errc:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown() error = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(100 * time.Millisecond):
		bossControl.Release()
		err := <-errc
		_ = timer.Shutdown(t.Context())
		t.Fatalf("Shutdown() blocked while command queue was full, later returned %v", err)
	}

	bossControl.Release()
	if err := timer.Shutdown(t.Context()); err != nil {
		t.Fatalf("final Shutdown() error = %v", err)
	}
}

func TestDispatchRejectsWhenWorkerPoolIsSaturated(t *testing.T) {
	workerPool, workerControl := blockedPool(t, 0)
	defer workerControl.Shutdown()

	timer, err := New(WithWorkerPool(workerPool))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	var runs atomic.Int64
	_, err = timer.Schedule(t.Context(), 0, countTimerTask{runs: &runs})
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}

	waitUntil(t, rejectedDispatchesEqualCheck{timer: timer, want: 1}, "worker saturation was not recorded")
	if got := runs.Load(); got != 0 {
		t.Fatalf("runs = %d, want 0", got)
	}
	if got := timer.Size(); got != 0 {
		t.Fatalf("Size() = %d, want 0", got)
	}
	if got := timer.Metrics().RejectedDispatches; got != 1 {
		t.Fatalf("RejectedDispatches = %d, want 1", got)
	}
	workerControl.Release()
}

func TestDispatchRetriesExpiredTaskWhenConfigured(t *testing.T) {
	workerPool, workerControl := blockedPool(t, 0)
	defer workerControl.Shutdown()

	timer, err := New(
		WithWorkerPool(workerPool),
		WithExpiredTaskPolicy(ExpiredTaskRetry),
		WithExpiredTaskRetryDelay(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	done := make(chan struct{})
	_, err = timer.Schedule(t.Context(), 0, closeTimerTask{done: done})
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}

	waitUntil(t, rejectedDispatchesAtLeastCheck{timer: timer, min: 1}, "first dispatch rejection was not recorded")
	workerControl.Release()
	waitForClose(t, done, "retried task did not run")
	if got := timer.Size(); got != 0 {
		t.Fatalf("Size() = %d, want 0", got)
	}
	if got := timer.Metrics().ExpiredTimeouts; got != 1 {
		t.Fatalf("ExpiredTimeouts = %d, want 1", got)
	}
}

func TestDispatchPanicIsRecorded(t *testing.T) {
	timer, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownTimer(t, timer)

	done := make(chan struct{})
	_, err = timer.Schedule(t.Context(), 0, panicTimerTask{done: done})
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}

	waitForClose(t, done, "panic task did not run")
	waitUntil(t, panickedTimeoutsEqualCheck{timer: timer, want: 1}, "panic was not recorded")
}

func shutdownTimer(t *testing.T, timer *Timer) {
	t.Helper()

	if err := timer.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func blockedPool(t *testing.T, queueCapacity int) (*executor.Pool, *blockedPoolControl) {
	t.Helper()

	pool, err := executor.New(
		executor.WithWorkers(1),
		executor.WithQueueCapacity(queueCapacity),
		executor.WithRejectPolicy(executor.RejectPolicyBlock),
	)
	if err != nil {
		t.Fatalf("executor.New() error = %v", err)
	}

	released := make(chan struct{})
	started := make(chan struct{})
	control := &blockedPoolControl{
		t:       t,
		pool:    pool,
		release: released,
	}
	task := blockingExecutorTask{
		started: started,
		release: released,
	}
	if err := pool.Execute(t.Context(), task); err != nil {
		t.Fatalf("blocking Execute() error = %v", err)
	}
	waitForClose(t, started, "blocking pool task did not start")

	return pool, control
}

func waitUntil(t *testing.T, condition condition, message string) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition.ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(message)
}

func waitForClose(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}
