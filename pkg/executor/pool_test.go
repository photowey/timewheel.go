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

package executor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type countPanicHandler struct {
	count *atomic.Int64
}

func (handler countPanicHandler) HandlePanic(any) {
	handler.count.Add(1)
}

type recordingPoolMetricSink struct {
	observed  atomic.Int64
	submitted atomic.Int64
}

func (sink *recordingPoolMetricSink) ObservePoolMetrics(metrics Metrics) {
	sink.observed.Add(1)
	sink.submitted.Store(metrics.SubmittedTasks)
}

type panickingPoolMetricSink struct {
	observed atomic.Int64
}

func (sink *panickingPoolMetricSink) ObservePoolMetrics(Metrics) {
	sink.observed.Add(1)
	panic("pool metric sink panic")
}

type poolMetricReleaseSignal struct {
	done       chan struct{}
	isReleased atomic.Bool
}

func newPoolMetricReleaseSignal() *poolMetricReleaseSignal {
	return &poolMetricReleaseSignal{
		done: make(chan struct{}),
	}
}

func (signal *poolMetricReleaseSignal) Release() {
	if signal.isReleased.CompareAndSwap(false, true) {
		close(signal.done)
	}
}

type blockingPoolMetricSink struct {
	entered     chan<- struct{}
	release     <-chan struct{}
	enteredOnce atomic.Bool
}

func (sink *blockingPoolMetricSink) ObservePoolMetrics(Metrics) {
	if sink.enteredOnce.CompareAndSwap(false, true) {
		close(sink.entered)
	}
	<-sink.release
}

type invalidOptionCase struct {
	name string
	opts []Option
}

func (tc invalidOptionCase) run(t *testing.T) {
	pool, err := New(tc.opts...)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if pool != nil {
		t.Fatalf("expected nil pool, got %#v", pool)
	}
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got %v", err)
	}
}

type noopTask struct{}

func (noopTask) Run() {}

type closeTask struct {
	done chan<- struct{}
}

func (task closeTask) Run() {
	close(task.done)
}

type blockingTask struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (task blockingTask) Run() {
	close(task.started)
	<-task.release
}

type panicTask struct {
	done chan<- struct{}
}

func (task panicTask) Run() {
	defer close(task.done)
	panic("boom")
}

type executeAsync struct {
	pool *Pool
	ctx  context.Context
	task Task
	errc chan<- error
}

func (op executeAsync) run() {
	op.errc <- op.pool.Execute(op.ctx, op.task)
}

type shutdownAsync struct {
	pool *Pool
	ctx  context.Context
	errc chan<- error
}

func (op shutdownAsync) run() {
	op.errc <- op.pool.Shutdown(op.ctx)
}

type completedTaskCheck struct {
	pool *Pool
	want int64
}

func (check completedTaskCheck) ready() bool {
	return check.pool.Metrics().CompletedTasks == check.want
}

type poolMetricSubmittedCheck struct {
	sink *recordingPoolMetricSink
	min  int64
}

func (check poolMetricSubmittedCheck) ready() bool {
	return check.sink.submitted.Load() >= check.min
}

type poolMetricObservedCheck struct {
	sink *panickingPoolMetricSink
	min  int64
}

func (check poolMetricObservedCheck) ready() bool {
	return check.sink.observed.Load() >= check.min
}

type poolMetricReporterDoneCheck struct {
	pool *Pool
}

func (check poolMetricReporterDoneCheck) ready() bool {
	select {
	case <-check.pool.metricReporterDone:
		return true
	default:
		return false
	}
}

type condition interface {
	ready() bool
}

var taskFuncRuns atomic.Int64

func recordTaskFuncRun() {
	taskFuncRuns.Add(1)
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	tests := []invalidOptionCase{
		{
			name: "empty name",
			opts: []Option{WithName("")},
		},
		{
			name: "zero workers",
			opts: []Option{WithWorkers(0)},
		},
		{
			name: "negative queue capacity",
			opts: []Option{WithQueueCapacity(-1)},
		},
		{
			name: "unknown reject policy",
			opts: []Option{WithRejectPolicy(RejectPolicyUnknown)},
		},
		{
			name: "nil option",
			opts: []Option{nil},
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

func TestTaskFuncRuns(t *testing.T) {
	taskFuncRuns.Store(0)

	task := TaskFunc(recordTaskFuncRun)
	task.Run()

	if got := taskFuncRuns.Load(); got != 1 {
		t.Fatalf("runs = %d, want 1", got)
	}
}

func TestPoolRejectsInvalidExecuteInputs(t *testing.T) {
	pool, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownPool(t, pool)

	err = pool.Execute(nil, noopTask{}) //nolint:staticcheck // Intentionally verifies nil context rejection.
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Execute(nil ctx) error = %v, want ErrInvalid", err)
	}

	err = pool.Execute(t.Context(), nil)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Execute(nil task) error = %v, want ErrInvalid", err)
	}

	err = pool.TryExecute(nil)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("TryExecute(nil task) error = %v, want ErrInvalid", err)
	}

	err = pool.Shutdown(nil) //nolint:staticcheck // Intentionally verifies nil context rejection.
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Shutdown(nil ctx) error = %v, want ErrInvalid", err)
	}
}

func TestPoolMetricsSnapshotIsImmutable(t *testing.T) {
	pool, err := New(WithWorkers(2))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownPool(t, pool)

	metrics := pool.Metrics()
	metrics.SubmittedTasks = 42
	metrics.CompletedTasks = 42
	metrics.RejectedTasks = 42
	metrics.PanickedTasks = 42
	metrics.QueueDepth = 42
	metrics.Workers = 42

	again := pool.Metrics()
	if again.SubmittedTasks == 42 ||
		again.CompletedTasks == 42 ||
		again.RejectedTasks == 42 ||
		again.PanickedTasks == 42 ||
		again.QueueDepth == 42 ||
		again.Workers == 42 {
		t.Fatalf("metrics snapshot mutated pool state: %#v", again)
	}
	if again.Workers != 2 {
		t.Fatalf("Workers = %d, want 2", again.Workers)
	}
}

func TestPoolMetricSinkObservesSnapshots(t *testing.T) {
	sink := &recordingPoolMetricSink{}
	pool, err := New(
		WithWorkers(1),
		WithQueueCapacity(1),
		WithMetricSink(sink),
		WithMetricReportInterval(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownPool(t, pool)

	done := make(chan struct{})
	if err := pool.Execute(t.Context(), closeTask{done: done}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	waitForClose(t, done, "task did not run")

	check := poolMetricSubmittedCheck{
		sink: sink,
		min:  1,
	}
	waitUntil(t, check, "metric sink did not observe submitted task")
}

func TestPoolMetricSinkPanicDoesNotStopReporter(t *testing.T) {
	sink := &panickingPoolMetricSink{}
	pool, err := New(
		WithMetricSink(sink),
		WithMetricReportInterval(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownPool(t, pool)

	check := poolMetricObservedCheck{
		sink: sink,
		min:  2,
	}
	waitUntil(t, check, "metric sink panic stopped the reporter")
}

func TestPoolMetricSinkReporterStopsOnShutdown(t *testing.T) {
	sink := &recordingPoolMetricSink{}
	pool, err := New(
		WithMetricSink(sink),
		WithMetricReportInterval(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if pool.metricReporterDone == nil {
		t.Fatal("metricReporterDone is nil")
	}
	if err := pool.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	check := poolMetricReporterDoneCheck{pool: pool}
	waitUntil(t, check, "metric reporter did not stop after shutdown")
}

func TestPoolShutdownWaitsForMetricSink(t *testing.T) {
	release := newPoolMetricReleaseSignal()
	defer release.Release()

	entered := make(chan struct{})
	sink := &blockingPoolMetricSink{
		entered: entered,
		release: release.done,
	}
	pool, err := New(
		WithMetricSink(sink),
		WithMetricReportInterval(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	waitForClose(t, entered, "metric sink was not entered")

	errc := make(chan error, 1)
	shutdownOp := shutdownAsync{
		pool: pool,
		ctx:  t.Context(),
		errc: errc,
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

func TestPoolExecuteRunsTask(t *testing.T) {
	pool, err := New(
		WithWorkers(1),
		WithQueueCapacity(1),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownPool(t, pool)

	done := make(chan struct{})
	err = pool.Execute(t.Context(), closeTask{done: done})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	waitForClose(t, done, "task did not run")

	metrics := pool.Metrics()
	if metrics.SubmittedTasks != 1 {
		t.Fatalf("SubmittedTasks = %d, want 1", metrics.SubmittedTasks)
	}
	if metrics.CompletedTasks != 1 {
		t.Fatalf("CompletedTasks = %d, want 1", metrics.CompletedTasks)
	}
	if metrics.QueueDepth != 0 {
		t.Fatalf("QueueDepth = %d, want 0", metrics.QueueDepth)
	}
}

func TestPoolExecuteRejectsWhenSaturated(t *testing.T) {
	pool, err := New(
		WithWorkers(1),
		WithQueueCapacity(1),
		WithRejectPolicy(RejectPolicyReject),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownPool(t, pool)

	block := make(chan struct{})
	started := make(chan struct{})
	if err := pool.Execute(t.Context(), blockingTask{
		started: started,
		release: block,
	}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	waitForClose(t, started, "blocking task did not start")

	if err := pool.Execute(t.Context(), noopTask{}); err != nil {
		t.Fatalf("queued Execute() error = %v", err)
	}

	err = pool.Execute(t.Context(), noopTask{})
	if !errors.Is(err, ErrSaturated) {
		t.Fatalf("Execute() error = %v, want ErrSaturated", err)
	}
	if got := pool.Metrics().RejectedTasks; got != 1 {
		t.Fatalf("RejectedTasks = %d, want 1", got)
	}
	close(block)
}

func TestPoolTryExecuteRejectsWhenSaturated(t *testing.T) {
	pool, err := New(
		WithWorkers(1),
		WithQueueCapacity(0),
		WithRejectPolicy(RejectPolicyBlock),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownPool(t, pool)

	block := make(chan struct{})
	started := make(chan struct{})
	if err := pool.Execute(t.Context(), blockingTask{
		started: started,
		release: block,
	}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	waitForClose(t, started, "blocking task did not start")

	err = pool.TryExecute(noopTask{})
	if !errors.Is(err, ErrSaturated) {
		t.Fatalf("TryExecute() error = %v, want ErrSaturated", err)
	}
	if got := pool.Metrics().RejectedTasks; got != 1 {
		t.Fatalf("RejectedTasks = %d, want 1", got)
	}
	close(block)
}

func TestPoolExecuteBlocksUntilContextCancelled(t *testing.T) {
	pool, err := New(
		WithWorkers(1),
		WithQueueCapacity(0),
		WithRejectPolicy(RejectPolicyBlock),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownPool(t, pool)

	block := make(chan struct{})
	started := make(chan struct{})
	if err := pool.Execute(t.Context(), blockingTask{
		started: started,
		release: block,
	}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	waitForClose(t, started, "blocking task did not start")

	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	executeOp := executeAsync{
		pool: pool,
		ctx:  ctx,
		task: noopTask{},
		errc: errc,
	}
	go executeOp.run()

	select {
	case err := <-errc:
		t.Fatalf("Execute() returned before context cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	err = <-errc
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled", err)
	}
	if got := pool.Metrics().RejectedTasks; got != 1 {
		t.Fatalf("RejectedTasks = %d, want 1", got)
	}
	close(block)
}

func TestPoolExecuteReturnsClosedWhenShutdownWhileBlocked(t *testing.T) {
	pool, err := New(
		WithWorkers(1),
		WithQueueCapacity(0),
		WithRejectPolicy(RejectPolicyBlock),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	block := make(chan struct{})
	started := make(chan struct{})
	if err := pool.Execute(t.Context(), blockingTask{
		started: started,
		release: block,
	}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	waitForClose(t, started, "blocking task did not start")

	errc := make(chan error, 1)
	executeOp := executeAsync{
		pool: pool,
		ctx:  t.Context(),
		task: noopTask{},
		errc: errc,
	}
	go executeOp.run()

	select {
	case err := <-errc:
		t.Fatalf("Execute() returned before shutdown: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	shutdownErr := make(chan error, 1)
	shutdownOp := shutdownAsync{
		pool: pool,
		ctx:  t.Context(),
		errc: shutdownErr,
	}
	go shutdownOp.run()

	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Execute() error = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Execute() did not return after shutdown")
	}

	close(block)
	if err := <-shutdownErr; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got := pool.Metrics().RejectedTasks; got != 1 {
		t.Fatalf("RejectedTasks = %d, want 1", got)
	}
}

func TestPoolExecuteBlocksUntilQueueHasCapacity(t *testing.T) {
	pool, err := New(
		WithWorkers(1),
		WithQueueCapacity(0),
		WithRejectPolicy(RejectPolicyBlock),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownPool(t, pool)

	block := make(chan struct{})
	started := make(chan struct{})
	if err := pool.Execute(t.Context(), blockingTask{
		started: started,
		release: block,
	}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	waitForClose(t, started, "blocking task did not start")

	done := make(chan struct{})
	errc := make(chan error, 1)
	executeOp := executeAsync{
		pool: pool,
		ctx:  t.Context(),
		task: closeTask{done: done},
		errc: errc,
	}
	go executeOp.run()

	select {
	case err := <-errc:
		t.Fatalf("Execute() returned before queue capacity was available: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(block)
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("blocked Execute() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Execute() did not return after queue capacity was available")
	}
	waitForClose(t, done, "blocked task did not run")
	check := completedTaskCheck{
		pool: pool,
		want: 2,
	}
	waitUntil(t, check, "completed task count was not updated")
	if got := pool.Metrics().SubmittedTasks; got != 2 {
		t.Fatalf("SubmittedTasks = %d, want 2", got)
	}
}

func TestPoolShutdownDrainsAcceptedTasks(t *testing.T) {
	pool, err := New(
		WithWorkers(1),
		WithQueueCapacity(1),
		WithRejectPolicy(RejectPolicyBlock),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	block := make(chan struct{})
	started := make(chan struct{})
	if err := pool.Execute(t.Context(), blockingTask{
		started: started,
		release: block,
	}); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	waitForClose(t, started, "blocking task did not start")

	drained := make(chan struct{})
	if err := pool.Execute(t.Context(), closeTask{done: drained}); err != nil {
		t.Fatalf("queued Execute() error = %v", err)
	}

	shutdownErr := make(chan error, 1)
	shutdownOp := shutdownAsync{
		pool: pool,
		ctx:  t.Context(),
		errc: shutdownErr,
	}
	go shutdownOp.run()

	select {
	case err := <-shutdownErr:
		t.Fatalf("Shutdown() returned before accepted tasks drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(block)
	waitForClose(t, drained, "queued task did not run")
	if err := <-shutdownErr; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestPoolRecoversPanicAndUpdatesMetrics(t *testing.T) {
	var recovered atomic.Int64
	pool, err := New(
		WithWorkers(1),
		WithQueueCapacity(1),
		WithPanicHandler(countPanicHandler{count: &recovered}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer shutdownPool(t, pool)

	done := make(chan struct{})
	if err := pool.Execute(t.Context(), panicTask{done: done}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	waitForClose(t, done, "panic task did not run")

	again := make(chan struct{})
	if err := pool.Execute(t.Context(), closeTask{done: again}); err != nil {
		t.Fatalf("Execute() after panic error = %v", err)
	}

	waitForClose(t, again, "worker did not continue after panic")
	check := completedTaskCheck{
		pool: pool,
		want: 2,
	}
	waitUntil(t, check, "completed task count was not updated")

	metrics := pool.Metrics()
	if metrics.PanickedTasks != 1 {
		t.Fatalf("PanickedTasks = %d, want 1", metrics.PanickedTasks)
	}
	if metrics.CompletedTasks != 2 {
		t.Fatalf("CompletedTasks = %d, want 2", metrics.CompletedTasks)
	}
	if got := recovered.Load(); got != 1 {
		t.Fatalf("panic handler calls = %d, want 1", got)
	}
}

func TestPoolShutdownIsIdempotent(t *testing.T) {
	pool, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := pool.Shutdown(t.Context()); err != nil {
		t.Fatalf("first Shutdown() error = %v", err)
	}
	if err := pool.Shutdown(t.Context()); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}

	err = pool.TryExecute(noopTask{})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("TryExecute() after shutdown error = %v, want ErrClosed", err)
	}

	err = pool.Execute(t.Context(), noopTask{})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Execute() after shutdown error = %v, want ErrClosed", err)
	}
}

func shutdownPool(t *testing.T, pool *Pool) {
	t.Helper()

	if err := pool.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
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
