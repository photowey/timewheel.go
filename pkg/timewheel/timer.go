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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/photowey/timewheel.go/internal/timingwheel"
	"github.com/photowey/timewheel.go/pkg/executor"
)

// Timer schedules delayed tasks and dispatches expired tasks to a worker pool.
type Timer struct {
	tick                  time.Duration
	bucketCount           int64
	commandCapacity       int
	maxPending            int64
	backpressurePolicy    BackpressurePolicy
	expiredTaskPolicy     ExpiredTaskPolicy
	expiredTaskRetryDelay time.Duration

	bossPool      *executor.Pool
	workerPool    *executor.Pool
	ownBossPool   bool
	ownWorkerPool bool

	commands      chan command
	commandSlots  chan struct{}
	shutdownC     chan struct{}
	schedulerDone chan struct{}
	delayQueue    *timingwheel.BucketDelayQueue
	wheel         *timingwheel.TimingWheel
	clock         timingwheel.Clock
	metrics       metrics
	enqueueMu     sync.Mutex
	enqueuesDone  sync.WaitGroup
	isClosed      atomic.Bool
	closeOnce     sync.Once
}

type schedulerTask struct {
	timer *Timer
}

var _ executor.Task = schedulerTask{}

func (task schedulerTask) Run() {
	task.timer.runScheduler()
}

// New creates a Timer. Missing boss and worker pools are created internally.
func New(opts ...Option) (*Timer, error) {
	cfg := defaultOptions()
	for _, opt := range opts {
		if err := opt.apply(&cfg); err != nil {
			return nil, err
		}
	}

	timer, err := newTimer(cfg)
	if err != nil {
		return nil, err
	}
	if err := timer.startScheduler(); err != nil {
		_ = timer.closeOwnedPools(context.Background())
		return nil, fmt.Errorf("timewheel: start scheduler: %w", err)
	}
	return timer, nil
}

// newTimer wires executor ownership and scheduler data structures.
func newTimer(cfg options) (*Timer, error) {
	bossPool := cfg.bossPool
	ownBossPool := false
	if bossPool == nil {
		var err error
		bossPool, err = executor.New(cfg.bossOptions...)
		if err != nil {
			return nil, fmt.Errorf("timewheel: create boss pool: %w", err)
		}
		ownBossPool = true
	}

	workerPool := cfg.workerPool
	ownWorkerPool := false
	if workerPool == nil {
		var err error
		workerPool, err = executor.New(cfg.workerOptions...)
		if err != nil {
			if ownBossPool {
				_ = bossPool.Shutdown(context.Background())
			}
			return nil, fmt.Errorf("timewheel: create worker pool: %w", err)
		}
		ownWorkerPool = true
	}

	return &Timer{
		tick:                  cfg.tick,
		bucketCount:           cfg.bucketCount,
		commandCapacity:       cfg.commandCapacity,
		maxPending:            cfg.maxPending,
		backpressurePolicy:    cfg.backpressurePolicy,
		expiredTaskPolicy:     cfg.expiredTaskPolicy,
		expiredTaskRetryDelay: cfg.expiredTaskRetryDelay,
		bossPool:              bossPool,
		workerPool:            workerPool,
		ownBossPool:           ownBossPool,
		ownWorkerPool:         ownWorkerPool,
		commands:              make(chan command, cfg.commandCapacity),
		commandSlots:          newCommandSlots(cfg.commandCapacity),
		shutdownC:             make(chan struct{}),
		schedulerDone:         make(chan struct{}),
		delayQueue:            timingwheel.NewBucketDelayQueue(),
		clock:                 timingwheel.RealClock{},
	}, nil
}

func (t *Timer) startScheduler() error {
	task := schedulerTask{timer: t}
	if t.ownBossPool {
		return t.bossPool.Execute(context.Background(), task)
	}
	return t.bossPool.TryExecute(task)
}

// newCommandSlots creates one reusable permit for each command queue slot.
func newCommandSlots(capacity int) chan struct{} {
	slots := make(chan struct{}, capacity)
	for range capacity {
		slots <- struct{}{}
	}
	return slots
}

// Schedule adds task to the timing wheel and returns a timeout handle.
func (t *Timer) Schedule(ctx context.Context, delay time.Duration, task Task) (*Timeout, error) {
	if ctx == nil || task == nil {
		return nil, fmt.Errorf("timewheel: schedule: %w", ErrInvalid)
	}
	if t.isClosed.Load() {
		t.metrics.rejectedSchedules.Add(1)
		return nil, fmt.Errorf("timewheel: schedule: %w", ErrClosed)
	}

	pending := t.metrics.pendingTimeouts.Add(1)
	if pending > t.maxPending {
		t.metrics.pendingTimeouts.Add(-1)
		t.metrics.rejectedSchedules.Add(1)
		return nil, fmt.Errorf("timewheel: schedule: %w", ErrSaturated)
	}

	if delay < 0 {
		delay = 0
	}
	entry := timingwheel.NewTaskEntry(
		timingwheel.Deadline(t.clock, delay),
		ctx,
		task,
	)
	timeout := &Timeout{
		timer: t,
		entry: entry,
	}
	cmd := command{
		kind:  commandKindSchedule,
		entry: entry,
	}

	if err := t.enqueue(ctx, cmd); err != nil {
		t.metrics.pendingTimeouts.Add(-1)
		t.metrics.rejectedSchedules.Add(1)
		return nil, err
	}

	t.metrics.scheduledTimeouts.Add(1)
	return timeout, nil
}

// Shutdown stops accepting schedules, cancels undispatched timeouts, and closes
// timer-owned executor pools.
func (t *Timer) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("timewheel: shutdown: %w", ErrInvalid)
	}

	t.closeOnce.Do(t.closeScheduler)

	select {
	case <-t.schedulerDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	return t.closeOwnedPools(ctx)
}

// closeOwnedPools shuts down only pools created by New.
func (t *Timer) closeOwnedPools(ctx context.Context) error {
	var err error
	if t.ownBossPool {
		if shutdownErr := t.bossPool.Shutdown(ctx); shutdownErr != nil {
			err = shutdownErr
		}
	}
	if t.ownWorkerPool {
		if shutdownErr := t.workerPool.Shutdown(ctx); shutdownErr != nil && err == nil {
			err = shutdownErr
		}
	}
	return err
}

// Metrics returns an immutable point-in-time snapshot of timer counters.
func (t *Timer) Metrics() Metrics {
	return t.metrics.snapshot()
}

// Size returns the number of accepted timeouts that are still pending.
func (t *Timer) Size() int64 {
	return t.metrics.pendingTimeouts.Load()
}

// enqueue applies the schedule backpressure policy to the command queue.
func (t *Timer) enqueue(ctx context.Context, cmd command) error {
	if err := t.beginEnqueue(); err != nil {
		return fmt.Errorf("timewheel: enqueue command: %w", err)
	}
	defer t.endEnqueue()

	switch t.backpressurePolicy {
	case BackpressureBlock:
		if err := t.acquireCommandSlot(ctx); err != nil {
			return err
		}
	default:
		if err := t.tryCommandSlot(); err != nil {
			return err
		}
	}
	return t.sendReservedCommand(cmd)
}

func (t *Timer) beginEnqueue() error {
	t.enqueueMu.Lock()
	defer t.enqueueMu.Unlock()

	if t.isClosed.Load() {
		return ErrClosed
	}
	t.enqueuesDone.Add(1)
	return nil
}

func (t *Timer) endEnqueue() {
	t.enqueuesDone.Done()
}

func (t *Timer) acquireCommandSlot(ctx context.Context) error {
	select {
	case <-t.commandSlots:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.shutdownC:
		return fmt.Errorf("timewheel: enqueue command: %w", ErrClosed)
	}
}

func (t *Timer) tryCommandSlot() error {
	select {
	case <-t.commandSlots:
		return nil
	default:
		return fmt.Errorf("timewheel: enqueue command: %w", ErrSaturated)
	}
}

// sendReservedCommand publishes a command after enqueue has reserved capacity.
func (t *Timer) sendReservedCommand(cmd command) error {
	t.enqueueMu.Lock()
	defer t.enqueueMu.Unlock()

	if t.isClosed.Load() {
		t.releaseCommandSlot()
		return fmt.Errorf("timewheel: enqueue command: %w", ErrClosed)
	}

	select {
	case t.commands <- cmd:
		t.metrics.commandQueueDepth.Add(1)
		return nil
	default:
		t.releaseCommandSlot()
		return fmt.Errorf("timewheel: enqueue command: %w", ErrSaturated)
	}
}

func (t *Timer) releaseCommandSlot() {
	t.commandSlots <- struct{}{}
}

// completePending decrements the count of accepted, incomplete timeouts.
func (t *Timer) completePending() {
	t.metrics.pendingTimeouts.Add(-1)
}

// cancel records a successful timeout cancellation.
func (t *Timer) cancel() {
	t.completePending()
	t.metrics.cancelledTimeouts.Add(1)
}

func (t *Timer) closeScheduler() {
	t.enqueueMu.Lock()
	t.isClosed.Store(true)
	close(t.shutdownC)
	t.enqueueMu.Unlock()
	t.enqueuesDone.Wait()
}
