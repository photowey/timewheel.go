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
	"time"

	"github.com/photowey/timewheel.go/internal/timingwheel"
	"github.com/photowey/timewheel.go/pkg/executor"
)

type timeoutDispatchTask struct {
	timer *Timer
	entry *timingwheel.TaskEntry
	task  Task
}

var _ executor.Task = timeoutDispatchTask{}

func (task timeoutDispatchTask) Run() {
	defer task.timer.recordDispatchPanic()
	task.task.Run(task.entry.Context)
}

type requeueEntryHandler struct {
	timer *Timer
}

var _ timingwheel.EntryHandler = requeueEntryHandler{}

func (handler requeueEntryHandler) HandleEntry(entry *timingwheel.TaskEntry) {
	handler.timer.requeueOrDispatch(entry)
}

type cancelEntryHandler struct {
	timer *Timer
}

var _ timingwheel.EntryHandler = cancelEntryHandler{}

func (handler cancelEntryHandler) HandleEntry(entry *timingwheel.TaskEntry) {
	handler.timer.cancelEntry(entry)
}

// runScheduler is the single owner of wheel mutation.
//
// Schedule callers only enqueue commands. This keeps bucket movement,
// expiration polling, overflow cascading, and worker dispatch serialized in the
// boss pool, while user tasks run on the worker pool.
func (t *Timer) runScheduler() {
	defer t.finishScheduler()

	t.wheel = timingwheel.NewTimingWheel(
		t.tick,
		t.bucketCount,
		timingwheel.StartTime(t.clock, t.tick),
		t.delayQueue,
	)

	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		// Drain command bursts before polling buckets so newly accepted due-now
		// entries do not wait behind the next timer sleep.
		t.drainCommands()
		if t.shouldShutdown() {
			return
		}

		now := t.clock.Now().UnixMilli()
		requeueHandler := requeueEntryHandler{timer: t}
		for {
			bucket, ok := t.delayQueue.Poll(now)
			if !ok {
				break
			}
			t.metrics.bucketExpirations.Add(1)
			t.wheel.AdvanceClock(bucket.Expiration())
			bucket.Flush(requeueHandler)
		}

		// Sleep until the next live bucket expiration, but wake early when a
		// schedule command arrives.
		wait := time.Hour
		if expiration, ok := t.delayQueue.PeekExpiration(); ok {
			now = t.clock.Now().UnixMilli()
			if expiration <= now {
				wait = 0
			} else {
				wait = time.Duration(expiration-now) * time.Millisecond
			}
		}

		if wait == 0 {
			continue
		}
		timer.Reset(wait)
		select {
		case <-t.shutdownC:
			return
		case cmd := <-t.commands:
			t.metrics.commandQueueDepth.Add(-1)
			t.releaseCommandSlot()
			t.handleCommand(cmd)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
}

func (t *Timer) finishScheduler() {
	t.cancelPending()
	close(t.schedulerDone)
}

// drainCommands processes already queued commands without blocking.
func (t *Timer) drainCommands() {
	for {
		select {
		case <-t.shutdownC:
			return
		case cmd := <-t.commands:
			t.metrics.commandQueueDepth.Add(-1)
			t.releaseCommandSlot()
			t.handleCommand(cmd)
		default:
			return
		}
	}
}

// shouldShutdown reports whether shutdown has been requested.
func (t *Timer) shouldShutdown() bool {
	select {
	case <-t.shutdownC:
		return true
	default:
		return false
	}
}

// handleCommand applies a scheduler command to the wheel.
func (t *Timer) handleCommand(cmd command) {
	switch cmd.kind {
	case commandKindSchedule:
		if cmd.entry.IsCancelled() {
			return
		}
		// Add returns true for already-due entries, including zero and negative
		// delays normalized by Schedule.
		if t.wheel.Add(cmd.entry) {
			t.dispatch(cmd.entry)
		}
	}
}

func (t *Timer) requeueOrDispatch(entry *timingwheel.TaskEntry) {
	if entry.IsCancelled() {
		return
	}
	// Flushed entries may either cascade down to a lower wheel level or become
	// due for worker dispatch.
	if t.wheel.Add(entry) {
		t.dispatch(entry)
	}
}

// dispatch claims an expired entry and submits its task to the worker pool.
//
// The entry is marked expired as soon as worker submission succeeds. Task
// execution may still be running, but it no longer blocks scheduler progress.
func (t *Timer) dispatch(entry *timingwheel.TaskEntry) {
	if !entry.ClaimDispatch() {
		return
	}

	task, ok := entry.Task.(Task)
	if !ok {
		if entry.Reject() {
			t.completePending()
		}
		return
	}

	err := t.workerPool.TryExecute(timeoutDispatchTask{
		timer: t,
		entry: entry,
		task:  task,
	})
	if err != nil {
		t.handleDispatchRejected(entry)
		return
	}

	if entry.Expire() {
		t.completePending()
		t.metrics.expiredTimeouts.Add(1)
	}
}

func (t *Timer) recordDispatchPanic() {
	if recovered := recover(); recovered != nil {
		t.metrics.panickedTimeouts.Add(1)
		// Re-panic so executor-level recovery and panic handlers observe the same
		// failure boundary as directly submitted executor tasks.
		panic(recovered)
	}
}

// handleDispatchRejected applies the configured policy when worker submission
// fails because the worker pool is saturated or closed.
func (t *Timer) handleDispatchRejected(entry *timingwheel.TaskEntry) {
	t.metrics.rejectedDispatches.Add(1)
	if t.expiredTaskPolicy == ExpiredTaskRetry {
		t.retry(entry)
		return
	}
	if entry.Reject() {
		t.completePending()
	}
}

// retry restores a dispatching entry to scheduled state and re-adds it.
func (t *Timer) retry(entry *timingwheel.TaskEntry) {
	delay := t.expiredTaskRetryDelay
	if delay < t.tick {
		delay = t.tick
	}
	deadline := timingwheel.Deadline(t.clock, delay)
	if !entry.RestoreScheduled(deadline) {
		if entry.Reject() {
			t.completePending()
		}
		return
	}
	// A retry delay can already be due if the scheduler clock advanced while
	// handling rejection, so Add may immediately request another dispatch.
	if t.wheel.Add(entry) {
		t.dispatch(entry)
	}
}

// cancelPending cancels accepted entries that were not dispatched before
// shutdown.
func (t *Timer) cancelPending() {
	t.drainPendingCommands()
	cancelHandler := cancelEntryHandler{timer: t}
	for {
		bucket, ok := t.delayQueue.Poll(int64(^uint64(0) >> 1))
		if !ok {
			break
		}
		bucket.Flush(cancelHandler)
	}
	if t.wheel != nil {
		t.wheel.Flush(cancelHandler)
	}
}

// drainPendingCommands cancels commands left in the bounded queue during
// shutdown cleanup.
func (t *Timer) drainPendingCommands() {
	for {
		select {
		case cmd := <-t.commands:
			t.metrics.commandQueueDepth.Add(-1)
			t.releaseCommandSlot()
			if cmd.kind == commandKindSchedule {
				t.cancelEntry(cmd.entry)
			}
		default:
			return
		}
	}
}

// cancelEntry marks a pending entry cancelled and updates timer counters once.
func (t *Timer) cancelEntry(entry *timingwheel.TaskEntry) {
	if entry != nil && entry.Cancel() {
		t.cancel()
	}
}
