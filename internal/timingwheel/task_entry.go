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

package timingwheel

import (
	"context"
	"sync/atomic"
)

// TaskEntry is the timing-wheel node for one scheduled task.
//
// Deadline and state are atomic because cancellation can happen from caller
// goroutines while the scheduler owns bucket movement and dispatch decisions.
type TaskEntry struct {
	deadline atomic.Int64
	state    atomic.Int32

	// Context is captured at schedule time and passed to the task on dispatch.
	Context context.Context
	// Task is stored as any so the internal wheel is independent from public APIs.
	Task any

	bucket *Bucket
	prev   *TaskEntry
	next   *TaskEntry
}

// NewTaskEntry creates a scheduled entry with its absolute millisecond deadline.
func NewTaskEntry(deadline int64, ctx context.Context, task any) *TaskEntry {
	entry := &TaskEntry{
		Context: ctx,
		Task:    task,
	}
	entry.deadline.Store(deadline)
	entry.state.Store(int32(TaskStateScheduled))
	return entry
}

// Deadline returns the absolute millisecond deadline.
func (e *TaskEntry) Deadline() int64 {
	return e.deadline.Load()
}

// SetDeadline updates the absolute millisecond deadline used for retry.
func (e *TaskEntry) SetDeadline(deadline int64) {
	e.deadline.Store(deadline)
}

// State returns the entry lifecycle state.
func (e *TaskEntry) State() TaskState {
	return TaskState(e.state.Load())
}

// Cancel marks a scheduled entry as cancelled.
func (e *TaskEntry) Cancel() bool {
	return e.state.CompareAndSwap(
		int32(TaskStateScheduled),
		int32(TaskStateCancelled),
	)
}

// ClaimDispatch moves a scheduled entry into the dispatching state.
func (e *TaskEntry) ClaimDispatch() bool {
	return e.state.CompareAndSwap(
		int32(TaskStateScheduled),
		int32(TaskStateDispatching),
	)
}

// RestoreScheduled moves a rejected dispatch attempt back to scheduled state.
func (e *TaskEntry) RestoreScheduled(deadline int64) bool {
	if !e.state.CompareAndSwap(
		int32(TaskStateDispatching),
		int32(TaskStateScheduled),
	) {
		return false
	}
	e.SetDeadline(deadline)
	return true
}

// Expire marks a dispatching entry as accepted by the worker pool.
func (e *TaskEntry) Expire() bool {
	return e.state.CompareAndSwap(
		int32(TaskStateDispatching),
		int32(TaskStateExpired),
	)
}

// Reject marks a dispatching entry as rejected.
func (e *TaskEntry) Reject() bool {
	return e.state.CompareAndSwap(
		int32(TaskStateDispatching),
		int32(TaskStateRejected),
	)
}

// IsCancelled reports whether the entry has been cancelled.
func (e *TaskEntry) IsCancelled() bool {
	return e.State() == TaskStateCancelled
}

// IsExpired reports whether the entry has been accepted for dispatch.
func (e *TaskEntry) IsExpired() bool {
	return e.State() == TaskStateExpired
}
