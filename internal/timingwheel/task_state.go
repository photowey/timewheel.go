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

// TaskState is the lifecycle state of an entry in the timing wheel.
type TaskState int32

const (
	// TaskStateUnknown is the zero value and is not assigned to live entries.
	TaskStateUnknown TaskState = iota
	// TaskStateScheduled means the entry is accepted and may still be cancelled.
	TaskStateScheduled
	// TaskStateCancelled means the entry was cancelled before dispatch.
	TaskStateCancelled
	// TaskStateDispatching means the scheduler claimed the entry for worker dispatch.
	TaskStateDispatching
	// TaskStateExpired means the entry was accepted by the worker pool.
	TaskStateExpired
	// TaskStateRejected means the entry could not be dispatched and will not retry.
	TaskStateRejected
)
