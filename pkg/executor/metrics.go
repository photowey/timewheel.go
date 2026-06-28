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

import "sync/atomic"

// Metrics is an immutable snapshot of pool counters.
type Metrics struct {
	SubmittedTasks int64
	CompletedTasks int64
	RejectedTasks  int64
	PanickedTasks  int64
	QueueDepth     int64
	Workers        int
}

type metrics struct {
	submittedTasks atomic.Int64
	completedTasks atomic.Int64
	rejectedTasks  atomic.Int64
	panickedTasks  atomic.Int64
	queueDepth     atomic.Int64
	workers        int
}

func (m *metrics) snapshot() Metrics {
	return Metrics{
		SubmittedTasks: m.submittedTasks.Load(),
		CompletedTasks: m.completedTasks.Load(),
		RejectedTasks:  m.rejectedTasks.Load(),
		PanickedTasks:  m.panickedTasks.Load(),
		QueueDepth:     m.queueDepth.Load(),
		Workers:        m.workers,
	}
}
