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

import "sync/atomic"

// Metrics is an immutable snapshot of timer counters.
type Metrics struct {
	ScheduledTimeouts  int64
	ExpiredTimeouts    int64
	CancelledTimeouts  int64
	RejectedSchedules  int64
	RejectedDispatches int64
	PanickedTimeouts   int64
	PendingTimeouts    int64
	CommandQueueDepth  int64
	BucketOffers       int64
	BucketExpirations  int64
	MaxBucketDelay     int64
}

type metrics struct {
	scheduledTimeouts  atomic.Int64
	expiredTimeouts    atomic.Int64
	cancelledTimeouts  atomic.Int64
	rejectedSchedules  atomic.Int64
	rejectedDispatches atomic.Int64
	panickedTimeouts   atomic.Int64
	pendingTimeouts    atomic.Int64
	commandQueueDepth  atomic.Int64
	bucketOffers       atomic.Int64
	bucketExpirations  atomic.Int64
	maxBucketDelay     atomic.Int64
}

func (m *metrics) snapshot() Metrics {
	return Metrics{
		ScheduledTimeouts:  m.scheduledTimeouts.Load(),
		ExpiredTimeouts:    m.expiredTimeouts.Load(),
		CancelledTimeouts:  m.cancelledTimeouts.Load(),
		RejectedSchedules:  m.rejectedSchedules.Load(),
		RejectedDispatches: m.rejectedDispatches.Load(),
		PanickedTimeouts:   m.panickedTimeouts.Load(),
		PendingTimeouts:    m.pendingTimeouts.Load(),
		CommandQueueDepth:  m.commandQueueDepth.Load(),
		BucketOffers:       m.bucketOffers.Load(),
		BucketExpirations:  m.bucketExpirations.Load(),
		MaxBucketDelay:     m.maxBucketDelay.Load(),
	}
}
