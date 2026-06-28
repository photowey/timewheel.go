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

import "time"

// TimingWheel stores deadlines in fixed-size slots and cascades long delays to
// lazily-created overflow wheels.
type TimingWheel struct {
	tick        int64
	bucketCount int64
	interval    int64
	currentTime int64
	buckets     []*Bucket
	queue       *BucketDelayQueue
	overflow    *TimingWheel
}

// StartTime rounds the clock down to the nearest tick boundary.
func StartTime(clock Clock, tick time.Duration) int64 {
	tickMillis := int64(tick / time.Millisecond)
	if tickMillis <= 0 {
		tickMillis = 1
	}
	now := clock.Now().UnixMilli()
	return now - (now % tickMillis)
}

// Deadline returns the absolute millisecond deadline for delay.
func Deadline(clock Clock, delay time.Duration) int64 {
	return clock.Now().Add(delay).UnixMilli()
}

// NewTimingWheel creates one timing-wheel level.
//
// The interval covered by this level is tick * bucketCount. Deadlines outside
// that interval are delegated to an overflow wheel with a larger tick.
func NewTimingWheel(
	tick time.Duration,
	bucketCount int64,
	startTime int64,
	queue *BucketDelayQueue,
) *TimingWheel {
	tickMillis := int64(tick / time.Millisecond)
	if tickMillis <= 0 {
		tickMillis = 1
	}
	currentTime := startTime - (startTime % tickMillis)
	buckets := make([]*Bucket, bucketCount)
	for i := range buckets {
		buckets[i] = NewBucket()
	}

	return &TimingWheel{
		tick:        tickMillis,
		bucketCount: bucketCount,
		interval:    tickMillis * bucketCount,
		currentTime: currentTime,
		buckets:     buckets,
		queue:       queue,
	}
}

// Add inserts entry into this wheel level.
//
// It returns true when the entry is already due and should be dispatched by the
// caller. Otherwise the entry is stored in either this level or an overflow
// level and the corresponding bucket is offered to the delay queue.
func (tw *TimingWheel) Add(entry *TaskEntry) bool {
	deadline := entry.Deadline()
	if deadline < tw.currentTime+tw.tick {
		return true
	}

	if deadline < tw.currentTime+tw.interval {
		virtualID := deadline / tw.tick
		bucket := tw.buckets[virtualID%tw.bucketCount]
		bucket.Add(entry)
		// A bucket is offered only when its expiration changes; repeated entries
		// in the same slot share the existing heap offer.
		if bucket.SetExpiration(virtualID * tw.tick) {
			tw.queue.Offer(bucket)
		}
		return false
	}

	if tw.overflow == nil {
		// Overflow wheels make long delays cheap without allocating a huge root
		// wheel. Each higher level advances at the lower level's full interval.
		tw.overflow = NewTimingWheel(
			time.Duration(tw.interval)*time.Millisecond,
			tw.bucketCount,
			tw.currentTime,
			tw.queue,
		)
	}
	return tw.overflow.Add(entry)
}

// AdvanceClock moves this level and all overflow levels to now's tick boundary.
func (tw *TimingWheel) AdvanceClock(now int64) {
	if now < tw.currentTime+tw.tick {
		return
	}
	tw.currentTime = now - (now % tw.tick)
	if tw.overflow != nil {
		tw.overflow.AdvanceClock(now)
	}
}

// Flush removes all entries from this level and every overflow level.
func (tw *TimingWheel) Flush(handler EntryHandler) {
	for _, bucket := range tw.buckets {
		bucket.Flush(handler)
	}
	if tw.overflow != nil {
		tw.overflow.Flush(handler)
	}
}
