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
	"testing"
	"time"
)

type collectEntriesHandler struct {
	entries *[]*TaskEntry
}

func (handler collectEntriesHandler) HandleEntry(entry *TaskEntry) {
	*handler.entries = append(*handler.entries, entry)
}

type seenEntriesHandler struct {
	seen map[*TaskEntry]bool
}

func (handler seenEntriesHandler) HandleEntry(entry *TaskEntry) {
	handler.seen[entry] = true
}

type cascadingEntriesHandler struct {
	wheel *TimingWheel
	due   *[]*TaskEntry
}

func (handler cascadingEntriesHandler) HandleEntry(entry *TaskEntry) {
	if handler.wheel.Add(entry) {
		*handler.due = append(*handler.due, entry)
	}
}

func TestClockHelpers(t *testing.T) {
	start := time.UnixMilli(1234)
	clock := NewManualClock(start)

	if got := StartTime(clock, 10*time.Millisecond); got != 1230 {
		t.Fatalf("StartTime() = %d, want 1230", got)
	}
	if got := Deadline(clock, 5*time.Millisecond); got != 1239 {
		t.Fatalf("Deadline() = %d, want 1239", got)
	}

	clock.Advance(6 * time.Millisecond)
	if got := clock.Now().UnixMilli(); got != 1240 {
		t.Fatalf("ManualClock Now() = %d, want 1240", got)
	}

	if got := StartTime(clock, time.Nanosecond); got != 1240 {
		t.Fatalf("StartTime(sub-millisecond tick) = %d, want 1240", got)
	}
}

func TestBucketDelayQueueOrdersByExpiration(t *testing.T) {
	queue := NewBucketDelayQueue()
	first := NewBucket()
	second := NewBucket()

	first.SetExpiration(20)
	second.SetExpiration(10)
	queue.Offer(first)
	queue.Offer(second)

	bucket, ok := queue.Poll(10)
	if !ok {
		t.Fatal("expected bucket")
	}
	if bucket != second {
		t.Fatalf("bucket = %p, want %p", bucket, second)
	}

	bucket, ok = queue.Poll(20)
	if !ok {
		t.Fatal("expected bucket")
	}
	if bucket != first {
		t.Fatalf("bucket = %p, want %p", bucket, first)
	}
}

func TestBucketDelayQueueSkipsStaleOffers(t *testing.T) {
	queue := NewBucketDelayQueue()
	bucket := NewBucket()

	bucket.SetExpiration(10)
	queue.Offer(bucket)
	bucket.SetExpiration(20)
	queue.Offer(bucket)

	got, ok := queue.Poll(10)
	if ok {
		t.Fatalf("expected stale offer skip, got %p", got)
	}

	got, ok = queue.Poll(20)
	if !ok {
		t.Fatal("expected current offer")
	}
	if got != bucket {
		t.Fatalf("bucket = %p, want %p", got, bucket)
	}
}

func TestBucketRemoveUpdatesMembership(t *testing.T) {
	bucket := NewBucket()
	entry := NewTaskEntry(10, nil, nil)
	bucket.Add(entry)

	if !bucket.Remove(entry) {
		t.Fatal("Remove() = false, want true")
	}
	if bucket.Len() != 0 {
		t.Fatalf("bucket len = %d, want 0", bucket.Len())
	}
	if entry.bucket != nil {
		t.Fatal("entry still points to bucket")
	}
	if bucket.Remove(entry) {
		t.Fatal("second Remove() = true, want false")
	}
}

func TestBucketFlushResetsExpiration(t *testing.T) {
	bucket := NewBucket()
	entry := NewTaskEntry(10, nil, nil)
	bucket.Add(entry)
	bucket.SetExpiration(10)

	var flushed []*TaskEntry
	bucket.Flush(collectEntriesHandler{entries: &flushed})

	if len(flushed) != 1 {
		t.Fatalf("flushed entries = %d, want 1", len(flushed))
	}
	if flushed[0] != entry {
		t.Fatalf("flushed entry = %p, want %p", flushed[0], entry)
	}
	if bucket.Expiration() != -1 {
		t.Fatalf("expiration = %d, want -1", bucket.Expiration())
	}
	if entry.bucket != nil {
		t.Fatal("entry still points to bucket")
	}
}

func TestTimingWheelFlushCancelsNestedEntries(t *testing.T) {
	queue := NewBucketDelayQueue()
	tw := NewTimingWheel(time.Millisecond, 8, 100, queue)
	root := NewTaskEntry(103, nil, nil)
	overflow := NewTaskEntry(125, nil, nil)

	if tw.Add(root) {
		t.Fatal("root entry due = true, want false")
	}
	if tw.Add(overflow) {
		t.Fatal("overflow entry due = true, want false")
	}

	seen := map[*TaskEntry]bool{}
	tw.Flush(seenEntriesHandler{seen: seen})

	if !seen[root] || !seen[overflow] {
		t.Fatalf("flushed entries = %#v, want root and overflow", seen)
	}
}

func TestTimingWheelAddReturnsDueForExpiredDeadline(t *testing.T) {
	queue := NewBucketDelayQueue()
	tw := NewTimingWheel(time.Millisecond, 8, 100, queue)
	entry := NewTaskEntry(100, nil, nil)

	if due := tw.Add(entry); !due {
		t.Fatal("Add() due = false, want true")
	}
}

func TestTimingWheelAddsEntryToBucket(t *testing.T) {
	queue := NewBucketDelayQueue()
	tw := NewTimingWheel(time.Millisecond, 8, 100, queue)
	entry := NewTaskEntry(103, nil, nil)

	if due := tw.Add(entry); due {
		t.Fatal("Add() due = true, want false")
	}

	bucket, ok := queue.Poll(103)
	if !ok {
		t.Fatal("expected bucket offer")
	}
	if bucket.Len() != 1 {
		t.Fatalf("bucket len = %d, want 1", bucket.Len())
	}
}

func TestTimingWheelCascadesOverflow(t *testing.T) {
	queue := NewBucketDelayQueue()
	tw := NewTimingWheel(time.Millisecond, 8, 100, queue)
	entry := NewTaskEntry(125, nil, nil)

	if due := tw.Add(entry); due {
		t.Fatal("Add() due = true, want false")
	}

	bucket, ok := queue.Poll(124)
	if !ok {
		t.Fatal("expected overflow bucket")
	}

	tw.AdvanceClock(bucket.Expiration())
	var due []*TaskEntry
	cascadingHandler := cascadingEntriesHandler{
		wheel: tw,
		due:   &due,
	}
	bucket.Flush(cascadingHandler)

	bucket, ok = queue.Poll(125)
	if !ok {
		t.Fatal("expected cascaded root bucket")
	}
	tw.AdvanceClock(bucket.Expiration())
	bucket.Flush(cascadingHandler)

	if len(due) != 1 {
		t.Fatalf("due entries = %d, want 1", len(due))
	}
	if due[0] != entry {
		t.Fatalf("due entry = %p, want %p", due[0], entry)
	}
}

func TestTaskEntryStateTransitions(t *testing.T) {
	type contextKey struct{}

	ctx := context.WithValue(context.Background(), contextKey{}, "value")
	entry := NewTaskEntry(1, ctx, "task")
	if entry.Context != ctx {
		t.Fatal("entry context was not retained")
	}
	if entry.Task != "task" {
		t.Fatalf("entry task = %v, want task", entry.Task)
	}
	if entry.State() != TaskStateScheduled {
		t.Fatalf("initial state = %v, want scheduled", entry.State())
	}
	entry.SetDeadline(2)
	if entry.Deadline() != 2 {
		t.Fatalf("deadline = %d, want 2", entry.Deadline())
	}

	cancelled := NewTaskEntry(1, nil, nil)
	if !cancelled.Cancel() {
		t.Fatal("Cancel() = false, want true")
	}
	if cancelled.Cancel() {
		t.Fatal("second Cancel() = true, want false")
	}

	expired := NewTaskEntry(1, nil, nil)
	if !expired.ClaimDispatch() {
		t.Fatal("ClaimDispatch() = false, want true")
	}
	if expired.Cancel() {
		t.Fatal("Cancel() after dispatch = true, want false")
	}
	if !expired.Expire() {
		t.Fatal("Expire() = false, want true")
	}

	retry := NewTaskEntry(1, nil, nil)
	if !retry.ClaimDispatch() {
		t.Fatal("ClaimDispatch() = false, want true")
	}
	if !retry.RestoreScheduled(2) {
		t.Fatal("RestoreScheduled() = false, want true")
	}
	if retry.Deadline() != 2 {
		t.Fatalf("deadline = %d, want 2", retry.Deadline())
	}

	rejected := NewTaskEntry(1, nil, nil)
	if !rejected.ClaimDispatch() {
		t.Fatal("ClaimDispatch() = false, want true")
	}
	if !rejected.Reject() {
		t.Fatal("Reject() = false, want true")
	}
	if rejected.Reject() {
		t.Fatal("second Reject() = true, want false")
	}
}
