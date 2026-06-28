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

const unsetExpiration int64 = -1

// EntryHandler handles entries flushed from a bucket or timing wheel.
type EntryHandler interface {
	HandleEntry(entry *TaskEntry)
}

// Bucket groups entries that share the same wheel slot expiration.
//
// Entries are kept in an intrusive circular list so moving an entry between
// buckets can unlink it without allocating another list node.
type Bucket struct {
	expiration int64
	root       TaskEntry
	len        int
}

// NewBucket creates an empty bucket with an unset expiration.
func NewBucket() *Bucket {
	b := &Bucket{
		expiration: unsetExpiration,
	}
	b.root.prev = &b.root
	b.root.next = &b.root
	return b
}

// Expiration returns the absolute millisecond expiration for this bucket.
func (b *Bucket) Expiration() int64 {
	return b.expiration
}

// SetExpiration updates the bucket expiration and reports whether it changed.
func (b *Bucket) SetExpiration(expiration int64) bool {
	if b.expiration == expiration {
		return false
	}
	b.expiration = expiration
	return true
}

// Len returns the number of entries currently linked into the bucket.
func (b *Bucket) Len() int {
	return b.len
}

// Add links entry into the bucket, removing it from any previous bucket first.
func (b *Bucket) Add(entry *TaskEntry) {
	if entry.bucket != nil {
		entry.bucket.Remove(entry)
	}

	tail := b.root.prev
	entry.next = &b.root
	entry.prev = tail
	tail.next = entry
	b.root.prev = entry
	entry.bucket = b
	b.len++
}

// Remove unlinks entry from the bucket if it belongs to this bucket.
func (b *Bucket) Remove(entry *TaskEntry) bool {
	if entry.bucket != b {
		return false
	}

	entry.prev.next = entry.next
	entry.next.prev = entry.prev
	entry.prev = nil
	entry.next = nil
	entry.bucket = nil
	b.len--
	return true
}

// Flush removes every entry and passes it to fn, then clears the bucket deadline.
func (b *Bucket) Flush(handler EntryHandler) {
	for b.len > 0 {
		entry := b.root.next
		b.Remove(entry)
		handler.HandleEntry(entry)
	}
	b.expiration = unsetExpiration
}
