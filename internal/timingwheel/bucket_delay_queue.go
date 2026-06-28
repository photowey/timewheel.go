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

import "container/heap"

// bucketOffer snapshots a bucket expiration when it is pushed to the heap.
type bucketOffer struct {
	bucket     *Bucket
	expiration int64
}

type bucketHeap []bucketOffer

func (h bucketHeap) Len() int {
	return len(h)
}

func (h bucketHeap) Less(i, j int) bool {
	return h[i].expiration < h[j].expiration
}

func (h bucketHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *bucketHeap) Push(v any) {
	*h = append(*h, v.(bucketOffer))
}

func (h *bucketHeap) Pop() any {
	old := *h
	n := len(old)
	v := old[n-1]
	*h = old[:n-1]
	return v
}

// BucketDelayQueue orders buckets by expiration for the scheduler loop.
//
// Buckets are reused, so a bucket can have stale heap offers after its
// expiration changes. PeekExpiration and Poll discard those stale offers.
type BucketDelayQueue struct {
	heap bucketHeap
}

// NewBucketDelayQueue creates an empty bucket expiration heap.
func NewBucketDelayQueue() *BucketDelayQueue {
	q := &BucketDelayQueue{
		heap: bucketHeap{},
	}
	heap.Init(&q.heap)
	return q
}

// Offer pushes the bucket's current expiration into the delay queue.
func (q *BucketDelayQueue) Offer(bucket *Bucket) {
	heap.Push(&q.heap, bucketOffer{
		bucket:     bucket,
		expiration: bucket.Expiration(),
	})
}

// PeekExpiration returns the next live bucket expiration without removing it.
func (q *BucketDelayQueue) PeekExpiration() (int64, bool) {
	for q.heap.Len() > 0 {
		offer := q.heap[0]
		if offer.bucket.Expiration() != offer.expiration {
			heap.Pop(&q.heap)
			continue
		}
		return offer.expiration, true
	}
	return 0, false
}

// Poll returns the next live bucket when its expiration is due at or before now.
func (q *BucketDelayQueue) Poll(now int64) (*Bucket, bool) {
	for q.heap.Len() > 0 {
		offer := heap.Pop(&q.heap).(bucketOffer)
		if offer.bucket.Expiration() != offer.expiration {
			continue
		}
		if offer.expiration > now {
			heap.Push(&q.heap, offer)
			return nil, false
		}
		return offer.bucket, true
	}
	return nil, false
}
