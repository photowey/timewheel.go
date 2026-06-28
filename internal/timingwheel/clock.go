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
	"sync"
	"time"
)

// Clock supplies time for the timing wheel.
type Clock interface {
	// Now returns the current wall-clock instant observed by the wheel.
	Now() time.Time
}

// RealClock reads time from the system clock.
type RealClock struct{}

var _ Clock = RealClock{}

// Now returns the current system time.
func (RealClock) Now() time.Time {
	return time.Now()
}

// ManualClock is a test clock whose time advances only when Advance is called.
type ManualClock struct {
	mu  sync.Mutex
	now time.Time
}

var _ Clock = (*ManualClock)(nil)

// NewManualClock creates a ManualClock pinned to now.
func NewManualClock(now time.Time) *ManualClock {
	return &ManualClock{now: now}
}

// Now returns the manual clock's current instant.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the manual clock forward by d.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
