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

import "github.com/photowey/timewheel.go/internal/timingwheel"

// Timeout is a handle for a scheduled task.
type Timeout struct {
	timer *Timer
	entry *timingwheel.TaskEntry
}

// Cancel prevents a scheduled timeout from being dispatched when it has not
// already been cancelled, rejected, expired, or claimed for dispatch.
func (t *Timeout) Cancel() bool {
	if t == nil || t.entry == nil {
		return false
	}
	if !t.entry.Cancel() {
		return false
	}
	if t.timer != nil {
		t.timer.cancel()
	}
	return true
}

// IsCancelled reports whether the timeout has been cancelled.
func (t *Timeout) IsCancelled() bool {
	if t == nil || t.entry == nil {
		return false
	}
	return t.entry.IsCancelled()
}

// IsExpired reports whether the timeout has been accepted for worker dispatch.
func (t *Timeout) IsExpired() bool {
	if t == nil || t.entry == nil {
		return false
	}
	return t.entry.IsExpired()
}
