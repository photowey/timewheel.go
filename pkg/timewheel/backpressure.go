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

// BackpressurePolicy controls schedule behavior when the command queue is full.
type BackpressurePolicy int

const (
	BackpressurePolicyUnknown BackpressurePolicy = iota
	// BackpressureReject returns ErrSaturated when the command queue is full.
	BackpressureReject
	// BackpressureBlock waits for command queue capacity, context cancellation,
	// or timer shutdown.
	BackpressureBlock
)

// ExpiredTaskPolicy controls behavior when an expired task cannot be submitted
// to the worker pool.
type ExpiredTaskPolicy int

const (
	ExpiredTaskPolicyUnknown ExpiredTaskPolicy = iota
	// ExpiredTaskReject rejects an expired timeout when worker dispatch is full.
	ExpiredTaskReject
	// ExpiredTaskRetry reschedules an expired timeout when worker dispatch is full.
	ExpiredTaskRetry
)
