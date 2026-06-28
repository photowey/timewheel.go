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

// Package executor provides a bounded goroutine pool used by timewheel and
// available for general task execution.
//
// A Pool accepts Task values, runs them on a fixed worker set, and applies the
// configured reject policy when the queue is full. Worker boundaries recover
// panics, update metrics, and keep the pool alive for later tasks.
//
// Metrics are available through Metrics. Configure WithMetricSink to push
// periodic snapshots to application-owned observability adapters.
package executor
