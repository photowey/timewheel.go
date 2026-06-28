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

// Package timewheel provides delayed task scheduling with a Kafka-style
// hierarchical timing wheel.
//
// A Timer uses one scheduler goroutine submitted to a boss executor pool to
// advance buckets and dispatch expired timeouts. User tasks run on a worker
// executor pool, so long-running tasks do not block time advancement.
//
// Use New without options for the built-in pools, or inject caller-owned pools
// with WithBossPool and WithWorkerPool when the application already owns its
// execution resources.
//
// Metrics are available through Metrics. Configure WithMetricSink to push
// periodic snapshots to application-owned observability adapters.
package timewheel
