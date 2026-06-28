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

import (
	"context"
	"time"
)

// MetricSink receives periodic immutable timer metric snapshots.
//
// Implementations must be fast and concurrency-safe. The timer recovers panics
// from the sink so observability code cannot stop scheduling.
type MetricSink interface {
	ObserveTimerMetrics(Metrics)
}

func (t *Timer) startMetricReporter() {
	if t.metricSink != nil {
		go t.reportTimerMetrics()
	}
}

func (t *Timer) reportTimerMetrics() {
	ticker := time.NewTicker(t.metricReportInterval)
	defer ticker.Stop()
	defer close(t.metricReporterDone)

	t.observeTimerMetrics()
	for {
		select {
		case <-t.shutdownC:
			return
		case <-ticker.C:
			t.observeTimerMetrics()
		}
	}
}

func (t *Timer) observeTimerMetrics() {
	defer recoverMetricSinkPanic()

	t.metricSink.ObserveTimerMetrics(t.Metrics())
}

func recoverMetricSinkPanic() {
	_ = recover()
}

func (t *Timer) waitMetricReporter(ctx context.Context) error {
	if t.metricReporterDone == nil {
		return nil
	}

	select {
	case <-t.metricReporterDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
