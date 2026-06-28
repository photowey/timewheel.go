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

package executor

import (
	"context"
	"time"
)

// MetricSink receives periodic immutable pool metric snapshots.
//
// Implementations must be fast and concurrency-safe. The pool recovers panics
// from the sink so observability code cannot stop task execution.
type MetricSink interface {
	ObservePoolMetrics(Metrics)
}

func (p *Pool) startMetricReporter() {
	if p.metricSink != nil {
		go p.reportPoolMetrics()
	}
}

func (p *Pool) reportPoolMetrics() {
	ticker := time.NewTicker(p.metricReportInterval)
	defer ticker.Stop()
	defer close(p.metricReporterDone)

	p.observePoolMetrics()
	for {
		select {
		case <-p.shutdownC:
			return
		case <-ticker.C:
			p.observePoolMetrics()
		}
	}
}

func (p *Pool) observePoolMetrics() {
	defer recoverMetricSinkPanic()

	p.metricSink.ObservePoolMetrics(p.Metrics())
}

func recoverMetricSinkPanic() {
	_ = recover()
}

func (p *Pool) waitMetricReporter(ctx context.Context) error {
	if p.metricReporterDone == nil {
		return nil
	}

	select {
	case <-p.metricReporterDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
