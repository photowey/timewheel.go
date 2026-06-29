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
	"fmt"
	"runtime"
	"time"
)

const (
	defaultName                 = "executor"
	defaultQueueCapacity        = 1024
	defaultMetricReportInterval = 10 * time.Second
)

// Option configures a Pool.
type Option func(*options) error

type options struct {
	name          string
	workers       int
	queueCapacity int
	rejectPolicy  RejectPolicy
	panicHandler  PanicHandler

	metricSink           MetricSink
	metricReportInterval time.Duration
}

func defaultOptions() options {
	return options{
		name:                 defaultName,
		workers:              runtime.GOMAXPROCS(0),
		queueCapacity:        defaultQueueCapacity,
		rejectPolicy:         RejectPolicyReject,
		metricReportInterval: defaultMetricReportInterval,
	}
}

// WithName sets the pool name.
func WithName(name string) Option {
	return func(o *options) error {
		if name == "" {
			return fmt.Errorf("executor: validate name: %w", ErrInvalid)
		}
		o.name = name
		return nil
	}
}

// WithWorkers sets the fixed worker count.
func WithWorkers(workers int) Option {
	return func(o *options) error {
		if workers <= 0 {
			return fmt.Errorf("executor: validate workers: %w", ErrInvalid)
		}
		o.workers = workers
		return nil
	}
}

// WithQueueCapacity sets the bounded task queue capacity.
func WithQueueCapacity(capacity int) Option {
	return func(o *options) error {
		if capacity < 0 {
			return fmt.Errorf("executor: validate queue capacity: %w", ErrInvalid)
		}
		o.queueCapacity = capacity
		return nil
	}
}

// WithRejectPolicy sets queue-full behavior.
func WithRejectPolicy(policy RejectPolicy) Option {
	return func(o *options) error {
		switch policy {
		case RejectPolicyReject, RejectPolicyBlock:
			o.rejectPolicy = policy
			return nil
		default:
			return fmt.Errorf("executor: validate reject policy: %w", ErrInvalid)
		}
	}
}

// WithPanicHandler sets the callback invoked after worker panic recovery.
func WithPanicHandler(handler PanicHandler) Option {
	return func(o *options) error {
		o.panicHandler = handler
		return nil
	}
}

// WithMetricSink sets the optional sink for periodic pool metric snapshots.
func WithMetricSink(sink MetricSink) Option {
	return func(o *options) error {
		if sink == nil {
			return fmt.Errorf("executor: validate metric sink: %w", ErrInvalid)
		}
		o.metricSink = sink
		return nil
	}
}

// WithMetricReportInterval sets how often a configured MetricSink receives snapshots.
func WithMetricReportInterval(interval time.Duration) Option {
	return func(o *options) error {
		if interval <= 0 {
			return fmt.Errorf("executor: validate metric report interval: %w", ErrInvalid)
		}
		o.metricReportInterval = interval
		return nil
	}
}
