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
	"fmt"
	"runtime"
	"time"

	"github.com/photowey/timewheel.go/pkg/executor"
)

const (
	defaultTick                 = time.Millisecond
	defaultBucketCount          = 512
	defaultCommandCapacity      = 65_536
	defaultMaxPending           = 1_000_000
	defaultMetricReportInterval = 10 * time.Second
)

// Option configures a Timer.
type Option func(*options) error

type poolConfigSource int

const (
	poolConfigSourceDefault poolConfigSource = iota
	poolConfigSourceOptions
	poolConfigSourcePool
)

type options struct {
	tick                  time.Duration
	bucketCount           int64
	commandCapacity       int
	maxPending            int64
	backpressurePolicy    BackpressurePolicy
	expiredTaskPolicy     ExpiredTaskPolicy
	expiredTaskRetryDelay time.Duration
	metricSink            MetricSink
	metricReportInterval  time.Duration

	bossOptions   []executor.Option
	workerOptions []executor.Option
	bossPool      *executor.Pool
	workerPool    *executor.Pool
	bossSource    poolConfigSource
	workerSource  poolConfigSource
}

func defaultOptions() options {
	return options{
		tick:                  defaultTick,
		bucketCount:           defaultBucketCount,
		commandCapacity:       defaultCommandCapacity,
		maxPending:            defaultMaxPending,
		backpressurePolicy:    BackpressureReject,
		expiredTaskPolicy:     ExpiredTaskReject,
		expiredTaskRetryDelay: defaultTick,
		metricReportInterval:  defaultMetricReportInterval,
		bossOptions: []executor.Option{
			executor.WithName("timewheel-boss"),
			executor.WithWorkers(1),
			executor.WithQueueCapacity(16),
			executor.WithRejectPolicy(executor.RejectPolicyReject),
		},
		workerOptions: []executor.Option{
			executor.WithName("timewheel-worker"),
			executor.WithWorkers(runtime.GOMAXPROCS(0)),
			executor.WithQueueCapacity(100_000),
			executor.WithRejectPolicy(executor.RejectPolicyReject),
		},
	}
}

// WithTick sets the base tick duration for the root timing wheel.
func WithTick(tick time.Duration) Option {
	return func(o *options) error {
		if tick <= 0 {
			return fmt.Errorf("timewheel: validate tick: %w", ErrInvalid)
		}
		o.tick = tick
		return nil
	}
}

// WithBucketCount sets the number of buckets per timing-wheel level.
func WithBucketCount(count int64) Option {
	return func(o *options) error {
		if count <= 0 {
			return fmt.Errorf("timewheel: validate bucket count: %w", ErrInvalid)
		}
		o.bucketCount = count
		return nil
	}
}

// WithCommandCapacity sets the bounded scheduler command queue capacity.
func WithCommandCapacity(capacity int) Option {
	return func(o *options) error {
		if capacity <= 0 {
			return fmt.Errorf("timewheel: validate command capacity: %w", ErrInvalid)
		}
		o.commandCapacity = capacity
		return nil
	}
}

// WithMaxPending sets the maximum number of accepted incomplete timeouts.
func WithMaxPending(maxPending int64) Option {
	return func(o *options) error {
		if maxPending <= 0 {
			return fmt.Errorf("timewheel: validate max pending: %w", ErrInvalid)
		}
		o.maxPending = maxPending
		return nil
	}
}

// WithBackpressurePolicy sets schedule behavior when the command queue is full.
func WithBackpressurePolicy(policy BackpressurePolicy) Option {
	return func(o *options) error {
		switch policy {
		case BackpressureReject, BackpressureBlock:
			o.backpressurePolicy = policy
			return nil
		default:
			return fmt.Errorf("timewheel: validate backpressure policy: %w", ErrInvalid)
		}
	}
}

// WithExpiredTaskPolicy sets dispatch behavior when the worker pool is full.
func WithExpiredTaskPolicy(policy ExpiredTaskPolicy) Option {
	return func(o *options) error {
		switch policy {
		case ExpiredTaskReject, ExpiredTaskRetry:
			o.expiredTaskPolicy = policy
			return nil
		default:
			return fmt.Errorf("timewheel: validate expired task policy: %w", ErrInvalid)
		}
	}
}

// WithExpiredTaskRetryDelay sets the delay used when ExpiredTaskRetry reschedules a timeout.
func WithExpiredTaskRetryDelay(delay time.Duration) Option {
	return func(o *options) error {
		if delay <= 0 {
			return fmt.Errorf("timewheel: validate expired task retry delay: %w", ErrInvalid)
		}
		o.expiredTaskRetryDelay = delay
		return nil
	}
}

// WithMetricSink sets the optional sink for periodic timer metric snapshots.
func WithMetricSink(sink MetricSink) Option {
	return func(o *options) error {
		if sink == nil {
			return fmt.Errorf("timewheel: validate metric sink: %w", ErrInvalid)
		}
		o.metricSink = sink
		return nil
	}
}

// WithMetricReportInterval sets how often a configured MetricSink receives snapshots.
func WithMetricReportInterval(interval time.Duration) Option {
	return func(o *options) error {
		if interval <= 0 {
			return fmt.Errorf("timewheel: validate metric report interval: %w", ErrInvalid)
		}
		o.metricReportInterval = interval
		return nil
	}
}

// WithBoss configures the timer-owned boss executor pool.
func WithBoss(opts ...executor.Option) Option {
	return func(o *options) error {
		if o.bossSource == poolConfigSourcePool {
			return fmt.Errorf("timewheel: validate boss pool: %w", ErrInvalid)
		}
		o.bossOptions = append([]executor.Option{}, opts...)
		o.bossSource = poolConfigSourceOptions
		return nil
	}
}

// WithWorker configures the timer-owned worker executor pool.
func WithWorker(opts ...executor.Option) Option {
	return func(o *options) error {
		if o.workerSource == poolConfigSourcePool {
			return fmt.Errorf("timewheel: validate worker pool: %w", ErrInvalid)
		}
		o.workerOptions = append([]executor.Option{}, opts...)
		o.workerSource = poolConfigSourceOptions
		return nil
	}
}

// WithBossPool injects a caller-owned boss executor pool.
func WithBossPool(pool *executor.Pool) Option {
	return func(o *options) error {
		if pool == nil || o.bossSource == poolConfigSourceOptions {
			return fmt.Errorf("timewheel: validate boss pool: %w", ErrInvalid)
		}
		o.bossPool = pool
		o.bossOptions = nil
		o.bossSource = poolConfigSourcePool
		return nil
	}
}

// WithWorkerPool injects a caller-owned worker executor pool.
func WithWorkerPool(pool *executor.Pool) Option {
	return func(o *options) error {
		if pool == nil || o.workerSource == poolConfigSourceOptions {
			return fmt.Errorf("timewheel: validate worker pool: %w", ErrInvalid)
		}
		o.workerPool = pool
		o.workerOptions = nil
		o.workerSource = poolConfigSourcePool
		return nil
	}
}
