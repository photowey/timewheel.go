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
	defaultTick            = time.Millisecond
	defaultBucketCount     = 512
	defaultCommandCapacity = 65_536
	defaultMaxPending      = 1_000_000
)

// Option configures a Timer.
type Option interface {
	apply(*options) error
}

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

type tickOption time.Duration

func (opt tickOption) apply(o *options) error {
	if opt <= 0 {
		return fmt.Errorf("timewheel: validate tick: %w", ErrInvalid)
	}
	o.tick = time.Duration(opt)
	return nil
}

// WithTick sets the base tick duration for the root timing wheel.
func WithTick(tick time.Duration) Option {
	return tickOption(tick)
}

type bucketCountOption int64

func (opt bucketCountOption) apply(o *options) error {
	if opt <= 0 {
		return fmt.Errorf("timewheel: validate bucket count: %w", ErrInvalid)
	}
	o.bucketCount = int64(opt)
	return nil
}

// WithBucketCount sets the number of buckets per timing-wheel level.
func WithBucketCount(count int64) Option {
	return bucketCountOption(count)
}

type commandCapacityOption int

func (opt commandCapacityOption) apply(o *options) error {
	if opt <= 0 {
		return fmt.Errorf("timewheel: validate command capacity: %w", ErrInvalid)
	}
	o.commandCapacity = int(opt)
	return nil
}

// WithCommandCapacity sets the bounded scheduler command queue capacity.
func WithCommandCapacity(capacity int) Option {
	return commandCapacityOption(capacity)
}

type maxPendingOption int64

func (opt maxPendingOption) apply(o *options) error {
	if opt <= 0 {
		return fmt.Errorf("timewheel: validate max pending: %w", ErrInvalid)
	}
	o.maxPending = int64(opt)
	return nil
}

// WithMaxPending sets the maximum number of accepted incomplete timeouts.
func WithMaxPending(maxPending int64) Option {
	return maxPendingOption(maxPending)
}

type backpressurePolicyOption BackpressurePolicy

func (opt backpressurePolicyOption) apply(o *options) error {
	switch policy := BackpressurePolicy(opt); policy {
	case BackpressureReject, BackpressureBlock:
		o.backpressurePolicy = policy
		return nil
	default:
		return fmt.Errorf("timewheel: validate backpressure policy: %w", ErrInvalid)
	}
}

// WithBackpressurePolicy sets schedule behavior when the command queue is full.
func WithBackpressurePolicy(policy BackpressurePolicy) Option {
	return backpressurePolicyOption(policy)
}

type expiredTaskPolicyOption ExpiredTaskPolicy

func (opt expiredTaskPolicyOption) apply(o *options) error {
	switch policy := ExpiredTaskPolicy(opt); policy {
	case ExpiredTaskReject, ExpiredTaskRetry:
		o.expiredTaskPolicy = policy
		return nil
	default:
		return fmt.Errorf("timewheel: validate expired task policy: %w", ErrInvalid)
	}
}

// WithExpiredTaskPolicy sets dispatch behavior when the worker pool is full.
func WithExpiredTaskPolicy(policy ExpiredTaskPolicy) Option {
	return expiredTaskPolicyOption(policy)
}

type expiredTaskRetryDelayOption time.Duration

func (opt expiredTaskRetryDelayOption) apply(o *options) error {
	if opt <= 0 {
		return fmt.Errorf("timewheel: validate expired task retry delay: %w", ErrInvalid)
	}
	o.expiredTaskRetryDelay = time.Duration(opt)
	return nil
}

// WithExpiredTaskRetryDelay sets the delay used when ExpiredTaskRetry reschedules a timeout.
func WithExpiredTaskRetryDelay(delay time.Duration) Option {
	return expiredTaskRetryDelayOption(delay)
}

type bossOptionsOption struct {
	opts []executor.Option
}

func (opt bossOptionsOption) apply(o *options) error {
	if o.bossSource == poolConfigSourcePool {
		return fmt.Errorf("timewheel: validate boss pool: %w", ErrInvalid)
	}
	o.bossOptions = append([]executor.Option{}, opt.opts...)
	o.bossSource = poolConfigSourceOptions
	return nil
}

// WithBoss configures the timer-owned boss executor pool.
func WithBoss(opts ...executor.Option) Option {
	return bossOptionsOption{opts: opts}
}

type workerOptionsOption struct {
	opts []executor.Option
}

func (opt workerOptionsOption) apply(o *options) error {
	if o.workerSource == poolConfigSourcePool {
		return fmt.Errorf("timewheel: validate worker pool: %w", ErrInvalid)
	}
	o.workerOptions = append([]executor.Option{}, opt.opts...)
	o.workerSource = poolConfigSourceOptions
	return nil
}

// WithWorker configures the timer-owned worker executor pool.
func WithWorker(opts ...executor.Option) Option {
	return workerOptionsOption{opts: opts}
}

type bossPoolOption struct {
	pool *executor.Pool
}

func (opt bossPoolOption) apply(o *options) error {
	if opt.pool == nil || o.bossSource == poolConfigSourceOptions {
		return fmt.Errorf("timewheel: validate boss pool: %w", ErrInvalid)
	}
	o.bossPool = opt.pool
	o.bossOptions = nil
	o.bossSource = poolConfigSourcePool
	return nil
}

// WithBossPool injects a caller-owned boss executor pool.
func WithBossPool(pool *executor.Pool) Option {
	return bossPoolOption{pool: pool}
}

type workerPoolOption struct {
	pool *executor.Pool
}

func (opt workerPoolOption) apply(o *options) error {
	if opt.pool == nil || o.workerSource == poolConfigSourceOptions {
		return fmt.Errorf("timewheel: validate worker pool: %w", ErrInvalid)
	}
	o.workerPool = opt.pool
	o.workerOptions = nil
	o.workerSource = poolConfigSourcePool
	return nil
}

// WithWorkerPool injects a caller-owned worker executor pool.
func WithWorkerPool(pool *executor.Pool) Option {
	return workerPoolOption{pool: pool}
}
