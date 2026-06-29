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
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Pool is a bounded goroutine pool.
type Pool struct {
	name          string
	workers       int
	queueCapacity int
	rejectPolicy  RejectPolicy
	panicHandler  PanicHandler
	metricSink    MetricSink

	metricReportInterval time.Duration

	queue              queue
	metrics            metrics
	workersDone        sync.WaitGroup
	submitsDone        sync.WaitGroup
	done               chan struct{}
	shutdownC          chan struct{}
	metricReporterDone chan struct{}
	submitMu           sync.Mutex
	isClosed           atomic.Bool
	closeOnce          sync.Once
}

// New creates a Pool and starts its worker goroutines.
func New(opts ...Option) (*Pool, error) {
	cfg := defaultOptions()
	for _, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("executor: validate option: %w", ErrInvalid)
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	p := &Pool{
		name:          cfg.name,
		workers:       cfg.workers,
		queueCapacity: cfg.queueCapacity,
		rejectPolicy:  cfg.rejectPolicy,
		panicHandler:  cfg.panicHandler,
		metricSink:    cfg.metricSink,

		metricReportInterval: cfg.metricReportInterval,
		queue:                newQueue(cfg.queueCapacity),
		done:                 make(chan struct{}),
		shutdownC:            make(chan struct{}),
	}
	p.metrics.workers = cfg.workers
	if cfg.metricSink != nil {
		p.metricReporterDone = make(chan struct{})
	}

	p.workersDone.Add(cfg.workers)
	for range cfg.workers {
		go p.worker()
	}
	go p.closeDoneWhenWorkersExit()
	p.startMetricReporter()

	return p, nil
}

func (p *Pool) closeDoneWhenWorkersExit() {
	p.workersDone.Wait()
	close(p.done)
}

// Execute submits task according to the pool reject policy.
func (p *Pool) Execute(ctx context.Context, task Task) error {
	if ctx == nil {
		return fmt.Errorf("executor: execute: %w", ErrInvalid)
	}
	if task == nil {
		return fmt.Errorf("executor: execute task: %w", ErrInvalid)
	}

	switch p.rejectPolicy {
	case RejectPolicyBlock:
		return p.executeBlocking(ctx, task)
	default:
		return p.TryExecute(task)
	}
}

func (p *Pool) executeBlocking(ctx context.Context, task Task) error {
	if err := p.beginSubmit(); err != nil {
		p.metrics.rejectedTasks.Add(1)
		return fmt.Errorf("executor: execute: %w", ErrClosed)
	}
	defer p.endSubmit()

	select {
	case p.queue.ch <- task:
		p.metrics.submittedTasks.Add(1)
		p.metrics.queueDepth.Add(1)
		return nil
	case <-ctx.Done():
		p.metrics.rejectedTasks.Add(1)
		return ctx.Err()
	case <-p.shutdownC:
		p.metrics.rejectedTasks.Add(1)
		return fmt.Errorf("executor: execute: %w", ErrClosed)
	}
}

// trySubmit attempts one non-blocking task handoff under the submit gate.
func (p *Pool) trySubmit(task Task) error {
	if err := p.beginSubmit(); err != nil {
		return err
	}
	defer p.endSubmit()

	select {
	case p.queue.ch <- task:
		p.metrics.submittedTasks.Add(1)
		p.metrics.queueDepth.Add(1)
		return nil
	default:
		return ErrSaturated
	}
}

func (p *Pool) beginSubmit() error {
	p.submitMu.Lock()
	defer p.submitMu.Unlock()

	if p.isClosed.Load() {
		return ErrClosed
	}
	p.submitsDone.Add(1)
	return nil
}

func (p *Pool) endSubmit() {
	p.submitsDone.Done()
}

func (p *Pool) rejectTryExecute(err error) error {
	p.metrics.rejectedTasks.Add(1)
	if err == ErrClosed {
		return fmt.Errorf("executor: try execute: %w", ErrClosed)
	}
	if err == ErrSaturated {
		return fmt.Errorf("executor: try execute: %w", ErrSaturated)
	}
	return fmt.Errorf("executor: try execute: %w", err)
}

// TryExecute submits task without blocking.
func (p *Pool) TryExecute(task Task) error {
	if task == nil {
		return fmt.Errorf("executor: try execute task: %w", ErrInvalid)
	}

	if err := p.trySubmit(task); err != nil {
		return p.rejectTryExecute(err)
	}
	return nil
}

// Shutdown stops accepting tasks and waits for accepted tasks to drain.
func (p *Pool) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("executor: shutdown: %w", ErrInvalid)
	}

	p.closeOnce.Do(p.closeQueue)

	select {
	case <-p.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return p.waitMetricReporter(ctx)
}

// Metrics returns an immutable point-in-time snapshot of pool counters.
func (p *Pool) Metrics() Metrics {
	return p.metrics.snapshot()
}

func (p *Pool) closeQueue() {
	p.submitMu.Lock()
	p.isClosed.Store(true)
	close(p.shutdownC)
	p.submitMu.Unlock()
	p.submitsDone.Wait()
	p.queue.close()
}
