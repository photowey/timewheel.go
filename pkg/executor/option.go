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
)

const (
	defaultName          = "executor"
	defaultQueueCapacity = 1024
)

// Option configures a Pool.
type Option interface {
	apply(*options) error
}

type options struct {
	name          string
	workers       int
	queueCapacity int
	rejectPolicy  RejectPolicy
	panicHandler  PanicHandler
}

func defaultOptions() options {
	return options{
		name:          defaultName,
		workers:       runtime.GOMAXPROCS(0),
		queueCapacity: defaultQueueCapacity,
		rejectPolicy:  RejectPolicyReject,
	}
}

type nameOption string

func (opt nameOption) apply(o *options) error {
	if opt == "" {
		return fmt.Errorf("executor: validate name: %w", ErrInvalid)
	}
	o.name = string(opt)
	return nil
}

// WithName sets the pool name.
func WithName(name string) Option {
	return nameOption(name)
}

type workersOption int

func (opt workersOption) apply(o *options) error {
	if opt <= 0 {
		return fmt.Errorf("executor: validate workers: %w", ErrInvalid)
	}
	o.workers = int(opt)
	return nil
}

// WithWorkers sets the fixed worker count.
func WithWorkers(workers int) Option {
	return workersOption(workers)
}

type queueCapacityOption int

func (opt queueCapacityOption) apply(o *options) error {
	if opt < 0 {
		return fmt.Errorf("executor: validate queue capacity: %w", ErrInvalid)
	}
	o.queueCapacity = int(opt)
	return nil
}

// WithQueueCapacity sets the bounded task queue capacity.
func WithQueueCapacity(capacity int) Option {
	return queueCapacityOption(capacity)
}

type rejectPolicyOption RejectPolicy

func (opt rejectPolicyOption) apply(o *options) error {
	switch policy := RejectPolicy(opt); policy {
	case RejectPolicyReject, RejectPolicyBlock:
		o.rejectPolicy = policy
		return nil
	default:
		return fmt.Errorf("executor: validate reject policy: %w", ErrInvalid)
	}
}

// WithRejectPolicy sets queue-full behavior.
func WithRejectPolicy(policy RejectPolicy) Option {
	return rejectPolicyOption(policy)
}

type panicHandlerOption struct {
	handler PanicHandler
}

func (opt panicHandlerOption) apply(o *options) error {
	o.panicHandler = opt.handler
	return nil
}

// WithPanicHandler sets the callback invoked after worker panic recovery.
func WithPanicHandler(handler PanicHandler) Option {
	return panicHandlerOption{handler: handler}
}
