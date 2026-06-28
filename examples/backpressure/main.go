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

package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/photowey/timewheel.go/pkg/executor"
	"github.com/photowey/timewheel.go/pkg/timewheel"
)

type noopTask struct{}

func (noopTask) Run(context.Context) {}

type blockingTask struct {
	started  chan<- struct{}
	released <-chan struct{}
}

func (task blockingTask) Run() {
	close(task.started)
	<-task.released
}

type releaseFunc struct {
	once     *sync.Once
	released chan struct{}
}

func (release releaseFunc) Release() {
	release.once.Do(release.close)
}

func (release releaseFunc) close() {
	close(release.released)
}

func main() {
	bossPool, releaseBoss, err := blockedBossPool()
	if err != nil {
		panic(err)
	}
	defer shutdownBlockedBossPool(bossPool, releaseBoss)

	timer, err := timewheel.New(
		timewheel.WithBossPool(bossPool),
		timewheel.WithCommandCapacity(1),
		timewheel.WithMaxPending(2),
		timewheel.WithBackpressurePolicy(timewheel.BackpressureReject),
	)
	if err != nil {
		panic(err)
	}
	defer shutdownTimer(timer, releaseBoss)

	if _, err := timer.Schedule(context.Background(), time.Hour, noopTask{}); err != nil {
		panic(err)
	}

	_, err = timer.Schedule(context.Background(), time.Hour, noopTask{})
	fmt.Printf("second schedule saturated=%t\n", errors.Is(err, timewheel.ErrSaturated))
	fmt.Printf("rejected=%d pending=%d\n", timer.Metrics().RejectedSchedules, timer.Size())
}

func blockedBossPool() (*executor.Pool, releaseFunc, error) {
	pool, err := executor.New(
		executor.WithName("blocked-boss"),
		executor.WithWorkers(1),
		executor.WithQueueCapacity(1),
		executor.WithRejectPolicy(executor.RejectPolicyReject),
	)
	if err != nil {
		return nil, releaseFunc{}, err
	}

	released := make(chan struct{})
	started := make(chan struct{})
	blocker := blockingTask{
		started:  started,
		released: released,
	}
	if err := pool.Execute(context.Background(), blocker); err != nil {
		_ = pool.Shutdown(context.Background())
		return nil, releaseFunc{}, err
	}
	<-started

	release := releaseFunc{
		once:     &sync.Once{},
		released: released,
	}

	return pool, release, nil
}

func shutdownBlockedBossPool(pool *executor.Pool, release releaseFunc) {
	release.Release()
	_ = pool.Shutdown(context.Background())
}

func shutdownTimer(timer *timewheel.Timer, release releaseFunc) {
	release.Release()
	_ = timer.Shutdown(context.Background())
}
