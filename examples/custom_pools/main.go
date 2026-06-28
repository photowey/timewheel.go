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
	"fmt"
	"time"

	"github.com/photowey/timewheel.go/pkg/executor"
	"github.com/photowey/timewheel.go/pkg/timewheel"
)

type customWorkerTask struct {
	done chan<- struct{}
}

func (task customWorkerTask) Run(context.Context) {
	fmt.Println("custom worker executed timeout")
	close(task.done)
}

func main() {
	bossPool, err := executor.New(
		executor.WithName("example-boss"),
		executor.WithWorkers(1),
		executor.WithQueueCapacity(16),
	)
	if err != nil {
		panic(err)
	}
	defer shutdownPool(bossPool)

	workerPool, err := executor.New(
		executor.WithName("example-worker"),
		executor.WithWorkers(2),
		executor.WithQueueCapacity(32),
	)
	if err != nil {
		panic(err)
	}
	defer shutdownPool(workerPool)

	timer, err := timewheel.New(
		timewheel.WithBossPool(bossPool),
		timewheel.WithWorkerPool(workerPool),
		timewheel.WithTick(time.Millisecond),
	)
	if err != nil {
		panic(err)
	}
	defer shutdownTimer(timer)

	done := make(chan struct{})
	_, err = timer.Schedule(
		context.Background(),
		5*time.Millisecond,
		customWorkerTask{done: done},
	)
	if err != nil {
		panic(err)
	}

	<-done
	fmt.Printf("worker completed=%d\n", workerPool.Metrics().CompletedTasks)
}

func shutdownPool(pool *executor.Pool) {
	_ = pool.Shutdown(context.Background())
}

func shutdownTimer(timer *timewheel.Timer) {
	_ = timer.Shutdown(context.Background())
}
