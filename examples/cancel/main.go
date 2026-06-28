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
	"sync/atomic"
	"time"

	"github.com/photowey/timewheel.go/pkg/timewheel"
)

type countRunTask struct {
	runs *atomic.Int64
}

func (task countRunTask) Run(context.Context) {
	task.runs.Add(1)
}

func main() {
	timer, err := timewheel.New(timewheel.WithTick(time.Millisecond))
	if err != nil {
		panic(err)
	}
	defer shutdownTimer(timer)

	var runs atomic.Int64
	timeout, err := timer.Schedule(
		context.Background(),
		50*time.Millisecond,
		countRunTask{runs: &runs},
	)
	if err != nil {
		panic(err)
	}

	cancelled := timeout.Cancel()
	time.Sleep(80 * time.Millisecond)

	fmt.Printf("cancelled=%t ran=%t pending=%d\n", cancelled, runs.Load() > 0, timer.Size())
}

func shutdownTimer(timer *timewheel.Timer) {
	_ = timer.Shutdown(context.Background())
}
