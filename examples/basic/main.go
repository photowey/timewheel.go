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

	"github.com/photowey/timewheel.go/pkg/timewheel"
)

type printAndCloseTask struct {
	done chan<- struct{}
}

func (task printAndCloseTask) Run(context.Context) {
	fmt.Println("timeout fired")
	close(task.done)
}

func main() {
	timer, err := timewheel.New(
		timewheel.WithTick(time.Millisecond),
		timewheel.WithBucketCount(64),
	)
	if err != nil {
		panic(err)
	}
	defer shutdownTimer(timer)

	done := make(chan struct{})
	_, err = timer.Schedule(
		context.Background(),
		10*time.Millisecond,
		printAndCloseTask{done: done},
	)
	if err != nil {
		panic(err)
	}

	<-done
	fmt.Printf("expired=%d pending=%d\n", timer.Metrics().ExpiredTimeouts, timer.Size())
}

func shutdownTimer(timer *timewheel.Timer) {
	_ = timer.Shutdown(context.Background())
}
