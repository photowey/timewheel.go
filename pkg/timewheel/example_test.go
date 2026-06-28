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

package timewheel_test

import (
	"context"
	"fmt"

	"github.com/photowey/timewheel.go/pkg/timewheel"
)

type exampleTask struct {
	done chan<- struct{}
}

func (task exampleTask) Run(context.Context) {
	fmt.Println("timeout fired")
	close(task.done)
}

func ExampleTimer_Schedule() {
	timer, err := timewheel.New()
	if err != nil {
		panic(err)
	}
	defer shutdownTimer(timer)

	done := make(chan struct{})
	_, err = timer.Schedule(context.Background(), 0, exampleTask{done: done})
	if err != nil {
		panic(err)
	}

	<-done

	// Output:
	// timeout fired
}

func shutdownTimer(timer *timewheel.Timer) {
	_ = timer.Shutdown(context.Background())
}
