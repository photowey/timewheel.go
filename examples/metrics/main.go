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

	"github.com/photowey/timewheel.go/pkg/executor"
	"github.com/photowey/timewheel.go/pkg/timewheel"
)

type snapshotSink struct {
	timerSnapshots atomic.Int64
	poolSnapshots  atomic.Int64
}

func (sink *snapshotSink) ObserveTimerMetrics(metrics timewheel.Metrics) {
	if metrics.ScheduledTimeouts > 0 {
		sink.timerSnapshots.Add(1)
	}
}

func (sink *snapshotSink) ObservePoolMetrics(metrics executor.Metrics) {
	if metrics.Workers > 0 {
		sink.poolSnapshots.Add(1)
	}
}

type closeTask struct {
	done chan<- struct{}
}

func (task closeTask) Run(context.Context) {
	close(task.done)
}

func main() {
	sink := &snapshotSink{}

	timer, err := timewheel.New(
		timewheel.WithMetricSink(sink),
		timewheel.WithMetricReportInterval(time.Millisecond),
		timewheel.WithWorker(
			executor.WithMetricSink(sink),
			executor.WithMetricReportInterval(time.Millisecond),
		),
	)
	if err != nil {
		panic(err)
	}
	defer shutdownTimer(timer)

	done := make(chan struct{})
	_, err = timer.Schedule(context.Background(), 0, closeTask{done: done})
	if err != nil {
		panic(err)
	}
	<-done

	if err := waitForMetricSnapshots(sink); err != nil {
		panic(err)
	}
	fmt.Printf("timer observed=%t pool observed=%t\n",
		sink.timerSnapshots.Load() > 0,
		sink.poolSnapshots.Load() > 0,
	)
}

func shutdownTimer(timer *timewheel.Timer) {
	_ = timer.Shutdown(context.Background())
}

func waitForMetricSnapshots(sink *snapshotSink) error {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if sink.timerSnapshots.Load() > 0 && sink.poolSnapshots.Load() > 0 {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("metric snapshots were not observed")
}
