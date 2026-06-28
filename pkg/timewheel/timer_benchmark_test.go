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
	"context"
	"sync"
	"testing"
	"time"
)

type benchmarkNoopTask struct{}

func (benchmarkNoopTask) Run(context.Context) {}

type benchmarkDoneTask struct {
	wg *sync.WaitGroup
}

func (task benchmarkDoneTask) Run(context.Context) {
	task.wg.Done()
}

func BenchmarkScheduleLongDelay(b *testing.B) {
	timer, err := New(
		WithTick(time.Millisecond),
		WithCommandCapacity(b.N+1),
		WithMaxPending(int64(b.N+1)),
	)
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}
	defer shutdownBenchmarkTimer(b, timer)

	task := benchmarkNoopTask{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := timer.Schedule(context.Background(), time.Hour, task); err != nil {
			b.Fatalf("Schedule() error = %v", err)
		}
	}
}

func BenchmarkScheduleZeroDelay(b *testing.B) {
	timer, err := New(
		WithTick(time.Millisecond),
		WithCommandCapacity(b.N+1),
		WithMaxPending(int64(b.N+1)),
	)
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}
	defer shutdownBenchmarkTimer(b, timer)

	var wg sync.WaitGroup
	wg.Add(b.N)
	task := benchmarkDoneTask{wg: &wg}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := timer.Schedule(context.Background(), 0, task); err != nil {
			b.Fatalf("Schedule() error = %v", err)
		}
	}
	wg.Wait()
}

func shutdownBenchmarkTimer(b *testing.B, timer *Timer) {
	b.Helper()

	if err := timer.Shutdown(context.Background()); err != nil {
		b.Fatalf("Shutdown() error = %v", err)
	}
}
