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

// worker owns one long-lived goroutine and exits when the task queue closes.
func (p *Pool) worker() {
	defer p.workersDone.Done()

	for task := range p.queue.ch {
		p.run(task)
		p.metrics.queueDepth.Add(-1)
	}
}

// run is the panic boundary for user tasks.
//
// Workers keep serving later tasks after a panic. The optional PanicHandler can
// observe recovered values without making every caller wrap its own task.
func (p *Pool) run(task Task) {
	defer p.completeTask()
	defer p.recoverTask()

	task.Run()
}

func (p *Pool) recoverTask() {
	if recovered := recover(); recovered != nil {
		p.metrics.panickedTasks.Add(1)
		if p.panicHandler != nil {
			p.panicHandler.HandlePanic(recovered)
		}
	}
}

func (p *Pool) completeTask() {
	p.metrics.completedTasks.Add(1)
}
