# timewheel.go

`timewheel.go` provides a Kafka-style hierarchical timing wheel for delayed
task scheduling in Go. A timer advances time in a scheduler task and dispatches
expired user tasks to a separate worker pool, so slow task execution does not
block bucket advancement.

## Getting Started

```bash
go get github.com/photowey/timewheel.go
```

```go
package main

import (
	"context"
	"fmt"

	"github.com/photowey/timewheel.go/pkg/timewheel"
)

type printTask struct {
	done chan<- struct{}
}

func (task printTask) Run(context.Context) {
	fmt.Println("timeout fired")
	close(task.done)
}

func shutdownTimer(timer *timewheel.Timer) {
	_ = timer.Shutdown(context.Background())
}

func main() {
	timer, err := timewheel.New()
	if err != nil {
		panic(err)
	}
	defer shutdownTimer(timer)

	done := make(chan struct{})
	_, err = timer.Schedule(context.Background(), 0, printTask{done: done})
	if err != nil {
		panic(err)
	}

	<-done
}
```

## Packages

- `pkg/timewheel`: delayed scheduling API
- `pkg/executor`: bounded goroutine pool

## Features

- Hierarchical timing wheel with overflow cascade.
- Built-in boss and worker executor pools when no pools are supplied.
- Caller-owned pool injection through `WithBossPool` and `WithWorkerPool`.
- Bounded schedule command queue and max-pending quota.
- Reject or block backpressure policy for scheduling.
- Reject or retry policy for worker dispatch saturation.
- Logical cancellation through `Timeout.Cancel`.
- Metrics snapshots for timers and executor pools, with optional metric sinks.

## Documentation

- Design: [docs/design.md](docs/design.md)
- Executable example: [pkg/timewheel/example_test.go](pkg/timewheel/example_test.go)
- Examples:
  - [basic](examples/basic)
  - [cancel](examples/cancel)
  - [custom pools](examples/custom_pools)
  - [backpressure](examples/backpressure)
  - [metrics](examples/metrics)

## License

Apache License 2.0.
