# Metric Sink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add optional snapshot-level metric sinks for `pkg/timewheel` and `pkg/executor`.

**Design Spec:** `docs/superpowers/specs/2026-06-29-metric-sink-design.md`

**Status:** Executed after design approval. Checkboxes below record completed implementation steps.

**Architecture:** Keep the existing pull-based `Metrics()` API. Add optional `MetricSink` interfaces and `WithMetricSink` / `WithMetricReportInterval` options. A reporter goroutine starts only when a sink is configured, periodically snapshots counters, recovers sink panics, and exits on shutdown.

**Tech Stack:** Go, standard library concurrency primitives, existing functional option pattern, existing `goleak` coverage.

---

### Task 1: Executor Metric Sink

**Files:**
- Create: `pkg/executor/metric_sink.go`
- Modify: `pkg/executor/option.go`
- Modify: `pkg/executor/pool.go`
- Test: `pkg/executor/pool_test.go`

- [x] Add tests for invalid nil sink, invalid report interval, snapshot delivery, panic recovery, and reporter shutdown.
- [x] Implement `MetricSink`, `WithMetricSink`, and `WithMetricReportInterval`.
- [x] Start the reporter only when a sink is configured.
- [x] Verify `go test ./pkg/executor -run 'Test(NewRejectsInvalidOptions|PoolMetricSink)' -count=1`.

### Task 2: Timewheel Metric Sink

**Files:**
- Create: `pkg/timewheel/metric_sink.go`
- Modify: `pkg/timewheel/option.go`
- Modify: `pkg/timewheel/timer.go`
- Test: `pkg/timewheel/timer_test.go`

- [x] Add tests for invalid nil sink, invalid report interval, snapshot delivery, panic recovery, and reporter shutdown.
- [x] Implement `MetricSink`, `WithMetricSink`, and `WithMetricReportInterval`.
- [x] Start the reporter only when a sink is configured.
- [x] Verify `go test ./pkg/timewheel -run 'Test(NewRejectsInvalidOptions|TimerMetricSink)' -count=1`.

### Task 3: Documentation And Verification

**Files:**
- Modify: `pkg/executor/doc.go`
- Modify: `pkg/timewheel/doc.go`
- Modify: `README.md`
- Modify: `docs/design.md`

- [x] Document that sinks are optional, snapshot-level, dependency-free, and must be fast and concurrency-safe.
- [x] Run `go test ./... -count=1`.
- [x] Run `go test -race ./... -count=1`.
- [x] Run `make ci`.
