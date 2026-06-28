SHELL := /bin/sh

.DEFAULT_GOAL := help

GO ?= go
GIT ?= git
GOFMT ?= gofmt
GOLANGCI_LINT ?= golangci-lint

COVERPROFILE ?= .tmp/coverage.out
BENCH ?= BenchmarkSchedule
BENCHTIME ?= 100ms
BENCHCOUNT ?= 1
LINT_TIMEOUT ?= 10m

.PHONY: help
help:
	@printf '%s\n' 'Timewheel.go development targets:'
	@printf '%s\n' '  make ci           run local release-readiness checks'
	@printf '%s\n' '  make test         run tests with shuffle and coverage'
	@printf '%s\n' '  make race         run race tests'
	@printf '%s\n' '  make vet          run go vet'
	@printf '%s\n' '  make lint         run golangci-lint'
	@printf '%s\n' '  make fmt          format Go packages'
	@printf '%s\n' '  make fmt-check    verify gofmt output'
	@printf '%s\n' '  make examples     run executable examples'
	@printf '%s\n' '  make bench-smoke  run the CI benchmark smoke test'
	@printf '%s\n' '  make bench        run the local benchmark suite'
	@printf '%s\n' '  make clean        remove generated local artifacts'

.PHONY: ci
ci: tidy-check fmt-check test race vet lint examples bench-smoke

.PHONY: mod-download
mod-download:
	$(GO) mod download

.PHONY: tidy
tidy:
	$(GO) mod tidy

.PHONY: tidy-check
tidy-check:
	$(GO) mod tidy
	$(GIT) diff --exit-code -- go.mod go.sum

.PHONY: fmt
fmt:
	$(GO) fmt ./...

.PHONY: fmt-check
fmt-check:
	@files="$$($(GOFMT) -l .)"; \
	if [ -n "$$files" ]; then \
		printf '%s\n' 'The following Go files are not formatted:'; \
		printf '%s\n' "$$files"; \
		exit 1; \
	fi

.PHONY: test
test:
	mkdir -p .tmp
	$(GO) test -shuffle=on -coverprofile=$(COVERPROFILE) ./...

.PHONY: coverage
coverage: test
	$(GO) tool cover -func=$(COVERPROFILE)

.PHONY: race
race:
	$(GO) test -race -shuffle=on ./...

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: lint
lint:
	$(GOLANGCI_LINT) run --timeout=$(LINT_TIMEOUT)

.PHONY: examples
examples:
	$(GO) run ./examples/basic
	$(GO) run ./examples/cancel
	$(GO) run ./examples/custom_pools
	$(GO) run ./examples/backpressure

.PHONY: bench-smoke
bench-smoke:
	$(GO) test -run '^$$' -bench='$(BENCH)' -benchmem -benchtime=$(BENCHTIME) -count=1 ./pkg/timewheel

.PHONY: bench
bench:
	$(GO) test -run '^$$' -bench=$(BENCH) -benchmem -benchtime=$(BENCHTIME) -count=$(BENCHCOUNT) ./...

.PHONY: clean
clean:
	rm -rf .tmp coverage.out coverage.html *.test bench.txt
