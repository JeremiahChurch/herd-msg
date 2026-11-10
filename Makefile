# Prefer a pinned toolchain from golang.org/dl (installed under ~/sdk) when the
# system Go is older than go.mod requires. Override with make GO=/path/to/go.
GO ?= $(if $(wildcard $(HOME)/sdk/go1.26.4/bin/go),$(HOME)/sdk/go1.26.4/bin/go,go)
VERSION ?= $(shell git describe --tags --always --dirty)
LDFLAGS := -s -w -X main.version=$(VERSION)

export CGO_ENABLED := 0

.PHONY: build test vet install dist

build:
	mkdir -p dist
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o dist/herd-msg ./cmd/herd-msg

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

install: build
	env -u CODEX_THREAD_ID -u CLAUDE_CODE_SESSION_ID dist/herd-msg setup

dist:
	mkdir -p dist
	GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o dist/herd-msg-linux-amd64 ./cmd/herd-msg

.PHONY: test-live
test-live:
	$(GO) test -tags live ./internal/e2e -count=1 -v -timeout 16m
