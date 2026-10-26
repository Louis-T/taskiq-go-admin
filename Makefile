ifneq (,$(wildcard ./.env))
	include .env
	export
endif

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)

.PHONY: run build test lint docker

run:
	go run -ldflags "$(LDFLAGS)" ./cmd/taskiq-admin

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/taskiq-admin ./cmd/taskiq-admin

test:
	go test -race ./...

lint:
	test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	go vet ./...

docker:
	docker build --build-arg VERSION=$(VERSION) -t ghcr.io/louis-t/taskiq-go-admin .
