SHELL=/bin/bash

all: streamtester

ldflags := -X 'github.com/livepeer/stream-tester/model.Version=$(shell git describe --dirty)'

.PHONY: streamtester
streamtester:
	go build -ldflags="$(ldflags)" cmd/streamtester/streamtester.go

.PHONY: docker
docker:
	docker build -t livepeer/streamtester:latest --build-arg version=$(shell git describe --dirty) .

.PHONY: push
push:
	docker push livepeer/streamtester:latest

.PHONY: localdocker
localdocker:
	docker build -f Dockerfile.debian -t livepeer/streamtester:latest --build-arg version=$(shell git describe --dirty) .