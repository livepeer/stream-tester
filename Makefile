SHELL=/bin/bash
GO_BUILD_DIR?=build/
ldflags := -X 'github.com/livepeer/stream-tester/model.Version=$(shell git describe --dirty)'

all: streamtester loadtester testdriver lapi mapi recordtester connector

# ldflags := -X 'github.com/livepeer/stream-tester/model.Version=$(shell git describe --dirty)' -X 'github.com/livepeer/stream-tester/model.IProduction=true'

.PHONY: streamtester
streamtester:
	go build -ldflags="$(ldflags)" -o "$(GO_BUILD_DIR)" cmd/streamtester/streamtester.go

.PHONY: loadtester
loadtester:
	go build -ldflags="$(ldflags)" -o "$(GO_BUILD_DIR)" cmd/loadtester/loadtester.go

.PHONY: testdriver
testdriver:
	go build -ldflags="$(ldflags)" -o "$(GO_BUILD_DIR)" cmd/testdriver/testdriver.go

.PHONY: lapi
lapi:
	go build -ldflags="$(ldflags)" -o "$(GO_BUILD_DIR)" cmd/lapi/lapi.go

.PHONY: mapi
mapi:
	go build -ldflags="$(ldflags)" -o "$(GO_BUILD_DIR)" cmd/mapi/mapi.go

.PHONY: recordtester
recordtester:
	go build -ldflags="$(ldflags)" -o "$(GO_BUILD_DIR)" cmd/recordtester/recordtester.go

.PHONY: connector
connector:
	go build -ldflags="$(ldflags)" -o "$(GO_BUILD_DIR)" cmd/mist-api-connector/mist-api-connector.go

.PHONY: stream-monitor
monitor:
	go build -ldflags="$(ldflags)" -o "$(GO_BUILD_DIR)" cmd/stream-monitor/stream-monitor.go

.PHONY: api-transcoder
api-transcoder:
	go build -ldflags="$(ldflags)" cmd/api-transcoder/api-transcoder.go

.PHONY: docker
docker:
	docker build -t livepeer/streamtester:latest --build-arg version=$(shell git describe --dirty) .

.PHONY: push
push:
	docker push livepeer/streamtester:latest

.PHONY: localdocker
localdocker:
	docker build -f Dockerfile.debian -t livepeer/streamtester:latest --build-arg version=$(shell git describe --dirty) .

.PHONY: release
release:
	@if [[ ! "$(version)" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-.+)?$$ ]]; then \
		echo "Must provide semantic version as arg to make." ; \
		echo "e.g. make release version=1.2.3-beta" ; \
		exit 1 ; \
	fi
	@git diff --quiet || { echo "Git working directory is dirty."; exit 1 ; }

	git tag -a v$(version) -m "Release v$(version)"
	git push origin v$(version)

	@echo "Release mist-api-connector? [y] "
	@read ans && [ $${ans:-y} = y ] || { echo "Mapic release aborted, branch not fast-forwarded."; exit 1 ; }

	git checkout mapic-release
	git merge --ff-only v$(version)
	git push origin mapic-release
