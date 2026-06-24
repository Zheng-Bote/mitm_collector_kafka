#!/usr/bin/sh

MITM_VERSION=$(git describe --tags)

CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${MITM_VERSION}" -o ./bin/mitm_collector_kafka main.go

cp bin/mitm_collector_kafka ../../scheduler/mitm_scheduler/bin/.
