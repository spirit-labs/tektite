ROOT = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
BUILD_DIR = $(ROOT)/out
CHANNEL ?= canary
VERSION ?= $(shell git describe --tags --dirty  --always)
GOOS ?= $(shell ./bin/go version | awk '{print $$NF}' | cut -d/ -f1)
GOARCH ?= $(shell ./bin/go version | awk '{print $$NF}' | cut -d/ -f2)

.PHONY: all protos build test docker-image start stop create-topics publish-payments connect status

all: protos build

protos:
	$(MAKE) -C ./protos
