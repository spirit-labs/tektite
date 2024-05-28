#!/bin/bash

rm testutils/*.txt; rm testutils/*.lock
pkill etcd
date > test-results.log
time go test ./... -count=1 -race -failfast -timeout 10m 2>&1 | tee -a test-results.log