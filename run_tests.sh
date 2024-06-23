#!/bin/bash
date > test-results.log
time go run gotest.tools/gotestsum@latest -f testname -- -count=1 -race -failfast -timeout 10m ./... 2>&1 | tee -a test-results.log