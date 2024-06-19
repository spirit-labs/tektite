#!/bin/bash
date > test-results.log
time go run gotest.tools/gotestsum@latest -f testname -- -count=1 -p=1 -race -failfast -timeout 10m -tags=integration ./... 2>&1 | tee -a test-results.log