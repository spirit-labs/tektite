#!/bin/bash
date > test-results.log
time gotestsum -f testname -- -count=1 -race -failfast -timeout 10m ./... 2>&1 | tee -a test-results.log
time gotestsum -f testname -- -count=1 -race -failfast -timeout 10m -tags=integration ./integration/... 2>&1 | tee -a test-results.log
