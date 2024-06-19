#!/bin/bash
date > test-results.log
time go test -count=1 -race -failfast -timeout 10m -tags=integration ./... 2>&1 | tee -a test-results.log