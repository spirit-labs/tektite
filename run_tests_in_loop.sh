#!/bin/bash

iteration=0

date > test-results.log
while true; do
    ((iteration++))

    iter="Running test iteration $iteration"
    echo "$iter" | tee -a test-results.log

    go run gotest.tools/gotestsum@latest -f testname -- -count=1 -race -failfast -timeout 10m -tags=integration ./... 2>&1 | tee -a test-results.log

    # Check the exit status of the test
    if [ $? -ne 0 ]; then
        echo "Go test failed. Exiting loop."
        break
    fi
done