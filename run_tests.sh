#!/bin/bash
# Copyright 2024 The Tektite Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

date > test-results.log
time go run gotest.tools/gotestsum@latest -f testname -- -count=1 -race -failfast -timeout 10m -tags=integration ./... 2>&1 | tee -a test-results.log