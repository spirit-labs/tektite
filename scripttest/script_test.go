// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !largecluster

package scripttest

import (
	"testing"
)

func TestScriptStandalone(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("-short: skipped")
	}
	testScript(t, 1, 1, false, tlsKeysInfo, etcdAddress)
}

func TestScriptThreeNodes(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("-short: skipped")
	}
	testScript(t, 3, 3, false, tlsKeysInfo, etcdAddress)
}

//	func TestScriptFiveNodes(t *testing.T) {
//		if testing.Short() {
//			t.Skip("-short: skipped")
//		}
//		testScript(t, 5, 3, false, tlsKeysInfo)
//	}
