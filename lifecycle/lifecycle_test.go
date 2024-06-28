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

package lifecycle

import (
	"fmt"
	"github.com/spirit-labs/tektite/conf"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestStartedHandlerActive(t *testing.T) {
	testHandler(t, true, "/started")
}

func TestStartedHandlerInactive(t *testing.T) {
	testHandler(t, false, "/started")
}

func TestLivenessHandlerActive(t *testing.T) {
	testHandler(t, true, "/liveness")
}

func TestLivenessHandlerInactive(t *testing.T) {
	testHandler(t, false, "/liveness")
}

func TestReadinessHandlerActive(t *testing.T) {
	testHandler(t, true, "/readiness")
}

func TestReadinessHandlerInactive(t *testing.T) {
	testHandler(t, false, "/readiness")
}

func testHandler(t *testing.T, active bool, path string) {
	t.Helper()
	cnf := conf.Config{}
	cnf.ApplyDefaults()
	cnf.LifeCycleEndpointEnabled = true
	cnf.LifeCycleAddress = "localhost:8913"
	cnf.StartupEndpointPath = "/started"
	cnf.LiveEndpointPath = "/liveness"
	cnf.ReadyEndpointPath = "/readiness"

	hndlr := NewLifecycleEndpoints(cnf)
	err := hndlr.Start()
	hndlr.SetActive(active)
	require.NoError(t, err)

	//goland:noinspection HttpUrlsUsage
	uri := fmt.Sprintf("http://%s%s", cnf.LifeCycleAddress, path)
	resp, err := http.Get(uri) //nolint:gosec
	require.NoError(t, err)
	if active {
		require.Equal(t, http.StatusOK, resp.StatusCode)
	} else {
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	}
	err = resp.Body.Close()
	require.NoError(t, err)

	err = hndlr.Stop()
	require.NoError(t, err)
}
