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
	"errors"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	log "github.com/spirit-labs/tektite/logger"
	"net/http"
	"sync/atomic"
)

/*
Endpoints provides HTTP lifecycle endpoints - these are typically used when deploying Tektite in k8s
and provide startup, readiness and live-ness endpoints.
*/
type Endpoints struct {
	conf    conf.Config
	server  *http.Server
	started atomic.Bool
	ready   atomic.Bool
	live    atomic.Bool
}

func NewLifecycleEndpoints(config conf.Config) *Endpoints {
	return &Endpoints{conf: config}
}

func (e *Endpoints) SetActive(active bool) {
	// For now we don't have fine grained control over started, ready or live but we can add this at a later date if
	// necessary
	e.started.Store(active)
	e.ready.Store(active)
	e.live.Store(active)
}

func (e *Endpoints) Start() error {
	if !e.conf.LifeCycleEndpointEnabled {
		return nil
	}

	sm := http.NewServeMux()
	sm.Handle(e.conf.StartupEndpointPath, &handler{state: &e.started})
	sm.Handle(e.conf.ReadyEndpointPath, &handler{state: &e.ready})
	sm.Handle(e.conf.LiveEndpointPath, &handler{state: &e.live})

	e.server = &http.Server{Addr: e.conf.LifeCycleAddress, Handler: sm}

	ln, err := common.Listen("tcp", e.conf.LifeCycleAddress)
	if err != nil {
		return err
	}

	common.Go(func() {
		err := e.server.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Errorf("lifecycle server failed to listen %v", err)
		}
	})
	return nil
}

func (e *Endpoints) Stop() error {
	if !e.conf.LifeCycleEndpointEnabled {
		return nil
	}
	return e.server.Close()
}

type handler struct {
	state *atomic.Bool
}

func (i *handler) ServeHTTP(writer http.ResponseWriter, _ *http.Request) {
	if i.state.Load() {
		writer.WriteHeader(http.StatusOK)
	} else {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}
}
