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

package remoting

import (
	"github.com/spirit-labs/tektite/common"
)

type rpcRespHandler struct {
	lock           common.SpinLock
	complete       bool
	completionFunc func(ClusterMessage, error)
}

func (r *rpcRespHandler) HandleResponse(resp ClusterMessage, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.complete {
		return
	}
	r.complete = true
	r.completionFunc(resp, err)
}

func (r *blockingRpcRespHandler) waitForResponse() (ClusterMessage, error) {
	rh := <-r.ch
	return rh.resp, rh.err
}

func (r *blockingRpcRespHandler) onComplete(resp ClusterMessage, err error) {
	r.ch <- respHolder{resp: resp, err: err}
}

type blockingRpcRespHandler struct {
	rpcRespHandler
	ch chan respHolder
}

func newBlockingRpcRespHandler() *blockingRpcRespHandler {
	r := &blockingRpcRespHandler{
		rpcRespHandler: rpcRespHandler{},
		ch:             make(chan respHolder, 1),
	}
	r.completionFunc = r.onComplete
	return r
}

type broadcastRespHandler struct {
	requiredResponses int // The total number of responses required
	respCount         int // The number of responses so far
	lock              common.SpinLock
	completionFunc    func(error)
	complete          bool
}

func (t *broadcastRespHandler) HandleResponse(_ ClusterMessage, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.complete {
		return
	}
	t.respCount++
	if err != nil || t.respCount == t.requiredResponses {
		t.complete = true
		t.completionFunc(err)
	}
}

func (brh *blockingBroadcastRespHandler) waitForResponse() error {
	err := <-brh.ch
	return err
}

type blockingBroadcastRespHandler struct {
	broadcastRespHandler
	ch chan error
}

func (brh *blockingBroadcastRespHandler) onComplete(err error) {
	brh.ch <- err
}

func newBlockingBroadcastRespHandler(requiredResponses int) *blockingBroadcastRespHandler {
	h := &blockingBroadcastRespHandler{
		broadcastRespHandler: broadcastRespHandler{requiredResponses: requiredResponses},
		ch:                   make(chan error, 1),
	}
	h.completionFunc = h.onComplete
	return h
}

func newBroadcastRespHandler(requiredResponses int, completionFunc func(error)) *broadcastRespHandler {
	return &broadcastRespHandler{requiredResponses: requiredResponses, completionFunc: completionFunc}
}
