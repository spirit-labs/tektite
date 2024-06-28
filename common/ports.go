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

package common

import (
	"fmt"
	"github.com/spirit-labs/tektite/errors"
	"net"
	"sync"
	"sync/atomic"
)

func AddressWithPort(host string) (string, error) {
	return tp.AddressWithPort(host)
}

func Listen(network, address string) (net.Listener, error) {
	if network != "tcp" {
		panic("network must be tcp")
	}
	return tp.listen(address)
}

var tp = newTestPorts()

type testPorts struct {
	enabled   atomic.Bool
	lock      sync.Mutex
	listeners map[string]net.Listener
}

func newTestPorts() *testPorts {
	return &testPorts{listeners: map[string]net.Listener{}}
}

func (t *testPorts) enable() {
	t.enabled.Store(true)
}

func (t *testPorts) listen(address string) (net.Listener, error) {
	if !t.enabled.Load() {
		return net.Listen("tcp", address)
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	listener, ok := t.listeners[address]
	if !ok {
		return nil, errors.Errorf("test ports is enabled and there is no registered listener for address %s", address)
	}
	return listener, nil
}

func (t *testPorts) AddressWithPort(host string) (string, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:0", host))
	if err != nil {
		return "", err
	}
	address := listener.Addr().String()
	t.lock.Lock()
	defer t.lock.Unlock()
	t.listeners[address] = &listenerWrapper{tp: t, address: address, listener: listener}
	return address, nil
}

func (t *testPorts) removeListener(address string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.listeners, address)
}

func EnableTestPorts() {
	tp.enable()
}

type listenerWrapper struct {
	tp       *testPorts
	address  string
	listener net.Listener
}

func (l *listenerWrapper) Accept() (net.Conn, error) {
	return l.listener.Accept()
}

func (l *listenerWrapper) Close() error {
	l.tp.removeListener(l.address)
	return l.listener.Close()
}

func (l *listenerWrapper) Addr() net.Addr {
	return l.listener.Addr()
}
