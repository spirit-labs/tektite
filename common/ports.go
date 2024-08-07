package common

import (
	"fmt"
	"github.com/spirit-labs/tektite/asl/errwrap"
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
		return nil, errwrap.Errorf("test ports is enabled and there is no registered listener for address %s", address)
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
