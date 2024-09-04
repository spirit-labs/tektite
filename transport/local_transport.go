package transport

import (
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"sync"
)

func NewLocalTransports() *LocalTransports {
	return &LocalTransports{
		transports: map[string]RequestHandler{},
	}
}

type LocalTransports struct {
	lock       sync.RWMutex
	transports map[string]RequestHandler
}

func (lt *LocalTransports) deliverMessage(address string, message []byte, responseHandler ResponseHandler) error {
	lt.lock.RLock()
	defer lt.lock.RUnlock()
	handler, ok := lt.transports[address]
	if !ok {
		return errors.Errorf("no handler found for address %s", address)
	}
	return handler(message, responseHandler)
}

func (lt *LocalTransports) NewLocalTransport(address string) (*LocalTransport, error) {
	lt.lock.Lock()
	defer lt.lock.Unlock()
	_, exists := lt.transports[address]
	if exists {
		return nil, errors.Errorf("handler already exists for address %s", address)
	}
	transport := &LocalTransport{
		address:    address,
		transports: lt,
	}
	lt.transports[address] = transport.handleRequest
	return transport, nil
}

type LocalTransport struct {
	address    string
	handler    RequestHandler
	transports *LocalTransports
}

func (l *LocalTransport) handleRequest(request []byte, responseWriter ResponseHandler) error {
	return l.handler(request, responseWriter)
}

func (l *LocalTransport) CreateConnection(address string, handler ResponseHandler) (Connection, error) {
	return &LocalConnection{
		transport: l,
		address:   address,
		handler:   handler,
	}, nil
}

func (l *LocalTransport) RegisterHandler(handler RequestHandler) {
	l.handler = handler
}

type LocalConnection struct {
	transport *LocalTransport
	address   string
	handler   ResponseHandler
}

func (l *LocalConnection) WriteMessage(message []byte) error {
	msgCopy := common.ByteSliceCopy(message)
	go func() {
		if err := l.transport.transports.deliverMessage(l.address, msgCopy, l.handler); err != nil {
			log.Errorf("failed to deliver message: %v", err)
		}
	}()
	return nil
}

func (l *LocalConnection) Close() error {
	//TODO implement me
	panic("implement me")
}
