package transport

import (
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"sync"
	"sync/atomic"
)

// LocalServer is a transport Server implementation that is only used where the client and server are in the same
// process. It is mainly used in testing.
type LocalServer struct {
	lock       sync.RWMutex
	address    string
	handlers   map[int]RequestHandler
	transports *LocalTransports
}

var _ Server = (*LocalServer)(nil)

func (l *LocalServer) RegisterHandler(handlerID int, handler RequestHandler) bool {
	l.lock.Lock()
	defer l.lock.Unlock()
	_, exists := l.handlers[handlerID]
	if exists {
		return false
	}
	l.handlers[handlerID] = handler
	return true
}

func (l *LocalServer) Address() string {
	return l.address
}

func (l *LocalServer) Stop() error {
	return nil
}

type LocalConnection struct {
	id         int
	transports *LocalTransports
	address    string
}

func (l *LocalConnection) SendRPC(handlerID int, request []byte) ([]byte, error) {
	handler, err := l.transports.getRequestHandler(handlerID, l.address)
	if err != nil {
		return nil, err
	}
	msgCopy := common.ByteSliceCopy(request)
	ch := make(chan responseHolder, 1)
	go func() {
		if err := handler(l.id, msgCopy, nil, func(response []byte, err error) error {
			if err != nil {
				ch <- responseHolder{
					err: maybeConvertError(err),
				}
			} else {
				ch <- responseHolder{
					response: response,
				}
			}
			return nil
		}); err != nil {
			log.Errorf("failed to handle request: %v", err)
		}
	}()
	respHolder := <-ch
	return respHolder.response, respHolder.err
}

func maybeConvertError(err error) error {
	if err == nil {
		return nil
	}
	var terr common.TektiteError
	if !errwrap.As(err, &terr) {
		terr.Code = common.InternalError
		terr.Msg = err.Error()
	}
	return terr
}

func (l *LocalConnection) Close() error {
	return nil
}

func NewLocalTransports() *LocalTransports {
	return &LocalTransports{
		transports: map[string]*LocalServer{},
	}
}

type LocalTransports struct {
	lock                 sync.RWMutex
	transports           map[string]*LocalServer
	connectionIDSequence int64
}

func (lt *LocalTransports) CreateConnection(address string) (Connection, error) {
	return &LocalConnection{
		id:         int(atomic.AddInt64(&lt.connectionIDSequence, 1)),
		transports: lt,
		address:    address,
	}, nil
}

func (lt *LocalTransports) NewLocalServer(address string) (*LocalServer, error) {
	lt.lock.Lock()
	defer lt.lock.Unlock()
	_, exists := lt.transports[address]
	if exists {
		return nil, errors.Errorf("transport already exists for address %s", address)
	}
	transport := &LocalServer{
		address:    address,
		transports: lt,
		handlers:   make(map[int]RequestHandler),
	}
	lt.transports[address] = transport
	return transport, nil
}

func (lt *LocalTransports) getRequestHandler(handlerID int, address string) (RequestHandler, error) {
	lt.lock.RLock()
	defer lt.lock.RUnlock()
	transport, ok := lt.transports[address]
	if !ok {
		return nil, errors.Errorf("no transport found for address %s", address)
	}
	transport.lock.RLock()
	defer transport.lock.RUnlock()
	handler, ok := transport.handlers[handlerID]
	if !ok {
		return nil, errors.Errorf("no handler found for handlerID %d", handlerID)
	}
	return handler, nil
}
