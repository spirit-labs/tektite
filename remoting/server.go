package remoting

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
)

const (
	readBuffSize      = 8 * 1024
	messageHeaderSize = 5 // 1 byte message type, 4 bytes length
)

type Server interface {
	Start() error
	Stop() error
	RegisterMessageHandler(messageType ClusterMessageType, handler ClusterMessageHandler)
	RegisterBlockingMessageHandler(messageType ClusterMessageType, handler BlockingClusterMessageHandler)
	RegisterConnectionClosedHandler(handler func(int))
}

func NewServer(listenAddress string, tlsConf conf.TLSConfig) Server {
	server := newServer(listenAddress, tlsConf)
	return server
}

func newServer(listenAddress string, tlsConf conf.TLSConfig) *server {
	return &server{
		listenAddress: listenAddress,
		tlsConf:       tlsConf,
	}
}

type server struct {
	listenAddress            string
	listener                 net.Listener
	started                  bool
	lock                     sync.RWMutex
	acceptLoopExitGroup      sync.WaitGroup
	connections              sync.Map
	messageHandlers          sync.Map
	responsesDisabled        atomic.Bool
	tlsConf                  conf.TLSConfig
	connectionSeq            uint64
	connectionClosedHandlers []func(int)
	closedHandlersLock       sync.Mutex
}

func (s *server) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return nil
	}
	list, err := s.createNetworkListener()
	if err != nil {
		return errors.WithStack(err)
	}
	s.listener = list
	s.started = true
	s.acceptLoopExitGroup.Add(1)
	common.Go(s.acceptLoop)
	log.Debugf("started remoting server on %s", s.listenAddress)
	return nil
}

func (s *server) RegisterConnectionClosedHandler(handler func(int)) {
	s.closedHandlersLock.Lock()
	defer s.closedHandlersLock.Unlock()
	s.connectionClosedHandlers = append(s.connectionClosedHandlers, handler)
}

func (s *server) createNetworkListener() (net.Listener, error) {
	var list net.Listener
	var err error
	var tlsConfig *tls.Config
	if s.tlsConf.Enabled {
		tlsConfig, err = conf.CreateServerTLSConfig(s.tlsConf)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		list, err = common.Listen("tcp", s.listenAddress)
		if err == nil {
			list = tls.NewListener(list, tlsConfig)
		}
	} else {
		list, err = common.Listen("tcp", s.listenAddress)
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return list, nil
}

func (s *server) acceptLoop() {
	defer s.acceptLoopExitGroup.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Ok - was closed
			break
		}
		c := s.newConnection(conn)
		s.connections.Store(c, struct{}{})
		c.start()
	}
}

func (s *server) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		return nil
	}
	if err := s.listener.Close(); err != nil {
		log.Warnf("failed to close listener %v", err)
	}
	// Wait for accept loop to exit
	s.acceptLoopExitGroup.Wait()
	// Now close connections
	s.connections.Range(func(conn, _ interface{}) bool {
		conn.(*connection).stop()
		return true
	})
	s.started = false
	log.Debugf("stopped remoting server on %s", s.listenAddress)
	return nil
}

func (s *server) ListenAddress() string {
	return s.listenAddress
}

func (s *server) RegisterMessageHandler(messageType ClusterMessageType, handler ClusterMessageHandler) {
	_, ok := s.messageHandlers.Load(messageType)
	if ok {
		panic(fmt.Sprintf("message handler with type %d already registered", messageType))
	}
	s.messageHandlers.Store(messageType, handler)
}

func (s *server) RegisterBlockingMessageHandler(messageType ClusterMessageType, handler BlockingClusterMessageHandler) {
	_, ok := s.messageHandlers.Load(messageType)
	if ok {
		panic(fmt.Sprintf("message handler with type %d already registered", messageType))
	}
	s.messageHandlers.Store(messageType, NewBlockingClusterMessageHandlerAdaptor(handler))
}

func (s *server) lookupMessageHandler(clusterMessage ClusterMessage) ClusterMessageHandler {
	l, ok := s.messageHandlers.Load(TypeForClusterMessage(clusterMessage))
	if !ok {
		panic(fmt.Sprintf("no message handler for type %d", TypeForClusterMessage(clusterMessage)))
	}
	return l.(ClusterMessageHandler)
}

func (s *server) removeConnection(conn *connection) {
	s.connections.Delete(conn)
}

// DisableResponses is used to disable responses - for testing only
func (s *server) DisableResponses() {
	s.responsesDisabled.Store(true)
}

func (s *server) newConnection(conn net.Conn) *connection {
	return &connection{
		s:         s,
		conn:      conn,
		writeChan: make(chan []byte, 1000),
		id:        int(atomic.AddUint64(&s.connectionSeq, 1)),
	}
}

func (s *server) connectionClosed(conn *connection) {
	s.closedHandlersLock.Lock()
	defer s.closedHandlersLock.Unlock()
	for _, handler := range s.connectionClosedHandlers {
		handler(conn.id)
	}
}

type connection struct {
	id         int
	s          *server
	conn       net.Conn
	closeGroup sync.WaitGroup
	writeChan  chan []byte
	lock       sync.Mutex
	closed     bool
}

func (c *connection) start() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.closeGroup.Add(2)
	common.Go(c.readLoop)
	common.Go(c.writeLoop)
}

var ServerConnectionReadLoopCount int64
var ServerConnectionWriteLoopCount int64

func (c *connection) readLoop() {
	atomic.AddInt64(&ServerConnectionReadLoopCount, 1)
	defer common.PanicHandler()
	readMessage(c.handleMessage, c.conn, func(_ error) {
		// We need to close the connection from this side too, to avoid leak of connections in CLOSE_WAIT state
		if err := c.conn.Close(); err != nil {
			// Do nothing
		}
		c.closeGroup.Done()
		c.connectionClosed()
	})
	c.s.removeConnection(c)
	c.lock.Lock()
	defer c.lock.Unlock()
	c.closed = true
	close(c.writeChan)
	atomic.AddInt64(&ServerConnectionReadLoopCount, -1)
}

func (c *connection) writeLoop() {
	atomic.AddInt64(&ServerConnectionWriteLoopCount, 1)
	defer c.closeGroup.Done()
	failed := false
	for msg := range c.writeChan {
		if failed {
			continue
		}
		err := writeMessage(responseMessageType, msg, c.conn)
		if err != nil {
			log.Debugf("failed to write response message %v", err)
			failed = true // We ignore further writes
			c.connectionClosed()
		}
	}
	atomic.AddInt64(&ServerConnectionWriteLoopCount, -1)
}

func (c *connection) connectionClosed() {
	c.s.connectionClosed(c)
}

func (c *connection) handleMessage(_ messageType, msg []byte) error {
	// Handle async
	c.handleMessageAsync0(msg)
	return nil
}

func (c *connection) handleMessageAsync0(msg []byte) {
	request := &ClusterRequest{}
	if err := request.deserialize(msg); err != nil {
		log.Errorf("failed to deserialize message %+v", err)
		return
	}
	handler := c.s.lookupMessageHandler(request.requestMessage)
	holder := MessageHolder{
		Message:      request.requestMessage,
		ConnectionID: c.id,
	}
	handler.HandleMessage(holder, func(respMsg ClusterMessage, respErr error) {
		if request.requiresResponse && !c.s.responsesDisabled.Load() {
			if err := c.sendResponse(request, respMsg, respErr); err != nil {
				// This can happen if the node the response is being sent to has crashed
				// It can be ignored
				log.Debugf("failed to send response %+v", err)
			}
		}
	})
}

func (c *connection) sendResponse(nf *ClusterRequest, respMsg ClusterMessage, respErr error) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.closed {
		return Error{Msg: "connection closed"}
	}
	resp := &ClusterResponse{
		sequence:        nf.sequence,
		responseMessage: respMsg,
	}
	if respErr == nil {
		resp.ok = true
	} else {
		resp.ok = false
		var perr errors.TektiteError
		if !errors.As(respErr, &perr) {
			perr = common.LogInternalError(respErr)
		}
		resp.errCode = int(perr.Code)
		resp.errMsg = perr.Error()
		resp.errExtraData = perr.ExtraData
	}
	buff, err := resp.serialize(nil)
	if err != nil {
		return err
	}
	c.writeChan <- buff
	return nil
}

func (c *connection) stop() {
	c.lock.Lock()
	c.closed = true
	if err := c.conn.Close(); err != nil {
		// Do nothing - connection might already have been closed (e.g. from Client)
	}
	c.lock.Unlock()
	c.closeGroup.Wait()
}

// Used in testing only
func (s *server) closeNetConns() {
	s.connections.Range(func(conn, _ interface{}) bool {
		//goland:noinspection GoUnhandledErrorResult
		conn.(*connection).conn.Close()
		return true
	})
}
