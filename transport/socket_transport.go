package transport

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/sockserver"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type SocketTransportVersion uint16

const (
	SocketTransportV1       SocketTransportVersion = 1
	responseBuffInitialSize                        = 4 * 1024
	dialTimeout                                    = 5 * time.Second
	defaultWriteTimeout                            = 5 * time.Second
)

var _ Server = (*SocketTransportServer)(nil)

/*
SocketTransportServer is a transport Server implementation that uses TCP sockets for communication between client and server.
It can also be configured to use TLS.
*/
type SocketTransportServer struct {
	lock         sync.RWMutex
	handlersLock sync.RWMutex
	handlers     map[int]RequestHandler
	socketServer *sockserver.SocketServer
	idSequence   int64
}

func NewSocketTransportServer(address string, tlsConf conf.TlsConf) *SocketTransportServer {
	server := &SocketTransportServer{
		handlers: make(map[int]RequestHandler),
	}
	server.socketServer = sockserver.NewSocketServer(address, tlsConf, server.newConnection)
	return server
}

func (s *SocketTransportServer) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.socketServer.Start(); err != nil {
		return err
	}
	return nil
}

func (s *SocketTransportServer) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.socketServer.Stop()
}

func (s *SocketTransportServer) RegisterHandler(handlerID int, handler RequestHandler) bool {
	s.handlersLock.Lock()
	defer s.handlersLock.Unlock()
	_, exists := s.handlers[handlerID]
	if exists {
		return false
	}
	s.handlers[handlerID] = handler
	return true
}
func (s *SocketTransportServer) Address() string {
	return s.socketServer.Address()
}

func (s *SocketTransportServer) getRequestHandler(handlerID int) (RequestHandler, bool) {
	s.handlersLock.RLock()
	defer s.handlersLock.RUnlock()
	handler, exists := s.handlers[handlerID]
	return handler, exists
}

func (s *SocketTransportServer) newConnection(conn net.Conn) sockserver.ServerConnection {
	return &SocketTransportServerConn{
		s:    s,
		conn: conn,
		id:   int(atomic.AddInt64(&s.idSequence, 1)),
	}
}

type SocketTransportServerConn struct {
	id     int
	s      *SocketTransportServer
	conn   net.Conn
	clHost string
}

func (c *SocketTransportServerConn) clientHost() string {
	if c.clHost != "" {
		return c.clHost
	}
	addr := c.conn.RemoteAddr().String()
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Sprintf("invalid-address:%s", addr)
	}
	c.clHost = host
	return host
}

func (c *SocketTransportServerConn) HandleMessage(buff []byte) error {
	version := SocketTransportVersion(binary.BigEndian.Uint16(buff))
	if version != SocketTransportV1 {
		return errors.Errorf("invalid transport version: %d - only version %d supported", version, SocketTransportV1)
	}
	correlationID := binary.BigEndian.Uint64(buff[2:])
	handlerID := int(binary.BigEndian.Uint64(buff[10:]))

	handler, ok := c.s.getRequestHandler(handlerID)
	if !ok {
		return errors.Errorf("no handler found with id %d", handlerID)
	}
	/*
		The response wire format is as follows:
		1. message length - int, 4 bytes, big endian
		2. socket transport version - int - 2 bytes, big endian
		3. correlation id - int - 8 bytes, big endian
		4. OK/error - byte, 0 if OK, 1 if error response
		5. the operation specific response bytes
	*/
	responseBuff := make([]byte, 15, responseBuffInitialSize)
	return handler(&ConnectionContext{ConnectionID: c.id, ClientAddress: c.clientHost()}, buff[18:], responseBuff, func(response []byte, err error) error {
		if err != nil {
			response = encodeErrorResponse(responseBuff, err)
		}
		binary.BigEndian.PutUint32(response, uint32(len(response)-4))
		binary.BigEndian.PutUint16(response[4:], uint16(SocketTransportV1))
		binary.BigEndian.PutUint64(response[6:], correlationID)
		if err != nil {
			response[14] = 1
		}
		_, err = c.conn.Write(response)
		return err
	})
}

func encodeErrorResponse(buff []byte, err error) []byte {
	errCode := common.InternalError
	var terr common.TektiteError
	if errwrap.As(err, &terr) {
		errCode = terr.Code
	}
	buff = binary.BigEndian.AppendUint16(buff, uint16(errCode))
	msg := err.Error()
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(msg)))
	buff = append(buff, msg...)
	return buff
}

// Client

type SocketTransportConnection struct {
	lock                  sync.Mutex
	closed                bool
	correlationIDSequence int64
	conn                  net.Conn
	closeWaitGroup        sync.WaitGroup
	responseChannels      map[int64]chan responseHolder
	writeTimeout          time.Duration
}

func (s *SocketTransportConnection) start() {
	s.closeWaitGroup.Add(1)
	go func() {
		defer s.readPanicHandler()
		defer s.closeWaitGroup.Done()
		if err := sockserver.ReadMessage(s.conn, s.responseHandler); err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") {
				log.Errorf("SocketTransportConnection: failed to read response message: %v", err)
			} else {
				// Normal to get these when closing - don't spam the logs
				log.Debugf("SocketTransportConnection: failed to read response message: %v", err)
			}
			s.sendErrorResponsesAndCloseConnection(err)
		}
	}()
}

func (s *SocketTransportConnection) sendErrorResponsesAndCloseConnection(err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.closed = true
	// With TLS client auth and client cert is invalid, only get an error on read - so we propagate this back to any
	// waiting RPCs
	for _, ch := range s.responseChannels {
		ch <- responseHolder{err: err}
	}
	s.responseChannels = map[int64]chan responseHolder{}
	if err := s.conn.Close(); err != nil {
		// Ignore
	}
}

func (s *SocketTransportConnection) readPanicHandler() {
	if r := recover(); r != nil {
		log.Errorf("failure in client connection readLoop: %v", r)
		if err := s.conn.Close(); err != nil {
			// Ignore
		}
	}
}

func (s *SocketTransportConnection) responseHandler(buff []byte) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	version := SocketTransportVersion(binary.BigEndian.Uint16(buff))
	if version != SocketTransportV1 {
		return errors.Errorf("invalid transport version: %d - only version %d supported", version, SocketTransportV1)
	}
	correlationID := int64(binary.BigEndian.Uint64(buff[2:]))
	ch, ok := s.responseChannels[correlationID]
	if !ok {
		return errors.Errorf("no client response handler found with id %d", correlationID)
	}
	delete(s.responseChannels, correlationID)
	// Next byte signifies whether error or not
	if buff[10] == 1 {
		// Error
		errorCode := binary.BigEndian.Uint16(buff[11:])
		msgLen := binary.BigEndian.Uint32(buff[13:])
		msg := string(buff[17 : 17+msgLen])
		ch <- responseHolder{
			err: common.NewTektiteError(common.ErrCode(errorCode), msg),
		}
	} else {
		// Must copy as connection reader re-uses the buffer
		copied := common.ByteSliceCopy(buff[11:])
		ch <- responseHolder{
			response: copied,
		}
	}
	return nil
}

func (s *SocketTransportConnection) SendRPC(handlerID int, request []byte) ([]byte, error) {
	buff, ch, err := s.createRequestAndRegisterResponseHandler(handlerID, request)
	if err != nil {
		return nil, err
	}
	if err := s.writeMessage(buff); err != nil {
		return nil, err
	}
	holder := <-ch
	return holder.response, holder.err
}

func (s *SocketTransportConnection) SendOneway(handlerID int, request []byte) error {
	buff := s.formatRequest(handlerID, request)
	if err := s.writeMessage(buff); err != nil {
		return err
	}
	return nil
}

func (s *SocketTransportConnection) writeMessage(buff []byte) error {
	// Set a write deadline so the write doesn't block for a long time in case the other side of the TCP connection
	// disappears
	if err := s.conn.SetWriteDeadline(time.Now().Add(s.writeTimeout)); err != nil {
		return convertNetworkError(err)
	}
	_, err := s.conn.Write(buff)
	if err != nil {
		return convertNetworkError(err)
	}
	return nil
}

func (s *SocketTransportConnection) SetWriteTimeout(timeout time.Duration) {
	s.writeTimeout = timeout
}

func convertNetworkError(err error) error {
	// We convert to Tektite unavailable errors, as they are retryable
	return common.NewTektiteErrorf(common.Unavailable, "transport error when sending rpc: %v", err)
}

func (s *SocketTransportConnection) createRequestAndRegisterResponseHandler(handlerID int,
	request []byte) ([]byte, chan responseHolder, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return nil, nil, common.NewTektiteErrorf(common.Unavailable, "connection is closed")
	}
	buff := s.formatRequest(handlerID, request)
	ch := make(chan responseHolder, 1)
	s.responseChannels[s.correlationIDSequence] = ch
	s.correlationIDSequence++
	return buff, ch, nil
}

func (s *SocketTransportConnection) formatRequest(handlerID int, request []byte) []byte {
	/*
			The request wire format is as follows:
			1. message length - int, 4 bytes, big endian
			2. socket transport version - int - 2 bytes, big endian
			3. correlation id - int - 8 bytes, big endian
		    4. handler id - int - 8 bytes, big endian
			5. the operation specific response bytes
	*/
	length := len(request) + 22
	buff := make([]byte, length)
	binary.BigEndian.PutUint32(buff, uint32(length-4))
	binary.BigEndian.PutUint16(buff[4:], uint16(SocketTransportV1))
	binary.BigEndian.PutUint64(buff[6:], uint64(s.correlationIDSequence))
	binary.BigEndian.PutUint64(buff[14:], uint64(handlerID))
	copy(buff[22:], request)
	return buff
}

func (s *SocketTransportConnection) NetConn() net.Conn {
	return s.conn
}

type responseHolder struct {
	response []byte
	err      error
}

func (s *SocketTransportConnection) Close() error {
	s.sendErrorResponsesAndCloseConnection(errors.New("connection closed"))
	s.closeWaitGroup.Wait()
	return nil
}

func (s *SocketTransportConnection) closeWaitingRPCs() {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, ch := range s.responseChannels {
		close(ch)
	}
	s.responseChannels = map[int64]chan responseHolder{}
}

func NewSocketClient(tlsConf *conf.ClientTlsConf) (*SocketClient, error) {
	var goTlsConf *tls.Config
	if tlsConf != nil {
		var err error
		goTlsConf, err = tlsConf.ToGoTlsConf()
		if err != nil {
			return nil, err
		}
	}
	return &SocketClient{
		tlsConf: goTlsConf,
	}, nil
}

type SocketClient struct {
	tlsConf *tls.Config
}

func (s *SocketClient) CreateConnection(address string) (Connection, error) {
	var netConn net.Conn
	var tcpConn *net.TCPConn
	if s.tlsConf != nil {
		var err error
		netConn, err = tls.Dial("tcp", address, s.tlsConf)
		if err != nil {
			return nil, convertNetworkError(err)
		}
		rawConn := netConn.(*tls.Conn).NetConn()
		tcpConn = rawConn.(*net.TCPConn)
	} else {
		d := net.Dialer{Timeout: dialTimeout}
		var err error
		netConn, err = d.Dial("tcp", address)
		if err != nil {
			return nil, convertNetworkError(err)
		}
		tcpConn = netConn.(*net.TCPConn)
	}
	if err := tcpConn.SetNoDelay(true); err != nil {
		return nil, convertNetworkError(err)
	}
	if err := tcpConn.SetKeepAlive(true); err != nil {
		return nil, convertNetworkError(err)
	}
	sc := &SocketTransportConnection{
		conn:             netConn,
		responseChannels: map[int64]chan responseHolder{},
		writeTimeout:     defaultWriteTimeout,
	}
	sc.start()
	return sc, nil
}
