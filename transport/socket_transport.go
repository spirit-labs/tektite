package transport

import (
	"crypto/tls"
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type SocketTransportVersion uint16

const (
	SocketTransportV1       SocketTransportVersion = 1
	readBuffSize                                   = 8 * 1024
	responseBuffInitialSize                        = 4 * 1024
	dialTimeout                                    = 5 * time.Second
	writeTimeout                                   = 5 * time.Second
)

var _ Server = (*SocketServer)(nil)

/*
SocketServer is a transport Server implementation that uses TCP sockets for communication between client and server.
It can also be configured to use TLS.
*/
type SocketServer struct {
	tlsConf             conf.TLSConfig
	lock                sync.RWMutex
	address             string
	handlers            map[int]RequestHandler
	started             bool
	listener            net.Listener
	acceptLoopExitGroup sync.WaitGroup
	serverConnections   sync.Map
}

func NewSocketServer(address string, tlsConf conf.TLSConfig) *SocketServer {
	return &SocketServer{
		tlsConf:  tlsConf,
		address:  address,
		handlers: make(map[int]RequestHandler),
	}
}

func (s *SocketServer) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return nil
	}
	list, err := s.createNetworkListener()
	if err != nil {
		return err
	}
	s.listener = list
	s.started = true
	s.acceptLoopExitGroup.Add(1)
	common.Go(s.acceptLoop)
	log.Infof("started socket transport server on address %s", s.address)
	return nil
}

func (s *SocketServer) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		return nil
	}
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			// Ignore
		}
	}
	// Wait for accept loop to exit
	s.acceptLoopExitGroup.Wait()
	// Now close connections
	s.serverConnections.Range(func(conn, _ interface{}) bool {
		conn.(*serverConnection).stop()
		return true
	})
	s.started = false
	return nil
}

func (s *SocketServer) RegisterHandler(handlerID int, handler RequestHandler) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, exists := s.handlers[handlerID]
	if exists {
		return false
	}
	s.handlers[handlerID] = handler
	return true
}

func (s *SocketServer) getRequestHandler(handlerID int) (RequestHandler, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	handler, exists := s.handlers[handlerID]
	return handler, exists
}

func (s *SocketServer) Address() string {
	return s.address
}

func (s *SocketServer) createNetworkListener() (net.Listener, error) {
	var list net.Listener
	var err error
	var tlsConfig *tls.Config
	if s.tlsConf.Enabled {
		tlsConfig, err = conf.CreateServerTLSConfig(s.tlsConf)
		if err != nil {
			return nil, err
		}
		list, err = common.Listen("tcp", s.address)
		if err == nil {
			list = tls.NewListener(list, tlsConfig)
		}
	} else {
		list, err = common.Listen("tcp", s.address)
	}
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (s *SocketServer) acceptLoop() {
	defer s.acceptLoopExitGroup.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Ok - was closed
			break
		}
		c := &serverConnection{
			s:    s,
			conn: conn,
		}
		s.serverConnections.Store(c, struct{}{})
		c.start()
	}
}

func (s *SocketServer) removeConnection(conn *serverConnection) {
	s.serverConnections.Delete(conn)
}

type serverConnection struct {
	s          *SocketServer
	conn       net.Conn
	closeGroup sync.WaitGroup
	lock       sync.Mutex
	closed     bool
}

func (c *serverConnection) start() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.closeGroup.Add(1)
	common.Go(c.readLoop)
}

func (c *serverConnection) readLoop() {
	defer c.readPanicHandler()
	defer c.closeGroup.Done()
	if err := readMessage(c.conn, c.handleMessage); err != nil {
		// Closed connection errors are normal on server shutdown - we ignore them
		ignoreErr := false
		if err == io.EOF {
			ignoreErr = true
		} else if ne, ok := err.(net.Error); ok {
			msg := ne.Error()
			ignoreErr = strings.Contains(msg, "use of closed network connection")
		}
		if !ignoreErr {
			log.Errorf("error in reading from server connection: %v", err)
		}
		if err := c.conn.Close(); err != nil {
			// Ignore
		}
	}
	c.cleanUp()
}

func (c *serverConnection) cleanUp() {
	c.s.removeConnection(c)
	c.lock.Lock()
	defer c.lock.Unlock()
	c.closed = true
}

func (c *serverConnection) readPanicHandler() {
	// We use a custom panic handler as we don't want the server to panic and crash if it receives a malformed
	// request which has insufficient bytes in the buffer which would cause a runtime error: index out of range panic
	if r := recover(); r != nil {
		log.Errorf("failure in connection readLoop: %v", r)
		if err := c.conn.Close(); err != nil {
			// Ignore
		}
		c.cleanUp()
	}
}

func readMessage(conn net.Conn, messageHandler func([]byte) error) error {
	buff := make([]byte, readBuffSize)
	var err error
	var readPos, n int
	for {
		// read the message size
		bytesRequired := 4 - readPos
		if bytesRequired > 0 {
			n, err = io.ReadAtLeast(conn, buff[readPos:], bytesRequired)
			if err != nil {
				break
			}
			readPos += n
		}
		totSize := 4 + int(binary.BigEndian.Uint32(buff))
		bytesRequired = totSize - readPos
		if bytesRequired > 0 {
			// If we haven't already read enough bytes, read the entire message body
			if totSize > len(buff) {
				// buffer isn't big enough, resize it
				nb := make([]byte, totSize)
				copy(nb, buff)
				buff = nb
			}
			n, err = io.ReadAtLeast(conn, buff[readPos:], bytesRequired)
			if err != nil {
				break
			}
			readPos += n
		}
		// Note that the buffer is reused so it's up to the protocol structs to copy any data in the message such
		// as records, uuid, []byte before the call to handleMessage returns
		err = messageHandler(buff[4:totSize])
		if err != nil {
			break
		}
		remainingBytes := readPos - totSize
		if remainingBytes > 0 {
			// Bytes for another message(s) have already been read, don't throw these away
			if remainingBytes < totSize {
				// we can copy directly as no overlap
				copy(buff, buff[totSize:readPos])
			} else {
				// too many bytes remaining, we have to create a new buffer
				nb := make([]byte, len(buff))
				copy(nb, buff[totSize:readPos])
				buff = nb
			}
		}
		readPos = remainingBytes
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func (c *serverConnection) handleMessage(buff []byte) error {
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
	return handler(buff[18:], responseBuff, func(response []byte, err error) error {
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

func (c *serverConnection) stop() {
	c.lock.Lock()
	c.closed = true
	if err := c.conn.Close(); err != nil {
		// Do nothing - connection might already have been closed (e.g. from Client)
	}
	c.lock.Unlock()
	c.closeGroup.Wait()
}

type SocketConnection struct {
	lock                  sync.Mutex
	correlationIDSequence int64
	conn                  net.Conn
	closeWaitGroup        sync.WaitGroup
	responseChannels      map[int64]chan responseHolder
}

func (s *SocketConnection) start() {
	s.closeWaitGroup.Add(1)
	go func() {
		defer s.readPanicHandler()
		defer s.closeWaitGroup.Done()
		if err := readMessage(s.conn, s.responseHandler); err != nil {
			log.Errorf("failed to read response message: %v", err)
			s.sendErrorResponsesAndCloseConnection(err)
		}
	}()
}

func (s *SocketConnection) sendErrorResponsesAndCloseConnection(err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	// With TLS client auth and client cert is invalid, only get an error on read - so we propagate this back to any
	// waiting RPCs
	for _, ch := range s.responseChannels {
		ch <- responseHolder{err: err}
	}
	if err := s.conn.Close(); err != nil {
		// Ignore
	}
}

func (s *SocketConnection) readPanicHandler() {
	if r := recover(); r != nil {
		log.Errorf("failure in client connection readLoop: %v", r)
		if err := s.conn.Close(); err != nil {
			// Ignore
		}
	}
}

func (s *SocketConnection) responseHandler(buff []byte) error {
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

func (s *SocketConnection) SendRPC(handlerID int, request []byte) ([]byte, error) {
	ch, err := s.sendRequest(handlerID, request)
	if err != nil {
		// If we get an error writing the request we consider the connection broken, so we close it
		// The user can then retry creating a new connection and sending the RPC again, potentially after a delay.
		if err := s.Close(); err != nil {
			// Ignore
		}
		return nil, err
	}
	holder := <-ch
	return holder.response, holder.err
}

func (s *SocketConnection) sendRequest(handlerID int, request []byte) (chan responseHolder, error) {
	buff, ch := s.createRequest(handlerID, request)
	// Set a write deadline so the write doesn't block for a long time in case the other side of the TCP connection
	// disappears
	if err := s.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return nil, err
	}
	_, err := s.conn.Write(buff)
	if err != nil {
		return nil, convertNetworkError(err)
	}
	return ch, nil
}

func convertNetworkError(err error) error {
	// We convert to Tektite unavailable errors, as they are retryable
	return common.NewTektiteErrorf(common.Unavailable, "transport error when sending rpc: %v", err)
}

func (s *SocketConnection) createRequest(handlerID int, request []byte) ([]byte, chan responseHolder) {
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
	ch := make(chan responseHolder, 1)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.responseChannels[s.correlationIDSequence] = ch
	s.correlationIDSequence++
	return buff, ch
}

type responseHolder struct {
	response []byte
	err      error
}

func (s *SocketConnection) Close() error {
	err := s.conn.Close()
	s.closeWaitGroup.Wait()
	return err
}

func NewSocketClient(tlsConf *client.TLSConfig) (*SocketClient, error) {
	var goTlsConf *tls.Config
	if tlsConf != nil {
		var err error
		goTlsConf, err = tlsConf.ToGoTlsConfig()
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
	if s.tlsConf != nil {
		var err error
		netConn, err = tls.Dial("tcp", address, s.tlsConf)
		if err != nil {
			return nil, convertNetworkError(err)
		}
	} else {
		d := net.Dialer{Timeout: dialTimeout}
		conn, err := d.Dial("tcp", address)
		if err != nil {
			return nil, convertNetworkError(err)
		}
		tcpNetConn := conn.(*net.TCPConn)
		if err = tcpNetConn.SetNoDelay(true); err != nil {
			return nil, err
		}
		if err = tcpNetConn.SetKeepAlive(true); err != nil {
			return nil, err
		}
		netConn = tcpNetConn
	}
	sc := &SocketConnection{
		conn:             netConn,
		responseChannels: map[int64]chan responseHolder{},
	}
	sc.start()
	return sc, nil
}
