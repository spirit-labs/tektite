package sockserver

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"io"
	"net"
	"strings"
	"sync"
)

type ConnectionFactory func(net.Conn) ServerConnection

type ServerConnection interface {
	HandleMessage(message []byte) error
}

const readBuffSize = 8 * 1024

/*
SocketServer is a Server that listens for connections on TCP sockets and reads messages which are length prefixed by a
big-endian 32 bit integer, and then calls a user specified message handler (created by the ConnectionFactory) for each
message.
SocketServer can also be configured to use TLS.
*/
type SocketServer struct {
	tlsConf             conf.TLSConfig
	lock                sync.RWMutex
	address             string
	connFactory         ConnectionFactory
	started             bool
	listener            net.Listener
	acceptLoopExitGroup sync.WaitGroup
	connections         sync.Map
}

func NewSocketServer(address string, tlsConf conf.TLSConfig, connFactory ConnectionFactory) *SocketServer {
	return &SocketServer{
		tlsConf:     tlsConf,
		address:     address,
		connFactory: connFactory,
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
	s.connections.Range(func(conn, _ interface{}) bool {
		conn.(*serverConnection).stop()
		return true
	})
	s.started = false
	return nil
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
			s:        s,
			conn:     conn,
			userConn: s.connFactory(conn),
		}
		s.connections.Store(c, struct{}{})
		c.start()
	}
}

func (s *SocketServer) removeConnection(conn *serverConnection) {
	s.connections.Delete(conn)
}

type serverConnection struct {
	s          *SocketServer
	conn       net.Conn
	closeGroup sync.WaitGroup
	lock       sync.Mutex
	closed     bool
	userConn   ServerConnection
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
	if err := ReadMessage(c.conn, c.userConn.HandleMessage); err != nil {
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
		// Log using fmt as logger might be cleaned up and unusable by this point
		fmt.Printf("failure in connection readLoop: %v\n", r)
		if err := c.conn.Close(); err != nil {
			// Ignore
		}
		c.cleanUp()
	}
}

// ReadMessage reads a message that is length prefixed with a big-endian 32 bit integer and calls the provided
// message handler with the message
func ReadMessage(conn net.Conn, messageHandler func([]byte) error) error {
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

func (c *serverConnection) stop() {
	c.lock.Lock()
	c.closed = true
	if err := c.conn.Close(); err != nil {
		// Do nothing - connection might already have been closed (e.g. from Client)
	}
	c.lock.Unlock()
	c.closeGroup.Wait()
}
