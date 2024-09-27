package sockserver

import (
	"crypto/tls"
	"encoding/binary"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"net"
	"sync"
	"time"
)

type SocketConnection struct {
	lock                  sync.Mutex
	conn                  net.Conn
	closeWaitGroup        sync.WaitGroup
	responseHandler func([]byte) error
}

const (
	writeTimeout = 5 * time.Second
	dialTimeout = 5 * time.Second
)

func (s *SocketConnection) start() {
	s.closeWaitGroup.Add(1)
	go func() {
		defer s.readPanicHandler()
		defer s.closeWaitGroup.Done()
		if err := ReadMessage(s.conn, s.responseHandler); err != nil {
			log.Errorf("failed to read response message: %v", err)
		}
	}()
}

func (s *SocketConnection) readPanicHandler() {
	if r := recover(); r != nil {
		log.Errorf("failure in client connection readLoop: %v", r)
		if err := s.conn.Close(); err != nil {
			// Ignore
		}
	}
}

func (s *SocketConnection) SendMessage(message []byte) error {
	if err := s.sendMessage(message); err != nil {
		// If we get an error writing the request we consider the connection broken, so we close it
		// The user can then retry creating a new connection and sending the RPC again, potentially after a delay.
		if err := s.Close(); err != nil {
			// Ignore
		}
		return err
	}
	return nil
}

func (s *SocketConnection) sendMessage(request []byte) error {
	buff := s.createRequest(request)
	// Set a write deadline so the write doesn't block for a long time in case the other side of the TCP connection
	// disappears
	if err := s.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return  err
	}
	_, err := s.conn.Write(buff)
	if err != nil {
		return convertNetworkError(err)
	}
	return nil
}

func convertNetworkError(err error) error {
	// We convert to Tektite unavailable errors, as they are retryable
	return common.NewTektiteErrorf(common.Unavailable, "transport error when sending rpc: %v", err)
}

func (s *SocketConnection) createRequest(request []byte) []byte {
	length := len(request) + 4
	buff := make([]byte, length)
	binary.BigEndian.PutUint32(buff, uint32(length-4))
	copy(buff[4:], request)
	return buff
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

func (s *SocketClient) CreateConnection(address string, responseHandler func([]byte) error) (*SocketConnection, error) {
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
		return nil, err
	}
	if err := tcpConn.SetKeepAlive(true); err != nil {
		return nil, err
	}
	sc := &SocketConnection{
		conn:             netConn,
		responseHandler: responseHandler,
	}
	sc.start()
	return sc, nil
}

