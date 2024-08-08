package remoting

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/spirit-labs/tektite/asl/certutil"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spirit-labs/tektite/common"

	log "github.com/spirit-labs/tektite/logger"
)

type clientConnection struct {
	lock          sync.RWMutex
	netConn       net.Conn
	closeGroup    sync.WaitGroup
	respHandlers  sync.Map
	reqSequence   int64
	closed        bool
	serverAddress string
	writeChan     chan queuedWrite
}

const maxBlockTime = 5 * time.Second
const writeChannelMaxSize = 1000

var ClientConnectionReadLoopCount int64
var ClientConnectionWriteLoopCount int64

type responseHandler interface {
	HandleResponse(resp ClusterMessage, err error)
}

type Error struct {
	Msg string
}

func (e Error) Error() string {
	return e.Msg
}

func createConnection(serverAddress string, tlsConfig *tls.Config) (*clientConnection, error) {
	netConn, err := createNetConnection(serverAddress, tlsConfig)
	if err != nil {
		return nil, err
	}
	cc := &clientConnection{
		netConn:       netConn,
		serverAddress: serverAddress,
		writeChan:     make(chan queuedWrite, writeChannelMaxSize),
	}
	cc.start()
	return cc, nil
}

func (c *clientConnection) QueueRequest(message ClusterMessage, respHandler responseHandler) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.closed {
		return Error{Msg: "connection closed"}
	}
	var cr *ClusterRequest
	var seq int64
	if respHandler != nil {
		seq = atomic.AddInt64(&c.reqSequence, 1)
		c.respHandlers.Store(seq, respHandler)
		cr = &ClusterRequest{
			requiresResponse: true,
			sequence:         seq,
			requestMessage:   message,
		}
	} else {
		cr = &ClusterRequest{
			requiresResponse: false,
			requestMessage:   message,
		}
	}
	buf, err := cr.serialize(nil)
	if err == nil {
		err = c.queueWrite(requestMessageType, buf, respHandler)
	}
	if err != nil && respHandler != nil {
		c.respHandlers.Delete(seq)
	}
	return err
}

type queuedWrite struct {
	msgType     messageType
	msg         []byte
	respHandler responseHandler
}

func (c *clientConnection) queueWrite(msgType messageType, msg []byte, respHandler responseHandler) error {
	select {
	case c.writeChan <- queuedWrite{
		msgType:     msgType,
		msg:         msg,
		respHandler: respHandler,
	}:
		return nil
	case <-time.After(maxBlockTime):
		log.Warn("timeout out waiting to write on connection")
		return common.NewTektiteErrorf(common.Unavailable, "timed out waiting to write")
	}
}

func (c *clientConnection) writeLoop() {
	defer common.TektitePanicHandler()
	atomic.AddInt64(&ClientConnectionWriteLoopCount, 1)
	for write := range c.writeChan {
		err := writeMessage(write.msgType, write.msg, c.netConn)
		if err != nil {
			if write.respHandler != nil {
				// If we get an error writing a message, then the response handler will likely also be called with error
				// to prevent two errors getting back to the user we send this error through the response handler - this
				// will ensure only one error gets to the user as the response handler will only return at most one error
				write.respHandler.HandleResponse(nil, err)
			} else {
				log.Warnf("failed to write message %+v", err)
			}
			// It might already be closed but close the underlying connection this will stop the read loop
			if err := c.netConn.Close(); err != nil {
				// Do nothing
			}
			break
		}
	}
	atomic.AddInt64(&ClientConnectionWriteLoopCount, -1)
	c.closeGroup.Done()
}

func (c *clientConnection) start() {
	c.closeGroup.Add(2)
	common.Go(c.writeLoop)
	atomic.AddInt64(&ClientConnectionReadLoopCount, 1)

	common.Go(func() {
		readMessage(c.handleMessage, c.netConn, func(err error) {
			// This will be called after the read loop has exited
			// Note We need to close the connection from this side too, to avoid leak of connections in CLOSE_WAIT state
			c.lock.Lock()
			c.closed = true
			c.lock.Unlock()
			if err := c.netConn.Close(); err != nil {
				// Do nothing
			}
			// We notify any waiting response handlers that the connection is closed
			c.respHandlers.Range(func(seq, v interface{}) bool {
				handler, ok := v.(responseHandler)
				if !ok {
					panic("not a responseHandler")
				}
				c.respHandlers.Delete(seq)
				remotingErr := Error{Msg: err.Error()}
				handler.HandleResponse(nil, remotingErr)
				return true
			})
			close(c.writeChan)
			atomic.AddInt64(&ClientConnectionReadLoopCount, -1)
			c.closeGroup.Done()
		})
	})
}

func (c *clientConnection) Close() {
	c.lock.Lock()
	c.closed = true
	c.lock.Unlock() // Note, we must unlock before closing the connection to avoid deadlock
	if err := c.netConn.Close(); err != nil {
		// Do nothing - connection might already have been closed from other side - this is ok
	}
	c.closeGroup.Wait()
}

func (c *clientConnection) ServerAddress() string {
	return c.serverAddress
}

func (c *clientConnection) handleMessage(msgType messageType, msg []byte) error {
	if msgType != responseMessageType {
		panic(fmt.Sprintf("unexpected message type %d msg %v", msgType, msg))
	}
	resp := &ClusterResponse{}
	if err := resp.deserialize(msg); err != nil {
		log.Errorf("failed to deserialize %v", err)
		return err
	}
	r, ok := c.respHandlers.LoadAndDelete(resp.sequence)
	if !ok {
		return Error{Msg: fmt.Sprintf("failed to find response handler msgType %d", msgType)}
	}
	handler, ok := r.(responseHandler)
	if !ok {
		panic("not a responseHandler")
	}
	if resp.errMsg != "" {
		respErr := common.NewTektiteError(common.ErrCode(resp.errCode), resp.errMsg)
		respErr.ExtraData = resp.errExtraData
		handler.HandleResponse(nil, respErr)
	} else {
		handler.HandleResponse(resp.responseMessage, nil)
	}
	return nil
}

func createRemotingError(err error) error {
	return errwrap.WithStack(Error{Msg: err.Error()})
}

func createNetConnection(serverAddress string, tlsConfig *tls.Config) (net.Conn, error) {
	addr, err := net.ResolveTCPAddr("tcp", serverAddress)
	if err != nil {
		return nil, createRemotingError(err)
	}
	var nc net.Conn
	if tlsConfig != nil {
		nc, err = tls.Dial("tcp", serverAddress, tlsConfig)
		if err != nil {
			return nil, createRemotingError(err)
		}
	} else {
		d := net.Dialer{Timeout: 1 * time.Second}
		conn, err := d.Dial("tcp", addr.String())
		if err != nil {
			return nil, createRemotingError(err)
		}
		tcpnc := conn.(*net.TCPConn)
		if err = tcpnc.SetNoDelay(true); err != nil {
			return nil, createRemotingError(err)
		}
		if err = tcpnc.SetKeepAlive(true); err != nil {
			return nil, createRemotingError(err)
		}
		nc = tcpnc
	}
	return nc, nil
}

// getClientTLSConfig builds client tls config for remoting intra-cluster mTLS.
func getClientTLSConfig(config conf.TLSConfig) (*tls.Config, error) {
	if !config.Enabled {
		return nil, nil
	}
	tlsConfig := &tls.Config{ // nolint: gosec
		MinVersion: tls.VersionTLS12,
	}
	keyPair, err := certutil.CreateKeyPair(config.CertPath, config.KeyPath)
	if err != nil {
		return nil, err
	}
	tlsConfig.Certificates = []tls.Certificate{keyPair}
	clientCerts, err := os.ReadFile(config.ClientCertsPath)
	if err != nil {
		return nil, err
	}
	trustedCertPool := x509.NewCertPool()
	if ok := trustedCertPool.AppendCertsFromPEM(clientCerts); !ok {
		return nil, errwrap.Errorf("failed to append trusted certs PEM (invalid PEM block?)")
	}
	tlsConfig.RootCAs = trustedCertPool
	return tlsConfig, nil
}
