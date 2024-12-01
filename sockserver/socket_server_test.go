package sockserver

import (
	"crypto/tls"
	"encoding/binary"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	common.EnableTestPorts()
}

const (
	serverKeyPath   = "testdata/serverkey.pem"
	serverCertPath  = "testdata/servercert.pem"
	clientCertPath1 = "testdata/selfsignedclientcert.pem"
	clientKeyPath1  = "testdata/selfsignedclientkey.pem"
)

func TestSocketServerNoTls(t *testing.T) {
	testSocketServer(t, conf.TlsConf{}, nil)
}

func TestSocketServerTls(t *testing.T) {
	testSocketServer(t, conf.TlsConf{
		Enabled:              true,
		ServerPrivateKeyFile: serverKeyPath,
		ServerCertFile:       serverCertPath,
	}, &client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	})

}

func TestSocketServerMutualTls(t *testing.T) {
	testSocketServer(t, conf.TlsConf{
		Enabled:              true,
		ServerPrivateKeyFile: serverKeyPath,
		ServerCertFile:       serverCertPath,
		ClientCertFile:       clientCertPath1,
		ClientAuthType:       "require-and-verify-client-cert",
	}, &client.TLSConfig{
		TrustedCertsPath: serverCertPath,
		KeyPath:          clientKeyPath1,
		CertPath:         clientCertPath1,
		NoVerify:         false,
	})
}

func TestSocketTransportServerTlsUntrustedServer(t *testing.T) {
	serverTlsConf := conf.TlsConf{
		Enabled:              true,
		ServerPrivateKeyFile: serverKeyPath,
		ServerCertFile:       serverCertPath,
	}

	address, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	serverConns := &testServerConnections{
		connections: make(map[string]*testServerConnection),
	}
	server := NewSocketServer(address, serverTlsConf, serverConns.newConnection)
	err = server.Start()
	require.NoError(t, err)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()

	_, err = createConnectionReturnError(t, address, &client.TLSConfig{})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "tls: failed to verify certificate: x509: certificate signed by unknown authority"))
}

func testSocketServer(t *testing.T, serverTlsConf conf.TlsConf, clientTlsConf *client.TLSConfig) {
	address, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	serverConns := &testServerConnections{
		connections: make(map[string]*testServerConnection),
	}
	server := NewSocketServer(address, serverTlsConf, serverConns.newConnection)
	err = server.Start()
	require.NoError(t, err)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()

	numConnections := 10
	numMessagesPerConnection := 10
	maxMessageLength := 10
	var sentMsgs = map[string][][]byte{}

	for i := 0; i < numConnections; i++ {
		conn := createConnection(t, address, clientTlsConf)
		var sent [][]byte
		for j := 0; j < numMessagesPerConnection; j++ {
			body := []byte(randomReadableString(maxMessageLength))
			sent = append(sent, body)
			msg := createLengthPrefixedMessage(body)
			_, err = conn.Write(msg)
			require.NoError(t, err)
		}
		sentMsgs[conn.LocalAddr().String()] = sent
	}

	testutils.WaitUntil(t, func() (bool, error) {
		return int(atomic.LoadInt64(&serverConns.totMsgs)) == numConnections*numMessagesPerConnection, nil
	})

	receivedConns := serverConns.getConnections()
	require.Equal(t, len(sentMsgs), len(receivedConns))
	for addr, msgs := range sentMsgs {
		serverConn, ok := receivedConns[addr]
		require.True(t, ok)
		receivedMsgs := serverConn.getMessages()
		require.Equal(t, len(msgs), len(receivedMsgs))
		for i, msg := range msgs {
			receivedMsg := receivedMsgs[i]
			require.Equal(t, msg, receivedMsg)
		}
	}
}

func createLengthPrefixedMessage(body []byte) []byte {
	var msg []byte
	msg = binary.BigEndian.AppendUint32(msg, uint32(len(body)))
	msg = append(msg, body...)
	return msg
}

type testServerConnections struct {
	lock        sync.Mutex
	connections map[string]*testServerConnection
	totMsgs     int64
}

func (t *testServerConnections) newConnection(conn net.Conn) ServerConnection {
	t.lock.Lock()
	defer t.lock.Unlock()
	sc := &testServerConnection{totMsgs: &t.totMsgs}
	t.connections[conn.RemoteAddr().String()] = sc
	return sc
}

func (t *testServerConnections) getConnections() map[string]*testServerConnection {
	t.lock.Lock()
	defer t.lock.Unlock()
	m := map[string]*testServerConnection{}
	for k, v := range t.connections {
		m[k] = v
	}
	return m
}

type testServerConnection struct {
	totMsgs  *int64
	lock     sync.Mutex
	messages [][]byte
}

func (t *testServerConnection) HandleMessage(message []byte) error {
	t.lock.Lock()
	t.messages = append(t.messages, common.ByteSliceCopy(message))
	t.lock.Unlock()
	atomic.AddInt64(t.totMsgs, 1)
	return nil
}

func (t *testServerConnection) getMessages() [][]byte {
	t.lock.Lock()
	defer t.lock.Unlock()
	copied := make([][]byte, len(t.messages))
	copy(copied, t.messages)
	return copied
}

func createConnection(t *testing.T, address string, tlsConf *client.TLSConfig) net.Conn {
	conn, err := createConnectionReturnError(t, address, tlsConf)
	require.NoError(t, err)
	return conn
}

func createConnectionReturnError(t *testing.T, address string, tlsConf *client.TLSConfig) (net.Conn, error) {
	var goTlsConf *tls.Config
	if tlsConf != nil {
		var err error
		goTlsConf, err = tlsConf.ToGoTlsConfig()
		if err != nil {
			return nil, err
		}
	}
	var netConn net.Conn
	var tcpConn *net.TCPConn
	if goTlsConf != nil {
		var err error
		netConn, err = tls.Dial("tcp", address, goTlsConf)
		if err != nil {
			return nil, err
		}
		rawConn := netConn.(*tls.Conn).NetConn()
		tcpConn = rawConn.(*net.TCPConn)
	} else {
		d := net.Dialer{Timeout: 5 * time.Second}
		var err error
		netConn, err = d.Dial("tcp", address)
		if err != nil {
			return nil, err
		}
		tcpConn = netConn.(*net.TCPConn)
	}
	if err := tcpConn.SetNoDelay(true); err != nil {
		return nil, err
	}
	if err := tcpConn.SetKeepAlive(true); err != nil {
		return nil, err
	}
	return netConn, nil
}

func randomReadableString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var sb strings.Builder
	sb.Grow(n)
	l := rand.Intn(n)
	for i := 0; i < l; i++ {
		sb.WriteByte(letters[rand.Intn(len(letters))])
	}
	return sb.String()
}
