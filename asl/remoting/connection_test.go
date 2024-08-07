package remoting

import (
	"fmt"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/asl/remoting/protos/remotingmsgs"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	serverCertPath = "testdata/servercert.pem"
	serverKeyPath  = "testdata/serverkey.pem"
)

func init() {
	common.EnableTestPorts()
}

func TestSendRequest(t *testing.T) {
	list := &echoListener{}
	server := startServerWithHandler(t, list, conf.TLSConfig{})
	defer stopServers(t, server)

	conn, err := createConnection(server.ListenAddress(), nil)
	require.NoError(t, err)

	msg := &remotingmsgs.RemotingTestMessage{SomeField: "badgers"}
	rh := newRespHandler()
	err = conn.QueueRequest(msg, rh)
	require.NoError(t, err)
	r, err := rh.waitForResponse()
	require.NoError(t, err)
	resp, ok := r.(*remotingmsgs.RemotingTestMessage)
	require.True(t, ok)
	require.Equal(t, "badgers", resp.SomeField)
	require.Equal(t, 1, list.getCalledCount())

	conn.Close()
}

func TestSendConcurrentRequests(t *testing.T) {
	list := &echoListener{}
	server := startServerWithHandler(t, list, conf.TLSConfig{})
	defer stopServers(t, server)

	conn, err := createConnection(server.ListenAddress(), nil)
	require.NoError(t, err)

	numRequests := 100
	var respHandlers []*testRespHandler
	for i := 0; i < numRequests; i++ {
		msg := &remotingmsgs.RemotingTestMessage{SomeField: fmt.Sprintf("badgers-%d", i)}
		rh := newRespHandler()
		err = conn.QueueRequest(msg, rh)
		require.NoError(t, err)
		respHandlers = append(respHandlers, rh)
	}

	for i, rh := range respHandlers {
		r, err := rh.waitForResponse()
		require.NoError(t, err)
		resp, ok := r.(*remotingmsgs.RemotingTestMessage)
		require.True(t, ok)
		require.Equal(t, fmt.Sprintf("badgers-%d", i), resp.SomeField)
	}
	require.Equal(t, numRequests, list.getCalledCount())

	conn.Close()
}

func TestSendRequestTLS(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cli_test")
	if err != nil {
		log.Fatalf("failed to create tmp dir %v", err)
	}
	defer func() {
		err := os.RemoveAll(tmpDir)
		if err != nil {
			log.Fatalf("failed to remove temp dir: %v", err)
		}
	}()

	tlsConf := conf.TLSConfig{
		Enabled:         true,
		KeyPath:         serverKeyPath,
		CertPath:        serverCertPath,
		ClientCertsPath: serverCertPath,
		ClientAuth:      "require-and-verify-client-cert",
	}

	list := &echoListener{}
	server := startServerWithHandler(t, list, tlsConf)
	defer stopServers(t, server)

	clientTLSConfig, err := getClientTLSConfig(tlsConf)
	require.NoError(t, err)
	conn, err := createConnection(server.ListenAddress(), clientTLSConfig)
	require.NoError(t, err)

	msg := &remotingmsgs.RemotingTestMessage{SomeField: "badgers"}
	rh := newRespHandler()
	err = conn.QueueRequest(msg, rh)
	require.NoError(t, err)
	r, err := rh.waitForResponse()
	require.NoError(t, err)
	resp, ok := r.(*remotingmsgs.RemotingTestMessage)
	require.True(t, ok)
	require.Equal(t, "badgers", resp.SomeField)
	require.Equal(t, 1, list.getCalledCount())

	conn.Close()
}

func TestResponseInternalError(t *testing.T) {

	// Non Tektite errors will get logged and returned as internal error
	err := errwrap.New("spiders")

	testResponseError(t, err, func(t *testing.T, perr common.TektiteError) {
		t.Helper()
		require.Equal(t, common.InternalError, perr.Code)
	})

}

func TestResponseTektiteError(t *testing.T) {

	// Tektite errors will get passed through
	err := common.NewTektiteError(common.ExecuteQueryError, "unknown query foo")

	testResponseError(t, err, func(t *testing.T, terr common.TektiteError) {
		t.Helper()
		require.Equal(t, common.ExecuteQueryError, terr.Code)
		require.Equal(t, err.Msg, terr.Msg)
	})
}

func testResponseError(t *testing.T, respErr error, checkFunc func(*testing.T, common.TektiteError)) {
	t.Helper()
	server := startServerWithHandler(t, &returnErrListener{err: respErr}, conf.TLSConfig{})
	defer stopServers(t, server)

	conn, err := createConnection(server.ListenAddress(), nil)
	require.NoError(t, err)

	msg := &remotingmsgs.RemotingTestMessage{SomeField: "badgers"}
	rh := newRespHandler()
	err = conn.QueueRequest(msg, rh)
	require.NoError(t, err)
	r, err := rh.waitForResponse()
	require.Nil(t, r)
	require.Error(t, err)
	//goland:noinspection GoTypeAssertionOnErrors
	perr, ok := err.(common.TektiteError)
	require.True(t, ok)

	checkFunc(t, perr)

	conn.Close()
}

func TestConnectFailedNoServer(t *testing.T) {
	conn, err := createConnection("localhost:7888", nil)
	require.Error(t, err)
	require.Nil(t, conn)
}

func TestCloseConnectionFromServer(t *testing.T) {
	server := startServerWithHandler(t, &echoListener{}, conf.TLSConfig{})
	defer stopServers(t, server)

	conn, err := createConnection(server.ListenAddress(), nil)
	require.NoError(t, err)

	err = server.Stop()
	require.NoError(t, err)

	// Give a little time for the connection to be closed
	time.Sleep(1 * time.Second)

	handler := newRespHandler()
	err = conn.QueueRequest(&remotingmsgs.RemotingTestMessage{SomeField: "badgers"}, handler)

	require.Error(t, err)
	require.Equal(t, "connection closed", err.Error())

	conn.Close()
}

func TestUseOfClosedConnection(t *testing.T) {
	server := startServerWithHandler(t, &echoListener{}, conf.TLSConfig{})
	defer stopServers(t, server)

	conn, err := createConnection(server.ListenAddress(), nil)
	require.NoError(t, err)

	conn.Close()

	handler := newRespHandler()
	err = conn.QueueRequest(&remotingmsgs.RemotingTestMessage{SomeField: "badgers"}, handler)
	require.Error(t, err)
	require.Equal(t, "connection closed", err.Error())
}

func TestConnectionClosedHandler(t *testing.T) {
	server := startServerWithHandler(t, &echoListener{}, conf.TLSConfig{})
	defer stopServers(t, server)

	var ccs1 ccStruct
	var ccs2 ccStruct
	server.RegisterConnectionClosedHandler(ccs1.connectionClosed)
	server.RegisterConnectionClosedHandler(ccs2.connectionClosed)

	conn1, err := createConnection(server.ListenAddress(), nil)
	require.NoError(t, err)
	conn1.Close()

	conn2, err := createConnection(server.ListenAddress(), nil)
	require.NoError(t, err)
	conn2.Close()

	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		ids1 := ccs1.getConnectionIDs()
		ids2 := ccs2.getConnectionIDs()
		return reflect.DeepEqual(ids1, ids2) && len(ids1) == 2, nil
	}, 2*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
}

type ccStruct struct {
	lock          sync.Mutex
	connectionIDs []int
}

func (c *ccStruct) connectionClosed(connectionID int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.connectionIDs = append(c.connectionIDs, connectionID)
}

func (c *ccStruct) getConnectionIDs() []int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.connectionIDs
}

func startServerWithHandler(t *testing.T, handler BlockingClusterMessageHandler, tlsConf conf.TLSConfig) *server {
	t.Helper()
	return startServerWithHandlerAndAddresss(t, handler, tlsConf)
}

func startServerWithHandlerAndAddresss(t *testing.T, handler BlockingClusterMessageHandler, tlsConf conf.TLSConfig) *server {
	t.Helper()

	address, err := common.AddressWithPort("localhost")
	require.NoError(t, err)

	server := newServer(address, tlsConf)
	err = server.Start()
	require.NoError(t, err)
	server.RegisterBlockingMessageHandler(ClusterMessageRemotingTestMessage, handler)
	return server
}

func newRespHandler() *testRespHandler {
	handler := &testRespHandler{}
	handler.wg.Add(1)
	return handler
}

type testRespHandler struct {
	resp ClusterMessage
	err  error
	wg   sync.WaitGroup
}

func (t *testRespHandler) HandleResponse(resp ClusterMessage, err error) {
	t.resp = resp
	t.err = err
	t.wg.Done()
}

func (t *testRespHandler) waitForResponse() (ClusterMessage, error) {
	t.wg.Wait()
	return t.resp, t.err
}

func stopServers(t *testing.T, servers ...*server) {
	t.Helper()
	for _, server := range servers {
		err := server.Stop()
		require.NoError(t, err)
	}
}

type echoListener struct {
	calledCount int64
	delay       time.Duration
}

func (e *echoListener) HandleMessage(messageHolder MessageHolder) (ClusterMessage, error) {
	if e.delay != 0 {
		time.Sleep(e.delay)
	}
	atomic.AddInt64(&e.calledCount, 1)
	return messageHolder.Message, nil
}

func (e *echoListener) getCalledCount() int {
	return int(atomic.LoadInt64(&e.calledCount))
}

type returnErrListener struct {
	err   error
	delay time.Duration
}

func (e *returnErrListener) HandleMessage(_ MessageHolder) (ClusterMessage, error) {
	if e.delay != 0 {
		time.Sleep(e.delay)
	}
	return nil, e.err
}
