package transport

import (
	"encoding/binary"
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func runTestCases(t *testing.T, serverFactory ServerFactory, connFactory ConnectionFactory) {
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			tc.f(t, serverFactory, connFactory)
		})
	}
}

type testFunc func(t *testing.T, serverFactory ServerFactory, connFactory ConnectionFactory)

type ServerFactory func(t *testing.T) Server

type testCase struct {
	caseName string
	f        testFunc
}

var testCases = []testCase{
	{caseName: "testRPC", f: testRPC},
	{caseName: "testOneway", f: testOneway},
	{caseName: "testErrorResponseTektiteError", f: testErrorResponseTektiteError},
	{caseName: "testErrorResponseUnexpectedError", f: testErrorResponseUnexpectedError},
	{caseName: "testInterleavedRPCs", f: testInterleavedRPCs},
	{caseName: "testConnectionIDs", f: testConnectionIDs},
}

func testRPC(t *testing.T, serverFactory ServerFactory, connFactory ConnectionFactory) {
	numNodes := 5
	// Create servers
	var servers []Server
	for i := 0; i < numNodes; i++ {
		servers = append(servers, serverFactory(t))
	}
	defer func() {
		for _, server := range servers {
			err := server.Stop()
			require.NoError(t, err)
		}
	}()
	// Register handlers
	numHandlerIDs := 10
	for i, server := range servers {
		nodeNum := i
		for j := 0; j < numHandlerIDs; j++ {
			handlerID := j
			server.RegisterHandler(handlerID, func(_ *ConnectionContext, request []byte, responseBuff []byte, responseWriter ResponseWriter) error {
				resp := fmt.Sprintf("node-%d-handler-id-%d-response-%s", nodeNum, handlerID, string(request))
				responseBuff = append(responseBuff, []byte(resp)...)
				return responseWriter(responseBuff, nil)
			})
		}
	}
	numRequestsPerHandler := 10
	for i, destServer := range servers {
		conn, err := connFactory(destServer.Address())
		require.NoError(t, err)
		for j := 0; j < numRequestsPerHandler; j++ {
			for l := 0; l < numHandlerIDs; l++ {
				request := fmt.Sprintf("request-%d-handler-id-%d", j, l)
				response, err := conn.SendRPC(l, []byte(request))
				require.NoError(t, err)
				expectedResponse := fmt.Sprintf("node-%d-handler-id-%d-response-%s", i, l, request)
				require.Equal(t, expectedResponse, string(response))
			}
		}
		err = conn.Close()
		require.NoError(t, err)
	}
}

func testOneway(t *testing.T, serverFactory ServerFactory, connFactory ConnectionFactory) {
	numNodes := 5
	// Create servers
	var servers []Server
	for i := 0; i < numNodes; i++ {
		servers = append(servers, serverFactory(t))
	}
	defer func() {
		for _, server := range servers {
			err := server.Stop()
			require.NoError(t, err)
		}
	}()

	var receiverReqs sync.Map

	// Register handlers
	numHandlerIDs := 10
	for i, server := range servers {
		for j := 0; j < numHandlerIDs; j++ {
			handlerID := j
			server.RegisterHandler(handlerID, func(_ *ConnectionContext, request []byte, responseBuff []byte, responseWriter ResponseWriter) error {
				receivedMsg := fmt.Sprintf("node-%d-%s", i, string(request))
				_, exists := receiverReqs.Load(receivedMsg)
				if exists {
					panic("duplicate receiver message")
				}
				receiverReqs.Store(receivedMsg, struct{}{})
				return nil
			})
		}
	}
	numRequestsPerHandler := 10
	var conns []Connection
	var expectedReceived sync.Map
	for i, destServer := range servers {
		conn, err := connFactory(destServer.Address())
		require.NoError(t, err)
		conns = append(conns, conn)
		for j := 0; j < numRequestsPerHandler; j++ {
			for l := 0; l < numHandlerIDs; l++ {
				request := fmt.Sprintf("request-%d-handler-id-%d", j, l)
				err := conn.SendOneway(l, []byte(request))
				require.NoError(t, err)
				receivedMsg := fmt.Sprintf("node-%d-%s", i, request)
				expectedReceived.Store(receivedMsg, struct{}{})
			}
		}
	}
	expectedCount := len(servers) * numRequestsPerHandler * numHandlerIDs
	testutils.WaitUntil(t, func() (bool, error) {
		count := 0
		receiverReqs.Range(func(key, value interface{}) bool {
			count++
			return true
		})
		if count != expectedCount {
			return false, nil
		}
		failed := false
		expectedReceived.Range(func(key, value interface{}) bool {
			msg := key.(string)
			_, exists := receiverReqs.Load(msg)
			if !exists {
				failed = true
				return false
			}
			return true
		})
		return !failed, nil
	})
	for _, conn := range conns {
		err := conn.Close()
		require.NoError(t, err)
	}
}

func testErrorResponseTektiteError(t *testing.T, serverFactory ServerFactory, connFactory ConnectionFactory) {
	// Tektite errors are sent back as-is
	err := common.NewTektiteErrorf(common.WasmError, "some wasm error")
	testErrorResponseWithError(t, serverFactory, connFactory, err, err)
}

func testErrorResponseUnexpectedError(t *testing.T, serverFactory ServerFactory, connFactory ConnectionFactory) {
	// Unexpected errors are sent back as a tektite error - internal server error
	respErr := &OtherError{Msg: "some other error"}
	expectedErr := common.NewTektiteError(common.InternalError, respErr.Msg)
	testErrorResponseWithError(t, serverFactory, connFactory, respErr, expectedErr)
}

type OtherError struct {
	Msg string
}

func (o OtherError) Error() string {
	return o.Msg
}

func testErrorResponseWithError(t *testing.T, serverFactory ServerFactory, connFactory ConnectionFactory, respErr error,
	expectedErr error) {
	server := serverFactory(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	handlerID := 23
	server.RegisterHandler(handlerID, func(_ *ConnectionContext, request []byte, responseBuff []byte, responseWriter ResponseWriter) error {
		return responseWriter(nil, respErr)
	})
	conn, err := connFactory(server.Address())
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()
	response, err := conn.SendRPC(handlerID, []byte("foo"))
	require.Error(t, err)
	require.Nil(t, response)
	require.Equal(t, expectedErr, err)
}

func testInterleavedRPCs(t *testing.T, serverFactory ServerFactory, connFactory ConnectionFactory) {
	server := serverFactory(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	handlerID := 23
	server.RegisterHandler(handlerID, func(_ *ConnectionContext, request []byte, responseBuff []byte, responseWriter ResponseWriter) error {
		// Send back response async
		// Need to copy request as handler responds async
		requestCopy := common.ByteSliceCopy(request)
		go func() {
			resp := []byte(fmt.Sprintf("%s-response", string(requestCopy)))
			responseBuff = append(responseBuff, resp...)
			if err := responseWriter(responseBuff, nil); err != nil {
				panic(err)
			}
		}()
		return nil
	})
	conn, err := connFactory(server.Address())
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()
	numRequests := 100
	type rpcResult struct {
		resp []byte
		err  error
	}
	chans := make([]chan rpcResult, 0, numRequests)
	for i := 0; i < numRequests; i++ {
		request := fmt.Sprintf("request-%d", i)
		ch := make(chan rpcResult, 1)
		go func() {
			resp, err := conn.SendRPC(handlerID, []byte(request))
			ch <- rpcResult{resp, err}
		}()
		chans = append(chans, ch)
	}
	for i, ch := range chans {
		res := <-ch
		require.NoError(t, res.err)
		expectedResult := fmt.Sprintf("request-%d-response", i)
		require.Equal(t, expectedResult, string(res.resp))
	}
}

func testConnectionIDs(t *testing.T, serverFactory ServerFactory, connFactory ConnectionFactory) {
	server := serverFactory(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	handlerID := 23
	server.RegisterHandler(handlerID, func(ctx *ConnectionContext, request []byte, responseBuff []byte, responseWriter ResponseWriter) error {
		responseBuff = binary.BigEndian.AppendUint64(responseBuff, uint64(ctx.ConnectionID))
		return responseWriter(responseBuff, nil)
	})
	numConns := 10
	prevConnID := -1
	for i := 0; i < numConns; i++ {
		conn, err := connFactory(server.Address())
		require.NoError(t, err)
		defer func() {
			err := conn.Close()
			require.NoError(t, err)
		}()
		resp, err := conn.SendRPC(handlerID, []byte("foo"))
		require.NoError(t, err)
		connID := int(binary.BigEndian.Uint64(resp))
		if prevConnID != -1 {
			require.Equal(t, connID, prevConnID+1)
		}
		prevConnID = connID
	}
}
