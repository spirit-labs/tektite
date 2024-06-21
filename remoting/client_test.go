package remoting

import (
	"fmt"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRPC(t *testing.T) {
	server := startServerWithHandler(t, &echoListener{}, conf.TLSConfig{})
	defer stopServers(t, server)

	client := &Client{}

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	r, err := client.SendRPC(msg, server.ListenAddress())
	require.NoError(t, err)
	resp, ok := r.(*clustermsgs.RemotingTestMessage)
	require.True(t, ok)
	require.Equal(t, "badgers", resp.SomeField)

	client.Stop()
}

func TestRPCInternalError(t *testing.T) {
	err := errors.New("spiders")
	testRPCError(t, err, func(t *testing.T, tektiteError errors.TektiteError) {
		t.Helper()
		require.Equal(t, errors.InternalError, tektiteError.Code)
	})
}

func TestRPCTektiteError(t *testing.T) {
	err := errors.NewTektiteError(errors.ExecuteQueryError, "unknown query foo")
	extraData := "some_extra_data"
	err.ExtraData = []byte(extraData)
	testRPCError(t, err, func(t *testing.T, tektiteError errors.TektiteError) {
		t.Helper()
		require.Equal(t, err.Code, tektiteError.Code)
		require.Equal(t, err.Msg, tektiteError.Msg)
		require.Equal(t, extraData, string(tektiteError.ExtraData))
	})
}

func testRPCError(t *testing.T, respErr error, checkFunc func(*testing.T, errors.TektiteError)) {
	t.Helper()
	server := startServerWithHandler(t, &returnErrListener{err: respErr}, conf.TLSConfig{})
	defer stopServers(t, server)

	client := &Client{}

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	r, err := client.SendRPC(msg, server.ListenAddress())
	require.Error(t, err)
	require.Nil(t, r)
	//goland:noinspection GoTypeAssertionOnErrors
	perr, ok := err.(errors.TektiteError)
	require.True(t, ok)

	checkFunc(t, perr)

	client.Stop()
}

func TestRPCConnectionError(t *testing.T) {
	server := startServerWithHandler(t, &echoListener{}, conf.TLSConfig{})
	defer stopServers(t, server)

	client := &Client{}

	// Send request successfully
	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	r, err := client.SendRPC(msg, server.ListenAddress())
	require.NoError(t, err)
	resp, ok := r.(*clustermsgs.RemotingTestMessage)
	require.True(t, ok)
	require.Equal(t, "badgers", resp.SomeField)

	server.closeNetConns()

	// We sleep a bit to give time for the client connections to be closed, they'll now be in the map, but closed
	time.Sleep(500 * time.Millisecond)

	// Try and send another message - should still get through as connection will be recreated
	msg = &clustermsgs.RemotingTestMessage{SomeField: "foxes"}
	r, err = client.SendRPC(msg, server.ListenAddress())
	require.NoError(t, err)
	resp, ok = r.(*clustermsgs.RemotingTestMessage)
	require.True(t, ok)
	require.Equal(t, "foxes", resp.SomeField)

	client.Stop()
}

func TestRPCServerNotAvailable(t *testing.T) {
	server := startServerWithHandler(t, &echoListener{}, conf.TLSConfig{})

	client := &Client{}

	// Send request successfully
	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	r, err := client.SendRPC(msg, server.ListenAddress())
	require.NoError(t, err)
	resp, ok := r.(*clustermsgs.RemotingTestMessage)
	require.True(t, ok)
	require.Equal(t, "badgers", resp.SomeField)

	stopServers(t, server)

	// Try and send another message - should not get through
	r, err = client.SendRPC(msg, server.ListenAddress())
	require.Error(t, err)
	require.Nil(t, r)

	client.Stop()
}

func TestBroadcast(t *testing.T) {
	numServers := 3
	var servers []*server
	var listeners []*echoListener
	for i := 0; i < numServers; i++ {
		listener := &echoListener{}
		listeners = append(listeners, listener)
		servers = append(servers, startServerWithHandlerAndAddresss(t, listener, conf.TLSConfig{}))
	}
	defer stopServers(t, servers...)

	client := &Client{}

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	var serverAddresses []string
	for _, server := range servers {
		serverAddresses = append(serverAddresses, server.ListenAddress())
	}
	err := client.Broadcast(msg, serverAddresses...)
	require.NoError(t, err)
	for _, listener := range listeners {
		require.Equal(t, 1, listener.getCalledCount())
	}

	client.Stop()
}

func TestBroadcastErrorAllServers(t *testing.T) {
	numServers := 3
	var servers []*server
	respErr := errors.New("spiders")
	listener := &returnErrListener{err: respErr}
	for i := 0; i < numServers; i++ {
		servers = append(servers, startServerWithHandlerAndAddresss(t, listener, conf.TLSConfig{}))
	}
	defer stopServers(t, servers...)

	client := &Client{}

	var serverAddresses []string
	for _, server := range servers {
		serverAddresses = append(serverAddresses, server.ListenAddress())
	}
	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	err := client.Broadcast(msg, serverAddresses...)
	require.Error(t, err)
	//goland:noinspection GoTypeAssertionOnErrors
	perr, ok := err.(errors.TektiteError)
	require.True(t, ok)
	require.Equal(t, errors.InternalError, perr.Code)

	client.Stop()
}

func TestBroadcastErrorOneServerInternalErrorNoDelays(t *testing.T) {
	err := errors.New("spiders")
	testBroadcastErrorOneServer(t, err, 0, 0, func(t *testing.T, tektiteError errors.TektiteError) {
		t.Helper()
		require.Equal(t, errors.InternalError, tektiteError.Code)
	})
}

func TestBroadcastErrorOneServertektiteErrorNoDelays(t *testing.T) {
	err := errors.NewTektiteError(errors.ExecuteQueryError, "unknown query foo")
	testBroadcastErrorOneServer(t, err, 0, 0, func(t *testing.T, tektiteError errors.TektiteError) {
		t.Helper()
		require.Equal(t, err.Code, tektiteError.Code)
		require.Equal(t, err.Msg, tektiteError.Msg)
	})
}

func TestBroadcastErrorOneServertektiteErrorDelayError(t *testing.T) {
	err := errors.NewTektiteError(errors.ExecuteQueryError, "unknown query foo")
	testBroadcastErrorOneServer(t, err, 100*time.Millisecond, 0, func(t *testing.T, tektiteError errors.TektiteError) {
		t.Helper()
		require.Equal(t, err.Code, tektiteError.Code)
		require.Equal(t, err.Msg, tektiteError.Msg)
	})
}

func TestBroadcastErrorOneServertektiteErrorDelayNonError(t *testing.T) {
	err := errors.NewTektiteError(errors.ExecuteQueryError, "unknown query foo")
	testBroadcastErrorOneServer(t, err, 0, 100*time.Millisecond, func(t *testing.T, tektiteError errors.TektiteError) {
		t.Helper()
		require.Equal(t, err.Code, tektiteError.Code)
		require.Equal(t, err.Msg, tektiteError.Msg)
	})
}

func testBroadcastErrorOneServer(t *testing.T, respErr error, errDelay time.Duration, nonErrDelay time.Duration,
	checkFunc func(*testing.T, errors.TektiteError)) {
	t.Helper()
	numServers := 3
	var servers []*server

	for i := 0; i < numServers; i++ {
		var handler BlockingClusterMessageHandler
		// We put the error return on only one handler and we have optional delays on error and non error return
		// to check error and non error responses coming back in different orders
		if i == 1 {
			handler = &returnErrListener{err: respErr, delay: errDelay}
		} else {
			handler = &echoListener{delay: nonErrDelay}
		}
		servers = append(servers, startServerWithHandlerAndAddresss(t, handler, conf.TLSConfig{}))
	}
	defer stopServers(t, servers...)

	client := &Client{}

	var serverAddresses []string
	for _, server := range servers {
		serverAddresses = append(serverAddresses, server.ListenAddress())
	}
	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	err := client.Broadcast(msg, serverAddresses...)
	require.Error(t, err)
	//goland:noinspection GoTypeAssertionOnErrors
	perr, ok := err.(errors.TektiteError)
	require.True(t, ok)

	checkFunc(t, perr)

	client.Stop()
}

func TestBroadcastNoServers(t *testing.T) {
	numServers := 3
	var serverAddresses []string
	for i := 0; i < numServers; i++ {
		address := fmt.Sprintf("localhost:%d", 7888+i)
		serverAddresses = append(serverAddresses, address)
	}

	client := &Client{}

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	err := client.Broadcast(msg, serverAddresses...)
	require.Error(t, err)

	client.Stop()
}

func TestBroadcastConnectionError(t *testing.T) {
	numServers := 3
	var servers []*server
	var listeners []*echoListener
	for i := 0; i < numServers; i++ {
		listener := &echoListener{}
		listeners = append(listeners, listener)
		servers = append(servers, startServerWithHandlerAndAddresss(t, listener, conf.TLSConfig{}))
	}
	defer stopServers(t, servers...)

	client := &Client{}

	var serverAddresses []string
	for _, server := range servers {
		serverAddresses = append(serverAddresses, server.ListenAddress())
	}

	// Send broadcast successfully
	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	err := client.Broadcast(msg, serverAddresses...)
	require.NoError(t, err)
	for _, listener := range listeners {
		require.Equal(t, 1, listener.getCalledCount())
	}

	// Now kill all connections
	for _, server := range servers {
		server.closeNetConns()
	}

	// We sleep to allow time for the client connections to be closed
	time.Sleep(500 * time.Millisecond)

	// Try and send again - we should get through to all three
	err = client.Broadcast(msg, serverAddresses...)
	require.NoError(t, err)

	client.Stop()
}
