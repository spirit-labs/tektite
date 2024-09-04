package transport

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLocalTransport(t *testing.T) {
	localTransports := NewLocalTransports()

	nodeCount := 5
	nodes := make([]*LocalTransport, nodeCount)
	addresses := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		address := fmt.Sprintf("node-%d", i)
		addresses[i] = address
		node, err := localTransports.NewLocalTransport(address)
		require.NoError(t, err)
		nodes[i] = node
	}

	nodes[1].RegisterHandler(func(message []byte, responseWriter ResponseHandler) error {
		resp := []byte("some response")
		return responseWriter(resp)
	})

	ch := make(chan string, 1)
	conn, err := nodes[0].CreateConnection(addresses[1], func(response []byte) error {
		ch <- string(response)
		return nil
	})
	require.NoError(t, err)

	msg := []byte("some message")
	err = conn.WriteMessage(msg)
	require.NoError(t, err)

	resp := <-ch
	require.Equal(t, "some response", resp)
}
