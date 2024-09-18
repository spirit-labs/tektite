package transport

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLocalTransport(t *testing.T) {
	serverFactory, connFactory := setupLocalTransports(t)
	runTestCases(t, serverFactory, connFactory)
}

func setupLocalTransports(t *testing.T) (ServerFactory, ConnectionFactory) {
	localTransports := NewLocalTransports()
	serverFactory := func(t *testing.T) Server {
		address := uuid.New().String()
		server, err := localTransports.NewLocalServer(address)
		require.NoError(t, err)
		return server
	}
	return serverFactory, localTransports.CreateConnection
}
