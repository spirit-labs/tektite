package fetchcache

import (
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConnCacheGetConnections(t *testing.T) {

	localTransports := transport.NewLocalTransports()

	localTransport, err := localTransports.NewLocalServer(uuid.New().String())
	require.NoError(t, err)

	maxConnections := 10

	connCache := NewConnectionCache(localTransport.Address(), maxConnections, localTransports.CreateConnection)

	require.Equal(t, 0, connCache.NumConnections())

	allConns := map[transport.Connection]struct{}{}
	for i := 0; i < 10*maxConnections; i++ {
		conn, err := connCache.GetConnection()
		require.NoError(t, err)
		maxConn := i + 1
		if maxConn > maxConnections {
			maxConn = maxConnections
		}
		require.Equal(t, maxConn, connCache.NumConnections())
		require.NotNil(t, conn)
		allConns[conn] = struct{}{}
	}

	require.Equal(t, maxConnections, len(allConns))

	connCache.Close()

	require.Equal(t, 0, connCache.NumConnections())
}

func TestCloseInvidualConnections(t *testing.T) {

	localTransports := transport.NewLocalTransports()

	localTransport, err := localTransports.NewLocalServer(uuid.New().String())
	require.NoError(t, err)

	maxConnections := 10

	connCache := NewConnectionCache(localTransport.Address(), maxConnections, localTransports.CreateConnection)

	require.Equal(t, 0, connCache.NumConnections())

	var conns []transport.Connection
	for i := 0; i < maxConnections; i++ {
		conn, err := connCache.GetConnection()
		require.NoError(t, err)
		conns = append(conns, conn)
	}

	require.Equal(t, maxConnections, connCache.NumConnections())

	for i, conn := range conns {
		err := conn.Close()
		require.NoError(t, err)
		require.Equal(t, maxConnections-i-1, connCache.NumConnections())
	}

	// Now create more
	for i := 0; i < maxConnections; i++ {
		_, err := connCache.GetConnection()
		require.NoError(t, err)
	}

	require.Equal(t, maxConnections, connCache.NumConnections())
}
