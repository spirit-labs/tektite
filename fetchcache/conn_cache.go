package fetchcache

import (
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/transport"
	"sync"
	"sync/atomic"
)

// ConnectionCache caches a set of connections for a particular target address
type ConnectionCache struct {
	lock        sync.RWMutex
	address     string
	connFactory transport.ConnectionFactory
	connections []*connectionWrapper
	pos         int64
}

func NewConnectionCache(address string, maxConnections int, connFactory transport.ConnectionFactory) *ConnectionCache {
	return &ConnectionCache{
		address:     address,
		connections: make([]*connectionWrapper, maxConnections),
		connFactory: connFactory,
	}
}

func (cc *ConnectionCache) GetConnection() (transport.Connection, error) {
	cl, index := cc.getCachedConnection()
	if cl != nil {
		return cl, nil
	}
	return cc.createConnection(index)
}

func (cc *ConnectionCache) Close() {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	for i, client := range cc.connections {
		if client != nil {
			if err := client.conn.Close(); err != nil {
				log.Warnf("failed to close connection: %v", err)
			}
		}
		cc.connections[i] = nil
	}
}

func (cc *ConnectionCache) NumConnections() int {
	cc.lock.RLock()
	defer cc.lock.RUnlock()
	num := 0
	for _, client := range cc.connections {
		if client != nil {
			num++
		}
	}
	return num
}

func (cc *ConnectionCache) getCachedConnection() (*connectionWrapper, int) {
	cc.lock.RLock()
	defer cc.lock.RUnlock()
	pos := atomic.AddInt64(&cc.pos, 1) - 1
	index := int(pos) % len(cc.connections)
	return cc.connections[index], index
}

func (cc *ConnectionCache) createConnection(index int) (*connectionWrapper, error) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cl := cc.connections[index]
	if cl != nil {
		return cl, nil
	}
	conn, err := cc.connFactory(cc.address)
	if err != nil {
		return nil, err
	}
	cl = &connectionWrapper{
		cc:    cc,
		index: index,
		conn:  conn,
	}
	cc.connections[index] = cl
	return cl, nil
}

func (cc *ConnectionCache) deleteClient(index int) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cc.connections[index] = nil
}

type connectionWrapper struct {
	cc    *ConnectionCache
	index int
	conn  transport.Connection
}

func (c *connectionWrapper) SendOneway(handlerID int, message []byte) error {
	return c.conn.SendOneway(handlerID, message)
}

func (c *connectionWrapper) SendRPC(handlerID int, request []byte) ([]byte, error) {
	return c.conn.SendRPC(handlerID, request)
}

func (c *connectionWrapper) Close() error {
	c.cc.deleteClient(c.index)
	return c.conn.Close()
}
