package transport

import (
	log "github.com/spirit-labs/tektite/logger"
	"sync"
	"sync/atomic"
)

// ConnectionCache caches a set of connections for a particular target address
type ConnectionCache struct {
	lock        sync.RWMutex
	address     string
	connFactory ConnectionFactory
	connections []*connectionWrapper
	pos         int64
}

func NewConnectionCache(address string, maxConnections int, connFactory ConnectionFactory) *ConnectionCache {
	return &ConnectionCache{
		address:     address,
		connections: make([]*connectionWrapper, maxConnections),
		connFactory: connFactory,
	}
}

func (cc *ConnectionCache) GetConnection() (Connection, error) {
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

func (cc *ConnectionCache) deleteConnection(index int) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cc.connections[index] = nil
}

type connectionWrapper struct {
	cc    *ConnectionCache
	index int
	conn  Connection
}

func (c *connectionWrapper) SendOneway(handlerID int, message []byte) error {
	return c.conn.SendOneway(handlerID, message)
}

func (c *connectionWrapper) SendRPC(handlerID int, request []byte) ([]byte, error) {
	return c.conn.SendRPC(handlerID, request)
}

func (c *connectionWrapper) Close() error {
	c.cc.deleteConnection(c.index)
	return c.conn.Close()
}

type ConnCaches struct {
	maxConnectionsPerAddress int
	connFactory              ConnectionFactory
	connCachesLock           sync.RWMutex
	connCaches               map[string]*ConnectionCache
}

func NewConnCaches(maxConnectionsPerAddress int, connFactory ConnectionFactory) *ConnCaches {
	return &ConnCaches{
		maxConnectionsPerAddress: maxConnectionsPerAddress,
		connFactory:              connFactory,
		connCaches:               map[string]*ConnectionCache{},
	}
}

func (c *ConnCaches) GetConnection(address string) (Connection, error) {
	connCache, ok := c.getConnCache(address)
	if !ok {
		connCache = c.createConnCache(address)
	}
	return connCache.GetConnection()
}

func (c *ConnCaches) Close() {
	c.connCachesLock.Lock()
	defer c.connCachesLock.Unlock()
	for _, connCache := range c.connCaches {
		connCache.Close()
	}
	c.connCaches = make(map[string]*ConnectionCache)
}

func (c *ConnCaches) getConnCache(address string) (*ConnectionCache, bool) {
	c.connCachesLock.RLock()
	defer c.connCachesLock.RUnlock()
	connCache, ok := c.connCaches[address]
	return connCache, ok
}

func (c *ConnCaches) createConnCache(address string) *ConnectionCache {
	c.connCachesLock.Lock()
	defer c.connCachesLock.Unlock()
	connCache, ok := c.connCaches[address]
	if ok {
		return connCache
	}
	connCache = NewConnectionCache(address, c.maxConnectionsPerAddress, c.connFactory)
	c.connCaches[address] = connCache
	return connCache
}
