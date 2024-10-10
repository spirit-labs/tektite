package fetcher

import (
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"sync"
	"sync/atomic"
)

// clientCache is a goroutine-safe cache of controller clients
type clientCache struct {
	lock          sync.RWMutex
	clientFactory controllerClientFactory
	clients       []*clientWrapper
	pos           int64
}

func newClientCache(maxClients int, clientFactory controllerClientFactory) *clientCache {
	return &clientCache{
		clients:       make([]*clientWrapper, maxClients),
		clientFactory: clientFactory,
	}
}

func (cc *clientCache) getClient() (ControlClient, error) {
	cl, index := cc.getCachedClient()
	if cl != nil {
		return cl, nil
	}
	return cc.createClient(index)
}

func (cc *clientCache) getCachedClient() (*clientWrapper, int) {
	cc.lock.RLock()
	defer cc.lock.RUnlock()
	pos := atomic.AddInt64(&cc.pos, 1) - 1
	index := int(pos) % len(cc.clients)
	return cc.clients[index], index
}

func (cc *clientCache) createClient(index int) (*clientWrapper, error) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cl := cc.clients[index]
	if cl != nil {
		return cl, nil
	}
	cli, err := cc.clientFactory()
	if err != nil {
		return nil, err
	}
	cl = &clientWrapper{
		cc:     cc,
		index:  index,
		client: cli,
	}
	cc.clients[index] = cl
	return cl, nil
}

func (cc *clientCache) deleteClient(index int) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cc.clients[index] = nil
}

func (cc *clientCache) close() {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	for _, cl := range cc.clients {
		if cl != nil {
			if err := cl.client.Close(); err != nil {
				log.Warnf("failed to close controller client: %v", err)
			}
		}
	}
}

type clientWrapper struct {
	cc     *clientCache
	index  int
	client ControlClient
}

func (c *clientWrapper) RegisterTableListener(topicID int, partitionID int, address string,
	resetSequence int64) (int64, error) {
	lro, err := c.client.RegisterTableListener(topicID, partitionID, address, resetSequence)
	if err != nil {
		// always close connection on error
		if err := c.Close(); err != nil {
			// Ignore
		}
	}
	return lro, err
}

func (c *clientWrapper) QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error) {
	queryRes, err := c.client.QueryTablesInRange(keyStart, keyEnd)
	if err != nil {
		// always close connection on error
		if err := c.Close(); err != nil {
			// Ignore
		}
	}
	return queryRes, err
}

func (c *clientWrapper) Close() error {
	c.cc.deleteClient(c.index)
	return c.client.Close()
}
