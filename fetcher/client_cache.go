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
	clients       []ControlClient
	pos           int64
}

func newClientCache(maxClients int, clientFactory controllerClientFactory) *clientCache {
	return &clientCache{
		clients:       make([]ControlClient, maxClients),
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

func (cc *clientCache) getCachedClient() (ControlClient, int) {
	cc.lock.RLock()
	defer cc.lock.RUnlock()
	pos := atomic.AddInt64(&cc.pos, 1) - 1
	index := int(pos) % len(cc.clients)
	return cc.clients[index], index
}

func (cc *clientCache) createClient(index int) (ControlClient, error) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cl := cc.clients[index]
	if cl != nil {
		return cl, nil
	}
	cl, err := cc.clientFactory()
	if err != nil {
		return nil, err
	}
	cc.clients[index] = &clientWrapper{
		cc:     cc,
		index:  index,
		client: cl,
	}
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
	for _, cc := range cc.clients {
		if err := cc.Close(); err != nil {
			log.Warnf("failed to close controller client: %v", err)
		}
	}
}

type clientWrapper struct {
	cc     *clientCache
	index  int
	client ControlClient
}

func (c *clientWrapper) FetchTablesForPrefix(topicID int, partitionID int, prefix []byte, offsetStart int64) (lsm.OverlappingTables, int64, error) {
	return c.client.FetchTablesForPrefix(topicID, partitionID, prefix, offsetStart)
}

func (c *clientWrapper) Close() error {
	c.cc.deleteClient(c.index)
	return c.client.Close()
}
