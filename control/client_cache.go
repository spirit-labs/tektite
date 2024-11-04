package control

import (
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/topicmeta"
	"sync"
	"sync/atomic"
)

// ClientCache is a goroutine-safe cache of controller clients
type ClientCache struct {
	lock          sync.RWMutex
	clientFactory ClientFactory
	clients       []*clientWrapper
	pos           int64
}

type ClientFactory func() (Client, error)

func NewClientCache(maxClients int, clientFactory ClientFactory) *ClientCache {
	return &ClientCache{
		clients:       make([]*clientWrapper, maxClients),
		clientFactory: clientFactory,
	}
}

func (cc *ClientCache) GetClient() (Client, error) {
	cl, index := cc.getCachedClient()
	if cl != nil {
		return cl, nil
	}
	return cc.createClient(index)
}

func (cc *ClientCache) getCachedClient() (*clientWrapper, int) {
	cc.lock.RLock()
	defer cc.lock.RUnlock()
	pos := atomic.AddInt64(&cc.pos, 1) - 1
	index := int(pos) % len(cc.clients)
	return cc.clients[index], index
}

func (cc *ClientCache) createClient(index int) (*clientWrapper, error) {
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

func (cc *ClientCache) deleteClient(index int) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cc.clients[index] = nil
}

func (cc *ClientCache) Close() {
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
	cc     *ClientCache
	index  int
	client Client
}

func (c *clientWrapper) PrePush(infos []offsets.GetOffsetTopicInfo, epochInfos []EpochInfo) ([]offsets.OffsetTopicInfo, int64, []bool, error) {
	offs, seq, epochsOK, err := c.client.PrePush(infos, epochInfos)
	if err != nil {
		c.closeConnection()
	}
	return offs, seq, epochsOK, err
}

func (c *clientWrapper) ApplyLsmChanges(regBatch lsm.RegistrationBatch) error {
	err := c.ApplyLsmChanges(regBatch)
	if err != nil {
		c.closeConnection()
	}
	return err
}

func (c *clientWrapper) RegisterL0Table(sequence int64, regEntry lsm.RegistrationEntry) error {
	err := c.RegisterL0Table(sequence, regEntry)
	if err != nil {
		c.closeConnection()
	}
	return err
}

func (c *clientWrapper) RegisterTableListener(topicID int, partitionID int, memberID int32,
	resetSequence int64) (int64, error) {
	lro, err := c.client.RegisterTableListener(topicID, partitionID, memberID, resetSequence)
	if err != nil {
		c.closeConnection()
	}
	return lro, err
}

func (c *clientWrapper) QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error) {
	queryRes, err := c.client.QueryTablesInRange(keyStart, keyEnd)
	if err != nil {
		c.closeConnection()
	}
	return queryRes, err
}

func (c *clientWrapper) PollForJob() (lsm.CompactionJob, error) {
	job, err := c.client.PollForJob()
	if err != nil {
		c.closeConnection()
	}
	return job, err
}

func (c *clientWrapper) GetTopicInfo(topicName string) (topicmeta.TopicInfo, int, error) {
	topicInfo, seq, err := c.client.GetTopicInfo(topicName)
	if err != nil {
		c.closeConnection()
	}
	return topicInfo, seq, err
}

func (c *clientWrapper) CreateTopic(topicInfo topicmeta.TopicInfo) error {
	err := c.client.CreateTopic(topicInfo)
	if err != nil {
		c.closeConnection()
	}
	return err
}

func (c *clientWrapper) DeleteTopic(topicName string) error {
	err := c.client.DeleteTopic(topicName)
	if err != nil {
		c.closeConnection()
	}
	return err
}

func (c *clientWrapper) GetCoordinatorInfo(groupID string) (int32, string, int, error) {
	memberID, address, groupEpoch, err := c.client.GetCoordinatorInfo(groupID)
	if err != nil {
		c.closeConnection()
	}
	return memberID, address, groupEpoch, err
}

func (c *clientWrapper) closeConnection() {
	// always close connection on error
	if err := c.Close(); err != nil {
		// Ignore
	}
}

func (c *clientWrapper) Close() error {
	c.cc.deleteClient(c.index)
	return c.client.Close()
}
