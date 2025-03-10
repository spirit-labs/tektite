package topicmeta

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/lsm"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/queryutils"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/transport"
)

/*
Manager lives on the controller and manages topic metadata persistently. Topic metadata includes the topic name, the
topic id and number of partitions. Methods exist to create and delete topics which don't return until the topic
metadata has been written to object storage.
When a topic is created or deleted a notification is sent to the local cache instances which live on all the non leader
agents, so the topic can be added or removed from the cache.
*/
type Manager struct {
	lock             sync.RWMutex
	started          bool
	lsm              lsmHolder
	objStore         objstore.Client
	kvWriter         kvWriter
	dataBucketName   string
	dataFormat       common.DataFormat
	topicInfosByName map[string]*TopicInfo
	topicInfosByID   map[int]*TopicInfo
	topicIDSequence  int64
	stopping         atomic.Bool
	membership       atomic.Value
	connCaches       *transport.ConnCaches
	dataPrefix       []byte
}

type lsmHolder interface {
	QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error)
	ApplyLsmChanges(regBatch lsm.RegistrationBatch, completionFunc func(error) error) error
}

type kvWriter func([]common.KV) error

func NewManager(lsm lsmHolder, objStore objstore.Client, dataBucketName string,
	dataFormat common.DataFormat, connCaches *transport.ConnCaches, kvWriter kvWriter) (*Manager, error) {
	dataPrefix, err := parthash.CreateHash([]byte("topic.meta"))
	if err != nil {
		return nil, err
	}
	return &Manager{
		lsm:              lsm,
		objStore:         objStore,
		kvWriter:         kvWriter,
		dataBucketName:   dataBucketName,
		dataFormat:       dataFormat,
		topicInfosByName: make(map[string]*TopicInfo),
		topicInfosByID:   make(map[int]*TopicInfo),
		connCaches:       connCaches,
		dataPrefix:       dataPrefix,
	}, nil
}

const (
	objStoreCallTimeout             = 5 * time.Second
	unavailabilityRetryDelay        = 1 * time.Second
	topicMetadataVersion     uint16 = 1
	TopicIDSequenceBase             = 1000
)

func (m *Manager) Start() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.started {
		return nil
	}
	if err := m.loadTopics(); err != nil {
		return err
	}
	m.started = true
	return nil
}

func (m *Manager) Stop() error {
	m.stopping.Store(true)
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return nil
	}
	m.started = false
	return nil
}

func (m *Manager) GetTopicInfoByID(topicID int) (TopicInfo, bool, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if !m.started {
		return TopicInfo{}, false, errors.New("topicmeta manager not started")
	}
	info, ok := m.topicInfosByID[topicID]
	if !ok {
		return TopicInfo{}, false, nil
	}
	return *info, true, nil
}

func (m *Manager) GetAllTopicInfos() ([]TopicInfo, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if !m.started {
		return nil, errors.New("topicmeta manager not started")
	}
	allInfos := make([]TopicInfo, 0, len(m.topicInfosByID))
	for _, info := range m.topicInfosByID {
		allInfos = append(allInfos, *info)
	}
	return allInfos, nil
}

func (m *Manager) GetTopicInfo(topicName string) (TopicInfo, int, bool, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if !m.started {
		return TopicInfo{}, 0, false, errors.New("topicmeta manager not started")
	}
	info, ok := m.topicInfosByName[topicName]
	if !ok {
		return TopicInfo{}, 0, false, nil
	}
	return *info, int(m.topicIDSequence), true, nil
}

func (m *Manager) CreateOrUpdateTopic(topicInfo TopicInfo, create bool) (int, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if topicInfo.PartitionCount < 1 {
		return 0, common.NewTektiteErrorf(common.InvalidPartitionCount, "invalid partition count %d", topicInfo.PartitionCount)
	}
	info, ok := m.topicInfosByName[topicInfo.Name]
	if create {
		if ok {
			return 0, common.NewTektiteErrorf(common.TopicAlreadyExists, "topic: %s already exists", topicInfo.Name)
		}
		topicInfo.ID = int(m.topicIDSequence)
		log.Debugf("%p created topic with id %d name %s partitions %d", m, topicInfo.ID, topicInfo.Name, topicInfo.PartitionCount)
		m.topicIDSequence++
		if topicInfo.RetentionTime == 0 {
			topicInfo.RetentionTime = -1
		}
	} else {
		if !ok {
			return 0, common.NewTektiteErrorf(common.TopicDoesNotExist, "topic: %s does not exist", topicInfo.Name)
		}
		if topicInfo.PartitionCount < info.PartitionCount {
			return 0, common.NewTektiteErrorf(common.InvalidPartitionCount, "cannot reduce partition count")
		}
		topicInfo.ID = info.ID
		topicInfo.RetentionTime = info.RetentionTime
		topicInfo.Compacted = info.Compacted
	}
	if err := m.WriteTopic(topicInfo); err != nil {
		return 0, err
	}
	m.topicInfosByName[topicInfo.Name] = &topicInfo
	m.topicInfosByID[topicInfo.ID] = &topicInfo
	m.SendTopicNotification(transport.HandlerIDMetaLocalCacheTopicAdded, topicInfo)
	return topicInfo.ID, nil
}

func (m *Manager) DeleteTopic(topicName string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	info, ok := m.topicInfosByName[topicName]
	if !ok {
		return common.NewTektiteErrorf(common.TopicDoesNotExist, "topic: %s does not exist", topicName)
	}
	// Note, we increment sequence on delete too, this is because it used to track any change in topics and is sent
	// in notifications so local caches can detect whether they have missed any notifications and invalidate
	m.topicIDSequence++
	if err := m.WriteTopicDeletion(info.ID); err != nil {
		return err
	}
	delete(m.topicInfosByName, topicName)
	delete(m.topicInfosByID, info.ID)
	m.SendTopicNotification(transport.HandlerIDMetaLocalCacheTopicDeleted, *info)
	return nil
}

func (m *Manager) loadTopics() error {
	allTopics, err := m.loadAllTopicsFromStorageWithRetry()
	if err != nil {
		return err
	}
	for _, topicInfo := range allTopics {
		m.topicInfosByName[topicInfo.Name] = &topicInfo
		m.topicInfosByID[topicInfo.ID] = &topicInfo
	}
	return nil
}

func (m *Manager) loadAllTopicsFromStorageWithRetry() ([]TopicInfo, error) {
	for {
		infos, err := m.loadAllTopicsFromStorage()
		if err == nil {
			return infos, nil
		}
		if m.stopping.Load() {
			return nil, errors.New("topicmeta manager is stopping")
		}
		if common.IsUnavailableError(err) {
			log.Warnf("Unable to load topics to unavailability, will retry after delay: %v", err)
			time.Sleep(unavailabilityRetryDelay)
		}
		return nil, err
	}
}

func (m *Manager) loadAllTopicsFromStorage() ([]TopicInfo, error) {
	keyEnd := common.IncBigEndianBytes(m.dataPrefix)
	tg := &tableGetter{
		bucketName: m.dataBucketName,
		objStore:   m.objStore,
	}
	mi, err := queryutils.CreateIteratorForKeyRange(m.dataPrefix, keyEnd, m.lsm, tg.GetSSTable)
	if err != nil {
		return nil, err
	}
	if mi == nil {
		m.topicIDSequence = TopicIDSequenceBase
		return nil, nil
	}
	defer mi.Close()
	var allTopics []TopicInfo
	for {
		ok, kv, err := mi.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		var info TopicInfo
		topicMetaVersion := binary.BigEndian.Uint16(kv.Value)
		if topicMetaVersion != topicMetadataVersion {
			return nil, errors.Errorf("invalid topic metadata version %d", topicMetaVersion)
		}
		info.Deserialize(kv.Value, 2)
		allTopics = append(allTopics, info)
	}
	if len(allTopics) > 0 {
		m.topicIDSequence = int64(allTopics[len(allTopics)-1].ID + 1)
	} else {
		m.topicIDSequence = TopicIDSequenceBase
	}
	return allTopics, nil
}

func (m *Manager) WriteTopic(topicInfo TopicInfo) error {
	key := encoding.KeyEncodeInt(m.dataPrefix, int64(topicInfo.ID))
	key = encoding.EncodeVersion(key, 0)
	// Encode a version number before the data
	value := binary.BigEndian.AppendUint16(nil, topicMetadataVersion)
	value = topicInfo.Serialize(value)
	value = common.AppendValueMetadata(value)
	return m.kvWriter([]common.KV{{Key: key, Value: value}})
}

func (m *Manager) WriteTopicDeletion(topicID int) error {
	key := encoding.KeyEncodeInt(m.dataPrefix, int64(topicID))
	key = encoding.EncodeVersion(key, 0)
	// Write a tombstone (nil value)
	return m.kvWriter([]common.KV{{Key: key}})
}

type tableGetter struct {
	bucketName string
	objStore   objstore.Client
}

func (n *tableGetter) GetSSTable(tableID sst.SSTableID) (*sst.SSTable, error) {
	buff, err := objstore.GetWithTimeout(n.objStore, n.bucketName, string(tableID), objStoreCallTimeout)
	if err != nil {
		return nil, err
	}
	return sst.GetSSTableFromBytes(buff)
}

type TopicNotification struct {
	Sequence int
	Info     TopicInfo
}

func (t *TopicNotification) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(t.Sequence))
	return t.Info.Serialize(buff)
}

func (t *TopicNotification) Deserialize(buff []byte, offset int) int {
	t.Sequence = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	return t.Info.Deserialize(buff, offset)
}

func (m *Manager) MembershipChanged(membership cluster.MembershipState) {
	m.membership.Store(membership)
}

func (m *Manager) sendNotificationToAddress(handlerID int, address string, notif []byte) error {
	conn, err := m.connCaches.GetConnection(address)
	if err != nil {
		return err
	}
	if _, err := conn.SendRPC(handlerID, notif); err != nil {
		if err2 := conn.Close(); err2 != nil {
			// Ignore
		}
		return err
	}
	return nil
}

func (m *Manager) SendTopicNotification(handlerID int, topicInfo TopicInfo) {
	var clusterMembership cluster.MembershipState
	memb := m.membership.Load()
	if memb != nil {
		clusterMembership = memb.(cluster.MembershipState)
	}
	if len(clusterMembership.Members) > 0 {
		notif := TopicNotification{
			Sequence: int(m.topicIDSequence),
			Info:     topicInfo,
		}
		bytes := notif.Serialize(nil)
		for i := 0; i < len(clusterMembership.Members); i++ {
			var memberData common.MembershipData
			memberData.Deserialize(clusterMembership.Members[i].Data, 0)
			if err := m.sendNotificationToAddress(handlerID, memberData.ClusterListenAddress, bytes); err != nil {
				// best effort - continue
				log.Warnf("Unable to send topic added notification: %v", err)
			}
		}
	}
}
