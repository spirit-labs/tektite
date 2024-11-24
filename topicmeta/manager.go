package topicmeta

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/queryutils"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/transport"
	"sync"
	"sync/atomic"
	"time"
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
	dataBucketName   string
	dataFormat       common.DataFormat
	topicInfosByName map[string]*TopicInfo
	topicInfosByID   map[int]*TopicInfo
	topicIDSequence  int64
	stopping         atomic.Bool
	membership       cluster.MembershipState
	connFactory      transport.ConnectionFactory
	connections      map[string]transport.Connection
}

type lsmHolder interface {
	QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error)
	ApplyLsmChanges(regBatch lsm.RegistrationBatch, completionFunc func(error) error) error
}

func NewManager(lsm lsmHolder, objStore objstore.Client, dataBucketName string,
	dataFormat common.DataFormat, connFactory transport.ConnectionFactory) (*Manager, error) {
	return &Manager{
		lsm:              lsm,
		objStore:         objStore,
		dataBucketName:   dataBucketName,
		dataFormat:       dataFormat,
		topicInfosByName: make(map[string]*TopicInfo),
		topicInfosByID:   make(map[int]*TopicInfo),
		connFactory:      connFactory,
		connections:      make(map[string]transport.Connection),
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

func (m *Manager) CreateTopic(topicInfo TopicInfo) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, ok := m.topicInfosByName[topicInfo.Name]
	if ok {
		return common.NewTektiteErrorf(common.TopicAlreadyExists, "topic: %s already exists", topicInfo.Name)
	}
	topicInfo.ID = int(m.topicIDSequence)
	log.Debugf("%p created topic with id %d name %s partitions %d", m, topicInfo.ID, topicInfo.Name, topicInfo.PartitionCount)
	m.topicIDSequence++
	if err := m.WriteTopic(topicInfo); err != nil {
		return err
	}
	m.topicInfosByName[topicInfo.Name] = &topicInfo
	m.topicInfosByID[topicInfo.ID] = &topicInfo
	m.SendTopicNotification(transport.HandlerIDMetaLocalCacheTopicAdded, topicInfo)
	return nil
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
			return nil, errors.New("offsetloader is stopping")
		}
		if common.IsUnavailableError(err) {
			log.Warnf("Unable to load topics to unavailability, will retry after delay: %v", err)
			time.Sleep(unavailabilityRetryDelay)
		}
	}
}

func (m *Manager) loadAllTopicsFromStorage() ([]TopicInfo, error) {
	prefix := createPrefix()
	keyEnd := common.IncBigEndianBytes(prefix)
	tg := &tableGetter{
		bucketName: m.dataBucketName,
		objStore:   m.objStore,
	}
	mi, err := queryutils.CreateIteratorForKeyRange(prefix, keyEnd, m.lsm, tg.GetSSTable)
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
	prefix := createPrefix()
	key := encoding.KeyEncodeInt(prefix, int64(topicInfo.ID))
	key = encoding.EncodeVersion(key, 0)
	// Encode a version number before the data
	buff := binary.BigEndian.AppendUint16(nil, topicMetadataVersion)
	value := topicInfo.Serialize(buff)
	return m.writeKV(common.KV{Key: key, Value: value})
}

func (m *Manager) WriteTopicDeletion(topicID int) error {
	prefix := createPrefix()
	key := encoding.KeyEncodeInt(prefix, int64(topicID))
	key = encoding.EncodeVersion(key, 0)
	// Write a tombstone (nil value)
	return m.writeKV(common.KV{Key: key})
}

func (m *Manager) writeKV(kv common.KV) error {
	iter := common.NewKvSliceIterator([]common.KV{kv})
	// Build ssTable
	table, smallestKey, largestKey, minVersion, maxVersion, err := sst.BuildSSTable(m.dataFormat, 0, 0, iter)
	if err != nil {
		return err
	}
	tableID := sst.CreateSSTableId()
	// Push ssTable to object store
	tableData := table.Serialize()
	if err := m.putWithRetry(tableID, tableData); err != nil {
		return err
	}
	// Register table with LSM
	regEntry := lsm.RegistrationEntry{
		Level:            0,
		TableID:          []byte(tableID),
		MinVersion:       minVersion,
		MaxVersion:       maxVersion,
		KeyStart:         smallestKey,
		KeyEnd:           largestKey,
		DeleteRatio:      table.DeleteRatio(),
		AddedTime:        uint64(time.Now().UnixMilli()),
		NumEntries:       uint64(table.NumEntries()),
		TableSize:        uint64(table.SizeBytes()),
		NumPrefixDeletes: uint32(table.NumPrefixDeletes()),
	}
	batch := lsm.RegistrationBatch{
		Registrations: []lsm.RegistrationEntry{regEntry},
	}
	ch := make(chan error, 1)
	if err := m.lsm.ApplyLsmChanges(batch, func(err error) error {
		ch <- err
		return nil
	}); err != nil {
		return err
	}
	return <-ch
}

func (m *Manager) putWithRetry(key string, value []byte) error {
	for {
		err := objstore.PutWithTimeout(m.objStore, m.dataBucketName, key, value, objStoreCallTimeout)
		if err == nil {
			return nil
		}
		if m.stopping.Load() {
			return errors.New("TopicMetaPersister is stopping")
		}
		if common.IsUnavailableError(err) {
			log.Warnf("Unable to write type info due to unavailability, will retry after delay: %v", err)
			time.Sleep(unavailabilityRetryDelay)
		}
	}
}

func createPrefix() []byte {
	// Note, the prefix here is 16 bytes, the first 8 bytes of which is the common.TopicMetadataSlabID
	// All table prefixes must be 16 bytes to avoid collisions with partition hashes used for data
	prefix := make([]byte, 16)
	binary.BigEndian.PutUint64(prefix, common.TopicMetadataSlabID)
	return prefix
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
	var table sst.SSTable
	table.Deserialize(buff, 0)
	return &table, nil
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
	m.lock.Lock()
	defer m.lock.Unlock()
	m.membership = membership
}

func (m *Manager) sendNotificationToAddress(handlerID int, address string, notif []byte) error {
	conn, ok := m.connections[address]
	if !ok {
		var err error
		conn, err = m.connFactory(address)
		if err != nil {
			return err
		}
		// cache it
		m.connections[address] = conn
	}
	if _, err := conn.SendRPC(handlerID, notif); err != nil {
		if err2 := conn.Close(); err2 != nil {
			// Ignore
		}
		delete(m.connections, address)
		return err
	}
	return nil
}

func (m *Manager) SendTopicNotification(handlerID int, topicInfo TopicInfo) {
	if len(m.membership.Members) > 0 {
		notif := TopicNotification{
			Sequence: int(m.topicIDSequence),
			Info:     topicInfo,
		}
		bytes := notif.Serialize(nil)
		for i := 0; i < len(m.membership.Members); i++ {
			var memberData common.MembershipData
			memberData.Deserialize(m.membership.Members[i].Data, 0)
			if err := m.sendNotificationToAddress(handlerID, memberData.ClusterListenAddress, bytes); err != nil {
				// best effort - continue
				log.Warnf("Unable to send topic added notification: %v", err)
			}
		}
	}
}
