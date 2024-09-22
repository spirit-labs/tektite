package pusher

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/sst"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

/*
TablePusher handles produce requests and buffers batches internally. After a timeout, or if buffer gets full it starts
the process of writing batches to permanent storage. This process involves:
* Requesting offsets for each topic partition that needs to be written - these come from the controller which
caches them in memory
* Building the record batches into an SSTable. The table contains one entry per record batch.
* Writing the SSTable to object storage
* Registering the SSTable metadata with the LSM. This involves inserting the SSTable metadata in level 0 of the LSM.
* Writing produce responses to all produce requests which were included in the written SSTable

# TODO

* Writing offsets for each partition into the SSTable
* Idempotent producers
* Producer timeout
* Improve locking?
*/
type TablePusher struct {
	lock             sync.Mutex
	cfg              Conf
	topicProvider    TopicInfoProvider
	objStore         objstore.Client
	clientFactory    ControllerClientFactory
	started          bool
	stopping         atomic.Bool
	controllerClient control.Client
	partitionRecords map[int]map[int][]bufferedEntry
	partitionHashes  *lru.Cache
	writeTimer       *time.Timer
	sizeBytes        int
}

type bufferedEntry struct {
	records        [][]byte
	completionFunc func(error)
}

type ControllerClientFactory func() (control.Client, error)

type TopicInfoProvider interface {
	GetTopicInfo(topicName string) (*TopicInfo, bool)
}

type TopicInfo struct {
	TopicID        int
	PartitionCount int
}

const (
	objStoreAvailabilityTimeout = 5 * time.Second
	partitionHashCacheMaxSize   = 100000
)

func NewTablePusher(cfg Conf, topicProvider TopicInfoProvider, objStore objstore.Client,
	clientFactory ControllerClientFactory) (*TablePusher, error) {
	partitionHashes, err := lru.New(partitionHashCacheMaxSize)
	if err != nil {
		return nil, err
	}
	return &TablePusher{
		cfg:              cfg,
		topicProvider:    topicProvider,
		objStore:         objStore,
		clientFactory:    clientFactory,
		partitionHashes:  partitionHashes,
		partitionRecords: map[int]map[int][]bufferedEntry{},
	}, nil
}

func (r *TablePusher) Start() error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.started {
		return nil
	}
	r.scheduleWriteTimer(r.cfg.WriteTimeout)
	r.started = true
	return nil
}

func (r *TablePusher) Stop() error {
	r.stopping.Store(true)
	r.lock.Lock()
	defer r.lock.Unlock()
	if !r.started {
		return nil
	}
	r.writeTimer.Stop()
	r.started = false
	return nil
}

func (r *TablePusher) scheduleWriteTimer(timeout time.Duration) {
	r.writeTimer = time.AfterFunc(timeout, func() {
		r.lock.Lock()
		defer r.lock.Unlock()
		if !r.started {
			return
		}
		if err := r.write(); err != nil {
			if common.IsUnavailableError(err) {
				// Temporary unavailability of object store or controller - we will schedule the next timer fire to
				// be longer as typically write timeout is short and we don't want to spam the logs with lots of errors
				log.Warnf("table pusher unable to write due to temporary unavailability: %v", err)
				r.scheduleWriteTimer(r.cfg.AvailabilityRetryInterval)
				return
			}
			// Unexpected error
			log.Errorf("table pusher failed to write: %v", err)
			r.handleUnexpectedError(err)
			return
		}
		r.scheduleWriteTimer(r.cfg.WriteTimeout)
	})
}

func (r *TablePusher) callCompletions(err error) {
	for _, partitions := range r.partitionRecords {
		for _, entries := range partitions {
			for _, entry := range entries {
				entry.completionFunc(err)
			}
		}
	}
}

func setPartitionError(errorCode int, errorMsg string, partitionResponse *kafkaprotocol.ProduceResponsePartitionProduceResponse, cf *common.CountDownFuture) {
	partitionResponse.ErrorCode = int16(errorCode)
	partitionResponse.ErrorMessage = &errorMsg
	cf.CountDown(nil)
}

func (r *TablePusher) HandleProduceRequest(req *kafkaprotocol.ProduceRequest,
	completionFunc func(resp *kafkaprotocol.ProduceResponse) error) error {
	var resp kafkaprotocol.ProduceResponse
	resp.Responses = make([]kafkaprotocol.ProduceResponseTopicProduceResponse, len(req.TopicData))
	toComplete := 0
	for _, topicData := range req.TopicData {
		toComplete += len(topicData.PartitionData)
	}
	// Note, the CountDownFuture provides a memory barrier so different goroutines can safely write into the ProduceResponse
	// (they never write into the same array indexes and the arrays are created before ingesting)
	cf := common.NewCountDownFuture(toComplete, func(_ error) {
		if err := completionFunc(&resp); err != nil {
			log.Errorf("failed to send produce response: %v", err)
		}
	})
	r.lock.Lock()
	defer r.lock.Unlock()
	for i, topicData := range req.TopicData {
		resp.Responses[i].Name = topicData.Name
		partitionResponses := make([]kafkaprotocol.ProduceResponsePartitionProduceResponse, len(topicData.PartitionData))
		resp.Responses[i].PartitionResponses = partitionResponses
		topicInfo, topicExists := r.topicProvider.GetTopicInfo(*topicData.Name)
	partitions:
		for j, partitionData := range topicData.PartitionData {
			partitionResponses[j].Index = partitionData.Index
			partitionID := int(partitionData.Index)
			if !topicExists {
				setPartitionError(kafkaprotocol.ErrorCodeUnknownTopicOrPartition,
					fmt.Sprintf("unknown topic: %s", *topicData.Name), &partitionResponses[j], cf)
				continue partitions
			}
			if partitionID < 0 || partitionID >= topicInfo.PartitionCount {
				setPartitionError(kafkaprotocol.ErrorCodeUnknownTopicOrPartition,
					fmt.Sprintf("unknown partition: %d for topic: %s", partitionID, *topicData.Name),
					&partitionResponses[j], cf)
				continue partitions
			}
			for _, records := range partitionData.Records {
				magic := records[16]
				if magic != 2 {
					setPartitionError(kafkaprotocol.ErrorCodeUnsupportedForMessageFormat,
						"unsupported message format", &partitionResponses[j], cf)
					continue partitions
				}
			}
			r.handleRecords(topicInfo.TopicID, partitionID, partitionData.Records, func(err error) {
				if err != nil {
					log.Errorf("failed to handle records: %v", err)
					partitionResponses[j].ErrorCode = kafkaprotocol.ErrorCodeUnknownServerError
				}
				cf.CountDown(nil)
			})
		}
	}
	if r.sizeBytes >= r.cfg.BufferMaxSizeBytes {
		for {
			if r.stopping.Load() {
				// break out of loop
				return (errors.New("table pusher is stopping"))
			}
			err := r.write()
			if err == nil {
				return nil
			}
			if !common.IsUnavailableError(err) {
				r.handleUnexpectedError(err)
				return err
			}
			// Temporary unavailability of object store or controller - we will retry after a delay
			log.Warnf("unavailability when attempting to write records: %v - will retry after delay", err)
			time.Sleep(r.cfg.WriteTimeout)
		}
	}
	return nil
}

func (r *TablePusher) handleRecords(topicID int, partitionID int, records [][]byte, completionFunc func(error)) {
	topicMap, ok := r.partitionRecords[topicID]
	if !ok {
		topicMap = make(map[int][]bufferedEntry)
		r.partitionRecords[topicID] = topicMap
	}
	topicMap[partitionID] = append(topicMap[partitionID], bufferedEntry{
		records:        records,
		completionFunc: completionFunc,
	})
	for _, record := range records {
		r.sizeBytes += len(record)
	}
}

func (r *TablePusher) handleUnexpectedError(err error) {
	// unexpected error - call all completions with error, and stop
	r.callCompletions(err)
	r.started = false
	r.stopping.Store(true)
}

func (r *TablePusher) getClient() (control.Client, error) {
	if r.controllerClient != nil {
		return r.controllerClient, nil
	}
	client, err := r.clientFactory()
	if err != nil {
		return nil, err
	}
	r.controllerClient = client
	return client, nil
}

func numRecords(records []byte) int {
	return int(binary.BigEndian.Uint32(records[57:]))
}

func (r *TablePusher) write() error {
	if len(r.partitionRecords) == 0 {
		// Nothing to do
		return nil
	}
	client, err := r.getClient()
	if err != nil {
		return err
	}
	// First, we request offsets for the batches
	var getOffSetInfos []control.GetOffsetInfo
	var partitionBatches [][]bufferedEntry
	for topicID, partitions := range r.partitionRecords {
		for partitionID, entries := range partitions {
			totRecords := 0
			for _, entry := range entries {
				for _, batch := range entry.records {
					totRecords += numRecords(batch)
				}
			}
			getOffSetInfos = append(getOffSetInfos, control.GetOffsetInfo{
				TopicID:     topicID,
				PartitionID: partitionID,
				NumOffsets:  totRecords,
			})
			partitionBatches = append(partitionBatches, entries)
		}
	}
	offsets, err := client.GetOffsets(getOffSetInfos)
	if err != nil {
		return err
	}
	// Create KVs for the batches
	kvs := make([]common.KV, 0, len(offsets))
	for i, getOffsetInfo := range getOffSetInfos {
		batches := partitionBatches[i]
		offset := offsets[i]
		for _, entry := range batches {
			for _, record := range entry.records {
				/*
					For each batch there will be one entry in the database.
					The key is: [partition_hash, offset, version]
					The value is: the record batch bytes

					The partition hash is created by sha256 hashing the [topic_id, partition_id] and taking the first 16 bytes.
					This creates an effectively unique key as the probability of collision is extraordinarily remote
					(secure random UUIDs are created the same way)
					We use the partition hash instead of the [topic_id, partition_id] as the key prefix for the data in the LSM
					as partition hashes will be evenly distributed across all possible values. This enables us to direct produce
					traffic to specific agents in an AZ such that a specific agent handles produces for partitions that whose
					partition hashes lie in a certain range. Each agent gets a non-overlapping range.
					This means that when SSTables are registered in L0 of the LSM they will have non overlapping key ranges depending
					on which agent they came from. The LSM can then parallelize compaction of non overlapping groups of tables
					thus allowing LSM compaction to scale with number of agents.
				*/
				partitionHash, err := r.getPartitionHash(getOffsetInfo.TopicID, getOffsetInfo.PartitionID)
				if err != nil {
					return err
				}
				key := make([]byte, 0, 32)
				key = append(key, partitionHash...)
				key = encoding.KeyEncodeInt(key, offset)
				key = encoding.EncodeVersion(key, 0)
				kvs = append(kvs, common.KV{
					Key:   key,
					Value: record,
				})
				offset += int64(numRecords(record))
			}
		}
	}
	// Sort by key - sstables are always in key order
	slices.SortFunc(kvs, func(a, b common.KV) int {
		return bytes.Compare(a.Key, b.Key)
	})
	iter := common.NewKvSliceIterator(kvs)
	// Build sstable
	table, smallestKey, largestKey, minVersion, maxVersion, err := sst.BuildSSTable(r.cfg.DataFormat,
		int(1.1*float64(r.sizeBytes)), len(kvs), iter)
	if err != nil {
		return err
	}
	tableID := fmt.Sprintf("sst-%s", uuid.New().String())
	// Push sstable to object store
	tableData := table.Serialize()
	if err := objstore.PutWithTimeout(r.objStore, r.cfg.DataBucketName, tableID, tableData,
		objStoreAvailabilityTimeout); err != nil {
		return err
	}

	// Register table with LSM
	regBatch := lsm.RegistrationBatch{
		Registrations: []lsm.RegistrationEntry{
			{
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
			},
		},
	}
	if err := client.ApplyLsmChanges(regBatch); err != nil {
		return err
	}
	// Send back completions
	r.callCompletions(nil)
	// reset
	r.partitionRecords = make(map[int]map[int][]bufferedEntry)
	r.sizeBytes = 0
	return nil
}

func (r *TablePusher) getPartitionHash(topicID int, partitionID int) ([]byte, error) {
	// We cache partition hashes in an LRU as crypto hashes like sha-256 are usually quite slow
	kb := make([]byte, 16)
	binary.BigEndian.PutUint64(kb, uint64(topicID))
	binary.BigEndian.PutUint64(kb[8:], uint64(partitionID))
	key := common.ByteSliceToStringZeroCopy(kb)
	h, ok := r.partitionHashes.Get(key)
	if ok {
		return h.([]byte), nil
	}
	hashFunc := sha256.New()
	if _, err := hashFunc.Write(kb); err != nil {
		return nil, err
	}
	out := hashFunc.Sum(nil)
	// we take the first 128 bits
	p := out[:16]
	r.partitionHashes.Add(key, p)
	return p, nil
}
