package pusher

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"go.uber.org/zap/zapcore"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

/*
TablePusher handles produce requests and buffers batches internally. After a timeout, or if buffer gets full it starts
the process of writing batches to permanent storage.
It handles committing of offsets - these are also buffered and written in the same table as any produced data.
The write process involves:
* Requesting offsets for each topic partition that needs to be written - these come from the controller which
caches them in memory. In the same call (PrePush) group epochs for any offsets to be committed are also passed in and
verified. The return of PrePush contains the offsets (if any) and whether each group epoch was OK or not.
* If group epochs were not valid, then any committed offsets for invalid epochs do not get written in the table.
* Building the record batches and committed offsets into an SSTable. The table contains one entry per record batch, and
one entry for each committed partition.
* Writing the SSTable to object storage
* Registering the SSTable metadata with the LSM. This involves inserting the SSTable metadata in level 0 of the LSM.
* Writing produce responses to all produce requests which were included in the written SSTable, and for any offset
commit responses.
*/
type TablePusher struct {
	lock               sync.Mutex
	cfg                Conf
	topicProvider      topicInfoProvider
	objStore           objstore.Client
	clientFactory      controllerClientFactory
	started            bool
	stopping           atomic.Bool
	controllerClient   ControlClient
	partitionRecords   map[int]map[int][]bufferedEntry
	offsetKVs          map[string][]common.KV
	offsetCompletions  map[string][]func(error)
	groupEpochInfos    map[string]int
	numOffsetsToCommit int
	partitionHashes    *parthash.PartitionHashes
	writeTimer         *time.Timer
	sizeBytes          int
}

type bufferedEntry struct {
	records        [][]byte
	completionFunc func(error)
}

type topicInfoProvider interface {
	GetTopicInfo(topicName string) (topicmeta.TopicInfo, error)
}

type controllerClientFactory func() (ControlClient, error)

type ControlClient interface {
	PrePush(infos []offsets.GetOffsetTopicInfo, epochInfos []control.GroupEpochInfo) ([]offsets.OffsetTopicInfo, int64,
		[]bool, error)
	RegisterL0Table(sequence int64, regEntry lsm.RegistrationEntry) error
	Close() error
}

const (
	objStoreAvailabilityTimeout = 5 * time.Second
)

var log *logger.TektiteLogger

func init() {
	var err error
	log, err = logger.GetLoggerWithLevel("pusher", zapcore.InfoLevel)
	if err != nil {
		panic(err)
	}
}

func NewTablePusher(cfg Conf, topicProvider topicInfoProvider, objStore objstore.Client,
	clientFactory controllerClientFactory, partitionHashes *parthash.PartitionHashes) (*TablePusher, error) {
	return &TablePusher{
		cfg:               cfg,
		topicProvider:     topicProvider,
		objStore:          objStore,
		clientFactory:     clientFactory,
		partitionHashes:   partitionHashes,
		partitionRecords:  map[int]map[int][]bufferedEntry{},
		groupEpochInfos:   map[string]int{},
		offsetKVs:         map[string][]common.KV{},
		offsetCompletions: map[string][]func(error){},
	}, nil
}

func (t *TablePusher) Start() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.started {
		return nil
	}
	t.scheduleWriteTimer(t.cfg.WriteTimeout)
	t.started = true
	return nil
}

func (t *TablePusher) Stop() error {
	t.stopping.Store(true)
	t.lock.Lock()
	defer t.lock.Unlock()
	if !t.started {
		return nil
	}
	t.writeTimer.Stop()
	t.started = false
	return nil
}

func (t *TablePusher) scheduleWriteTimer(timeout time.Duration) {
	t.writeTimer = time.AfterFunc(timeout, func() {
		t.lock.Lock()
		defer t.lock.Unlock()
		if !t.started {
			return
		}
		log.Debug("scheduling write on timer")
		if err := t.write(); err != nil {
			// We close the client, so it will be recreated on any retry
			t.closeClient()
			if common.IsUnavailableError(err) {
				// Temporary unavailability of object store or controller - we will schedule the next timer fire to
				// be longer as typically write timeout is short and we don't want to spam the logs with lots of errors
				log.Warnf("table pusher unable to write due to temporary unavailability: %v", err)
				t.scheduleWriteTimer(t.cfg.AvailabilityRetryInterval)
				return
			}
			// Unexpected error
			log.Errorf("table pusher failed to write: %v", err)
			t.handleUnexpectedError(err)
			return
		}
		t.scheduleWriteTimer(t.cfg.WriteTimeout)
	})
}

func (t *TablePusher) closeClient() {
	if t.controllerClient != nil {
		if err := t.controllerClient.Close(); err != nil {
			// Ignore
		}
		t.controllerClient = nil
	}
}

func (t *TablePusher) callCompletions(err error) {
	for _, partitions := range t.partitionRecords {
		for _, entries := range partitions {
			for _, entry := range entries {
				entry.completionFunc(err)
			}
		}
	}
	for _, offsetCompletions := range t.offsetCompletions {
		for _, completionFunc := range offsetCompletions {
			completionFunc(err)
		}
	}
}

func setPartitionError(errorCode int, errorMsg string, partitionResponse *kafkaprotocol.ProduceResponsePartitionProduceResponse, cf *common.CountDownFuture) {
	partitionResponse.ErrorCode = int16(errorCode)
	partitionResponse.ErrorMessage = &errorMsg
	cf.CountDown(nil)
}

func (t *TablePusher) HandleProduceRequest(req *kafkaprotocol.ProduceRequest,
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
	t.lock.Lock()
	defer t.lock.Unlock()
	for i, topicData := range req.TopicData {
		resp.Responses[i].Name = topicData.Name
		partitionResponses := make([]kafkaprotocol.ProduceResponsePartitionProduceResponse, len(topicData.PartitionData))
		resp.Responses[i].PartitionResponses = partitionResponses
		topicInfo, err := t.topicProvider.GetTopicInfo(*topicData.Name)
		topicExists := true
		if err != nil {
			if !common.IsTektiteErrorWithCode(err, common.TopicDoesNotExist) {
				log.Warnf("failed to get topic info: %v", err)
			}
			topicExists = false
		}
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
			log.Debugf("handling records batch for topic: %s partition: %d", *topicData.Name, partitionID)
			t.handleRecords(topicInfo.ID, partitionID, partitionData.Records, func(err error) {
				if err != nil {
					log.Errorf("failed to handle records: %v", err)
					partitionResponses[j].ErrorCode = kafkaprotocol.ErrorCodeUnknownServerError
				}
				cf.CountDown(nil)
			})
		}
	}
	if t.sizeBytes >= t.cfg.BufferMaxSizeBytes {
		for {
			if t.stopping.Load() {
				// break out of loop
				return errors.New("table pusher is stopping")
			}
			err := t.write()
			if err == nil {
				return nil
			}
			// Close the client - it will be recreated on any retry
			t.closeClient()
			if !common.IsUnavailableError(err) {
				t.handleUnexpectedError(err)
				return err
			}
			// Temporary unavailability of object store or controller - we will retry after a delay
			log.Warnf("unavailability when attempting to write records: %v - will retry after delay", err)
			time.Sleep(t.cfg.WriteTimeout)
		}
	}
	return nil
}

func (t *TablePusher) HandleOffsetCommit(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	if err := checkRPCVersion(request); err != nil {
		return err
	}
	var req OffsetCommitRequest
	req.Deserialize(request, 2)
	return t.handleOffsetCommit0(req.Request, req.GroupEpoch, func(resp *kafkaprotocol.OffsetCommitResponse) {
		responseBuff = resp.Write(req.RequestVersion, responseBuff, nil)
		if err := responseWriter(responseBuff, nil); err != nil {
			log.Errorf("failed to write offset commit: %v", err)
		}
	})
}

func (t *TablePusher) handleOffsetCommit0(req *kafkaprotocol.OffsetCommitRequest, groupEpoch int,
	completionFunc func(resp *kafkaprotocol.OffsetCommitResponse)) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	var resp kafkaprotocol.OffsetCommitResponse
	resp.Topics = make([]kafkaprotocol.OffsetCommitResponseOffsetCommitResponseTopic, len(req.Topics))
	for i, topicData := range req.Topics {
		resp.Topics[i].Name = req.Topics[i].Name
		resp.Topics[i].Partitions = make([]kafkaprotocol.OffsetCommitResponseOffsetCommitResponsePartition, len(topicData.Partitions))
		for j, partData := range topicData.Partitions {
			resp.Topics[i].Partitions[j].PartitionIndex = partData.PartitionIndex
		}
	}
	groupID := *req.GroupId
	lastEpoch, ok := t.groupEpochInfos[groupID]
	if ok && groupEpoch != lastEpoch {
		log.Warnf("rejecting offset commit from group %s as epoch is invalid", groupID)
		fillInErrorCodesForOffsetCommitResponse(&resp, kafkaprotocol.ErrorCodeCoordinatorNotAvailable)
		completionFunc(&resp)
		return nil
	}
	if !ok {
		t.groupEpochInfos[groupID] = groupEpoch
	}
	partHash, err := parthash.CreateHash([]byte(groupID))
	if err != nil {
		panic(err) // won't happen
	}
	// Convert to KV pairs
	var kvs []common.KV
	for i, topicData := range req.Topics {
		foundTopic := false
		info, err := t.topicProvider.GetTopicInfo(*topicData.Name)
		if err != nil {
			log.Errorf("failed to find topic %s", *topicData.Name)
		} else {
			foundTopic = true
		}
		for j, partitionData := range topicData.Partitions {
			if !foundTopic {
				resp.Topics[i].Partitions[j].ErrorCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
				continue
			}
			offset := partitionData.CommittedOffset
			// key is [partition_hash, topic_id, partition_id] value is [offset]
			key := CreateOffsetKey(partHash, info.ID, int(partitionData.PartitionIndex))
			value := make([]byte, 8)
			binary.BigEndian.PutUint64(value, uint64(offset))
			kvs = append(kvs, common.KV{
				Key:   key,
				Value: value,
			})
			t.numOffsetsToCommit++
			log.Debugf("group %s topic %d partition %d committing offset %d", *req.GroupId, info.ID,
				partitionData.PartitionIndex, offset)
		}
	}
	t.offsetKVs[groupID] = append(t.offsetKVs[groupID], kvs...)
	t.offsetCompletions[groupID] = append(t.offsetCompletions[groupID], func(err error) {
		if err != nil {
			if common.IsUnavailableError(err) {
				log.Warnf("failed to handle offset commit: %v", err)
				fillInErrorCodesForOffsetCommitResponse(&resp, kafkaprotocol.ErrorCodeCoordinatorNotAvailable)
			} else {
				log.Errorf("failed to handle offset commit: %v", err)
				fillInErrorCodesForOffsetCommitResponse(&resp, kafkaprotocol.ErrorCodeUnknownServerError)
			}
		}
		completionFunc(&resp)
	})
	return nil
}

func checkRPCVersion(request []byte) error {
	rpcVersion := binary.BigEndian.Uint16(request)
	if rpcVersion != 1 {
		// Currently just 1
		return errors.New("invalid rpc version")
	}
	return nil
}

func fillInErrorCodesForOffsetCommitResponse(resp *kafkaprotocol.OffsetCommitResponse, errorCode int) {
	for i, topicData := range resp.Topics {
		for j := range topicData.Partitions {
			resp.Topics[i].Partitions[j].ErrorCode = int16(errorCode)
		}
	}
}

func (t *TablePusher) failOffsetCommits(groupID string) {
	log.Warnf("attempt to commit offsets for invalid group epoch for consumer group %s - will be ignored", groupID)
	completions, ok := t.offsetCompletions[groupID]
	if !ok {
		panic("not found offset completions")
	}
	kvs, ok := t.offsetKVs[groupID]
	if !ok {
		panic("not found offset kvs")
	}
	for _, completion := range completions {
		err := common.NewTektiteErrorf(common.Unavailable, "unable to commit offsets for group %s as epoch is invalid",
			groupID)
		completion(err)
	}
	delete(t.offsetKVs, groupID)
	delete(t.offsetCompletions, groupID)
	t.numOffsetsToCommit -= len(kvs)
}

func CreateOffsetKey(partHash []byte, topicID int, partitionID int) []byte {
	var key []byte
	key = append(key, partHash...)
	key = binary.BigEndian.AppendUint64(key, uint64(topicID))
	key = binary.BigEndian.AppendUint64(key, uint64(partitionID))
	return key
}

func (t *TablePusher) handleRecords(topicID int, partitionID int, records [][]byte, completionFunc func(error)) {
	topicMap, ok := t.partitionRecords[topicID]
	if !ok {
		topicMap = make(map[int][]bufferedEntry)
		t.partitionRecords[topicID] = topicMap
	}
	if len(records) > 1 {
		panic("too many records")
	}
	records = extractBatches(records[0])
	topicMap[partitionID] = append(topicMap[partitionID], bufferedEntry{
		records:        records,
		completionFunc: completionFunc,
	})
	for _, record := range records {
		t.sizeBytes += len(record)
	}
}

func extractBatches(buff []byte) [][]byte {
	// Multiple record batches are concatenated together
	var batches [][]byte
	for {
		batchLen := binary.BigEndian.Uint32(buff[8:])
		batch := buff[:int(batchLen)+12] // 12: First two fields are not included in size
		batches = append(batches, batch)
		if int(batchLen)+12 == len(buff) {
			break
		}
		buff = buff[int(batchLen)+12:]
	}
	return batches
}

func (t *TablePusher) handleUnexpectedError(err error) {
	// unexpected error - call all completions with error, and stop
	t.callCompletions(err)
	t.reset()
	t.started = false
	t.stopping.Store(true)
}

func (t *TablePusher) getClient() (ControlClient, error) {
	if t.controllerClient != nil {
		return t.controllerClient, nil
	}
	client, err := t.clientFactory()
	if err != nil {
		return nil, err
	}
	t.controllerClient = client
	return client, nil
}

func intCompare(i1, i2 int) int {
	if i1 < i2 {
		return -1
	} else if i1 == i2 {
		return 0
	} else {
		return 1
	}
}

func (t *TablePusher) write() error {
	if len(t.partitionRecords) == 0 && t.numOffsetsToCommit == 0 {
		// Nothing to do
		return nil
	}
	client, err := t.getClient()
	if err != nil {
		return err
	}
	// Prepare the offsets to request
	getOffSetInfos := make([]offsets.GetOffsetTopicInfo, 0, len(t.partitionRecords))
	for topicID, partitions := range t.partitionRecords {
		var offsetInfo offsets.GetOffsetTopicInfo
		offsetInfo.TopicID = topicID
		offsetInfo.PartitionInfos = make([]offsets.GetOffsetPartitionInfo, 0, len(partitions))
		for partitionID, entries := range partitions {
			totRecords := 0
			for _, entry := range entries {
				for _, batch := range entry.records {
					totRecords += kafkaencoding.NumRecords(batch)
				}
			}
			offsetInfo.PartitionInfos = append(offsetInfo.PartitionInfos, offsets.GetOffsetPartitionInfo{
				PartitionID: partitionID,
				NumOffsets:  totRecords,
			})
		}
		getOffSetInfos = append(getOffSetInfos, offsetInfo)
	}
	// The topics and partitions sent to PrePush must be ordered - this is because locks are applied for each
	// offset to be got, and ordering the locks prevents deadlock between multiple pushers requesting offsets from
	// same partitions.
	if len(getOffSetInfos) > 1 {
		slices.SortFunc(getOffSetInfos, func(a, b offsets.GetOffsetTopicInfo) int {
			return intCompare(a.TopicID, b.TopicID)
		})
		for _, topicInfo := range getOffSetInfos {
			if len(topicInfo.PartitionInfos) > 1 {
				slices.SortFunc(topicInfo.PartitionInfos, func(a, b offsets.GetOffsetPartitionInfo) int {
					return intCompare(a.PartitionID, b.PartitionID)
				})
			}
		}
	}
	// Prepare the epoch infos
	groupEpochInfos := make([]control.GroupEpochInfo, 0, len(t.groupEpochInfos))
	for groupID, epoch := range t.groupEpochInfos {
		groupEpochInfos = append(groupEpochInfos, control.GroupEpochInfo{
			GroupID:    groupID,
			GroupEpoch: epoch,
		})
	}
	// Now make the prePush call - this gets any offsets for topic data to be written and also provides epochs for
	// the consumer groups of any offsets being committed - this allows them to be verified by the controller
	// to prevent any zombie writes of offsets
	offs, seq, epochsOK, err := client.PrePush(getOffSetInfos, groupEpochInfos)
	if err != nil {
		return err
	}
	if len(offs) != len(getOffSetInfos) {
		panic("invalid offsets returned")
	}
	if len(epochsOK) != len(t.groupEpochInfos) {
		panic("invalid epochs ok returned")
	}
	for i, ok := range epochsOK {
		if !ok {
			// If invalid epochs for any committed offsets are returned we fail the offset commits for that groupID
			// and they are not included in the table that is pushed
			groupID := groupEpochInfos[i].GroupID
			t.failOffsetCommits(groupID)
		}
	}
	// Create KVs for the batches
	kvs := make([]common.KV, 0, len(offs)+t.numOffsetsToCommit)
	// Add any offsets to commit
	for _, offsetKVs := range t.offsetKVs {
		kvs = append(kvs, offsetKVs...)
	}
	for i, topOffset := range offs {
		partitionRecs := t.partitionRecords[topOffset.TopicID]
		for j, partInfo := range topOffset.PartitionInfos {
			partitionHash, err := t.partitionHashes.GetPartitionHash(topOffset.TopicID, partInfo.PartitionID)
			if err != nil {
				return err
			}
			// The returned offset is the last offset
			lastOffset := partInfo.Offset
			offset := lastOffset - int64(getOffSetInfos[i].PartitionInfos[j].NumOffsets) + 1
			batches := partitionRecs[partInfo.PartitionID]
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

					key := make([]byte, 0, 32)
					key = append(key, partitionHash...)
					key = encoding.KeyEncodeInt(key, offset)
					key = encoding.EncodeVersion(key, 0)
					// Fill in base offset
					binary.BigEndian.PutUint64(record, uint64(offset))
					kvs = append(kvs, common.KV{
						Key:   key,
						Value: record,
					})
					offset += int64(kafkaencoding.NumRecords(record))
				}
			}
		}
	}
	// Sort by key - ssTables are always in key order
	slices.SortFunc(kvs, func(a, b common.KV) int {
		return bytes.Compare(a.Key, b.Key)
	})
	iter := common.NewKvSliceIterator(kvs)
	// Build ssTable
	table, smallestKey, largestKey, minVersion, maxVersion, err := sst.BuildSSTable(t.cfg.DataFormat,
		int(1.1*float64(t.sizeBytes)), len(kvs), iter)
	if err != nil {
		return err
	}
	// Push ssTable to object store
	tableID := sst.CreateSSTableId()
	tableData := table.Serialize()
	if err := objstore.PutWithTimeout(t.objStore, t.cfg.DataBucketName, tableID, tableData,
		objStoreAvailabilityTimeout); err != nil {
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
	if err := client.RegisterL0Table(seq, regEntry); err != nil {
		return err
	}
	// Send back completions
	t.callCompletions(nil)
	// reset - the state
	t.reset()
	return nil
}

func (t *TablePusher) reset() {
	t.partitionRecords = make(map[int]map[int][]bufferedEntry)
	if t.numOffsetsToCommit > 0 {
		t.offsetKVs = map[string][]common.KV{}
		t.offsetCompletions = map[string][]func(error){}
		t.groupEpochInfos = make(map[string]int)
		t.numOffsetsToCommit = 0
	}
	t.sizeBytes = 0
}

type OffsetCommitRequest struct {
	GroupEpoch     int
	RequestVersion int16
	Request        *kafkaprotocol.OffsetCommitRequest
}

func (o *OffsetCommitRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(o.GroupEpoch))
	buff = binary.BigEndian.AppendUint16(buff, uint16(o.RequestVersion))
	return o.Request.Write(o.RequestVersion, buff, nil)
}

func (o *OffsetCommitRequest) Deserialize(buff []byte, offset int) int {
	o.GroupEpoch = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	o.RequestVersion = int16(binary.BigEndian.Uint16(buff[offset:]))
	offset += 2
	o.Request = &kafkaprotocol.OffsetCommitRequest{}
	br, err := o.Request.Read(o.RequestVersion, buff[offset:])
	if err != nil {
		panic(err)
	}
	offset += br
	return offset
}
