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
It also allows direct writing of KVs - these are also buffered and written in the same table as any produced data.
The write process involves:
* Requesting offsets for each topic partition that needs to be written - these come from the controller which
caches them in memory. In the same call (PrePush) epochs for any kvs to be committed are also passed in and
verified. The return of PrePush contains the offsets (if any) and whether each epoch was OK or not.
* If epochs were not valid, then any KVs for invalid epochs do not get written in the table.
* Building the record batches and committed offsets into an SSTable. The table contains one entry per record batch, and
one entry for each committed partition.
* Writing the SSTable to object storage
* Registering the SSTable metadata with the LSM. This involves inserting the SSTable metadata in level 0 of the LSM.
* Writing produce responses to all produce requests which were included in the written SSTable, and for any offset
commit responses.
*/
type TablePusher struct {
	lock                 sync.Mutex
	cfg                  Conf
	topicProvider        topicInfoProvider
	objStore             objstore.Client
	clientFactory        controllerClientFactory
	started              bool
	stopping             atomic.Bool
	controllerClient     ControlClient
	partitionRecords     map[int]map[int][]bufferedEntry
	directKVs            map[string][]common.KV
	directCompletions    map[string][]func(error)
	directWriterEpochs   map[string]int
	numDirectKVsToCommit int
	partitionHashes      *parthash.PartitionHashes
	writeTimer           *time.Timer
	sizeBytes            int
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
	PrePush(infos []offsets.GetOffsetTopicInfo, epochInfos []control.EpochInfo) ([]offsets.OffsetTopicInfo, int64,
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
		cfg:                cfg,
		topicProvider:      topicProvider,
		objStore:           objStore,
		clientFactory:      clientFactory,
		partitionHashes:    partitionHashes,
		partitionRecords:   map[int]map[int][]bufferedEntry{},
		directWriterEpochs: map[string]int{},
		directKVs:          map[string][]common.KV{},
		directCompletions:  map[string][]func(error){},
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
	for _, offsetCompletions := range t.directCompletions {
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

func (t *TablePusher) HandleDirectWrite(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	if err := checkRPCVersion(request); err != nil {
		return err
	}
	var req DirectWriteRequest
	req.Deserialize(request, 2)
	t.addDirectKVs(&req, func(err error) {
		if err := responseWriter(responseBuff, err); err != nil {
			log.Errorf("failed to write response: %v", err)
		}
	})
	return nil
}

func (t *TablePusher) addDirectKVs(req *DirectWriteRequest, completionFunc func(err error)) {
	lastEpoch, ok := t.directWriterEpochs[req.WriterKey]
	if ok && req.WriterEpoch != lastEpoch {
		msg := fmt.Sprintf("table pusher rejecting direct write from key %s as epoch is invalid", req.WriterKey)
		log.Warn(msg)
		completionFunc(common.NewTektiteErrorf(common.Unavailable, msg))
		return
	}
	if !ok {
		t.directWriterEpochs[req.WriterKey] = req.WriterEpoch
	}
	t.directKVs[req.WriterKey] = append(t.directKVs[req.WriterKey], req.KVs...)
	t.directCompletions[req.WriterKey] = append(t.directCompletions[req.WriterKey], completionFunc)
	t.numDirectKVsToCommit += len(req.KVs)
}

func checkRPCVersion(request []byte) error {
	rpcVersion := binary.BigEndian.Uint16(request)
	if rpcVersion != 1 {
		// Currently just 1
		return errors.New("invalid rpc version")
	}
	return nil
}

func (t *TablePusher) failDirectWrites(writerKey string) {
	log.Warnf("attempt to write data for invalid group epoch for writer key %s - will be ignored", writerKey)
	completions, ok := t.directCompletions[writerKey]
	if !ok {
		panic("not found direct completions")
	}
	kvs, ok := t.directKVs[writerKey]
	if !ok {
		panic("not found direct kvs")
	}
	for _, completion := range completions {
		err := common.NewTektiteErrorf(common.Unavailable, "unable to commit direct writes for key %s as epoch is invalid",
			writerKey)
		completion(err)
	}
	delete(t.directKVs, writerKey)
	delete(t.directCompletions, writerKey)
	t.numDirectKVsToCommit -= len(kvs)
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
	if len(t.partitionRecords) == 0 && t.numDirectKVsToCommit == 0 {
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
	groupEpochInfos := make([]control.EpochInfo, 0, len(t.directWriterEpochs))
	for groupID, epoch := range t.directWriterEpochs {
		groupEpochInfos = append(groupEpochInfos, control.EpochInfo{
			Key:   groupID,
			Epoch: epoch,
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
	if len(epochsOK) != len(t.directWriterEpochs) {
		panic("invalid epochs ok returned")
	}
	for i, ok := range epochsOK {
		if !ok {
			// If invalid epochs for any committed offsets are returned we fail the offset commits for that groupID
			// and they are not included in the table that is pushed
			groupID := groupEpochInfos[i].Key
			t.failDirectWrites(groupID)
		}
	}
	// Create KVs for the batches
	kvs := make([]common.KV, 0, len(offs)+t.numDirectKVsToCommit)
	// Add any offsets to commit
	for _, offsetKVs := range t.directKVs {
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
	if t.numDirectKVsToCommit > 0 {
		t.directKVs = map[string][]common.KV{}
		t.directCompletions = map[string][]func(error){}
		t.directWriterEpochs = make(map[string]int)
		t.numDirectKVsToCommit = 0
	}
	t.sizeBytes = 0
}

type DirectWriteRequest struct {
	WriterKey   string
	WriterEpoch int
	KVs         []common.KV
}

func (o *DirectWriteRequest) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(o.WriterKey)))
	buff = append(buff, o.WriterKey...)
	buff = binary.BigEndian.AppendUint64(buff, uint64(o.WriterEpoch))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(o.KVs)))
	for _, kv := range o.KVs {
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(kv.Key)))
		buff = append(buff, kv.Key...)
		buff = binary.BigEndian.AppendUint32(buff, uint32(len(kv.Value)))
		buff = append(buff, kv.Value...)
	}
	return buff
}

func (o *DirectWriteRequest) Deserialize(buff []byte, offset int) int {
	ln := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	o.WriterKey = string(buff[offset : offset+ln])
	offset += ln
	o.WriterEpoch = int(binary.BigEndian.Uint64(buff[offset:]))
	offset += 8
	ln = int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	o.KVs = make([]common.KV, ln)
	for i := 0; i < ln; i++ {
		lk := int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		key := buff[offset : offset+lk]
		offset += lk
		lv := int(binary.BigEndian.Uint32(buff[offset:]))
		offset += 4
		value := buff[offset : offset+lv]
		offset += lv
		o.KVs[i] = common.KV{
			Key:   key,
			Value: value,
		}
	}
	return offset
}
