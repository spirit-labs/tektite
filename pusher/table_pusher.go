package pusher

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/acls"
	"github.com/spirit-labs/tektite/asl/encoding"
	auth "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/queryutils"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"math"
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
	tableGetter          sst.TableGetter
	leaderChecker        LeaderChecker
	partitionRecords     map[int]map[int][]bufferedRecords
	produceCompletions   []func(error)
	directKVs            map[string][]common.KV
	snapshotKVs          []common.KV
	timestampOffsetKVs   map[string][]byte
	directCompletions    map[string][]func(error)
	directWriterEpochs   map[string]int
	numDirectKVsToCommit int
	partitionHashes      *parthash.PartitionHashes
	writeTimer           *time.Timer
	snapshotTimer        *time.Timer
	sizeBytes            int
	producerSeqs         map[int]map[int]map[int]*sequenceInfo
	offsetTimes          map[int]map[int]*offsetTime
	stats                Stats
}

type bufferedRecords [][]byte

type topicInfoProvider interface {
	GetTopicInfo(topicName string) (topicmeta.TopicInfo, bool, error)
}

type controllerClientFactory func() (ControlClient, error)

type ControlClient interface {
	PrePush(infos []offsets.GenerateOffsetTopicInfo, epochInfos []control.EpochInfo) ([]offsets.OffsetTopicInfo, int64,
		[]bool, error)
	RegisterL0Table(sequence int64, regEntry lsm.RegistrationEntry) error
	QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error)
	Close() error
}

type LeaderChecker interface {
	IsLeader(topicID int, partitionID int) (bool, error)
}

type Stats struct {
	ProducedBatchCount int64
}

const (
	objStoreAvailabilityTimeout = 5 * time.Second
	offsetSnapshotFormatVersion = 1
	offsetTimeFormatVersion     = 1
)

func NewTablePusher(cfg Conf, topicProvider topicInfoProvider, objStore objstore.Client,
	clientFactory controllerClientFactory, tableGetter sst.TableGetter, partitionHashes *parthash.PartitionHashes,
	leaderChecker LeaderChecker) (*TablePusher, error) {
	return &TablePusher{
		cfg:                cfg,
		topicProvider:      topicProvider,
		objStore:           objStore,
		clientFactory:      clientFactory,
		tableGetter:        tableGetter,
		partitionHashes:    partitionHashes,
		leaderChecker:      leaderChecker,
		partitionRecords:   map[int]map[int][]bufferedRecords{},
		directWriterEpochs: map[string]int{},
		directKVs:          map[string][]common.KV{},
		directCompletions:  map[string][]func(error){},
		producerSeqs:       map[int]map[int]map[int]*sequenceInfo{},
		offsetTimes:        map[int]map[int]*offsetTime{},
		timestampOffsetKVs: map[string][]byte{},
	}, nil
}

func (t *TablePusher) Start() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.started {
		return nil
	}
	t.scheduleWriteTimer(t.cfg.WriteTimeout)
	t.scheduleSnapshotTimer(t.cfg.OffsetSnapshotInterval)
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
	t.snapshotTimer.Stop()
	t.started = false
	return nil
}

func (t *TablePusher) GetStats() Stats {
	return Stats{
		ProducedBatchCount: atomic.LoadInt64(&t.stats.ProducedBatchCount),
	}
}

func (t *TablePusher) scheduleWriteTimer(timeout time.Duration) {
	t.writeTimer = time.AfterFunc(timeout, func() {
		t.lock.Lock()
		defer t.lock.Unlock()
		if !t.started {
			return
		}
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
			t.handleError(err)
			return
		}
		t.scheduleWriteTimer(t.cfg.WriteTimeout)
	})
}

func (t *TablePusher) scheduleSnapshotTimer(timeout time.Duration) {
	t.snapshotTimer = time.AfterFunc(timeout, func() {
		t.lock.Lock()
		defer t.lock.Unlock()
		if !t.started {
			return
		}
		if err := t.maybeSnapshotSequences(); err != nil {
			log.Errorf("failed to snapshot sequences: %v", err)
		}
		if err := t.maybeSnapshotOffsetsByTime(); err != nil {
			log.Errorf("failed to snapshot offset times: %v", err)
		}
		t.scheduleSnapshotTimer(t.cfg.OffsetSnapshotInterval)
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
	for _, completionFunc := range t.produceCompletions {
		completionFunc(err)
	}
	if err == nil {
		atomic.AddInt64(&t.stats.ProducedBatchCount, int64(len(t.produceCompletions)))
	}
	for _, offsetCompletions := range t.directCompletions {
		for _, completionFunc := range offsetCompletions {
			completionFunc(err)
		}
	}
}

func setPartitionError(errorCode int, errorMsg string, partitionResponse *kafkaprotocol.ProduceResponsePartitionProduceResponse) {
	partitionResponse.ErrorCode = int16(errorCode)
	partitionResponse.ErrorMessage = &errorMsg
}

func (t *TablePusher) HandleProduceRequest(authContext *auth.Context, req *kafkaprotocol.ProduceRequest,
	completionFunc func(resp *kafkaprotocol.ProduceResponse) error) error {
	var resp kafkaprotocol.ProduceResponse
	resp.Responses = make([]kafkaprotocol.ProduceResponseTopicProduceResponse, len(req.TopicData))
	toComplete := 0
	for _, topicData := range req.TopicData {
		toComplete += len(topicData.PartitionData)
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	txAuthed := true
	if req.TransactionalId != nil && authContext != nil {
		transactionalID := common.SafeDerefStringPtr(req.TransactionalId)
		var err error
		txAuthed, err = authContext.Authorize(acls.ResourceTypeTransactionalID, transactionalID, acls.OperationWrite)
		if err != nil {
			fillAllErrors(&resp, kafkaprotocol.ErrorCodeUnknownServerError, err.Error())
			return completionFunc(&resp)
		}
	}
	recordsAdded := false
	for i, topicData := range req.TopicData {
		resp.Responses[i].Name = topicData.Name
		partitionResponses := make([]kafkaprotocol.ProduceResponsePartitionProduceResponse, len(topicData.PartitionData))
		resp.Responses[i].PartitionResponses = partitionResponses
		topicName := common.SafeDerefStringPtr(topicData.Name)
		topicInfo, topicExists, err := t.topicProvider.GetTopicInfo(topicName)
		errCode := kafkaprotocol.ErrorCodeNone
		var errMsg string
		if err == nil {
			if !topicExists {
				errCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
				errMsg = fmt.Sprintf("unknown topic: %s", topicName)
			} else if !txAuthed {
				errCode = kafkaprotocol.ErrorCodeTransactionalIDAuthorizationFailed
				errMsg = "not authorised to produce to transactional id"
			} else if authContext != nil {
				var authorised bool
				authorised, err = authContext.Authorize(acls.ResourceTypeTopic, topicName, acls.OperationWrite)
				if err == nil && !authorised {
					errCode = kafkaprotocol.ErrorCodeTopicAuthorizationFailed
					errMsg = fmt.Sprintf("not authorised to write to topic: %s", topicName)
				}
			}
		}
		if err != nil {
			if common.IsUnavailableError(err) {
				// Send back unknown topic as client will retry
				log.Warnf("failed to get topic info: %v", err)
				errCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
				errMsg = err.Error()
			} else {
				errCode = kafkaprotocol.ErrorCodeUnknownServerError
			}
		}
		var topicMap map[int][]bufferedRecords
	partitions:
		for j, partitionData := range topicData.PartitionData {
			partitionResponses[j].Index = partitionData.Index
			partitionID := int(partitionData.Index)
			if errCode != kafkaprotocol.ErrorCodeNone {
				setPartitionError(errCode, errMsg, &partitionResponses[j])
				continue partitions
			}
			if t.leaderChecker != nil && t.cfg.EnforceProduceOnLeader {
				leader, err := t.leaderChecker.IsLeader(topicInfo.ID, partitionID)
				if err != nil {
					return err
				}
				if !leader {
					// We check whether this agent is the "leader" for the topic, partition. We want produces for
					// same topic partitions to be handled at same agent in same AZ as that gives less load on the LSM
					// compactor as each agent handles a non overlapping range of keys, which allows compaction to be
					// parallelised. We return an error if this agent doesn't handle the partitions key range, this should
					// result in the client re-requesting metadata from an agent which should have the correct mapping.
					// If we didn't do this then clients could continue sending batches to the wrong agents until
					// cluster metadata timeout is hit, default is 5 minutes.
					setPartitionError(kafkaprotocol.ErrorCodeNotLeaderOrFollower,
						fmt.Sprintf("produce arrived at wrong agent for partition: %d for topic: %s", partitionID, *topicData.Name),
						&partitionResponses[j])
					continue partitions
				}
			}
			if partitionID < 0 || partitionID >= topicInfo.PartitionCount {
				setPartitionError(kafkaprotocol.ErrorCodeUnknownTopicOrPartition,
					fmt.Sprintf("unknown partition: %d for topic: %s", partitionID, *topicData.Name),
					&partitionResponses[j])
				continue partitions
			}
			for _, records := range partitionData.Records {
				magic := records[16]
				if magic != 2 {
					setPartitionError(kafkaprotocol.ErrorCodeUnsupportedForMessageFormat,
						"unsupported message format", &partitionResponses[j])
					continue partitions
				}
			}
			log.Debugf("handling records batch for topic: %s partition: %d", *topicData.Name, partitionID)
			if len(partitionData.Records) > 1 {
				panic("too many records")
			}
			recordBatches := extractBatches(partitionData.Records[0])
			for _, records := range recordBatches {
				dupRes, err := t.checkDuplicates(records, topicInfo.ID, partitionID)
				if err != nil {
					log.Errorf("failed to check duplicate records: %v", err)
					setPartitionError(kafkaprotocol.ErrorCodeUnknownServerError, err.Error(), &partitionResponses[j])
					continue partitions
				}
				if dupRes == 0 {
					// OK
					if topicMap == nil {
						var ok bool
						topicMap, ok = t.partitionRecords[topicInfo.ID]
						if !ok {
							topicMap = make(map[int][]bufferedRecords)
							t.partitionRecords[topicInfo.ID] = topicMap
						}
					}
					topicMap[partitionID] = append(topicMap[partitionID], [][]byte{records})
					t.sizeBytes += len(records)
					recordsAdded = true
				} else if dupRes == -1 {
					// duplicate
					setPartitionError(kafkaprotocol.ErrorCodeDuplicateSequenceNumber,
						fmt.Sprintf("duplicate records for topic %s partition %d", *topicData.Name, partitionID), &partitionResponses[j])
					continue partitions
				} else {
					// gap
					setPartitionError(kafkaprotocol.ErrorCodeOutOfOrderSequenceNumber,
						fmt.Sprintf("out of order records for topic %s partition %d", *topicData.Name, partitionID), &partitionResponses[j])
					continue partitions
				}
			}
		}
	}
	if recordsAdded {
		// Add a completion that will be called when all records have been written
		t.produceCompletions = append(t.produceCompletions, func(err error) {
			if err != nil {
				if common.IsUnavailableError(err) {
					log.Warnf("failed to handle records: %v", err)
					fillAllErrors(&resp, kafkaprotocol.ErrorCodeLeaderNotAvailable, err.Error())
				} else {
					log.Errorf("failed to handle records: %v", err)
					fillAllErrors(&resp, kafkaprotocol.ErrorCodeUnknownServerError, err.Error())
				}
			}
			if err := completionFunc(&resp); err != nil {
				log.Errorf("failed to send produce response: %v", err)
			}
		})
	} else {
		// All partitions errored - no records were added - send response now
		return completionFunc(&resp)
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
				t.handleError(err)
				return err
			}
			// Temporary unavailability of object store or controller - we will retry after a delay
			log.Warnf("unavailability when attempting to write records: %v - will retry after delay", err)
			time.Sleep(t.cfg.WriteTimeout)
		}
	}
	return nil
}

func fillAllErrors(resp *kafkaprotocol.ProduceResponse, errCode int, errMsg string) {
	for i := 0; i < len(resp.Responses); i++ {
		for j := 0; j < len(resp.Responses[i].PartitionResponses); j++ {
			resp.Responses[i].PartitionResponses[j].ErrorCode = int16(errCode)
			resp.Responses[i].PartitionResponses[j].ErrorMessage = &errMsg
		}
	}
}

func (t *TablePusher) HandleDirectProduceRequest(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	if err := checkRPCVersion(request); err != nil {
		return err
	}
	var req DirectProduceRequest
	req.Deserialize(request, 2)
	t.handleDirectProduce(&req, func(err error) {
		if err := responseWriter(responseBuff, err); err != nil {
			log.Errorf("failed to write response: %v", err)
		}
	})
	return nil
}

// handleDirectProduce is for producing topic data (which requires offsets to be generated) internally without
// having to go through a Kafka produce request. One use is for writing transaction markers.
func (t *TablePusher) handleDirectProduce(req *DirectProduceRequest, completionFunc func(error)) {
	t.lock.Lock()
	defer t.lock.Unlock()
	for _, topicReq := range req.TopicProduceRequests {
		topicMap, ok := t.partitionRecords[topicReq.TopicID]
		if !ok {
			topicMap = make(map[int][]bufferedRecords)
			t.partitionRecords[topicReq.TopicID] = topicMap
		}
		for _, partReq := range topicReq.PartitionProduceRequests {
			topicMap[partReq.PartitionID] = append(topicMap[partReq.PartitionID], [][]byte{partReq.Batch})
			t.sizeBytes += len(partReq.Batch)
		}
	}
	t.produceCompletions = append(t.produceCompletions, completionFunc)
}

func (t *TablePusher) HandleDirectWriteRequest(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if err := checkRPCVersion(request); err != nil {
		return err
	}
	var req common.DirectWriteRequest
	req.Deserialize(request, 2)
	if len(req.KVs) == 0 {
		return errors.Errorf("zero length direct kvs submitted")
	}
	t.addDirectKVs(&req, func(err error) {
		if err := responseWriter(responseBuff, err); err != nil {
			log.Errorf("failed to write response: %v", err)
		}
	})
	return nil
}

func (t *TablePusher) AddDirectKVs(req *common.DirectWriteRequest, completionFunc func(err error)) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.addDirectKVs(req, completionFunc)
}

func (t *TablePusher) addDirectKVs(req *common.DirectWriteRequest, completionFunc func(err error)) {
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
	// TODO we could maybe store all three of these in a single struct in a single map?
	delete(t.directKVs, writerKey)
	delete(t.directCompletions, writerKey)
	delete(t.directWriterEpochs, writerKey)
	t.numDirectKVsToCommit -= len(kvs)
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

func (t *TablePusher) handleError(err error) {
	t.callCompletions(err)
	t.reset()
	// PartitionOutOfRange can occur if partition count is reduced but produced records for old partitions still in transit
	// We don't want to stop inm that case
	if !common.IsTektiteErrorWithCode(err, common.PartitionOutOfRange) {
		// unexpected error - call all completions with error, and stop
		log.Errorf("got unexpected error in table pusher, will stop. %v", err)
		t.started = false
		t.stopping.Store(true)
	}
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

func (t *TablePusher) ForceWrite() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.write()
}

func (t *TablePusher) write() error {
	if len(t.partitionRecords) == 0 && t.numDirectKVsToCommit == 0 && len(t.timestampOffsetKVs) == 0 {
		// Nothing to do
		return nil
	}
	client, err := t.getClient()
	if err != nil {
		return err
	}
	// Prepare the offsets to request
	getOffSetInfos := make([]offsets.GenerateOffsetTopicInfo, 0, len(t.partitionRecords))
	for topicID, partitions := range t.partitionRecords {
		var offsetInfo offsets.GenerateOffsetTopicInfo
		offsetInfo.TopicID = topicID
		offsetInfo.PartitionInfos = make([]offsets.GenerateOffsetPartitionInfo, 0, len(partitions))
		for partitionID, entries := range partitions {
			totRecords := 0
			for _, entry := range entries {
				for _, batch := range entry {
					totRecords += kafkaencoding.NumRecords(batch)
				}
			}
			offsetInfo.PartitionInfos = append(offsetInfo.PartitionInfos, offsets.GenerateOffsetPartitionInfo{
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
		slices.SortFunc(getOffSetInfos, func(a, b offsets.GenerateOffsetTopicInfo) int {
			return intCompare(a.TopicID, b.TopicID)
		})
	}
	for _, topicInfo := range getOffSetInfos {
		if len(topicInfo.PartitionInfos) > 1 {
			slices.SortFunc(topicInfo.PartitionInfos, func(a, b offsets.GenerateOffsetPartitionInfo) int {
				return intCompare(a.PartitionID, b.PartitionID)
			})
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
	numRemainingKvs := len(offs) + t.numDirectKVsToCommit + len(t.timestampOffsetKVs)
	if numRemainingKvs == 0 {
		// After failing direct writes there may be nothing to do
		return nil
	}
	// Create KVs for the batches
	kvs := make([]common.KV, 0, numRemainingKvs)
	// Add any offsets to commit
	for _, offsetKVs := range t.directKVs {
		kvs = append(kvs, offsetKVs...)
	}
	// Add any offset snapshots
	kvs = append(kvs, t.snapshotKVs...)
	// Add any timestamp-offset index KVs
	for sKey, value := range t.timestampOffsetKVs {
		kvs = append(kvs, common.KV{
			Key:   common.StringToByteSliceZeroCopy(sKey),
			Value: value,
		})
	}
	// Prepare the data KVs
	for i, topOffset := range offs {
		partitionRecs := t.partitionRecords[topOffset.TopicID]
		for j, partInfo := range topOffset.PartitionInfos {
			partitionHash, err := t.partitionHashes.GetPartitionHash(topOffset.TopicID, partInfo.PartitionID)
			if err != nil {
				return err
			}
			log.Debugf("table pusher writing entry for topic %d partition %d", topOffset.TopicID, partInfo.PartitionID)
			// The returned offset is the last offset
			lastOffset := partInfo.Offset
			offset := lastOffset - int64(getOffSetInfos[i].PartitionInfos[j].NumOffsets) + 1
			batches := partitionRecs[partInfo.PartitionID]
			for _, entry := range batches {
				for _, records := range entry {
					/*
							For each batch there will be one entry in the database.
							The key is: [partition_hash, entry_type, offset, version]
							entry_type is byte representing the type of the entry - it's 0 for partition data, 1 for
						    sequence snapshot.
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
					key := make([]byte, 0, 33)
					key = append(key, partitionHash...)
					key = append(key, common.EntryTypeTopicData)
					key = encoding.KeyEncodeInt(key, offset)
					key = encoding.EncodeVersion(key, 0)
					// Fill in base offset
					binary.BigEndian.PutUint64(records, uint64(offset))
					// We encode topic id and partition id in the metdata at the end of the value
					value := common.AppendValueMetadata(records, int64(topOffset.TopicID), int64(partInfo.PartitionID))
					kvs = append(kvs, common.KV{
						Key:   key,
						Value: value,
					})
					offset += int64(kafkaencoding.NumRecords(records))
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
	t.updateOffsetTimes()
	// Send back completions
	t.callCompletions(nil)
	// reset - the state
	t.reset()
	return nil
}

func (t *TablePusher) updateOffsetTimes() {
	// Now update offset times
	// For each topic partition we maintain the latest offset and timestamp. This is then periodically stored to
	// permanent storage. It acts as index allowing us to lookup the offset for a particular timestamp. We can scan
	// from the last stored index value to get the latest value < required time.
	for topicID, partitions := range t.partitionRecords {
		partitionsMap, ok := t.offsetTimes[topicID]
		if !ok {
			partitionsMap = map[int]*offsetTime{}
			t.offsetTimes[topicID] = partitionsMap
		}
		for partitionID, entries := range partitions {
			ot, ok := partitionsMap[partitionID]
			if !ok {
				ot = &offsetTime{}
				partitionsMap[partitionID] = ot
			}
			for _, entry := range entries {
				for _, batch := range entry {
					baseTimestamp := kafkaencoding.BaseTimestamp(batch)
					baseOffset := kafkaencoding.BaseOffset(batch)
					ot.timestamp = baseTimestamp
					ot.offset = baseOffset
				}
			}
		}
	}
}

func (t *TablePusher) reset() {
	t.partitionRecords = make(map[int]map[int][]bufferedRecords)
	t.produceCompletions = t.produceCompletions[:0]
	if t.numDirectKVsToCommit > 0 {
		t.directKVs = map[string][]common.KV{}
		t.directCompletions = map[string][]func(error){}
		t.directWriterEpochs = make(map[string]int)
		t.numDirectKVsToCommit = 0
	}
	t.snapshotKVs = nil
	if len(t.timestampOffsetKVs) > 0 {
		t.timestampOffsetKVs = map[string][]byte{}
	}
	t.sizeBytes = 0
}

type sequenceInfo struct {
	expectedSequence int32
	offset           int64
	dirty            bool
}

type offsetTime struct {
	offset    int64
	timestamp int64
}

func (t *TablePusher) checkDuplicates(batch []byte, topicID int, partitionID int) (int, error) {
	producerID := int(kafkaencoding.ProducerID(batch))
	if producerID == -1 {
		// No producer id - not idempotent producer
		return 0, nil
	}
	baseOffset := kafkaencoding.BaseOffset(batch)
	baseSequence := kafkaencoding.BaseSequence(batch)
	lastOffsetDelta := kafkaencoding.LastOffsetDelta(batch)
	return t.checkOffset(producerID, topicID, partitionID, baseOffset, baseSequence, lastOffsetDelta)
}

// sequence is the last sequence in the batch
func (t *TablePusher) checkOffset(producerID int, topicID int, partitionID int, baseOffset int64, baseSequence int32, lastOffsetDelta int32) (int, error) {
	producerMap, ok := t.producerSeqs[producerID]
	if !ok {
		producerMap = map[int]map[int]*sequenceInfo{}
		t.producerSeqs[producerID] = producerMap
	}
	topicMap, ok := producerMap[topicID]
	if !ok {
		topicMap = map[int]*sequenceInfo{}
		producerMap[topicID] = topicMap
	}
	offInfo, ok := topicMap[partitionID]
	if !ok {
		// load the sequence from the database
		seq, err := t.loadExpectedSequence(producerID, topicID, partitionID)
		if err != nil {
			return 0, err
		}
		offInfo = &sequenceInfo{
			expectedSequence: seq,
		}
		topicMap[partitionID] = offInfo
	}
	if baseSequence == offInfo.expectedSequence {
		// OK
		newExpected := int64(offInfo.expectedSequence) + int64(lastOffsetDelta) + 1
		// wrap to zero
		offInfo.expectedSequence = int32(newExpected % (1 + math.MaxInt32))
		offInfo.offset = baseOffset
		offInfo.dirty = true
		return 0, nil
	} else if baseSequence < offInfo.expectedSequence {
		// duplicate
		log.Warnf("duplicate - producer %d got sequence %d was expecting %d", producerID, baseSequence, offInfo.expectedSequence)
		return -1, nil
	} else {
		// gap
		log.Warnf("gap - producer %d got sequence %d was expecting %d", producerID, baseSequence, offInfo.expectedSequence)
		return 1, nil
	}
}

func (t *TablePusher) loadExpectedSequence(producerID int, topicID int, partitionID int) (int32, error) {
	// First we lookup any snapshot
	key, err := t.createOffsetSnapshotKey(producerID, topicID, partitionID)
	if err != nil {
		return 0, err
	}
	val, err := t.getLatestValueWithKey(key)
	if err != nil {
		return 0, err
	}
	var offset int64
	if len(val) > 0 {
		version := binary.BigEndian.Uint16(val)
		if version != offsetSnapshotFormatVersion {
			return 0, errors.New("invalid offsetSnapshot format version")
		}
		offset = int64(binary.BigEndian.Uint64(val[2:]))
	}
	// Now we need to scan through and find latest sequence for the producer starting at the snapshotted offset
	partHash, err := t.partitionHashes.GetPartitionHash(topicID, partitionID)
	if err != nil {
		return 0, err
	}
	prefix := common.ByteSliceCopy(partHash)
	prefix = append(prefix, common.EntryTypeTopicData)
	prefix = encoding.KeyEncodeInt(prefix, offset)
	keyEnd := common.ByteSliceCopy(partHash)
	keyEnd = append(keyEnd, common.EntryTypeTopicData+1)
	controlClient, err := t.getClient()
	if err != nil {
		return 0, err
	}
	iter, err := queryutils.CreateIteratorForKeyRange(prefix, keyEnd, controlClient, t.tableGetter)
	if err != nil {
		return 0, err
	}
	if iter == nil {
		return 0, nil
	}
	defer iter.Close()
	var sequence int32
	for {
		ok, kv, err := iter.Next()
		if err != nil {
			return 0, err
		}
		if !ok {
			break
		}
		if bytes.Equal(prefix, kv.Key[:len(prefix)]) {
			recordProducerID := int(kafkaencoding.ProducerID(kv.Value))
			if producerID == recordProducerID {
				baseSequence := kafkaencoding.BaseSequence(kv.Value)
				lastOffsetDelta := kafkaencoding.LastOffsetDelta(kv.Value)
				seq := int64(baseSequence) + int64(lastOffsetDelta) + 1
				seq = seq % (math.MaxInt32 + 1) // wrap it
				sequence = int32(seq)
			}
		} else {
			break
		}
	}
	return sequence, nil
}

func (t *TablePusher) maybeSnapshotSequences() error {
	var kvs []common.KV
	for producerID, producerMap := range t.producerSeqs {
		for topicID, topicMap := range producerMap {
			for partitionID, seqInfo := range topicMap {
				if seqInfo.dirty {
					key, err := t.createOffsetSnapshotKey(producerID, topicID, partitionID)
					if err != nil {
						return err
					}
					value := make([]byte, 0, 10)
					value = binary.BigEndian.AppendUint16(value, uint16(offsetSnapshotFormatVersion))
					// We store the offset - this lets us index back into the actual data so we can
					// load latest sequence after the snapshot
					value = binary.BigEndian.AppendUint64(value, uint64(seqInfo.offset))
					value = common.AppendValueMetadata(value, int64(topicID), int64(partitionID))
					kvs = append(kvs, common.KV{
						Key:   key,
						Value: value,
					})
					seqInfo.dirty = false
				}
			}
		}
	}
	t.snapshotKVs = append(t.snapshotKVs, kvs...)
	return nil
}

func (t *TablePusher) createOffsetSnapshotKey(producerID int, topicID int, partitionID int) ([]byte, error) {
	partHash, err := t.partitionHashes.GetPartitionHash(topicID, partitionID)
	if err != nil {
		return nil, err
	}
	key := make([]byte, 0, 33)
	key = append(key, partHash...)
	key = append(key, common.EntryTypeOffsetSnapshot)
	key = binary.BigEndian.AppendUint64(key, uint64(producerID))
	key = encoding.EncodeVersion(key, 0)
	return key, nil
}

func (t *TablePusher) getLatestValueWithKey(key []byte) ([]byte, error) {
	keyEnd := common.IncBigEndianBytes(key)
	controlClient, err := t.getClient()
	if err != nil {
		return nil, err
	}
	queryRes, err := controlClient.QueryTablesInRange(key, keyEnd)
	if err != nil {
		return nil, err
	}
	if len(queryRes) == 0 {
		// no stored txinfo
		return nil, nil
	}
	// We take the first one as that's the most recent
	nonOverlapping := queryRes[0]
	res := nonOverlapping[0]
	tableID := res.ID
	sstTable, err := t.tableGetter(tableID)
	if err != nil {
		return nil, err
	}
	iter, err := sstTable.NewIterator(key, keyEnd)
	if err != nil {
		return nil, err
	}
	ok, kv, err := iter.Next()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	if len(kv.Value) == 0 {
		// tombstone
		return nil, nil
	}
	return kv.Value, nil
}

func (t *TablePusher) maybeSnapshotOffsetsByTime() error {
	if len(t.offsetTimes) == 0 {
		return nil
	}
	for topicID, partitionMap := range t.offsetTimes {
		for partitionID, ot := range partitionMap {
			key, err := t.createOffsetTimeKey(topicID, partitionID, ot.timestamp)
			if err != nil {
				return err
			}
			value := make([]byte, 0, 10)
			value = binary.BigEndian.AppendUint16(value, uint16(offsetTimeFormatVersion))
			value = binary.BigEndian.AppendUint64(value, uint64(ot.offset))
			value = common.AppendValueMetadata(value)
			// We add these to a map as there can be duplicates - we only want to keep the latest one
			t.timestampOffsetKVs[common.ByteSliceToStringZeroCopy(key)] = value
		}
	}
	t.offsetTimes = map[int]map[int]*offsetTime{}
	return nil
}

func (t *TablePusher) createOffsetTimeKey(topicID int, partitionID int, timestamp int64) ([]byte, error) {
	partHash, err := t.partitionHashes.GetPartitionHash(topicID, partitionID)
	if err != nil {
		return nil, err
	}
	key := make([]byte, 0, 33)
	key = append(key, partHash...)
	key = append(key, common.EntryTypeOffsetTime)
	// Note we store the timestamp such that lower timestamps have a greater key than greater timestamps
	// this means when we lookup an offset for a timestamp, and there is no index entry for that exact timestamp it
	// will return the index entry for the next smallest timestamp - we then iterate from there. otherwise we could
	// miss valid offsets for timestamps >= requested timestamp
	key = encoding.KeyEncodeInt(key, math.MaxInt64-timestamp)
	key = encoding.EncodeVersion(key, 0)
	return key, nil
}

// GetOffsetForTimestamp returns the first offset where the timestamp is >= provided timestamp, or -1 if no
// such record exists
func (t *TablePusher) GetOffsetForTimestamp(topicID int, partitionID int, timestamp int64) (int64, error) {
	// First we lookup any snapshot
	partHash, err := t.partitionHashes.GetPartitionHash(topicID, partitionID)
	if err != nil {
		return 0, err
	}
	key := make([]byte, 0, 25)
	key = append(key, partHash...)
	key = append(key, common.EntryTypeOffsetTime)
	key = encoding.KeyEncodeInt(key, math.MaxInt64-timestamp)
	keyEnd := make([]byte, 0, 25)
	keyEnd = append(keyEnd, partHash...)
	keyEnd = append(keyEnd, common.EntryTypeOffsetTime+1)
	controlClient, err := t.getClient()
	if err != nil {
		return 0, err
	}
	iter, err := queryutils.CreateIteratorForKeyRange(key, keyEnd, controlClient, t.tableGetter)
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	// this will return next lowest timestamp entry if it exists and there isn't an entry for exact timestamp match
	ok, kv, err := iter.Next()
	if err != nil {
		return 0, err
	}
	var savedOffset int64
	if ok {
		version := binary.BigEndian.Uint16(kv.Value)
		if version != offsetTimeFormatVersion {
			return 0, errors.New("invalid offsetTime format version")
		}
		savedOffset = int64(binary.BigEndian.Uint64(kv.Value[2:]))
	}
	// Now we need to scan through and find latest offset starting at the snapshotted offset
	prefix := common.ByteSliceCopy(partHash)
	prefix = append(prefix, common.EntryTypeTopicData)
	prefix = encoding.KeyEncodeInt(prefix, savedOffset)
	searchEnd := common.ByteSliceCopy(partHash)
	searchEnd = append(searchEnd, common.EntryTypeTopicData+1)
	iter, err = queryutils.CreateIteratorForKeyRange(prefix, searchEnd, controlClient, t.tableGetter)
	if err != nil {
		return 0, err
	}
	if iter == nil {
		return 0, nil
	}
	defer iter.Close()
	offset := int64(-1)
	for {
		ok, kv, err := iter.Next()
		if err != nil {
			return 0, err
		}
		if !ok {
			break
		}
		maxTimestamp := kafkaencoding.MaxTimestamp(kv.Value)
		if maxTimestamp >= timestamp {
			// Search for the first offset with timestamp >= requested timestamp in the batch
			offset = getOffsetForTimestampInRecords(kv.Value, timestamp)
			if offset == -1 {
				return 0, errors.Errorf("corrupt record batch - does not contain correct timestamps")
			}
			break
		}
	}
	return offset, nil
}

func getOffsetForTimestampInRecords(bytes []byte, timestamp int64) int64 {
	baseOffset := int64(binary.BigEndian.Uint64(bytes))
	baseTimeStamp := int64(binary.BigEndian.Uint64(bytes[27:]))
	off := 57
	numRecords := int(binary.BigEndian.Uint32(bytes[off:]))
	off += 4
	for i := 0; i < numRecords; i++ {
		recordLength, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		recordStart := off
		off++ // skip past attributes
		timestampDelta, bytesRead := binary.Varint(bytes[off:])
		recordTimestamp := baseTimeStamp + timestampDelta
		if recordTimestamp >= timestamp {
			offset := baseOffset + int64(i)
			return offset
		}
		off += bytesRead
		_, bytesRead = binary.Varint(bytes[off:])
		off += bytesRead
		keyLength, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		if keyLength != -1 {
			ikl := int(keyLength)
			off += ikl
		}
		valueLength, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		ivl := int(valueLength)
		off += ivl
		headersEnd := recordStart + int(recordLength)
		off = headersEnd
	}
	return -1
}
