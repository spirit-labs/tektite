package tx

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/pusher"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"github.com/spirit-labs/tektite/types"
	"math"
	"sync"
	"time"
)

type Coordinator struct {
	lock               sync.RWMutex
	controlClientCache *control.ClientCache
	tableGetter        sst.TableGetter
	membership         cluster.MembershipState
	connCaches         *transport.ConnCaches
	topicProvider      topicInfoProvider
	partHashes         *parthash.PartitionHashes
	txInfos            map[int64]*txInfo
}

type topicInfoProvider interface {
	GetTopicInfo(topicName string) (topicmeta.TopicInfo, bool, error)
}

func NewCoordinator(controlClientCache *control.ClientCache, tableGetter sst.TableGetter,
	connCaches *transport.ConnCaches, topicProvider topicInfoProvider,
	partHashes *parthash.PartitionHashes) *Coordinator {
	return &Coordinator{
		controlClientCache: controlClientCache,
		tableGetter:        tableGetter,
		topicProvider:      topicProvider,
		partHashes:         partHashes,
		txInfos:            make(map[int64]*txInfo),
		connCaches:         connCaches,
	}
}

const (
	producerIDSequenceName     = "pid"
	transactionMetadataVersion = uint16(1)
)

func (c *Coordinator) Start() error {
	return nil
}

func (c *Coordinator) Stop() error {
	return nil
}

func (c *Coordinator) MembershipChanged(_ int32, memberState cluster.MembershipState) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.membership = memberState
	return nil
}

func (c *Coordinator) HandleInitProducerID(req *kafkaprotocol.InitProducerIdRequest) *kafkaprotocol.InitProducerIdResponse {
	resp := &kafkaprotocol.InitProducerIdResponse{}
	err := c.handleInitProducerID(req, resp)
	resp.ErrorCode = kafkaencoding.ErrorCodeForError(err, kafkaprotocol.ErrorCodeCoordinatorNotAvailable)
	return resp
}

func (c *Coordinator) HandleAddPartitionsToTxn(req *kafkaprotocol.AddPartitionsToTxnRequest) *kafkaprotocol.AddPartitionsToTxnResponse {
	resp := &kafkaprotocol.AddPartitionsToTxnResponse{}
	err := c.handleAddPartitionsToTxn(req, resp)
	resp.ErrorCode = kafkaencoding.ErrorCodeForError(err, kafkaprotocol.ErrorCodeCoordinatorNotAvailable)
	return resp
}

func (c *Coordinator) handleAddPartitionsToTxn(req *kafkaprotocol.AddPartitionsToTxnRequest, resp *kafkaprotocol.AddPartitionsToTxnResponse) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	info, err := c.getTxInfo(req.V3AndBelowProducerId, req.V3AndBelowProducerEpoch)
	if err != nil {
		return err
	}
	// FIXME - in case of error of later adds need to make sure previous adds weren't added!
	for _, topic := range req.V3AndBelowTopics {
		topicName := common.SafeDerefStringPtr(topic.Name)
		topicInfo, exists, err := c.topicProvider.GetTopicInfo(topicName)
		if err != nil {
			if common.IsUnavailableError(err) {
				// Send back topic does not exist so client retries
				log.Warnf("failed to find topic %s: %v", topicName, err)
				return &kafkaencoding.KafkaError{
					ErrorCode: kafkaprotocol.ErrorCodeUnknownTopicOrPartition,
					ErrorMsg:  fmt.Sprintf("unknown topic: %s", topicName),
				}
			} else {
				log.Errorf("failed to find topic %s: %v", topicName, err)
				return &kafkaencoding.KafkaError{
					ErrorCode: kafkaprotocol.ErrorCodeUnknownServerError,
				}
			}
		}
		if !exists {
			return &kafkaencoding.KafkaError{
				ErrorCode: kafkaprotocol.ErrorCodeUnknownTopicOrPartition,
				ErrorMsg:  fmt.Sprintf("unknown topic: %s", topicName),
			}
		}
		topicID := int64(topicInfo.ID)
		for _, partitionID := range topic.Partitions {
			if err := info.addPartition(topicID, partitionID); err != nil {
				return err
			}
		}
	}
	return info.store()
}

func (c *Coordinator) HandleAddOffsetsToTxn(req *kafkaprotocol.AddOffsetsToTxnRequest) *kafkaprotocol.AddOffsetsToTxnResponse {
	resp := &kafkaprotocol.AddOffsetsToTxnResponse{}
	err := c.handleAddOffsetsToTxn(req, resp)
	resp.ErrorCode = kafkaencoding.ErrorCodeForError(err, kafkaprotocol.ErrorCodeNone)
	return resp
}

func (c *Coordinator) handleAddOffsetsToTxn(req *kafkaprotocol.AddOffsetsToTxnRequest, _ *kafkaprotocol.AddOffsetsToTxnResponse) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	info, err := c.getTxInfo(req.ProducerId, req.ProducerEpoch)
	if err != nil {
		return err
	}
	groupID := common.SafeDerefStringPtr(req.GroupId)
	if err := info.addConsumerGroup(groupID); err != nil {
		return err
	}
	return info.store()
}

func (c *Coordinator) getTxInfo(producerID int64, producerEpoch int16) (*txInfo, error) {
	info, ok := c.txInfos[producerID]
	if !ok {
		return nil, &kafkaencoding.KafkaError{
			ErrorCode: kafkaprotocol.ErrorCodeInvalidProducerIDMapping,
			ErrorMsg:  "unknown producer ID",
		}
	}
	if info.storedState.producerEpoch != producerEpoch {
		return nil, &kafkaencoding.KafkaError{
			ErrorCode: kafkaprotocol.ErrorCodeInvalidProducerEpoch,
			ErrorMsg:  "invalid producer epoch",
		}
	}
	return info, nil
}

func (c *Coordinator) HandleEndTxn(req *kafkaprotocol.EndTxnRequest) *kafkaprotocol.EndTxnResponse {
	c.lock.RLock()
	defer c.lock.RUnlock()
	resp := &kafkaprotocol.EndTxnResponse{}
	err := c.handleEndTxn(req, resp)
	errCode := kafkaencoding.ErrorCodeForError(err, kafkaprotocol.ErrorCodeCoordinatorNotAvailable)
	resp.ErrorCode = errCode
	return resp
}

func (c *Coordinator) handleEndTxn(req *kafkaprotocol.EndTxnRequest, _ *kafkaprotocol.EndTxnResponse) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	info, err := c.getTxInfo(req.ProducerId, req.ProducerEpoch)
	if err != nil {
		return err
	}
	return info.endTx(req.Committed)
}

func (c *Coordinator) handleInitProducerID(req *kafkaprotocol.InitProducerIdRequest, resp *kafkaprotocol.InitProducerIdResponse) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if req.ProducerId == -1 && req.ProducerEpoch != -1 {
		// Compatibility with Kafka broker
		return &kafkaencoding.KafkaError{
			ErrorCode: kafkaprotocol.ErrorCodeInvalidRequest,
			ErrorMsg:  "producerId == -1 but epoch is specified",
		}
	}
	if req.TransactionalId == nil {
		// The producer is idempotent but not configured for transactions - just generate a pid and return it
		pid, err := c.generatePid()
		if err != nil {
			return err
		}
		resp.ProducerId = pid
		resp.ProducerEpoch = 0
		return nil
	}
	transactionalID := common.SafeDerefStringPtr(req.TransactionalId)
	if transactionalID == "" {
		// Compatibility with Kafka broker
		return &kafkaencoding.KafkaError{
			ErrorCode: kafkaprotocol.ErrorCodeInvalidRequest,
			ErrorMsg:  "transactionalID not specified",
		}
	}

	// Load any existing record
	key := "t." + transactionalID
	storedState, err := c.loadTxInfo(transactionalID)
	if err != nil {
		return err
	}

	// TODO verify passed in producer id and epoch. logic is complex here!!

	if storedState == nil {
		// First time transactionalID was used or no transactional id - generate a pid
		pid, err := c.generatePid()
		if err != nil {
			return err
		}
		storedState = &txStoredState{
			pid:           pid,
			producerEpoch: 0,
			partitions:    map[int64][]int32{},
		}
	} else {

		// TODO resolve any prepared but not committed/aborted transaction state

		// bump the kafka epoch
		if storedState.producerEpoch == math.MaxInt16 {
			storedState.producerEpoch = 0
		} else {
			storedState.producerEpoch++
		}
	}
	resp.ProducerId = storedState.pid
	resp.ProducerEpoch = storedState.producerEpoch
	// load the tektite epoch
	cl, err := c.controlClientCache.GetClient()
	if err != nil {
		return err
	}
	_, _, tektiteEpoch, err := cl.GetCoordinatorInfo(key)
	if err != nil {
		return err
	}
	partHash, err := parthash.CreateHash([]byte(key))
	if err != nil {
		return err
	}
	info := txInfo{
		c:            c,
		partHash:     partHash,
		key:          key,
		storedState:  *storedState,
		tektiteEpoch: int64(tektiteEpoch),
	}
	// Store the tx state
	if err := info.store(); err != nil {
		return err
	}
	c.addTxInfo(&info)
	return nil
}

func (c *Coordinator) addTxInfo(info *txInfo) {
	c.lock.RUnlock()
	c.lock.Lock()
	defer func() {
		c.lock.Unlock()
		c.lock.RLock()
	}()
	c.txInfos[info.storedState.pid] = info
}

func createRequestBuffer() []byte {
	buff := make([]byte, 0, 128)                  // Initial size guess
	buff = binary.BigEndian.AppendUint16(buff, 1) // rpc version - currently 1
	return buff
}

func (c *Coordinator) generatePid() (int64, error) {
	cl, err := c.controlClientCache.GetClient()
	if err != nil {
		return 0, err
	}
	pid, err := cl.GenerateSequence(producerIDSequenceName)
	if err != nil {
		return 0, err
	}
	return pid, nil
}

func (c *Coordinator) loadTxInfo(transactionalID string) (*txStoredState, error) {
	key := "t." + transactionalID
	keyStart, err := parthash.CreateHash([]byte(key))
	if err != nil {
		return nil, err
	}
	val, err := c.getLatestValueWithKey(keyStart)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, nil
	}
	version := binary.BigEndian.Uint16(val)
	if version != transactionMetadataVersion {
		return nil, errors.New("invalid transactional metadata version")
	}
	info := &txStoredState{}
	info.Deserialize(val, 2)
	return info, nil
}

// TODO combine with other similar methods (e.g. in table pusher, but do a search for all usage of QueryTablesInRange)
func (c *Coordinator) getLatestValueWithKey(key []byte) ([]byte, error) {
	keyEnd := common.IncBigEndianBytes(key)
	cl, err := c.controlClientCache.GetClient()
	if err != nil {
		return nil, err
	}
	queryRes, err := cl.QueryTablesInRange(key, keyEnd)
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
	sstTable, err := c.tableGetter(tableID)
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

type txStatus int

const (
	txStatusNotStarted     = txStatus(0)
	txStatusBegin          = txStatus(1)
	txStatusPrepareCommit  = txStatus(2)
	txStatusPrepareAbort   = txStatus(3)
	txStatusCompleteCommit = txStatus(4)
	txStatusCompleteAbort  = txStatus(5)
)

type txInfo struct {
	lock         sync.Mutex
	c            *Coordinator
	partHash     []byte
	key          string
	storedState  txStoredState
	tektiteEpoch int64
}

func (t *txInfo) addPartition(topicID int64, partitionID int32) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if err := t.checkState(); err != nil {
		return err
	}
	// check if partition already added
	parts := t.storedState.partitions[topicID]
	for _, partID := range parts {
		if partID == partitionID {
			return &kafkaencoding.KafkaError{
				ErrorCode: kafkaprotocol.ErrorCodeInvalidTxnState,
				ErrorMsg:  "topic partition is already added to transaction",
			}
		}
	}
	t.storedState.partitions[topicID] = append(parts, partitionID)
	return nil
}

func (t *txInfo) addConsumerGroup(groupID string) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if err := t.checkState(); err != nil {
		return err
	}
	for _, group := range t.storedState.consumerGroups {
		if group == groupID {
			return &kafkaencoding.KafkaError{
				ErrorCode: kafkaprotocol.ErrorCodeInvalidTxnState,
				ErrorMsg:  fmt.Sprintf("consumer group %s is already added to transaction", groupID),
			}
		}
	}
	t.storedState.consumerGroups = append(t.storedState.consumerGroups, groupID)
	return nil
}

func (t *txInfo) checkState() error {
	if t.storedState.status == txStatusNotStarted {
		t.storedState.status = txStatusBegin
	}
	if t.storedState.status != txStatusBegin {
		return &kafkaencoding.KafkaError{
			ErrorCode: kafkaprotocol.ErrorCodeInvalidTxnState,
			ErrorMsg:  fmt.Sprintf("cannot add partition to transaction in state %d", t.storedState.status),
		}
	}
	return nil
}

func (t *txInfo) endTx(commit bool) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.storedState.status != txStatusBegin {
		return &kafkaencoding.KafkaError{
			ErrorCode: kafkaprotocol.ErrorCodeInvalidTxnState,
			ErrorMsg:  fmt.Sprintf("cannot end transaction as not in begin state %d", t.storedState.status),
		}
	}
	// copy so we don't change status on error
	infoCopy := t
	if commit {
		infoCopy.storedState.status = txStatusPrepareCommit
	} else {
		infoCopy.storedState.status = txStatusPrepareAbort
	}
	// Write the prepare
	if err := infoCopy.store(); err != nil {
		return err
	}
	// Write transaction markers
	if err := t.sendTransactionMarkers(); err != nil {
		return err
	}
	// Write committed offsets
	// TODO
	//t.c.offsetCommitter.CompleteTx()

	// If we get here then the tx is complete
	infoCopy.storedState.partitions = map[int64][]int32{}
	infoCopy.storedState.consumerGroups = nil
	if commit {
		infoCopy.storedState.status = txStatusCompleteCommit
	} else {
		infoCopy.storedState.status = txStatusCompleteAbort
	}
	if err := infoCopy.store(); err != nil {
		return err
	}
	t.storedState = infoCopy.storedState
	return nil
}

func (t *txInfo) storeCommittedOffsets() error {
	cl, err := t.c.controlClientCache.GetClient()
	if err != nil {
		return err
	}
	for _, group := range t.storedState.consumerGroups {
		key := "g." + group
		// TODO cache this?
		_, address, _, err := cl.GetCoordinatorInfo(key)
		conn, err := t.c.connCaches.GetConnection(address)
		if err != nil {
			return err
		}
		// TODO send an RPC to the group coordinator passing group id, producer if and producer epoch
		// this will tell it to change committed offsets to live offsets for the producer id and group
		// it will also validate producer id and producer epoch.
		_, err = conn.SendRPC(0, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *txInfo) sendTransactionMarkers() error {
	timestamp := types.Timestamp{Val: time.Now().UnixMilli()}
	pusherBatches := map[string]map[int64]map[int32][]byte{}
	for topicID, topicParts := range t.storedState.partitions {
		for _, partitionID := range topicParts {
			batchBytes := make([]byte, 61)
			batchBytes, ok := kafkaencoding.AppendToBatch(batchBytes, 0, nil, nil, nil, timestamp,
				timestamp, math.MaxInt, true)
			if !ok {
				panic("could not append to batch")
			}
			kafkaencoding.SetBatchHeader(batchBytes, 0, 0, timestamp, timestamp, 1)
			batchBytes[21] = batchBytes[21] | (1 << 5) // set as controlBatch
			partitionHash, err := t.c.partHashes.GetPartitionHash(int(topicID), int(partitionID))
			if err != nil {
				return err
			}
			pusherAddress, ok := cluster.ChooseMemberAddressForHash(partitionHash, t.c.membership.Members)
			if !ok {
				// No available pushers
				log.Warnf("cannot commit transaction as no members in cluster")
				return &kafkaencoding.KafkaError{
					ErrorCode: kafkaprotocol.ErrorCodeCoordinatorNotAvailable,
					ErrorMsg:  "cannot commit transaction as no members in cluster",
				}
			}
			batches, ok := pusherBatches[pusherAddress]
			if !ok {
				batches = map[int64]map[int32][]byte{}
				pusherBatches[pusherAddress] = batches
			}
			topicMap, ok := batches[topicID]
			if !ok {
				topicMap = make(map[int32][]byte)
				batches[topicID] = topicMap
			}
			topicMap[partitionID] = batchBytes
		}
	}
	// For each pusher send a single direct produce request with all the topics and partitions
	requests := make(map[string]*pusher.DirectProduceRequest, len(pusherBatches))
	for pusherAddress, topicMap := range pusherBatches {
		req := pusher.DirectProduceRequest{TopicProduceRequests: make([]pusher.TopicProduceRequest, len(topicMap))}
		for topicID, partitions := range topicMap {
			topicProduceRequest := pusher.TopicProduceRequest{
				TopicID:                  int(topicID),
				PartitionProduceRequests: make([]pusher.PartitionProduceRequest, len(partitions)),
			}
			for partitionID, batch := range partitions {
				topicProduceRequest.PartitionProduceRequests = append(topicProduceRequest.PartitionProduceRequests,
					pusher.PartitionProduceRequest{
						PartitionID: int(partitionID),
						Batch:       batch,
					})
			}
			req.TopicProduceRequests = append(req.TopicProduceRequests, topicProduceRequest)
		}
		requests[pusherAddress] = &req
	}
	// TODO Send them in parallel
	for pusherAddress, req := range requests {
		conn, err := t.c.connCaches.GetConnection(pusherAddress)
		if err != nil {
			return &kafkaencoding.KafkaError{
				ErrorCode: kafkaprotocol.ErrorCodeCoordinatorNotAvailable,
				ErrorMsg:  fmt.Sprintf("failed to get table pusher connection %v", err),
			}
		}
		buff := req.Serialize(createRequestBuffer())
		_, err = conn.SendRPC(transport.HandlerIDTablePusherDirectProduce, buff)
		if err != nil {
			if common.IsUnavailableError(err) {
				log.Warnf("failed to handle offset commit: %v", err)
				return &kafkaencoding.KafkaError{
					ErrorCode: kafkaprotocol.ErrorCodeCoordinatorNotAvailable,
					ErrorMsg:  fmt.Sprintf("failed to handle transaction marker write: %v", err),
				}
			} else {
				log.Errorf("failed to handle offset commit: %v", err)
				return &kafkaencoding.KafkaError{
					ErrorCode: kafkaprotocol.ErrorCodeUnknownServerError,
				}
			}
		}
	}
	return nil
}

func (t *txInfo) store() error {
	key := make([]byte, 0, 16)
	key = append(key, t.partHash...)
	key = encoding.EncodeVersion(key, 0)
	value := make([]byte, 0, 32)
	value = binary.BigEndian.AppendUint16(value, transactionMetadataVersion)
	value = t.storedState.Serialize(value)
	value = common.AppendValueMetadata(value)
	return t.sendDirectWrite([]common.KV{{
		Key:   key,
		Value: value,
	}})
}

func (t *txInfo) sendDirectWrite(kvs []common.KV) error {
	pusherAddress, ok := cluster.ChooseMemberAddressForHash(t.partHash, t.c.membership.Members)
	if !ok {
		// No available pushers
		return &kafkaencoding.KafkaError{ErrorCode: kafkaprotocol.ErrorCodeCoordinatorNotAvailable,
			ErrorMsg: "cannot store transaction as no members in cluster"}
	}
	pusherReq := common.DirectWriteRequest{
		WriterKey:   t.key,
		WriterEpoch: int(t.tektiteEpoch),
		KVs:         kvs,
	}
	buff := pusherReq.Serialize(createRequestBuffer())
	conn, err := t.c.connCaches.GetConnection(pusherAddress)
	if err != nil {
		return err
	}
	_, err = conn.SendRPC(transport.HandlerIDTablePusherDirectWrite, buff)
	return err
}

type txStoredState struct {
	status         txStatus
	pid            int64
	producerEpoch  int16
	partitions     map[int64][]int32
	consumerGroups []string
}

func (t *txStoredState) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(t.pid))
	buff = binary.BigEndian.AppendUint16(buff, uint16(t.producerEpoch))
	return buff
}

func (t *txStoredState) Deserialize(buff []byte, offset int) int {
	t.pid = int64(binary.BigEndian.Uint64(buff[offset:]))
	offset = offset + 8
	t.producerEpoch = int16(binary.BigEndian.Uint16(buff[offset:]))
	offset = offset + 2
	return offset
}
