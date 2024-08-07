package opers

import (
	"encoding/binary"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/kafka"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"sync"
	"sync/atomic"
	"time"
)

type BridgeFromOperator struct {
	BaseOperator
	id                   string
	schema               *OperatorSchema
	lock                 sync.Mutex
	procMgr              ProcessorManager
	stopped              bool
	receiverID           int
	consumers            map[int]*consumerHolder
	pollTimeout          time.Duration
	maxMessages          int
	msgClient            kafka.MessageClient
	consumerTimer        *common.TimerHandle
	disableIngest        bool
	cfg                  *conf.Config
	ingestedMessageCount *uint64
	ingestPaused         bool
	offsetsSlabID        uint64
	maxPartitionID       int
	ingestEnabled        *atomic.Bool
	lastFlushedVersion   *int64
	topicName            string
	watermarkOperator    *WaterMarkOperator
	hashCache            *partitionHashCache
}

const (
	defaultPollTimeout = 50 * time.Millisecond
	defaultMaxMessages = 1000
)

var KafkaSchema = evbatch.NewEventSchema([]string{OffsetColName, EventTimeColName, "key", "hdrs", "val"},
	[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeBytes, types.ColumnTypeBytes, types.ColumnTypeBytes})

func NewBridgeFromOperator(receiverID int, pollTimeout time.Duration, maxMessages int, topicName string, partitions int,
	kafkaProps map[string]string, clientFactory kafka.ClientFactory, testDisableIngest bool,
	procMgr ProcessorManager, offsetsSlabID int, cfg *conf.Config, ingestedMessageCount *uint64, mappingID string,
	ingestEnabled *atomic.Bool, lastFlushedVersion *int64) (*BridgeFromOperator, error) {
	schema := &OperatorSchema{
		EventSchema:     KafkaSchema,
		PartitionScheme: NewPartitionScheme(mappingID, partitions, true, cfg.ProcessorCount),
	}
	var msgClient kafka.MessageClient
	if clientFactory != nil {
		var err error
		msgClient, err = clientFactory(topicName, kafkaProps)
		if err != nil {
			return nil, err
		}
	}
	minOffsets := make([]int64, schema.PartitionScheme.MaxPartitionID+1)
	for i := 0; i < len(minOffsets); i++ {
		minOffsets[i] = -1
	}
	return &BridgeFromOperator{
		id:                   uuid.New().String(),
		schema:               schema,
		receiverID:           receiverID,
		pollTimeout:          pollTimeout,
		maxMessages:          maxMessages,
		msgClient:            msgClient,
		consumers:            map[int]*consumerHolder{},
		disableIngest:        testDisableIngest,
		procMgr:              procMgr,
		cfg:                  cfg,
		ingestedMessageCount: ingestedMessageCount,
		offsetsSlabID:        uint64(offsetsSlabID),
		ingestEnabled:        ingestEnabled,
		lastFlushedVersion:   lastFlushedVersion,
		topicName:            topicName,
		hashCache:            newPartitionHashCache(schema.MappingID, schema.Partitions),
	}, nil
}

type processorBatchReceiver struct {
	bf        *BridgeFromOperator
	processor proc.Processor
}

func (p *processorBatchReceiver) HandleMessages(messages []*kafka.Message) error {
	return p.bf.IngestMessages(p.processor, messages)
}

func (bf *BridgeFromOperator) IngestMessages(processor proc.Processor, msgs []*kafka.Message) error {
	colBuilders := evbatch.CreateColBuilders(bf.schema.EventSchema.ColumnTypes())
	partitionID := -1
	var batches map[int][]evbatch.ColumnBuilder
	var maxEventTime int64
	for _, msg := range msgs {
		partID := int(msg.PartInfo.PartitionID)
		var theColBuilders []evbatch.ColumnBuilder
		if batches == nil && (partitionID == -1 || partID == partitionID) {
			theColBuilders = colBuilders
			partitionID = partID
		} else {
			// More than one partition in batch
			if batches == nil {
				batches = make(map[int][]evbatch.ColumnBuilder, 2)
				batches[partitionID] = colBuilders
			}
			colBuilders, ok := batches[partID]
			if !ok {
				colBuilders = evbatch.CreateColBuilders(bf.schema.EventSchema.ColumnTypes())
				batches[partID] = colBuilders
			}
			theColBuilders = colBuilders
		}
		theColBuilders[0].(*evbatch.IntColBuilder).Append(msg.PartInfo.Offset)
		unixMillis := msg.TimeStamp.In(time.UTC).UnixMilli()
		if unixMillis > maxEventTime {
			maxEventTime = unixMillis
		}
		eventTime := types.NewTimestamp(unixMillis)
		theColBuilders[1].(*evbatch.TimestampColBuilder).Append(eventTime)
		theColBuilders[2].(*evbatch.BytesColBuilder).Append(msg.Key)
		// Note, we store the raw headers, that includes the number of headers as first element, so no headers
		// as varint is []byte{0}
		if len(msg.Headers) > 0 {
			theColBuilders[3].(*evbatch.BytesColBuilder).Append(createMessageHeaders(msg.Headers))
		} else {
			theColBuilders[3].(*evbatch.BytesColBuilder).Append(singleZeroByteArray)
		}
		theColBuilders[4].(*evbatch.BytesColBuilder).Append(msg.Value)
	}

	// We encode the maxEventTime on the evBatchBytes - this allows us to avoid iterating over all rows in the batch
	// to get the max event time in kafka_in when the batch is received.

	evBatchBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(evBatchBytes, uint64(maxEventTime))

	if batches == nil {
		// Single partition case
		evBatch := evbatch.NewBatchFromBuilders(bf.schema.EventSchema, colBuilders...)
		// Note: we don't need to set processor id as it's only used when forwarding
		pb := proc.NewProcessBatch(-1, evBatch, bf.receiverID, partitionID, -1)
		pb.EvBatchBytes = evBatchBytes
		if err := processor.IngestBatchSync(pb); err != nil {
			return err
		}
	} else {
		// Multiple partitions
		for partID, colBuilders := range batches {
			evBatch := evbatch.NewBatchFromBuilders(bf.schema.EventSchema, colBuilders...)
			pb := proc.NewProcessBatch(-1, evBatch, bf.receiverID, partID, -1)
			pb.EvBatchBytes = evBatchBytes
			if err := processor.IngestBatchSync(pb); err != nil {
				return err
			}
		}
	}
	atomic.AddUint64(bf.ingestedMessageCount, uint64(len(msgs)))
	log.Debugf("bridge from ingested %d msgs - %s ", len(msgs), bf.topicName)
	return nil
}

var singleZeroByteArray = []byte{0}

func createMessageHeaders(headers []kafka.MessageHeader) []byte {
	bytes := make([]byte, 0, 64)
	bytes = binary.AppendVarint(bytes, int64(len(headers)))
	for _, hdr := range headers {
		headerName := hdr.Key
		headerVal := hdr.Value
		bytes = binary.AppendVarint(bytes, int64(len(headerName)))
		bytes = append(bytes, []byte(headerName)...)
		bytes = binary.AppendVarint(bytes, int64(len(headerVal)))
		bytes = append(bytes, headerVal...)
	}
	return bytes
}

func (bf *BridgeFromOperator) ReceiveBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	if bf.watermarkOperator.waterMarkType == WaterMarkTypeEventTime {
		maxEventTime := int64(binary.LittleEndian.Uint64(execCtx.EventBatchBytes()))
		bf.watermarkOperator.updateMaxEventTime(int(maxEventTime), execCtx.Processor().ID())
	}
	partitionID := execCtx.PartitionID()
	if batch.RowCount > 0 {
		lastOffset := int(batch.GetIntColumn(0).Get(batch.RowCount - 1))
		bf.storeLastOffset(partitionID, lastOffset, execCtx)
	}
	return batch, bf.sendBatchDownStream(batch, execCtx)
}

func (bf *BridgeFromOperator) ForwardingProcessorCount() int {
	return len(bf.schema.PartitionScheme.ProcessorIDs)
}

func (bf *BridgeFromOperator) ReceiveBarrier(execCtx StreamExecContext) error {
	bf.watermarkOperator.setWatermark(execCtx)
	return bf.BaseOperator.HandleBarrier(execCtx)
}

func (bf *BridgeFromOperator) RequiresBarriersInjection() bool {
	return true
}

func (bf *BridgeFromOperator) HandleStreamBatch(*evbatch.Batch, StreamExecContext) (*evbatch.Batch, error) {
	panic("not used")
}

func (bf *BridgeFromOperator) HandleBarrier(StreamExecContext) error {
	panic("not used")
}

func (bf *BridgeFromOperator) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not used")
}

func (bf *BridgeFromOperator) InSchema() *OperatorSchema {
	return bf.schema
}

func (bf *BridgeFromOperator) OutSchema() *OperatorSchema {
	return bf.schema
}

func (bf *BridgeFromOperator) Setup(mgr StreamManagerCtx) error {
	bf.lock.Lock()
	defer bf.lock.Unlock()
	mgr.RegisterReceiver(bf.receiverID, bf)
	if bf.disableIngest {
		return nil
	}
	processors := mgr.ProcessorManager().RegisterListener(bf.id, bf.processorChange)
	for _, p := range processors {
		if err := bf.processorStarted(p, false); err != nil {
			return err
		}
	}
	bf.scheduleConsumerCheckTimer(true)
	return nil
}

func (bf *BridgeFromOperator) Teardown(mgr StreamManagerCtx, completeCB func(error)) {
	bf.lock.Lock()
	defer bf.lock.Unlock()
	bf.stopped = true
	if bf.consumerTimer != nil {
		bf.consumerTimer.Stop()
	}
	// We must stop the consumers async as we can't hold the stream manager lock while they are being stopped, as
	// stop blocks until complete and won't complete until consumers have finished processing and processing could
	// be blocked waiting to get read lock on stream manager to handle batch
	go func() {
		bf.lock.Lock()
		for pid, consumer := range bf.consumers {
			delete(bf.consumers, pid)
			// stop the consumer - this blocks, waiting for processing to complete.
			consumer.stop(false)
		}
		bf.lock.Unlock()
		completeCB(nil)
	}()

	mgr.ProcessorManager().UnregisterListener(bf.id)
	mgr.UnregisterReceiver(bf.receiverID)
}

func (bf *BridgeFromOperator) processorChange(processor proc.Processor, started bool, promoted bool) {
	bf.lock.Lock()
	defer bf.lock.Unlock()
	if bf.stopped {
		return
	}
	if started {
		if err := bf.processorStarted(processor, promoted); err != nil {
			log.Errorf("failed to handle processorStarted %v", err)
		}
	} else {
		bf.processorStopped(processor)
	}
}

func (bf *BridgeFromOperator) processorStarted(processor proc.Processor, promoted bool) error {
	partitions, ok := bf.schema.PartitionScheme.ProcessorPartitionMapping[processor.ID()]
	if !ok {
		// that processor doesn't have any partitions - nothing to do
		return nil
	}
	log.Debugf("bridge from for topic %s processor %d started with partitions %v", bf.topicName, processor.ID(), partitions)

	_, exists := bf.consumers[processor.ID()]
	if exists {
		panic("consumer already exists")
	}
	holder := &consumerHolder{
		bf:         bf,
		processor:  processor,
		partitions: partitions,
	}
	bf.consumers[processor.ID()] = holder

	ingestEnabled := bf.ingestEnabled.Load()

	if promoted || !ingestEnabled {
		// If processor newly promoted from replica to leader, we keep it paused for now, this is because the failure
		// process will occur right after this, which will immediately pause ingest, do failure, then re-enable.
		// There is no point in starting consumer active after failure, allowing them to consume messages then immediately
		// stopping then, doing failure, rolling back and restarting them. Better to start them as paused
		log.Debugf("bridge from for topic %s processor %d started ingest is not enabled", bf.topicName, processor.ID())
		holder.paused = true
	} else {
		log.Debugf("bridge from for topic %s processor %d started ingest is enabled", bf.topicName, processor.ID())
		startOffsets, err := bf.getStartOffsetsForConsumer(holder, atomic.LoadInt64(bf.lastFlushedVersion))
		if err != nil {
			return err
		}
		holder.startOffsets = startOffsets
		holder.start()
	}
	return nil
}

func (bf *BridgeFromOperator) processorStopped(processor proc.Processor) {
	holder, exists := bf.consumers[processor.ID()]
	if !exists {
		return
	}
	holder.stop(false)
	delete(bf.consumers, processor.ID())
}

func (bf *BridgeFromOperator) scheduleConsumerCheckTimer(first bool) {
	bf.consumerTimer = common.ScheduleTimer(bf.cfg.ConsumerRetryInterval, first, bf.checkConsumers)
}

func (bf *BridgeFromOperator) checkConsumers() {
	bf.lock.Lock()
	defer bf.lock.Unlock()
	if bf.stopped {
		return
	}
	for _, holder := range bf.consumers {
		holder.checkStarted()
	}
	bf.scheduleConsumerCheckTimer(false)
}

func (bf *BridgeFromOperator) numConsumers() int {
	bf.lock.Lock()
	defer bf.lock.Unlock()
	return len(bf.consumers)
}

func (bf *BridgeFromOperator) getConsumers() []*consumerHolder {
	bf.lock.Lock()
	defer bf.lock.Unlock()
	var consumers []*consumerHolder
	for _, ch := range bf.consumers {
		consumers = append(consumers, ch)
	}
	return consumers
}

type consumerHolder struct {
	bf           *BridgeFromOperator
	processor    proc.Processor
	consumer     *MessageConsumer
	partitions   []int
	paused       bool
	startOffsets []int64
}

// always called with bf lock held
func (c *consumerHolder) start() {
	if c.consumer != nil {
		panic("already started")
	}
	msgProvider, err := c.bf.msgClient.NewMessageProvider(c.partitions, c.startOffsets)
	if err != nil {
		log.Warnf("%s: failed to create message provider %v", c.bf.cfg.LogScope, err)
		return
	}
	consumer, err := NewMessageConsumer(&processorBatchReceiver{
		bf:        c.bf,
		processor: c.processor,
	}, msgProvider, c.bf.pollTimeout, c.bf.maxMessages, func(err error) {
		// run on separate GR to prevent deadlock
		common.Go(func() {
			c.bf.lock.Lock()
			defer c.bf.lock.Unlock()
			c.stop(true)
		})
	})
	if err != nil {
		log.Warnf("failed to create message consumer %v", err)
		return
	}
	c.consumer = consumer
	c.paused = false
}

// always called with bf lock held
func (c *consumerHolder) checkStarted() {
	if c.consumer == nil && !c.paused {
		c.start()
	}
}

// always called with bf lock held
func (c *consumerHolder) stop(fromLoop bool) {
	if c.consumer == nil {
		return
	}
	consumer := c.consumer
	c.consumer = nil
	if consumer != nil {
		if err := consumer.Stop(fromLoop); err != nil {
			log.Debugf("failed to stop consumer %v", err)
		}
	}
}

func (c *consumerHolder) pause() {
	c.paused = true
	c.stop(false)
}

func (bf *BridgeFromOperator) loadLastOffset(partitionID int, maxVersion uint64, processor proc.Processor) (int64, error) {
	partitionHash := bf.hashCache.getHash(partitionID)
	key := encoding.EncodeEntryPrefix(partitionHash, bf.offsetsSlabID, 24)
	v, err := processor.GetWithMaxVersion(key, maxVersion)
	if err != nil {
		return 0, err
	}
	var lastOffset int64
	if v == nil {
		lastOffset = -1
	} else {
		lastOffset = int64(binary.LittleEndian.Uint64(v))
	}
	return lastOffset, nil
}

func (bf *BridgeFromOperator) storeLastOffset(partitionID int, offset int, execCtx StreamExecContext) {
	partitionHash := bf.hashCache.getHash(partitionID)
	key := encoding.EncodeEntryPrefix(partitionHash, bf.offsetsSlabID, 32)
	key = encoding.EncodeVersion(key, uint64(execCtx.WriteVersion()))
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(offset))
	execCtx.StoreEntry(common.KV{
		Key:   key,
		Value: value,
	}, false)
	if log.DebugEnabled {
		log.Debugf("bridge from for topic %s storing offset %d for partition %d, at version %d - processor %d", bf.topicName,
			offset, partitionID, execCtx.WriteVersion(), execCtx.Processor().ID())
	}
}

// Start and stop ingest

func (bf *BridgeFromOperator) stopIngest() {
	bf.lock.Lock()
	defer bf.lock.Unlock()

	// Failure recovery is in progress - so we stop the consumers while this is occurring.
	for _, holder := range bf.consumers {
		holder.pause()
	}
}

func (bf *BridgeFromOperator) startIngest(version int64) error {
	bf.lock.Lock()
	defer bf.lock.Unlock()

	// reset the offsets from the last flushed version and restart consumers from those
	for _, holder := range bf.consumers {
		startOffsets, err := bf.getStartOffsetsForConsumer(holder, version)
		if err != nil {
			return err
		}
		holder.startOffsets = startOffsets
		holder.start()
	}

	return nil
}

func (bf *BridgeFromOperator) getStartOffsetsForConsumer(holder *consumerHolder, version int64) ([]int64, error) {
	startOffsets := make([]int64, len(holder.partitions))
	for i, partID := range holder.partitions {
		var lastOffset int64 = -1
		// Can be -1 if startup first time before any version is flushed
		if version != -1 {
			var err error
			lastOffset, err = bf.loadLastOffset(partID, uint64(version), holder.processor)
			if err != nil {
				return nil, err
			}
			log.Debugf("bridge from for topic %s loaded last offset %d for partition %d at version %d", bf.topicName,
				lastOffset, partID, version)
		}
		if lastOffset == -1 {
			startOffsets[i] = -1
		} else {
			startOffsets[i] = lastOffset + 1
		}
	}
	return startOffsets, nil
}

func (bf *BridgeFromOperator) GetWatermarkOperator() *WaterMarkOperator {
	return bf.watermarkOperator
}
