package opers

import (
	"bytes"
	"fmt"
	"github.com/alecthomas/participle/v2/lexer"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/kafka"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

const SlabSequenceName = "seq.slab"
const ReceiverSequenceName = "seq.receiver"

var deleteSlabSchema = evbatch.NewEventSchema([]string{"key", "value"},
	[]types.ColumnType{types.ColumnTypeBytes, types.ColumnTypeBytes})

type StreamManager interface {
	proc.BatchHandler
	proc.ReceiverInfoProvider
	DeployStream(streamDesc parser.CreateStreamDesc, receiverSequences []int, slabSequences []int, tsl string,
		commandID int64) error
	UndeployStream(deleteStremDesc parser.DeleteStreamDesc, commandID int64) error
	GetStream(name string) *StreamInfo
	GetAllStreams() []*StreamInfo
	GetKafkaEndpoint(name string) *KafkaEndpointInfo
	GetAllKafkaEndpoints() []*KafkaEndpointInfo
	RegisterSystemSlab(slabName string, persistorReceiverID int, deleterReceiverID int, slabID int,
		schema *OperatorSchema, keyCols []string, noCache bool) error
	SetProcessorManager(procMgr ProcessorManager)
	GetIngestedMessageCount() int
	PrepareForShutdown()
	StreamCount() int
	Start() error
	Stop() error
	Loaded()
	RegisterChangeListener(listener func(streamName string, deployed bool))
	StartIngest(version int) error
	StopIngest() error
	StreamMetaIteratorProvider() *StreamMetaIteratorProvider
	Dump()
	RegisterReceiverWithLock(id int, receiver Receiver)
}

type Receiver interface {
	InSchema() *OperatorSchema
	OutSchema() *OperatorSchema
	ForwardingProcessorCount() int
	ReceiveBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error)
	ReceiveBarrier(execCtx StreamExecContext) error
	RequiresBarriersInjection() bool
}

type StreamManagerCtx interface {
	RegisterReceiver(id int, receiver Receiver)
	UnregisterReceiver(id int)
	ProcessorManager() ProcessorManager
}

type slabRetentions interface {
	RegisterSlabRetention(slabID int, retention time.Duration) error
	UnregisterSlabRetention(slabID int) error
}

type slabRetention struct {
	slabID    int
	Retention time.Duration
}

type ProcessorManager interface {
	RegisterListener(listenerName string, listener proc.ProcessorListener) []proc.Processor
	UnregisterListener(listenerName string)
	AfterReceiverChange()
	ForwardBatch(batch *proc.ProcessBatch, replicate bool, completionFunc func(error))
	GetProcessor(processorID int) proc.Processor
}

// SlabInfo A Slab represents tabular storage. We don't call it table as we distinguish between table and stream in the mental
// model. A stored stream and a stored table both use a slab for underlying storage.
type SlabInfo struct {
	StreamName    string
	SlabID        int
	Schema        *OperatorSchema
	KeyColIndexes []int
	Type          SlabType
}

type SlabType int

const SlabTypeUserStream = SlabType(0)
const SlabTypeUserTable = SlabType(1)
const SlabTypeQueryableInternal = SlabType(2)
const SlabTypeInternal = SlabType(3)

func NewStreamManager(messageClientFactory kafka.ClientFactory,
	slabRetentions slabRetentions, expressionFactory *expr.ExpressionFactory, cfg *conf.Config, disableIngest bool) StreamManager {
	mgr := &streamManager{
		slabRetentions:         slabRetentions,
		expressionFactory:      expressionFactory,
		messageClientFactory:   messageClientFactory,
		streams:                map[string]*StreamInfo{},
		kafkaEndpoints:         map[string]*KafkaEndpointInfo{},
		receivers:              map[int]Receiver{},
		cfg:                    cfg,
		disableIngest:          disableIngest,
		requiredCompletions:    -1,
		lastProcessedCommandID: -1,
		bridgeFromOpers:        map[*BridgeFromOperator]struct{}{},
		partitionOperators:     map[*PartitionOperator]struct{}{},
		lastFlushedVersion:     -1,
		streamMemStore:         treemap.NewWithStringComparator(),
	}
	mgr.streamMetaIterProvider = &StreamMetaIteratorProvider{pm: mgr}
	mgr.receivers[common.DummyReceiverID] = newDummyReceiver()
	mgr.receivers[common.DeleteSlabReceiverID] = &deleteSlabReceiver{
		schema: &OperatorSchema{
			EventSchema: deleteSlabSchema,
		},
		sm: mgr,
	}
	mgr.calculateInjectableReceivers()
	return mgr
}

type streamManager struct {
	lock                   sync.RWMutex
	slabRetentions         slabRetentions
	expressionFactory      *expr.ExpressionFactory
	streams                map[string]*StreamInfo
	kafkaEndpoints         map[string]*KafkaEndpointInfo
	receivers              map[int]Receiver
	injectableReceivers    atomic.Pointer[map[int][]int]
	requiredCompletions    int
	processorManager       ProcessorManager
	messageClientFactory   kafka.ClientFactory
	cfg                    *conf.Config
	disableIngest          bool
	ingestedMessageCount   uint64
	shuttingDown           bool
	shutdownLock           sync.Mutex
	lastProcessedCommandID int
	loaded                 bool
	changeListeners        []func(string, bool)
	bridgeFromOpers        map[*BridgeFromOperator]struct{}
	partitionOperators     map[*PartitionOperator]struct{}
	ingestEnabled          atomic.Bool
	lastFlushedVersion     int64
	sysStreamCount         int
	streamMemStore         *treemap.Map
	streamMetaIterProvider *StreamMetaIteratorProvider
	lastCommandID          int64
}

func (sm *streamManager) GetIngestedMessageCount() int {
	return int(atomic.LoadUint64(&sm.ingestedMessageCount))
}

type StreamInfo struct {
	Operators             []Operator
	StreamDesc            parser.CreateStreamDesc
	UserSlab              *SlabInfo
	ExtraSlabs            map[string]*SlabInfo
	DownstreamStreamNames map[string]struct{}
	UpstreamStreamNames   map[string]Operator
	Tsl                   string
	InSchema              *OperatorSchema
	OutSchema             *OperatorSchema
	SystemStream          bool
	CommandID             int64
	Undeploying           bool
	StreamMeta            bool
}

type KafkaEndpointInfo struct {
	Name        string
	InEndpoint  *KafkaInOperator
	OutEndpoint *KafkaOutOperator
	Schema      *OperatorSchema
}

func validateStream(streamDesc *parser.CreateStreamDesc) error {
	lastIndex := len(streamDesc.OperatorDescs) - 1
	for i := 0; i < len(streamDesc.OperatorDescs); i++ {
		op := streamDesc.OperatorDescs[i]
		switch o := op.(type) {
		case *parser.BackfillDesc:
			ok := false
			if i == 1 {
				_, hasContinuation := streamDesc.OperatorDescs[0].(*parser.ContinuationDesc)
				if hasContinuation {
					ok = true
				}
			}
			if !ok {
				return statementErrorAtTokenNamef("", o, "'backfill' must be directly after the parent stream in a child stream")
			}
		case *parser.BridgeFromDesc:
			if i != 0 {
				return statementErrorAtTokenNamef("", o, "'bridge from' must be the first operator in a stream")
			}
		case *parser.BridgeToDesc:
			if i == 0 {
				return statementErrorAtTokenNamef("", o, "'bridge to' cannot be the first operator in a stream")
			}
			if i != lastIndex {
				return statementErrorAtTokenNamef("", o, "'bridge to' must be the last operator in a stream")
			}
		case *parser.ContinuationDesc:
			if i != 0 {
				return statementErrorAtTokenNamef("", o, "continuation (->) must be at the start of a child stream")
			}
		case *parser.FilterDesc:
			if i == 0 {
				return statementErrorAtTokenNamef("", o, "'filter' cannot be the first operator in a stream")
			}
		case *parser.JoinDesc:
			if i != 0 {
				return statementErrorAtTokenNamef("", o, "'join' must be the first operator in a stream")
			}
		case *parser.KafkaInDesc:
			if i != 0 {
				return statementErrorAtTokenNamef("", o, "'kafka in' must be the first operator in a stream")
			}
		case *parser.PartitionDesc:
			if i == 0 {
				return statementErrorAtTokenNamef("", o, "'partition' cannot be the first operator in a stream")
			}
		case *parser.ProjectDesc:
			if i == 0 {
				return statementErrorAtTokenNamef("", o, "'project' cannot be the first operator in a stream")
			}
		case *parser.StoreStreamDesc:
			if i == 0 {
				return statementErrorAtTokenNamef("", o, "'store stream' cannot be the first operator in a stream")
			}
			if i != lastIndex {
				return statementErrorAtTokenNamef("", o, "'store stream' must be the last operator in a stream")
			}
		case *parser.StoreTableDesc:
			if i == 0 {
				return statementErrorAtTokenNamef("", o, "'store table' cannot be the first operator in a stream")
			}
			if i != lastIndex {
				return statementErrorAtTokenNamef("", o, "'store table' must be the last operator in a stream")
			}
		case *parser.KafkaOutDesc:
			if i == 0 {
				return statementErrorAtTokenNamef("", o, "'kafka out' cannot be the first operator in a stream")
			}
			if i != lastIndex {
				return statementErrorAtTokenNamef("", o, "'kafka out' must be the last operator in a stream")
			}
		case *parser.AggregateDesc:
			if i == 0 {
				return statementErrorAtTokenNamef("", o, "'aggregate' cannot be the first operator in a stream")
			}
			store := true
			if o.Store != nil {
				store = *o.Store
			}
			if store && i != lastIndex {
				return statementErrorAtTokenNamef("", o, "'aggregate' with 'store' = true must be the last operator in a stream")
			}
		case *parser.TopicDesc:
			if i != 0 || i != lastIndex {
				return statementErrorAtTokenNamef("", o, "'topic' must be the only operator in a stream")
			}
		case *parser.UnionDesc:
			if i != 0 {
				return statementErrorAtTokenNamef("", o, "'union' must be the first operator in a stream")
			}
		}
	}
	return nil
}

func (sm *streamManager) SetProcessorManager(procMgr ProcessorManager) {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	sm.processorManager = procMgr
}

func (sm *streamManager) PrepareForShutdown() {
	sm.shutdownLock.Lock()
	defer sm.shutdownLock.Unlock()
	sm.shuttingDown = true
}

func (sm *streamManager) StreamCount() int {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	return len(sm.streams) - sm.sysStreamCount // we don't count the system ones
}

func (sm *streamManager) RegisterChangeListener(listener func(string, bool)) {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	sm.changeListeners = append(sm.changeListeners, listener)
}

func (sm *streamManager) callChangeListeners(streamName string, deployed bool) {
	for _, listener := range sm.changeListeners {
		listener(streamName, deployed)
	}
}

func (sm *streamManager) DeployStream(streamDesc parser.CreateStreamDesc, receiverSequences []int,
	slabSequences []int, tsl string, commandID int64) error {
	if isReservedIdentifierName(streamDesc.StreamName) {
		return statementErrorAtTokenNamef(streamDesc.StreamName, &streamDesc, "stream name '%s' is a reserved name", streamDesc.StreamName)
	}
	_, exists := sm.streams[streamDesc.StreamName]
	if exists {
		return statementErrorAtTokenNamef(streamDesc.StreamName, &streamDesc, "stream '%s' already exists", streamDesc.StreamName)
	}
	if err := validateStream(&streamDesc); err != nil {
		return err
	}
	streamDesc = sm.maybeRewriteDesc(streamDesc)
	return sm.deployStream(streamDesc, receiverSequences, slabSequences, tsl, commandID)
}

func (sm *streamManager) maybeRewriteDesc(streamDesc parser.CreateStreamDesc) parser.CreateStreamDesc {
	var descs []parser.Parseable
	for _, desc := range streamDesc.OperatorDescs {
		topicDesc, ok := desc.(*parser.TopicDesc)
		if ok {
			// Convert Topic to KafkaIn and KafkaOut
			kIn := &parser.KafkaInDesc{
				Partitions:           topicDesc.Partitions,
				WatermarkType:        topicDesc.WatermarkType,
				WatermarkLateness:    topicDesc.WatermarkLateness,
				WatermarkIdleTimeout: topicDesc.WatermarkIdleTimeout,
			}
			kOut := &parser.KafkaOutDesc{
				Retention: topicDesc.Retention,
			}
			descs = append(descs, kIn)
			descs = append(descs, kOut)
		} else {
			descs = append(descs, desc)
		}
	}
	streamDesc.OperatorDescs = descs
	return streamDesc
}

func (sm *streamManager) deployStream(streamDesc parser.CreateStreamDesc, receiverSequences []int,
	slabSequences []int, tsl string, commandID int64) error {
	log.Debugf("deploying stream %s", streamDesc.StreamName)
	sm.shutdownLock.Lock()
	defer sm.shutdownLock.Unlock()
	if sm.shuttingDown {
		return errors.NewTektiteErrorf(errors.ShutdownError, "cluster is shutting down")
	}
	sm.lock.Lock()
	defer sm.lock.Unlock()
	var operators []Operator
	var prevOperator Operator
	var kafkaEndpointInfo *KafkaEndpointInfo
	var retentions []slabRetention
	var deferredWirings []func(info *StreamInfo)
	receiverSliceSeqs := &sliceSeq{seqs: receiverSequences}
	slabSliceSeqs := &sliceSeq{seqs: slabSequences}
	extraSlabInfos := map[string]*SlabInfo{}
	var userSlab *SlabInfo
	for _, desc := range streamDesc.OperatorDescs {
		var oper Operator
		var err error
		switch op := desc.(type) {
		case *TestSourceDesc:
			partitionScheme := NewPartitionScheme(streamDesc.StreamName, op.Partitions, false, sm.cfg.ProcessorCount)
			operSchema := &OperatorSchema{
				EventSchema:     evbatch.NewEventSchema(op.ColumnNames, op.ColumnTypes),
				PartitionScheme: partitionScheme,
			}
			oper = &testSourceOper{
				schema: operSchema,
			}
		case *parser.BridgeFromDesc:
			oper, err = sm.deployBridgeFromOperator(streamDesc.StreamName, op, receiverSliceSeqs, slabSliceSeqs, extraSlabInfos)
		case *parser.BridgeToDesc:
			oper, retentions, userSlab, err = sm.deployBridgeToOperator(streamDesc.StreamName, op, prevOperator, receiverSliceSeqs,
				slabSliceSeqs, extraSlabInfos, retentions)
		case *parser.KafkaInDesc:
			oper, kafkaEndpointInfo, err = sm.deployKafkaInOperator(streamDesc.StreamName, op, receiverSliceSeqs,
				slabSliceSeqs, extraSlabInfos)
		case *parser.KafkaOutDesc:
			oper, retentions, userSlab, err = sm.deployKafkaOutOperator(streamDesc.StreamName, op,
				prevOperator, kafkaEndpointInfo, slabSliceSeqs, extraSlabInfos, retentions)
		case *parser.FilterDesc:
			oper, err = NewFilterOperator(prevOperator.OutSchema(), op.Expr, sm.expressionFactory)
		case *parser.ProjectDesc:
			oper, err = NewProjectOperator(prevOperator.OutSchema(), op.Expressions, true, sm.expressionFactory)
		case *parser.PartitionDesc:
			oper, err = sm.deployPartitionOperator(op, prevOperator, receiverSliceSeqs)
		case *parser.AggregateDesc:
			oper, retentions, userSlab, err = sm.deployAggregateOperator(streamDesc.StreamName, op, prevOperator,
				slabSliceSeqs, receiverSliceSeqs, retentions, extraSlabInfos)
		case *parser.StoreStreamDesc:
			oper, retentions, userSlab, err = sm.deployStoreStreamOperator(streamDesc.StreamName, op,
				prevOperator, slabSliceSeqs, extraSlabInfos, retentions)
		case *parser.StoreTableDesc:
			oper, retentions, userSlab, err = sm.deployStoreTableOperator(streamDesc.StreamName, op, prevOperator,
				slabSliceSeqs, retentions)
		case *parser.BackfillDesc:
			oper, err = sm.deployBackfillOperator(streamDesc.StreamName, operators, receiverSliceSeqs, slabSliceSeqs, op, extraSlabInfos)
		case *parser.JoinDesc:
			var deferredWiring func(info *StreamInfo)
			oper, extraSlabInfos, retentions, deferredWiring, err = sm.deployJoinOperator(streamDesc.StreamName, op, receiverSliceSeqs,
				slabSliceSeqs, extraSlabInfos, retentions)
			if err == nil {
				deferredWirings = append(deferredWirings, deferredWiring)
			}
		case *parser.ContinuationDesc:
			upstreamStream, ok := sm.streams[op.ParentStreamName]
			if !ok {
				return statementErrorAtTokenNamef(op.ParentStreamName, op, "unknown parent stream '%s'",
					op.ParentStreamName)
			}
			upstreamLastOper := upstreamStream.Operators[len(upstreamStream.Operators)-1]
			oper = &ContinuationOperator{
				schema: upstreamLastOper.OutSchema(),
			}
			oper.SetParentOperator(upstreamLastOper)

			deferredWiring := func(info *StreamInfo) {
				info.UpstreamStreamNames[upstreamStream.StreamDesc.StreamName] = oper
				upstreamStream.DownstreamStreamNames[streamDesc.StreamName] = struct{}{}
				upstreamLastOper.AddDownStreamOperator(oper)
				sm.storeStreamMeta(upstreamStream)
			}
			deferredWirings = append(deferredWirings, deferredWiring)
		case *parser.UnionDesc:
			var deferredWiring func(info *StreamInfo)
			oper, deferredWiring, err = sm.deployUnionOperator(streamDesc.StreamName, op, receiverSliceSeqs)
			deferredWirings = append(deferredWirings, deferredWiring)
		default:
			panic("unexpected operator")
		}
		if err != nil {
			return err
		}
		operators = append(operators, oper)
		if prevOperator != nil {
			oper.SetParentOperator(prevOperator)
		}
		prevOperator = oper
	}
	if streamDesc.TestSink {
		// Only used in tests - We add a special sink operator which captures the outgoing batches and contexts
		operators = append(operators, newTestSinkOper(prevOperator.OutSchema()))
	}

	info := &StreamInfo{
		Operators:             operators,
		StreamDesc:            streamDesc,
		UserSlab:              userSlab,
		ExtraSlabs:            extraSlabInfos,
		DownstreamStreamNames: map[string]struct{}{},
		UpstreamStreamNames:   map[string]Operator{},
		Tsl:                   tsl,
		InSchema:              operators[0].InSchema(),
		OutSchema:             operators[len(operators)-1].OutSchema(),
		CommandID:             commandID,
	}
	for i, oper := range operators {
		oper.SetStreamInfo(info)
		if i != len(operators)-1 {
			nextOper := operators[i+1]
			oper.AddDownStreamOperator(nextOper)
		}
		if sm.loaded {
			if err := oper.Setup(sm); err != nil {
				return err
			}
		}
	}
	sm.streams[streamDesc.StreamName] = info
	if kafkaEndpointInfo != nil {
		sm.kafkaEndpoints[streamDesc.StreamName] = kafkaEndpointInfo
	}
	sm.invalidateCachedInfo()
	for _, retention := range retentions {
		if retention.Retention != 0 {
			if err := sm.slabRetentions.RegisterSlabRetention(retention.slabID, retention.Retention); err != nil {
				if !sm.loaded {
					// When reprocessing command log on restart these can fail as processor not started yet, that's
					// ok, they should already be registered
					log.Debugf("failed to register slab retentions (not loaded): %v", err)
				} else {
					log.Warnf("failed to register slab retentions: %v", err)
				}
			}
		}
	}
	for _, deferred := range deferredWirings {
		deferred(info)
	}
	sm.storeStreamMeta(info)
	sm.lastCommandID = commandID
	if sm.loaded {
		sm.calculateInjectableReceivers()
	}
	sm.callChangeListeners(streamDesc.StreamName, true)
	if sm.loaded {
		sm.processorManager.AfterReceiverChange() // Must be outside of lock
	}
	return nil
}

func (sm *streamManager) deployBridgeFromOperator(streamName string, op *parser.BridgeFromDesc,
	receiverSliceSeqs *sliceSeq, slabSliceSeqs *sliceSeq, extraSlabInfos map[string]*SlabInfo) (*BridgeFromOperator, error) {
	receiverID := receiverSliceSeqs.GetNextID()
	var pollTimeout = defaultPollTimeout
	if op.PollTimeout != nil {
		pollTimeout = *op.PollTimeout
	}
	if op.Partitions < 1 {
		return nil, statementErrorAtTokenNamef("partitions", op, "invalid value for 'partitions' - must be > 0")
	}
	var maxPollMessages = defaultMaxMessages
	if op.MaxPollMessages != nil {
		maxPollMessages = *op.MaxPollMessages
	}
	if maxPollMessages < 1 {
		return nil, statementErrorAtTokenNamef("max_poll_messages", op, "invalid value for 'max_poll_messages' - must be > 0")
	}
	offsetsSlabID := slabSliceSeqs.GetNextID()

	bf, err := NewBridgeFromOperator(receiverID, pollTimeout, maxPollMessages, op.TopicName, op.Partitions,
		op.Props, sm.messageClientFactory, sm.disableIngest,
		sm.processorManager, offsetsSlabID, sm.cfg, &sm.ingestedMessageCount, getMappingID(), &sm.ingestEnabled,
		&sm.lastFlushedVersion)
	if err != nil {
		return nil, err
	}
	extraSlabInfos[fmt.Sprintf("offsets-bridge-from-%s-%d", streamName, offsetsSlabID)] =
		&SlabInfo{
			StreamName: streamName,
			SlabID:     offsetsSlabID,
			Type:       SlabTypeInternal,
			Schema:     bf.OutSchema(),
		}
	wmType, wmLateness, wmIdleTimeout, err := defaultWatermarkArgs(op.WatermarkType, op.WatermarkLateness,
		op.WatermarkIdleTimeout, op)
	if err != nil {
		return nil, err
	}
	watermarkOperator := NewWaterMarkOperator(bf.InSchema(), wmType, wmLateness, wmIdleTimeout)
	bf.watermarkOperator = watermarkOperator
	sm.bridgeFromOpers[bf] = struct{}{}
	return bf, nil
}

func defaultWatermarkArgs(wmType *string, wmLateness *time.Duration, wmIdleTimeout *time.Duration, op errMsgAtPositionProvider) (string, time.Duration, time.Duration, error) {
	wType := "event_time"
	if wmType != nil {
		wType = *wmType
	}
	lateness := 1 * time.Second
	if wmLateness != nil {
		lateness = *wmLateness
	}
	if wType == "processing_time" && wmIdleTimeout != nil {
		return "", 0, 0, statementErrorAtTokenNamef("watermark_idle_timeout", op,
			"watermark_idle_timeout must not be provided with `processing_time` watermark type")
	}
	idleTimeout := 1 * time.Minute
	if wmIdleTimeout != nil {
		idleTimeout = *wmIdleTimeout
	}
	return wType, lateness, idleTimeout, nil
}

func (sm *streamManager) deployBridgeToOperator(streamName string, op *parser.BridgeToDesc, prevOperator Operator,
	receiverSliceSeqs *sliceSeq, slabSliceSeqs *sliceSeq, extraSlabInfos map[string]*SlabInfo,
	prefixRetentions []slabRetention) (*BridgeToOperator,
	[]slabRetention, *SlabInfo, error) {
	if !verifyKafkaSchema(prevOperator.OutSchema().EventSchema) {
		return nil, nil, nil, statementErrorAtTokenNamef("", op,
			"input to 'bridge to' operator must have column types: [key:bytes, hdrs:bytes, val:bytes]")
	}
	slabID := slabSliceSeqs.GetNextID()

	offsetsSlabID := -1
	hasOffset := HasOffsetColumn(prevOperator.OutSchema().EventSchema)
	if !hasOffset {
		// Need to add an offset in the store operator
		offsetsSlabID = slabSliceSeqs.GetNextID()
		extraSlabInfos[fmt.Sprintf("bridge-to-offsets-%s-%d", streamName, offsetsSlabID)] =
			&SlabInfo{
				StreamName: streamName,
				SlabID:     offsetsSlabID,
				Type:       SlabTypeInternal,
				Schema:     prevOperator.OutSchema(),
			}
	}
	var ret time.Duration
	if op.Retention != nil {
		ret = *op.Retention
	} else {
		ret = 24 * time.Hour
	}
	sso, prefixRetentions, userSlabInfo, err := sm.setupStoreStreamOperator(streamName, ret, prevOperator.OutSchema(),
		slabID, offsetsSlabID, prefixRetentions)
	if err != nil {
		return nil, nil, nil, err
	}
	prefixRetentions = append(prefixRetentions, slabRetention{
		slabID:    slabID,
		Retention: ret,
	})
	backfillReceiverID := receiverSliceSeqs.GetNextID()
	backfillOffsetsSlabID := slabSliceSeqs.GetNextID()
	extraSlabInfos[fmt.Sprintf("bridge-to-back-fill-offsets-%s-%d", streamName, offsetsSlabID)] =
		&SlabInfo{
			StreamName: streamName,
			SlabID:     backfillOffsetsSlabID,
			Type:       SlabTypeInternal,
			Schema:     prevOperator.OutSchema(),
		}
	bfo := NewBackfillOperator(sso.OutSchema(), sm.cfg, slabID, backfillOffsetsSlabID, sm.cfg.MaxBackfillBatchSize, backfillReceiverID, true)
	bt, err := NewBridgeToOperator(op, sso, bfo, sm.messageClientFactory)
	if err != nil {
		return nil, nil, nil, err
	}
	return bt, prefixRetentions, userSlabInfo, nil
}

// We currently use the same constant mapping id for bridge from and kafka in / topic operators and for partitions (unless
// provided in partition config). This means that we know that partition N for any stream always executes on the same
// processor as the same partition-processor mapping will be used for all. This makes things like joins easier as it
// means an extra repartition step doesn't need to occur on inputs, so they end up with the same mapping.
// The down-side is that partitions might not be distributed across processors as evenly as they would if different streams
// used different mapping ids (the [mapping id, partition number] tuple is hashed to get the processor).
// We will, in the future support configuring mapping id on bridge from and kafka in / topic if this is an issue.
func getMappingID() string {
	return "_default_"
}

func (sm *streamManager) deployKafkaInOperator(streamName string, op *parser.KafkaInDesc,
	receiverSliceSeqs *sliceSeq, slabSliceSeqs *sliceSeq, extraSlabInfos map[string]*SlabInfo) (Operator, *KafkaEndpointInfo, error) {
	receiverID := receiverSliceSeqs.GetNextID()
	offsetsSlabID := slabSliceSeqs.GetNextID()
	kafkaIn := NewKafkaInOperator(getMappingID(), offsetsSlabID, receiverID,
		op.Partitions, sm.cfg.KafkaUseServerTimestamp, sm.cfg.ProcessorCount)
	wmType, wmLateness, wmIdleTimeout, err := defaultWatermarkArgs(op.WatermarkType, op.WatermarkLateness,
		op.WatermarkIdleTimeout, op)
	if err != nil {
		return nil, nil, err
	}
	waterMarkOperator := NewWaterMarkOperator(kafkaIn.OutSchema(), wmType, wmLateness, wmIdleTimeout)
	kafkaIn.watermarkOperator = waterMarkOperator
	kafkaEndpointInfo := &KafkaEndpointInfo{
		Name:       streamName,
		InEndpoint: kafkaIn,
		Schema:     kafkaIn.OutSchema(),
	}
	extraSlabInfos[fmt.Sprintf("offsets-kafka-in-%s-%d", streamName, offsetsSlabID)] =
		&SlabInfo{
			StreamName: streamName,
			SlabID:     offsetsSlabID,
			Type:       SlabTypeInternal,
			Schema:     kafkaIn.OutSchema(),
		}
	return kafkaIn, kafkaEndpointInfo, nil
}

func (sm *streamManager) setupStoreStreamOperator(streamName string, retention time.Duration,
	schema *OperatorSchema, slabID int, offsetsSlabID int,
	prefixRetentions []slabRetention) (*StoreStreamOperator, []slabRetention, *SlabInfo, error) {
	tso, err := NewStoreStreamOperator(schema, slabID, offsetsSlabID, sm.cfg.NodeID)
	if err != nil {
		return nil, nil, nil, err
	}
	userSlabInfo := &SlabInfo{
		StreamName:    streamName,
		SlabID:        slabID,
		Schema:        tso.OutSchema(),
		KeyColIndexes: []int{0},
		Type:          SlabTypeUserStream,
	}
	prefixRetentions = append(prefixRetentions, slabRetention{
		slabID:    slabID,
		Retention: retention,
	})
	return tso, prefixRetentions, userSlabInfo, nil
}

func (sm *streamManager) deployStoreStreamOperator(streamName string, op *parser.StoreStreamDesc,
	prevOperator Operator, slabSliceSeqs *sliceSeq, extraSlabInfos map[string]*SlabInfo,
	prefixRetentions []slabRetention) (Operator, []slabRetention, *SlabInfo, error) {
	slabID := slabSliceSeqs.GetNextID()
	offsetsSlabID := -1
	if !HasOffsetColumn(prevOperator.OutSchema().EventSchema) {
		// Need to add an offset in the store operator
		offsetsSlabID = slabSliceSeqs.GetNextID()
		extraSlabInfos[fmt.Sprintf("to-stream-offsets-%s-%d", streamName, offsetsSlabID)] =
			&SlabInfo{
				StreamName: streamName,
				SlabID:     offsetsSlabID,
				Type:       SlabTypeInternal,
				Schema:     prevOperator.OutSchema(),
			}
	}
	ret := time.Duration(0)
	if op.Retention != nil {
		ret = *op.Retention
	}
	return sm.setupStoreStreamOperator(streamName, ret, prevOperator.OutSchema(), slabID, offsetsSlabID, prefixRetentions)
}

func verifyKafkaSchema(eventSchema *evbatch.EventSchema) bool {
	// consumer-endpoint operator requires the input schema to have cols same as KafkaSchema, with or without the offset
	// column. It will add the offset if it doesn't already exist
	hasOffset := HasOffsetColumn(eventSchema)
	var colTypesToMatch []types.ColumnType
	inColTypes := eventSchema.ColumnTypes()
	if hasOffset {
		colTypesToMatch = inColTypes[1:]
	} else {
		colTypesToMatch = inColTypes
	}
	return reflect.DeepEqual(colTypesToMatch, KafkaSchema.ColumnTypes()[1:])
}

func (sm *streamManager) deployKafkaOutOperator(streamName string, op *parser.KafkaOutDesc,
	prevOperator Operator, kafkaEndpointInfo *KafkaEndpointInfo, slabSliceSeqs *sliceSeq,
	extraSlabInfos map[string]*SlabInfo, prefixRetentions []slabRetention) (Operator, []slabRetention, *SlabInfo, error) {

	if !verifyKafkaSchema(prevOperator.OutSchema().EventSchema) {
		return nil, nil, nil, statementErrorAtTokenNamef("", op,
			"input to 'kafka out' operator must have column types: [key:bytes, hdrs:bytes, val:bytes]")
	}

	slabID := slabSliceSeqs.GetNextID()
	offsetsSlabID := -1
	hasOffset := HasOffsetColumn(prevOperator.OutSchema().EventSchema)
	addedOffset := false
	if !hasOffset {
		// Need to add an offset in the store operator
		offsetsSlabID = slabSliceSeqs.GetNextID()
		addedOffset = true
	}
	// The consumer_endpoint operator wraps a to-stream operator, we create that next
	ret := time.Duration(0)
	if op.Retention != nil {
		ret = *op.Retention
	}
	storeStreamOperator, prefixRetentions, userSlabInfo, err := sm.setupStoreStreamOperator(streamName, ret, prevOperator.OutSchema(),
		slabID, offsetsSlabID, prefixRetentions)

	storeOffset := false
	if hasOffset {
		// Already have an offset on the stream, so we need to locate the offset slabID - we need to traverse upstream
		// from this operator until we find a to-stream with addOffset = false or a kafka in
		var err error
		offsetsSlabID, err = sm.findOffsetsSlabID(prevOperator)
		if err != nil {
			return nil, nil, nil, err
		}

		if offsetsSlabID == -1 {
			// There is an incoming offset column but there is no slab where the last generated offset is stored - this
			// would be the case for a bridge from operator followed by a kafka out - here the offset is generated
			// by the external kafka not internally in tektite. the kafka out operator needs the offset slab id so
			// it can load the last offset for a partition - as this is needed by the kafka protocol.
			// in this case we can store the last offset ourselves from inside the kafka out operator
			offsetsSlabID = slabSliceSeqs.GetNextID()
			storeOffset = true
			addedOffset = true
		}
	}
	if addedOffset {
		// We added an offset slanb so we need to add it to the extra slabs so it gets deleted on undeploy
		extraSlabInfos[fmt.Sprintf("kafka-out-offsets-%s-%d", streamName, offsetsSlabID)] =
			&SlabInfo{
				StreamName: streamName,
				SlabID:     offsetsSlabID,
				Type:       SlabTypeInternal,
				Schema:     prevOperator.OutSchema(),
			}
	}
	kafkaOutOper, err := NewKafkaOutOperator(storeStreamOperator, slabID, offsetsSlabID, prevOperator.OutSchema(),
		sm.processorManager, storeOffset)
	if err != nil {
		return nil, nil, nil, err
	}
	updatedExisting := false
	if kafkaEndpointInfo != nil {
		// We have a kafka in on the same stream - make partition mapping and number of partitions are the same
		// otherwise location of producer side partition and consumer side partition could be different - and that's
		// not supported in the kafka cluster state
		s1 := prevOperator.OutSchema()
		s2 := kafkaEndpointInfo.InEndpoint.outSchema
		if s1.Partitions != s2.Partitions || s1.MappingID != s2.MappingID {
			return nil, nil, nil, statementErrorAtTokenNamef("", op, "'kafka in' and 'kafka out' have different partition schemes. is there a partition operator between them?")
		}
		kafkaEndpointInfo.OutEndpoint = kafkaOutOper
		updatedExisting = true
	}
	if !updatedExisting {
		sm.kafkaEndpoints[streamName] = &KafkaEndpointInfo{
			Name:        streamName,
			OutEndpoint: kafkaOutOper,
			Schema:      prevOperator.OutSchema(),
		}
	}
	return kafkaOutOper, prefixRetentions, userSlabInfo, nil
}

func (sm *streamManager) deployPartitionOperator(op *parser.PartitionDesc,
	prevOperator Operator, receiverSliceSeqs *sliceSeq) (Operator, error) {
	receiverID := receiverSliceSeqs.GetNextID()
	var mappingID string
	if op.Mapping == "" {
		mappingID = getMappingID()
	} else {
		mappingID = strings.Trim(op.Mapping, `"`)
	}
	if op.Partitions <= 0 {
		return nil, statementErrorAtTokenNamef("partitions", op, "invalid value for 'partitions' - must be > 0")
	}
	po, err := NewPartitionOperator(prevOperator.OutSchema(), op.KeyExprs, op.Partitions, receiverID, mappingID,
		sm.cfg, op)
	if err != nil {
		return nil, err
	}
	sm.partitionOperators[po] = struct{}{}
	return po, nil
}

func (sm *streamManager) deployAggregateOperator(streamName string, op *parser.AggregateDesc,
	prevOperator Operator, slabSliceSeqs *sliceSeq, receiverSliceSeqs *sliceSeq,
	prefixRetentions []slabRetention, extraSlabInfos map[string]*SlabInfo) (Operator, []slabRetention, *SlabInfo, error) {
	windowed := op.Size != nil
	aggStateSlabID := slabSliceSeqs.GetNextID()
	extraSlabInfos[fmt.Sprintf("aggregate-%s-%d", streamName, aggStateSlabID)] =
		&SlabInfo{
			StreamName: streamName,
			SlabID:     aggStateSlabID,
			Type:       SlabTypeInternal,
			Schema:     prevOperator.OutSchema(),
		}
	openWindowsSlabID := -1
	var size, hop time.Duration
	includeWindowCols := false
	if windowed {
		if op.Hop == nil {
			return nil, nil, nil, statementErrorAtTokenNamef("", op, "'hop' must be specified for a windowed aggregation")
		}
		size = *op.Size
		hop = *op.Hop
		if hop < 1*time.Millisecond {
			return nil, nil, nil, statementErrorAtTokenNamef("hop", op, "'hop' (%s) must be > 0 ms", hop)
		}
		if hop > size {
			return nil, nil, nil, statementErrorAtTokenNamef("hop", op, "'hop' (%s) cannot be greater than 'size' (%s)", hop, size)
		}

		openWindowsSlabID = slabSliceSeqs.GetNextID()
		extraSlabInfos[fmt.Sprintf("open-windows-aggregate-%s-%d", streamName, openWindowsSlabID)] =
			&SlabInfo{
				StreamName: streamName,
				SlabID:     openWindowsSlabID,
				Type:       SlabTypeInternal,
				Schema:     prevOperator.OutSchema(),
			}
		if op.IncludeWindowCols != nil {
			includeWindowCols = *op.IncludeWindowCols
		}
	} else {
		if op.Hop != nil {
			return nil, nil, nil, statementErrorAtTokenNamef("hop", op, "'hop' must not be specified for a non windowed aggregation")
		}
		if op.IncludeWindowCols != nil {
			return nil, nil, nil, statementErrorAtTokenNamef("window_cols", op, "'window_cols' must not be specified for a non windowed aggregation")
		}
	}
	storeResults := true
	if op.Store != nil {
		storeResults = *op.Store
	}

	closedWindowReceiverID := receiverSliceSeqs.GetNextID()

	var lateness time.Duration
	if op.Lateness != nil {
		lateness = *op.Lateness
	}

	var resultsSlabID int
	if windowed {
		if storeResults {
			resultsSlabID = slabSliceSeqs.GetNextID()
			if op.Retention != nil {
				prefixRetentions = append(prefixRetentions, slabRetention{
					slabID:    resultsSlabID,
					Retention: *op.Retention,
				})
			}
		}
	} else {
		if op.Retention != nil {
			prefixRetentions = append(prefixRetentions, slabRetention{
				slabID:    aggStateSlabID,
				Retention: *op.Retention,
			})
		}
	}
	aggOper, err := NewAggregateOperator(prevOperator.OutSchema(), op, aggStateSlabID, openWindowsSlabID, resultsSlabID,
		closedWindowReceiverID, size, hop, lateness, storeResults, includeWindowCols, sm.expressionFactory)
	if err != nil {
		return nil, nil, nil, err
	}
	var userSlab *SlabInfo
	if storeResults {
		var exposedSlabID int
		if windowed {
			exposedSlabID = resultsSlabID
		} else {
			exposedSlabID = aggStateSlabID
		}
		userSlab = &SlabInfo{
			StreamName:    streamName,
			SlabID:        exposedSlabID,
			Schema:        aggOper.outSchema,
			KeyColIndexes: aggOper.outKeyColIndexes,
			Type:          SlabTypeUserTable,
		}
	}
	return aggOper, prefixRetentions, userSlab, nil
}

func (sm *streamManager) deployStoreTableOperator(streamName string, op *parser.StoreTableDesc,
	prevOperator Operator, slabSliceSeqs *sliceSeq,
	prefixRetentions []slabRetention) (Operator, []slabRetention, *SlabInfo, error) {
	slabID := slabSliceSeqs.GetNextID()
	to, err := NewStoreTableOperator(prevOperator.OutSchema(), slabID, op.KeyCols, sm.cfg.NodeID, false, op)
	if err != nil {
		return nil, nil, nil, err
	}
	userSlab := &SlabInfo{
		StreamName:    streamName,
		SlabID:        slabID,
		Schema:        to.OutSchema(),
		KeyColIndexes: to.outKeyCols,
		Type:          SlabTypeUserTable,
	}
	var ret time.Duration
	if op.Retention != nil {
		ret = *op.Retention
	}
	prefixRetentions = append(prefixRetentions, slabRetention{
		slabID:    slabID,
		Retention: ret,
	})
	return to, prefixRetentions, userSlab, nil
}

func (sm *streamManager) deployBackfillOperator(streamName string, operators []Operator,
	receiverSliceSeqs *sliceSeq, slabSliceSeqs *sliceSeq, desc *parser.BackfillDesc, extraSlabInfos map[string]*SlabInfo) (*BackfillOperator, error) {
	// Already verified in validateStream() so this is safe:
	continuation := operators[0].(*ContinuationOperator)
	upstreamStream := continuation.GetParentOperator().GetStreamInfo()

	fromSlab := upstreamStream.UserSlab
	if fromSlab == nil || fromSlab.Type != SlabTypeUserStream {
		return nil, statementErrorAtTokenNamef(upstreamStream.StreamDesc.StreamName, desc,
			"parent stream '%s' must be a stored stream when there is a 'backfill' operator", upstreamStream.StreamDesc.StreamName)
	}
	receiverID := receiverSliceSeqs.GetNextID()
	offsetsSlabID := slabSliceSeqs.GetNextID()
	backfillOper := NewBackfillOperator(fromSlab.Schema, sm.cfg, fromSlab.SlabID, offsetsSlabID,
		sm.cfg.MaxBackfillBatchSize, receiverID, false)
	extraSlabInfos[fmt.Sprintf("offsets-backfill-%s-%d", streamName, offsetsSlabID)] =
		&SlabInfo{
			StreamName: streamName,
			SlabID:     offsetsSlabID,
			Type:       SlabTypeInternal,
			Schema:     backfillOper.OutSchema(),
		}
	return backfillOper, nil
}

func (sm *streamManager) deployJoinOperator(streamName string, op *parser.JoinDesc,
	receiverSliceSeqs *sliceSeq, slabSliceSeqs *sliceSeq, extraSlabInfos map[string]*SlabInfo,
	prefixRetentions []slabRetention) (*JoinOperator, map[string]*SlabInfo, []slabRetention,
	func(info *StreamInfo), error) {
	leftStream, ok := sm.streams[op.LeftStream]
	if !ok {
		return nil, nil, nil, nil, statementErrorAtTokenNamef(op.LeftStream, op, "unknown stream '%s'", op.LeftStream)
	}
	rightStream, ok := sm.streams[op.RightStream]
	if !ok {
		return nil, nil, nil, nil, statementErrorAtTokenNamef(op.RightStream, op, "unknown stream '%s'", op.RightStream)
	}
	leftOper := leftStream.Operators[len(leftStream.Operators)-1]
	rightOper := rightStream.Operators[len(rightStream.Operators)-1]
	leftSchema := leftOper.OutSchema()
	rightSchema := rightOper.OutSchema()
	samePartitionScheme := leftSchema.PartitionScheme.MappingID == rightSchema.PartitionScheme.MappingID &&
		leftSchema.PartitionScheme.Partitions == rightSchema.PartitionScheme.Partitions
	if !samePartitionScheme {
		return nil, nil, nil, nil, statementErrorAtTokenNamef("", op,
			"cannot join '%s' and '%s' directly - they must have same number of partitions and same mapping",
			op.LeftStream, op.RightStream)
	}

	receiverID := receiverSliceSeqs.GetNextID()
	leftSlabID := slabSliceSeqs.GetNextID()
	extraSlabInfos[fmt.Sprintf("join-%s-left", streamName)] =
		&SlabInfo{
			StreamName: streamName,
			SlabID:     leftSlabID,
			Type:       SlabTypeInternal,
			Schema:     leftSchema,
		}
	rightSlabID := slabSliceSeqs.GetNextID()
	extraSlabInfos[fmt.Sprintf("join-%s-right", streamName)] =
		&SlabInfo{
			StreamName: streamName,
			SlabID:     rightSlabID,
			Type:       SlabTypeInternal,
			Schema:     leftSchema,
		}

	if op.LeftIsTable && op.RightIsTable {
		return nil, nil, nil, nil, statementErrorAtTokenNamef("", op, "cannot join a table with a table")
	}
	isStreamTableJoin := op.LeftIsTable || op.RightIsTable

	within := time.Duration(-1)
	if op.Within != nil {
		within = *op.Within
	}

	if isStreamTableJoin && within != -1 {
		return nil, nil, nil, nil, statementErrorAtTokenNamef("within", op, "'within' must not be specified for a stream-table join")
	}
	if !isStreamTableJoin && within == -1 {
		return nil, nil, nil, nil, statementErrorAtTokenNamef("within", op, "'within' must be specified for a stream-stream join")
	}

	// We set retention for the join tables to 2 * within by default, this can be overridden by specify retention on the
	// deployment if required.
	if isStreamTableJoin && op.Retention != nil {
		return nil, nil, nil, nil, statementErrorAtTokenNamef("retention", op, "'retention' must not be specified for a stream-table join")
	}

	var ret time.Duration
	if op.Retention != nil {
		ret = *op.Retention
	} else {
		ret = 2 * within
	}
	if !isStreamTableJoin {
		prefixRetentions = append(prefixRetentions, slabRetention{
			slabID:    leftSlabID,
			Retention: ret,
		})
		prefixRetentions = append(prefixRetentions, slabRetention{
			slabID:    rightSlabID,
			Retention: ret,
		})
	}
	jo, err := NewJoinOperator(leftSlabID, rightSlabID, leftOper, rightOper, op.LeftIsTable, op.RightIsTable, leftStream.UserSlab,
		rightStream.UserSlab, op.JoinElements, within, sm.cfg.NodeID, receiverID, op)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	deferredWiring := func(info *StreamInfo) {
		// Wiring to other streams must only be done once the entire stream has been created OK, otherwise if
		// it fails to deploy on a later operator we can be left with it partially wired up
		info.UpstreamStreamNames[leftStream.StreamDesc.StreamName] = jo.leftInput
		info.UpstreamStreamNames[rightStream.StreamDesc.StreamName] = jo.rightInput

		leftStream.DownstreamStreamNames[streamName] = struct{}{}
		rightStream.DownstreamStreamNames[streamName] = struct{}{}

		if jo.leftInput != nil {
			leftOper.AddDownStreamOperator(jo.leftInput)
		}
		if jo.rightInput != nil {
			rightOper.AddDownStreamOperator(jo.rightInput)
		}

		// Upstream info has changed so need to persist again
		sm.storeStreamMeta(leftStream)
		sm.storeStreamMeta(rightStream)
	}

	return jo, extraSlabInfos, prefixRetentions, deferredWiring, nil
}

func (sm *streamManager) deployUnionOperator(streamName string, desc *parser.UnionDesc, receiverSliceSeqs *sliceSeq) (Operator, func(info *StreamInfo), error) {
	var schema *OperatorSchema
	var inputs []Operator
	var inputInfos []*StreamInfo
	for i, feedingStreamName := range desc.StreamNames {
		stream, ok := sm.streams[feedingStreamName]
		if !ok {
			return nil, nil, statementErrorAtTokenNamef(feedingStreamName, desc, "unknown stream '%s'", feedingStreamName)
		}
		lastOper := stream.Operators[len(stream.Operators)-1]
		inputs = append(inputs, lastOper)
		inputInfos = append(inputInfos, stream)
		thisSchema := lastOper.OutSchema()
		if HasOffsetColumn(thisSchema.EventSchema) {
			// remove offset column - when we compare inputs some may have offset and some may not. as long as all
			// other column types are same, we are good to union.
			thisSchema = &OperatorSchema{
				EventSchema:     evbatch.NewEventSchema(thisSchema.EventSchema.ColumnNames()[1:], thisSchema.EventSchema.ColumnTypes()[1:]),
				PartitionScheme: thisSchema.PartitionScheme,
			}
		}
		if schema != nil {
			samePartitionScheme := schema.PartitionScheme.MappingID == thisSchema.PartitionScheme.MappingID &&
				schema.PartitionScheme.Partitions == thisSchema.PartitionScheme.Partitions
			if !samePartitionScheme {
				return nil, nil, statementErrorAtTokenNamef("", desc,
					"cannot create union - input %d has different number of partitions or different mapping", i)
			}
			if !reflect.DeepEqual(schema.EventSchema.ColumnTypes(), thisSchema.EventSchema.ColumnTypes()) {
				return nil, nil, statementErrorAtTokenNamef("", desc,
					"cannot create union - input %d has different column types or number of columns: %v, expected: %v", i,
					schema.EventSchema.ColumnTypes(), thisSchema.EventSchema.ColumnTypes())
			}
		} else {
			schema = thisSchema
		}
	}
	receiverID := receiverSliceSeqs.GetNextID()
	uo := NewUnionOperator(receiverID, inputs)
	deferredWiring := func(info *StreamInfo) {
		for i, inputInfo := range inputInfos {
			// Wiring to other streams must only be done once the entire stream has been created OK, otherwise if
			// it fails to deploy on a later operator we can be left with it partially wired up
			inputName := inputInfo.StreamDesc.StreamName
			info.UpstreamStreamNames[inputName] = uo
			inputInfo.DownstreamStreamNames[streamName] = struct{}{}
			inputs[i].AddDownStreamOperator(uo)
			// Upstream info has changed so need to persist again
			sm.storeStreamMeta(inputInfo)
		}
	}
	return uo, deferredWiring, nil
}

func (sm *streamManager) findOffsetsSlabID(oper Operator) (int, error) {
	switch o := oper.(type) {
	case *StoreStreamOperator:
		if o.addOffset {
			return o.offsetsSlabID, nil
		}
	case *KafkaInOperator:
		return o.offsetsSlabID, nil
	case *BridgeFromOperator:
		return -1, nil
	case *PartitionOperator:
		return -1, nil
	default:
		parent := oper.GetParentOperator()
		if parent != nil {
			return sm.findOffsetsSlabID(parent)
		}
	}
	return -1, nil
}

func (sm *streamManager) Loaded() {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	for _, pInfo := range sm.streams {
		ops := pInfo.Operators
		for _, op := range ops {
			if err := op.Setup(sm); err != nil {
				log.Errorf("failure in operator setup %v", err)
			}
		}
	}
	sm.calculateInjectableReceivers()
	sm.loaded = true
	// We must call AfterReceiverChange to invalidate processor manager and processor cached receiver info state
	// otherwise correct barriers might not be injected
	// must be called with lock held for consistent versions
	sm.processorManager.AfterReceiverChange()
}

func (sm *streamManager) UndeployStream(deleteStreamDesc parser.DeleteStreamDesc, commandID int64) error {
	sm.shutdownLock.Lock()
	defer sm.shutdownLock.Unlock()
	if sm.shuttingDown {
		return errors.NewTektiteErrorf(errors.ShutdownError, "cluster is shutting down")
	}
	sm.lock.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			sm.lock.Unlock()
		}
	}()

	log.Debugf("%s: node: %d undeploying stream %s", sm.cfg.LogScope, sm.cfg.NodeID, deleteStreamDesc.StreamName)

	// First we look up the stream info
	info, ok := sm.streams[deleteStreamDesc.StreamName]
	if !ok {
		return statementErrorAtTokenNamef(deleteStreamDesc.StreamName, &deleteStreamDesc, "unknown stream '%s'", deleteStreamDesc.StreamName)
	}
	if info.Undeploying {
		return errors.NewTektiteErrorf(errors.InternalError, "stream is already beiung undeployed")
	}
	if len(info.DownstreamStreamNames) > 0 {
		var dsNames []string
		for dsName := range info.DownstreamStreamNames {
			dsNames = append(dsNames, dsName)
		}
		sort.Strings(dsNames)
		return statementErrorAtTokenNamef(deleteStreamDesc.StreamName, &deleteStreamDesc,
			"cannot delete stream %s - it has child streams: %v - they must be deleted first",
			deleteStreamDesc.StreamName, dsNames)
	}
	info.Undeploying = true

	var tearDownChan chan error
	if sm.loaded {
		tearDownChan = make(chan error, 1)
		cf := common.NewCountDownFuture(len(info.Operators), func(err error) {
			tearDownChan <- err
		})
		for _, oper := range info.Operators {
			oper.Teardown(sm, cf.CountDown)
		}
	}
	if len(info.DownstreamStreamNames) > 0 {
		var dsNames []string
		for dsName := range info.DownstreamStreamNames {
			dsNames = append(dsNames, dsName)
		}
		sort.Strings(dsNames)
		return statementErrorAtTokenNamef(deleteStreamDesc.StreamName, &deleteStreamDesc,
			"cannot delete stream %s - it has child streams: %v - they must be deleted first",
			deleteStreamDesc.StreamName, dsNames)
	}
	delete(sm.streams, deleteStreamDesc.StreamName)
	for upstreamStreamName, oper := range info.UpstreamStreamNames {
		upstream, ok := sm.streams[upstreamStreamName]
		if !ok {
			panic("cannot find upstream")
		}
		delete(upstream.DownstreamStreamNames, deleteStreamDesc.StreamName)
		if oper != nil {
			upstream.Operators[len(upstream.Operators)-1].RemoveDownStreamOperator(oper)
		}
	}
	delete(sm.kafkaEndpoints, info.StreamDesc.StreamName)
	kafkaInOper, ok := info.Operators[0].(*BridgeFromOperator)
	if ok {
		delete(sm.bridgeFromOpers, kafkaInOper)
	}
	partitionOper, ok := info.Operators[0].(*PartitionOperator)
	if ok {
		delete(sm.partitionOperators, partitionOper)
	}
	// Now delete the data from the store
	if info.UserSlab != nil {
		sm.deleteSlab(info.UserSlab)
		sm.unregisterSlabRetention(info.UserSlab.SlabID)
	}
	for _, slabInfo := range info.ExtraSlabs {
		sm.deleteSlab(slabInfo)
		sm.unregisterSlabRetention(slabInfo.SlabID)
	}
	sm.invalidateCachedInfo()
	sm.deleteStreamMeta(deleteStreamDesc.StreamName)
	if sm.loaded {
		sm.calculateInjectableReceivers()
	}
	sm.callChangeListeners(deleteStreamDesc.StreamName, false)
	sm.lastCommandID = commandID
	if sm.loaded {
		// Note, this must be called with the stream manager lock held to ensure that barriers don't get injected
		// with stale receivers after a stream has been deployed - otherwise we could have data flowing through
		// the newly updated graph but barriers only injected in some of it. If the version then completed it would
		// be invalid
		sm.processorManager.AfterReceiverChange()
	}
	sm.lock.Unlock()
	unlocked = true
	if tearDownChan != nil {
		// Wait for teardown to complete outside lock
		return <-tearDownChan
	}
	return nil
}

func (sm *streamManager) unregisterSlabRetention(slabID int) {
	if err := sm.slabRetentions.UnregisterSlabRetention(slabID); err != nil {
		log.Warnf("failed to unregister slab retention %d", slabID)
	}
}

func (sm *streamManager) deleteSlab(slabInfo *SlabInfo) {
	// We send a batch to each processor that the slab uses. On receipt it will write the tombstones
	// Note - that we will do this on each node of the cluster so we will have one tombstone and one end marker per
	// node per partition. We only need one per partition, but it doesn't matter having too many.
	log.Debugf("deleting slab %d partitions %d lp %d", slabInfo.SlabID, slabInfo.Schema.Partitions, len(slabInfo.Schema.PartitionProcessorMapping))

	for procID, partIDs := range slabInfo.Schema.ProcessorPartitionMapping {
		colBuilders := evbatch.CreateColBuilders(deleteSlabSchema.ColumnTypes())

		for _, partID := range partIDs {
			// Create a tombstone for the partition id
			partitionHash := proc.CalcPartitionHash(slabInfo.Schema.MappingID, uint64(partID))

			// Sanity check
			expectedProcID := proc.CalcProcessorForHash(partitionHash, sm.cfg.ProcessorCount)
			if expectedProcID != procID {
				panic("invalid processor")
			}

			tombstone := encoding.EncodeEntryPrefix(partitionHash, uint64(slabInfo.SlabID), 24)
			// And we create an end marker - the end marker is the next slab id + a zero so as not to conflict with
			// any tombstone on the next partition
			// We write an end marker so that the sstable to be pushed will have a prefix tombstone and the end marker
			// in the same sstable. When this gets merged into L1 it will overlap with all sstables in L1 with same partition
			// and will delete all those entries, leaving just the tombstone and the end marker, and that will repeat through
			// the levels of the database
			endMarker := encoding.EncodeEntryPrefix(partitionHash, uint64(slabInfo.SlabID)+1, 25)
			endMarker = append(endMarker, 0)

			// prefix delete tombstones and end markers are not versioned and have special version math.MaxUint64 which
			// identifies them in MergingIterator
			colBuilders[0].(*evbatch.BytesColBuilder).Append(encoding.EncodeVersion(tombstone, math.MaxUint64))
			colBuilders[1].(*evbatch.BytesColBuilder).AppendNull()
			colBuilders[0].(*evbatch.BytesColBuilder).Append(encoding.EncodeVersion(endMarker, math.MaxUint64))
			colBuilders[1].(*evbatch.BytesColBuilder).Append([]byte{'x'}) // value doesn't matter but can't be null
		}

		eventBatch := evbatch.NewBatchFromBuilders(deleteSlabSchema, colBuilders...)
		// Set partition to be the first one - doesn't matter as long as it hits the correct processor
		batch := proc.NewProcessBatch(procID, eventBatch, common.DeleteSlabReceiverID, partIDs[0], -1)

		sm.processorManager.ForwardBatch(batch, false, func(err error) {
			if err != nil {
				log.Warnf("%s: failed to write tombstones for deleting slab %d %v", sm.cfg.LogScope, slabInfo.SlabID, err)
			}
		})
	}
}

type deleteSlabReceiver struct {
	schema *OperatorSchema
	sm     *streamManager
}

func (d *deleteSlabReceiver) InSchema() *OperatorSchema {
	return d.schema
}

func (d *deleteSlabReceiver) OutSchema() *OperatorSchema {
	panic("should not be called")
}

func (d *deleteSlabReceiver) ForwardingProcessorCount() int {
	panic("should not be called")
}

func (d *deleteSlabReceiver) ReceiveBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	// Write the tombstones and endMarkers - they must be written from the processor that owns the partition hashes
	// hence using a receiver to do this.
	for i := 0; i < batch.RowCount; i++ {
		key := batch.GetBytesColumn(0).Get(i)
		var value []byte
		valCol := batch.GetBytesColumn(1)
		if !valCol.IsNull(i) {
			value = valCol.Get(i)
		}
		// Need to copy otherwise encoding version will overwrite original slice in batch and corrupt next entry
		execCtx.StoreEntry(common.KV{
			Key:   key,
			Value: value,
		}, true)
	}
	return nil, nil
}

func (d *deleteSlabReceiver) ReceiveBarrier(execCtx StreamExecContext) error {
	return nil
}

func (d *deleteSlabReceiver) RequiresBarriersInjection() bool {
	return false
}

func (sm *streamManager) RegisterSystemSlab(slabName string, persistorReceiverID int, deleterReceiverID int,
	slabID int, schema *OperatorSchema, keyCols []string, noCache bool) error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	to, err := NewStoreTableOperator(schema, slabID, keyCols, -1, noCache, nil)
	if err != nil {
		return err
	}
	sm.receivers[persistorReceiverID] = &sysTableReceiver{schema: schema, oper: to}
	if deleterReceiverID != -1 {
		del, err := NewRowDeleteOperator(schema, slabID, to.inKeyCols)
		if err != nil {
			return err
		}
		sm.receivers[deleterReceiverID] = &sysTableReceiver{oper: del, schema: del.schema}
	}
	var keyColIndexes []int
	for _, keyCol := range keyCols {
		for i, colName := range schema.EventSchema.ColumnNames() {
			if keyCol == colName {
				keyColIndexes = append(keyColIndexes, i)
			}
		}
	}
	slabInfo := &SlabInfo{
		SlabID:        slabID,
		Schema:        schema,
		KeyColIndexes: keyColIndexes,
		Type:          SlabTypeQueryableInternal,
	}
	streamInfo := &StreamInfo{
		StreamDesc:   parser.CreateStreamDesc{StreamName: slabName},
		Operators:    []Operator{to},
		UserSlab:     slabInfo,
		SystemStream: true,
	}
	sm.streams[slabName] = streamInfo
	sm.sysStreamCount++
	return nil
}

const sysStreamName = "sys.streams"

var sysStreamsPartitionScheme = PartitionScheme{
	MappingID:                 sysStreamName,
	Partitions:                1,
	ProcessorIDs:              []int{0},
	PartitionProcessorMapping: map[int]int{0: 0},
	ProcessorPartitionMapping: map[int][]int{0: {0}},
}

func (sm *streamManager) Start() error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	sm.streams[sysStreamName] = &StreamInfo{
		UserSlab: &SlabInfo{
			StreamName: sysStreamName,
			SlabID:     common.StreamMetaSlabID,
			Schema: &OperatorSchema{
				EventSchema:     streamSchema,
				PartitionScheme: sysStreamsPartitionScheme,
			},
			KeyColIndexes: []int{0},
			Type:          SlabTypeQueryableInternal,
		},
		SystemStream: true,
		StreamMeta:   true,
	}
	sm.sysStreamCount++
	return nil
}

func (sm *streamManager) Stop() error {
	sm.lock.Lock()
	var opers []Operator
	for _, info := range sm.streams {
		for _, oper := range info.Operators {
			opers = append(opers, oper)
		}
	}
	delete(sm.streams, sysStreamName)
	ch := make(chan error, 1)
	cf := common.NewCountDownFuture(len(opers), func(err error) {
		ch <- err
	})
	for _, oper := range opers {
		oper.Teardown(sm, cf.CountDown)
	}
	sm.lock.Unlock()
	// We must wait for tearDown completion outside lock
	return <-ch
}

type sysTableReceiver struct {
	schema *OperatorSchema
	oper   Operator
}

func (s *sysTableReceiver) ReceiveBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	execCtx.CheckInProcessorLoop()
	return s.oper.HandleStreamBatch(batch, execCtx)
}

func (s *sysTableReceiver) ReceiveBarrier(StreamExecContext) error {
	panic("not used")
}

func (s *sysTableReceiver) InSchema() *OperatorSchema {
	return s.schema
}

func (s *sysTableReceiver) OutSchema() *OperatorSchema {
	return s.schema
}

func (s *sysTableReceiver) ForwardingProcessorCount() int {
	return len(s.schema.PartitionScheme.ProcessorIDs)
}

func (s *sysTableReceiver) RequiresBarriersInjection() bool {
	return false
}

func (sm *streamManager) GetStream(name string) *StreamInfo {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	return sm.streams[name]
}

func (sm *streamManager) GetAllStreams() []*StreamInfo {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	allStreams := make([]*StreamInfo, 0, len(sm.streams))
	for _, info := range sm.streams {
		allStreams = append(allStreams, info)
	}
	return allStreams
}

func (sm *streamManager) GetKafkaEndpoint(name string) *KafkaEndpointInfo {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	return sm.kafkaEndpoints[name]
}

func (sm *streamManager) GetAllKafkaEndpoints() []*KafkaEndpointInfo {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	var endpoints []*KafkaEndpointInfo
	for _, endpoint := range sm.kafkaEndpoints {
		endpoints = append(endpoints, endpoint)
	}
	return endpoints
}

func (sm *streamManager) RegisterReceiverWithLock(id int, receiver Receiver) {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	sm.RegisterReceiver(id, receiver)
}

func (sm *streamManager) RegisterReceiver(id int, receiver Receiver) {
	if receiver == nil {
		panic("cannot register nil receiver")
	}
	sm.receivers[id] = receiver
}

func (sm *streamManager) UnregisterReceiver(id int) {
	delete(sm.receivers, id)
}

func (sm *streamManager) ProcessorManager() ProcessorManager {
	return sm.processorManager
}

func (sm *streamManager) numStreams() int {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	return len(sm.streams)
}

// BatchHandler implementation

func (sm *streamManager) HandleProcessBatch(processor proc.Processor, processBatch *proc.ProcessBatch,
	_ bool) (bool, *mem.Batch, []*proc.ProcessBatch, error) {
	sm.lock.RLock()
	defer sm.lock.RUnlock()
	receiver, ok := sm.receivers[processBatch.ReceiverID]
	if !ok {
		if !processBatch.Barrier {
			// normal to get these for a barrier shortly after undeploy so we don't log in that case
			log.Warnf("cannot handle batch in stream manager - no receiver registered with id %d",
				processBatch.ReceiverID)
		}
		// Returning an error - if it's a forwarded batch the sender will retry - this can occur if batches are forwarded
		// right after stream creation, stream might not have been fully deployed yet on receiving node.
		return false, nil, nil, nil
	}
	var eventSchema *evbatch.EventSchema
	receiverInSchema := receiver.InSchema()
	// receiverInSchema can be nil in the case of forwarding a batch in a join for processing.
	if receiverInSchema != nil {
		eventSchema = receiverInSchema.EventSchema
	}
	processBatch.CheckDeserializeEvBatch(eventSchema)
	ec := sm.newExecContext(processBatch, processor)

	if processBatch.Barrier {
		// We set the last executed command id on the barrier. The command id is propagated as barriers are forwarded
		// and passed to the version manager when the version completes. The version manager will ensure the command
		// id is the same for all processors completing a version. In this way we can guarantee if we complete a version
		// ok then barriers have been sent through the graph as of the same command id and therefore the version
		// is consistent. If version manager receives different command id then the version is doomed and advanced.
		processBatch.CommandID = int(sm.lastCommandID)
		err := receiver.ReceiveBarrier(ec)
		if err != nil {
			return false, nil, nil, err
		}
		return true, ec.entries, ec.GetForwardBarriers(), nil
	} else {
		_, err := receiver.ReceiveBatch(processBatch.EvBatch, ec)
		if err != nil {
			return false, nil, nil, err
		}
		return true, ec.entries, ec.GetForwardBatches(), nil
	}
}

func (sm *streamManager) GetForwardingProcessorCount(receiverID int) (int, bool) {
	sm.lock.RLock()
	defer sm.lock.RUnlock()
	receiver, ok := sm.receivers[receiverID]
	if !ok {
		return 0, false
	}
	return receiver.ForwardingProcessorCount(), true
}

/*
GetRequiredCompletions returns the number of completions required to complete a version.
A completion is sent once a barrier reaches a receiver that doesn't forward the barrier any further.
To compute this number we must compute all the receivers which do not forward any further - we call these
"terminal receivers". We then sum the number of processors that each of the terminal receivers run on - as
each processor will send a different completion.
*/
func (sm *streamManager) GetRequiredCompletions() int {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if sm.requiredCompletions != -1 {
		return sm.requiredCompletions
	}

	// First we create a tree of the roots and any operators that forward barriers to other processors, e.g.
	// partition, join etc
	tree := &partitionTree{}
	for _, receiver := range sm.receivers {
		if receiver.RequiresBarriersInjection() {
			// top level operator (kafka_in)
			oper, ok := receiver.(Operator)
			if ok {
				buildPartitionTree(oper, tree)
			}
		}
	}

	requiredCompletions := 0
	if len(tree.children) > 0 {

		// Then we find the terminal operator schemas
		terminalOperators := findTerminalOperators(tree)

		// Then we sum the processor counts
		for _, oper := range terminalOperators {
			requiredCompletions += len(oper.OutSchema().PartitionScheme.ProcessorIDs)
		}
	}

	// We add one for the dummy receiver - this ensures we always have at least one receiver so versions can always
	// complete
	sm.requiredCompletions = requiredCompletions + 1

	return sm.requiredCompletions
}

func buildPartitionTree(operator Operator, tree *partitionTree) {
	switch t := operator.(type) {
	// These operators are either top level, or forward batches to other processors
	case *BridgeFromOperator, *KafkaInOperator, *PartitionOperator, *JoinOperator, *UnionOperator:
		// Insert a new node in the tree
		subTree := &partitionTree{}
		subTree.operator = t
		tree.children = append(tree.children, subTree)
		tree = subTree
	}
	for _, child := range operator.GetDownStreamOperators() {
		buildPartitionTree(child, tree)
	}
}

type partitionTree struct {
	operator Operator
	children []*partitionTree
}

func findTerminalOperators(tree *partitionTree) []Operator {
	termOps := recurseFindTerminalOperators(tree)
	// If there are joins or unions an operator could appear more than once, so we remove duplicates
	terminalOperators := map[Operator]struct{}{}
	for _, op := range termOps {
		terminalOperators[op] = struct{}{}
	}
	termOpsSlice := make([]Operator, 0, len(terminalOperators))
	for termOp := range terminalOperators {
		termOpsSlice = append(termOpsSlice, termOp)
	}
	return termOpsSlice
}

func recurseFindTerminalOperators(tree *partitionTree) []Operator {
	if len(tree.children) == 0 {
		return []Operator{tree.operator}
	}
	var schemas []Operator
	for _, child := range tree.children {
		schemas = append(schemas, recurseFindTerminalOperators(child)...)
	}
	return schemas
}

func (sm *streamManager) calculateInjectableReceivers() {
	injectableReceivers := map[int][]int{}
	for id, receiver := range sm.receivers {
		if receiver.RequiresBarriersInjection() {
			for _, procID := range receiver.OutSchema().PartitionScheme.ProcessorIDs {
				injectableReceivers[procID] = append(injectableReceivers[procID], id)
			}
		}

	}
	sm.injectableReceivers.Store(&injectableReceivers)
}

func (sm *streamManager) GetInjectableReceivers(processorID int) []int {
	// Note that this method must not lock on the stream manager to avoid a deadlock situation when deploying
	// a stream, where the stream mgr is locked and attempts to lock the processor manager to register a listener
	// and when handling a version broadcast which locks the processor manager then attempts to lock stream manager
	// when calling this method.
	receivers := *sm.injectableReceivers.Load()
	return receivers[processorID]
}

func (sm *streamManager) invalidateCachedInfo() {
	sm.requiredCompletions = -1
}

func (sm *streamManager) StopIngest() error {
	sm.lock.Lock()
	log.Debugf("node %d stream manager stop ingest", sm.cfg.NodeID)
	sm.ingestEnabled.Store(false)
	var fkOpers []*BridgeFromOperator
	for fk := range sm.bridgeFromOpers {
		fkOpers = append(fkOpers, fk)
	}
	var partOpers []*PartitionOperator
	for po := range sm.partitionOperators {
		partOpers = append(partOpers, po)
	}
	sm.lock.Unlock()
	// We must call stopIngest outside lock to avoid a deadlock, where we stop ingest which waits for message consumers
	// to stop, but message consumer could be waiting for ingest to complete, but the processor is blocked trying to
	// call HandleProcessBatch on the stream manager (which gets read lock)
	for _, fk := range fkOpers {
		fk.stopIngest()
	}
	return nil
}

func (sm *streamManager) StartIngest(version int) error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	log.Debugf("node %d stream manager start ingest, version %d", sm.cfg.NodeID, version)
	sm.ingestEnabled.Store(true)
	atomic.StoreInt64(&sm.lastFlushedVersion, int64(version))
	for fk := range sm.bridgeFromOpers {
		if err := fk.startIngest(int64(version)); err != nil {
			return err
		}
	}
	return nil
}

func (sm *streamManager) Dump() {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	log.Infof("dumping stream manager on node %d", sm.cfg.NodeID)
	for _, p := range sm.streams {
		log.Infof("stream: %s: %v", p.StreamDesc.StreamName, p)
	}
}

// storeStreamMeta - stores the stream info in a mem table, so it can be queried like any other table.
// note this is not a real persistent table
func (sm *streamManager) storeStreamMeta(info *StreamInfo) {
	// convert to batch
	builders := evbatch.CreateColBuilders(streamSchema.ColumnTypes())
	builders[0].(*evbatch.StringColBuilder).Append(info.StreamDesc.StreamName)
	builders[1].(*evbatch.StringColBuilder).Append(ExtractStreamDefinition(info.Tsl))
	if info.InSchema == nil {
		builders[2].AppendNull()
		builders[3].AppendNull()
		builders[4].AppendNull()
	} else {
		builders[2].(*evbatch.StringColBuilder).Append(info.InSchema.EventSchema.String())
		builders[3].(*evbatch.IntColBuilder).Append(int64(info.InSchema.Partitions))
		builders[4].(*evbatch.StringColBuilder).Append(info.InSchema.MappingID)
	}
	builders[5].(*evbatch.StringColBuilder).Append(info.OutSchema.EventSchema.String())
	builders[6].(*evbatch.IntColBuilder).Append(int64(info.OutSchema.Partitions))
	builders[7].(*evbatch.StringColBuilder).Append(info.OutSchema.MappingID)
	var childSlice []string
	for childStream := range info.DownstreamStreamNames {
		// sort them so output is deterministic
		childSlice = append(childSlice, childStream)
	}
	if len(childSlice) > 0 {
		sort.Strings(childSlice)
		var childBuilder strings.Builder
		for i, childStream := range childSlice {
			childBuilder.WriteString(childStream)
			if i != len(childSlice)-1 {
				childBuilder.WriteString(", ")
			}
		}
		builders[8].(*evbatch.StringColBuilder).Append(childBuilder.String())
	} else {
		builders[8].AppendNull()
	}
	batch := evbatch.NewBatchFromBuilders(streamSchema, builders...)
	prefix := createTableKeyPrefix(sysStreamsPartitionScheme.MappingID, common.StreamMetaSlabID, 0, 64)
	keyBuff := evbatch.EncodeKeyCols(batch, 0, []int{0}, prefix)
	keyBuff = encoding.EncodeVersion(keyBuff, 0) // not versioned
	rowBuff := make([]byte, 0, rowInitialBufferSize)
	rowBuff = evbatch.EncodeRowCols(batch, 0, []int{1, 2, 3, 4, 5, 6, 7, 8}, rowBuff)
	sm.streamMemStore.Put(string(keyBuff), rowBuff)
}

func (sm *streamManager) deleteStreamMeta(streamName string) {
	keyBuff := createTableKeyPrefix(sysStreamsPartitionScheme.MappingID, common.StreamMetaSlabID, 0, 64)
	keyBuff = append(keyBuff, 1)
	keyBuff = encoding.KeyEncodeString(keyBuff, streamName)
	keyBuff = encoding.EncodeVersion(keyBuff, 0) // not versioned
	log.Debugf("deleting stream meta for stream %s with key: %v", streamName, keyBuff)
	sm.streamMemStore.Remove(string(keyBuff))
}

func (sm *streamManager) streamMetaIterator(startKey []byte, endKey []byte) iteration.Iterator {
	sm.lock.RLock()
	defer sm.lock.RUnlock()
	var entries []common.KV
	iter := sm.streamMemStore.Iterator()
	for iter.Next() {
		key := []byte(iter.Key().(string))
		value := iter.Value().([]byte)
		if (startKey == nil || bytes.Compare(key, startKey) >= 0) && (endKey == nil || bytes.Compare(key, endKey) < 0) {
			entries = append(entries, common.KV{
				Key:   key,
				Value: value,
			})
		}
	}
	return iteration.NewStaticIterator(entries)
}

func (sm *streamManager) StreamMetaIteratorProvider() *StreamMetaIteratorProvider {
	return sm.streamMetaIterProvider
}

type StreamMetaIteratorProvider struct {
	pm *streamManager
}

func (s *StreamMetaIteratorProvider) NewIterator(keyStart []byte, keyEnd []byte, _ uint64, _ bool) (iteration.Iterator, error) {
	return s.pm.streamMetaIterator(keyStart, keyEnd), nil
}

func ExtractStreamDefinition(tsl string) string {
	if tsl == "" {
		return ""
	}
	// remove the "<stream_name> := " part from the tsl
	pos := strings.Index(tsl, "=") + 1
	for unicode.IsSpace(rune(tsl[pos])) {
		pos++
	}
	def := tsl[pos:]
	return replaceWhitespaceRuns(def)
}

func replaceWhitespaceRuns(in string) string {
	var sb strings.Builder
	lastWhitespace := false
	inQuotes := false
	for _, r := range in {
		whitespace := unicode.IsSpace(r)
		if whitespace && !inQuotes {
			if !lastWhitespace {
				sb.WriteRune(' ')
			}
			lastWhitespace = true
		} else {
			sb.WriteRune(r)
			lastWhitespace = false
			quote := r == '"'
			if quote {
				inQuotes = !inQuotes
			}
		}
	}
	return sb.String()
}

type forwardBarrierInfo struct {
	receiverID  int
	processorID int
}

type buildersInfo struct {
	builders    []evbatch.ColumnBuilder
	evSchema    *evbatch.EventSchema
	processorID int
}

func (sm *streamManager) newExecContext(processBatch *proc.ProcessBatch, processor proc.Processor) *execContext {
	return &execContext{
		processBatch: processBatch,
		processor:    processor,
	}
}

type execContext struct {
	processBatch *proc.ProcessBatch
	processor    proc.Processor
	entries      *mem.Batch
	// map[partition_id]->(map[receiver_id]->builders)
	partitionBuilders map[int]map[int]buildersInfo
	numForwardBatches int
	forwardBarriers   []forwardBarrierInfo
}

func (e *execContext) CheckInProcessorLoop() {
	e.processor.CheckInProcessorLoop()
}

func (e *execContext) BackFill() bool {
	return e.processBatch.BackFill
}

func (e *execContext) Get(key []byte) ([]byte, error) {
	// First look in cache
	val, exists := e.processor.WriteCache().Get(key)
	if exists {
		return val, nil
	}
	// Then look in store
	return e.processor.Get(key)
}

func (e *execContext) StoreEntry(kv common.KV, noCache bool) {
	if noCache {
		if e.entries == nil {
			e.entries = mem.NewBatch()
		}
		e.entries.AddEntry(kv)
	} else {
		e.processor.WriteCache().Put(kv)
	}
}

func (e *execContext) ForwardEntry(processorID int, receiverID int, remotePartitionID int, rowIndex int, batch *evbatch.Batch,
	schema *evbatch.EventSchema) {
	if e.partitionBuilders == nil {
		e.partitionBuilders = make(map[int]map[int]buildersInfo, 48)
	}
	receiverBuilders, ok := e.partitionBuilders[remotePartitionID]
	if !ok {
		receiverBuilders = map[int]buildersInfo{}
		e.partitionBuilders[remotePartitionID] = receiverBuilders
	}
	info, ok := receiverBuilders[receiverID]
	if !ok {
		builders := evbatch.CreateColBuilders(schema.ColumnTypes())
		info = buildersInfo{
			builders:    builders,
			evSchema:    schema,
			processorID: processorID,
		}
		receiverBuilders[receiverID] = info
		e.numForwardBatches++
	}
	for i, fType := range schema.ColumnTypes() {
		evbatch.CopyColumnEntry(fType, info.builders, i, rowIndex, batch)
	}
}

func (e *execContext) ForwardBarrier(processorID int, receiverID int) {
	e.forwardBarriers = append(e.forwardBarriers, forwardBarrierInfo{
		receiverID:  receiverID,
		processorID: processorID,
	})
}

func (e *execContext) GetForwardBatches() []*proc.ProcessBatch {
	if e.partitionBuilders == nil {
		return nil
	}
	pbArr := make([]*proc.ProcessBatch, 0, e.numForwardBatches)
	for remotePartitionID, receiverBatches := range e.partitionBuilders {
		for receiverID, info := range receiverBatches {
			batch := evbatch.NewBatchFromBuilders(info.evSchema, info.builders...)
			pb := proc.NewProcessBatch(info.processorID, batch, receiverID, remotePartitionID, e.Processor().ID())
			pbArr = append(pbArr, pb)
		}
	}
	return pbArr
}

func (e *execContext) GetForwardBarriers() []*proc.ProcessBatch {
	var barriers []*proc.ProcessBatch
	for _, info := range e.forwardBarriers {
		barrier := proc.NewBarrierProcessBatch(info.processorID, info.receiverID, e.processBatch.Version, e.processBatch.Watermark,
			e.processor.ID(), e.processBatch.CommandID)
		barriers = append(barriers, barrier)
	}
	return barriers
}

func (e *execContext) WriteVersion() int {
	return e.processBatch.Version
}

func (e *execContext) PartitionID() int {
	return e.processBatch.PartitionID
}

func (e *execContext) ForwardSequence() int {
	return e.processBatch.ForwardSequence
}

func (e *execContext) ForwardingProcessorID() int {
	return e.processBatch.ForwardingProcessorID
}

func (e *execContext) Processor() proc.Processor {
	return e.processor
}

func (e *execContext) WaterMark() int {
	return e.processBatch.Watermark
}

func (e *execContext) SetWaterMark(waterMark int) {
	e.processBatch.Watermark = waterMark
}

func (e *execContext) EventBatchBytes() []byte {
	return e.processBatch.EvBatchBytes
}

func (e *execContext) ReceiverID() int {
	return e.processBatch.ReceiverID
}

type sliceSeq struct {
	seqs []int
	pos  int
}

func (s *sliceSeq) GetNextID() int {
	id := s.seqs[s.pos]
	s.pos++
	return id
}

// The dummy receiver is used to ensure there is always at least one injectable receiver in the stream manager
// this is necessary to ensure barriers are injected and snapshot versions complete. Otherwise, if all streams
// are deleted and version has not completed, then written data can remain in an uncompleted version which will
// not be visible or flushed to cloud storage
func newDummyReceiver() Receiver {
	evSchema := evbatch.NewEventSchema([]string{"x"}, []types.ColumnType{types.ColumnTypeInt})
	opSchema := &OperatorSchema{
		EventSchema: evSchema,
		PartitionScheme: PartitionScheme{
			MappingID:    "dummy",
			Partitions:   1,
			ProcessorIDs: []int{0},
		},
	}
	return &dummyReceiver{opSchema: opSchema}
}

type dummyReceiver struct {
	opSchema *OperatorSchema
}

func (d *dummyReceiver) InSchema() *OperatorSchema {
	return d.opSchema
}

func (d *dummyReceiver) OutSchema() *OperatorSchema {
	return d.opSchema
}

func (d *dummyReceiver) ForwardingProcessorCount() int {
	return len(d.opSchema.PartitionScheme.ProcessorIDs)
}

func (d *dummyReceiver) ReceiveBatch(*evbatch.Batch, StreamExecContext) (*evbatch.Batch, error) {
	return nil, nil
}

func (d *dummyReceiver) ReceiveBarrier(StreamExecContext) error {
	return nil
}

func (d *dummyReceiver) RequiresBarriersInjection() bool {
	return true
}

func isReservedIdentifierName(name string) bool {
	_, reserved := reservedIdentifierNames[name]
	return reserved
}

type errMsgAtPositionProvider interface {
	ErrorMsgAtToken(msg string, tokenVal string) string
	ErrorMsgAtPosition(msg string, position lexer.Position) string
}

func statementErrorAtTokenNamef(tokenName string, provider errMsgAtPositionProvider, msg string, args ...interface{}) error {
	if provider == nil {
		return errors.NewStatementError(fmt.Sprintf(msg, args...))
	}
	msg = fmt.Sprintf(msg, args...)
	msg = provider.ErrorMsgAtToken(msg, tokenName)
	return errors.NewStatementError(msg)
}

func statementErrorAtPositionf(token lexer.Token, provider errMsgAtPositionProvider, msg string, args ...interface{}) error {
	if provider == nil {
		return errors.NewStatementError(fmt.Sprintf(msg, args...))
	}
	msg = fmt.Sprintf(msg, args...)
	msg = provider.ErrorMsgAtPosition(msg, token.Pos)
	return errors.NewStatementError(msg)
}

type TestSourceDesc struct {
	ColumnNames []string
	ColumnTypes []types.ColumnType
	Partitions  int
}

func (t *TestSourceDesc) Parse(*parser.ParseContext) error {
	return nil
}

var reservedIdentifierNames = map[string]struct{}{
	"by":         {},
	"with":       {},
	"from":       {},
	"to":         {},
	"const":      {},
	"partition":  {},
	"offset":     {},
	"event_time": {},
	"ws":         {},
	"we":         {},
}

var streamSchema = evbatch.NewEventSchema([]string{"stream_name", "stream_def", "in_schema", "in_partitions", "in_mapping",
	"out_schema", "out_partitions", "out_mapping", "child_streams"},
	[]types.ColumnType{types.ColumnTypeString, types.ColumnTypeString, types.ColumnTypeString, types.ColumnTypeInt,
		types.ColumnTypeString, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeString})
