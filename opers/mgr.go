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
	"github.com/spirit-labs/tektite/retention"
	"github.com/spirit-labs/tektite/types"
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

type prefixRetention interface {
	AddPrefixRetention(prefixRetention retention.PrefixRetention)
}

type ProcessorManager interface {
	RegisterListener(listenerName string, listener proc.ProcessorListener) []proc.Processor
	UnregisterListener(listenerName string)
	AfterReceiverChange()
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

type store interface {
	NewIterator(keyStart []byte, keyEnd []byte, highestVersion uint64, preserveTombstones bool) (iteration.Iterator, error)
	Get(key []byte) ([]byte, error)
	GetWithMaxVersion(key []byte, maxVersion uint64) ([]byte, error)
	Write(batch *mem.Batch) error
}

func NewStreamManager(messageClientFactory kafka.ClientFactory, stor store,
	prefixRetentionService prefixRetention, expressionFactory *expr.ExpressionFactory, cfg *conf.Config, disableIngest bool) StreamManager {
	mgr := &streamManager{
		prefixRetentionService: prefixRetentionService,
		expressionFactory:      expressionFactory,
		messageClientFactory:   messageClientFactory,
		stor:                   stor,
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
	mgr.calculateInjectableReceivers()
	return mgr
}

type streamManager struct {
	lock                   sync.RWMutex
	prefixRetentionService prefixRetention
	expressionFactory      *expr.ExpressionFactory
	streams                map[string]*StreamInfo
	kafkaEndpoints         map[string]*KafkaEndpointInfo
	receivers              map[int]Receiver
	injectableReceivers    atomic.Pointer[map[int][]int]
	requiredCompletions    int
	processorManager       ProcessorManager
	messageClientFactory   kafka.ClientFactory
	stor                   store
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

func (pm *streamManager) GetIngestedMessageCount() int {
	return int(atomic.LoadUint64(&pm.ingestedMessageCount))
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

func (pm *streamManager) SetProcessorManager(procMgr ProcessorManager) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	pm.processorManager = procMgr
}

func (pm *streamManager) PrepareForShutdown() {
	pm.shutdownLock.Lock()
	defer pm.shutdownLock.Unlock()
	pm.shuttingDown = true
}

func (pm *streamManager) StreamCount() int {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	return len(pm.streams) - pm.sysStreamCount // we don't count the system ones
}

func (pm *streamManager) RegisterChangeListener(listener func(string, bool)) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	pm.changeListeners = append(pm.changeListeners, listener)
}

func (pm *streamManager) callChangeListeners(streamName string, deployed bool) {
	for _, listener := range pm.changeListeners {
		listener(streamName, deployed)
	}
}

func (pm *streamManager) DeployStream(streamDesc parser.CreateStreamDesc, receiverSequences []int,
	slabSequences []int, tsl string, commandID int64) error {
	if isReservedIdentifierName(streamDesc.StreamName) {
		return statementErrorAtTokenNamef(streamDesc.StreamName, &streamDesc, "stream name '%s' is a reserved name", streamDesc.StreamName)
	}
	_, exists := pm.streams[streamDesc.StreamName]
	if exists {
		return statementErrorAtTokenNamef(streamDesc.StreamName, &streamDesc, "stream '%s' already exists", streamDesc.StreamName)
	}
	if err := validateStream(&streamDesc); err != nil {
		return err
	}
	streamDesc = pm.maybeRewriteDesc(streamDesc)
	return pm.deployStream(streamDesc, receiverSequences, slabSequences, tsl, commandID)
}

func (pm *streamManager) maybeRewriteDesc(streamDesc parser.CreateStreamDesc) parser.CreateStreamDesc {
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

func (pm *streamManager) deployStream(streamDesc parser.CreateStreamDesc, receiverSequences []int,
	slabSequences []int, tsl string, commandID int64) error {
	log.Debugf("deploying stream %s", streamDesc.StreamName)
	pm.shutdownLock.Lock()
	defer pm.shutdownLock.Unlock()
	if pm.shuttingDown {
		return errors.NewTektiteErrorf(errors.ShutdownError, "cluster is shutting down")
	}
	pm.lock.Lock()
	defer pm.lock.Unlock()
	var operators []Operator
	var prevOperator Operator
	var kafkaEndpointInfo *KafkaEndpointInfo
	var prefixRetentions []retention.PrefixRetention
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
			partitionScheme := NewPartitionScheme(streamDesc.StreamName, op.Partitions, false, pm.cfg.ProcessorCount)
			operSchema := &OperatorSchema{
				EventSchema:     evbatch.NewEventSchema(op.ColumnNames, op.ColumnTypes),
				PartitionScheme: partitionScheme,
			}
			oper = &testSourceOper{
				schema: operSchema,
			}
		case *parser.BridgeFromDesc:
			oper, err = pm.deployBridgeFromOperator(streamDesc.StreamName, op, receiverSliceSeqs, slabSliceSeqs, extraSlabInfos)
		case *parser.BridgeToDesc:
			oper, prefixRetentions, userSlab, err = pm.deployBridgeToOperator(streamDesc.StreamName, op, prevOperator, receiverSliceSeqs,
				slabSliceSeqs, extraSlabInfos, prefixRetentions)
		case *parser.KafkaInDesc:
			oper, kafkaEndpointInfo, err = pm.deployKafkaInOperator(streamDesc.StreamName, op, receiverSliceSeqs,
				slabSliceSeqs)
		case *parser.KafkaOutDesc:
			oper, prefixRetentions, userSlab, err = pm.deployKafkaOutOperator(streamDesc.StreamName, op,
				prevOperator, kafkaEndpointInfo, slabSliceSeqs, extraSlabInfos, prefixRetentions)
		case *parser.FilterDesc:
			oper, err = NewFilterOperator(prevOperator.OutSchema(), op.Expr, pm.expressionFactory)
		case *parser.ProjectDesc:
			oper, err = NewProjectOperator(prevOperator.OutSchema(), op.Expressions, true, pm.expressionFactory)
		case *parser.PartitionDesc:
			oper, err = pm.deployPartitionOperator(op, prevOperator, receiverSliceSeqs)
		case *parser.AggregateDesc:
			oper, prefixRetentions, userSlab, err = pm.deployAggregateOperator(streamDesc.StreamName, op, prevOperator,
				slabSliceSeqs, receiverSliceSeqs, prefixRetentions, pm.stor, extraSlabInfos)
		case *parser.StoreStreamDesc:
			oper, prefixRetentions, userSlab, err = pm.deployStoreStreamOperator(streamDesc.StreamName, op,
				prevOperator, slabSliceSeqs, extraSlabInfos, prefixRetentions)
		case *parser.StoreTableDesc:
			oper, prefixRetentions, userSlab, err = pm.deployStoreTableOperator(streamDesc.StreamName, op, prevOperator,
				slabSliceSeqs, prefixRetentions)
		case *parser.BackfillDesc:
			oper, err = pm.deployBackfillOperator(operators, receiverSliceSeqs, op)
		case *parser.JoinDesc:
			var deferredWiring func(info *StreamInfo)
			oper, extraSlabInfos, prefixRetentions, deferredWiring, err = pm.deployJoinOperator(streamDesc.StreamName, op, receiverSliceSeqs,
				slabSliceSeqs, extraSlabInfos, prefixRetentions)
			if err == nil {
				deferredWirings = append(deferredWirings, deferredWiring)
			}
		case *parser.ContinuationDesc:
			upstreamStream, ok := pm.streams[op.ParentStreamName]
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
				pm.storeStreamMeta(upstreamStream)
			}
			deferredWirings = append(deferredWirings, deferredWiring)
		case *parser.UnionDesc:
			var deferredWiring func(info *StreamInfo)
			oper, deferredWiring, err = pm.deployUnionOperator(streamDesc.StreamName, op, receiverSliceSeqs)
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
		if pm.loaded {
			if err := oper.Setup(pm); err != nil {
				return err
			}
		}
	}
	pm.streams[streamDesc.StreamName] = info
	if kafkaEndpointInfo != nil {
		pm.kafkaEndpoints[streamDesc.StreamName] = kafkaEndpointInfo
	}
	pm.invalidateCachedInfo()
	for _, prefixRetention := range prefixRetentions {
		pm.prefixRetentionService.AddPrefixRetention(prefixRetention)
	}
	for _, deferred := range deferredWirings {
		deferred(info)
	}
	pm.storeStreamMeta(info)
	pm.lastCommandID = commandID
	if pm.loaded {
		pm.calculateInjectableReceivers()
	}
	pm.callChangeListeners(streamDesc.StreamName, true)
	if pm.loaded {
		pm.processorManager.AfterReceiverChange() // Must be outside of lock
	}
	return nil
}

func (pm *streamManager) deployBridgeFromOperator(streamName string, op *parser.BridgeFromDesc,
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
	dedupSlabID := slabSliceSeqs.GetNextID()

	extraSlabInfos[fmt.Sprintf("dedup-bridge-from-%s-%d", streamName, dedupSlabID)] =
		&SlabInfo{
			StreamName: streamName,
			SlabID:     dedupSlabID,
			Type:       SlabTypeInternal,
		}
	bf, err := NewBridgeFromOperator(receiverID, pollTimeout, maxPollMessages, op.TopicName, op.Partitions,
		op.Props, pm.messageClientFactory, pm.disableIngest,
		pm.stor, pm.processorManager, dedupSlabID, pm.cfg, &pm.ingestedMessageCount, getMappingID(), &pm.ingestEnabled,
		&pm.lastFlushedVersion)
	if err != nil {
		return nil, err
	}
	wmType, wmLateness, wmIdleTimeout, err := defaultWatermarkArgs(op.WatermarkType, op.WatermarkLateness,
		op.WatermarkIdleTimeout, op)
	if err != nil {
		return nil, err
	}
	watermarkOperator := NewWaterMarkOperator(bf.InSchema(), wmType, wmLateness, wmIdleTimeout)
	bf.watermarkOperator = watermarkOperator
	pm.bridgeFromOpers[bf] = struct{}{}
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

func (pm *streamManager) deployBridgeToOperator(streamName string, op *parser.BridgeToDesc, prevOperator Operator,
	receiverSliceSeqs *sliceSeq, slabSliceSeqs *sliceSeq, extraSlabInfos map[string]*SlabInfo,
	prefixRetentions []retention.PrefixRetention) (*BridgeToOperator,
	[]retention.PrefixRetention, *SlabInfo, error) {
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
		extraSlabInfos[fmt.Sprintf("consumer-endpoint-offsets-%s-%d", streamName, offsetsSlabID)] =
			&SlabInfo{
				StreamName: streamName,
				SlabID:     offsetsSlabID,
				Type:       SlabTypeInternal,
			}
	}
	var ret time.Duration
	if op.Retention != nil {
		ret = *op.Retention
	} else {
		ret = 24 * time.Hour
	}
	sso, prefixRetentions, userSlabInfo, err := pm.setupStoreStreamOperator(streamName, ret, prevOperator.OutSchema(),
		slabID, offsetsSlabID, prefixRetentions)
	if err != nil {
		return nil, nil, nil, err
	}
	prefRetention := createPrefixRetention(ret, slabID)
	if prefRetention != nil {
		prefixRetentions = append(prefixRetentions, *prefRetention)
	}
	backfillReceiverID := receiverSliceSeqs.GetNextID()
	bfo := NewBackfillOperator(sso.OutSchema(), pm.stor, pm.cfg, slabID, pm.cfg.MaxBackfillBatchSize, backfillReceiverID, true)
	bt, err := NewBridgeToOperator(op, sso, bfo, pm.messageClientFactory)
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

func (pm *streamManager) deployKafkaInOperator(streamName string, op *parser.KafkaInDesc,
	receiverSliceSeqs *sliceSeq, slabSliceSeqs *sliceSeq) (Operator, *KafkaEndpointInfo, error) {
	receiverID := receiverSliceSeqs.GetNextID()
	offsetsSlabID := slabSliceSeqs.GetNextID()
	kafkaIn := NewKafkaInOperator(getMappingID(), pm.stor, offsetsSlabID, receiverID,
		op.Partitions, pm.cfg.KafkaUseServerTimestamp, pm.cfg.ProcessorCount)
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
	return kafkaIn, kafkaEndpointInfo, nil
}

func (pm *streamManager) setupStoreStreamOperator(streamName string, retention time.Duration,
	schema *OperatorSchema, slabID int, offsetsSlabID int,
	prefixRetentions []retention.PrefixRetention) (*StoreStreamOperator, []retention.PrefixRetention, *SlabInfo, error) {
	tso, err := NewStoreStreamOperator(schema, slabID, offsetsSlabID, pm.stor, pm.cfg.NodeID)
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
	prefixRetention := createPrefixRetention(retention, slabID)
	if prefixRetention != nil {
		prefixRetentions = append(prefixRetentions, *prefixRetention)
	}
	return tso, prefixRetentions, userSlabInfo, nil
}

func (pm *streamManager) deployStoreStreamOperator(streamName string, op *parser.StoreStreamDesc,
	prevOperator Operator, slabSliceSeqs *sliceSeq, extraSlabInfos map[string]*SlabInfo,
	prefixRetentions []retention.PrefixRetention) (Operator, []retention.PrefixRetention, *SlabInfo, error) {
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
			}
	}
	ret := time.Duration(0)
	if op.Retention != nil {
		ret = *op.Retention
	}
	return pm.setupStoreStreamOperator(streamName, ret, prevOperator.OutSchema(), slabID, offsetsSlabID, prefixRetentions)
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

func (pm *streamManager) deployKafkaOutOperator(streamName string, op *parser.KafkaOutDesc,
	prevOperator Operator, kafkaEndpointInfo *KafkaEndpointInfo, slabSliceSeqs *sliceSeq,
	extraSlabInfos map[string]*SlabInfo, prefixRetentions []retention.PrefixRetention) (Operator, []retention.PrefixRetention, *SlabInfo, error) {

	if !verifyKafkaSchema(prevOperator.OutSchema().EventSchema) {
		return nil, nil, nil, statementErrorAtTokenNamef("", op,
			"input to 'kafka out' operator must have column types: [key:bytes, hdrs:bytes, val:bytes]")
	}

	slabID := slabSliceSeqs.GetNextID()
	offsetsSlabID := -1
	hasOffset := HasOffsetColumn(prevOperator.OutSchema().EventSchema)
	if !hasOffset {
		// Need to add an offset in the store operator
		offsetsSlabID = slabSliceSeqs.GetNextID()
		extraSlabInfos[fmt.Sprintf("consumer-endpoint-offsets-%s-%d", streamName, offsetsSlabID)] =
			&SlabInfo{
				StreamName: streamName,
				SlabID:     offsetsSlabID,
				Type:       SlabTypeInternal,
			}
	}
	// The consumer_endpoint operator wraps a to-stream operator, we create that next
	ret := time.Duration(0)
	if op.Retention != nil {
		ret = *op.Retention
	}
	storeStreamOperator, prefixRetentions, userSlabInfo, err := pm.setupStoreStreamOperator(streamName, ret, prevOperator.OutSchema(),
		slabID, offsetsSlabID, prefixRetentions)

	storeOffset := false
	if hasOffset {
		// Already have an offset on the stream, so we need to locate the offset slabID - we need to traverse upstream
		// from this operator until we find a to-stream with addOffset = false or a kafka in
		var err error
		offsetsSlabID, err = pm.findOffsetsSlabID(prevOperator)
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
		}
	}
	kafkaOutOper, err := NewKafkaOutOperator(storeStreamOperator, slabID, offsetsSlabID, prevOperator.OutSchema(),
		pm.stor, storeOffset)
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
		pm.kafkaEndpoints[streamName] = &KafkaEndpointInfo{
			Name:        streamName,
			OutEndpoint: kafkaOutOper,
			Schema:      prevOperator.OutSchema(),
		}
	}
	return kafkaOutOper, prefixRetentions, userSlabInfo, nil
}

func (pm *streamManager) deployPartitionOperator(op *parser.PartitionDesc,
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
		pm.cfg, op)
	if err != nil {
		return nil, err
	}
	pm.partitionOperators[po] = struct{}{}
	return po, nil
}

func (pm *streamManager) deployAggregateOperator(streamName string, op *parser.AggregateDesc,
	prevOperator Operator, slabSliceSeqs *sliceSeq, receiverSliceSeqs *sliceSeq,
	prefixRetentions []retention.PrefixRetention, store store, extraSlabInfos map[string]*SlabInfo) (Operator, []retention.PrefixRetention, *SlabInfo, error) {
	windowed := op.Size != nil
	aggStateSlabID := slabSliceSeqs.GetNextID()
	extraSlabInfos[fmt.Sprintf("aggregate-%s-%d", streamName, aggStateSlabID)] =
		&SlabInfo{
			StreamName: streamName,
			SlabID:     aggStateSlabID,
			Type:       SlabTypeInternal,
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
				resultsRetention := createPrefixRetention(*op.Retention, resultsSlabID)
				if resultsRetention != nil {
					prefixRetentions = append(prefixRetentions, *resultsRetention)
				}
			}
		}
		// We set retention on the internal aggregate state to be 1 hour + (size + lateness)
		// This is not ideal - really we want to mark prefix for deletion as soon as window is closed but
		// registering a prefix retention for each closed window does not scale. We need to implement efficient
		// range deletions in the database.
		r := size + lateness + time.Hour
		prefix := make([]byte, 0, 8)
		prefix = encoding.AppendUint64ToBufferBE(prefix, uint64(aggStateSlabID))
		ret := &retention.PrefixRetention{Prefix: prefix, Retention: uint64(r.Milliseconds())}
		prefixRetentions = append(prefixRetentions, *ret)
	} else {
		if op.Retention != nil {
			internalRetention := createPrefixRetention(*op.Retention, aggStateSlabID)
			if internalRetention != nil {
				prefixRetentions = append(prefixRetentions, *internalRetention)
			}
		}
	}
	aggOper, err := NewAggregateOperator(prevOperator.OutSchema(), op, aggStateSlabID, openWindowsSlabID, resultsSlabID,
		closedWindowReceiverID, size, hop, store, lateness, storeResults, includeWindowCols, pm.expressionFactory)
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

func (pm *streamManager) deployStoreTableOperator(streamName string, op *parser.StoreTableDesc,
	prevOperator Operator, slabSliceSeqs *sliceSeq,
	prefixRetentions []retention.PrefixRetention) (Operator, []retention.PrefixRetention, *SlabInfo, error) {
	slabID := slabSliceSeqs.GetNextID()
	to, err := NewStoreTableOperator(prevOperator.OutSchema(), slabID, pm.stor, op.KeyCols, pm.cfg.NodeID, false, op)
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
	ret := time.Duration(0)
	if op.Retention != nil {
		ret = *op.Retention
	}
	prefixRetention := createPrefixRetention(ret, slabID)
	if prefixRetention != nil {
		prefixRetentions = append(prefixRetentions, *prefixRetention)
	}
	return to, prefixRetentions, userSlab, nil
}

func (pm *streamManager) deployBackfillOperator(operators []Operator,
	receiverSliceSeqs *sliceSeq, desc *parser.BackfillDesc) (*BackfillOperator, error) {
	// Already verified in validateStream() so this is safe:
	continuation := operators[0].(*ContinuationOperator)
	upstreamStream := continuation.GetParentOperator().GetStreamInfo()

	fromSlab := upstreamStream.UserSlab
	if fromSlab == nil || fromSlab.Type != SlabTypeUserStream {
		return nil, statementErrorAtTokenNamef(upstreamStream.StreamDesc.StreamName, desc,
			"parent stream '%s' must be a stored stream when there is a 'backfill' operator", upstreamStream.StreamDesc.StreamName)
	}
	receiverID := receiverSliceSeqs.GetNextID()
	backfillOper := NewBackfillOperator(fromSlab.Schema, pm.stor, pm.cfg, fromSlab.SlabID,
		pm.cfg.MaxBackfillBatchSize, receiverID, false)
	return backfillOper, nil
}

func (pm *streamManager) deployJoinOperator(streamName string, op *parser.JoinDesc,
	receiverSliceSeqs *sliceSeq, slabSliceSeqs *sliceSeq, extraSlabInfos map[string]*SlabInfo,
	prefixRetentions []retention.PrefixRetention) (*JoinOperator, map[string]*SlabInfo, []retention.PrefixRetention,
	func(info *StreamInfo), error) {
	leftStream, ok := pm.streams[op.LeftStream]
	if !ok {
		return nil, nil, nil, nil, statementErrorAtTokenNamef(op.LeftStream, op, "unknown stream '%s'", op.LeftStream)
	}
	rightStream, ok := pm.streams[op.RightStream]
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
		}
	rightSlabID := slabSliceSeqs.GetNextID()
	extraSlabInfos[fmt.Sprintf("join-%s-right", streamName)] =
		&SlabInfo{
			StreamName: streamName,
			SlabID:     rightSlabID,
			Type:       SlabTypeInternal,
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
		prefixRetentions = append(prefixRetentions, retention.PrefixRetention{
			Prefix:    encoding.AppendUint64ToBufferBE(nil, uint64(leftSlabID)),
			Retention: uint64(ret.Milliseconds()),
		})
		prefixRetentions = append(prefixRetentions, retention.PrefixRetention{
			Prefix:    encoding.AppendUint64ToBufferBE(nil, uint64(rightSlabID)),
			Retention: uint64(ret.Milliseconds()),
		})
	}
	jo, err := NewJoinOperator(leftSlabID, rightSlabID, leftOper, rightOper, op.LeftIsTable, op.RightIsTable, leftStream.UserSlab,
		rightStream.UserSlab, op.JoinElements, within, pm.stor, pm.cfg.NodeID, receiverID, op)
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
		pm.storeStreamMeta(leftStream)
		pm.storeStreamMeta(rightStream)
	}

	return jo, extraSlabInfos, prefixRetentions, deferredWiring, nil
}

func (pm *streamManager) deployUnionOperator(streamName string, desc *parser.UnionDesc, receiverSliceSeqs *sliceSeq) (Operator, func(info *StreamInfo), error) {
	var schema *OperatorSchema
	var inputs []Operator
	var inputInfos []*StreamInfo
	for i, feedingStreamName := range desc.StreamNames {
		stream, ok := pm.streams[feedingStreamName]
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
			pm.storeStreamMeta(inputInfo)
		}
	}
	return uo, deferredWiring, nil
}

func (pm *streamManager) findOffsetsSlabID(oper Operator) (int, error) {
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
			return pm.findOffsetsSlabID(parent)
		}
	}
	return -1, nil
}

func (pm *streamManager) Loaded() {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	for _, pInfo := range pm.streams {
		ops := pInfo.Operators
		for _, op := range ops {
			if err := op.Setup(pm); err != nil {
				log.Errorf("failure in operator setup %v", err)
			}
		}
	}
	pm.calculateInjectableReceivers()
	pm.loaded = true
	// We must call AfterReceiverChange to invalidate processor manager and processor cached receiver info state
	// otherwise correct barriers might not be injected
	// must be called with lock held for consistent versions
	pm.processorManager.AfterReceiverChange()
}

func (pm *streamManager) UndeployStream(deleteStreamDesc parser.DeleteStreamDesc, commandID int64) error {
	pm.shutdownLock.Lock()
	defer pm.shutdownLock.Unlock()
	log.Debugf("undeploying stream %s", deleteStreamDesc.StreamName)
	if pm.shuttingDown {
		return errors.NewTektiteErrorf(errors.ShutdownError, "cluster is shutting down")
	}
	pm.lock.Lock()
	defer pm.lock.Unlock()

	// First we look up the stream info
	info, ok := pm.streams[deleteStreamDesc.StreamName]
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

	if pm.loaded {
		for _, oper := range info.Operators {
			oper.Teardown(pm, &pm.lock)
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
	delete(pm.streams, deleteStreamDesc.StreamName)
	for upstreamStreamName, oper := range info.UpstreamStreamNames {
		upstream, ok := pm.streams[upstreamStreamName]
		if !ok {
			panic("cannot find upstream")
		}
		delete(upstream.DownstreamStreamNames, deleteStreamDesc.StreamName)
		if oper != nil {
			upstream.Operators[len(upstream.Operators)-1].RemoveDownStreamOperator(oper)
		}
	}
	delete(pm.kafkaEndpoints, info.StreamDesc.StreamName)
	kafkaInOper, ok := info.Operators[0].(*BridgeFromOperator)
	if ok {
		delete(pm.bridgeFromOpers, kafkaInOper)
	}
	partitionOper, ok := info.Operators[0].(*PartitionOperator)
	if ok {
		delete(pm.partitionOperators, partitionOper)
	}
	// Now delete the data from the store
	if info.UserSlab != nil {
		pm.deleteSlab(info.UserSlab)
	}
	for _, slabInfo := range info.ExtraSlabs {
		pm.deleteSlab(slabInfo)
	}
	pm.invalidateCachedInfo()
	pm.deleteStreamMeta(deleteStreamDesc.StreamName)
	if pm.loaded {
		pm.calculateInjectableReceivers()
	}
	pm.callChangeListeners(deleteStreamDesc.StreamName, false)
	pm.lastCommandID = commandID
	if pm.loaded {
		// Note, this must be called with the stream manager lock held to ensure that barriers don't get injected
		// with stale receivers after a stream has been deployed - otherwise we could have data flowing through
		// the newly updated graph but barriers only injected in some of it. If the version then completed it would
		// be invalid
		pm.processorManager.AfterReceiverChange()
	}
	return nil
}

func (pm *streamManager) deleteSlab(slabInfo *SlabInfo) {
	prefix := encoding.AppendUint64ToBufferBE(nil, uint64(slabInfo.SlabID))
	pm.prefixRetentionService.AddPrefixRetention(retention.PrefixRetention{Prefix: prefix})
}

func (pm *streamManager) RegisterSystemSlab(slabName string, persistorReceiverID int, deleterReceiverID int,
	slabID int, schema *OperatorSchema, keyCols []string, noCache bool) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	to, err := NewStoreTableOperator(schema, slabID, pm.stor, keyCols, -1, noCache, nil)
	if err != nil {
		return err
	}
	pm.receivers[persistorReceiverID] = &sysTableReceiver{schema: schema, oper: to}
	if deleterReceiverID != -1 {
		del, err := NewRowDeleteOperator(schema, slabID, pm.stor, to.inKeyCols)
		if err != nil {
			return err
		}
		pm.receivers[deleterReceiverID] = &sysTableReceiver{oper: del, schema: del.schema}
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
	pm.streams[slabName] = streamInfo
	pm.sysStreamCount++
	return nil
}

const sysStreamName = "sys.streams"

func (pm *streamManager) Start() error {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	pm.streams[sysStreamName] = &StreamInfo{
		UserSlab: &SlabInfo{
			StreamName: sysStreamName,
			SlabID:     common.StreamMetaSlabID,
			Schema: &OperatorSchema{
				EventSchema: streamSchema,
				PartitionScheme: PartitionScheme{
					MappingID:                 sysStreamName,
					Partitions:                1,
					ProcessorIDs:              []int{0},
					PartitionProcessorMapping: map[int]int{0: 0},
					ProcessorPartitionMapping: map[int][]int{0: {0}},
				},
			},
			KeyColIndexes: []int{0},
			Type:          SlabTypeQueryableInternal,
		},
		SystemStream: true,
		StreamMeta:   true,
	}
	pm.sysStreamCount++
	return nil
}

func (pm *streamManager) Stop() error {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	for _, info := range pm.streams {
		for _, oper := range info.Operators {
			// Makes sure oper resources are cleaned up - e.g. kafka_in consumers are stopped
			oper.Teardown(pm, &pm.lock)
		}
	}
	delete(pm.streams, sysStreamName)
	return nil
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

func (pm *streamManager) GetStream(name string) *StreamInfo {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	return pm.streams[name]
}

func (pm *streamManager) GetAllStreams() []*StreamInfo {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	allStreams := make([]*StreamInfo, 0, len(pm.streams))
	for _, info := range pm.streams {
		allStreams = append(allStreams, info)
	}
	return allStreams
}

func (pm *streamManager) GetKafkaEndpoint(name string) *KafkaEndpointInfo {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	return pm.kafkaEndpoints[name]
}

func (pm *streamManager) GetAllKafkaEndpoints() []*KafkaEndpointInfo {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	var endpoints []*KafkaEndpointInfo
	for _, endpoint := range pm.kafkaEndpoints {
		endpoints = append(endpoints, endpoint)
	}
	return endpoints
}

func (pm *streamManager) RegisterReceiverWithLock(id int, receiver Receiver) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	pm.RegisterReceiver(id, receiver)
}

func (pm *streamManager) RegisterReceiver(id int, receiver Receiver) {
	if receiver == nil {
		panic("cannot register nil receiver")
	}
	pm.receivers[id] = receiver
}

func (pm *streamManager) UnregisterReceiver(id int) {
	delete(pm.receivers, id)
}

func (pm *streamManager) ProcessorManager() ProcessorManager {
	return pm.processorManager
}

func (pm *streamManager) numStreams() int {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	return len(pm.streams)
}

// BatchHandler implementation

func (pm *streamManager) HandleProcessBatch(processor proc.Processor, processBatch *proc.ProcessBatch,
	_ bool) (bool, *mem.Batch, []*proc.ProcessBatch, error) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	receiver, ok := pm.receivers[processBatch.ReceiverID]
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
	ec := pm.newExecContext(processBatch, processor)

	if processBatch.Barrier {
		// We set the last executed command id on the barrier. The command id is propagated as barriers are forwarded
		// and passed to the version manager when the version completes. The version manager will ensure the command
		// id is the same for all processors completing a version. In this way we can guarantee if we complete a version
		// ok then barriers have been sent through the graph as of the same command id and therefore the version
		// is consistent. If version manager receives different command id then the version is doomed and advanced.
		processBatch.CommandID = int(pm.lastCommandID)
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

func (pm *streamManager) GetForwardingProcessorCount(receiverID int) (int, bool) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	receiver, ok := pm.receivers[receiverID]
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
func (pm *streamManager) GetRequiredCompletions() int {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	if pm.requiredCompletions != -1 {
		return pm.requiredCompletions
	}

	// First we create a tree of the roots and any operators that forward barriers to other processors, e.g.
	// partition, join etc
	tree := &partitionTree{}
	for _, receiver := range pm.receivers {
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
	pm.requiredCompletions = requiredCompletions + 1

	return pm.requiredCompletions
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

func (pm *streamManager) calculateInjectableReceivers() {
	injectableReceivers := map[int][]int{}
	for id, receiver := range pm.receivers {
		if receiver.RequiresBarriersInjection() {
			for _, procID := range receiver.OutSchema().PartitionScheme.ProcessorIDs {
				injectableReceivers[procID] = append(injectableReceivers[procID], id)
			}
		}

	}
	pm.injectableReceivers.Store(&injectableReceivers)
}

func (pm *streamManager) GetInjectableReceivers(processorID int) []int {
	// Note that this method must not lock on the stream manager to avoid a deadlock situation when deploying
	// a stream, where the stream mgr is locked and attempts to lock the processor manager to register a listener
	// and when handling a version broadcast which locks the processor manager then attempts to lock stream manager
	// when calling this method.
	receivers := *pm.injectableReceivers.Load()
	return receivers[processorID]
}

func createPrefixRetention(ret time.Duration, slabID int) *retention.PrefixRetention {
	if ret == 0 {
		return nil
	}
	prefix := make([]byte, 0, 8)
	prefix = encoding.AppendUint64ToBufferBE(prefix, uint64(slabID))
	return &retention.PrefixRetention{Prefix: prefix, Retention: uint64(ret.Milliseconds())}
}

func (pm *streamManager) invalidateCachedInfo() {
	pm.requiredCompletions = -1
}

func (pm *streamManager) StopIngest() error {
	pm.lock.Lock()
	log.Debugf("node %d stream manager stop ingest", pm.cfg.NodeID)
	pm.ingestEnabled.Store(false)
	var fkOpers []*BridgeFromOperator
	for fk := range pm.bridgeFromOpers {
		fkOpers = append(fkOpers, fk)
	}
	var partOpers []*PartitionOperator
	for po := range pm.partitionOperators {
		partOpers = append(partOpers, po)
	}
	pm.lock.Unlock()
	// We must call stopIngest outside lock to avoid a deadlock, where we stop ingest which waits for message consumers
	// to stop, but message consumer could be waiting for ingest to complete, but the processor is blocked trying to
	// call HandleProcessBatch on the stream manager (which gets read lock)
	for _, fk := range fkOpers {
		fk.stopIngest()
	}
	return nil
}

func (pm *streamManager) StartIngest(version int) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	log.Debugf("node %d stream manager start ingest, version %d", pm.cfg.NodeID, version)
	pm.ingestEnabled.Store(true)
	atomic.StoreInt64(&pm.lastFlushedVersion, int64(version))
	for fk := range pm.bridgeFromOpers {
		if err := fk.startIngest(int64(version)); err != nil {
			return err
		}
	}
	return nil
}

func (pm *streamManager) Dump() {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	log.Infof("dumping stream manager on node %d", pm.cfg.NodeID)
	for _, p := range pm.streams {
		log.Infof("stream: %s: %v", p.StreamDesc.StreamName, p)
	}
}

// storeStreamMeta - stores the stream info in a mem table, so it can be queried like any other table.
// note this is not a real persistent table
func (pm *streamManager) storeStreamMeta(info *StreamInfo) {
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
	prefix := createTableKeyPrefix(common.StreamMetaSlabID, 0, 64)
	keyBuff := evbatch.EncodeKeyCols(batch, 0, []int{0}, prefix)
	keyBuff = encoding.EncodeVersion(keyBuff, 0) // not versioned
	rowBuff := make([]byte, 0, rowInitialBufferSize)
	rowBuff = evbatch.EncodeRowCols(batch, 0, []int{1, 2, 3, 4, 5, 6, 7, 8}, rowBuff)
	log.Debugf("storing stream meta for stream %s with key: %v", info.StreamDesc.StreamName, keyBuff)
	pm.streamMemStore.Put(string(keyBuff), rowBuff)
}

func (pm *streamManager) deleteStreamMeta(streamName string) {
	keyBuff := createTableKeyPrefix(common.StreamMetaSlabID, 0, 64)
	keyBuff = append(keyBuff, 1)
	keyBuff = encoding.KeyEncodeString(keyBuff, streamName)
	keyBuff = encoding.EncodeVersion(keyBuff, 0) // not versioned
	log.Debugf("deleting stream meta for stream %s with key: %v", streamName, keyBuff)
	pm.streamMemStore.Remove(string(keyBuff))
}

func (pm *streamManager) streamMetaIterator(startKey []byte, endKey []byte) iteration.Iterator {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	var entries []common.KV
	iter := pm.streamMemStore.Iterator()
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

func (pm *streamManager) StreamMetaIteratorProvider() *StreamMetaIteratorProvider {
	return pm.streamMetaIterProvider
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

func (pm *streamManager) newExecContext(processBatch *proc.ProcessBatch, processor proc.Processor) *execContext {
	return &execContext{
		processBatch: processBatch,
		processor:    processor,
		store:        pm.stor,
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
	store             store
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
	return e.store.Get(key)
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
