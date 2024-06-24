//go:build integration

package integration

import (
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/levels"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/tabcache"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestEventTimeWaterMark(t *testing.T) {

	streamMgr, processorMgr, cfg := setup(t)
	defer func() {
		err := streamMgr.Stop()
		require.NoError(t, err)
		err = processorMgr.Stop()
		require.NoError(t, err)
	}()

	receiverID := 1000000
	tsl := `test_stream := 
			(kafka in partitions=10) ->
			(store stream)`
	gatheringOper, ppm := deployStream(t, streamMgr, tsl, receiverID)

	partitionID := 0
	processor := getProcessorForPartition(t, partitionID, ppm, processorMgr)

	etMax := time.Date(2023, 11, 11, 11, 11, 11, 0, time.UTC)
	etMaxUnixMillis := etMax.UnixMilli()

	pb := createBatchWithMaxEventTime(etMaxUnixMillis, processor.ID(), receiverID, partitionID)

	err := processor.IngestBatchSync(pb)
	require.NoError(t, err)

	// prompt injection of barriers
	processorMgr.HandleVersionBroadcast(102, 100, 90)

	lag := 1 * time.Second
	expectedWM := int(etMax.Add(-lag).UnixMilli())

	waitUntilReceiveWaterMark(t, expectedWM, processor.ID(), gatheringOper)
	for i := 0; i < cfg.ProcessorCount; i++ {
		if i == processor.ID() {
			continue
		}
		// other processors should have received idle
		lastWM, ok := gatheringOper.getLastWaterMark(i)
		if ok {
			require.Equal(t, -1, lastWM)
		}
	}
}

func TestProcessingTimeWaterMark(t *testing.T) {

	streamMgr, processorMgr, _ := setup(t)
	defer func() {
		err := streamMgr.Stop()
		require.NoError(t, err)
		err = processorMgr.Stop()
		require.NoError(t, err)
	}()

	receiverID := 1000000
	tsl := `test_stream := (kafka in partitions=10 watermark_type=processing_time) -> (store stream)`
	gatheringOper, ppm := deployStream(t, streamMgr, tsl, receiverID)

	partitionID := 0
	processor := getProcessorForPartition(t, partitionID, ppm, processorMgr)

	etMax := time.Date(2023, 11, 11, 11, 11, 11, 0, time.UTC)
	etMaxUnixMillis := etMax.UnixMilli()
	pb := createBatchWithMaxEventTime(etMaxUnixMillis, processor.ID(), receiverID, partitionID)

	err := processor.IngestBatchSync(pb)
	require.NoError(t, err)

	tm := time.Now().UTC().Add(-1 * time.Second)
	expected := int(tm.UnixMilli())
	// prompt injection of barriers
	processorMgr.HandleVersionBroadcast(102, 100, 90)

	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		lastWM, ok := gatheringOper.getLastWaterMark(processor.ID())
		if !ok {
			return false, nil
		}
		return lastWM >= expected, nil
	}, 5*time.Second, 100*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestForwardedWaterMark(t *testing.T) {

	streamMgr, processorMgr, _ := setup(t)
	defer func() {
		err := streamMgr.Stop()
		require.NoError(t, err)
		err = processorMgr.Stop()
		require.NoError(t, err)
	}()

	receiverID := 1000000
	tsl := `test_stream := 
			(kafka in partitions=10) ->
			(partition by val partitions=10) ->
			(store stream)`
	gatheringOper, ppm := deployStream(t, streamMgr, tsl, receiverID)

	// Get unique set of processors
	processors := map[int]int{}
	for i := 0; i < 10; i++ {
		procID := ppm[i]
		processors[procID] = i
	}

	// Send one batch on each processor
	// On one processor send batch with lower event time, than the others
	first := true
	for procID, partitionID := range processors {
		var etMax time.Time
		if first {
			etMax = time.Date(2023, 11, 11, 11, 11, 9, 0, time.UTC)
		} else {
			etMax = time.Date(2023, 11, 11, 11, 11, 11, 0, time.UTC)
		}
		etMaxUnixMillis := etMax.UnixMilli()
		processor := processorMgr.GetProcessor(procID)
		require.NotNil(t, processor)

		pb := createBatchWithMaxEventTime(etMaxUnixMillis, processor.ID(), receiverID, partitionID)

		err := processor.IngestBatchSync(pb)
		require.NoError(t, err)
		first = false
	}

	// prompt injection of barriers
	processorMgr.HandleVersionBroadcast(102, 100, 90)

	lag := 1 * time.Second
	// The forwarded watermark should be the min of all the processor watermarks
	expectedForwardedMax := time.Date(2023, 11, 11, 11, 11, 9, 0, time.UTC)
	expectedWM := int(expectedForwardedMax.Add(-lag).UnixMilli())

	infoIn := streamMgr.GetStream("test_stream")
	require.NotNil(t, infoIn)
	// Find the partition operator
	partitionOperator := infoIn.Operators[len(infoIn.Operators)-2].(*opers.PartitionOperator)
	partitionMapping := partitionOperator.GetPartitionProcessorMapping()

	// Get the unique set of processors the partition operator will forward to - they'll be a barrier on each of these
	forwardedProcessors := map[int]int{}
	for i := 0; i < 10; i++ {
		procID := partitionMapping[i]
		forwardedProcessors[procID] = i
	}

	// Wait until we get barrier forwarded
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		// All the processors in the unique set should get the low value
		for procID := range forwardedProcessors {
			wm, ok := gatheringOper.getLastWaterMark(procID)
			if !ok {
				return false, nil
			}
			if wm != expectedWM {
				return false, nil
			}
		}
		return true, nil
	}, 5*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestWaterMarkAllProcessorsIdle(t *testing.T) {

	streamMgr, processorMgr, _ := setup(t)
	defer func() {
		err := streamMgr.Stop()
		require.NoError(t, err)
		err = processorMgr.Stop()
		require.NoError(t, err)
	}()

	receiverID := 1000000
	tsl := `test_stream := 
			(kafka in partitions=10 watermark_idle_timeout=10ms) ->
			(partition by val partitions=10) ->
			(store stream)`
	gatheringOper, ppm := deployStream(t, streamMgr, tsl, receiverID)

	// Get unique set of processors
	processors := map[int]int{}
	for i := 0; i < 10; i++ {
		procID := ppm[i]
		processors[procID] = i
	}

	// Send one batch on each processor
	for procID, partitionID := range processors {
		et := time.Date(2023, 11, 11, 11, 11, 11, 0, time.UTC)
		etMaxUnixMillis := et.UnixMilli()
		processor := processorMgr.GetProcessor(procID)
		require.NotNil(t, processor)

		pb := createBatchWithMaxEventTime(etMaxUnixMillis, processor.ID(), receiverID, partitionID)

		err := processor.IngestBatchSync(pb)
		require.NoError(t, err)
	}

	// Wait a bit for the watermark to become idle
	time.Sleep(10 * time.Millisecond)

	// prompt injection of barriers
	processorMgr.HandleVersionBroadcast(102, 100, 90)

	infoIn := streamMgr.GetStream("test_stream")
	require.NotNil(t, infoIn)
	// Find the partition operator
	partitionOperator := infoIn.Operators[len(infoIn.Operators)-2].(*opers.PartitionOperator)
	partitionMapping := partitionOperator.GetPartitionProcessorMapping()

	// Get the unique set of processors the partition operator will forward to - they'll be a barrier on each of these
	forwardedProcessors := map[int]int{}
	for i := 0; i < 10; i++ {
		procID := partitionMapping[i]
		forwardedProcessors[procID] = i
	}

	// Wait until we get barrier forwarded
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		// All the processors in the unique set should get the low value
		for procID := range forwardedProcessors {
			wm, ok := gatheringOper.getLastWaterMark(procID)
			if !ok {
				return false, nil
			}
			// Should be -1 - all processors idle
			if wm != -1 {
				return false, nil
			}
		}
		return true, nil
	}, 5*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestWaterMarkNotAllProcessorsIdle(t *testing.T) {

	streamMgr, processorMgr, _ := setup(t)
	defer func() {
		err := streamMgr.Stop()
		require.NoError(t, err)
		err = processorMgr.Stop()
		require.NoError(t, err)
	}()

	receiverID := 1000000
	tsl := `test_stream := 
			(kafka in partitions=10 watermark_idle_timeout=1h) ->
			(partition by val partitions=10) ->
			(store stream)`
	gatheringOper, ppm := deployStream(t, streamMgr, tsl, receiverID)

	infoIn := streamMgr.GetStream("test_stream")
	require.NotNil(t, infoIn)

	waterMarkOper := infoIn.Operators[0].(*opers.KafkaInOperator).GetWatermarkOperator()

	// Get unique set of processors
	processors := map[int]int{}
	for i := 0; i < 10; i++ {
		procID := ppm[i]
		processors[procID] = i
	}

	// Send one batch on each processor
	// On one processor send batch with lower event time, than the others
	// Mark two of the processors (not the first one) as idle
	first := true
	idleCount := 0
	for procID, partitionID := range processors {
		var etMax time.Time
		if first {
			etMax = time.Date(2023, 11, 11, 11, 11, 9, 0, time.UTC)
		} else {
			etMax = time.Date(2023, 11, 11, 11, 11, 11, 0, time.UTC)
			if idleCount < 2 {
				waterMarkOper.SetIdleForProcessor(procID)
				idleCount++
			}
		}
		etMaxUnixMillis := etMax.UnixMilli()
		processor := processorMgr.GetProcessor(procID)
		require.NotNil(t, processor)

		pb := createBatchWithMaxEventTime(etMaxUnixMillis, processor.ID(), receiverID, partitionID)

		err := processor.IngestBatchSync(pb)
		require.NoError(t, err)
		first = false
	}

	// prompt injection of barriers
	processorMgr.HandleVersionBroadcast(102, 100, 90)

	// Find the partition operator
	partitionOperator := infoIn.Operators[len(infoIn.Operators)-2].(*opers.PartitionOperator)
	partitionMapping := partitionOperator.GetPartitionProcessorMapping()

	// Get the unique set of processors the partition operator will forward to - they'll be a barrier on each of these
	forwardedProcessors := map[int]int{}
	for i := 0; i < 10; i++ {
		procID := partitionMapping[i]
		forwardedProcessors[procID] = i
	}

	// The forwarded watermark should be the min of all the processor watermarks
	lag := time.Second
	expectedForwardedMax := time.Date(2023, 11, 11, 11, 11, 9, 0, time.UTC)
	expectedWM := int(expectedForwardedMax.Add(-lag).UnixMilli())

	// Wait until we get barrier forwarded
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		// All the processors in the unique set should get the low value
		for procID := range forwardedProcessors {
			wm, ok := gatheringOper.getLastWaterMark(procID)
			if !ok {
				return false, nil
			}
			// Even though two of the processors were idle, this shouldn't hold back the watermark from the other
			// processors
			if wm != expectedWM {
				return false, nil
			}
		}
		return true, nil
	}, 5*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestEventTimeWaterMarkUsingKafkaIn(t *testing.T) {

	streamMgr, processorMgr, cfg := setup(t)
	defer func() {
		err := streamMgr.Stop()
		require.NoError(t, err)
		err = processorMgr.Stop()
		require.NoError(t, err)
	}()

	receiverID := 1000000
	tsl := `test_stream := 
			(bridge from test_topic
				partitions = 10
				props = ()
			) ->
			(store stream)`
	gatheringOper, ppm := deployStream(t, streamMgr, tsl, receiverID)

	partitionID := 0
	processor := getProcessorForPartition(t, partitionID, ppm, processorMgr)

	etMax := time.Date(2023, 11, 11, 11, 11, 11, 0, time.UTC)
	etMaxUnixMillis := etMax.UnixMilli()

	pb := createBatchWithMaxEventTime(etMaxUnixMillis, processor.ID(), receiverID, partitionID)

	err := processor.IngestBatchSync(pb)
	require.NoError(t, err)

	// prompt injection of barriers
	processorMgr.HandleVersionBroadcast(102, 100, 90)

	lag := 1 * time.Second
	expectedWM := int(etMax.Add(-lag).UnixMilli())

	waitUntilReceiveWaterMark(t, expectedWM, processor.ID(), gatheringOper)
	for i := 0; i < cfg.ProcessorCount; i++ {
		if i == processor.ID() {
			continue
		}
		// other processors should have received idle
		lastWM, ok := gatheringOper.getLastWaterMark(i)
		if ok {
			require.Equal(t, -1, lastWM)
		}
	}
}

func getProcessorForPartition(t *testing.T, partitionID int, ppm map[int]int, processorMgr proc.Manager) proc.Processor {
	processorID := ppm[partitionID]
	processor := processorMgr.GetProcessor(processorID)
	require.NotNil(t, processor)
	return processor
}

func setup(t *testing.T) (opers.StreamManager, proc.Manager, *conf.Config) {

	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.LogScope = t.Name()
	cfg.ProcessorCount = 10
	streamMgr := opers.NewStreamManager(nil, &testSlabRetentions{},
		&expr.ExpressionFactory{}, cfg, true)

	stateMgr := &testClustStateMgr{}

	objStoreClient := dev.NewInMemStore(0)
	tc, err := tabcache.NewTableCache(objStoreClient, cfg)
	require.NoError(t, err)

	processorMgr := proc.NewProcessorManagerWithFailure(stateMgr, streamMgr, cfg, nil,
		func(processorID int) proc.BatchHandler {
			return streamMgr
		}, nil, &testIngestNotifier{}, objStoreClient, nil, tc, false)
	vmgrClient := &testVmgrClient{
		initialWriteVersion:     101,
		initialCompletedVersion: 100,
		initialFlushedVersion:   90,
	}
	processorMgr.SetVersionManagerClient(vmgrClient)
	err = processorMgr.Start()
	require.NoError(t, err)

	// wait for initial version to be initialised
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		return processorMgr.GetCurrentVersion() != -1, nil
	}, 5*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	stateMgr.SetClusterStateHandler(processorMgr.HandleClusterState)

	streamMgr.SetProcessorManager(processorMgr)
	streamMgr.Loaded()

	deployProcessors(t, processorMgr, cfg.ProcessorCount)

	return streamMgr, processorMgr, cfg
}

type testCommandBatchIngestor struct {
	lm *levels.LevelManager
}

func (tcbi *testCommandBatchIngestor) ingest(buff []byte, complFunc func(error)) {
	regBatch := levels.RegistrationBatch{}
	regBatch.Deserialize(buff, 1)
	go func() {
		err := tcbi.lm.ApplyChanges(regBatch, false, 0)
		complFunc(err)
	}()
}

func deployStream(t *testing.T, streamMgr opers.StreamManager, tsl string,
	receiverID int) (*gatheringWMOperator, map[int]int) {
	ast, err := parser.NewParser(nil).ParseTSL(tsl)
	require.NoError(t, err, ast)
	err = streamMgr.DeployStream(*ast.CreateStream, []int{1000, 1001, 1002, 1003}, []int{1000, 1001, 1002, 1003}, "", 123)
	require.NoError(t, err)

	infoIn := streamMgr.GetStream("test_stream")
	require.NotNil(t, infoIn)

	var watermarkOper *opers.WaterMarkOperator
	firstOper := infoIn.Operators[0]
	producerEndpointOper, ok := firstOper.(*opers.KafkaInOperator)
	var ppm map[int]int
	if ok {
		watermarkOper = producerEndpointOper.GetWatermarkOperator()
		ppm = producerEndpointOper.InSchema().PartitionProcessorMapping
	} else {
		kafkaInOper, ok := firstOper.(*opers.BridgeFromOperator)
		if ok {
			watermarkOper = kafkaInOper.GetWatermarkOperator()
		} else {
			panic("wrong operator type")
		}
		ppm = kafkaInOper.InSchema().PartitionProcessorMapping
	}

	streamMgr.RegisterReceiverWithLock(receiverID, watermarkOper)

	gatheringOper := newGatheringWMOperator()
	infoIn.Operators[len(infoIn.Operators)-1].AddDownStreamOperator(gatheringOper)

	return gatheringOper, ppm
}

func waitUntilReceiveWaterMark(t *testing.T, watermark int, processorID int, gatheringOper *gatheringWMOperator) {
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		lastWM, ok := gatheringOper.getLastWaterMark(processorID)
		return ok && lastWM == watermark, nil
	}, 5*time.Second, 100*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
}

func createBatchWithMaxEventTime(etMaxUnixMillis int64, processorID int, receiverID int, partitionID int) *proc.ProcessBatch {
	dataIn := [][]any{
		{int64(0), types.NewTimestamp(etMaxUnixMillis - 100), []byte("key0"), nil, []byte("val0")},
		{int64(1), types.NewTimestamp(etMaxUnixMillis - 50), []byte("key1"), nil, []byte("val1")},
		{int64(2), types.NewTimestamp(etMaxUnixMillis), []byte("key2"), nil, []byte("val2")},
	}
	batch := createEventBatch(opers.KafkaSchema.ColumnNames(), opers.KafkaSchema.ColumnTypes(), dataIn)
	pb := proc.NewProcessBatch(processorID, batch, receiverID, partitionID, -1)
	return pb
}

func deployProcessors(t *testing.T, procMgr proc.Manager, count int) {
	var groupStates [][]clustmgr.GroupNode
	for i := 0; i < count; i++ {
		groupStates = append(groupStates, []clustmgr.GroupNode{
			{
				NodeID:        0,
				Leader:        true,
				Valid:         true,
				JoinedVersion: 123,
			},
		})
	}
	state := clustmgr.ClusterState{
		Version:     123,
		GroupStates: groupStates,
	}
	err := procMgr.HandleClusterState(state)
	require.NoError(t, err)
}

type testVmgrClient struct {
	initialWriteVersion     int
	initialCompletedVersion int
	initialFlushedVersion   int
}

func (t *testVmgrClient) FailureComplete(int, int) error {
	return nil
}

func (t *testVmgrClient) IsFailureComplete(int) (bool, error) {
	return false, nil
}

func (t *testVmgrClient) FailureDetected(int, int) error {
	return nil
}

func (t *testVmgrClient) GetLastFailureFlushedVersion(int) (int, error) {
	return 0, nil
}

func (t *testVmgrClient) VersionFlushed(int, int, int) error {
	return nil
}

func (t *testVmgrClient) VersionComplete(int, int, int, bool, func(error)) {
}

func (t *testVmgrClient) GetVersions() (int, int, int, error) {
	return t.initialWriteVersion, t.initialCompletedVersion, t.initialFlushedVersion, nil
}

func (t *testVmgrClient) Start() error {
	return nil
}

func (t *testVmgrClient) Stop() error {
	return nil
}

func createEventBatch(columnNames []string, columnTypes []types.ColumnType, data [][]any) *evbatch.Batch {
	schema := evbatch.NewEventSchema(columnNames, columnTypes)
	colBuilders := evbatch.CreateColBuilders(columnTypes)
	for _, row := range data {
		for j, colType := range columnTypes {
			colBuilder := colBuilders[j]
			if row[j] == nil {
				colBuilder.AppendNull()
				continue
			}
			switch colType.ID() {
			case types.ColumnTypeIDInt:
				colBuilder.(*evbatch.IntColBuilder).Append(row[j].(int64))
			case types.ColumnTypeIDFloat:
				colBuilder.(*evbatch.FloatColBuilder).Append(row[j].(float64))
			case types.ColumnTypeIDBool:
				colBuilder.(*evbatch.BoolColBuilder).Append(row[j].(bool))
			case types.ColumnTypeIDDecimal:
				colBuilder.(*evbatch.DecimalColBuilder).Append(row[j].(types.Decimal))
			case types.ColumnTypeIDString:
				colBuilder.(*evbatch.StringColBuilder).Append(row[j].(string))
			case types.ColumnTypeIDBytes:
				colBuilder.(*evbatch.BytesColBuilder).Append(row[j].([]byte))
			case types.ColumnTypeIDTimestamp:
				colBuilder.(*evbatch.TimestampColBuilder).Append(row[j].(types.Timestamp))
			default:
				panic("unknown type")
			}
		}
	}
	return evbatch.NewBatchFromBuilders(schema, colBuilders...)
}

func newGatheringWMOperator() *gatheringWMOperator {
	return &gatheringWMOperator{
		procLastWatermarks:   map[int]int{},
		partitionLastBatches: map[int]*evbatch.Batch{},
	}
}

type gatheringWMOperator struct {
	lock                 sync.Mutex
	procLastWatermarks   map[int]int
	partitionLastBatches map[int]*evbatch.Batch
}

func (g *gatheringWMOperator) HandleStreamBatch(batch *evbatch.Batch, execCtx opers.StreamExecContext) (*evbatch.Batch, error) {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.partitionLastBatches[execCtx.PartitionID()] = batch
	return nil, nil
}

func (g *gatheringWMOperator) HandleQueryBatch(*evbatch.Batch, opers.QueryExecContext) (*evbatch.Batch, error) {
	return nil, nil
}

func (g *gatheringWMOperator) HandleBarrier(execCtx opers.StreamExecContext) error {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.procLastWatermarks[execCtx.Processor().ID()] = execCtx.WaterMark()
	return nil
}

func (g *gatheringWMOperator) getLastWaterMark(processorID int) (int, bool) {
	g.lock.Lock()
	defer g.lock.Unlock()
	wm, ok := g.procLastWatermarks[processorID]
	return wm, ok
}

func (g *gatheringWMOperator) getLastPartitionBatch(partitionID int) (*evbatch.Batch, bool) {
	g.lock.Lock()
	defer g.lock.Unlock()
	batch, ok := g.partitionLastBatches[partitionID]
	return batch, ok
}

func (g *gatheringWMOperator) InSchema() *opers.OperatorSchema {
	return nil
}

func (g *gatheringWMOperator) OutSchema() *opers.OperatorSchema {
	return nil
}

func (g *gatheringWMOperator) Setup(opers.StreamManagerCtx) error {
	return nil
}

func (g *gatheringWMOperator) AddDownStreamOperator(opers.Operator) {
}

func (g *gatheringWMOperator) GetDownStreamOperators() []opers.Operator {
	return nil
}

func (g *gatheringWMOperator) RemoveDownStreamOperator(opers.Operator) {
}

func (g *gatheringWMOperator) GetParentOperator() opers.Operator {
	return nil
}

func (g *gatheringWMOperator) SetParentOperator(opers.Operator) {
}

func (g *gatheringWMOperator) SetStreamInfo(*opers.StreamInfo) {
}

func (g *gatheringWMOperator) GetStreamInfo() *opers.StreamInfo {
	return nil
}

func (g *gatheringWMOperator) Teardown(opers.StreamManagerCtx, *sync.RWMutex) {
}

type testSlabRetentions struct {
}

func (t testSlabRetentions) RegisterSlabRetention(slabID int, retention time.Duration) error {
	return nil
}

func (t testSlabRetentions) UnregisterSlabRetention(slabID int) error {
	return nil
}
