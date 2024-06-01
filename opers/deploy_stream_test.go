package opers

import (
	"fmt"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/kafka/fake"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	store2 "github.com/spirit-labs/tektite/store"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/tppm"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"math"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeployUndeployStream(t *testing.T) {
	mgr, _, store := createManager()
	defer stopStore(t, store)

	tsl1 := `test_stream1 := (store stream)`
	columnNames := []string{"event_time", "f1", "f2", "f3"}
	columnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString}
	deployStream(t, tsl1, mgr, columnNames, columnTypes, true, false)

	numStreams := mgr.numStreams()
	require.Equal(t, 1, numStreams)

	err := mgr.UndeployStream(parser.DeleteStreamDesc{StreamName: "test_stream1"}, 0)
	require.NoError(t, err)

	numStreams = mgr.numStreams()
	require.Equal(t, 0, numStreams)

	tsl2 := `test_stream2 := (aggregate sum(f2) by f1)`
	deployStream(t, tsl2, mgr, columnNames, columnTypes, true, false)

	numStreams = mgr.numStreams()
	require.Equal(t, 1, numStreams)

	err = mgr.UndeployStream(parser.DeleteStreamDesc{StreamName: "test_stream2"}, 0)
	require.NoError(t, err)

	numStreams = mgr.numStreams()
	require.Equal(t, 0, numStreams)

	tsl3 := `test_stream3 := (store table by f1)`
	deployStream(t, tsl3, mgr, columnNames, columnTypes, true, false)

	numStreams = mgr.numStreams()
	require.Equal(t, 1, numStreams)

	err = mgr.UndeployStream(parser.DeleteStreamDesc{StreamName: "test_stream3"}, 0)
	require.NoError(t, err)

	numStreams = mgr.numStreams()
	require.Equal(t, 0, numStreams)

	deployStream(t, tsl1, mgr, columnNames, columnTypes, true, false)
	deployStream(t, tsl2, mgr, columnNames, columnTypes, true, false)
	deployStream(t, tsl3, mgr, columnNames, columnTypes, true, false)

	numStreams = mgr.numStreams()
	require.Equal(t, 3, numStreams)

	err = mgr.UndeployStream(parser.DeleteStreamDesc{StreamName: "test_stream1"}, 0)
	require.NoError(t, err)
	err = mgr.UndeployStream(parser.DeleteStreamDesc{StreamName: "test_stream2"}, 0)
	require.NoError(t, err)
	err = mgr.UndeployStream(parser.DeleteStreamDesc{StreamName: "test_stream3"}, 0)
	require.NoError(t, err)

	numStreams = mgr.numStreams()
	require.Equal(t, 0, numStreams)
}

func TestCannotDeleteStreamWithDownstream(t *testing.T) {
	mgr, _, store := createManager()
	defer stopStore(t, store)

	tsl1 := `test_stream1 := (store stream)`
	deployStream(t, tsl1, mgr, []string{"offset", "event_time", "f1", "f2", "f3"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString},
		true, false)

	tsl1_1 := `test_stream1_1 := test_stream1->(project "f3", "f2")->(store stream)`
	deployStream(t, tsl1_1, mgr, nil, nil, false, false)

	tsl1_2 := `test_stream1_2 := test_stream1->(project "f2", "f3")->(store stream)`
	deployStream(t, tsl1_2, mgr, nil, nil, false, false)

	tsl1_1_1 := `test_stream1_1_1 := test_stream1_1->(project "f3")->(store stream)`
	deployStream(t, tsl1_1_1, mgr, nil, nil, false, false)

	numStreams := mgr.numStreams()
	require.Equal(t, 4, numStreams)

	err := mgr.UndeployStream(createDeleteStreamDesc(t, "test_stream1"), 0)
	require.Error(t, err)
	require.Equal(t, `cannot delete stream test_stream1 - it has child streams: [test_stream1_1 test_stream1_2] - they must be deleted first (line 1 column 8):
delete(test_stream1)
       ^`, err.Error())

	err = mgr.UndeployStream(createDeleteStreamDesc(t, "test_stream1_1"), 0)
	require.Error(t, err)
	require.Equal(t, `cannot delete stream test_stream1_1 - it has child streams: [test_stream1_1_1] - they must be deleted first (line 1 column 8):
delete(test_stream1_1)
       ^`, err.Error())

	pi := mgr.GetStream("test_stream1_1")
	require.Equal(t, 1, len(pi.DownstreamStreamNames))

	err = mgr.UndeployStream(createDeleteStreamDesc(t, "test_stream1_1_1"), 0)
	require.NoError(t, err)
	numStreams = mgr.numStreams()
	require.Equal(t, 3, numStreams)

	pi = mgr.GetStream("test_stream1_1")
	require.Equal(t, 0, len(pi.DownstreamStreamNames))

	err = mgr.UndeployStream(createDeleteStreamDesc(t, "test_stream1_1"), 0)
	require.NoError(t, err)

	numStreams = mgr.numStreams()
	require.Equal(t, 2, numStreams)

	err = mgr.UndeployStream(createDeleteStreamDesc(t, "test_stream1_2"), 0)
	require.NoError(t, err)

	numStreams = mgr.numStreams()
	require.Equal(t, 1, numStreams)

	err = mgr.UndeployStream(createDeleteStreamDesc(t, "test_stream1"), 0)
	require.NoError(t, err)

	numStreams = mgr.numStreams()
	require.Equal(t, 0, numStreams)
}

func createDeleteStreamDesc(t *testing.T, streamName string) parser.DeleteStreamDesc {
	tsl, err := parser.NewParser(nil).ParseTSL(fmt.Sprintf("delete(%s)", streamName))
	require.NoError(t, err)
	return *tsl.DeleteStream
}

func TestToStreamAlreadyHasOffset(t *testing.T) {
	mgr, pm, store := createManager()
	defer pm.Close()
	defer stopStore(t, store)

	pm.SetBatchHandler(mgr)

	tsl := `test_stream1 :=  (filter by f2 >= 2.1f) -> (store stream)`
	columnNames := []string{"offset", "f1", "f2", "f3"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString}

	pm.AddActiveProcessor(0)

	dataIn := [][]any{
		{int64(0), int64(10), float64(1.1), "foo1"},
		{int64(1), int64(5), float64(2.1), "foo2"},
		{int64(2), int64(13), float64(3.1), "foo3"},
	}

	deployStream(t, tsl, mgr, columnNames, columnTypes, true, true)

	injectBatch(t, "test_stream1", 0, 0, dataIn, mgr, pm)

	expectedOut := [][]any{
		{int64(1), int64(5), float64(2.1), "foo2"},
		{int64(2), int64(13), float64(3.1), "foo3"},
	}
	streamInfo := mgr.GetStream("test_stream1")
	require.NotNil(t, streamInfo)

	verifyRowsInTablePartition(t, []types.ColumnType{types.ColumnTypeInt}, []int{0},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString}, []int{1, 2, 3},
		expectedOut, "test_stream1", streamInfo.UserSlab.SlabID, 0, store)
}

func TestBackfillSinglePartition(t *testing.T) {
	mgr, pm, store := createManager()
	defer pm.Close()
	defer stopStore(t, store)
	pm.SetBatchHandler(mgr)

	tsl := `test_stream1 := (filter by f2 >= 2.1f)-> (store stream)`
	columnNames := []string{"offset", "event_time", "f1", "f2", "f3"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString}

	for procID := 0; procID < conf.DefaultProcessorCount; procID++ {
		pm.AddActiveProcessor(procID)
	}

	deployStream(t, tsl, mgr, columnNames, columnTypes, true, false)

	info := mgr.GetStream("test_stream1")
	require.NotNil(t, info)
	lastOper := info.Operators[len(info.Operators)-1]
	ppm := lastOper.OutSchema().PartitionScheme.PartitionProcessorMapping

	dataIn := [][]any{
		{int64(0), types.NewTimestamp(0), int64(10), float64(1.1), "foo1"},
		{int64(1), types.NewTimestamp(1), int64(5), float64(2.1), "foo2"},
		{int64(2), types.NewTimestamp(2), int64(13), float64(3.1), "foo3"},
	}
	processorID, ok := ppm[0]
	require.True(t, ok)
	injectBatch(t, "test_stream1", 0, processorID, dataIn, mgr, pm)

	expectedOut := [][]any{
		{int64(2), types.NewTimestamp(2), int64(13), float64(3.1), "foo3"},
	}

	tsl = `test_stream2 := test_stream1 -> (backfill) -> (filter by f2 > 2.1f)`
	deployStream(t, tsl, mgr, columnNames, columnTypes, false, true)

	verifyReceivedData(t, "test_stream2", 0, expectedOut, mgr)
}

func TestBackfillAllProcessorsAndPartitions(t *testing.T) {
	mgr, pm, store := createManager()
	defer pm.Close()
	defer stopStore(t, store)
	pm.SetBatchHandler(mgr)

	tsl := `test_stream1 := (store stream)`
	columnNames := []string{"offset", "event_time", "f1", "f2", "f3"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString}

	for procID := 0; procID < conf.DefaultProcessorCount; procID++ {
		pm.AddActiveProcessor(procID)
	}

	deployStream(t, tsl, mgr, columnNames, columnTypes, true, false)

	info := mgr.GetStream("test_stream1")
	require.NotNil(t, info)
	lastOper := info.Operators[len(info.Operators)-1]
	ppm := lastOper.OutSchema().PartitionScheme.PartitionProcessorMapping

	var partsData [][][]any
	for partID := 0; partID < 9; partID++ {
		partData := [][]any{
			{int64(0), types.NewTimestamp(0), int64(partID + 10), float64(1.1), "foo1"},
			{int64(1), types.NewTimestamp(1), int64(partID + 11), float64(2.1), "foo2"},
			{int64(2), types.NewTimestamp(2), int64(partID + 12), float64(3.1), "foo3"},
		}
		partsData = append(partsData, partData)
		processorID, ok := ppm[partID]
		require.True(t, ok)
		injectBatch(t, "test_stream1", partID, processorID, partData, mgr, pm)
	}

	tsl = `test_stream2 := test_stream1 -> (backfill)`
	deployStream(t, tsl, mgr, columnNames, columnTypes, false, true)

	for partID, partData := range partsData {
		verifyReceivedData(t, "test_stream2", partID, partData, mgr)
	}
}

func TestBackfillDeployBeforeFillData(t *testing.T) {
	mgr, pm, store := createManager()
	defer pm.Close()
	defer stopStore(t, store)
	pm.SetBatchHandler(mgr)

	tsl := `test_stream1 :=  (filter by f2 >= 2.1f) -> (store stream)`
	columnNames := []string{"offset", "event_time", "f1", "f2", "f3"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString}

	for procID := 0; procID < conf.DefaultProcessorCount; procID++ {
		pm.AddActiveProcessor(procID)
	}

	deployStream(t, tsl, mgr, columnNames, columnTypes, true, false)

	tsl = `test_stream2 := test_stream1 -> (backfill) -> (filter by f2 > 2.1f)`
	deployStream(t, tsl, mgr, columnNames, columnTypes, false, true)

	dataIn := [][]any{
		{int64(0), types.NewTimestamp(0), int64(10), float64(1.1), "foo1"},
		{int64(1), types.NewTimestamp(1), int64(5), float64(2.1), "foo2"},
		{int64(2), types.NewTimestamp(2), int64(13), float64(3.1), "foo3"},
	}
	info := mgr.GetStream("test_stream2")
	lastOper := info.Operators[len(info.Operators)-1]
	ppm := lastOper.OutSchema().PartitionScheme.ProcessorPartitionMapping

	var processorID, partitionID int
	for procID, pids := range ppm {
		if len(pids) > 0 {
			processorID = procID
			partitionID = pids[0]
			break
		}
	}

	injectBatch(t, "test_stream1", partitionID, processorID, dataIn, mgr, pm)

	expectedOut := [][]any{
		{int64(2), types.NewTimestamp(2), int64(13), float64(3.1), "foo3"},
	}
	verifyReceivedData(t, "test_stream2", partitionID, expectedOut, mgr)
}

func TestTableSingleColKey(t *testing.T) {
	mgr, pm, store := createManager()
	defer stopStore(t, store)
	defer pm.Close()
	pm.SetBatchHandler(mgr)

	tsl := `test_stream1 := (store table by f1)`
	columnNames := []string{"f0", "f1", "f2", "f3"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString}

	pm.AddActiveProcessor(0)

	deployStream(t, tsl, mgr, columnNames, columnTypes, true, true)

	dataIn := [][]any{
		{int64(0), int64(10), float64(1.1), "foo1"},
		{int64(1), int64(5), float64(2.1), "foo2"},
		{int64(2), int64(13), float64(3.1), "foo3"},
	}

	injectBatch(t, "test_stream1", 0, 0, dataIn, mgr, pm)

	expectedOut := [][]any{
		{int64(1), int64(5), float64(2.1), "foo2"},
		{int64(0), int64(10), float64(1.1), "foo1"},
		{int64(2), int64(13), float64(3.1), "foo3"},
	}

	streamInfo := mgr.GetStream("test_stream1")
	require.NotNil(t, streamInfo)

	verifyRowsInTablePartition(t, []types.ColumnType{types.ColumnTypeInt}, []int{1},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString}, []int{0, 2, 3},
		expectedOut, "test_stream1", streamInfo.UserSlab.SlabID, 0, store)

	dataIn = [][]any{
		{int64(0), int64(10), float64(1.1), "foo1"},
		{int64(11), int64(5), float64(13.1), "foo22"},
		{int64(12), int64(13), float64(23.1), "foo33"},
	}
	injectBatch(t, "test_stream1", 0, 0, dataIn, mgr, pm)
	expectedOut = [][]any{
		{int64(11), int64(5), float64(13.1), "foo22"},
		{int64(0), int64(10), float64(1.1), "foo1"},
		{int64(12), int64(13), float64(23.1), "foo33"},
	}

	verifyRowsInTablePartition(t, []types.ColumnType{types.ColumnTypeInt}, []int{1},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString}, []int{0, 2, 3},
		expectedOut, "test_stream1", streamInfo.UserSlab.SlabID, 0, store)
}

func TestTableMultipleColKey(t *testing.T) {
	mgr, pm, store := createManager()
	defer stopStore(t, store)
	defer pm.Close()
	pm.SetBatchHandler(mgr)

	tsl := `test_stream1 := (store table by f1,f3)`
	columnNames := []string{"f0", "f1", "f2", "f3"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString}

	pm.AddActiveProcessor(0)

	deployStream(t, tsl, mgr, columnNames, columnTypes, true, true)

	dataIn := [][]any{
		{int64(0), int64(10), float64(1.1), "foo1"},
		{int64(1), int64(10), float64(2.1), "foo2"},
		{int64(2), int64(13), float64(3.1), "foo3"},
	}

	injectBatch(t, "test_stream1", 0, 0, dataIn, mgr, pm)

	expectedOut := [][]any{
		{int64(0), int64(10), float64(1.1), "foo1"},
		{int64(1), int64(10), float64(2.1), "foo2"},
		{int64(2), int64(13), float64(3.1), "foo3"},
	}
	streamInfo := mgr.GetStream("test_stream1")
	require.NotNil(t, streamInfo)

	verifyRowsInTablePartition(t, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString}, []int{1, 3},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat}, []int{0, 2},
		expectedOut, "test_stream1", streamInfo.UserSlab.SlabID, 0, store)

	dataIn = [][]any{
		{int64(0), int64(10), float64(1.1), "foo1"},
		{int64(1), int64(10), float64(22.1), "foo2"},
		{int64(2), int64(13), float64(23.1), "foo3"},
	}
	injectBatch(t, "test_stream1", 0, 0, dataIn, mgr, pm)
	expectedOut = [][]any{
		{int64(0), int64(10), float64(1.1), "foo1"},
		{int64(1), int64(10), float64(22.1), "foo2"},
		{int64(2), int64(13), float64(23.1), "foo3"},
	}
	verifyRowsInTablePartition(t, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString}, []int{1, 3},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat}, []int{0, 2},
		expectedOut, "test_stream1", streamInfo.UserSlab.SlabID, 0, store)
}

func TestAggregate(t *testing.T) {
	mgr, pm, store := createManager()
	defer stopStore(t, store)
	defer pm.Close()
	pm.SetBatchHandler(mgr)

	columnNames := []string{"event_time", "f0", "f1", "f2", "f3"}
	columnTypes := []types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeInt, types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString}

	pm.AddActiveProcessor(0)

	tsl := `test_stream1 := (aggregate sum(f2), count(f3) by f1)`
	deployStream(t, tsl, mgr, columnNames, columnTypes, true, true)

	dataIn := [][]any{
		{types.NewTimestamp(1000), int64(0), int64(10), float64(1.1), "foo1"},
		{types.NewTimestamp(1000), int64(1), int64(10), float64(2.1), "foo2"},
		{types.NewTimestamp(1000), int64(2), int64(13), float64(3.1), "foo3"},
		{types.NewTimestamp(1000), int64(3), int64(13), float64(3.2), "foo4"},
		{types.NewTimestamp(1000), int64(4), int64(14), float64(4.1), "foo5"},
	}

	injectBatch(t, "test_stream1", 0, 0, dataIn, mgr, pm)

	expectedOut := [][]any{
		{types.NewTimestamp(1000), int64(10), float64(3.2), int64(2)},
		{types.NewTimestamp(1000), int64(13), float64(6.300000000000001), int64(2)},
		{types.NewTimestamp(1000), int64(14), float64(4.1), int64(1)},
	}
	streamInfo := mgr.GetStream("test_stream1")
	require.NotNil(t, streamInfo)

	verifyRowsInTablePartition(t, []types.ColumnType{types.ColumnTypeInt}, []int{1},
		[]types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeFloat, types.ColumnTypeInt}, []int{0, 2, 3},
		expectedOut, "test_stream1", streamInfo.UserSlab.SlabID, 0, store)

	dataIn = [][]any{
		{types.NewTimestamp(1000), int64(5), int64(10), float64(3.3), "foo6"},
		{types.NewTimestamp(1000), int64(7), int64(13), float64(6.1), "foo7"},
		{types.NewTimestamp(1000), int64(8), int64(13), float64(7.2), "foo8"},
		{types.NewTimestamp(1000), int64(9), int64(17), float64(8.1), "foo9"},
	}
	injectBatch(t, "test_stream1", 0, 0, dataIn, mgr, pm)
	expectedOut = [][]any{
		{types.NewTimestamp(1000), int64(10), float64(6.5), int64(3)},
		{types.NewTimestamp(1000), int64(13), float64(19.600000000000001), int64(4)},
		{types.NewTimestamp(1000), int64(14), float64(4.1), int64(1)},
		{types.NewTimestamp(1000), int64(17), float64(8.1), int64(1)},
	}

	verifyRowsInTablePartition(t, []types.ColumnType{types.ColumnTypeInt}, []int{1},
		[]types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeFloat, types.ColumnTypeInt}, []int{0, 2, 3},
		expectedOut, "test_stream1", streamInfo.UserSlab.SlabID, 0, store)
}

func TestDeployStreamAlreadyExists(t *testing.T) {
	mgr, _, _ := createManager()
	tsl := `test_stream1 :=  (filter by f1 >= 2) -> (store stream)`
	columnNames := []string{"f1", "f2", "f3"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString}
	deployStream(t, tsl, mgr, columnNames, columnTypes, true, false)
	err := deployStreamReturnError(t, tsl, mgr, columnNames, columnTypes, true, false)
	require.Error(t, err)
	require.Equal(t, `stream 'test_stream1' already exists (line 1 column 1):
test_stream1 :=  (filter by f1 >= 2) -> (store stream)
^`, err.Error())
}

func TestUndeployStreamDoesNotExist(t *testing.T) {
	mgr, _, store := createManager()
	defer stopStore(t, store)
	err := mgr.UndeployStream(createDeleteStreamDesc(t, "does_not_exist"), 0)
	require.Error(t, err)
	require.Equal(t, `unknown stream 'does_not_exist' (line 1 column 8):
delete(does_not_exist)
       ^`, err.Error())
}

func TestStreamStart(t *testing.T) {
	mgr, _, st := createManager()
	//goland:noinspection GoUnhandledErrorResult
	defer st.Stop()
	tsl := `test_stream1 := (filter by f1 >= 2)`
	columnNames := []string{"f1", "f2", "f3"}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeString}
	err := deployStreamReturnError(t, tsl, mgr, columnNames, columnTypes, false, false)
	require.Error(t, err)
	require.Equal(t, `'filter' cannot be the first operator in a stream (line 1 column 18):
test_stream1 := (filter by f1 >= 2)
                 ^`, err.Error())
}

func TestStartStopIngest(t *testing.T) {
	fakeKafka := &fake.Kafka{}
	numPartitions := 40
	_, err := fakeKafka.CreateTopic("test_topic", numPartitions)
	require.NoError(t, err)

	mgr, tpm, store := createManagerWithFakeFafka(fakeKafka)
	defer stopStore(t, store)

	require.Equal(t, false, mgr.ingestEnabled.Load())

	tsl := fmt.Sprintf(`test_stream1 := 
		(bridge from test_topic	partitions = %d	props = ()) -> (store stream)`, numPartitions)
	deployStream(t, tsl, mgr, nil, nil, false, false)

	info := mgr.GetStream("test_stream1")
	require.NotNil(t, info)

	fk := info.Operators[0].(*BridgeFromOperator)
	consumers := fk.getConsumers()
	require.Equal(t, 0, len(consumers))

	require.Equal(t, false, mgr.ingestEnabled.Load())

	expectedMapping := CalcProcessorPartitionMapping("_default_", numPartitions, conf.DefaultProcessorCount)
	// Activate each processor
	procCount := 0
	for processorID := range expectedMapping {
		tpm.AddActiveProcessor(processorID)
		procCount++
	}

	require.Equal(t, false, mgr.ingestEnabled.Load())
	consumers = fk.getConsumers()
	require.Equal(t, procCount, len(consumers))
	for _, consumer := range consumers {
		require.True(t, consumer.paused)
	}

	err = mgr.StartIngest(100)
	require.NoError(t, err)

	consumers = fk.getConsumers()
	require.Equal(t, procCount, len(consumers))
	for _, consumer := range consumers {
		require.False(t, consumer.paused)
	}

	err = mgr.StopIngest()

	consumers = fk.getConsumers()
	require.Equal(t, procCount, len(consumers))
	for _, consumer := range consumers {
		require.True(t, consumer.paused)
	}
}

func TestDeployKafkaInWhenIngestEnabled(t *testing.T) {
	testDeployKafkaInWhenIngestEnabled(t, true)
}

func TestDeployKafkaInWhenIngestNotEnabled(t *testing.T) {
	testDeployKafkaInWhenIngestEnabled(t, false)
}

func testDeployKafkaInWhenIngestEnabled(t *testing.T, enabled bool) {
	fakeKafka := &fake.Kafka{}
	numPartitions := 40
	_, err := fakeKafka.CreateTopic("test_topic", numPartitions)
	require.NoError(t, err)

	mgr, tpm, store := createManagerWithFakeFafka(fakeKafka)
	defer stopStore(t, store)

	if enabled {
		err := mgr.StartIngest(100)
		require.NoError(t, err)
	}
	require.Equal(t, enabled, mgr.ingestEnabled.Load())

	tsl := fmt.Sprintf(`test_stream1 := (bridge from test_topic partitions = %d props = ()) -> (store stream)`, numPartitions)
	deployStream(t, tsl, mgr, nil, nil, false, false)

	info := mgr.GetStream("test_stream1")
	require.NotNil(t, info)

	fk := info.Operators[0].(*BridgeFromOperator)
	consumers := fk.getConsumers()
	require.Equal(t, 0, len(consumers))

	expectedMapping := CalcProcessorPartitionMapping("_default_", numPartitions, conf.DefaultProcessorCount)
	// Activate each processor
	procCount := 0
	for processorID := range expectedMapping {
		tpm.AddActiveProcessor(processorID)
		procCount++
	}

	consumers = fk.getConsumers()
	require.Equal(t, procCount, len(consumers))
	for _, consumer := range consumers {
		require.Equal(t, !enabled, consumer.paused)
	}
}

func stopStore(t *testing.T, store *store2.Store) {
	err := store.Stop()
	require.NoError(t, err)
}

func createManager() (*streamManager, *tppm.TestProcessorManager, *store2.Store) {
	return createManagerWithFakeFafka(nil)
}

func createManagerWithFakeFafka(fk *fake.Kafka) (*streamManager, *tppm.TestProcessorManager, *store2.Store) {
	store := store2.TestStore()
	pm := tppm.NewTestProcessorManager(store)

	if err := store.Start(); err != nil {
		panic(err)
	}
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	plm := NewStreamManager(fake.NewFakeMessageClientFactory(fk), store, &dummySlabRetentions{}, &expr.ExpressionFactory{}, cfg, false)
	plm.SetProcessorManager(pm)
	plm.Loaded()
	return plm.(*streamManager), pm, store
}

func deployStream(t *testing.T, tsl string, mgr StreamManager, injectedColumnNames []string, injectedColumnTypes []types.ColumnType,
	testSource bool, testSink bool) {
	err := deployStreamReturnError(t, tsl, mgr, injectedColumnNames, injectedColumnTypes, testSource, testSink)
	require.NoError(t, err)
}

var streamSeqStartSeq uint64

func getNextSeqStart(num int) int {
	return int(atomic.AddUint64(&streamSeqStartSeq, uint64(num)))
}

func deployStreamReturnError(t *testing.T, tsl string, mgr StreamManager, injectedColumnNames []string, injectedColumnTypes []types.ColumnType,
	testSource bool, testSink bool) error {
	seqStart := getNextSeqStart(3)
	cp, err := parser.NewParser(nil).ParseTSL(tsl)
	require.NoError(t, err, tsl)
	if testSink {
		cp.CreateStream.TestSink = true
	}
	if testSource {
		cp.CreateStream.TestSource = true
		// insert a test source operator so we can inject rows
		operDesc := &TestSourceDesc{
			ColumnNames: injectedColumnNames,
			ColumnTypes: injectedColumnTypes,
			Partitions:  10,
		}
		cp.CreateStream.OperatorDescs = append([]parser.Parseable{operDesc}, cp.CreateStream.OperatorDescs...)
	}
	receiverSeqs := []int{seqStart, seqStart + 1, seqStart + 2}
	slabSeqs := []int{seqStart, seqStart + 1, seqStart + 2}
	return mgr.DeployStream(*cp.CreateStream, receiverSeqs, slabSeqs, "", 123)
}

func injectBatch(t *testing.T, streamNameIn string, partitionID int, processorID int, dataIn [][]any, mgr *streamManager,
	pm *tppm.TestProcessorManager) {
	injectBatchWithSchema(t, streamNameIn, partitionID, processorID, dataIn, mgr, pm)
}

func injectBatchWithSchema(t *testing.T, streamNameIn string, partitionID int, processorID int, dataIn [][]any,
	mgr *streamManager, pm *tppm.TestProcessorManager) {
	infoIn := mgr.GetStream(streamNameIn)
	require.NotNil(t, infoIn)
	firstOper := infoIn.Operators[0]
	sourceOper, ok := firstOper.(*testSourceOper)
	require.True(t, ok)
	batch := createEventBatch(firstOper.InSchema().EventSchema.ColumnNames(),
		firstOper.InSchema().EventSchema.ColumnTypes(), dataIn)
	processor := pm.GetProcessor(processorID)

	pb := proc.NewProcessBatch(processorID, batch, sourceOper.receiverID, partitionID, -1)
	err := processor.IngestBatchSync(pb)
	require.NoError(t, err)
}

func verifyReceivedData(t *testing.T, streamNameOut string, partitionID int, expectedDataOut [][]any, mgr StreamManager) {
	infoOut := mgr.GetStream(streamNameOut)
	require.NotNil(t, infoOut)
	lastOper := infoOut.Operators[len(infoOut.Operators)-1]
	sinkOper, ok := lastOper.(*testSinkOper)
	require.True(t, ok)
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		outBatches := sinkOper.GetPartitionBatches()[partitionID]
		if len(outBatches) == 0 {
			if len(expectedDataOut) == 0 {
				return true, nil
			}
			return false, nil
		}
		require.Equal(t, 1, len(outBatches))
		outBatch := outBatches[0]
		actualOutData := convertBatchToAnyArray(outBatch)
		log.Debugf("got actual out: %v", actualOutData)
		log.Debugf("expected: %v", expectedDataOut)
		return reflect.DeepEqual(expectedDataOut, actualOutData), nil
	}, 10*time.Second, 1*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
}

func verifyRowsInTablePartition(t *testing.T, keyColumnTypes []types.ColumnType, outKeyColIndexes []int,
	rowColumnTypes []types.ColumnType, outRowColIndexes []int, data [][]any, mappingID string, slabID int, partitionID int, store *store2.Store) {
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		actualOut := loadRowsFromPartition(t, keyColumnTypes, outKeyColIndexes, rowColumnTypes, outRowColIndexes,
			mappingID, slabID, partitionID, store)
		return reflect.DeepEqual(data, actualOut), nil
	}, 5*time.Second, 5*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
}

func loadRowsFromPartition(t *testing.T, keyColumnTypes []types.ColumnType, outKeyColIndexes []int,
	rowColumnTypes []types.ColumnType, outRowColIndexes []int, mappingID string, slabID int,
	partitionID int, store *store2.Store) [][]any {
	partitionHash := proc.CalcPartitionHash(mappingID, uint64(partitionID))
	keyStart := encoding.EncodeEntryPrefix(partitionHash, uint64(slabID), 24)
	keyEnd := encoding.EncodeEntryPrefix(partitionHash, uint64(slabID)+1, 24)
	iter, err := store.NewIterator(keyStart, keyEnd, math.MaxInt64, false)
	require.NoError(t, err)
	defer iter.Close()
	numCols := len(keyColumnTypes) + len(rowColumnTypes)
	var actualOut [][]any
	for {
		valid, err := iter.IsValid()
		require.NoError(t, err)
		if !valid {
			break
		}
		curr := iter.Current()
		row := make([]any, numCols)
		keySlice, _, err := encoding.DecodeKeyToSlice(curr.Key, 24, keyColumnTypes)
		require.NoError(t, err)
		rowSlice, _ := encoding.DecodeRowToSlice(curr.Value, 0, rowColumnTypes)
		for i, keyCol := range outKeyColIndexes {
			row[keyCol] = keySlice[i]
		}
		for i, rowCol := range outRowColIndexes {
			row[rowCol] = rowSlice[i]
		}
		actualOut = append(actualOut, row)
		err = iter.Next()
		require.NoError(t, err)
	}
	return actualOut
}
