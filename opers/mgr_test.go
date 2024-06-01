package opers

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/kafka"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/retention"
	store2 "github.com/spirit-labs/tektite/store"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/tppm"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
	"time"
)

func TestBridgeFromOperSingleProcessorMultiplePartitions(t *testing.T) {
	msgs := [][]*kafka.Message{
		{
			createKafkaMessage(0, 0, "key1_1", "val1_1", 1001),
			createKafkaMessage(0, 1, "key1_2", "val1_2", 1002),
			createKafkaMessage(0, 2, "key1_3", "val1_3", 1003),
		},
		{
			createKafkaMessage(1, 0, "key2_1", "val2_1", 1004),
			createKafkaMessage(1, 1, "key2_2", "val2_2", 1006),
			createKafkaMessage(1, 2, "key2_3", "val2_3", 1001),
		},
		{
			createKafkaMessage(2, 0, "key3_1", "val3_1", 1007),
			createKafkaMessage(2, 1, "key3_2", "val3_2", 1008),
			createKafkaMessage(2, 2, "key3_3", "val3_3", 1009),
		},
	}
	testBridgeFromOper(t, msgs, 0)
}

func TestBridgeFromOperMultipleProcessorsMultiplePartitions(t *testing.T) {
	msgs := [][]*kafka.Message{
		{
			createKafkaMessage(0, 0, "key1_1", "val1_1", 1001),
			createKafkaMessage(0, 1, "key1_2", "val1_2", 1002),
			createKafkaMessage(0, 2, "key1_3", "val1_3", 1003),
		},
		{
			createKafkaMessage(1, 0, "key2_1", "val2_1", 1004),
			createKafkaMessage(1, 1, "key2_2", "val2_2", 1006),
			createKafkaMessage(1, 2, "key2_3", "val2_3", 1001),
		},
		{
			createKafkaMessage(2, 0, "key3_1", "val3_1", 1007),
			createKafkaMessage(2, 1, "key3_2", "val3_2", 1008),
			createKafkaMessage(2, 2, "key3_3", "val3_3", 1009),
		},
		{
			createKafkaMessage(3, 0, "key4_1", "val4_1", 1001),
			createKafkaMessage(3, 1, "key4_2", "val4_2", 1002),
			createKafkaMessage(3, 2, "key4_3", "val4_3", 1003),
		},
		{
			createKafkaMessage(4, 0, "key5_1", "val5_1", 1004),
			createKafkaMessage(4, 1, "key5_2", "val5_2", 1006),
			createKafkaMessage(4, 2, "key5_3", "val5_3", 1001),
		},
		{
			createKafkaMessage(5, 0, "key6_1", "val6_1", 1007),
			createKafkaMessage(5, 1, "key6_2", "val6_2", 1008),
			createKafkaMessage(5, 2, "key6_3", "val6_3", 1009),
		},
	}
	testBridgeFromOper(t, msgs, 0)
}

func TestBridgeFromOperMultipleProcessorsMultiplePartitionsFailureOnCreate(t *testing.T) {
	msgs := [][]*kafka.Message{
		{
			createKafkaMessage(0, 0, "key1_1", "val1_1", 1001),
			createKafkaMessage(0, 1, "key1_2", "val1_2", 1002),
			createKafkaMessage(0, 2, "key1_3", "val1_3", 1003),
		},
		{
			createKafkaMessage(1, 0, "key2_1", "val2_1", 1004),
			createKafkaMessage(1, 1, "key2_2", "val2_2", 1006),
			createKafkaMessage(1, 2, "key2_3", "val2_3", 1001),
		},
		{
			createKafkaMessage(2, 0, "key3_1", "val3_1", 1007),
			createKafkaMessage(2, 1, "key3_2", "val3_2", 1008),
			createKafkaMessage(2, 2, "key3_3", "val3_3", 1009),
		},
		{
			createKafkaMessage(3, 0, "key4_1", "val4_1", 1001),
			createKafkaMessage(3, 1, "key4_2", "val4_2", 1002),
			createKafkaMessage(3, 2, "key4_3", "val4_3", 1003),
		},
		{
			createKafkaMessage(4, 0, "key5_1", "val5_1", 1004),
			createKafkaMessage(4, 1, "key5_2", "val5_2", 1006),
			createKafkaMessage(4, 2, "key5_3", "val5_3", 1001),
		},
		{
			createKafkaMessage(5, 0, "key6_1", "val6_1", 1007),
			createKafkaMessage(5, 1, "key6_2", "val6_2", 1008),
			createKafkaMessage(5, 2, "key6_3", "val6_3", 1009),
		},
	}
	testBridgeFromOper(t, msgs, 3)
}

func testBridgeFromOper(t *testing.T, msgs [][]*kafka.Message, numFailuresToCreate int) {

	store := store2.TestStore()
	err := store.Start()
	require.NoError(t, err)
	defer func() {
		err := store.Stop()
		require.NoError(t, err)
	}()
	pm := tppm.NewTestProcessorManager(store)
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	fact := msgClientFact{msgs: msgs,
		numFailuresToCreate: numFailuresToCreate}
	mgr := NewStreamManager(fact.createTestMessageClient, store, &dummyPrefixRetention{}, &expr.ExpressionFactory{}, cfg, false)
	mgr.SetProcessorManager(pm)
	mgr.Loaded()
	pm.SetBatchHandler(mgr)
	err = mgr.StartIngest(0)
	require.NoError(t, err)

	topicName := "test_topic"

	numPartitions := len(msgs)

	stream := parser.CreateStreamDesc{
		StreamName: "test_pipeline",
		OperatorDescs: []parser.Parseable{
			&parser.BridgeFromDesc{
				TopicName: topicName,
				Props: map[string]string{
					`prop1`: `val1`,
					`prop2`: `val2`,
					`prop3`: `val3`,
				},
				Partitions: numPartitions,
			},
		},
		TestSink: true,
	}

	receiverID := 1001
	receiverSeqs := []int{receiverID}
	commandID := int64(1234)
	err = mgr.DeployStream(stream, receiverSeqs, []int{2001}, "", commandID)
	require.NoError(t, err)

	pi := mgr.GetStream("test_pipeline")
	require.NotNil(t, pi)
	require.Equal(t, commandID, pi.CommandID)
	sinkOper := pi.Operators[len(pi.Operators)-1].(*testSinkOper)

	expectedMapping := CalcProcessorPartitionMapping("_default_", numPartitions, *cfg.ProcessorCount)
	// Activate each processor
	for processorID := range expectedMapping {
		pm.AddActiveProcessor(processorID)
	}
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		processorBatches := sinkOper.GetProcessorBatches()
		numBatches := 0
		for _, b := range processorBatches {
			numBatches += len(b)
		}
		// Should be one batch from each partition
		return numBatches == numPartitions, nil
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	processorBatches := sinkOper.GetProcessorBatches()

	for processorID, parts := range expectedMapping {
		receivedBatches := processorBatches[processorID]
		require.Equal(t, 1, len(receivedBatches))
		expectedMsgs := calcExpectedMessages(msgs, parts)
		receivedBatch := receivedBatches[0]
		require.Equal(t, len(expectedMsgs), receivedBatch.RowCount)
		for i, msg := range expectedMsgs {
			eventTime := receivedBatch.GetTimestampColumn(1).Get(i)
			key := receivedBatch.GetBytesColumn(2).Get(i)
			require.Equal(t, []byte{0}, receivedBatch.GetBytesColumn(3).Get(i))
			value := receivedBatch.GetBytesColumn(4).Get(i)
			require.Equal(t, msg.Key, key)
			require.Equal(t, msg.Value, value)
			require.Equal(t, msg.TimeStamp.In(time.UTC).UnixMilli(), eventTime.Val)
		}
	}

	ki := pi.Operators[0].(*BridgeFromOperator)
	require.Equal(t, len(expectedMapping), ki.numConsumers())

	var props = map[string]string{
		"prop1": "val1",
		"prop2": "val2",
		"prop3": "val3",
	}
	for _, consumer := range ki.getConsumers() {
		msgProvider := consumer.consumer.msgProvider.(*testMessageProvider)
		require.Equal(t, topicName, msgProvider.topicName)
		require.Equal(t, props, msgProvider.properties)
	}
}

func TestAddRemoveProcessors(t *testing.T) {
	st := store2.TestStore()
	pm := tppm.NewTestProcessorManager(st)
	defer pm.Close()
	err := st.Start()
	require.NoError(t, err)
	defer stopStore(t, st)

	msgs := [][]*kafka.Message{
		{createKafkaMessage(0, 0, "key1_1", "val1_1", 1001)},
		{createKafkaMessage(1, 0, "key2_1", "val2_1", 1004)},
		{createKafkaMessage(2, 0, "key3_1", "val3_1", 1007)},
		{createKafkaMessage(3, 0, "key4_1", "val4_1", 1001)},
		{createKafkaMessage(4, 0, "key5_1", "val5_1", 1004)},
		{createKafkaMessage(5, 0, "key6_1", "val6_1", 1007)},
		{createKafkaMessage(6, 0, "key7_1", "val7_1", 1001)},
		{createKafkaMessage(7, 0, "key8_1", "val8_1", 1004)},
		{createKafkaMessage(8, 0, "key9_1", "val9_1", 1007)},
	}
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	fact := msgClientFact{msgs: msgs}
	mgr := NewStreamManager(fact.createTestMessageClient, st, &dummyPrefixRetention{}, &expr.ExpressionFactory{}, cfg, false)
	mgr.SetProcessorManager(pm)
	mgr.Loaded()
	pm.SetBatchHandler(mgr)
	numPartitions := len(msgs)
	stream := parser.CreateStreamDesc{
		StreamName: "test_stream",
		OperatorDescs: []parser.Parseable{
			&parser.BridgeFromDesc{
				TopicName: "test_topic",
				Props: map[string]string{
					`"prop1"`: `"val1"`,
					`"prop2"`: `"val2"`,
					`"prop3"`: `"val3"`,
				},
				Partitions: numPartitions,
			},
		},
		TestSink: true,
	}

	commandID := int64(23)
	receiverID := 1001
	err = mgr.DeployStream(stream, []int{receiverID}, []int{2001}, "", commandID)
	require.NoError(t, err)

	pi := mgr.GetStream("test_stream")
	require.NotNil(t, pi)
	ki := pi.Operators[0].(*BridgeFromOperator)
	require.Equal(t, 0, ki.numConsumers())

	var processorIDs []int
	expectedMapping := CalcProcessorPartitionMapping("_default_", numPartitions, *cfg.ProcessorCount)
	// Gather some processor ids
	for processorID := range expectedMapping {
		processorIDs = append(processorIDs, processorID)
		if len(processorIDs) == 3 {
			break
		}
	}

	pm.AddActiveProcessor(processorIDs[0])
	require.Equal(t, 1, ki.numConsumers())

	pm.AddActiveProcessor(processorIDs[1])
	require.Equal(t, 2, ki.numConsumers())

	pm.RemoveActiveProcessor(processorIDs[0])
	require.Equal(t, 1, ki.numConsumers())

	pm.AddActiveProcessor(processorIDs[0])
	require.Equal(t, 2, ki.numConsumers())

	pm.AddActiveProcessor(processorIDs[2])
	require.Equal(t, 3, ki.numConsumers())
}

func TestGetInjectableReceivers(t *testing.T) {
	st := store2.TestStore()
	pm := tppm.NewTestProcessorManager(st)
	defer pm.Close()
	//goland:noinspection GoUnhandledErrorResult
	st.Start()
	//goland:noinspection GoUnhandledErrorResult
	defer st.Stop()

	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	mgr := NewStreamManager(nil, st, &dummyPrefixRetention{}, &expr.ExpressionFactory{}, cfg, false).(*streamManager)
	mgr.SetProcessorManager(pm)
	mgr.Loaded()
	pm.SetBatchHandler(mgr)

	require.Equal(t, []int{common.DummyReceiverID}, mgr.GetInjectableReceivers(0))

	numPartitions1 := 5
	stream1 := parser.CreateStreamDesc{
		StreamName: "test_stream1",
		OperatorDescs: []parser.Parseable{
			&parser.BridgeFromDesc{
				TopicName: "test_topic",
				Props: map[string]string{
					`"prop1"`: `"val1"`,
					`"prop2"`: `"val2"`,
					`"prop3"`: `"val3"`,
				},
				Partitions: numPartitions1,
			},
		},
		TestSink: true,
	}

	receiverID1 := 1001
	err := mgr.DeployStream(stream1, []int{receiverID1}, []int{2001}, "", 23)
	require.NoError(t, err)

	numPartitions2 := 5
	stream2 := parser.CreateStreamDesc{
		StreamName: "test_stream2",
		OperatorDescs: []parser.Parseable{
			&parser.BridgeFromDesc{
				TopicName: "test_topic",
				Props: map[string]string{
					`"prop1"`: `"val1"`,
					`"prop2"`: `"val2"`,
					`"prop3"`: `"val3"`,
				},
				Partitions: numPartitions2,
			},
		},
		TestSink: true,
	}
	receiverID2 := 2001
	err = mgr.DeployStream(stream2, []int{receiverID2}, []int{3001}, "", 24)
	require.NoError(t, err)

	receiver1, ok := mgr.receivers[receiverID1]
	require.True(t, ok)
	processorIDs1 := receiver1.OutSchema().PartitionScheme.ProcessorIDs
	processorSet1 := map[int]struct{}{}
	for _, procID := range processorIDs1 {
		processorSet1[procID] = struct{}{}
	}

	receiver2 := mgr.receivers[receiverID2]
	processorIDs2 := receiver2.OutSchema().PartitionScheme.ProcessorIDs
	processorSet2 := map[int]struct{}{}
	for _, procID := range processorIDs2 {
		processorSet2[procID] = struct{}{}
	}

	for procID := 0; procID < *cfg.ProcessorCount; procID++ {
		var expectedReceiverIDs []int
		if procID == 0 {
			expectedReceiverIDs = append(expectedReceiverIDs, common.DummyReceiverID)
		}
		_, ok := processorSet1[procID]
		if ok {
			expectedReceiverIDs = append(expectedReceiverIDs, receiverID1)
		}
		_, ok = processorSet2[procID]
		if ok {
			expectedReceiverIDs = append(expectedReceiverIDs, receiverID2)
		}
		actualReceiverIDs := mgr.GetInjectableReceivers(procID)
		sort.Ints(actualReceiverIDs)
		require.Equal(t, expectedReceiverIDs, actualReceiverIDs)
	}

	err = mgr.UndeployStream(parser.DeleteStreamDesc{StreamName: "test_stream1"}, 0)
	require.NoError(t, err)
	for procID := 0; procID < *cfg.ProcessorCount; procID++ {
		var expectedReceiverIDs []int
		if procID == 0 {
			expectedReceiverIDs = append(expectedReceiverIDs, common.DummyReceiverID)
		}
		_, ok := processorSet2[procID]
		if ok {
			expectedReceiverIDs = append(expectedReceiverIDs, receiverID2)
		}
		actualReceiverIDs := mgr.GetInjectableReceivers(procID)
		sort.Ints(actualReceiverIDs)

		require.Equal(t, expectedReceiverIDs, actualReceiverIDs)
	}

	err = mgr.UndeployStream(parser.DeleteStreamDesc{StreamName: "test_stream2"}, 0)
	require.NoError(t, err)
	for procID := 0; procID < *cfg.ProcessorCount; procID++ {
		actualReceiverIDs := mgr.GetInjectableReceivers(procID)
		if procID == 0 {
			require.Equal(t, []int{common.DummyReceiverID}, actualReceiverIDs)
		} else {
			require.Nil(t, actualReceiverIDs)
		}
	}

}

func TestGetForwardingProcessorCountKafkaIn(t *testing.T) {
	st := store2.TestStore()
	pm := tppm.NewTestProcessorManager(st)
	defer pm.Close()
	//goland:noinspection GoUnhandledErrorResult
	st.Start()
	//goland:noinspection GoUnhandledErrorResult
	defer st.Stop()

	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	mgr := NewStreamManager(nil, st, &dummyPrefixRetention{}, &expr.ExpressionFactory{}, cfg, true).(*streamManager)
	mgr.SetProcessorManager(pm)
	mgr.Loaded()
	pm.SetBatchHandler(mgr)

	receiverID1 := 1001
	receiverID2 := 1002
	_, ok := mgr.GetForwardingProcessorCount(receiverID1)
	require.False(t, ok)
	_, ok = mgr.GetForwardingProcessorCount(receiverID2)
	require.False(t, ok)

	numPartitions := 5
	stream1 := parser.CreateStreamDesc{
		StreamName: "test_stream1",
		OperatorDescs: []parser.Parseable{
			&parser.BridgeFromDesc{
				TopicName: "test_topic",
				Props: map[string]string{
					`"prop1"`: `"val1"`,
					`"prop2"`: `"val2"`,
					`"prop3"`: `"val3"`,
				},
				Partitions: numPartitions,
			},
			&parser.PartitionDesc{
				KeyExprs:   []string{"val"},
				Partitions: 10,
			},
		},
		TestSink: true,
	}

	err := mgr.DeployStream(stream1, []int{receiverID1, receiverID2}, []int{3001, 3002, 3003}, "", 23)
	require.NoError(t, err)

	partitionReceiver := mgr.receivers[receiverID2]
	require.NotNil(t, partitionReceiver)
	expectedProcessorCount := partitionReceiver.ForwardingProcessorCount()

	actualProcessorCount, ok := mgr.GetForwardingProcessorCount(receiverID2)
	require.True(t, ok)
	require.Equal(t, expectedProcessorCount, actualProcessorCount)

	err = mgr.UndeployStream(parser.DeleteStreamDesc{StreamName: "test_stream1"}, 0)
	require.NoError(t, err)

	_, ok = mgr.GetForwardingProcessorCount(receiverID2)
	require.False(t, ok)
}

func TestGetForwardingProcessorCountMultiplePartitions(t *testing.T) {
	st := store2.TestStore()
	pm := tppm.NewTestProcessorManager(st)
	defer pm.Close()
	//goland:noinspection GoUnhandledErrorResult
	st.Start()
	//goland:noinspection GoUnhandledErrorResult
	defer st.Stop()

	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	mgr := NewStreamManager(nil, st, &dummyPrefixRetention{}, &expr.ExpressionFactory{}, cfg, true).(*streamManager)
	mgr.SetProcessorManager(pm)
	mgr.Loaded()
	pm.SetBatchHandler(mgr)

	stream1 := parser.CreateStreamDesc{
		StreamName: "test_stream1",
		OperatorDescs: []parser.Parseable{
			&parser.BridgeFromDesc{
				TopicName: "test_topic",
				Props: map[string]string{
					`"prop1"`: `"val1"`,
					`"prop2"`: `"val2"`,
					`"prop3"`: `"val3"`,
				},
				Partitions: 5,
			},
			&parser.PartitionDesc{
				KeyExprs:   []string{"val"},
				Partitions: 10,
			},
			&parser.StoreStreamDesc{},
		},
		TestSink: true,
	}

	err := mgr.DeployStream(stream1, []int{1000, 1001}, []int{1000, 1001, 1002, 1003}, "", 23)
	require.NoError(t, err)

	stream2 := parser.CreateStreamDesc{
		StreamName: "test_stream2",
		OperatorDescs: []parser.Parseable{
			&parser.ContinuationDesc{ParentStreamName: "test_stream1"},
			&parser.PartitionDesc{
				KeyExprs:   []string{"val"},
				Partitions: 20,
			},
		},
		TestSink: true,
	}

	err = mgr.DeployStream(stream2, []int{1004, 1005}, []int{2001, 2002}, "", 23)
	require.NoError(t, err)

	pi := mgr.GetStream("test_stream2")
	partOper := pi.Operators[1].(*PartitionOperator)
	receiverID := partOper.forwardReceiverID
	partitionReceiver, ok := mgr.receivers[receiverID]
	require.True(t, ok)
	expectedProcessorCount := partitionReceiver.ForwardingProcessorCount()

	actualProcessorCount, ok := mgr.GetForwardingProcessorCount(receiverID)
	require.True(t, ok)
	require.Equal(t, expectedProcessorCount, actualProcessorCount)

	err = mgr.UndeployStream(parser.DeleteStreamDesc{StreamName: "test_stream2"}, 0)
	require.NoError(t, err)

	_, ok = mgr.GetForwardingProcessorCount(receiverID)
	require.False(t, ok)
}

func TestGetRequiredCompletions(t *testing.T) {
	st := store2.TestStore()
	pm := tppm.NewTestProcessorManager(st)
	defer pm.Close()
	//goland:noinspection GoUnhandledErrorResult
	st.Start()
	//goland:noinspection GoUnhandledErrorResult
	defer st.Stop()

	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	mgr := NewStreamManager(nil, st, &dummyPrefixRetention{}, &expr.ExpressionFactory{}, cfg, false).(*streamManager)
	mgr.SetProcessorManager(pm)
	mgr.Loaded()
	pm.SetBatchHandler(mgr)

	// dummy receiver
	require.Equal(t, 1, mgr.GetRequiredCompletions())

	// Deploy top level stream:

	stream1 := parser.CreateStreamDesc{
		StreamName: "toplevel_1",
		OperatorDescs: []parser.Parseable{
			&parser.BridgeFromDesc{
				TopicName:  "test_topic",
				Partitions: 5,
			},
			&parser.StoreStreamDesc{},
		},
	}

	err := mgr.DeployStream(stream1, []int{1000, 1001}, []int{1000, 1001, 1002}, "", 23)
	require.NoError(t, err)

	pi := mgr.GetStream("toplevel_1")

	numProcs := len(pi.Operators[len(pi.Operators)-1].OutSchema().PartitionScheme.ProcessorIDs)
	require.Equal(t, numProcs+1, mgr.GetRequiredCompletions())

	// Deploy another top level 1, this time with a partition
	stream2 := parser.CreateStreamDesc{
		StreamName: "toplevel_2",
		OperatorDescs: []parser.Parseable{
			&parser.BridgeFromDesc{
				TopicName:  "test_topic",
				Partitions: 5,
			},
			&parser.PartitionDesc{
				KeyExprs:   []string{"val"},
				Partitions: 6,
			},
			&parser.StoreStreamDesc{},
		},
	}
	err = mgr.DeployStream(stream2, []int{2000, 2001}, []int{2000, 2001, 2002, 2003}, "", 24)
	require.NoError(t, err)

	pi = mgr.GetStream("toplevel_2")
	numProcs2 := len(pi.Operators[len(pi.Operators)-1].OutSchema().PartitionScheme.ProcessorIDs)
	require.Equal(t, numProcs+numProcs2+1, mgr.GetRequiredCompletions())

	// Now add a child stream with no partition
	stream3 := parser.CreateStreamDesc{
		StreamName: "child_1",
		OperatorDescs: []parser.Parseable{
			&parser.ContinuationDesc{ParentStreamName: "toplevel_1"},
			&parser.StoreStreamDesc{},
		},
	}
	err = mgr.DeployStream(stream3, []int{3000, 3001}, []int{3000, 3001}, "", 25)
	require.NoError(t, err)

	// Shouldn't change the number of required completions
	require.Equal(t, numProcs+numProcs2+1, mgr.GetRequiredCompletions())

	// Now add another child with a partition
	stream4 := parser.CreateStreamDesc{
		StreamName: "child_2",
		OperatorDescs: []parser.Parseable{
			&parser.ContinuationDesc{ParentStreamName: "toplevel_1"},
			&parser.PartitionDesc{
				KeyExprs:   []string{"val"},
				Partitions: 14,
			},
			&parser.StoreStreamDesc{},
		},
	}

	err = mgr.DeployStream(stream4, []int{4000, 4001}, []int{4000, 4001, 4002}, "", 26)
	require.NoError(t, err)

	pi = mgr.GetStream("child_2")
	numProcs4 := len(pi.Operators[len(pi.Operators)-1].OutSchema().PartitionScheme.ProcessorIDs)
	require.Equal(t, numProcs2+numProcs4+1, mgr.GetRequiredCompletions())

	err = mgr.UndeployStream(parser.DeleteStreamDesc{StreamName: "child_2"}, 0)
	require.NoError(t, err)
	require.Equal(t, numProcs+numProcs2+1, mgr.GetRequiredCompletions())
}

func calcExpectedMessages(partMsgs [][]*kafka.Message, partIDs []int) []*kafka.Message {
	var expected []*kafka.Message
	for i := 0; i < len(partMsgs[0]); i++ {
		for _, partID := range partIDs {
			expected = append(expected, partMsgs[partID][i])
		}
	}
	return expected
}

func createKafkaMessage(partitionID int, offset int, key string, value string, ts int64) *kafka.Message {
	return createKafkaMessageBytes(partitionID, offset, []byte(key), []byte(value), ts)
}

func createKafkaMessageBytes(partitionID int, offset int, key []byte, value []byte, ts int64) *kafka.Message {
	return &kafka.Message{
		PartInfo: kafka.PartInfo{
			PartitionID: int32(partitionID),
			Offset:      int64(offset),
		},
		TimeStamp: time.UnixMilli(ts).In(time.UTC),
		Key:       key,
		Value:     value,
		Headers:   nil,
	}
}

type dummyPrefixRetention struct {
}

func (d *dummyPrefixRetention) AddPrefixRetention(retention.PrefixRetention) {
}
