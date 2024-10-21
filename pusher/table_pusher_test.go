package pusher

import (
	"bytes"
	"context"
	"errors"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	common.EnableTestPorts()
}

type simpleTopicInfoProvider struct {
	infos map[string]topicmeta.TopicInfo
}

func (s *simpleTopicInfoProvider) GetTopicInfo(topicName string) (topicmeta.TopicInfo, error) {
	info, ok := s.infos[topicName]
	if !ok {
		return topicmeta.TopicInfo{}, common.NewTektiteErrorf(common.TopicDoesNotExist, "unknown topic: %s", topicName)
	}
	return info, nil
}

func TestTablePusherHandleProduceBatchSimple(t *testing.T) {
	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway
	objStore := dev.NewInMemStore(0)
	topicID := 1234
	seq := int64(23)
	numRecordsInBatch := 10

	controllerClient := &testControllerClient{
		sequence: seq,
		offsets: []offsets.OffsetTopicInfo{
			{
				TopicID: topicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 12,
						Offset:      int64(numRecordsInBatch - 1),
					},
				},
			},
		},
	}
	clientFactory := func() (ControlClient, error) {
		return controllerClient, nil
	}
	topicProvider := &simpleTopicInfoProvider{infos: map[string]topicmeta.TopicInfo{
		"topic1": {ID: topicID, PartitionCount: 20},
	}}
	partHashes, err := parthash.NewPartitionHashes(100)
	require.NoError(t, err)
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, partHashes)
	require.NoError(t, err)
	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	recordBatch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, numRecordsInBatch)

	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							recordBatch,
						},
					},
				},
			},
		},
	}
	respCh := make(chan *kafkaprotocol.ProduceResponse, 1)
	err = pusher.HandleProduceRequest(&req, func(resp *kafkaprotocol.ProduceResponse) error {
		respCh <- resp
		return nil
	})
	require.NoError(t, err)
	checkNoPartitionResponseErrors(t, respCh, &req)

	// check that table has been pushed to object store

	ssTables, objects := getSSTablesFromStore(t, cfg.DataBucketName, objStore)
	require.Equal(t, 1, len(ssTables))
	iter, err := ssTables[0].NewIterator(nil, nil)
	require.NoError(t, err)
	for {
		ok, kv, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		require.Equal(t, recordBatch, kv.Value)
	}

	// check getOffsets was called with correct args
	getOffsetInvocs := controllerClient.getGetOffsetInvocations()
	require.Equal(t, 1, len(getOffsetInvocs))
	infos := getOffsetInvocs[0].infos

	expectedInfos := []offsets.GetOffsetTopicInfo{
		{
			TopicID: topicID,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
				{
					PartitionID: 12,
					NumOffsets:  numRecordsInBatch,
				},
			},
		},
	}

	require.Equal(t, expectedInfos, infos)

	// check that table has been registered with LSM, and correct sequence provided

	receivedRegs := controllerClient.getRegistrations()
	require.Equal(t, 1, len(receivedRegs))

	reg := receivedRegs[0].regEntry
	require.Equal(t, []byte(objects[0].Key), []byte(reg.TableID))
	require.Equal(t, seq, receivedRegs[0].seq)
}

func TestTablePusherHandleProduceBatchMultipleTopicsAndPartitions(t *testing.T) {
	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway
	objStore := dev.NewInMemStore(0)
	topicID1 := 1234
	topicID2 := 4321

	seq := int64(46)
	controllerClient := &testControllerClient{
		offsets: []offsets.OffsetTopicInfo{
			{
				TopicID: topicID1,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 7,
						Offset:      32,
					},
					{
						PartitionID: 12,
						Offset:      1002,
					},
				},
			},
			{
				TopicID: topicID2,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 23,
						Offset:      564,
					},
				},
			},
		},
		sequence: seq,
	}
	clientFactory := func() (ControlClient, error) {
		return controllerClient, nil
	}
	topicProvider := &simpleTopicInfoProvider{infos: map[string]topicmeta.TopicInfo{
		"topic1": {ID: topicID1, PartitionCount: 20},
		"topic2": {ID: topicID2, PartitionCount: 30},
	}}
	partHashes, err := parthash.NewPartitionHashes(100)
	require.NoError(t, err)
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, partHashes)
	require.NoError(t, err)

	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	recordBatch1 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 10)
	recordBatch2 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 15)
	recordBatch3 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 20)
	recordBatch4 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 25)

	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							recordBatch1,
						},
					},
					{
						Index: 12,
						Records: [][]byte{
							recordBatch2,
						},
					},
					{
						Index: 7,
						Records: [][]byte{
							recordBatch3,
						},
					},
				},
			},
			{
				Name: common.StrPtr("topic2"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 23,
						Records: [][]byte{
							recordBatch4,
						},
					},
				},
			},
		},
	}
	respCh := make(chan *kafkaprotocol.ProduceResponse, 1)
	err = pusher.HandleProduceRequest(&req, func(resp *kafkaprotocol.ProduceResponse) error {
		respCh <- resp
		return nil
	})
	require.NoError(t, err)

	checkNoPartitionResponseErrors(t, respCh, &req)

	// verify get offsets called correctly
	// note, will be sorted in topic, partition order
	expectedGetOffsets := []offsets.GetOffsetTopicInfo{
		{
			TopicID: topicID1,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
				{
					PartitionID: 7,
					NumOffsets:  20,
				},
				{
					PartitionID: 12,
					NumOffsets:  10 + 15,
				},
			},
		},
		{
			TopicID: topicID2,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
				{
					PartitionID: 23,
					NumOffsets:  25,
				},
			},
		},
	}
	getOffsetInvs := controllerClient.getGetOffsetInvocations()
	require.Equal(t, 1, len(getOffsetInvs))
	require.Equal(t, expectedGetOffsets, getOffsetInvs[0].infos)

	// check that table has been pushed to object store

	ssTables, objects := getSSTablesFromStore(t, cfg.DataBucketName, objStore)

	var receivedKVs []common.KV
	iter, err := ssTables[0].NewIterator(nil, nil)
	require.NoError(t, err)
	for {
		ok, kv, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		receivedKVs = append(receivedKVs, kv)
	}
	require.Equal(t, 4, len(receivedKVs))

	var expectedKVs []common.KV

	// offset 32
	// asked for 20
	// first 13
	expectedKey1, err := createExpectedKey(topicID1, 7, 13)
	require.NoError(t, err)

	// offset 1002
	// asked for 25
	// 10 and 15 batches
	expectedKey2, err := createExpectedKey(topicID1, 12, 978)
	require.NoError(t, err)
	expectedKey3, err := createExpectedKey(topicID1, 12, 988)
	require.NoError(t, err)

	// offset 564
	// asked for 25
	expectedKey4, err := createExpectedKey(topicID2, 23, 540)
	require.NoError(t, err)

	expectedKVs = append(expectedKVs, common.KV{
		Key:   expectedKey1,
		Value: recordBatch3,
	}, common.KV{
		Key:   expectedKey2,
		Value: recordBatch1,
	}, common.KV{
		Key:   expectedKey3,
		Value: recordBatch2,
	}, common.KV{
		Key:   expectedKey4,
		Value: recordBatch4,
	})

	slices.SortFunc(expectedKVs, func(a, b common.KV) int {
		return bytes.Compare(a.Key, b.Key)
	})

	require.Equal(t, len(expectedKVs), len(receivedKVs))
	require.Equal(t, expectedKVs, receivedKVs)

	receivedRegs := controllerClient.getRegistrations()
	require.Equal(t, 1, len(receivedRegs))

	reg := receivedRegs[0].regEntry
	require.Equal(t, []byte(objects[0].Key), []byte(reg.TableID))

	require.Equal(t, seq, receivedRegs[0].seq)
}

func TestTablePusherPushWhenBufferIsFull(t *testing.T) {
	batch1 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 100)
	batch2 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 100)

	// So it won't write after receiving batch1 but will write after batch2
	bufferMaxSize := len(batch1) + len(batch2) - 10

	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Hour // So it doesn't push on timer
	cfg.BufferMaxSizeBytes = bufferMaxSize
	objStore := dev.NewInMemStore(0)
	topicID := 1234

	controllerClient := &testControllerClient{
		offsets: []offsets.OffsetTopicInfo{
			{
				TopicID: topicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 12,
						Offset:      1000,
					},
					{
						PartitionID: 23,
						Offset:      1000,
					},
				},
			},
		},
	}
	clientFactory := func() (ControlClient, error) {
		return controllerClient, nil
	}
	topicProvider := &simpleTopicInfoProvider{infos: map[string]topicmeta.TopicInfo{
		"topic1": {ID: topicID, PartitionCount: 30},
	}}

	partHashes, err := parthash.NewPartitionHashes(100)
	require.NoError(t, err)
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, partHashes)
	require.NoError(t, err)

	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	req1 := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							batch1,
						},
					},
				},
			},
		},
	}
	var batch1Complete atomic.Bool
	respCh1 := make(chan *kafkaprotocol.ProduceResponse, 1)
	err = pusher.HandleProduceRequest(&req1, func(resp *kafkaprotocol.ProduceResponse) error {
		batch1Complete.Store(true)
		respCh1 <- resp
		return nil
	})
	require.NoError(t, err)

	// Wait a little bit
	time.Sleep(50 * time.Millisecond)
	// Should not have been written
	require.False(t, batch1Complete.Load())

	// Now send batch2

	req2 := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 23,
						Records: [][]byte{
							batch2,
						},
					},
				},
			},
		},
	}
	respCh2 := make(chan *kafkaprotocol.ProduceResponse, 1)
	err = pusher.HandleProduceRequest(&req2, func(resp *kafkaprotocol.ProduceResponse) error {
		respCh2 <- resp
		return nil
	})
	require.NoError(t, err)

	// They should now both complete
	checkNoPartitionResponseErrors(t, respCh1, &req1)

	checkNoPartitionResponseErrors(t, respCh2, &req2)

	// check that 1 table has been pushed to object store
	ssTables, objects := getSSTablesFromStore(t, cfg.DataBucketName, objStore)
	require.Equal(t, 1, len(ssTables))
	require.Equal(t, 2, ssTables[0].NumEntries())

	// check that table has been registered with LSM
	receivedRegs := controllerClient.getRegistrations()
	require.Equal(t, 1, len(receivedRegs))
	receivedReg := receivedRegs[0]
	reg := receivedReg.regEntry
	require.Equal(t, []byte(objects[0].Key), []byte(reg.TableID))
}

func TestTablePusherPushWhenTimeoutIsExceeded(t *testing.T) {
	batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 100)

	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 250 * time.Millisecond
	cfg.BufferMaxSizeBytes = math.MaxInt
	objStore := dev.NewInMemStore(0)
	topicID := 1234

	controllerClient := &testControllerClient{
		offsets: []offsets.OffsetTopicInfo{
			{
				TopicID: topicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 12,
						Offset:      1000,
					},
				},
			},
		},
	}
	clientFactory := func() (ControlClient, error) {
		return controllerClient, nil
	}
	topicProvider := &simpleTopicInfoProvider{infos: map[string]topicmeta.TopicInfo{
		"topic1": {ID: topicID, PartitionCount: 20},
	}}

	partHashes, err := parthash.NewPartitionHashes(100)
	require.NoError(t, err)
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, partHashes)
	require.NoError(t, err)

	start := time.Now()
	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							batch,
						},
					},
				},
			},
		},
	}
	var batchComplete atomic.Bool
	respCh := make(chan *kafkaprotocol.ProduceResponse, 1)
	err = pusher.HandleProduceRequest(&req, func(resp *kafkaprotocol.ProduceResponse) error {
		batchComplete.Store(true)
		respCh <- resp
		return nil
	})
	require.NoError(t, err)

	time.Sleep(cfg.WriteTimeout / 2)
	// Should not have been written
	require.False(t, batchComplete.Load())

	// Wait until it completes
	testutils.WaitUntil(t, func() (bool, error) {
		return batchComplete.Load(), nil
	})
	require.True(t, time.Since(start) >= cfg.WriteTimeout)

	checkNoPartitionResponseErrors(t, respCh, &req)

	// check that 1 table has been pushed to object store
	ssTables, objects := getSSTablesFromStore(t, cfg.DataBucketName, objStore)
	require.Equal(t, 1, len(ssTables))
	require.Equal(t, 1, ssTables[0].NumEntries())

	// check that table has been registered with LSM
	receivedRegs := controllerClient.getRegistrations()
	require.Equal(t, 1, len(receivedRegs))
	reg := receivedRegs[0].regEntry
	require.Equal(t, []byte(objects[0].Key), []byte(reg.TableID))
}

func TestTablePusherHandleProduceBatchMixtureErrorsAndSuccesses(t *testing.T) {
	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway
	objStore := dev.NewInMemStore(0)
	topicID1 := 1234
	topicID2 := 4321

	seq := int64(46)
	controllerClient := &testControllerClient{
		offsets: []offsets.OffsetTopicInfo{
			{
				TopicID: topicID1,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 7,
						Offset:      32,
					},
					{
						PartitionID: 12,
						Offset:      1002,
					},
				},
			},
			{
				TopicID: topicID2,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 23,
						Offset:      564,
					},
				},
			},
		},
		sequence: seq,
	}
	clientFactory := func() (ControlClient, error) {
		return controllerClient, nil
	}
	topicProvider := &simpleTopicInfoProvider{infos: map[string]topicmeta.TopicInfo{
		"topic1": {ID: topicID1, PartitionCount: 20},
		"topic2": {ID: topicID2, PartitionCount: 30},
	}}
	partHashes, err := parthash.NewPartitionHashes(100)
	require.NoError(t, err)
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, partHashes)
	require.NoError(t, err)

	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	recordBatch1 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 10)
	recordBatch2 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 15)
	recordBatch3 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 20)
	recordBatch4 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 25)

	recordBatch5 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 25)
	recordBatch6 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 25)

	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							recordBatch1,
						},
					},
					{
						Index: 12,
						Records: [][]byte{
							recordBatch2,
						},
					},
					{
						Index: 999,
						Records: [][]byte{
							recordBatch5,
						},
					},
					{
						Index: 7,
						Records: [][]byte{
							recordBatch3,
						},
					},
				},
			},
			{
				Name: common.StrPtr("topic2"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 23,
						Records: [][]byte{
							recordBatch4,
						},
					},
				},
			},
			{
				Name: common.StrPtr("unknown_topic"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 23,
						Records: [][]byte{
							recordBatch6,
						},
					},
				},
			},
		},
	}
	respCh := make(chan *kafkaprotocol.ProduceResponse, 1)
	err = pusher.HandleProduceRequest(&req, func(resp *kafkaprotocol.ProduceResponse) error {
		respCh <- resp
		return nil
	})
	require.NoError(t, err)

	checkResponseErrors(t, respCh, [][]expectedErr{
		{
			noErr, noErr, expectedErr{errCode: kafkaprotocol.ErrorCodeUnknownTopicOrPartition,
				errMsg: "unknown partition: 999 for topic: topic1"}, noErr,
		},
		{
			noErr,
		},
		{
			expectedErr{errCode: kafkaprotocol.ErrorCodeUnknownTopicOrPartition,
				errMsg: "unknown topic: unknown_topic"},
		},
	})

	// verify get offsets called correctly
	// note, will be sorted in topic, partition order
	expectedGetOffsets := []offsets.GetOffsetTopicInfo{
		{
			TopicID: topicID1,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
				{
					PartitionID: 7,
					NumOffsets:  20,
				},
				{
					PartitionID: 12,
					NumOffsets:  10 + 15,
				},
			},
		},
		{
			TopicID: topicID2,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
				{
					PartitionID: 23,
					NumOffsets:  25,
				},
			},
		},
	}
	getOffsetInvs := controllerClient.getGetOffsetInvocations()
	require.Equal(t, 1, len(getOffsetInvs))
	require.Equal(t, expectedGetOffsets, getOffsetInvs[0].infos)

	// check that table has been pushed to object store

	ssTables, objects := getSSTablesFromStore(t, cfg.DataBucketName, objStore)

	var receivedKVs []common.KV
	iter, err := ssTables[0].NewIterator(nil, nil)
	require.NoError(t, err)
	for {
		ok, kv, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		receivedKVs = append(receivedKVs, kv)
	}
	require.Equal(t, 4, len(receivedKVs))

	var expectedKVs []common.KV

	// offset 32
	// asked for 20
	// first 13
	expectedKey1, err := createExpectedKey(topicID1, 7, 13)
	require.NoError(t, err)

	// offset 1002
	// asked for 25
	// 10 and 15 batches
	expectedKey2, err := createExpectedKey(topicID1, 12, 978)
	require.NoError(t, err)
	expectedKey3, err := createExpectedKey(topicID1, 12, 988)
	require.NoError(t, err)

	// offset 564
	// asked for 25
	expectedKey4, err := createExpectedKey(topicID2, 23, 540)
	require.NoError(t, err)

	expectedKVs = append(expectedKVs, common.KV{
		Key:   expectedKey1,
		Value: recordBatch3,
	}, common.KV{
		Key:   expectedKey2,
		Value: recordBatch1,
	}, common.KV{
		Key:   expectedKey3,
		Value: recordBatch2,
	}, common.KV{
		Key:   expectedKey4,
		Value: recordBatch4,
	})

	slices.SortFunc(expectedKVs, func(a, b common.KV) int {
		return bytes.Compare(a.Key, b.Key)
	})

	require.Equal(t, len(expectedKVs), len(receivedKVs))
	require.Equal(t, expectedKVs, receivedKVs)

	receivedRegs := controllerClient.getRegistrations()
	require.Equal(t, 1, len(receivedRegs))

	reg := receivedRegs[0].regEntry
	require.Equal(t, []byte(objects[0].Key), []byte(reg.TableID))

	require.Equal(t, seq, receivedRegs[0].seq)
}

func TestTablePusherUnexpectedError(t *testing.T) {
	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway
	objStore := &failingObjectStoreClient{}
	topicID := 1234
	controllerClient := &testControllerClient{
		offsets: []offsets.OffsetTopicInfo{
			{
				TopicID: topicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 12,
						Offset:      1000,
					},
				},
			},
		},
	}
	clientFactory := func() (ControlClient, error) {
		return controllerClient, nil
	}
	topicProvider := &simpleTopicInfoProvider{infos: map[string]topicmeta.TopicInfo{
		"topic1": {ID: topicID, PartitionCount: 20},
	}}
	partHashes, err := parthash.NewPartitionHashes(100)
	require.NoError(t, err)
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, partHashes)
	require.NoError(t, err)
	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	msgs := []testutils.RawKafkaMessage{
		{
			Timestamp: time.Now().UnixMilli(),
			Key:       []byte("key1"),
			Value:     []byte("val1"),
		},
	}
	recordBatch := testutils.CreateKafkaRecordBatch(msgs, 0)

	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							recordBatch,
						},
					},
				},
			},
		},
	}
	respCh := make(chan *kafkaprotocol.ProduceResponse, 1)
	err = pusher.HandleProduceRequest(&req, func(resp *kafkaprotocol.ProduceResponse) error {
		respCh <- resp
		return nil
	})
	require.NoError(t, err)

	expected := [][]expectedErr{
		{{errCode: kafkaprotocol.ErrorCodeUnknownServerError}},
	}
	checkResponseErrors(t, respCh, expected)

	ssTables, _ := getSSTablesFromStore(t, cfg.DataBucketName, objStore)
	require.Equal(t, 0, len(ssTables))

	receivedRegs := controllerClient.getRegistrations()
	require.Equal(t, 0, len(receivedRegs))
}

func TestTablePusherTemporaryUnavailability(t *testing.T) {
	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway
	cfg.AvailabilityRetryInterval = 500 * time.Millisecond
	inMemClient := dev.NewInMemStore(0)
	objStore := &unavailableObjStoreClient{
		cl: inMemClient,
	}
	topicID := 1234
	controllerClient := &testControllerClient{
		offsets: []offsets.OffsetTopicInfo{
			{
				TopicID: topicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 12,
						Offset:      1000,
					},
				},
			},
		},
	}
	clientFactory := func() (ControlClient, error) {
		return controllerClient, nil
	}
	topicProvider := &simpleTopicInfoProvider{infos: map[string]topicmeta.TopicInfo{
		"topic1": {ID: topicID, PartitionCount: 30},
	}}
	partHashes, err := parthash.NewPartitionHashes(100)
	require.NoError(t, err)
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, partHashes)
	require.NoError(t, err)
	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	start := time.Now()

	// Push a couple of batches - obj store is unavailable
	msgs := []testutils.RawKafkaMessage{
		{
			Timestamp: time.Now().UnixMilli(),
			Key:       []byte("key1"),
			Value:     []byte("val1"),
		},
	}
	recordBatch1 := testutils.CreateKafkaRecordBatch(msgs, 0)
	req1 := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							recordBatch1,
						},
					},
				},
			},
		},
	}
	respCh1 := make(chan *kafkaprotocol.ProduceResponse, 1)
	completionCalled1 := atomic.Bool{}
	err = pusher.HandleProduceRequest(&req1, func(resp *kafkaprotocol.ProduceResponse) error {
		completionCalled1.Store(true)
		respCh1 <- resp
		return nil
	})
	require.NoError(t, err)

	recordBatch2 := testutils.CreateKafkaRecordBatch(msgs, 0)
	req2 := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 20,
						Records: [][]byte{
							recordBatch2,
						},
					},
				},
			},
		},
	}
	respCh2 := make(chan *kafkaprotocol.ProduceResponse, 1)
	completionCalled2 := atomic.Bool{}
	err = pusher.HandleProduceRequest(&req2, func(resp *kafkaprotocol.ProduceResponse) error {
		completionCalled2.Store(true)
		respCh2 <- resp
		return nil
	})
	require.NoError(t, err)

	time.Sleep(250 * time.Millisecond)
	// Unavailable so no completions called
	require.False(t, completionCalled1.Load())
	require.False(t, completionCalled2.Load())

	// Now make available
	objStore.available.Store(true)

	// Should now be written and complete

	checkNoPartitionResponseErrors(t, respCh1, &req1)
	checkNoPartitionResponseErrors(t, respCh2, &req2)

	require.True(t, time.Since(start) >= cfg.AvailabilityRetryInterval)

	ssTables, _ := getSSTablesFromStore(t, cfg.DataBucketName, objStore)
	require.Equal(t, 1, len(ssTables))

	receivedRegs := controllerClient.getRegistrations()
	require.Equal(t, 1, len(receivedRegs))
}

func checkNoPartitionResponseErrors(t *testing.T, respCh chan *kafkaprotocol.ProduceResponse, req *kafkaprotocol.ProduceRequest) {
	resp := <-respCh
	require.NotNil(t, resp)
	require.Equal(t, len(req.TopicData), len(resp.Responses))
	for i, topicResp := range resp.Responses {
		require.Equal(t, len(req.TopicData[i].PartitionData), len(topicResp.PartitionResponses))
		for _, pResp := range topicResp.PartitionResponses {
			var errMsg string
			if pResp.ErrorMessage != nil {
				errMsg = *pResp.ErrorMessage
			}
			require.Equalf(t, kafkaprotocol.ErrorCodeNone, int(pResp.ErrorCode),
				"expected no error but got errorCode: %d message: %s", pResp.ErrorCode, errMsg)
		}
	}
}

var noErr = expectedErr{
	errCode: kafkaprotocol.ErrorCodeNone,
}

type expectedErr struct {
	errCode int
	errMsg  string
}

func checkResponseErrors(t *testing.T, respCh chan *kafkaprotocol.ProduceResponse, expectedCodes [][]expectedErr) {
	resp := <-respCh
	require.NotNil(t, resp)
	require.Equal(t, len(expectedCodes), len(resp.Responses))
	for i, topicResp := range resp.Responses {
		require.Equal(t, len(expectedCodes[i]), len(topicResp.PartitionResponses))
		for j, pResp := range topicResp.PartitionResponses {
			var errMsg string
			if pResp.ErrorMessage != nil {
				errMsg = *pResp.ErrorMessage
			}
			expectedCode := expectedCodes[i][j].errCode
			require.Equalf(t, expectedCode, int(pResp.ErrorCode),
				"expected errorCode: %d but got: %dmsg: %s", expectedCode, pResp.ErrorCode, errMsg)
			expectedMsg := expectedCodes[i][j].errMsg
			require.Equalf(t, expectedMsg, errMsg,
				"expected errMsg: %s but got: %s", expectedMsg, errMsg)
		}
	}
}

func getSSTablesFromStore(t *testing.T, databucketName string, objStore objstore.Client) ([]*sst.SSTable, []objstore.ObjectInfo) {
	objects, err := objStore.ListObjectsWithPrefix(context.Background(), databucketName, "sst-", 10)
	require.NoError(t, err)
	var ssTables []*sst.SSTable
	for _, info := range objects {
		sstableBytes, err := objStore.Get(context.Background(), databucketName, info.Key)
		require.NoError(t, err)
		ssTable := &sst.SSTable{}
		ssTable.Deserialize(sstableBytes, 0)
		ssTables = append(ssTables, ssTable)
	}
	return ssTables, objects
}

func createExpectedKey(topicID int, partitionID int, offset int64) ([]byte, error) {
	key, err := parthash.CreatePartitionHash(topicID, partitionID)
	if err != nil {
		return nil, err
	}
	key = encoding.KeyEncodeInt(key, offset)
	key = encoding.EncodeVersion(key, 0)
	return key, nil
}

type testControllerClient struct {
	lock                 sync.Mutex
	registrations        []regL0TableInvocation
	getOffsetInvocations []getOffsetsInvocation
	offsets              []offsets.OffsetTopicInfo
	sequence             int64
}

type regL0TableInvocation struct {
	seq      int64
	regEntry lsm.RegistrationEntry
}

type getOffsetsInvocation struct {
	infos []offsets.GetOffsetTopicInfo
}

func (t *testControllerClient) RegisterL0Table(sequence int64, regEntry lsm.RegistrationEntry) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.registrations = append(t.registrations, regL0TableInvocation{
		seq:      sequence,
		regEntry: regEntry,
	})
	return nil
}

func (t *testControllerClient) getRegistrations() []regL0TableInvocation {
	t.lock.Lock()
	defer t.lock.Unlock()
	copied := make([]regL0TableInvocation, len(t.registrations))
	copy(copied, t.registrations)
	return copied
}

func (t *testControllerClient) GetOffsets(infos []offsets.GetOffsetTopicInfo) ([]offsets.OffsetTopicInfo, int64, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.getOffsetInvocations = append(t.getOffsetInvocations, getOffsetsInvocation{infos: infos})
	return t.offsets, t.sequence, nil
}

func (t *testControllerClient) getGetOffsetInvocations() []getOffsetsInvocation {
	t.lock.Lock()
	defer t.lock.Unlock()
	copied := make([]getOffsetsInvocation, len(t.getOffsetInvocations))
	copy(copied, t.getOffsetInvocations)
	return copied
}

func (t *testControllerClient) Close() error {
	return nil
}

type failingObjectStoreClient struct {
}

func (f *failingObjectStoreClient) Get(_ context.Context, _ string, _ string) ([]byte, error) {
	panic("should not be called")
}

func (f *failingObjectStoreClient) Put(_ context.Context, _ string, _ string, _ []byte) error {
	return errors.New("some random error")
}

func (f *failingObjectStoreClient) PutIfNotExists(_ context.Context, _ string, _ string, _ []byte) (bool, error) {
	panic("should not be called")
}

func (f *failingObjectStoreClient) Delete(_ context.Context, _ string, _ string) error {
	panic("should not be called")
}

func (f *failingObjectStoreClient) DeleteAll(_ context.Context, _ string, _ []string) error {
	panic("should not be called")
}

func (f *failingObjectStoreClient) ListObjectsWithPrefix(_ context.Context, _ string, _ string, _ int) ([]objstore.ObjectInfo, error) {
	return nil, nil
}

func (f *failingObjectStoreClient) Start() error {
	return nil
}

func (f *failingObjectStoreClient) Stop() error {
	return nil
}

type unavailableObjStoreClient struct {
	available atomic.Bool
	cl        objstore.Client
}

func (u *unavailableObjStoreClient) Get(ctx context.Context, bucket string, key string) ([]byte, error) {
	if !u.available.Load() {
		return nil, common.NewTektiteErrorf(common.Unavailable, "object store is unavailable")
	}
	return u.cl.Get(ctx, bucket, key)
}

func (u *unavailableObjStoreClient) Put(ctx context.Context, bucket string, key string, value []byte) error {
	if !u.available.Load() {
		return common.NewTektiteErrorf(common.Unavailable, "object store is unavailable")
	}
	return u.cl.Put(ctx, bucket, key, value)
}

func (u *unavailableObjStoreClient) PutIfNotExists(ctx context.Context, bucket string, key string, value []byte) (bool, error) {
	if !u.available.Load() {
		return false, common.NewTektiteErrorf(common.Unavailable, "object store is unavailable")
	}
	return u.cl.PutIfNotExists(ctx, bucket, key, value)
}

func (u *unavailableObjStoreClient) Delete(ctx context.Context, bucket string, key string) error {
	if !u.available.Load() {
		return common.NewTektiteErrorf(common.Unavailable, "object store is unavailable")
	}
	return u.cl.Delete(ctx, bucket, key)
}

func (u *unavailableObjStoreClient) DeleteAll(ctx context.Context, bucket string, keys []string) error {
	if !u.available.Load() {
		return common.NewTektiteErrorf(common.Unavailable, "object store is unavailable")
	}
	return u.cl.DeleteAll(ctx, bucket, keys)
}

func (u *unavailableObjStoreClient) ListObjectsWithPrefix(ctx context.Context, bucket string, prefix string, maxKeys int) ([]objstore.ObjectInfo, error) {
	if !u.available.Load() {
		return nil, common.NewTektiteErrorf(common.Unavailable, "object store is unavailable")
	}
	return u.cl.ListObjectsWithPrefix(ctx, bucket, prefix, maxKeys)
}

func (u *unavailableObjStoreClient) Start() error {
	return u.cl.Start()
}

func (u *unavailableObjStoreClient) Stop() error {
	return u.cl.Stop()
}

func TestExtractBatches(t *testing.T) {
	numBatches := 10
	offset := 1001
	var batches [][]byte
	var concat []byte
	for i := 0; i < numBatches; i++ {
		numRecords := rand.Intn(1000) + 1
		batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(offset, numRecords)
		batches = append(batches, batch)
		offset += numRecords
		concat = append(concat, batch...)
	}
	extracted := extractBatches(concat)
	require.Equal(t, numBatches, len(extracted))
	for i, batch := range batches {
		require.Equal(t, batch, extracted[i])
	}
}
