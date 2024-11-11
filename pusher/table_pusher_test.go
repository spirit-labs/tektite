package pusher

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
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

func TestTablePusherWriteDirectSingleWriter(t *testing.T) {
	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway
	objStore := dev.NewInMemStore(0)
	seq := int64(23)

	controllerClient := &testControllerClient{
		sequence: seq,
	}
	clientFactory := func() (ControlClient, error) {
		return controllerClient, nil
	}

	writerKey := "writer1"
	controllerClient.epochsOkMap = map[string]bool{
		writerKey: true,
	}

	topicProvider := &simpleTopicInfoProvider{}

	partHashes, err := parthash.NewPartitionHashes(100)
	require.NoError(t, err)
	tableGetter := &testTableGetter{}
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, tableGetter.getTable, partHashes)
	require.NoError(t, err)
	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	numKvs := 10
	var kvs []common.KV
	for i := 0; i < numKvs; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		val := []byte(fmt.Sprintf("val-%05d", i))
		kvs = append(kvs, common.KV{
			Key:   key,
			Value: val,
		})
	}
	sortedKvs := make([]common.KV, numKvs)
	copy(sortedKvs, kvs)
	rand.Shuffle(len(kvs), func(i, j int) { kvs[i], kvs[j] = kvs[j], kvs[i] })

	groupEpoch := 23

	req := DirectWriteRequest{
		WriterKey:   writerKey,
		WriterEpoch: groupEpoch,
		KVs:         kvs,
	}

	respCh := make(chan error, 1)
	pusher.AddDirectKVs(&req, func(err error) {
		respCh <- err
	})
	err = <-respCh
	require.NoError(t, err)

	// check that table has been pushed to object store
	ssTables, _ := getSSTablesFromStore(t, cfg.DataBucketName, objStore)
	require.Equal(t, 1, len(ssTables))
	iter, err := ssTables[0].NewIterator(nil, nil)
	require.NoError(t, err)

	// check correct KVs - should be in order
	for _, expected := range sortedKvs {
		ok, kv, err := iter.Next()
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, expected, kv)
	}
	ok, _, err := iter.Next()
	require.NoError(t, err)
	require.False(t, ok)

	// check called with correct epoch
	invocs := controllerClient.getPrePushInvocations()
	require.Equal(t, 1, len(invocs))
	require.Equal(t, 0, len(invocs[0].infos))
	require.Equal(t, 1, len(invocs[0].epochInfos))
	epochInfo := invocs[0].epochInfos[0]
	require.Equal(t, groupEpoch, epochInfo.Epoch)
	require.Equal(t, writerKey, epochInfo.Key)
}

func TestTablePusherDirectWriteMultipleWritersOK(t *testing.T) {
	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway
	objStore := dev.NewInMemStore(0)
	seq := int64(23)

	controllerClient := &testControllerClient{
		sequence: seq,
	}

	numWriters := 10
	var writerKeys []string
	epochsOKMap := map[string]bool{}
	for i := 0; i < numWriters; i++ {
		writerKey := fmt.Sprintf("test-writer-%d", i)
		epochsOKMap[writerKey] = true
		writerKeys = append(writerKeys, writerKey)
	}
	controllerClient.epochsOkMap = epochsOKMap

	clientFactory := func() (ControlClient, error) {
		return controllerClient, nil
	}
	topicProvider := &simpleTopicInfoProvider{}
	partHashes, err := parthash.NewPartitionHashes(100)
	require.NoError(t, err)
	tableGetter := &testTableGetter{}
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, tableGetter.getTable, partHashes)
	require.NoError(t, err)
	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	var allKVs []common.KV
	numKvsPerWriter := 10
	var chans []chan error
	for i, writerKey := range writerKeys {
		var kvs []common.KV
		for j := 0; j < numKvsPerWriter; j++ {
			key := []byte(fmt.Sprintf("writer-%05d-key-%05d", i, j))
			val := []byte(fmt.Sprintf("writer-%05d-val-%05d", i, j))
			kvs = append(kvs, common.KV{
				Key:   key,
				Value: val,
			})
		}
		allKVs = append(allKVs, kvs...)
		rand.Shuffle(len(kvs), func(i, j int) { kvs[i], kvs[j] = kvs[j], kvs[i] })
		req := DirectWriteRequest{
			WriterKey:   writerKey,
			WriterEpoch: 23 + i,
			KVs:         kvs,
		}

		respCh := make(chan error, 1)
		pusher.AddDirectKVs(&req, func(err error) {
			respCh <- err
		})
		chans = append(chans, respCh)
	}

	for _, ch := range chans {
		err := <-ch
		require.NoError(t, err)
	}

	// check that table has been pushed to object store
	ssTables, _ := getSSTablesFromStore(t, cfg.DataBucketName, objStore)
	require.Equal(t, 1, len(ssTables))
	iter, err := ssTables[0].NewIterator(nil, nil)
	require.NoError(t, err)
	var kvs []common.KV
	for {
		ok, kv, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		kvs = append(kvs, kv)
	}
	require.Equal(t, numWriters*numKvsPerWriter, len(kvs))
	require.Equal(t, allKVs, kvs)

	// check called with correct group epochs
	invocs := controllerClient.getPrePushInvocations()
	require.Equal(t, 1, len(invocs))
	require.Equal(t, 0, len(invocs[0].infos))
	epochInfos := invocs[0].epochInfos
	require.Equal(t, numWriters, len(epochInfos))

	receivedInfos := map[string]control.EpochInfo{}
	for _, info := range epochInfos {
		receivedInfos[info.Key] = info
	}
	for i := 0; i < numWriters; i++ {
		writerKey := writerKeys[i]
		received, ok := receivedInfos[writerKey]
		require.True(t, ok)
		require.Equal(t, writerKey, received.Key)
		require.Equal(t, 23+i, received.Epoch)
	}
}

func TestTablePusherDirectWriteMultipleGroupsInvalidEpochs(t *testing.T) {
	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway
	objStore := dev.NewInMemStore(0)
	seq := int64(23)

	controllerClient := &testControllerClient{
		sequence: seq,
	}

	numWriters := 10
	var writerKeys []string
	epochsOKMap := map[string]bool{}
	for i := 0; i < numWriters; i++ {
		writerKey := fmt.Sprintf("test-writer-%d", i)
		// Every other epoch is OK
		epochOK := i%2 == 0
		epochsOKMap[writerKey] = epochOK
		writerKeys = append(writerKeys, writerKey)
	}
	controllerClient.epochsOkMap = epochsOKMap

	clientFactory := func() (ControlClient, error) {
		return controllerClient, nil
	}
	topicProvider := &simpleTopicInfoProvider{}
	partHashes, err := parthash.NewPartitionHashes(100)
	require.NoError(t, err)
	tableGetter := &testTableGetter{}
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, tableGetter.getTable, partHashes)
	require.NoError(t, err)
	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	var expectedKVs []common.KV
	numKvsPerWriter := 10
	var chans []chan error
	for i, writerKey := range writerKeys {
		var kvs []common.KV
		for j := 0; j < numKvsPerWriter; j++ {
			key := []byte(fmt.Sprintf("writer-%05d-key-%05d", i, j))
			val := []byte(fmt.Sprintf("writer-%05d-val-%05d", i, j))
			kvs = append(kvs, common.KV{
				Key:   key,
				Value: val,
			})
		}
		if i%2 == 0 {
			expectedKVs = append(expectedKVs, kvs...)
		}
		rand.Shuffle(len(kvs), func(i, j int) { kvs[i], kvs[j] = kvs[j], kvs[i] })
		req := DirectWriteRequest{
			WriterKey:   writerKey,
			WriterEpoch: 23 + i,
			KVs:         kvs,
		}

		respCh := make(chan error, 1)
		pusher.AddDirectKVs(&req, func(err error) {
			respCh <- err
		})
		chans = append(chans, respCh)
	}

	for i, ch := range chans {
		err := <-ch
		if i%2 == 0 {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			require.Equal(t, fmt.Sprintf("unable to commit direct writes for key %s as epoch is invalid", writerKeys[i]), err.Error())
		}
	}

	// check that table has been pushed to object store, but only containing non failed keys
	ssTables, _ := getSSTablesFromStore(t, cfg.DataBucketName, objStore)
	require.Equal(t, 1, len(ssTables))
	iter, err := ssTables[0].NewIterator(nil, nil)
	require.NoError(t, err)
	var kvs []common.KV
	for {
		ok, kv, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		kvs = append(kvs, kv)
	}
	require.Equal(t, len(expectedKVs), len(kvs))
	require.Equal(t, expectedKVs, kvs)

	// check called with correct group epochs
	invocs := controllerClient.getPrePushInvocations()
	require.Equal(t, 1, len(invocs))
	require.Equal(t, 0, len(invocs[0].infos))
	epochInfos := invocs[0].epochInfos
	require.Equal(t, numWriters, len(epochInfos))

	receivedInfos := map[string]control.EpochInfo{}
	for _, info := range epochInfos {
		receivedInfos[info.Key] = info
	}
	for i := 0; i < numWriters; i++ {
		writerKey := writerKeys[i]
		received, ok := receivedInfos[writerKey]
		require.True(t, ok)
		require.Equal(t, writerKey, received.Key)
		require.Equal(t, 23+i, received.Epoch)
	}
}

func TestTablePusherHandleDirectProduce(t *testing.T) {
	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway
	objStore := dev.NewInMemStore(0)
	topicID1 := 1234
	topicID2 := 2234
	seq := int64(23)
	numRecordsInBatch := 10

	controllerClient := &testControllerClient{
		sequence: seq,
		offsets: []offsets.OffsetTopicInfo{
			{
				TopicID: topicID1,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 12,
						Offset:      int64(numRecordsInBatch - 1),
					},
					{
						PartitionID: 13,
						Offset:      int64(numRecordsInBatch - 1),
					},
				},
			},
			{
				TopicID: topicID2,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 9,
						Offset:      int64(numRecordsInBatch - 1),
					},
					{
						PartitionID: 7,
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
		"topic1": {ID: topicID1, PartitionCount: 20},
		"topic2": {ID: topicID2, PartitionCount: 20},
	}}
	partHashes, err := parthash.NewPartitionHashes(100)
	require.NoError(t, err)
	tableGetter := &testTableGetter{}
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, tableGetter.getTable, partHashes)
	require.NoError(t, err)
	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	recordBatch1 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, numRecordsInBatch)
	recordBatch2 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, numRecordsInBatch)
	recordBatch3 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, numRecordsInBatch)
	recordBatch4 := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, numRecordsInBatch)

	req := DirectProduceRequest{
		TopicProduceRequests: []TopicProduceRequest{
			{
				TopicID: topicID1,
				PartitionProduceRequests: []PartitionProduceRequest{
					{
						PartitionID: 12,
						Batch:       recordBatch1,
					},
					{
						PartitionID: 13,
						Batch:       recordBatch2,
					},
				},
			},
			{
				TopicID: topicID2,
				PartitionProduceRequests: []PartitionProduceRequest{
					{
						PartitionID: 9,
						Batch:       recordBatch3,
					},
					{
						PartitionID: 7,
						Batch:       recordBatch4,
					},
				},
			},
		},
	}
	respCh := make(chan error, 1)
	pusher.handleDirectProduce(&req, func(err error) {
		respCh <- err
	})
	err = <-respCh
	require.NoError(t, err)

	// check that table has been pushed to object store

	ssTables, objects := getSSTablesFromStore(t, cfg.DataBucketName, objStore)
	require.Equal(t, 1, len(ssTables))
	iter, err := ssTables[0].NewIterator(nil, nil)
	require.NoError(t, err)
	var batches [][]byte
	for {
		ok, kv, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		batches = append(batches, kv.Value)
	}
	// Should be in topic, partition order
	require.Equal(t, 4, len(batches))
	require.Equal(t, recordBatch1, batches[0])
	require.Equal(t, recordBatch2, batches[1])
	require.Equal(t, recordBatch4, batches[2])
	require.Equal(t, recordBatch3, batches[3])

	// check getOffsets was called with correct args
	getOffsetInvocs := controllerClient.getPrePushInvocations()
	require.Equal(t, 1, len(getOffsetInvocs))
	infos := getOffsetInvocs[0].infos

	expectedInfos := []offsets.GetOffsetTopicInfo{
		{
			TopicID: topicID1,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
				{
					PartitionID: 12,
					NumOffsets:  numRecordsInBatch,
				},
				{
					PartitionID: 13,
					NumOffsets:  numRecordsInBatch,
				},
			},
		},
		{
			TopicID: topicID2,
			PartitionInfos: []offsets.GetOffsetPartitionInfo{
				{
					PartitionID: 7,
					NumOffsets:  numRecordsInBatch,
				},
				{
					PartitionID: 9,
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
	tableGetter := &testTableGetter{}
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, tableGetter.getTable, partHashes)
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
	getOffsetInvocs := controllerClient.getPrePushInvocations()
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
	tableGetter := &testTableGetter{}
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, tableGetter.getTable, partHashes)
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
	getOffsetInvs := controllerClient.getPrePushInvocations()
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
	tableGetter := &testTableGetter{}
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, tableGetter.getTable, partHashes)
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
	tableGetter := &testTableGetter{}
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, tableGetter.getTable, partHashes)
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
	tableGetter := &testTableGetter{}
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, tableGetter.getTable, partHashes)
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
	getOffsetInvs := controllerClient.getPrePushInvocations()
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
	tableGetter := &testTableGetter{}
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, tableGetter.getTable, partHashes)
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
		{{errCode: kafkaprotocol.ErrorCodeUnknownServerError, errMsg: "some random error"}},
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
	tableGetter := &testTableGetter{}

	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, tableGetter.getTable, partHashes)
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

func TestTablePusherIdempotentProducer(t *testing.T) {
	pusher, _, _ := setupTablePusherForIdempotentProducer(t)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	for i := 0; i < 10; i++ {

		producerID := i + 123

		// First batch should be sent without error
		sendBatchWithDedup(t, pusher, producerID, 0, 999, kafkaprotocol.ErrorCodeNone)

		sendBatchWithDedup(t, pusher, producerID, 0, 999, kafkaprotocol.ErrorCodeDuplicateSequenceNumber)

		sendBatchWithDedup(t, pusher, producerID, 1000, 333, kafkaprotocol.ErrorCodeNone)

		sendBatchWithDedup(t, pusher, producerID, 1000, 333, kafkaprotocol.ErrorCodeDuplicateSequenceNumber)

		sendBatchWithDedup(t, pusher, producerID, 1001, 333, kafkaprotocol.ErrorCodeDuplicateSequenceNumber)

		sendBatchWithDedup(t, pusher, producerID, 1334, 500, kafkaprotocol.ErrorCodeNone)

		sendBatchWithDedup(t, pusher, producerID, 1836, 500, kafkaprotocol.ErrorCodeOutOfOrderSequenceNumber)

		sendBatchWithDedup(t, pusher, producerID, 2000, 500, kafkaprotocol.ErrorCodeOutOfOrderSequenceNumber)

		sendBatchWithDedup(t, pusher, producerID, 1835, 500, kafkaprotocol.ErrorCodeNone)
	}

}

func setupStoredDataAndSnapshot(t *testing.T, producerID int, baseSequence int, numRecords int,
	tableGetter *mapTableGetter, controllerClient *testControllerClient) {
	offsetStart := baseSequence
	//offsetStart := math.MaxInt32 - 99 - 10
	kv := createSequenceSnapshotKV(t, 1234, 12, producerID, offsetStart)
	setupTableWithOffsetSnapshot(t, []common.KV{kv}, tableGetter, controllerClient)

	// create some data with this offset
	batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(offsetStart, 10)
	binary.BigEndian.PutUint64(batch[43:], uint64(producerID))
	binary.BigEndian.PutUint32(batch[23:], uint32(numRecords-1)) // lastOffsetDelta
	binary.BigEndian.PutUint32(batch[53:], uint32(baseSequence)) // baseSequence

	partHash, err := parthash.CreatePartitionHash(1234, 12)
	require.NoError(t, err)
	key := common.ByteSliceCopy(partHash)
	key = append(key, common.EntryTypeTopicData)
	key = encoding.KeyEncodeInt(key, int64(offsetStart))
	key = encoding.EncodeVersion(key, 0)

	kvs := []common.KV{{Key: key, Value: batch}}
	iter := common.NewKvSliceIterator(kvs)
	table, _, _, _, _, err := sst.BuildSSTable(common.DataFormatV1, 0, 0, iter)
	require.NoError(t, err)
	tableID := sst.CreateSSTableId()
	tableGetter.tables[tableID] = table
	controllerClient.queryRes = []lsm.NonOverlappingTables{
		[]lsm.QueryTableInfo{
			{
				ID: []byte(tableID),
			},
		},
	}
}

func TestTablePusherIdempotentProducerSequenceWrap1(t *testing.T) {
	producerID := 123
	pusher, tableGetter, controllerClient := setupTablePusherForIdempotentProducer(t)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	// Set up an initial batch with 10 records such that next expected sequence is setup to be math.MaxInt32 - 99
	setupStoredDataAndSnapshot(t, producerID, math.MaxInt32-99-10, 10, tableGetter, controllerClient)

	// Send a batch whose last sequence is exactly int32 max value
	sendBatchWithDedup(t, pusher, producerID, math.MaxInt32-99, 99, kafkaprotocol.ErrorCodeNone)

	// Next batch sequence should wrap to zero
	sendBatchWithDedup(t, pusher, producerID, 0, 99, kafkaprotocol.ErrorCodeNone)

	sendBatchWithDedup(t, pusher, producerID, 100, 99, kafkaprotocol.ErrorCodeNone)
}

func TestTablePusherIdempotentProducerSequenceWrap2(t *testing.T) {
	producerID := 123
	pusher, tableGetter, controllerClient := setupTablePusherForIdempotentProducer(t)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	// Set up an initial batch with 10 records such that next expected sequence is setup to be math.MaxInt32 - 99
	setupStoredDataAndSnapshot(t, producerID, math.MaxInt32-99-10, 10, tableGetter, controllerClient)

	// Send a batch whose last sequence would take it over int32 max value
	sendBatchWithDedup(t, pusher, producerID, math.MaxInt32-99, 149, kafkaprotocol.ErrorCodeNone)

	// Next batch sequence should wrap to 50
	sendBatchWithDedup(t, pusher, producerID, 50, 99, kafkaprotocol.ErrorCodeNone)

	sendBatchWithDedup(t, pusher, producerID, 150, 99, kafkaprotocol.ErrorCodeNone)
}

func TestTablePusherLoadSequenceFromSnapshotAndData(t *testing.T) {
	producerID := 123
	pusher, tableGetter, controllerClient := setupTablePusherForIdempotentProducer(t)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	// Set up an initial batch with 10 records such that next expected sequence is setup to be 110
	setupStoredDataAndSnapshot(t, producerID, 100, 10, tableGetter, controllerClient)

	sendBatchWithDedup(t, pusher, producerID, 110, 9, kafkaprotocol.ErrorCodeNone)

	sendBatchWithDedup(t, pusher, producerID, 120, 19, kafkaprotocol.ErrorCodeNone)

	sendBatchWithDedup(t, pusher, producerID, 140, 19, kafkaprotocol.ErrorCodeNone)
}

func TestTablePusherStoreOffsetSnapshot(t *testing.T) {
	pusher, _, _ := setupTablePusherForIdempotentProducerWithConfigSetter(t, func(cfg *Conf) {
		// Set timeout higher so we snapshot before writing
		cfg.WriteTimeout = 2 * time.Second
		// Set snapshot interval lower
		cfg.OffsetSnapshotInterval = 1 * time.Second
	})
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	// Send some data with different producers
	var chans []chan *kafkaprotocol.ProduceResponse
	numProducers := 10
	for i := 0; i < numProducers; i++ {
		ch := sendBatchWithDedupReturnChannel(t, pusher, i, 0, 9, kafkaprotocol.ErrorCodeNone)
		chans = append(chans, ch)
		ch = sendBatchWithDedupReturnChannel(t, pusher, i, 10, 9, kafkaprotocol.ErrorCodeNone)
		chans = append(chans, ch)
	}

	// Wait to be written
	for _, ch := range chans {
		resp := <-ch
		require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Responses[0].PartitionResponses[0].ErrorCode))
	}

	// Check table written
	ssTables, _ := getSSTablesFromStore(t, pusher.cfg.DataBucketName, pusher.objStore)

	require.Equal(t, 1, len(ssTables))

	partHash, err := parthash.CreatePartitionHash(1234, 12)
	require.NoError(t, err)

	for i := 0; i < numProducers; i++ {
		key := make([]byte, 25)
		copy(key, partHash)
		key[16] = common.EntryTypeOffsetSnapshot
		binary.BigEndian.PutUint64(key[17:], uint64(i))

		iter, err := ssTables[0].NewIterator(key, common.IncBigEndianBytes(key))
		require.NoError(t, err)
		var receivedKVs []common.KV

		for {
			ok, kv, err := iter.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			receivedKVs = append(receivedKVs, kv)
		}

		require.Equal(t, 1, len(receivedKVs))

		// value should be the offset
		kv := receivedKVs[0]
		require.Equal(t, 10, len(kv.Value))
		require.Equal(t, offsetSnapshotFormatVersion, int(binary.BigEndian.Uint16(kv.Value)))
		offset := binary.BigEndian.Uint64(kv.Value[2:])
		require.Equal(t, 10, int(offset))
	}
}

func setupTablePusherForIdempotentProducer(t *testing.T) (*TablePusher, *mapTableGetter, *testControllerClient) {
	return setupTablePusherForIdempotentProducerWithConfigSetter(t, nil)
}

func setupTablePusherForIdempotentProducerWithConfigSetter(t *testing.T, cfgSetter func(conf *Conf)) (*TablePusher, *mapTableGetter, *testControllerClient) {

	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway

	if cfgSetter != nil {
		cfgSetter(&cfg)
	}

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
	tableGetter := &mapTableGetter{
		tables: map[string]*sst.SSTable{},
	}

	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory, tableGetter.getTable, partHashes)
	require.NoError(t, err)
	err = pusher.Start()
	require.NoError(t, err)

	return pusher, tableGetter, controllerClient
}

func setupTableWithOffsetSnapshot(t *testing.T, kvs []common.KV, tg *mapTableGetter, cc *testControllerClient) {
	iter := common.NewKvSliceIterator(kvs)
	table, _, _, _, _, err := sst.BuildSSTable(common.DataFormatV1, 0, 0, iter)
	require.NoError(t, err)
	tableID := sst.CreateSSTableId()
	tg.tables[tableID] = table
	cc.queryResOffsetSnapshot = []lsm.NonOverlappingTables{
		[]lsm.QueryTableInfo{
			{
				ID: []byte(tableID),
			},
		},
	}
}

func createSequenceSnapshotKV(t *testing.T, topicID int, partitionID int, producerID int, baseOffset int) common.KV {
	partHash, err := parthash.CreatePartitionHash(topicID, partitionID)
	require.NoError(t, err)
	key := make([]byte, 25)
	copy(key, partHash)
	key[16] = common.EntryTypeOffsetSnapshot
	binary.BigEndian.PutUint64(key[17:], uint64(producerID))
	value := make([]byte, 0, 6)
	value = binary.BigEndian.AppendUint16(value, uint16(offsetSnapshotFormatVersion))
	value = encoding.KeyEncodeInt(value, int64(baseOffset))
	return common.KV{Key: key, Value: value}
}

func sendBatchWithDedup(t *testing.T, pusher *TablePusher, producerID int, baseSequence int, offsetDelta int, expectedErr int) {
	ch := sendBatchWithDedupReturnChannel(t, pusher, producerID, baseSequence, offsetDelta, expectedErr)
	resp := <-ch
	require.Equal(t, expectedErr, int(resp.Responses[0].PartitionResponses[0].ErrorCode))
}

func sendBatchWithDedupReturnChannel(t *testing.T, pusher *TablePusher, producerID int, baseSequence int, offsetDelta int, expectedErr int) chan *kafkaprotocol.ProduceResponse {
	recordBatch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(baseSequence, offsetDelta)

	binary.BigEndian.PutUint32(recordBatch[23:], uint32(offsetDelta)) // lastOffsetDelta
	binary.BigEndian.PutUint64(recordBatch[43:], uint64(producerID))
	binary.BigEndian.PutUint32(recordBatch[53:], uint32(baseSequence))

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
	err := pusher.HandleProduceRequest(&req, func(resp *kafkaprotocol.ProduceResponse) error {
		respCh <- resp
		return nil
	})
	require.NoError(t, err)
	return respCh
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
	key = append(key, common.EntryTypeTopicData)
	key = encoding.KeyEncodeInt(key, offset)
	key = encoding.EncodeVersion(key, 0)
	return key, nil
}

type testControllerClient struct {
	lock                   sync.Mutex
	registrations          []regL0TableInvocation
	prePushInvocations     []prePushInvocation
	offsets                []offsets.OffsetTopicInfo
	epochsOkMap            map[string]bool
	sequence               int64
	queryRes               lsm.OverlappingTables
	queryResOffsetSnapshot lsm.OverlappingTables
}

type regL0TableInvocation struct {
	seq      int64
	regEntry lsm.RegistrationEntry
}

type prePushInvocation struct {
	infos      []offsets.GetOffsetTopicInfo
	epochInfos []control.EpochInfo
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

func (t *testControllerClient) QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if len(keyStart) > 16 && keyStart[16] == common.EntryTypeOffsetSnapshot {
		return t.queryResOffsetSnapshot, nil
	}
	return t.queryRes, nil
}

func (t *testControllerClient) getRegistrations() []regL0TableInvocation {
	t.lock.Lock()
	defer t.lock.Unlock()
	copied := make([]regL0TableInvocation, len(t.registrations))
	copy(copied, t.registrations)
	return copied
}

func (t *testControllerClient) PrePush(infos []offsets.GetOffsetTopicInfo, epochInfos []control.EpochInfo) ([]offsets.OffsetTopicInfo, int64,
	[]bool, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.prePushInvocations = append(t.prePushInvocations, prePushInvocation{infos: infos, epochInfos: epochInfos})
	var epochsOk []bool
	for _, epochInfo := range epochInfos {
		epochOk, exists := t.epochsOkMap[epochInfo.Key]
		if !exists {
			panic("expected epochInfo to exist")
		}
		epochsOk = append(epochsOk, epochOk)
	}
	return t.offsets, t.sequence, epochsOk, nil
}

func (t *testControllerClient) getPrePushInvocations() []prePushInvocation {
	t.lock.Lock()
	defer t.lock.Unlock()
	copied := make([]prePushInvocation, len(t.prePushInvocations))
	copy(copied, t.prePushInvocations)
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

type testTableGetter struct {
	table *sst.SSTable
}

func (t *testTableGetter) getTable(tableID sst.SSTableID) (*sst.SSTable, error) {
	return t.table, nil
}

type mapTableGetter struct {
	tables map[string]*sst.SSTable
}

func (t *mapTableGetter) getTable(tableID sst.SSTableID) (*sst.SSTable, error) {
	table, ok := t.tables[string(tableID)]
	if !ok {
		return nil, errors.New("cannot find table")
	}
	return table, nil
}
