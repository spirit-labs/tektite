package topicmeta

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/compress"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/sst"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestCreateLoadDeleteTopics(t *testing.T) {
	lsmH := &testLsmHolder{}
	objStore := dev.NewInMemStore(0)

	dataBucketName := "test-bucket"
	kvw := func(kvs []common.KV) error {
		return writeKV(kvs, lsmH, objStore, dataBucketName, common.DataFormatV1)
	}
	mgr, err := NewManager(lsmH, objStore, dataBucketName, common.DataFormatV1, nil, kvw)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	numTopics := 100
	var infos []TopicInfo

	expectedSeq := TopicIDSequenceBase
	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%03d", i)
		_, _, exists, err := mgr.GetTopicInfo(topicName)
		require.NoError(t, err)
		require.False(t, exists)
		info := TopicInfo{
			Name:           topicName,
			PartitionCount: i + 1,
			RetentionTime:  time.Duration(1000000 + i),
		}
		id, err := mgr.CreateOrUpdateTopic(info, true)
		require.NoError(t, err)
		require.Equal(t, expectedSeq, id)
		received, seq, exists, err := mgr.GetTopicInfo(topicName)
		require.NoError(t, err)
		require.True(t, exists)
		info.ID = id
		require.Equal(t, info, received)
		expectedSeq++
		require.Equal(t, expectedSeq, seq)
		infos = append(infos, info)
	}

	// Now restart
	err = mgr.Stop()
	require.NoError(t, err)

	mgr, err = NewManager(lsmH, objStore, "test-bucket", common.DataFormatV1, nil, kvw)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	// Topics should still be there
	for _, info := range infos {
		received, _, exists, err := mgr.GetTopicInfo(info.Name)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, info, received)
	}

	// Now delete half of them
	for i := 0; i < len(infos)/2; i++ {
		info := infos[i]
		err = mgr.DeleteTopic(info.Name)
		require.NoError(t, err)
		_, _, exists, err := mgr.GetTopicInfo(info.Name)
		require.NoError(t, err)
		require.False(t, exists)
	}

	// Rest should still be there
	for i := len(infos) / 2; i < len(infos); i++ {
		info := infos[i]
		received, _, exists, err := mgr.GetTopicInfo(info.Name)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, info, received)
	}

	// Restart again
	err = mgr.Stop()
	require.NoError(t, err)
	mgr, err = NewManager(lsmH, objStore, dataBucketName, common.DataFormatV1, nil, kvw)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	for i, info := range infos {
		received, _, exists, err := mgr.GetTopicInfo(info.Name)
		if i < len(infos)/2 {
			require.NoError(t, err)
			require.False(t, exists)
		} else {
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, info, received)
		}
	}

	// Delete the rest
	for i := len(infos) / 2; i < len(infos); i++ {
		info := infos[i]
		err = mgr.DeleteTopic(info.Name)
		require.NoError(t, err)
		_, _, exists, err := mgr.GetTopicInfo(info.Name)
		require.NoError(t, err)
		require.False(t, exists)
	}

	// Restart again
	err = mgr.Stop()
	require.NoError(t, err)
	mgr, err = NewManager(lsmH, objStore, dataBucketName, common.DataFormatV1, nil, kvw)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	// Should be none
	for _, info := range infos {
		_, _, exists, err := mgr.GetTopicInfo(info.Name)
		require.NoError(t, err)
		require.False(t, exists)
	}
}

func TestUpdateTopicInfo(t *testing.T) {
	lsmH := &testLsmHolder{}
	objStore := dev.NewInMemStore(0)

	dataBucketName := "test-bucket"
	kvw := func(kvs []common.KV) error {
		return writeKV(kvs, lsmH, objStore, dataBucketName, common.DataFormatV1)
	}
	mgr, err := NewManager(lsmH, objStore, dataBucketName, common.DataFormatV1, nil, kvw)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)
	topicName := "test-topic"
	info := TopicInfo{
		Name:           topicName,
		PartitionCount: 10,
		RetentionTime:  -1,
	}
	id, err := mgr.CreateOrUpdateTopic(info, true)
	require.NoError(t, err)
	require.Equal(t, 1000, id)
	received, _, exists, err := mgr.GetTopicInfo(topicName)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, info.Name, received.Name)
	require.Equal(t, info.PartitionCount, received.PartitionCount)

	// Now double number of partitions
	info.PartitionCount *= 2
	id, err = mgr.CreateOrUpdateTopic(info, false)
	require.NoError(t, err)
	require.Equal(t, 1000, id)
	received, _, exists, err = mgr.GetTopicInfo(topicName)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, info.Name, received.Name)
	require.Equal(t, info.PartitionCount, received.PartitionCount)

	// Now restart
	err = mgr.Stop()
	require.NoError(t, err)
	mgr, err = NewManager(lsmH, objStore, "test-bucket", common.DataFormatV1, nil, kvw)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	received, _, exists, err = mgr.GetTopicInfo(info.Name)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, info.Name, received.Name)
	require.Equal(t, info.PartitionCount, received.PartitionCount)
}

func TestCreateTopicInfoInvalidPartitionCount(t *testing.T) {
	lsmH := &testLsmHolder{}
	objStore := dev.NewInMemStore(0)

	dataBucketName := "test-bucket"
	kvw := func(kvs []common.KV) error {
		return writeKV(kvs, lsmH, objStore, dataBucketName, common.DataFormatV1)
	}
	mgr, err := NewManager(lsmH, objStore, dataBucketName, common.DataFormatV1, nil, kvw)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)
	topicName := "test-topic"
	info := TopicInfo{
		Name:           topicName,
		PartitionCount: -1,
		RetentionTime:  -1,
	}
	_, err = mgr.CreateOrUpdateTopic(info, true)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.InvalidPartitionCount))
}

func TestUpdateTopicInfoInvalidPartitionCount(t *testing.T) {
	lsmH := &testLsmHolder{}
	objStore := dev.NewInMemStore(0)

	dataBucketName := "test-bucket"
	kvw := func(kvs []common.KV) error {
		return writeKV(kvs, lsmH, objStore, dataBucketName, common.DataFormatV1)
	}
	mgr, err := NewManager(lsmH, objStore, dataBucketName, common.DataFormatV1, nil, kvw)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)
	topicName := "test-topic"
	info := TopicInfo{
		Name:           topicName,
		PartitionCount: 10,
		RetentionTime:  -1,
	}
	_, err = mgr.CreateOrUpdateTopic(info, true)
	require.NoError(t, err)
	received, _, exists, err := mgr.GetTopicInfo(topicName)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, info.Name, received.Name)
	require.Equal(t, info.PartitionCount, received.PartitionCount)

	info.PartitionCount = -1
	_, err = mgr.CreateOrUpdateTopic(info, false)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.InvalidPartitionCount))
}

func TestCreateTopicInfoTopicAlreadyExists(t *testing.T) {
	lsmH := &testLsmHolder{}
	objStore := dev.NewInMemStore(0)

	dataBucketName := "test-bucket"
	kvw := func(kvs []common.KV) error {
		return writeKV(kvs, lsmH, objStore, dataBucketName, common.DataFormatV1)
	}
	mgr, err := NewManager(lsmH, objStore, dataBucketName, common.DataFormatV1, nil, kvw)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)
	topicName := "test-topic"
	info := TopicInfo{
		Name:           topicName,
		PartitionCount: 10,
		RetentionTime:  -1,
	}
	_, err = mgr.CreateOrUpdateTopic(info, true)
	require.NoError(t, err)

	_, err = mgr.CreateOrUpdateTopic(info, true)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.TopicAlreadyExists))
}

func TestUpdateTopicInfoInvalidTopic(t *testing.T) {
	lsmH := &testLsmHolder{}
	objStore := dev.NewInMemStore(0)

	dataBucketName := "test-bucket"
	kvw := func(kvs []common.KV) error {
		return writeKV(kvs, lsmH, objStore, dataBucketName, common.DataFormatV1)
	}
	mgr, err := NewManager(lsmH, objStore, dataBucketName, common.DataFormatV1, nil, kvw)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)
	topicName := "test-topic"
	info := TopicInfo{
		Name:           topicName,
		PartitionCount: 10,
		RetentionTime:  -1,
	}
	_, err = mgr.CreateOrUpdateTopic(info, true)
	require.NoError(t, err)
	received, _, exists, err := mgr.GetTopicInfo(topicName)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, info.Name, received.Name)
	require.Equal(t, info.PartitionCount, received.PartitionCount)

	info.Name = "unknown"
	_, err = mgr.CreateOrUpdateTopic(info, false)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.TopicDoesNotExist))
}

func TestGetTopicInfoSequence(t *testing.T) {
	lsmH := &testLsmHolder{}
	objStore := dev.NewInMemStore(0)

	dataBucketName := "test-bucket"
	kvw := func(kvs []common.KV) error {
		return writeKV(kvs, lsmH, objStore, dataBucketName, common.DataFormatV1)
	}
	mgr, err := NewManager(lsmH, objStore, dataBucketName, common.DataFormatV1, nil, kvw)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	numTopics := 100
	expectedSeq := TopicIDSequenceBase
	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%03d", i)
		info := TopicInfo{
			Name:           topicName,
			PartitionCount: i + 1,
			RetentionTime:  time.Duration(1000000 + i),
		}
		id, err := mgr.CreateOrUpdateTopic(info, true)
		require.NoError(t, err)
		require.Equal(t, expectedSeq, id)
		received, seq, exists, err := mgr.GetTopicInfo(topicName)
		require.NoError(t, err)
		require.True(t, exists)
		info.ID = id
		require.Equal(t, info, received)
		expectedSeq++
		require.Equal(t, expectedSeq, seq)
	}
	// Delete one - should make sequence increase again
	topicName := fmt.Sprintf("topic-%03d", numTopics-1)
	err = mgr.DeleteTopic(topicName)
	require.NoError(t, err)
	expectedSeq++

	topicName = fmt.Sprintf("topic-%03d", 0)
	_, seq, exists, err := mgr.GetTopicInfo(topicName)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, expectedSeq, seq)
}

func TestGetTopicInfoSequencePersisted(t *testing.T) {
	lsmH := &testLsmHolder{}
	objStore := dev.NewInMemStore(0)

	dataBucketName := "test-bucket"
	kvw := func(kvs []common.KV) error {
		return writeKV(kvs, lsmH, objStore, dataBucketName, common.DataFormatV1)
	}
	mgr, err := NewManager(lsmH, objStore, dataBucketName, common.DataFormatV1, nil, kvw)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	numTopics := 10
	var infos []TopicInfo
	expectedSeq := TopicIDSequenceBase
	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%03d", i)
		info := TopicInfo{
			Name:           topicName,
			PartitionCount: i + 1,
			RetentionTime:  time.Duration(1000000 + i),
		}
		id, err := mgr.CreateOrUpdateTopic(info, true)
		require.NoError(t, err)
		require.Equal(t, expectedSeq, id)
		received, seq, exists, err := mgr.GetTopicInfo(topicName)
		require.NoError(t, err)
		require.True(t, exists)
		info.ID = id
		require.Equal(t, info, received)
		expectedSeq++
		require.Equal(t, expectedSeq, seq)
		infos = append(infos, info)
	}

	// restart
	err = mgr.Stop()
	require.NoError(t, err)
	mgr, err = NewManager(lsmH, objStore, "test-bucket", common.DataFormatV1, nil, kvw)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%03d", i)
		received, seq, exists, err := mgr.GetTopicInfo(topicName)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, infos[i], received)
		require.Equal(t, expectedSeq, seq)
	}

	// Add more topics
	for i := numTopics; i < numTopics*2; i++ {
		topicName := fmt.Sprintf("topic-%03d", i)
		info := TopicInfo{
			Name:           topicName,
			ID:             expectedSeq,
			PartitionCount: i + 1,
			RetentionTime:  time.Duration(1000000 + i),
		}
		id, err := mgr.CreateOrUpdateTopic(info, true)
		require.NoError(t, err)
		require.Equal(t, expectedSeq, id)
		received, seq, exists, err := mgr.GetTopicInfo(topicName)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, info, received)
		expectedSeq++
		require.Equal(t, expectedSeq, seq)
		infos = append(infos, info)
	}

	// restart again
	err = mgr.Stop()
	require.NoError(t, err)
	mgr, err = NewManager(lsmH, objStore, "test-bucket", common.DataFormatV1, nil, kvw)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	for i := 0; i < numTopics*2; i++ {
		topicName := fmt.Sprintf("topic-%03d", i)
		received, seq, exists, err := mgr.GetTopicInfo(topicName)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, infos[i], received)
		require.Equal(t, expectedSeq, seq)
	}
}

func TestSerializeDeserializeTopicNotification(t *testing.T) {
	notif := TopicNotification{
		Sequence: 1234,
		Info: TopicInfo{
			ID:             123123,
			Name:           "topic1234",
			PartitionCount: 123,
			RetentionTime:  34123,
		},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = notif.Serialize(buff)
	var notif2 TopicNotification
	off := notif2.Deserialize(buff, 3)
	require.Equal(t, notif, notif2)
	require.Equal(t, off, len(buff))
}

type testLsmHolder struct {
	lock    sync.Mutex
	batches []lsm.RegistrationBatch
}

func (t *testLsmHolder) QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	var ids []lsm.NonOverlappingTables
	for _, batch := range t.batches {
		for _, reg := range batch.Registrations {
			if lsm.HasOverlap(keyStart, keyEnd, reg.KeyStart, reg.KeyEnd) {
				ids = append(ids, []lsm.QueryTableInfo{{
					ID: reg.TableID,
				}})
			}
		}
	}
	return ids, nil
}

func (t *testLsmHolder) ApplyLsmChanges(regBatch lsm.RegistrationBatch, completionFunc func(error) error) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	// prepend so newest are first
	t.batches = append([]lsm.RegistrationBatch{regBatch}, t.batches...)
	return completionFunc(nil)
}

func writeKV(kvs []common.KV, lsmH lsmHolder, objStore objstore.Client, dataBucketName string,
	dataFormat common.DataFormat) error {
	iter := common.NewKvSliceIterator(kvs)

	// Build ssTable
	table, smallestKey, largestKey, minVersion, maxVersion, err := sst.BuildSSTable(dataFormat, 0, 0, iter)
	if err != nil {
		return err
	}
	tableID := sst.CreateSSTableId()
	// Push ssTable to object store
	tableData, err := table.ToStorageBytes(compress.CompressionTypeNone)
	if err != nil {
		return err
	}
	if err := objstore.PutWithTimeout(objStore, dataBucketName, tableID, tableData, objStoreCallTimeout); err != nil {
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
	batch := lsm.RegistrationBatch{
		Registrations: []lsm.RegistrationEntry{regEntry},
	}
	ch := make(chan error, 1)
	if err := lsmH.ApplyLsmChanges(batch, func(err error) error {
		ch <- err
		return nil
	}); err != nil {
		return err
	}
	return <-ch
}
