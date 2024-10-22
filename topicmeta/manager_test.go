package topicmeta

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestManager(t *testing.T) {
	lsmH := &testLsmHolder{}
	objStore := dev.NewInMemStore(0)

	mgr, err := NewManager(lsmH, objStore, "test-bucket", common.DataFormatV1, nil)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	numTopics := 100
	var infos []TopicInfo

	expectedSeq := topicIDSequenceBase
	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%03d", i)
		_, _, err := mgr.GetTopicInfo(topicName)
		require.Error(t, err)
		require.True(t, common.IsTektiteErrorWithCode(err, common.TopicDoesNotExist))
		info := TopicInfo{
			Name:           topicName,
			ID:             expectedSeq,
			PartitionCount: i + 1,
			RetentionTime:  time.Duration(1000000 + i),
		}
		err = mgr.CreateTopic(info)
		require.NoError(t, err)
		received, seq, err := mgr.GetTopicInfo(topicName)
		require.NoError(t, err)
		require.Equal(t, info, received)
		expectedSeq++
		require.Equal(t, expectedSeq, seq)
		infos = append(infos, info)
	}

	// Now restart
	err = mgr.Stop()
	require.NoError(t, err)
	mgr, err = NewManager(lsmH, objStore, "test-bucket", common.DataFormatV1, nil)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	// Topics should still be there
	for _, info := range infos {
		received, _, err := mgr.GetTopicInfo(info.Name)
		require.NoError(t, err)
		require.Equal(t, info, received)
	}

	// Now delete half of them
	for i := 0; i < len(infos)/2; i++ {
		info := infos[i]
		err = mgr.DeleteTopic(info.Name)
		require.NoError(t, err)
		_, _, err := mgr.GetTopicInfo(info.Name)
		require.Error(t, err)
		require.True(t, common.IsTektiteErrorWithCode(err, common.TopicDoesNotExist))
	}

	// Rest should still be there
	for i := len(infos) / 2; i < len(infos); i++ {
		info := infos[i]
		received, _, err := mgr.GetTopicInfo(info.Name)
		require.NoError(t, err)
		require.Equal(t, info, received)
	}

	// Restart again
	err = mgr.Stop()
	require.NoError(t, err)
	mgr, err = NewManager(lsmH, objStore, "test-bucket", common.DataFormatV1, nil)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	for i, info := range infos {
		received, _, err := mgr.GetTopicInfo(info.Name)
		if i < len(infos)/2 {
			require.Error(t, err)
			require.True(t, common.IsTektiteErrorWithCode(err, common.TopicDoesNotExist))
		} else {
			require.NoError(t, err)
			require.Equal(t, info, received)
		}
	}

	// Delete the rest
	for i := len(infos) / 2; i < len(infos); i++ {
		info := infos[i]
		err = mgr.DeleteTopic(info.Name)
		require.NoError(t, err)
		_, _, err := mgr.GetTopicInfo(info.Name)
		require.Error(t, err)
		require.True(t, common.IsTektiteErrorWithCode(err, common.TopicDoesNotExist))
	}

	// Restart again
	err = mgr.Stop()
	require.NoError(t, err)
	mgr, err = NewManager(lsmH, objStore, "test-bucket", common.DataFormatV1, nil)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	// Should be none
	for _, info := range infos {
		_, _, err := mgr.GetTopicInfo(info.Name)
		require.Error(t, err)
		require.True(t, common.IsTektiteErrorWithCode(err, common.TopicDoesNotExist))
	}
}

func TestGetTopicInfoSequence(t *testing.T) {
	lsmH := &testLsmHolder{}
	objStore := dev.NewInMemStore(0)

	mgr, err := NewManager(lsmH, objStore, "test-bucket", common.DataFormatV1, nil)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	numTopics := 100
	expectedSeq := topicIDSequenceBase
	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%03d", i)
		info := TopicInfo{
			Name:           topicName,
			ID:             expectedSeq,
			PartitionCount: i + 1,
			RetentionTime:  time.Duration(1000000 + i),
		}
		err = mgr.CreateTopic(info)
		require.NoError(t, err)
		received, seq, err := mgr.GetTopicInfo(topicName)
		require.NoError(t, err)
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
	_, seq, err := mgr.GetTopicInfo(topicName)
	require.NoError(t, err)
	require.Equal(t, expectedSeq, seq)
}

func TestGetTopicInfoSequencePersisted(t *testing.T) {
	lsmH := &testLsmHolder{}
	objStore := dev.NewInMemStore(0)

	mgr, err := NewManager(lsmH, objStore, "test-bucket", common.DataFormatV1, nil)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	numTopics := 10
	var infos []TopicInfo
	expectedSeq := topicIDSequenceBase
	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%03d", i)
		info := TopicInfo{
			Name:           topicName,
			ID:             expectedSeq,
			PartitionCount: i + 1,
			RetentionTime:  time.Duration(1000000 + i),
		}
		err = mgr.CreateTopic(info)
		require.NoError(t, err)
		received, seq, err := mgr.GetTopicInfo(topicName)
		require.NoError(t, err)
		require.Equal(t, info, received)
		expectedSeq++
		require.Equal(t, expectedSeq, seq)
		infos = append(infos, info)
	}

	// restart
	err = mgr.Stop()
	require.NoError(t, err)
	mgr, err = NewManager(lsmH, objStore, "test-bucket", common.DataFormatV1, nil)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("topic-%03d", i)
		received, seq, err := mgr.GetTopicInfo(topicName)
		require.NoError(t, err)
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
		err = mgr.CreateTopic(info)
		require.NoError(t, err)
		received, seq, err := mgr.GetTopicInfo(topicName)
		require.NoError(t, err)
		require.Equal(t, info, received)
		expectedSeq++
		require.Equal(t, expectedSeq, seq)
		infos = append(infos, info)
	}

	// restart again
	err = mgr.Stop()
	require.NoError(t, err)
	mgr, err = NewManager(lsmH, objStore, "test-bucket", common.DataFormatV1, nil)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	for i := 0; i < numTopics*2; i++ {
		topicName := fmt.Sprintf("topic-%03d", i)
		received, seq, err := mgr.GetTopicInfo(topicName)
		require.NoError(t, err)
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
