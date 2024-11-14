package topicmeta

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNotifications(t *testing.T) {
	lsmH := &testLsmHolder{}
	objStore := dev.NewInMemStore(0)

	transports := transport.NewLocalTransports()
	mgr, err := NewManager(lsmH, objStore, "test-bucket", common.DataFormatV1, transports.CreateConnection)
	require.NoError(t, err)
	err = mgr.Start()
	require.NoError(t, err)

	controlClientFactory := func() (ControllerClient, error) {
		return &testControllerClient{mgr: mgr}, nil
	}

	numLocalCaches := 3
	var localCaches []*LocalCache
	var memberEntries []cluster.MembershipEntry
	var localCacheTransports []*transport.LocalServer
	for i := 0; i < numLocalCaches; i++ {
		localCache := NewLocalCache(controlClientFactory)
		localCaches = append(localCaches, localCache)
		address := uuid.New().String()
		memberData := common.MembershipData{
			ClusterListenAddress: address,
		}
		memberEntries = append(memberEntries, cluster.MembershipEntry{
			ID:   int32(i),
			Data: memberData.Serialize(nil),
		})
		localServer, err := transports.NewLocalServer(address)
		require.NoError(t, err)
		localCacheTransports = append(localCacheTransports, localServer)
		localServer.RegisterHandler(transport.HandlerIDMetaLocalCacheTopicAdded, localCache.HandleTopicAdded)
		localServer.RegisterHandler(transport.HandlerIDMetaLocalCacheTopicDeleted, localCache.HandleTopicDeleted)
	}

	mgr.MembershipChanged(cluster.MembershipState{
		ClusterVersion: 1,
		Members:        memberEntries,
	})

	// Create topics - this should cause notifications to be sent to the local caches
	numTopics := 10
	var infos []TopicInfo
	for i := 0; i < numTopics; i++ {
		info := TopicInfo{
			ID:             1000 + i,
			Name:           fmt.Sprintf("foo-topic-%d", i),
			PartitionCount: i + 1,
			RetentionTime:  time.Duration(i + 10000),
		}
		infos = append(infos, info)
		err = mgr.CreateTopic(info)
		require.NoError(t, err)
	}

	for _, localCache := range localCaches {
		receivedInfos := localCache.getTopicInfos()
		require.Equal(t, numTopics, len(receivedInfos))
		for _, info := range infos {
			require.Equal(t, info, receivedInfos[info.Name])
		}
	}

	// Now delete half of them
	for i := 0; i < numTopics/2; i++ {
		err = mgr.DeleteTopic(infos[i].Name)
		require.NoError(t, err)
	}

	for _, localCache := range localCaches {
		receivedInfos := localCache.getTopicInfos()
		require.Equal(t, numTopics/2, len(receivedInfos))
		for i, info := range infos {
			received, ok := receivedInfos[info.Name]
			if i < len(infos)/2 {
				require.False(t, ok)
			} else {
				require.Equal(t, info, received)
			}
		}
	}

	// Create another topic
	info := TopicInfo{
		ID:             1000 + numTopics + 5,
		Name:           fmt.Sprintf("foo-topic-%d", numTopics),
		PartitionCount: numTopics + 1,
		RetentionTime:  time.Duration(numTopics + 10000),
	}
	infos = append(infos, info)
	err = mgr.CreateTopic(info)
	require.NoError(t, err)

	for _, localCache := range localCaches {
		receivedInfos := localCache.getTopicInfos()
		require.Equal(t, numTopics/2+1, len(receivedInfos))
		for i, info2 := range infos {
			received, ok := receivedInfos[info2.Name]
			if i < len(infos)/2 {
				require.False(t, ok)
			} else {
				require.Equal(t, info2, received)
			}
		}
	}

	// Send notifications with invalid sequence
	var notif TopicNotification
	notif.Sequence = 1023
	notif.Info = TopicInfo{
		ID:             1000 + numTopics + 1,
		Name:           fmt.Sprintf("foo-topic-%d", numTopics+1),
		PartitionCount: numTopics + 1 + 1,
		RetentionTime:  time.Duration(numTopics + 1 + 10000),
	}
	buff := notif.Serialize(nil)
	for _, localCache := range localCaches {
		err = localCache.HandleTopicAdded(nil, buff, nil, func(response []byte, err error) error {
			return nil
		})
		require.NoError(t, err)
		// Cache should be invalidated
		require.Equal(t, 0, len(localCache.getTopicInfos()))
	}
}

type testControllerClient struct {
	mgr *Manager
}

func (t *testControllerClient) GetTopicInfo(topicName string) (TopicInfo, int, bool, error) {
	return t.mgr.GetTopicInfo(topicName)
}

func (t *testControllerClient) Close() error {
	return nil
}
