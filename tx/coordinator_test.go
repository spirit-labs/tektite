package tx

import (
	"encoding/binary"
	"errors"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestInitProducerNoTransactionalID(t *testing.T) {
	cfg := NewConf()
	producerID := int64(23)
	controlClient := &testControlClient{seq: producerID}
	clientFactory := func() (control.Client, error) {
		return controlClient, nil
	}
	controlClientCache := control.NewClientCache(10, clientFactory)
	tableGetter := &testTableGetter{}
	localTransports := transport.NewLocalTransports()
	partHashes, err := parthash.NewPartitionHashes(0)
	require.NoError(t, err)
	topicProvider := &testTopicInfoProvider{infos: map[string]topicmeta.TopicInfo{}}
	connCaches := transport.NewConnCaches(10, localTransports.CreateConnection)
	coordinator := NewCoordinator(cfg, controlClientCache, tableGetter.getTable, connCaches,
		topicProvider, partHashes)
	numRequests := 100
	for i := 0; i < numRequests; i++ {
		req := &kafkaprotocol.InitProducerIdRequest{}
		resp := coordinator.HandleInitProducerID(req)
		require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.ErrorCode))
		require.Equal(t, producerID, resp.ProducerId)
		require.Equal(t, 0, int(resp.ProducerEpoch))
		producerID++
	}
}

func TestInitProducerErrorUnavailableError(t *testing.T) {
	testInitProducerError(t, common.NewTektiteErrorf(common.Unavailable, "force unavailable"),
		kafkaprotocol.ErrorCodeCoordinatorNotAvailable)
}

func TestInitProducerErrorUnexpectedError(t *testing.T) {
	testInitProducerError(t, errors.New("force unavailable"),
		kafkaprotocol.ErrorCodeUnknownServerError)
}

func testInitProducerError(t *testing.T, injectError error, expectedErrCode int) {
	cfg := NewConf()
	producerID := int64(23)
	writerEpoch := 7
	controlClient := &testControlClient{
		seq:                 producerID,
		coordinatorMemberID: 1,
		coordinatorAddress:  "some-address",
		coordinatorEpoch:    writerEpoch,
	}
	clientFactory := func() (control.Client, error) {
		return controlClient, nil
	}
	controlClientCache := control.NewClientCache(10, clientFactory)

	tableGetter := &testTableGetter{}
	localTransports := transport.NewLocalTransports()
	topicProvider := &testTopicInfoProvider{infos: map[string]topicmeta.TopicInfo{}}
	partHashes, err := parthash.NewPartitionHashes(0)
	require.NoError(t, err)
	connCaches := transport.NewConnCaches(10, localTransports.CreateConnection)
	coordinator := NewCoordinator(cfg, controlClientCache, tableGetter.getTable, connCaches,
		topicProvider, partHashes)

	fp := &fakePusherSink{}
	transportServer, err := localTransports.NewLocalServer(uuid.New().String())
	require.NoError(t, err)
	transportServer.RegisterHandler(transport.HandlerIDTablePusherDirectWrite, fp.HandleDirectWrite)
	memberData := common.MembershipData{
		ClusterListenAddress: transportServer.Address(),
	}

	err = coordinator.MembershipChanged(0, cluster.MembershipState{
		LeaderVersion:  1,
		ClusterVersion: 1,
		Members: []cluster.MembershipEntry{
			{
				ID:   0,
				Data: memberData.Serialize(nil),
			},
		},
	})
	require.NoError(t, err)

	transactionalID := "transactionalID1"

	fp.directWriteErr = injectError

	req := &kafkaprotocol.InitProducerIdRequest{
		TransactionalId: common.StrPtr(transactionalID),
	}
	resp := coordinator.HandleInitProducerID(req)
	require.Equal(t, expectedErrCode, int(resp.ErrorCode))
}

func TestInitProducerWithTransactionalID(t *testing.T) {
	cfg := NewConf()
	producerID := int64(23)
	writerEpoch := 7
	controlClient := &testControlClient{
		seq:                 producerID,
		coordinatorMemberID: 1,
		coordinatorAddress:  "some-address",
		coordinatorEpoch:    writerEpoch,
	}
	clientFactory := func() (control.Client, error) {
		return controlClient, nil
	}
	controlClientCache := control.NewClientCache(10, clientFactory)

	tableGetter := &testTableGetter{}
	localTransports := transport.NewLocalTransports()
	topicProvider := &testTopicInfoProvider{infos: map[string]topicmeta.TopicInfo{}}
	partHashes, err := parthash.NewPartitionHashes(0)
	connCaches := transport.NewConnCaches(10, localTransports.CreateConnection)
	coordinator := NewCoordinator(cfg, controlClientCache, tableGetter.getTable, connCaches,
		topicProvider, partHashes)
	fp := &fakePusherSink{}
	transportServer, err := localTransports.NewLocalServer(uuid.New().String())
	require.NoError(t, err)
	transportServer.RegisterHandler(transport.HandlerIDTablePusherDirectWrite, fp.HandleDirectWrite)
	memberData := common.MembershipData{
		ClusterListenAddress: transportServer.Address(),
	}

	err = coordinator.MembershipChanged(0, cluster.MembershipState{
		LeaderVersion:  1,
		ClusterVersion: 1,
		Members: []cluster.MembershipEntry{
			{
				ID:   0,
				Data: memberData.Serialize(nil),
			},
		},
	})
	require.NoError(t, err)

	transactionalID := "transactionalID1"

	numInits := 10
	expectedProducerEpoch := 0
	for i := 0; i < numInits; i++ {
		req := &kafkaprotocol.InitProducerIdRequest{
			TransactionalId: common.StrPtr(transactionalID),
		}
		resp := coordinator.HandleInitProducerID(req)
		require.NoError(t, err)
		require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.ErrorCode))
		require.Equal(t, producerID, resp.ProducerId)
		require.Equal(t, expectedProducerEpoch, int(resp.ProducerEpoch))

		received, rcpVer := fp.getReceived()
		require.NotNil(t, received)

		require.Equal(t, 1, int(rcpVer))

		require.Equal(t, "t."+transactionalID, received.WriterKey)
		require.Equal(t, writerEpoch, received.WriterEpoch)

		partHash, err := parthash.CreateHash([]byte("t." + transactionalID))
		require.NoError(t, err)

		require.Equal(t, 1, len(received.KVs))

		storedState := txStoredState{
			pid:           resp.ProducerId,
			producerEpoch: resp.ProducerEpoch,
		}
		expectedKV := createExpectedKV(partHash, &storedState)

		require.Equal(t, expectedKV, received.KVs[0])

		// setup a table with the stored txstate
		iter := common.NewKvSliceIterator([]common.KV{expectedKV})
		table, _, _, _, _, err := sst.BuildSSTable(common.DataFormatV1, 0, 0, iter)
		require.NoError(t, err)
		tableGetter.table = table
		tableID := sst.CreateSSTableId()
		controlClient.queryRes = []lsm.NonOverlappingTables{
			[]lsm.QueryTableInfo{
				{
					ID: []byte(tableID),
				},
			},
		}
		expectedProducerEpoch++

		// increment the writer epoch too so we can test it's stored in the state
		writerEpoch++
		controlClient.coordinatorEpoch = writerEpoch
	}
}

func createExpectedKV(partHash []byte, storedState *txStoredState) common.KV {
	kvKey := encoding.EncodeVersion(partHash, 0)
	value := make([]byte, 0, 32)
	value = binary.BigEndian.AppendUint16(value, transactionMetadataVersion)
	value = storedState.Serialize(value)
	value = common.AppendValueMetadata(value)
	return common.KV{Key: kvKey, Value: value}
}

type testTableGetter struct {
	table *sst.SSTable
}

func (t *testTableGetter) getTable(tableID sst.SSTableID) (*sst.SSTable, error) {
	return t.table, nil
}

type testControlClient struct {
	lock                sync.Mutex
	seq                 int64
	queryRes            lsm.OverlappingTables
	coordinatorMemberID int32
	coordinatorAddress  string
	coordinatorEpoch    int
}

func (t *testControlClient) PrePush(infos []offsets.GenerateOffsetTopicInfo, epochInfos []control.EpochInfo) ([]offsets.OffsetTopicInfo, int64, []bool, error) {
	panic("should not be called")
}

func (t *testControlClient) ApplyLsmChanges(regBatch lsm.RegistrationBatch) error {
	panic("should not be called")
}

func (t *testControlClient) RegisterL0Table(sequence int64, regEntry lsm.RegistrationEntry) error {
	panic("should not be called")
}

func (t *testControlClient) QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error) {
	return t.queryRes, nil
}

func (t *testControlClient) RegisterTableListener(topicID int, partitionID int, memberID int32, resetSequence int64) (int64, error) {
	panic("should not be called")
}

func (t *testControlClient) PollForJob() (lsm.CompactionJob, error) {
	panic("should not be called")
}

func (t *testControlClient) GetOffsetInfos(infos []offsets.GetOffsetTopicInfo) ([]offsets.OffsetTopicInfo, error) {
	panic("should not be called")
}

func (t *testControlClient) GetTopicInfo(topicName string) (topicmeta.TopicInfo, int, bool, error) {
	panic("should not be called")
}

func (t *testControlClient) GetTopicInfoByID(topicID int) (topicmeta.TopicInfo, bool, error) {
	panic("should not be called")
}

func (t *testControlClient) GetAllTopicInfos() ([]topicmeta.TopicInfo, error) {
	panic("should not be called")
}

func (t *testControlClient) CreateTopic(topicInfo topicmeta.TopicInfo) error {
	panic("should not be called")
}

func (t *testControlClient) DeleteTopic(topicName string) error {
	panic("should not be called")
}

func (t *testControlClient) GetCoordinatorInfo(key string) (memberID int32, address string, groupEpoch int, err error) {
	return t.coordinatorMemberID, t.coordinatorAddress, t.coordinatorEpoch, nil
}

func (t *testControlClient) GenerateSequence(sequenceName string) (int64, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	seq := t.seq
	t.seq++
	return seq, nil
}

func (t *testControlClient) PutUserCredentials(username string, storedKey []byte, serverKey []byte, salt string, iters int) error {
	panic("should not be called")
}

func (t *testControlClient) DeleteUserCredentials(username string) error {
	panic("should not be called")
}

func (t *testControlClient) Close() error {
	return nil
}

type fakePusherSink struct {
	lock               sync.Mutex
	receivedRPCVersion int16
	received           *common.DirectWriteRequest
	directWriteErr     error
}

func (f *fakePusherSink) HandleDirectWrite(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	if f.directWriteErr != nil {
		return f.directWriteErr
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	f.received = &common.DirectWriteRequest{}
	f.receivedRPCVersion = int16(binary.BigEndian.Uint16(request))
	f.received.Deserialize(request, 2)

	return responseWriter(responseBuff, nil)
}

func (f *fakePusherSink) getReceived() (*common.DirectWriteRequest, int16) {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.received, f.receivedRPCVersion
}

type testTopicInfoProvider struct {
	infos map[string]topicmeta.TopicInfo
}

func (t *testTopicInfoProvider) GetTopicInfo(topicName string) (topicmeta.TopicInfo, bool, error) {
	info, ok := t.infos[topicName]
	if !ok {
		return topicmeta.TopicInfo{}, false, nil
	}
	return info, true, nil
}
