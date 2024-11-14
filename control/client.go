package control

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"sync"
)

type Client interface {
	PrePush(infos []offsets.GenerateOffsetTopicInfo, epochInfos []EpochInfo) ([]offsets.OffsetTopicInfo, int64,
		[]bool, error)

	GetOffsetInfos(infos []offsets.GetOffsetTopicInfo) ([]offsets.OffsetTopicInfo, error)

	ApplyLsmChanges(regBatch lsm.RegistrationBatch) error

	RegisterL0Table(sequence int64, regEntry lsm.RegistrationEntry) error

	QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error)

	RegisterTableListener(topicID int, partitionID int, memberID int32, resetSequence int64) (int64, error)

	PollForJob() (lsm.CompactionJob, error)

	GetTopicInfo(topicName string) (topicmeta.TopicInfo, int, bool, error)

	GetAllTopicInfos() ([]topicmeta.TopicInfo, error)

	CreateTopic(topicInfo topicmeta.TopicInfo) error

	DeleteTopic(topicName string) error

	GetCoordinatorInfo(key string) (memberID int32, address string, groupEpoch int, err error)

	GenerateSequence(sequenceName string) (int64, error)

	Close() error
}

// on error, the caller must close the connection
type client struct {
	m             *Controller
	lock          sync.RWMutex
	leaderVersion int
	address       string
	conn          transport.Connection
	connFactory   transport.ConnectionFactory
	closed        bool
}

var _ Client = &client{}

func (c *client) RegisterL0Table(sequence int64, regEntry lsm.RegistrationEntry) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	req := RegisterL0Request{
		LeaderVersion: c.leaderVersion,
		Sequence:      sequence,
		RegEntry:      regEntry,
	}
	request := req.Serialize(createRequestBuffer())
	_, err = conn.SendRPC(transport.HandlerIDControllerRegisterL0Table, request)
	return err
}

func (c *client) ApplyLsmChanges(regBatch lsm.RegistrationBatch) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	req := ApplyChangesRequest{
		LeaderVersion: c.leaderVersion,
		RegBatch:      regBatch,
	}
	request := req.Serialize(createRequestBuffer())
	_, err = conn.SendRPC(transport.HandlerIDControllerApplyChanges, request)
	return err
}

func (c *client) QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	req := QueryTablesInRangeRequest{
		LeaderVersion: c.leaderVersion,
		KeyStart:      keyStart,
		KeyEnd:        keyEnd,
	}
	request := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerQueryTablesInRange, request)
	if err != nil {
		return nil, err
	}
	queryRes, _ := lsm.DeserializeOverlappingTables(respBuff, 0)
	return queryRes, nil
}

func (c *client) RegisterTableListener(topicID int, partitionID int, memberID int32, resetSequence int64) (int64, error) {
	conn, err := c.getConnection()
	if err != nil {
		return 0, err
	}
	req := RegisterTableListenerRequest{
		LeaderVersion: c.leaderVersion,
		TopicID:       topicID,
		PartitionID:   partitionID,
		MemberID:      memberID,
		ResetSequence: resetSequence,
	}
	request := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerRegisterTableListener, request)
	if err != nil {
		return 0, err
	}
	var resp RegisterTableListenerResponse
	resp.Deserialize(respBuff, 0)
	return resp.LastReadableOffset, nil
}

func (c *client) PrePush(infos []offsets.GenerateOffsetTopicInfo, epochInfos []EpochInfo) ([]offsets.OffsetTopicInfo,
	int64, []bool, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, 0, nil, err
	}
	req := PrePushRequest{
		LeaderVersion: c.leaderVersion,
		Infos:         infos,
		EpochInfos:    epochInfos,
	}
	request := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerPrepush, request)
	if err != nil {
		return nil, 0, nil, err
	}
	var resp PrePushResponse
	resp.Deserialize(respBuff, 0)
	return resp.Offsets, resp.Sequence, resp.EpochsOK, nil
}

func (c *client) GetOffsetInfos(infos []offsets.GetOffsetTopicInfo) ([]offsets.OffsetTopicInfo, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	req := GetOffsetInfoRequest{
		LeaderVersion:       c.leaderVersion,
		GetOffsetTopicInfos: infos,
	}
	request := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerGetOffsetInfo, request)
	if err != nil {
		return nil, err
	}
	var resp GetOffsetInfoResponse
	resp.Deserialize(respBuff, 0)
	return resp.OffsetInfos, nil
}

func (c *client) PollForJob() (lsm.CompactionJob, error) {
	conn, err := c.getConnection()
	if err != nil {
		return lsm.CompactionJob{}, err
	}
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerPollForJob, createRequestBuffer())
	if err != nil {
		return lsm.CompactionJob{}, err
	}
	var job lsm.CompactionJob
	job.Deserialize(respBuff, 0)
	return job, nil
}

func (c *client) GetTopicInfo(topicName string) (topicmeta.TopicInfo, int, bool, error) {
	conn, err := c.getConnection()
	if err != nil {
		return topicmeta.TopicInfo{}, 0, false, err
	}
	req := GetTopicInfoRequest{
		LeaderVersion: c.leaderVersion,
		TopicName:     topicName,
	}
	buff := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerGetTopicInfo, buff)
	if err != nil {
		return topicmeta.TopicInfo{}, 0, false, err
	}
	var resp GetTopicInfoResponse
	resp.Deserialize(respBuff, 0)
	return resp.Info, resp.Sequence, resp.Exists, nil
}

func (c *client) GetAllTopicInfos() ([]topicmeta.TopicInfo, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	req := GetAllTopicInfosRequest{
		LeaderVersion: c.leaderVersion,
	}
	buff := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerGetAllTopicInfos, buff)
	if err != nil {
		return nil, err
	}
	var resp GetAllTopicInfosResponse
	resp.Deserialize(respBuff, 0)
	return resp.TopicInfos, nil
}

func (c *client) CreateTopic(topicInfo topicmeta.TopicInfo) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	req := CreateTopicRequest{
		LeaderVersion: c.leaderVersion,
		Info:          topicInfo,
	}
	buff := req.Serialize(createRequestBuffer())
	_, err = conn.SendRPC(transport.HandlerIDControllerCreateTopic, buff)
	return err
}

func (c *client) DeleteTopic(topicName string) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	req := DeleteTopicRequest{
		LeaderVersion: c.leaderVersion,
		TopicName:     topicName,
	}
	buff := req.Serialize(createRequestBuffer())
	_, err = conn.SendRPC(transport.HandlerIDControllerDeleteTopic, buff)
	return err
}

func (c *client) GetCoordinatorInfo(groupID string) (int32, string, int, error) {
	conn, err := c.getConnection()
	if err != nil {
		return 0, "", 0, err
	}
	req := GetGroupCoordinatorInfoRequest{
		LeaderVersion: c.leaderVersion,
		GroupID:       groupID,
	}
	buff := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerGetGroupCoordinatorInfo, buff)
	if err != nil {
		return 0, "", 0, err
	}
	var resp GetGroupCoordinatorInfoResponse
	resp.Deserialize(respBuff, 0)
	return resp.MemberID, resp.Address, resp.GroupEpoch, nil
}

func (c *client) GenerateSequence(sequenceName string) (int64, error) {
	conn, err := c.getConnection()
	if err != nil {
		return 0, err
	}
	req := GenerateSequenceRequest{
		LeaderVersion: c.leaderVersion,
		SequenceName:  sequenceName,
	}
	buff := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerGenerateSequence, buff)
	if err != nil {
		return 0, err
	}
	var resp GenerateSequenceResponse
	resp.Deserialize(respBuff, 0)
	return resp.Sequence, nil
}

func (c *client) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.address = ""
	c.closed = true
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return err
		}
		c.conn = nil
	}
	return nil
}

func (c *client) getConnection() (transport.Connection, error) {
	conn, err := c.getCachedConnection()
	if err != nil {
		return nil, err
	}
	if conn != nil {
		return conn, nil
	}
	if err := c.createConnection(); err != nil {
		return nil, err
	}
	return c.conn, nil
}

func (c *client) getCachedConnection() (transport.Connection, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.closed {
		return nil, errors.New("client has been closed")
	}
	return c.conn, nil
}

func (c *client) createConnection() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.conn != nil {
		return nil
	}
	conn, err := c.connFactory(c.address)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func createRequestBuffer() []byte {
	buff := make([]byte, 0, 128)                  // Initial size guess
	buff = binary.BigEndian.AppendUint16(buff, 1) // rpc version - currently 1
	return buff
}
