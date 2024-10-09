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
	GetOffsets(infos []offsets.GetOffsetTopicInfo) ([]int64, error)

	ApplyLsmChanges(regBatch lsm.RegistrationBatch) error

	RegisterL0Table(writtenOffsetInfos []offsets.UpdateWrittenOffsetInfo, regEntry lsm.RegistrationEntry) error

	QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error)

	FetchTablesForPrefix(topicID int, partitionID int, prefix []byte, offsetStart int64) (lsm.OverlappingTables, int64, error)

	PollForJob() (lsm.CompactionJob, error)

	GetTopicInfo(topicName string) (topicmeta.TopicInfo, int, error)

	CreateTopic(topicInfo topicmeta.TopicInfo) error

	DeleteTopic(topicName string) error

	Close() error
}

// on error, the caller must close the connection
type client struct {
	m              *Controller
	lock           sync.RWMutex
	clusterVersion int
	address        string
	conn           transport.Connection
	connFactory    transport.ConnectionFactory
	closed         bool
}

var _ Client = &client{}

func (c *client) RegisterL0Table(writtenOffsetInfos []offsets.UpdateWrittenOffsetInfo, regEntry lsm.RegistrationEntry) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	req := RegisterL0Request{
		ClusterVersion: c.clusterVersion,
		OffsetInfos:    writtenOffsetInfos,
		RegEntry:       regEntry,
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
		ClusterVersion: c.clusterVersion,
		RegBatch:       regBatch,
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
		ClusterVersion: c.clusterVersion,
		KeyStart:       keyStart,
		KeyEnd:         keyEnd,
	}
	request := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerQueryTablesInRange, request)
	if err != nil {
		return nil, err
	}
	return lsm.DeserializeOverlappingTables(respBuff, 0), nil
}

func (c *client) FetchTablesForPrefix(topicID int, partitionID int, prefix []byte, offsetStart int64) (lsm.OverlappingTables, int64, error) {
	// TODO
	return nil, 0, nil
}

func (c *client) GetOffsets(infos []offsets.GetOffsetTopicInfo) ([]int64, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	req := GetOffsetsRequest{
		ClusterVersion: c.clusterVersion,
		Infos:          infos,
	}
	request := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerGetOffsets, request)
	if err != nil {
		return nil, err
	}
	var resp GetOffsetsResponse
	resp.Deserialize(respBuff, 0)
	return resp.Offsets, nil
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

func (c *client) GetTopicInfo(topicName string) (topicmeta.TopicInfo, int, error) {
	conn, err := c.getConnection()
	if err != nil {
		return topicmeta.TopicInfo{}, 0, err
	}
	req := GetTopicInfoRequest{
		ClusterVersion: c.clusterVersion,
		TopicName:      topicName,
	}
	buff := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerGetTopicInfo, buff)
	if err != nil {
		return topicmeta.TopicInfo{}, 0, err
	}
	var resp GetTopicInfoResponse
	resp.Deserialize(respBuff, 0)
	return resp.Info, resp.Sequence, nil
}

func (c *client) CreateTopic(topicInfo topicmeta.TopicInfo) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	req := CreateTopicRequest{
		ClusterVersion: c.clusterVersion,
		Info:           topicInfo,
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
		ClusterVersion: c.clusterVersion,
		TopicName:      topicName,
	}
	buff := req.Serialize(createRequestBuffer())
	_, err = conn.SendRPC(transport.HandlerIDControllerDeleteTopic, buff)
	return err
}

func (c *client) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.address = ""
	c.closed = true
	if c.conn != nil {
		return c.conn.Close()
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
