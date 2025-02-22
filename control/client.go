package control

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/acls"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"sync"
)

type Client interface {
	PrePush(infos []offsets.GenerateOffsetTopicInfo, epochInfos []EpochInfo) ([]offsets.OffsetTopicInfo, int64,
		[]bool, error)

	RegisterL0Table(sequence int64, regEntry lsm.RegistrationEntry) error

	GetOffsetInfos(infos []offsets.GetOffsetTopicInfo) ([]offsets.OffsetTopicInfo, error)

	ApplyLsmChanges(regBatch lsm.RegistrationBatch) error

	QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error)

	QueryTablesForPartition(topicID int, partitionID int, keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, int64, error)

	PollForJob() (lsm.CompactionJob, error)

	GetTopicInfo(topicName string) (topicmeta.TopicInfo, int, bool, error)

	GetTopicInfoByID(topicID int) (topicmeta.TopicInfo, bool, error)

	GetAllTopicInfos() ([]topicmeta.TopicInfo, error)

	CreateOrUpdateTopic(topicInfo topicmeta.TopicInfo, create bool) error

	DeleteTopic(topicName string) error

	GetCoordinatorInfo(key string) (memberID int32, address string, groupEpoch int, err error)

	GenerateSequence(sequenceName string) (int64, error)

	PutUserCredentials(username string, storedKey []byte, serverKey []byte, salt string, iters int) error

	DeleteUserCredentials(username string) error

	Authorise(principal string, resourceType acls.ResourceType, resourceName string, operation acls.Operation) (bool, error)

	CreateAcls(aclEntries []acls.AclEntry) error

	ListAcls(resourceType acls.ResourceType, resourceNameFilter string, patternTypeFilter acls.ResourcePatternType,
		principal string, host string, operation acls.Operation, permission acls.Permission) ([]acls.AclEntry, error)

	DeleteAcls(resourceType acls.ResourceType, resourceNameFilter string, patternTypeFilter acls.ResourcePatternType,
		principal string, host string, operation acls.Operation, permission acls.Permission) error

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

func (c *client) QueryTablesForPartition(topicID int, partitionID int, keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, int64, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, 0, err
	}
	req := QueryTablesForPartitionRequest{
		LeaderVersion: c.leaderVersion,
		TopicID:       topicID,
		PartitionID:   partitionID,
		KeyStart:      keyStart,
		KeyEnd:        keyEnd,
	}
	request := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerQueryTablesForPartition, request)
	if err != nil {
		return nil, 0, err
	}
	var res QueryTablesForPartitionResponse
	res.Deserialize(respBuff, 0)
	return res.Overlapping, res.LastReadableOffset, nil
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

func (c *client) GetTopicInfoByID(topicID int) (topicmeta.TopicInfo, bool, error) {
	conn, err := c.getConnection()
	if err != nil {
		return topicmeta.TopicInfo{}, false, err
	}
	req := GetTopicInfoByIDRequest{
		LeaderVersion: c.leaderVersion,
		TopicID:       topicID,
	}
	buff := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerGetTopicInfoByID, buff)
	if err != nil {
		return topicmeta.TopicInfo{}, false, err
	}
	var resp GetTopicInfoResponse
	resp.Deserialize(respBuff, 0)
	return resp.Info, resp.Exists, nil
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

func (c *client) CreateOrUpdateTopic(topicInfo topicmeta.TopicInfo, create bool) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	req := CreateOrUpdateTopicRequest{
		LeaderVersion: c.leaderVersion,
		Create:        create,
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
		Key:           groupID,
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

func (c *client) PutUserCredentials(username string, storedKey []byte, serverKey []byte, salt string, iters int) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	req := PutUserCredentialsRequest{
		LeaderVersion: c.leaderVersion,
		Username:      username,
		StoredKey:     storedKey,
		ServerKey:     serverKey,
		Salt:          salt,
		Iters:         iters,
	}
	buff := req.Serialize(createRequestBuffer())
	_, err = conn.SendRPC(transport.HandlerIDControllerPutUserCredentials, buff)
	return err
}

func (c *client) DeleteUserCredentials(username string) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	req := DeleteUserCredentialsRequest{
		LeaderVersion: c.leaderVersion,
		Username:      username,
	}
	buff := req.Serialize(createRequestBuffer())
	_, err = conn.SendRPC(transport.HandlerIDControllerDeleteUserCredentials, buff)
	return err
}

func (c *client) Authorise(principal string, resourceType acls.ResourceType, resourceName string,
	operation acls.Operation) (bool, error) {
	conn, err := c.getConnection()
	if err != nil {
		return false, err
	}
	req := AuthoriseRequest{
		LeaderVersion: c.leaderVersion,
		Principal:     principal,
		ResourceType:  resourceType,
		ResourceName:  resourceName,
		Operation:     operation,
	}
	buff := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerAuthorise, buff)
	if err != nil {
		return false, err
	}
	var resp AuthoriseResponse
	resp.Deserialize(respBuff, 0)
	return resp.Authorised, nil
}

func (c *client) CreateAcls(aclEntries []acls.AclEntry) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	req := CreateAclsRequest{
		LeaderVersion: c.leaderVersion,
		AclEntries:    aclEntries,
	}
	buff := req.Serialize(createRequestBuffer())
	_, err = conn.SendRPC(transport.HandlerIDControllerCreateAcls, buff)
	return err
}

func (c *client) ListAcls(resourceType acls.ResourceType, resourceNameFilter string, patternTypeFilter acls.ResourcePatternType,
	principal string, host string, operation acls.Operation, permission acls.Permission) ([]acls.AclEntry, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	req := ListOrDeleteAclsRequest{
		LeaderVersion:      c.leaderVersion,
		ResourceType:       resourceType,
		ResourceNameFilter: resourceNameFilter,
		PatternTypeFilter:  patternTypeFilter,
		Principal:          principal,
		Host:               host,
		Operation:          operation,
		Permission:         permission,
	}
	buff := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerListAcls, buff)
	if err != nil {
		return nil, err
	}
	var resp ListAclsResponse
	resp.Deserialize(respBuff, 0)
	return resp.AclEntries, nil
}

func (c *client) DeleteAcls(resourceType acls.ResourceType, resourceNameFilter string, patternTypeFilter acls.ResourcePatternType,
	principal string, host string, operation acls.Operation, permission acls.Permission) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	req := ListOrDeleteAclsRequest{
		LeaderVersion:      c.leaderVersion,
		ResourceType:       resourceType,
		ResourceNameFilter: resourceNameFilter,
		PatternTypeFilter:  patternTypeFilter,
		Principal:          principal,
		Host:               host,
		Operation:          operation,
		Permission:         permission,
	}
	buff := req.Serialize(createRequestBuffer())
	_, err = conn.SendRPC(transport.HandlerIDControllerDeleteAcls, buff)
	return err
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
	conn, err = c.createConnection()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *client) getCachedConnection() (transport.Connection, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.closed {
		return nil, errors.New("client has been closed")
	}
	return c.conn, nil
}

func (c *client) createConnection() (transport.Connection, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.conn != nil {
		return c.conn, nil
	}
	conn, err := c.connFactory(c.address)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return c.conn, nil
}

func createRequestBuffer() []byte {
	buff := make([]byte, 0, 128)                  // Initial size guess
	buff = binary.BigEndian.AppendUint16(buff, 1) // rpc version - currently 1
	return buff
}
