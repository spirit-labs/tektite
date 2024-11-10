package control

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"sync"
	"sync/atomic"
	"time"
)

/*
Controller lives on each agent and activates / deactivates depending on whether the agent is the cluster leader as defined
by being the first member of the cluster state. At any one time there is only one controller active on the cluster.
Controller handles updates and queries to the LSM, offsets, and topic metadata.
*/
type Controller struct {
	cfg                        Conf
	lock                       sync.RWMutex
	started                    bool
	stopping                   atomic.Bool
	objStoreClient             objstore.Client
	connectionFactory          transport.ConnectionFactory
	transportServer            transport.Server
	lsmHolder                  *LsmHolder
	offsetsCache               *offsets.Cache
	topicMetaManager           *topicmeta.Manager
	currentMembership          cluster.MembershipState
	tableListeners             *tableListeners
	groupCoordinatorController *CoordinatorController
	tableGetter                sst.TableGetter
	sequences                  *Sequences
	memberID                   int32
}

func NewController(cfg Conf, objStoreClient objstore.Client, connectionFactory transport.ConnectionFactory,
	transportServer transport.Server) *Controller {
	control := &Controller{
		cfg:                        cfg,
		objStoreClient:             objStoreClient,
		connectionFactory:          connectionFactory,
		transportServer:            transportServer,
		tableListeners:             newTableListeners(cfg.TableNotificationInterval, connectionFactory),
		groupCoordinatorController: NewGroupCoordinatorController(),
		memberID:                   -1,
	}
	return control
}

const (
	objStoreCallTimeout      = 5 * time.Second
	unavailabilityRetryDelay = 1 * time.Second
)

func (c *Controller) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	// Register the handlers
	c.transportServer.RegisterHandler(transport.HandlerIDControllerRegisterL0Table, c.handleRegisterL0Table)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerApplyChanges, c.handleApplyChanges)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerRegisterTableListener, c.handleRegisterTableListener)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerQueryTablesInRange, c.handleQueryTablesInRange)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerGetOffsets, c.handlePrePush)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerPollForJob, c.handlePollForJob)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerGetTopicInfo, c.handleGetTopicInfo)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerCreateTopic, c.handleCreateTopic)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerDeleteTopic, c.handleDeleteTopic)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerGetGroupCoordinatorInfo, c.handleGetGroupCoordinatorInfo)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerGenerateSequence, c.handleGenerateSequenceRequest)
	c.tableListeners.start()
	c.started = true
	return nil
}

func (c *Controller) Stop() error {
	c.stopping.Store(true)
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return nil
	}
	return c.stop()
}

func (c *Controller) SetTableGetter(getter sst.TableGetter) {
	c.tableGetter = getter
}

func (c *Controller) stop() error {
	if c.lsmHolder != nil {
		if err := c.lsmHolder.stop(); err != nil {
			return err
		}
		c.lsmHolder = nil
	}
	if c.topicMetaManager != nil {
		if err := c.topicMetaManager.Stop(); err != nil {
			return err
		}
		c.topicMetaManager = nil
	}
	if c.offsetsCache != nil {
		c.offsetsCache.Stop()
		c.offsetsCache = nil
	}
	c.tableListeners.stop()
	if c.sequences != nil {
		c.sequences.Stop()
		c.sequences = nil
	}
	c.currentMembership = cluster.MembershipState{}
	c.started = false
	return nil
}

func (c *Controller) GetGroupCoordinatorController() *CoordinatorController {
	return c.groupCoordinatorController
}

// MembershipChanged is called when membership of the cluster changes. If we are first entry in the state then we will
// host the LSM
func (c *Controller) MembershipChanged(thisMemberID int32, newState cluster.MembershipState) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return errors.New("controller not started")
	}
	if len(newState.Members) > 0 && newState.Members[0].ID == thisMemberID {
		// This controller is activating as leader
		if c.lsmHolder == nil {
			lsmHolder := NewLsmHolder(c.cfg.ControllerStateUpdaterBucketName, c.cfg.ControllerStateUpdaterKeyPrefix,
				c.cfg.ControllerMetaDataBucketName, c.cfg.ControllerMetaDataKeyPrefix, c.objStoreClient, c.cfg.LsmConf)
			if err := lsmHolder.Start(); err != nil {
				return err
			}
			c.lsmHolder = lsmHolder
			topicMetaManager, err := topicmeta.NewManager(lsmHolder, c.objStoreClient, c.cfg.SSTableBucketName, c.cfg.DataFormat,
				c.connectionFactory)
			if err != nil {
				return err
			}
			if err := topicMetaManager.Start(); err != nil {
				return err
			}
			c.topicMetaManager = topicMetaManager
			cache, err := offsets.NewOffsetsCache(topicMetaManager, lsmHolder, c.objStoreClient, c.cfg.SSTableBucketName)
			if err != nil {
				return err
			}
			if err := cache.Start(); err != nil {
				return err
			}
			c.offsetsCache = cache
			c.sequences = NewSequences(lsmHolder, c.tableGetter, c.objStoreClient, c.cfg.SSTableBucketName,
				c.cfg.DataFormat, int64(c.cfg.SequencesBlockSize))
		}
	} else {
		// This controller is not leader
		if c.lsmHolder != nil {
			// It was leader before - this could happen if this agent was evicted but is still running as a zombie
			// In which case we need to stop
			if err := c.stop(); err != nil {
				return err
			}
		}
	}
	c.currentMembership = newState
	if c.offsetsCache != nil {
		c.offsetsCache.MembershipChanged()
	}
	if c.topicMetaManager != nil {
		c.topicMetaManager.MembershipChanged(newState)
	}
	c.tableListeners.membershipChanged(&newState)
	c.groupCoordinatorController.MembershipChanged(&newState)
	if c.memberID == -1 {
		c.memberID = thisMemberID
	} else if c.memberID != thisMemberID {
		panic(fmt.Sprintf("memberID changed was %d this %d", c.memberID, thisMemberID))
	}
	return nil
}

func (c *Controller) MemberID() int32 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.memberID
}

func (c *Controller) Client() (Client, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.currentMembership.ClusterVersion == 0 {
		return nil, common.NewTektiteErrorf(common.Unavailable, "controller has not received cluster membership")
	}
	if len(c.currentMembership.Members) == 0 {
		return nil, common.NewTektiteErrorf(common.Unavailable, "no members in cluster")
	}
	var leaderMembershipData common.MembershipData
	leaderMembershipData.Deserialize(c.currentMembership.Members[0].Data, 0)
	return &client{
		m:             c,
		address:       leaderMembershipData.ClusterListenAddress,
		leaderVersion: c.currentMembership.LeaderVersion,
		connFactory:   c.connectionFactory,
	}, nil
}

func (c *Controller) handleRegisterL0Table(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.requestChecks(request, responseWriter) {
		return nil
	}
	var req RegisterL0Request
	req.Deserialize(request, 2)
	if err := c.checkLeaderVersion(req.LeaderVersion); err != nil {
		return responseWriter(nil, err)
	}
	regBatch := lsm.RegistrationBatch{
		Registrations: []lsm.RegistrationEntry{req.RegEntry},
	}
	return c.lsmHolder.ApplyLsmChanges(regBatch, func(err error) error {
		if err != nil {
			return responseWriter(nil, err)
		}
		offsetInfos, tableIDs, err := c.offsetsCache.MaybeReleaseOffsets(req.Sequence, req.RegEntry.TableID)
		if err != nil {
			return err
		}
		if len(tableIDs) > 0 {
			if err := c.tableListeners.sendTableRegisteredNotification(tableIDs, offsetInfos); err != nil {
				return err
			}
		}
		// Send back zero byte to represent nil OK response
		responseBuff = append(responseBuff, 0)
		return responseWriter(responseBuff, nil)
	})
}

func (c *Controller) handleApplyChanges(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.requestChecks(request, responseWriter) {
		return nil
	}
	var req ApplyChangesRequest
	req.Deserialize(request, 2)
	if err := c.checkLeaderVersion(req.LeaderVersion); err != nil {
		return responseWriter(nil, err)
	}
	return c.lsmHolder.ApplyLsmChanges(req.RegBatch, func(err error) error {
		if err != nil {
			return responseWriter(nil, err)
		}
		// Send back zero byte to represent nil OK response
		responseBuff = append(responseBuff, 0)
		return responseWriter(responseBuff, nil)
	})
}

func (c *Controller) handleRegisterTableListener(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.requestChecks(request, responseWriter) {
		return nil
	}
	var req RegisterTableListenerRequest
	req.Deserialize(request, 2)
	if err := c.checkLeaderVersion(req.LeaderVersion); err != nil {
		return responseWriter(nil, err)
	}
	lro, err := c.offsetsCache.GetLastReadableOffset(req.TopicID, req.PartitionID)
	if err != nil {
		return responseWriter(nil, err)
	}
	var memberAddress string
	for _, member := range c.currentMembership.Members {
		if member.ID == req.MemberID {
			var data common.MembershipData
			data.Deserialize(member.Data, 0)
			memberAddress = data.ClusterListenAddress
		}
	}
	if memberAddress == "" {
		return common.NewTektiteErrorf(common.Unavailable, "unable to register table listener - unknown cluster member %d", req.MemberID)
	}
	c.tableListeners.maybeRegisterListenerForPartition(req.MemberID, memberAddress, req.TopicID, req.PartitionID, req.ResetSequence)
	resp := RegisterTableListenerResponse{
		LastReadableOffset: lro,
	}
	responseBuff = resp.Serialize(responseBuff)
	return responseWriter(responseBuff, nil)
}

func (c *Controller) handleQueryTablesInRange(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.requestChecks(request, responseWriter) {
		return nil
	}
	var req QueryTablesInRangeRequest
	req.Deserialize(request, 2)
	if err := c.checkLeaderVersion(req.LeaderVersion); err != nil {
		return responseWriter(nil, err)
	}
	res, err := c.lsmHolder.QueryTablesInRange(req.KeyStart, req.KeyEnd)
	if err != nil {
		return responseWriter(nil, err)
	}
	responseBuff = res.Serialize(responseBuff)
	return responseWriter(responseBuff, nil)
}

func (c *Controller) handlePrePush(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.requestChecks(request, responseWriter) {
		return nil
	}
	var req PrePushRequest
	req.Deserialize(request, 2)
	if err := c.checkLeaderVersion(req.LeaderVersion); err != nil {
		return responseWriter(nil, err)
	}

	var epochsOK []bool
	if len(req.EpochInfos) > 0 {
		// First we check whether the group epochs are valid
		epochsOK = c.groupCoordinatorController.CheckGroupEpochs(req.EpochInfos)
	}

	// And then we get the offsets
	offs, seq, err := c.offsetsCache.GetOffsets(req.Infos)
	if err != nil {
		return responseWriter(nil, err)
	}
	resp := PrePushResponse{
		Offsets:  offs,
		Sequence: seq,
		EpochsOK: epochsOK,
	}
	responseBuff = resp.Serialize(responseBuff)
	return responseWriter(responseBuff, nil)
}

func (c *Controller) handlePollForJob(ctx *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.requestChecks(request, responseWriter) {
		return nil
	}
	c.lsmHolder.lsmManager.PollForJob(ctx.ConnectionID, func(job *lsm.CompactionJob, err error) {
		if err != nil {
			if err := responseWriter(nil, err); err != nil {
				log.Errorf("failed to write error response %v", err)
			}
			return
		}
		buff := job.Serialize(nil)
		responseBuff = append(responseBuff, buff...)
		if err := responseWriter(responseBuff, nil); err != nil {
			log.Errorf("failed to write response %v", err)
		}
	})
	return nil
}

func (c *Controller) handleGetTopicInfo(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.requestChecks(request, responseWriter) {
		return nil
	}
	var req GetTopicInfoRequest
	req.Deserialize(request, 2)
	if err := c.checkLeaderVersion(req.LeaderVersion); err != nil {
		return responseWriter(nil, err)
	}
	info, seq, err := c.topicMetaManager.GetTopicInfo(req.TopicName)
	if err != nil {
		return responseWriter(nil, err)
	}
	var resp GetTopicInfoResponse
	resp.Sequence = seq
	resp.Info = info
	responseBuff = resp.Serialize(responseBuff)
	return responseWriter(responseBuff, nil)
}

func (c *Controller) handleCreateTopic(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.requestChecks(request, responseWriter) {
		return nil
	}
	var req CreateTopicRequest
	req.Deserialize(request, 2)
	if err := c.checkLeaderVersion(req.LeaderVersion); err != nil {
		return responseWriter(nil, err)
	}
	err := c.topicMetaManager.CreateTopic(req.Info)
	if err != nil {
		return responseWriter(nil, err)
	}
	return responseWriter(responseBuff, nil)
}

func (c *Controller) handleDeleteTopic(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.requestChecks(request, responseWriter) {
		return nil
	}
	var req DeleteTopicRequest
	req.Deserialize(request, 2)
	if err := c.checkLeaderVersion(req.LeaderVersion); err != nil {
		return responseWriter(nil, err)
	}
	err := c.topicMetaManager.DeleteTopic(req.TopicName)
	if err != nil {
		return responseWriter(nil, err)
	}
	return responseWriter(responseBuff, nil)
}

func (c *Controller) handleGetGroupCoordinatorInfo(_ *transport.ConnectionContext, request []byte, responseBuff []byte,
	responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.requestChecks(request, responseWriter) {
		return nil
	}
	var req GetGroupCoordinatorInfoRequest
	req.Deserialize(request, 2)
	if err := c.checkLeaderVersion(req.LeaderVersion); err != nil {
		return responseWriter(nil, err)
	}
	address, groupEpoch, err := c.groupCoordinatorController.GetGroupCoordinatorInfo(req.GroupID)
	if err != nil {
		return responseWriter(nil, err)
	}
	resp := GetGroupCoordinatorInfoResponse{
		Address:    address,
		GroupEpoch: groupEpoch,
	}
	responseBuff = resp.Serialize(responseBuff)
	return responseWriter(responseBuff, nil)
}

func (c *Controller) handleGenerateSequenceRequest(_ *transport.ConnectionContext, request []byte, responseBuff []byte,
	responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.requestChecks(request, responseWriter) {
		return nil
	}
	var req GenerateSequenceRequest
	req.Deserialize(request, 2)
	if err := c.checkLeaderVersion(req.LeaderVersion); err != nil {
		return responseWriter(nil, err)
	}
	seq, err := c.sequences.GenerateSequence(req.SequenceName)
	if err != nil {
		return responseWriter(nil, err)
	}
	var resp GenerateSequenceResponse
	resp.Sequence = seq
	responseBuff = resp.Serialize(responseBuff)
	return responseWriter(responseBuff, nil)
}

func (c *Controller) requestChecks(request []byte, responseWriter transport.ResponseWriter) bool {
	var err error
	err = c.checkStarted()
	if err == nil {
		err = c.checkLeader()
		if err == nil {
			err = checkRPCVersion(request)
			if err == nil {
				return true
			}
		}
	}
	log.Errorf("cannot handle controller request: %v", err)
	if err := responseWriter(nil, err); err != nil {
		log.Errorf("failed to write error response %v", err)
	}
	return false
}

func checkRPCVersion(request []byte) error {
	rpcVersion := binary.BigEndian.Uint16(request)
	if rpcVersion != 1 {
		// Currently just 1
		return errors.New("invalid rpc version")
	}
	return nil
}

func (c *Controller) checkStarted() error {
	if !c.started {
		return common.NewTektiteErrorf(common.Unavailable, "controller is not started")
	}
	return nil
}

func (c *Controller) checkLeader() error {
	if c.lsmHolder == nil {
		return common.NewTektiteErrorf(common.Unavailable, "controller is not leader")
	}
	return nil
}

func (c *Controller) checkLeaderVersion(clusterVersion int) error {
	if clusterVersion != c.currentMembership.LeaderVersion {
		// This will occur when a cluster change occurs and a client created from an older leader version tries
		// to perform an operation.
		// We return an unavailable error which will cause the caller to close the connection
		// and create a new one, with the correct version
		return common.NewTektiteErrorf(common.Unavailable, "controller - leader version mismatch")
	}
	return nil
}
