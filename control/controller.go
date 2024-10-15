package control

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"sync"
)

/*
Controller lives on each agent and activates / deactivates depending on whether the agent is the cluster leader as defined
by being the first member of the cluster state. At any one time there is only one controller active on the cluster.
Controller handles updates and queries to the LSM, offsets, and topic metadata.
*/
type Controller struct {
	cfg               Conf
	lock              sync.RWMutex
	started           bool
	objStoreClient    objstore.Client
	connectionFactory transport.ConnectionFactory
	transportServer   transport.Server
	lsmHolder         *LsmHolder
	offsetsCache      *offsets.Cache
	topicMetaManager  *topicmeta.Manager
	currentMembership cluster.MembershipState
	tableListeners    *tableListeners
}

func NewController(cfg Conf, objStoreClient objstore.Client, connectionFactory transport.ConnectionFactory,
	transportServer transport.Server) *Controller {
	control := &Controller{
		cfg:               cfg,
		objStoreClient:    objStoreClient,
		connectionFactory: connectionFactory,
		transportServer:   transportServer,
		tableListeners:    newTableListeners(cfg.TableNotificationInterval, connectionFactory),
	}
	return control
}

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
	c.transportServer.RegisterHandler(transport.HandlerIDControllerGetOffsets, c.handleGetOffsets)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerPollForJob, c.handlePollForJob)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerGetTopicInfo, c.handleGetTopicInfo)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerCreateTopic, c.handleCreateTopic)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerDeleteTopic, c.handleDeleteTopic)
	c.tableListeners.start()
	c.started = true
	return nil
}

func (c *Controller) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return nil
	}
	if c.lsmHolder != nil {
		if err := c.lsmHolder.stop(); err != nil {
			return err
		}
	}
	if c.topicMetaManager != nil {
		if err := c.topicMetaManager.Stop(); err != nil {
			return err
		}
	}
	c.tableListeners.stop()
	c.lsmHolder = nil
	c.currentMembership = cluster.MembershipState{}
	c.started = false
	return nil
}

// MembershipChanged is called when membership of the cluster changes. If we are first entry in the state then we will
// host the LSM
func (c *Controller) MembershipChanged(newState cluster.MembershipState) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return errors.New("controller not started")
	}
	if len(newState.Members) > 0 && newState.Members[0].Address == c.transportServer.Address() {
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
	} else {
		if c.lsmHolder != nil {
			if err := c.lsmHolder.Stop(); err != nil {
				return err
			}
			c.lsmHolder = nil
			c.offsetsCache.Stop()
			c.offsetsCache = nil
			if err := c.topicMetaManager.Stop(); err != nil {
				return err
			}
			c.topicMetaManager = nil
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
	return nil
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
	return &client{
		m:              c,
		address:        c.currentMembership.Members[0].Address,
		clusterVersion: c.currentMembership.ClusterVersion,
		connFactory:    c.connectionFactory,
	}, nil
}

func (c *Controller) handleRegisterL0Table(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if err := c.checkStarted(); err != nil {
		return err
	}
	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	var req RegisterL0Request
	req.Deserialize(request, 2)
	if err := c.checkClusterVersion(req.ClusterVersion); err != nil {
		return responseWriter(nil, err)
	}
	regBatch := lsm.RegistrationBatch{
		Registrations: []lsm.RegistrationEntry{req.RegEntry},
	}
	return c.lsmHolder.ApplyLsmChanges(regBatch, func(err error) error {
		if err != nil {
			return responseWriter(nil, err)
		}
		lastReadableInfos, err := c.offsetsCache.UpdateWrittenOffsets(req.OffsetInfos)
		if err != nil {
			return err
		}
		// Note, if no readable offsets updated we will still send back a notification with the table id so it
		// can be cached on the fetcher
		if err := c.tableListeners.sendTableRegisteredNotification(req.RegEntry.TableID, lastReadableInfos); err != nil {
			return err
		}
		// Send back zero byte to represent nil OK response
		responseBuff = append(responseBuff, 0)
		return responseWriter(responseBuff, nil)
	})
}

func (c *Controller) handleApplyChanges(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if err := c.checkStarted(); err != nil {
		return err
	}
	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	var req ApplyChangesRequest
	req.Deserialize(request, 2)
	if err := c.checkClusterVersion(req.ClusterVersion); err != nil {
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
	if err := c.checkStarted(); err != nil {
		return err
	}
	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	var req RegisterTableListenerRequest
	req.Deserialize(request, 2)
	if err := c.checkClusterVersion(req.ClusterVersion); err != nil {
		return responseWriter(nil, err)
	}
	lro, err := c.offsetsCache.GetLastReadableOffset(req.TopicID, req.PartitionID)
	if err != nil {
		return responseWriter(nil, err)
	}
	c.tableListeners.maybeRegisterListenerForPartition(req.Address, req.TopicID, req.PartitionID, req.ResetSequence)
	resp := RegisterTableListenerResponse{
		LastReadableOffset: lro,
	}
	responseBuff = resp.Serialize(responseBuff)
	return responseWriter(responseBuff, nil)
}

func (c *Controller) handleQueryTablesInRange(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if err := c.checkStarted(); err != nil {
		return err
	}
	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	var req QueryTablesInRangeRequest
	req.Deserialize(request, 2)
	if err := c.checkClusterVersion(req.ClusterVersion); err != nil {
		return responseWriter(nil, err)
	}
	res, err := c.lsmHolder.QueryTablesInRange(req.KeyStart, req.KeyEnd)
	if err != nil {
		return responseWriter(nil, err)
	}
	responseBuff = res.Serialize(responseBuff)
	return responseWriter(responseBuff, nil)
}

func (c *Controller) handleGetOffsets(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if err := c.checkStarted(); err != nil {
		return err
	}
	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	var req GetOffsetsRequest
	req.Deserialize(request, 2)
	if err := c.checkClusterVersion(req.ClusterVersion); err != nil {
		return responseWriter(nil, err)
	}
	offs, err := c.offsetsCache.GetOffsets(req.Infos)
	if err != nil {
		return responseWriter(nil, err)
	}
	resp := GetOffsetsResponse{Offsets: offs}
	responseBuff = resp.Serialize(responseBuff)
	return responseWriter(responseBuff, nil)
}

func (c *Controller) handlePollForJob(ctx *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if err := c.checkStarted(); err != nil {
		return err
	}
	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
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
	if err := c.checkStarted(); err != nil {
		return err
	}
	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	var req GetTopicInfoRequest
	req.Deserialize(request, 2)
	if err := c.checkClusterVersion(req.ClusterVersion); err != nil {
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
	if err := c.checkStarted(); err != nil {
		return err
	}
	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	var req CreateTopicRequest
	req.Deserialize(request, 2)
	if err := c.checkClusterVersion(req.ClusterVersion); err != nil {
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
	if err := c.checkStarted(); err != nil {
		return err
	}
	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	var req DeleteTopicRequest
	req.Deserialize(request, 2)
	if err := c.checkClusterVersion(req.ClusterVersion); err != nil {
		return responseWriter(nil, err)
	}
	err := c.topicMetaManager.DeleteTopic(req.TopicName)
	if err != nil {
		return responseWriter(nil, err)
	}
	return responseWriter(responseBuff, nil)
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
		return errors.New("not started")
	}
	return nil
}

func (c *Controller) checkClusterVersion(clusterVersion int) error {
	if clusterVersion != c.currentMembership.ClusterVersion {
		// This will occur when a cluster change occurs and a client created from an older cluster version tries
		// to perform an operation. We return an unavailable error which will cause the caller to close the connection
		// and create a new one, with the correct version
		return common.NewTektiteErrorf(common.Unavailable, "controller - cluster version mismatch")
	}
	return nil
}
