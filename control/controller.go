package control

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/streammeta"
	"github.com/spirit-labs/tektite/transport"
	"sync"
)

/*
Controller lives on each agent and activates / deactivates depending on whether the agent is the cluster leader as defined
by being the first member of the cluster state.
*/
type Controller struct {
	cfg               Conf
	lock              sync.RWMutex
	started           bool
	objStoreClient    objstore.Client
	connectionFactory transport.ConnectionFactory
	transportServer   transport.Server
	topicProvider     streammeta.TopicInfoProvider
	loaderFactory     offsetLoaderFactory
	lsmHolder         *LsmHolder
	offsetsCache      *offsets.Cache
	offsetsLoader offsets.PartitionOffsetLoader
	currentMembership cluster.MembershipState
}

type offsetLoaderFactory func() (offsets.PartitionOffsetLoader, error)

func NewController(cfg Conf, objStoreClient objstore.Client, connectionFactory transport.ConnectionFactory,
	transportServer transport.Server, topicProvider streammeta.TopicInfoProvider, loaderFactory offsetLoaderFactory) *Controller {
	control := &Controller{
		cfg:               cfg,
		objStoreClient:    objStoreClient,
		connectionFactory: connectionFactory,
		transportServer:   transportServer,
		topicProvider:     topicProvider,
		loaderFactory:     loaderFactory,
	}
	if loaderFactory == nil {
		loaderFactory = control.createOffsetsLoader
	}
	control.loaderFactory = loaderFactory
	return control
}

func (c *Controller) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	// Register the handlers
	c.transportServer.RegisterHandler(transport.HandlerIDControllerRegisterL0Table, c.handleRegisterL0Request)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerApplyChanges, c.handleApplyChanges)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerQueryTablesInRange, c.handleQueryTablesInRange)
	c.transportServer.RegisterHandler(transport.HandlerIDControllerGetOffsets, c.handleGetOffsets)
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
	if c.offsetsLoader != nil {
		c.offsetsLoader.Stop()
	}
	c.lsmHolder = nil
	c.currentMembership = cluster.MembershipState{}
	c.started = false
	return nil
}

func (c *Controller) createOffsetsLoader() (offsets.PartitionOffsetLoader, error) {
	return NewOffsetsLoader(c.lsmHolder, c.objStoreClient, c.cfg.SSTableBucketName)
}

// MembershipChanged is called when membership of the cluster changes. If we are first entry in the state then we will
// host the LSM
func (c *Controller) MembershipChanged(newState cluster.MembershipState) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return errors.New("manager not started")
	}
	if len(newState.Members) > 0 && newState.Members[0].Address == c.transportServer.Address() {
		lsmHolder := NewLsmHolder(c.cfg.ControllerStateUpdaterBucketName, c.cfg.ControllerStateUpdaterKeyPrefix,
			c.cfg.ControllerMetaDataBucketName, c.cfg.ControllerMetaDataKeyPrefix, c.objStoreClient, c.cfg.LsmConf)
		if err := lsmHolder.Start(); err != nil {
			return err
		}
		c.lsmHolder = lsmHolder
		loader, err := c.loaderFactory()
		if err != nil {
			return err
		}
		c.offsetsLoader = loader
		cache := offsets.NewOffsetsCache(c.topicProvider, loader)
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
			c.offsetsCache = nil
		}
	}
	c.currentMembership = newState
	c.offsetsCache.MembershipChanged()
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

func (c *Controller) handleRegisterL0Request(request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	var req RegisterL0Request
	req.Deserialize(request, 2)
	if err := c.checkClusterVersion(req.ClusterVersion); err != nil {
		return responseWriter(nil, err)
	}
	if err := c.offsetsCache.UpdateWrittenOffsets(req.OffsetInfos); err != nil {
		return err
	}
	regBatch := lsm.RegistrationBatch{
		Registrations: []lsm.RegistrationEntry{req.RegEntry},
	}
	return c.lsmHolder.ApplyLsmChanges(regBatch, func(err error) error {
		if err != nil {
			return responseWriter(nil, err)
		}
		// Send back zero byte to represent nil OK response
		responseBuff = append(responseBuff, 0)
		return responseWriter(responseBuff, nil)
	})
}

func (c *Controller) handleApplyChanges(request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.Lock()
	defer c.lock.Unlock()
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

func (c *Controller) handleQueryTablesInRange(request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
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

func (c *Controller) handleGetOffsets(request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
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

func checkRPCVersion(request []byte) error {
	rpcVersion := binary.BigEndian.Uint16(request)
	if rpcVersion != 1 {
		// Currently just 1
		return errors.New("invalid rpc version")
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
