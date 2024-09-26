package control

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/transport"
	"sync"
)

/*
Controller lives on each agent and activates / deactivates depending on whether the agent is the cluster leader as defined
by being the first member of the cluster state.
*/
type Controller struct {
	lock                   sync.RWMutex
	started                bool
	stateUpdatorBucketName string
	stateUpdatorKeyPrefix  string
	dataBucketName         string
	dataKeyPrefix          string
	objStoreClient         objstore.Client
	connectionFactory      transport.ConnectionFactory
	transportServer        transport.Server
	topicProvider          offsets.TopicInfoProvider
	offsetLoader           offsets.PartitionOffsetLoader
	lsmOpts                lsm.ManagerOpts
	lsmHolder              *LsmHolder
	offsetsCache           *offsets.Cache
	currentMembership      cluster.MembershipState
}

func NewManager(stateUpdatorBucketName string, stateUpdatorKeyPrefix string, dataBucketName string,
	dataKeyPrefix string, objStoreClient objstore.Client, connectionFactory transport.ConnectionFactory,
	transportServer transport.Server, topicProvider offsets.TopicInfoProvider, offsetLoader offsets.PartitionOffsetLoader, lsmOpts lsm.ManagerOpts) *Controller {
	return &Controller{
		stateUpdatorBucketName: stateUpdatorBucketName,
		stateUpdatorKeyPrefix:  stateUpdatorKeyPrefix,
		dataBucketName:         dataBucketName,
		dataKeyPrefix:          dataKeyPrefix,
		objStoreClient:         objStoreClient,
		connectionFactory:      connectionFactory,
		transportServer:        transportServer,
		topicProvider:          topicProvider,
		offsetLoader:           offsetLoader,
		lsmOpts:                lsmOpts,
	}
}

func (sm *Controller) Start() error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if sm.started {
		return nil
	}
	// Register the handlers
	sm.transportServer.RegisterHandler(transport.HandlerIDControllerRegisterL0Table, sm.handleRegisterL0Request)
	sm.transportServer.RegisterHandler(transport.HandlerIDControllerApplyChanges, sm.handleApplyChanges)
	sm.transportServer.RegisterHandler(transport.HandlerIDControllerQueryTablesInRange, sm.handleQueryTablesInRange)
	sm.transportServer.RegisterHandler(transport.HandlerIDControllerGetOffsets, sm.handleGetOffsets)
	sm.started = true
	return nil
}

func (sm *Controller) Stop() error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if !sm.started {
		return nil
	}
	if sm.lsmHolder != nil {
		if err := sm.lsmHolder.stop(); err != nil {
			return err
		}
	}
	sm.lsmHolder = nil
	sm.currentMembership = cluster.MembershipState{}
	sm.started = false
	return nil
}

// MembershipChanged is called when membership of the cluster changes. If we are first entry in the state then we will
// host the LSM
func (sm *Controller) MembershipChanged(newState cluster.MembershipState) error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if !sm.started {
		return errors.New("manager not started")
	}
	if len(newState.Members) > 0 && newState.Members[0].Address == sm.transportServer.Address() {
		lsmHolder := NewLsmHolder(sm.stateUpdatorBucketName, sm.stateUpdatorKeyPrefix, sm.dataBucketName,
			sm.dataKeyPrefix, sm.objStoreClient, sm.lsmOpts)
		if err := lsmHolder.Start(); err != nil {
			return err
		}
		cache := offsets.NewOffsetsCache(sm.topicProvider, sm.offsetLoader)
		if err := cache.Start(); err != nil {
			return err
		}
		sm.lsmHolder = lsmHolder
		sm.offsetsCache = cache
	} else {
		if sm.lsmHolder != nil {
			if err := sm.lsmHolder.Stop(); err != nil {
				return err
			}
			sm.lsmHolder = nil
			sm.offsetsCache = nil
		}
	}
	sm.currentMembership = newState
	sm.offsetsCache.MembershipChanged()
	return nil
}

func (sm *Controller) Client() (Client, error) {
	if sm.currentMembership.ClusterVersion == 0 {
		return nil, common.NewTektiteErrorf(common.Unavailable, "manager has not received cluster membership")
	}
	if len(sm.currentMembership.Members) == 0 {
		return nil, common.NewTektiteErrorf(common.Unavailable, "no members in cluster")
	}
	return &client{
		m:              sm,
		address:        sm.currentMembership.Members[0].Address,
		clusterVersion: sm.currentMembership.ClusterVersion,
		connFactory:    sm.connectionFactory,
	}, nil
}

func (sm *Controller) handleRegisterL0Request(request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	var req RegisterL0Request
	req.Deserialize(request, 2)
	if err := sm.checkClusterVersion(req.ClusterVersion); err != nil {
		return responseWriter(nil, err)
	}
	if err := sm.offsetsCache.UpdateWrittenOffsets(req.OffsetInfos); err != nil {
		return err
	}
	regBatch := lsm.RegistrationBatch{
		Registrations: []lsm.RegistrationEntry{req.RegEntry},
	}
	return sm.lsmHolder.ApplyLsmChanges(regBatch, func(err error) error {
		if err != nil {
			return responseWriter(nil, err)
		}
		// Send back zero byte to represent nil OK response
		responseBuff = append(responseBuff, 0)
		return responseWriter(responseBuff, nil)
	})
}

func (sm *Controller) handleApplyChanges(request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	var req ApplyChangesRequest
	req.Deserialize(request, 2)
	if err := sm.checkClusterVersion(req.ClusterVersion); err != nil {
		return responseWriter(nil, err)
	}
	return sm.lsmHolder.ApplyLsmChanges(req.RegBatch, func(err error) error {
		if err != nil {
			return responseWriter(nil, err)
		}
		// Send back zero byte to represent nil OK response
		responseBuff = append(responseBuff, 0)
		return responseWriter(responseBuff, nil)
	})
}

func (sm *Controller) handleQueryTablesInRange(request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	sm.lock.RLock()
	defer sm.lock.RUnlock()
	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	var req QueryTablesInRangeRequest
	req.Deserialize(request, 2)
	if err := sm.checkClusterVersion(req.ClusterVersion); err != nil {
		return responseWriter(nil, err)
	}
	res, err := sm.lsmHolder.QueryTablesInRange(req.KeyStart, req.KeyEnd)
	if err != nil {
		return responseWriter(nil, err)
	}
	responseBuff = res.Serialize(responseBuff)
	return responseWriter(responseBuff, nil)
}

func (sm *Controller) handleGetOffsets(request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	sm.lock.RLock()
	defer sm.lock.RUnlock()
	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	var req GetOffsetsRequest
	req.Deserialize(request, 2)
	if err := sm.checkClusterVersion(req.ClusterVersion); err != nil {
		return responseWriter(nil, err)
	}
	offs, err := sm.offsetsCache.GetOffsets(req.Infos)
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

func (sm *Controller) checkClusterVersion(clusterVersion int) error {
	if clusterVersion != sm.currentMembership.ClusterVersion {
		// This will occur when a cluster change occurs and a client created from an older cluster version tries
		// to perform an operation. We return an unavailable error which will cause the caller to close the connection
		// and create a new one, with the correct version
		return common.NewTektiteErrorf(common.Unavailable, "controller - cluster version mismatch")
	}
	return nil
}
