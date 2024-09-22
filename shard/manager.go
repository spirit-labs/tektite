package shard

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/transport"
	"sync"
)

/*
Manager manages instances of LSM shards on an agent. It responds to changes in the membership of the cluster and as
members are added or removed it calculates whether it needs to start or stop shard instances on this node.
*/
type Manager struct {
	lock                   sync.RWMutex
	started                bool
	numShards              int
	stateUpdatorBucketName string
	stateUpdatorKeyPrefix  string
	dataBucketName         string
	dataKeyPrefix          string
	objStoreClient         objstore.Client
	connectionFactory      transport.ConnectionFactory
	transportServer        transport.Server
	topicProvider topicInfoProvider
	offsetLoader partitionOffsetLoader
	lsmOpts                lsm.ManagerOpts
	shards                 map[int]*ShardHolder
	currentMembership      cluster.MembershipState
}

type ShardHolder struct {
	LsmShard     *LsmShard
	OffsetsCache *OffsetsCache
}

func (s *ShardHolder) stop() error {
	return s.LsmShard.Stop()
}

func NewManager(numShards int, stateUpdatorBucketName string, stateUpdatorKeyPrefix string, dataBucketName string,
	dataKeyPrefix string, objStoreClient objstore.Client, connectionFactory transport.ConnectionFactory,
	transportServer transport.Server, topicProvider topicInfoProvider, offsetLoader partitionOffsetLoader, lsmOpts lsm.ManagerOpts) *Manager {
	return &Manager{
		numShards:              numShards,
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
		shards:                 map[int]*ShardHolder{},
	}
}

func (sm *Manager) Start() error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if sm.started {
		return nil
	}
	// Register the handlers
	sm.transportServer.RegisterHandler(transport.HandlerIDShardApplyChanges, sm.handleApplyChanges)
	sm.transportServer.RegisterHandler(transport.HandlerIDShardQueryTablesInRange, sm.handleQueryTablesInRange)
	sm.transportServer.RegisterHandler(transport.HandlerIDShardGetOffsets, sm.handleGetOffsets)
	sm.started = true
	return nil
}

func (sm *Manager) Stop() error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if !sm.started {
		return nil
	}
	for _, shard := range sm.shards {
		if err := shard.stop(); err != nil {
			return err
		}
	}
	sm.shards = map[int]*ShardHolder{}
	sm.currentMembership = cluster.MembershipState{}
	sm.started = false
	return nil
}

// MembershipChanged is called when membership of the cluster changes. The manager will now create or stop LSM shards
// as appropriate
func (sm *Manager) MembershipChanged(newState cluster.MembershipState) error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if !sm.started {
		return errors.New("manager not started")
	}
	sm.currentMembership = newState
	address := sm.transportServer.Address()
	newShards := make(map[int]*ShardHolder, len(sm.shards))
	for shardID := 0; shardID < sm.numShards; shardID++ {
		shardAddress, ok := sm.addressForShard(shardID)
		if ok && (shardAddress == address) {
			// The controller lives on this node
			shard, ok := sm.shards[shardID]
			if ok {
				// If we already have it, then keep it
				newShards[shardID] = shard
			} else {
				// Create and start a controller
				// Note, each shard needs a unique prefix for updator and data keys
				updatorPrefix := fmt.Sprintf("%s-%06d", sm.stateUpdatorKeyPrefix, shardID)
				dataKeyPrefix := fmt.Sprintf("%s-%06d", sm.dataKeyPrefix, shardID)
				lsmShard := NewLsmShard(sm.stateUpdatorBucketName, updatorPrefix, sm.dataBucketName,
					dataKeyPrefix, sm.objStoreClient, sm.lsmOpts)
				if err := lsmShard.Start(); err != nil {
					return err
				}
				offsets := NewOffsetsCache(shardID, sm.topicProvider, sm.offsetLoader)
				if err := offsets.Start(); err != nil {
					return err
				}
				sh := &ShardHolder{
					LsmShard:     lsmShard,
					OffsetsCache: offsets,
				}
				newShards[shardID] = sh
			}
		} else {
			shard, ok := sm.shards[shardID]
			if ok {
				// The shard was on this node but has moved, so we need to close the one here
				if err := shard.stop(); err != nil {
					return err
				}
			}
		}
	}
	sm.shards = newShards
	return nil
}

func (sm *Manager) AddressForShard(shardID int) (string, bool) {
	sm.lock.RLock()
	defer sm.lock.RUnlock()
	return sm.addressForShard(shardID)
}

func (sm *Manager) Client(shardID int) (Client, error) {
	if sm.currentMembership.ClusterVersion == 0 {
		return nil, common.NewTektiteErrorf(common.Unavailable, "manager has not received cluster membership")
	}
	return &client{
		m:              sm,
		shardID:        shardID,
		clusterVersion: sm.currentMembership.ClusterVersion,
		connFactory:    sm.connectionFactory,
	}, nil
}

func (sm *Manager) GetShard(shardID int) (*ShardHolder, bool) {
	sm.lock.RLock()
	defer sm.lock.RUnlock()
	return sm.getShard(shardID)
}

func (sm *Manager) getShard(shardID int) (*ShardHolder, bool) {
	controller, ok := sm.shards[shardID]
	return controller, ok
}

func (sm *Manager) addressForShard(shardID int) (string, bool) {
	lms := len(sm.currentMembership.Members)
	if lms == 0 {
		return "", false
	}
	index := shardID % len(sm.currentMembership.Members)
	return sm.currentMembership.Members[index].Address, true
}

func (sm *Manager) handleApplyChanges(request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
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
	shard, err := sm.getShardWithError(req.ShardID)
	if err != nil {
		return responseWriter(nil, err)
	}
	return shard.LsmShard.ApplyLsmChanges(req.RegBatch, func(err error) error {
		if err != nil {
			return responseWriter(nil, err)
		}
		// Send back zero byte to represent nil OK response
		responseBuff = append(responseBuff, 0)
		return responseWriter(responseBuff, nil)
	})
}

func (sm *Manager) handleQueryTablesInRange(request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
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
	shard, err := sm.getShardWithError(req.ShardID)
	if err != nil {
		return responseWriter(nil, err)
	}
	res, err := shard.LsmShard.QueryTablesInRange(req.KeyStart, req.KeyEnd)
	if err != nil {
		return responseWriter(nil, err)
	}
	responseBuff = res.Serialize(responseBuff)
	return responseWriter(responseBuff, nil)
}


func (sm *Manager) handleGetOffsets(request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
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
	shard, err := sm.getShardWithError(req.ShardID)
	if err != nil {
		return responseWriter(nil, err)
	}
	offsets, err := shard.OffsetsCache.GetOffsets(req.Infos)
	if err != nil {
		return responseWriter(nil, err)
	}
	resp := GetOffsetsResponse{Offsets: offsets}
	responseBuff = resp.Serialize(responseBuff)
	return responseWriter(responseBuff, nil)
}

func (sm *Manager) getShardWithError(shardID int) (*ShardHolder, error) {
	shard, ok := sm.getShard(shardID)
	if !ok {
		// Shard not found - most likely membership changed. We send back an error and the client will retry
		return nil, common.NewTektiteErrorf(common.Unavailable, "shard %d not found", shardID)
	}
	return shard, nil
}

func checkRPCVersion(request []byte) error {
	rpcVersion := binary.BigEndian.Uint16(request)
	if rpcVersion != 1 {
		// Currently just 1
		return errors.New("invalid rpc version")
	}
	return nil
}

func (sm *Manager) checkClusterVersion(clusterVersion int) error {
	if clusterVersion != sm.currentMembership.ClusterVersion {
		// This will occur when a cluster change occurs and a client created from an older cluster version tries
		// to perform an operation. We return an unavailable error which will cause the caller to close the connection
		// and create a new one, with the correct version
		return common.NewTektiteErrorf(common.Unavailable, "shard manager - cluster version mismatch")
	}
	return nil
}

func (sm *Manager) getShards() map[int]*ShardHolder {
	sm.lock.RLock()
	defer sm.lock.RUnlock()
	shards := make(map[int]*ShardHolder, len(sm.shards))
	for shardID, shard := range sm.shards {
		shards[shardID] = shard
	}
	return shards
}
