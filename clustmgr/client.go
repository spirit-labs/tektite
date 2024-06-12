package clustmgr

import (
	"context"
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
	"sync"
	"sync/atomic"

	"strconv"
	"strings"
	"time"
)

type Client interface {
	Start() error
	Stop(halt bool) error
	MarkGroupAsValid(nodeID int, groupID int, joinedVersion int) (bool, error)
	GetValidGroups() (map[string]int, error)
	GetLock(lockName string, timeout time.Duration) (bool, error)
	ReleaseLock(lockName string) error
	GetClusterState() (*ClusterState, map[int]int64, int64, int64, error)
	SetClusterState(clusterState *ClusterState, activeNodes map[int]int64, previousVersion int64, maxRevision int64) (bool, error)
	GetNodeRevision() int64
	PrepareForShutdown()
	GetMutex(name string) (*Mutex, error)
}

type Mutex struct {
	session *concurrency.Session
	mut     *concurrency.Mutex
}

func (m *Mutex) Close() error {
	if err := m.mut.Unlock(context.Background()); err != nil {
		return convertEtcdError(err)
	}
	return convertEtcdError(m.session.Close())
}

func NewClient(keyPrefix string, clusterName string, nodeID int, endpoints []string, leaseTime time.Duration,
	sendUpdatePeriod time.Duration, callTimeout time.Duration, nodeChangeHandler func(nodes map[int]int64, maxRevision int64), clusterStateHandler func(cs ClusterState),
	logScope string) Client {
	if clusterName == conf.DefaultClusterName {
		panic("using default cluster name")
	}
	fullPrefix := fmt.Sprintf("%s%s/", keyPrefix, clusterName)
	nodesKey := fmt.Sprintf("%s%s", fullPrefix, "nodes/")
	thisNodeKey := fmt.Sprintf("%s%d", nodesKey, nodeID)
	clusterStateKey := fmt.Sprintf("%s%s", fullPrefix, "cluster_state")
	locksKey := fmt.Sprintf("%s%s", fullPrefix, "locks/")
	validGroupsKey := fmt.Sprintf("%s%s", fullPrefix, "valid_groups/")
	mutexesKey := fmt.Sprintf("%s%s", fullPrefix, "mutexes/")
	c := &client{
		nodesKey:            nodesKey,
		thisNodeKey:         thisNodeKey,
		clusterStateKey:     clusterStateKey,
		locksKey:            locksKey,
		validGroupsKey:      validGroupsKey,
		mutexesKey:          mutexesKey,
		clusterName:         clusterName,
		nodeID:              nodeID,
		endpoints:           endpoints,
		leaseTime:           leaseTime,
		callTimeout:         callTimeout,
		sendUpdatePeriod:    sendUpdatePeriod,
		nodesChangeHandler:  nodeChangeHandler,
		clusterStateHandler: clusterStateHandler,
		activeNodes:         map[int]int64{},
		notAvailableMsg:     fmt.Sprintf("etcd not available on %v. will retry", endpoints),
		logScope:            logScope,
		clusterHead:         fullPrefix,
	}
	c.stopWG.Add(5)
	return c
}

type client struct {
	started                    bool
	nodesKey                   string
	thisNodeKey                string
	clusterStateKey            string
	locksKey                   string
	validGroupsKey             string
	mutexesKey                 string
	endpoints                  []string
	clusterName                string
	nodeID                     int
	leaseTime                  time.Duration
	callTimeout                time.Duration
	activeNodes                map[int]int64
	nodesChangeHandler         func(map[int]int64, int64)
	clusterStateHandler        func(cs ClusterState)
	cli                        *clientv3.Client
	leaseID                    clientv3.LeaseID
	lock                       sync.Mutex
	sendUpdatePeriod           time.Duration
	clientCtx                  context.Context
	cancelFunc                 context.CancelFunc
	stopped                    atomic.Bool
	requiresNodesUpdate        bool
	maxNodesRevision           int64
	requiresClusterStateUpdate bool
	currentClusterStateInfo    clusterStateInfo
	stopWG                     sync.WaitGroup
	thisNodeRev                int64
	locksLock                  sync.Mutex
	shuttingDown               bool
	notAvailableMsg            string
	logScope                   string
	clusterHead                string
}

func (c *client) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		panic("already started")
	}
	if c.isStopped() {
		panic("cannot be restarted")
	}

	log.Debugf("%s: node %d etcd client starting", c.logScope, c.nodeID)

	// The etcd client noisily logs stuff, we suppress this
	etcdLogger := log.CreateLogger(zap.ErrorLevel, "console")
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   c.endpoints,
		DialTimeout: 5 * time.Second,
		Logger:      etcdLogger,
	})
	if err != nil {
		return err
	}
	c.cli = cli

	c.clientCtx, c.cancelFunc = context.WithCancel(context.Background())

	// Create a lease
	//goland:noinspection ALL
	leaseTimeSeconds := c.leaseTime / time.Second
	log.Debugf("creating lease with ttl %d seconds", leaseTimeSeconds)

	// When starting the etcd client we retry all rpcs until the client is stopped so Tektite will await startup
	// and not bomb out if etcd is not available
	resp, err := callEtcdWithRetry(c, func() (*clientv3.LeaseGrantResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), c.callTimeout)
		defer cancel()
		return c.cli.Grant(ctx, int64(leaseTimeSeconds))
	})
	if err != nil {
		return err
	}
	c.leaseID = resp.ID

	// First we delete any old node key that might remain - e.g. if the node crashes and quickly restarts - it might
	// have incorrect valid groups
	if err := c.deleteNodeKey(); err != nil {
		log.Debugf("failed to delete key %s", c.thisNodeKey)
	}

	// And we delete valid groups for this node that might remain
	if err := c.deleteValidGroupsKey(); err != nil {
		return err
	}

	log.Debugf("%s: node %d etcd client adding node key %s", c.logScope, c.nodeID, c.thisNodeKey)

	// Put a key with lease
	putResp, err := callEtcdWithRetry(c, func() (*clientv3.PutResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), c.callTimeout)
		defer cancel()
		return c.cli.Put(ctx, c.thisNodeKey, "", clientv3.WithLease(c.leaseID))
	})

	log.Debugf("%s: node %d etcd client added node key %s", c.logScope, c.nodeID, c.thisNodeKey)

	c.thisNodeRev = putResp.Header.Revision

	// Start goroutine to keep the lease alive
	common.Go(c.keepLeaseAlive)

	// Get the initial nodes state
	getResp, err := callEtcdWithRetry(c, func() (*clientv3.GetResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), c.callTimeout)
		defer cancel()
		return c.cli.Get(ctx, c.nodesKey, clientv3.WithPrefix())
	})
	if err != nil {
		return err
	}
	for _, kv := range getResp.Kvs {
		log.Debugf("%s: node %d etcd client got initial node state %s", c.logScope, c.nodeID, string(kv.Key))
		if err := c.handleNodeAdd(string(kv.Key), kv.ModRevision); err != nil {
			return err
		}
	}
	c.requiresNodesUpdate = true

	// Watch the nodes directory for changes
	nodesWatchCh := c.cli.Watch(c.clientCtx, c.nodesKey, clientv3.WithPrefix(),
		clientv3.WithRev(getResp.Header.Revision+1))
	common.Go(func() {
		c.nodesWatchLoop(nodesWatchCh)
	})

	// Watch the valid groups directory for changes
	validGroupsWatchCh := c.cli.Watch(c.clientCtx, c.validGroupsKey, clientv3.WithPrefix(),
		clientv3.WithRev(getResp.Header.Revision+1))
	common.Go(func() {
		c.validGroupsWatchLoop(validGroupsWatchCh)
	})

	// Get the initial cluster state
	getResp, err = callEtcdWithRetry(c, func() (*clientv3.GetResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), c.callTimeout)
		defer cancel()
		return c.cli.Get(ctx, c.clusterStateKey)
	})
	if err != nil {
		return err
	}
	if len(getResp.Kvs) > 0 {
		bytes := getResp.Kvs[0].Value
		info := deserializeClusterState(bytes)
		c.currentClusterStateInfo = info
		c.requiresClusterStateUpdate = true
	}

	// Watch the cluster state key for changes
	clusterStateWatchCh := c.cli.Watch(c.clientCtx, c.clusterStateKey, clientv3.WithRev(getResp.Header.Revision+1))
	common.Go(func() {
		c.clusterStateWatchLoop(clusterStateWatchCh)
	})

	// Start the updater - note we do not send an update on any change otherwise we can get a storm of updates when
	// cluster starting/stopping, so we send them periodically
	common.Go(c.sendUpdateLoop)
	c.started = true
	return nil
}

func callEtcdWithRetry[R any](c *client, action func() (R, error)) (R, error) {
	act := func() (R, error) {
		r, err := action()
		return r, convertEtcdError(err)
	}
	return common.CallWithRetryOnUnavailableWithTimeout(act, c.isStopped, 1*time.Second,
		time.Duration(math.MaxInt64), c.notAvailableMsg)
}

func (c *client) Stop(halt bool) error {
	if c.isStopped() {
		return nil
	}
	c.stopped.Store(true)
	c.locksLock.Lock()
	defer c.locksLock.Unlock()
	c.lock.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			c.lock.Unlock()
		}
	}()
	if !halt {
		// When "halting" in tests we do not delete the key - this simulates a crash as the key would not be deleted
		// in that case
		if err := c.deleteNodeKey(); err != nil {
			log.Warnf("failed to delete key %s %v", c.thisNodeKey, err)
		}
		if err := c.deleteValidGroupsKey(); err != nil {
			log.Warnf("failed to delete valid groups %s %v", c.validGroupsKey, err)
		}
		if c.shuttingDown {
			// Cluster shut-down is occurring, we delete the cluster state key so when the cluster
			// is restarted it doesn't have an old invalid one in etcd
			if err := c.deleteClusterState(); err != nil {
				log.Warnf("failed to delete cluster state key %s %v", c.clusterStateKey, err)
			}
		}
	}
	c.cancelFunc() // Stops the watchers and keep alive
	if err := c.cli.Close(); err != nil {
		log.Warnf("failed to close etcd client %v", err)
	}
	c.lock.Unlock()
	unlocked = true
	c.stopWG.Wait() // Must wait outside lock
	return nil
}

func (c *client) isStopped() bool {
	return c.stopped.Load()
}

func (c *client) GetNodeRevision() int64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.thisNodeRev
}

func (c *client) PrepareForShutdown() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.shuttingDown = true
}

func (c *client) nodesWatchLoop(watchCh clientv3.WatchChan) {
	defer c.stopWG.Done()
	for watchResp := range watchCh {
		c.handleNodesWatchEvents(watchResp)
	}
}

func (c *client) validGroupsWatchLoop(watchCh clientv3.WatchChan) {
	defer c.stopWG.Done()
	for watchResp := range watchCh {
		c.handleValidGroupsWatchLoop(watchResp)
	}
}

func (c *client) handleValidGroupsWatchLoop(watchResp clientv3.WatchResponse) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.isStopped() || c.shuttingDown {
		return
	}
	for _, event := range watchResp.Events {
		revision := event.Kv.ModRevision
		if revision > c.maxNodesRevision {
			c.maxNodesRevision = revision
		}
	}
	c.requiresNodesUpdate = true
}

func (c *client) handleNodesWatchEvents(watchResp clientv3.WatchResponse) {
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Debugf("%s: node %d etcd client got node watch event", c.logScope, c.nodeID)

	if c.isStopped() || c.shuttingDown {
		log.Debugf("%s: node %d etcd client got node watch event but not processing it as stopped? %t shuttingdown %t", c.logScope, c.nodeID, c.isStopped(), c.shuttingDown)
		return
	}
	for _, event := range watchResp.Events {
		key := string(event.Kv.Key)
		if event.Type == clientv3.EventTypeDelete {
			log.Debugf("%s: node %d etcd client handling node delete: %s", c.logScope, c.nodeID, key)
			if err := c.handleNodeDelete(key, event.Kv.ModRevision); err != nil {
				log.Errorf("failed to handle delete %v", err)
			}
		} else {
			log.Debugf("%s: node %d etcd client handling node add: %s", c.logScope, c.nodeID, key)
			if err := c.handleNodeAdd(key, event.Kv.ModRevision); err != nil {
				log.Errorf("failed to handle add %v", err)
			}
		}
	}
}

func (c *client) handleNodeAdd(key string, revision int64) error {
	key = strings.TrimPrefix(key, c.nodesKey)
	nodeID, err := strconv.Atoi(key)
	if err != nil {
		return errors.Errorf("invalid key %s", key)
	}
	c.activeNodes[nodeID] = revision
	if revision > c.maxNodesRevision {
		c.maxNodesRevision = revision
	}
	c.requiresNodesUpdate = true
	return nil
}

func (c *client) handleNodeDelete(key string, revision int64) error {
	key = strings.TrimPrefix(key, c.nodesKey)
	nodeID, err := strconv.Atoi(key)
	if err != nil {
		return errors.Errorf("invalid key %s", key)
	}
	delete(c.activeNodes, nodeID)
	if revision > c.maxNodesRevision {
		c.maxNodesRevision = revision
	}
	c.requiresNodesUpdate = true
	return nil
}

func (c *client) clusterStateWatchLoop(watchCh clientv3.WatchChan) {
	defer c.stopWG.Done()
	for watchResp := range watchCh {
		c.handleClusterStateWatchEvents(watchResp)
	}
}

func (c *client) handleClusterStateWatchEvents(watchResp clientv3.WatchResponse) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.isStopped() || c.shuttingDown {
		return
	}
	for _, event := range watchResp.Events {
		if event.Type == clientv3.EventTypePut {
			info := deserializeClusterState(event.Kv.Value)
			c.currentClusterStateInfo = info
			c.requiresClusterStateUpdate = true
		}
	}
}

type clusterStateInfo struct {
	cs          *ClusterState
	nodesState  map[int]int64
	maxRevision int64
}

func (c *client) sendUpdateLoop() {
	defer c.stopWG.Done()
	ticker := time.NewTicker(c.sendUpdatePeriod)
	for {
		select {
		case <-c.clientCtx.Done():
			return
		case <-ticker.C:
			c.sendUpdate()
		}
	}
}

func (c *client) sendUpdate() {
	c.lock.Lock()
	defer c.lock.Unlock()
	defer func() {
		c.requiresNodesUpdate = false
		c.requiresClusterStateUpdate = false
	}()
	if c.requiresNodesUpdate {
		nodeCopy := make(map[int]int64, len(c.activeNodes))
		valid := false
		for nid, version := range c.activeNodes {
			nodeCopy[nid] = version
			if nid == c.nodeID && version == c.thisNodeRev {
				valid = true
			}
		}
		if valid {
			c.nodesChangeHandler(nodeCopy, c.maxNodesRevision)
		}
	}
	if c.requiresClusterStateUpdate {
		cs := *c.currentClusterStateInfo.cs
		c.clusterStateHandler(cs)
	}
}

func (c *client) deleteNodeKey() error {
	log.Debugf("client on node %d deleting node key %s", c.nodeID, c.thisNodeKey)
	ctx, cancel := context.WithTimeout(context.Background(), c.callTimeout)
	defer cancel()
	_, err := c.cli.Delete(ctx, c.thisNodeKey, clientv3.WithPrefix())
	if err != nil {
		return convertEtcdError(err)
	}
	return nil
}

func (c *client) deleteValidGroupsKey() error {
	key := fmt.Sprintf("%s%d", c.validGroupsKey, c.nodeID)
	ctx, cancel := context.WithTimeout(context.Background(), c.callTimeout)
	defer cancel()
	_, err := c.cli.Delete(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return convertEtcdError(err)
	}
	return nil
}

func (c *client) keepLeaseAlive() {
	defer c.stopWG.Done()
	keepAliveCh, err := c.cli.KeepAlive(c.clientCtx, c.leaseID)
	if err != nil {
		log.Warnf("failed to keep alive %v", err)
	}

	// Loop over the keep alive channel to handle lease renewals
loop:
	for {
		select {
		case _, ok := <-keepAliveCh:
			if !ok {
				log.Warnf("etcd client node %d - lease expired", c.nodeID)
				// Lease expired, so exit the loop
				break loop
			}
		case <-c.clientCtx.Done():
			// Context was canceled, so exit the loop
			break loop
		}
	}
}

func (c *client) MarkGroupAsValid(nodeID int, groupID int, joinedVersion int) (bool, error) {
	// We encode the joined version as a zero padded string - this is because etcd using a lexical comparison when
	// comparing values.
	key := fmt.Sprintf("%s%d/%d", c.validGroupsKey, nodeID, groupID)
	newVersion := fmt.Sprintf("%020d", joinedVersion)

	ctx, cancel := context.WithTimeout(context.Background(), c.callTimeout)
	defer cancel()

	// Will only succeed if old version < new version
	resp, err := c.cli.Txn(ctx).If(clientv3.Compare(clientv3.Value(key), "<", newVersion)).
		Then(clientv3.OpPut(key, newVersion)).
		Commit()
	if err != nil {
		return false, convertEtcdError(err)
	}
	if resp.Succeeded {
		return true, nil
	}
	// Maybe there is no pre-existing entry- in this case it's ok to put
	resp, err = c.cli.Txn(ctx).If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, newVersion)).
		Commit()
	if err != nil {
		return false, convertEtcdError(err)
	}
	return resp.Succeeded, nil
}

func (c *client) GetValidGroups() (map[string]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.callTimeout)
	defer cancel()
	getResp, err := c.cli.Get(ctx, c.validGroupsKey, clientv3.WithPrefix())
	if err != nil {
		return nil, convertEtcdError(err)
	}
	validGroups := make(map[string]int, len(getResp.Kvs))
	for _, kv := range getResp.Kvs {
		sKey := strings.TrimPrefix(string(kv.Key), c.validGroupsKey)
		unpadded := strings.TrimLeft(string(kv.Value), "0")
		if unpadded == "" {
			unpadded = "0"
		}
		joinedVersion, err := strconv.ParseUint(unpadded, 10, 64)
		if err != nil {
			return nil, err
		}
		validGroups[sKey] = int(joinedVersion)
	}
	return validGroups, nil
}

func (c *client) GetEtcdClient() *clientv3.Client {
	return c.cli
}

func (c *client) GetClusterHead() string {
	return c.clusterHead
}

func (c *client) GetMutex(name string) (*Mutex, error) {
	session, err := concurrency.NewSession(c.cli)
	if err != nil {
		return nil, convertEtcdError(err)
	}
	mut := concurrency.NewMutex(session, fmt.Sprintf("%s%s", c.mutexesKey, name))
	if err := mut.Lock(context.Background()); err != nil {
		return nil, convertEtcdError(err)
	}
	return &Mutex{
		session: session,
		mut:     mut,
	}, nil
}

func (c *client) GetLock(lockName string, timeout time.Duration) (bool, error) {
	c.locksLock.Lock()
	defer c.locksLock.Unlock()
	if c.isStopped() {
		return false, errors.New("cluster client is stopped")
	}
	// create a session for distributed locking
	session, err := concurrency.NewSession(c.cli)
	if err != nil {
		log.Errorf("client %d failed to create session %v", c.nodeID, err)
		return false, convertEtcdError(err)
	}
	//goland:noinspection GoUnhandledErrorResult
	defer session.Close()

	lockKey := c.createLockKey(lockName)

	// create a mutex for the lock
	mutex := concurrency.NewMutex(session, lockKey)

	ctx, cancel := context.WithTimeout(context.Background(), c.callTimeout)
	defer cancel()

	// attempt to acquire the lock
	if err := mutex.TryLock(ctx); err != nil {
		if errors.Is(err, concurrency.ErrLocked) {
			return false, nil
		}
		return false, convertEtcdError(err)
	}

	defer func() {
		if err := mutex.Unlock(ctx); err != nil {
			log.Errorf("failed to unlock mutex %v", err)
		}
	}()

	// check if the lock already exists
	resp, err := c.cli.Get(ctx, lockKey)
	if err != nil {
		return false, convertEtcdError(err)
	}
	if len(resp.Kvs) > 0 {
		// Lock already held
		return false, nil
	}

	// create a lease for the lock
	grantResp, err := c.cli.Grant(ctx, int64(timeout/time.Second))
	if err != nil {
		return false, convertEtcdError(err)
	}

	// set the lease ID in the lock node's value
	if _, err := c.cli.Put(ctx, lockKey, "", clientv3.WithLease(grantResp.ID)); err != nil {
		_, err2 := c.cli.Revoke(ctx, grantResp.ID) // revoke the lease if setting the lock node fails
		if err2 != nil {
			log.Errorf("failed to revoke lease %v", err2)
		}
		return false, convertEtcdError(err)
	}
	return true, nil
}

func (c *client) ReleaseLock(lockName string) error {
	c.locksLock.Lock()
	defer c.locksLock.Unlock()
	if c.isStopped() {
		return errors.New("cluster client is stopped")
	}
	lockKey := c.createLockKey(lockName)
	ctx, cancel := context.WithTimeout(context.Background(), c.callTimeout)
	defer cancel()
	delResp, err := c.cli.Delete(ctx, lockKey)
	if err == nil {
		if delResp.Deleted != 1 {
			panic("no lock key deleted")
		}
	}
	return convertEtcdError(err)
}

func (c *client) createLockKey(lockName string) string {
	return fmt.Sprintf("%s%s", c.locksKey, lockName)
}

func (c *client) GetClusterState() (*ClusterState, map[int]int64, int64, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.callTimeout)
	defer cancel()
	resp, err := c.cli.Get(ctx, c.clusterStateKey)
	if err != nil {
		return nil, nil, 0, 0, convertEtcdError(err)
	}
	if len(resp.Kvs) == 0 {
		return nil, nil, 0, 0, nil
	}
	info := deserializeClusterState(resp.Kvs[0].Value)
	return info.cs, info.nodesState, resp.Kvs[0].Version, info.maxRevision, nil
}

func deserializeClusterState(bytes []byte) clusterStateInfo {
	cs := &ClusterState{}
	offset := cs.Deserialize(bytes, 0)
	var ne uint32
	ne, offset = encoding.ReadUint32FromBufferLE(bytes, offset)
	nodesState := make(map[int]int64, int(ne))
	for i := 0; i < int(ne); i++ {
		var nid uint64
		nid, offset = encoding.ReadUint64FromBufferLE(bytes, offset)
		var ver uint64
		ver, offset = encoding.ReadUint64FromBufferLE(bytes, offset)
		nodesState[int(nid)] = int64(ver)
	}
	maxRevision, _ := encoding.ReadUint32FromBufferLE(bytes, offset)
	return clusterStateInfo{
		cs:          cs,
		nodesState:  nodesState,
		maxRevision: int64(maxRevision),
	}
}

func (c *client) SetClusterState(clusterState *ClusterState, nodesState map[int]int64, previousVersion int64, maxRevision int64) (bool, error) {
	log.Debugf("%s: client node id %d setting cluster state %v", c.logScope, c.nodeID, clusterState)
	ctx, cancel := context.WithTimeout(context.Background(), c.callTimeout)
	defer cancel()
	txn := c.cli.Txn(ctx)
	bytes := clusterState.Serialize(nil)
	bytes = encoding.AppendUint32ToBufferLE(bytes, uint32(len(nodesState)))
	for nid, ver := range nodesState {
		bytes = encoding.AppendUint64ToBufferLE(bytes, uint64(nid))
		bytes = encoding.AppendUint64ToBufferLE(bytes, uint64(ver))
	}
	bytes = encoding.AppendUint64ToBufferLE(bytes, uint64(maxRevision))
	// Only put the KV if the current version matches the specified version - this protects against network
	// partitions
	resp, err := txn.If(clientv3.Compare(clientv3.Version(c.clusterStateKey), "=", previousVersion)).
		Then(clientv3.OpPut(c.clusterStateKey, string(bytes))).Commit()
	if err != nil {
		return false, convertEtcdError(err)
	}
	return resp.Succeeded, nil
}

func (c *client) deleteClusterState() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.callTimeout)
	defer cancel()
	_, err := c.cli.Delete(ctx, c.clusterStateKey, clientv3.WithPrefix())
	return convertEtcdError(err)
}

func convertEtcdError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.NewTektiteErrorf(errors.Unavailable, "etcd appears to be unavailable - timed out making call")
	}
	if e, ok := status.FromError(err); ok {
		if e.Code() == codes.Unavailable || e.Code() == codes.Canceled || e.Code() == codes.DeadlineExceeded {
			return errors.NewTektiteErrorf(errors.Unavailable, "etcd unavailable %v", e)
		}
	}
	return err
}
