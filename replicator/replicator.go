package replicator

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/arista"
	"github.com/spirit-labs/tektite/cluster"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/transport"
	"github.com/timandy/routine"
	"sync"
	"sync/atomic"
	"time"
)

type StateMachineFactory func() StateMachine

type StateMachine interface {
	// NewCommand creates a new command
	NewCommand() (Command, error)
	// LoadInitialState is called on the initial leader - it should load it's state, including command sequence from permanent storage
	// and return the command sequence
	LoadInitialState() (int64, error)
	// UpdateState is called to process a command and update the state machine
	UpdateState(command Command, commandSequence int64) (any, error)
	// Flush is called on the leader to flush persisted state to storage - flush can occur asynchronously and when complete
	// flushCompleted must be called with the sequence number flushed
	Flush(flushCompleted func(flushedSequence int64, err error)) error
	// Flushed is called on the followers to signal that commands up to and including flushedCommandSeq have been flushed
	// followers can then discard any state before that, if they want
	Flushed(flushedCommandSeq int64) error
	// Initialise is called on a new follower when it joins - it should load any state from permanent storage from the provided
	// sequence number
	Initialise(commandSequence int64) error
	// Reset - resets all internal state
	Reset() error
}

type Command interface {
	Serialize(buff []byte) ([]byte, error)
	Deserialize(buff []byte) ([]byte, error)
}

type ReplicatorOpts struct {
	FlushInterval time.Duration
}

const (
	DefaultFlushInterval = 5 * time.Second

	// FIXME - but this in opts
	SyncRetryInterval = 5 * time.Second
)

type membership interface {
	SetValid(valid bool)
	MemberFailed(address string)
}

type membershipFactory func(address string, membershipChangedHandler func(state cluster.MembershipState) error) membership

func NewReplicator(transport transport.Transport, replicationFactor int, address string,
	membershipFactory membershipFactory, opts ReplicatorOpts) *Replicator {
	if opts.FlushInterval == 0 {
		opts.FlushInterval = DefaultFlushInterval
	}
	r := &Replicator{
		transport:         transport,
		address:           address,
		replicationFactor: replicationFactor,
		minReplications:   (replicationFactor + 1) / 2,
		opts:              opts,
		groups:            map[int]*ReplicationGroup{},
	}
	mShip := membershipFactory(address, r.membershipChanged)
	r.membership = mShip
	transport.RegisterHandler(r.handleRequest)
	return r
}

type Replicator struct {
	lock              sync.RWMutex
	started           bool
	address           string
	transport         transport.Transport
	membership        membership
	replicationFactor int
	minReplications   int
	opts              ReplicatorOpts
	groups            map[int]*ReplicationGroup
	flushTimer        *time.Timer
	membershipState   *cluster.MembershipState
	leader            bool
	state             ReplicaState
	syncedGroups      int64
	syncingFollowers  []string
	syncTimer         *time.Timer
}

type ReplicaState int

const (
	ReplicaStateUninitialised = ReplicaState(iota)
	ReplicaStateLeader
	ReplicaStateInvalidFollower
	ReplicaStateFollower
)

func (r *Replicator) CreateGroup(id int, stateMachine StateMachine) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	_, exists := r.groups[id]
	if exists {
		return fmt.Errorf("group with id %d already exists", id)
	}
	group := &ReplicationGroup{
		replicator:   r,
		id:           id,
		stateMachine: stateMachine,
		connections:  map[string]transport.Connection{},
	}
	r.groups[id] = group
	return nil
}

func (r *Replicator) ApplyCommand(command Command, groupID int) (any, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	if r.state == ReplicaStateUninitialised {
		return nil, &ReplicationError{ErrorCode: ErrorCodeLeaderNotInitialised}
	}
	if r.state != ReplicaStateLeader {
		return nil, &ReplicationError{ErrorCode: ErrorCodeNotLeader}
	}
	if len(r.membershipState.Members)-1 < r.minReplications {
		return nil, &ReplicationError{ErrorCode: ErrorCodeInsufficientFollowers}
	}
	group, ok := r.groups[groupID]
	if !ok {
		return nil, fmt.Errorf("group with id %d not found", groupID)
	}
	return group.applyCommand(command)
}

func (r *Replicator) scheduleFlush() {
	r.flushTimer = time.AfterFunc(r.opts.FlushInterval, func() {
		r.lock.Lock()
		defer r.lock.Unlock()
		if !r.started {
			return
		}
		if err := r.flush(); err != nil {
			log.Errorf("Error flushing replication groups: %v", err)
		}
		r.scheduleFlush()
	})
}

func (r *Replicator) Flush() error {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.flush()
}

func (r *Replicator) flush() error {
	if r.state != ReplicaStateLeader {
		return nil
	}
	resultCh := make(chan flushResult, len(r.groups))
	for _, group := range r.groups {
		if err := group.flush(resultCh); err != nil {
			return err
		}
	}
	for i := 0; i < len(r.groups); i++ {
		res := <-resultCh
		if res.err != nil {
			return res.err
		}
	}
	return nil
}

func (r *Replicator) ReceiveSyncRequest(address string) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.syncingFollowers = append(r.syncingFollowers, address)
	// TODO - maybe flush straight away to avoid waiting for timer?
	return nil
}

func (r *Replicator) MembershipChanged(membership cluster.MembershipState) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if !r.started {
		return
	}
	if err := r.membershipChanged(membership); err != nil {
		log.Errorf("failed to handle membershipState change: %v", err)
	}
}

func (r *Replicator) membershipChanged(membership cluster.MembershipState) error {
	r.membershipState = &membership
	r.leader = len(r.membershipState.Members) > 0 && r.membershipState.Members[0].Address == r.address
	// Note the Membership - will automatically only add itself as member if there are no other members - so only for new
	// leader - then it will start updating
	// Then there will be an StartUpdating method on Membership which will add self as follower
	switch r.state {
	case ReplicaStateUninitialised:
		if r.leader {
			// New leader
			if err := r.initialiseNewLeader(); err != nil {
				return err
			}
		} else {
			// New follower
			if err := r.startFollowerJoin(); err != nil {
				return err
			}
		}
	case ReplicaStateLeader:
		if !r.leader {
			// not leader any more
			if err := r.unmakeLeader(); err != nil {
				return err
			}
		} else {
			// Update syncing members - they might have become followers
			var newSyncing []string
			for _, syncing := range r.syncingFollowers {
				isMember := false
				for _, member := range r.membershipState.Members {
					if member.Address == syncing {
						// Syncing follower is now in group
						isMember = true
						break
					}
				}
				if !isMember {
					newSyncing = append(newSyncing, syncing)
				}
			}
			r.syncingFollowers = newSyncing
		}
	case ReplicaStateFollower:
		if r.leader {
			if err := r.makeLeader(); err != nil {
				return err
			}
			r.syncingFollowers = nil
		}
	case ReplicaStateInvalidFollower:
		// nothing to do
	default:
		panic("unknown replicator state")
	}
	return nil
}

func (r *Replicator) initialiseNewLeader() error {
	for _, group := range r.groups {
		// TODO can be called in parallel
		if err := group.loadInitialLeaderState(); err != nil {
			return err
		}
	}
	r.state = ReplicaStateLeader
	return nil
}

func (r *Replicator) startFollowerJoin() error {
	// send sync to leader
	r.state = ReplicaStateInvalidFollower
	for _, group := range r.groups {
		group.setUnsyncedFollower()
	}
	r.sendSyncToLeader()
	return nil
}

func (r *Replicator) unmakeLeader() error {
	// We were leader but have been evicted from the membershipState, maybe due to a temporary network issue
	for _, group := range r.groups {
		// TODO can be called in parallel
		if err := group.reset(); err != nil {
			return err
		}
	}
	r.syncingFollowers = nil
	r.membership.SetValid(false)
	// Try and rejoin as a follower
	return r.startFollowerJoin()
}

func (r *Replicator) makeLeader() error {
	r.state = ReplicaStateLeader
	return nil
}
func (r *Replicator) allGroupsSynced() {
	r.membership.SetValid(true)
	r.state = ReplicaStateFollower
}

func (r *Replicator) Start() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.started {
		return
	}
	r.scheduleFlush()
	r.started = true
}

func (r *Replicator) Stop() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if !r.started {
		return
	}
	r.started = false
	r.flushTimer.Stop()
	if r.syncTimer != nil {
		r.syncTimer.Stop()
	}
	for _, group := range r.groups {
		group.Close()
	}
	r.groups = map[int]*ReplicationGroup{}
}

func (r *Replicator) GetGroups() map[int]*ReplicationGroup {
	r.lock.RLock()
	defer r.lock.RUnlock()
	groupsCopy := make(map[int]*ReplicationGroup, len(r.groups))
	for id, group := range r.groups {
		groupsCopy[id] = group
	}
	return groupsCopy
}

func (r *Replicator) handleRequest(message []byte, responseWriter transport.ResponseHandler) error {
	r.lock.RLock()
	defer r.lock.RUnlock()
	// TODO version this protocol
	isSync := message[0] == 1
	if isSync {
		return r.handleSyncRequest(message[1:], responseWriter)
	}
	groupID := int(binary.BigEndian.Uint64(message))
	group, ok := r.groups[groupID]
	if !ok {
		return errors.Errorf("group with id %d not found", groupID)
	}
	return group.handleRequest(message[8:], responseWriter)
}

func (r *Replicator) handleSyncRequest(message []byte, responseWriter transport.ResponseHandler) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	epoch := int(binary.BigEndian.Uint64(message))
	l := binary.BigEndian.Uint16(message[8:])
	address := string(message[10 : 10+int(l)])
	var errorCode ErrorCode
	if r.membershipState.Epoch != epoch {
		errorCode = ErrorCodeInvalidEpoch
	} else if r.state != ReplicaStateLeader {
		errorCode = ErrorCodeNotLeader
	} else {
		isMember := false
		for _, member := range r.membershipState.Members {
			if address == member.Address {
				isMember = true
			}
		}
		if isMember {
			errorCode = ErrorCodeSyncingFollowerIsMember
		} else {
			// Sync can be retried on temporary network failure so there can be duplicates
			alreadySyncing := false
			for _, syncing := range r.syncingFollowers {
				if address == syncing {
					alreadySyncing = true
					break
				}
			}
			if !alreadySyncing {
				r.syncingFollowers = append(r.syncingFollowers, address)
			}
			errorCode = ErrorCodeNode
		}
	}
	resp := binary.BigEndian.AppendUint16(nil, uint16(errorCode))
	return responseWriter(resp)
}

func (r *Replicator) sendSyncToLeader() {
	if err := r.syncWithLeader(); err != nil {
		// Can happen if leader changes
		log.Warnf("failed to send sync to leader: %v - will retry", err)
		// Retry after delay
		r.scheduleSync()
	}
}

func (r *Replicator) scheduleSync() {
	r.syncTimer = time.AfterFunc(SyncRetryInterval, func() {
		r.lock.Lock()
		defer r.lock.Unlock()
		if !r.started {
			return
		}
		r.sendSyncToLeader()
	})
}

func (r *Replicator) syncWithLeader() error {
	leader := r.membershipState.Members[0].Address
	// FIXME close connection
	conn, err := r.transport.CreateConnection(leader, func(message []byte) error {
		if len(message) != 2 {
			return errors.New("invalid sync response")
		}
		errorCode := ErrorCode(binary.BigEndian.Uint16(message))
		r.handleSyncResponse(errorCode)
		return nil
	})
	if err != nil {
		return err
	}
	buff := make([]byte, 0, 1+8+2+len(r.address))
	buff = append(buff, 1) // sync
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.membershipState.Epoch))
	buff = binary.BigEndian.AppendUint16(buff, uint16(len(r.address))) // address
	buff = append(buff, r.address...)
	return conn.WriteMessage(buff)
}

func (r *Replicator) handleSyncResponse(errorCode ErrorCode) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.state != ReplicaStateInvalidFollower {
		log.Errorf("got sync response but invalid state: %d", r.state)
		return
	}
	if errorCode == ErrorCodeNode {
		return
	}
	// reschedule
	log.Warnf("got error from sync with leader: %s", errorCode.String())
	r.scheduleSync()
}

const (
	replicateTimeout             = 5 * time.Second
	networkErrorMaxRetryDuration = 10 * time.Second
	networkErrorRetryInterval    = 1 * time.Second
)

type ReplicationGroup struct {
	replicator        *Replicator
	lock              sync.Mutex
	connections       map[string]transport.Connection
	id                int
	stateMachine      StateMachine
	commandSeq        int64
	flushedCommandSeq int64
	epoch             int64
	responseHolder    atomic.Pointer[responseChanHolder]
	unsyncedFollower  bool
}

func (g *ReplicationGroup) setUnsyncedFollower() {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.unsyncedFollower = true
}

func (g *ReplicationGroup) getConnection(address string) (transport.Connection, error) {
	// FIXME - remove connections when they fail
	conn, exists := g.connections[address]
	if exists {
		return conn, nil
	}
	conn, err := g.replicator.transport.CreateConnection(address, func(message []byte) error {
		return g.handleResponse(address, message)
	})
	if err != nil {
		return nil, err
	}
	g.connections[address] = conn
	return conn, nil
}

func (g *ReplicationGroup) handleResponse(address string, response []byte) error {
	commandSequence := int64(binary.BigEndian.Uint64(response))
	responseCode := ErrorCode(binary.BigEndian.Uint16(response[8:]))
	holder := *g.responseHolder.Load()
	// FIXME we should pass back epoch too just to be on safe side
	if holder.commandSequence != commandSequence {
		log.Warnf("Unexpected command sequence in response %d expected %d", commandSequence, holder.commandSequence)
		return nil
	}
	holder.ch <- replicationResponse{
		address:   address,
		errorCode: responseCode,
	}
	return nil
}

type responseChanHolder struct {
	commandSequence int64
	ch              chan replicationResponse
}

type replicationResponse struct {
	address   string
	errorCode ErrorCode
}

type flushResult struct {
	flushedSeq int64
	err        error
}

func (g *ReplicationGroup) flush(resultCh chan flushResult) error {
	g.lock.Lock()
	defer g.lock.Unlock()
	goID := routine.Goid()
	return g.stateMachine.Flush(func(flushedSeq int64, err error) {
		if err != nil {
			resultCh <- flushResult{
				err: err,
			}
		} else {
			if goID != routine.Goid() {
				// Different goroutine, so need to lock
				g.lock.Lock()
				defer g.lock.Unlock()
			}
			g.flushedCommandSeq = flushedSeq
			// Send a flushed message
			if err := g.replicateCommand(nil, true); err != nil {
				resultCh <- flushResult{
					err: err,
				}
			} else {
				resultCh <- flushResult{
					flushedSeq: flushedSeq,
				}
			}
		}
	})
}

func (g *ReplicationGroup) reset() error {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.commandSeq = 0
	g.flushedCommandSeq = 0
	g.epoch = -1
	return g.stateMachine.Reset()
}

func (g *ReplicationGroup) loadInitialLeaderState() error {
	g.lock.Lock()
	defer g.lock.Unlock()
	commandSeq, err := g.stateMachine.LoadInitialState()
	if err != nil {
		return err
	}
	g.commandSeq = commandSeq
	return nil
}

func (g *ReplicationGroup) applyCommand(command Command) (any, error) {
	g.lock.Lock()
	defer g.lock.Unlock()
	if err := g.replicateCommand(command, false); err != nil {
		return nil, err
	}
	res, err := g.stateMachine.UpdateState(command, g.commandSeq)
	if err != nil {
		return nil, err
	}
	g.commandSeq++
	return res, nil
}

func (g *ReplicationGroup) replicateCommand(command Command, isFlush bool) error {
	// FIXME this must be two phase
	buff := make([]byte, 0, 64)
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.id))
	if isFlush {
		buff = append(buff, byte(1))
	} else {
		buff = append(buff, byte(0))
	}
	if isFlush {
		buff = binary.BigEndian.AppendUint64(buff, uint64(g.flushedCommandSeq))
	} else {
		buff = binary.BigEndian.AppendUint64(buff, uint64(g.commandSeq))
	}
	buff = binary.BigEndian.AppendUint64(buff, uint64(g.epoch))
	if buff != nil {
		var err error
		buff, err = command.Serialize(buff)
		if err != nil {
			return err
		}
	}
	members := g.replicator.membershipState.Members
	followers := members[1:]

	replicateAddresses := make([]string, 0, len(followers)+len(g.replicator.syncingFollowers))
	for _, follower := range followers {
		replicateAddresses = append(replicateAddresses, follower.Address)
	}
	replicateAddresses = append(replicateAddresses, g.replicator.syncingFollowers...)

	respHolder := &responseChanHolder{
		commandSequence: g.commandSeq,
		ch:              make(chan replicationResponse, len(replicateAddresses)),
	}
	g.commandSeq++
	g.responseHolder.Store(respHolder)
	sentFollowers := make([]string, 0, len(replicateAddresses))
	for _, address := range replicateAddresses {
		conn, err := g.getConnection(address)
		if err != nil {
			return err
		}
		sent := g.sendReplicationWithRetry(conn, address, buff)
		if sent {
			sentFollowers = append(sentFollowers, address)
		}
	}
	if len(sentFollowers) < g.replicator.minReplications {
		return &ReplicationError{ErrorCode: ErrorCodeInsufficientReplications}
	}
	// TODO should we check received addresses???
	var successes int
loop:
	for i := 0; i < len(sentFollowers); i++ {
		select {
		case resp := <-respHolder.ch:
			if resp.errorCode != ErrorCodeNode {
				log.Warnf("Replica %s returned error code %d %s", resp.address, resp.errorCode,
					resp.errorCode.String())
				g.replicator.membership.MemberFailed(resp.address)
			} else {
				successes++
			}
		case <-time.After(replicateTimeout):
			break loop
		}
	}
	// TODO deal with ErrorCodeLeaderSequenceTooAdvanced
	if successes < g.replicator.minReplications {
		// Insufficient replications
		return errors.Errorf("unable to replicate to sufficient replicas")
	}
	return nil
}

func (g *ReplicationGroup) sendReplicationWithRetry(conn transport.Connection, address string, message []byte) bool {
	start := arista.NanoTime()
	for {
		err := conn.WriteMessage(message)
		if err == nil {
			return true
		}
		if int64(arista.NanoTime()-start) >= networkErrorMaxRetryDuration.Nanoseconds() {
			log.Errorf("error in writing replication, follower will be marked invalid %v", err)
			g.replicator.membership.MemberFailed(address)
			return false
		}
		log.Warnf("error when sending replication - will retry %v", err)
		time.Sleep(networkErrorRetryInterval)
		continue
	}
}

func (g *ReplicationGroup) handleRequest(message []byte, responseWriter transport.ResponseHandler) error {
	isFlush := message[0] == 1
	commandSeq := int64(binary.BigEndian.Uint64(message[1:]))
	epoch := int64(binary.BigEndian.Uint64(message[9:]))
	if g.replicator.leader {
		log.Warnf("replication arrived at leader: %s", g.replicator.address)
		return g.writeReplicationResponse(ErrorCodeLeader, commandSeq, responseWriter)
	}
	if commandSeq < g.commandSeq {
		log.Warnf("replicator group %d duplicate command received: %d expected: %d", g.id, commandSeq,
			g.commandSeq+1)
		return g.writeReplicationResponse(ErrorCodeNode, commandSeq, responseWriter)
	}
	if commandSeq > g.commandSeq+1 {
		// This can occur if one of the followers has a higher command sequence than others then becomes leader
		// and then replicates at that higher sequence to a follower who sees a gap in the sequences.
		// A follower can have a higher sequence if a previous replication failed after replicating to some but not all
		// followers.
		log.Warnf("replicator group %d unexpected command received: %d expected: %d - leader sequence too advanced", g.id, commandSeq,
			g.commandSeq+1)
		return g.writeReplicationResponse(ErrorCodeLeaderSequenceTooAdvanced, commandSeq, responseWriter)
	}
	if g.epoch != epoch {
		log.Warnf("replicator group %d received replication at wrong epoch received: %d expected: %d", g.id, epoch,
			g.epoch)
		return g.writeReplicationResponse(ErrorCodeInvalidEpoch, commandSeq, responseWriter)
	}
	if isFlush {
		if g.unsyncedFollower {
			// We are a new follower waiting for a flush
			// The new follower can now load state from the flushed sequence
			if err := g.stateMachine.Initialise(commandSeq); err != nil {
				return err
			}
			// Now we are synced
			g.commandSeq = commandSeq
			g.unsyncedFollower = false
			if int(atomic.AddInt64(&g.replicator.syncedGroups, 1)) == len(g.replicator.groups) {
				// We have received a flush on each group, so we are fully synced as a follower and can add ourself
				// to the membershipState group
				g.replicator.allGroupsSynced()
			}
		} else {
			// Tell the state machine that it can flush any data <= commandSeq
			if err := g.stateMachine.Flushed(commandSeq); err != nil {
				return err
			}
		}
	} else {
		// Invalid replicas ignore commands until they see a flush
		if !g.unsyncedFollower {
			command, err := g.createCommand(message[17:])
			if err != nil {
				return err
			}
			_, err = g.stateMachine.UpdateState(command, commandSeq)
			if err != nil {
				return err
			}
		}
	}
	return g.writeReplicationResponse(ErrorCodeNode, commandSeq, responseWriter)
}

func (g *ReplicationGroup) createCommand(message []byte) (Command, error) {
	command, err := g.stateMachine.NewCommand()
	if err != nil {
		return nil, err
	}
	_, err = command.Deserialize(message)
	if err != nil {
		return nil, err
	}
	return command, nil
}

func (g *ReplicationGroup) writeReplicationResponse(errorcode ErrorCode, commandSequence int64, responseWriter transport.ResponseHandler) error {
	buff := make([]byte, 20)
	binary.BigEndian.PutUint64(buff, uint64(g.id))
	binary.BigEndian.PutUint64(buff[8:], uint64(commandSequence))
	binary.BigEndian.PutUint16(buff[16:], uint16(errorcode))
	return responseWriter(buff)
}

func (g *ReplicationGroup) Close() {
	for _, conn := range g.connections {
		if err := conn.Close(); err != nil {
			log.Debugf("failed to close replication group connection: %v", err)
		}
	}
}

type ErrorCode int16

const (
	ErrorCodeNode ErrorCode = iota
	ErrorCodeInvalidEpoch
	ErrorCodeLeaderSequenceTooAdvanced
	ErrorCodeInternalError
	ErrorCodeNotLeader
	ErrorCodeLeaderNotInitialised
	ErrorCodeLeader
	ErrorCodeInsufficientFollowers
	ErrorCodeInsufficientReplications
	ErrorCodeSyncingFollowerIsMember
)

type ReplicationError struct {
	ErrorCode ErrorCode
}

func (r ReplicationError) Error() string {
	return fmt.Sprintf("replication error: %d %s", r.ErrorCode, r.ErrorCode.String())
}

func (e ErrorCode) String() string {
	switch e {
	case ErrorCodeNode:
		return ""
	case ErrorCodeInvalidEpoch:
		return "replica invalid epoch"
	case ErrorCodeLeaderSequenceTooAdvanced:
		return "leader sequence too advanced"
	case ErrorCodeInternalError:
		return "replica internal error"
	case ErrorCodeNotLeader:
		return "replica is not leader"
	case ErrorCodeLeaderNotInitialised:
		return "leader not initialised"
	case ErrorCodeLeader:
		return "replica is leader"
	case ErrorCodeInsufficientFollowers:
		return "insufficient followers"
	case ErrorCodeInsufficientReplications:
		return "insufficient replications"
	case ErrorCodeSyncingFollowerIsMember:
		return "syncing follower is member"
	default:
		panic("unknown replicator code")
	}
}
