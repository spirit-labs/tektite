package replicator

import (
	"encoding/binary"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

/*

* create a simple handler (maybe rename to statemachine) and apply commands to it, at end of test verify that state on all replicas is the same
* now simulate transient network failure such that it is retried and works, and followers get duplicates, make sure they are rejected and state machines are equal.
* test with network retries past limit and replica is removed from group
* test with replica returning wrong epoch - no need to remove from group here as already wrong epoch.
* test failover to another up to date replica - what if new leader has higher state than followers? this can happen if previous replication failed half way. think about whether this matters?
* test bouncing follower, make sure it re-initialises ok
* test with flushing.
* chaos test - set up a cluster and inject randomly: 1) network errors, then add/remove nodes randomly. verify state is always the same at points, and at end of test.

 */

func TestReplicator(t *testing.T) {
	numReplicas := 3

	localTransports := transport.NewLocalTransports()

	replicators := make([]*Replicator, numReplicas)

	numGroups := 10

	initialState := cluster.MembershipState{
		Epoch: 1,
	}

	memberships := newTestMemberships()

	for i := 0; i < numReplicas; i++ {
		address := uuid.New().String()
		initialState.Members = append(initialState.Members, cluster.MembershipEntry{Address: address})
		trans, err := localTransports.NewLocalTransport(address)
		require.NoError(t, err)
		replicator := NewReplicator(trans, numReplicas, address, memberships.createMembership, ReplicatorOpts{})
		err = replicator.CreateGroup(1, &testStateMachine{})
		require.NoError(t, err)
		replicators[i] = replicator
		replicator.Start()
	}

	command := &testCommand{
		data: "some-data",
	}

	// Apply the initial cluster state
	err := memberships.sendUpdate(initialState)
	require.NoError(t, err)

	_, err = replicators[0].ApplyCommand(command, 1)

	for i := 0; i < numReplicas; i++ {
		groups := replicators[i].GetGroups()
		require.Equal(t, numGroups, len(groups))
		group := groups[0]
		require.NotNil(t, group)
		handler := group.stateMachine.(*testStateMachine)
		commands := handler.getCommands()
		require.Equal(t, 1, len(commands))
		require.Equal(t, command, commands[0])
	}

	require.NoError(t, err)
}

type testMemberships struct {
	lock        sync.Mutex
	memberships map[string]*testMembership
}

func newTestMemberships() *testMemberships {
	return &testMemberships{
		memberships: make(map[string]*testMembership),
	}
}

func (t *testMemberships) sendUpdate(state cluster.MembershipState) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	for _, membership := range t.memberships {
		if err := membership.membershipChangedHandler(state); err != nil {
			return err
		}
	}
	return nil
}

func (t *testMemberships) createMembership(address string,
	membershipChangedHandler func(state cluster.MembershipState) error) membership {
	t.lock.Lock()
	defer t.lock.Unlock()
	ms := &testMembership{
		membershipChangedHandler: membershipChangedHandler,
	}
	t.memberships[address] = ms
	return ms
}

type testMembership struct {
	membershipChangedHandler func(state cluster.MembershipState) error
}

func (t *testMembership) SetValid(valid bool) {
}

func (t *testMembership) MemberFailed(address string) {
}

type testStateMachine struct {
	lock     sync.Mutex
	commands []Command
}

func (t *testStateMachine) LoadInitialState() (int64, error) {
	return 0, nil
}

func (t *testStateMachine) UpdateState(command Command, commandSequence int64) (any, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.commands = append(t.commands, command)
	return nil, nil
}

func (t *testStateMachine) Flush(flushCompleted func(flushedSequence int64, err error)) error {
	//TODO implement me
	panic("implement me")
}

func (t *testStateMachine) Flushed(flushedCommandSeq int64) error {
	//TODO implement me
	panic("implement me")
}

func (t *testStateMachine) Initialise(commandSequence int64) error {
	//TODO implement me
	panic("implement me")
}

func (t *testStateMachine) Reset() error {
	//TODO implement me
	panic("implement me")
}

func (t *testStateMachine) getCommands() []Command {
	t.lock.Lock()
	defer t.lock.Unlock()
	commandsCopy := make([]Command, len(t.commands))
	copy(commandsCopy, t.commands)
	return commandsCopy
}

func (t *testStateMachine) NewCommand() (Command, error) {
	return &testCommand{}, nil
}

type testCommand struct {
	data string
}

func (t *testCommand) Serialize(buff []byte) ([]byte, error) {
	ltd := len(t.data)
	buff = binary.BigEndian.AppendUint32(buff, uint32(ltd))
	buff = append(buff, t.data...)
	return buff, nil
}

func (t *testCommand) Deserialize(buff []byte) ([]byte, error) {
	ltd := binary.BigEndian.Uint32(buff)
	end := 4 + int(ltd)
	t.data = string(buff[4:end])
	return buff[end:], nil
}
