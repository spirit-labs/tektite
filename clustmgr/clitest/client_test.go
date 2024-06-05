package clitest

import (
	"context"
	"fmt"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync/atomic"
	"testing"
	"time"
)

var etcdAddress string

func TestMain(m *testing.M) {
	etcd, err := testutils.CreateEtcdContainer()
	if err != nil {
		panic(err)
	}
	etcdAddress = etcd.Address()
	defer etcd.Stop()
	m.Run()
}

func TestMembershipChanges(t *testing.T) {
	t.Parallel()
	cleanUp(t)
	ch := make(chan map[int]int64, 10)
	nodeID := 23
	updatePeriod := 1 * time.Second
	leaseTime := 2 * time.Second
	cli := clustmgr.NewClient(t.Name(), "test_cluster", nodeID,
		[]string{etcdAddress}, leaseTime, updatePeriod, 5*time.Second,
		func(state map[int]int64) {
			ch <- state
		}, func(cs clustmgr.ClusterState) {})
	err := cli.Start()
	require.NoError(t, err)
	defer func() {
		err := cli.Stop(false)
		require.NoError(t, err)
	}()
	time.Sleep(updatePeriod)
	state := <-ch

	require.Equal(t, 1, len(state))
	ver0, ok := state[23]
	require.True(t, ok)

	// Now create some more clients
	var extraClients []clustmgr.Client
	defer func() {
		for _, cl := range extraClients {
			//goland:noinspection GoUnhandledErrorResult
			cl.Stop(false)
		}
	}()
	numExtraNodes := 3
	for i := 0; i < numExtraNodes; i++ {
		client := clustmgr.NewClient(t.Name(), "test_cluster", nodeID+i+1,
			[]string{etcdAddress}, leaseTime, updatePeriod, 5*time.Second,
			func(state map[int]int64) {}, func(cs clustmgr.ClusterState) {})
		err := client.Start()
		require.NoError(t, err)
		extraClients = append(extraClients, client)
	}

	time.Sleep(updatePeriod)
	state = <-ch

	require.Equal(t, 4, len(state))
	ver0_1, ok := state[23]
	require.True(t, ok)
	require.Equal(t, ver0, ver0_1)

	ver1, ok := state[24]
	require.True(t, ok)

	ver2, ok := state[25]
	require.True(t, ok)

	ver3, ok := state[26]
	require.True(t, ok)

	// Now stop one
	err = extraClients[1].Stop(false)
	require.NoError(t, err)
	state = <-ch

	require.Equal(t, 3, len(state))

	ver0_1, ok = state[23]
	require.True(t, ok)
	require.Equal(t, ver0, ver0_1)

	ver1_1, ok := state[24]
	require.True(t, ok)
	require.Equal(t, ver1, ver1_1)

	ver3_1, ok := state[26]
	require.True(t, ok)
	require.Equal(t, ver3, ver3_1)

	// Stop another
	err = extraClients[2].Stop(false)
	require.NoError(t, err)
	state = <-ch

	require.Equal(t, 2, len(state))

	ver0_1, ok = state[23]
	require.True(t, ok)
	require.Equal(t, ver0, ver0_1)

	ver1_1, ok = state[24]
	require.True(t, ok)
	require.Equal(t, ver1, ver1_1)

	// restart one
	extraClients[1] = clustmgr.NewClient(t.Name(), "test_cluster", nodeID+1+1,
		[]string{etcdAddress}, leaseTime, updatePeriod, 5*time.Second,
		func(state map[int]int64) {}, func(cs clustmgr.ClusterState) {})
	err = extraClients[1].Start()
	require.NoError(t, err)
	require.NoError(t, err)
	state = <-ch

	require.Equal(t, 3, len(state))

	ver0_1, ok = state[23]
	require.True(t, ok)
	require.Equal(t, ver0, ver0_1)

	ver1_1, ok = state[24]
	require.True(t, ok)
	require.Equal(t, ver1, ver1_1)

	// The version must increase when the node rejoins
	ver2_1, ok := state[25]
	require.True(t, ok)
	require.Greater(t, ver2_1, ver2)
	require.Greater(t, ver2_1, ver0)
	require.Greater(t, ver2_1, ver1)
}

func TestLockGetRelease(t *testing.T) {
	t.Parallel()
	cleanUp(t)
	cli1 := clustmgr.NewClient(t.Name(), "test_cluster", 10,
		[]string{etcdAddress}, 1*time.Second, 1*time.Second, 5*time.Second,
		func(state map[int]int64) {}, func(cs clustmgr.ClusterState) {})
	err := cli1.Start()
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer cli1.Stop(false)

	cli2 := clustmgr.NewClient(t.Name(), "test_cluster", 20,
		[]string{etcdAddress}, 1*time.Second, 1*time.Second, 5*time.Second,
		func(state map[int]int64) {}, func(cs clustmgr.ClusterState) {})
	err = cli2.Start()
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer cli2.Stop(false)

	// Get lock - OK
	ok, err := cli1.GetLock("lock1", 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	// Get same lock again on same client, should fail
	ok, err = cli1.GetLock("lock1", 5*time.Second)
	require.NoError(t, err)
	require.False(t, ok)

	// Try again on different client - should fail
	ok, err = cli2.GetLock("lock1", 5*time.Second)
	require.NoError(t, err)
	require.False(t, ok)

	// Try a different lock on the different client - OK
	ok, err = cli2.GetLock("lock2", 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	// And then try and get it on the first client, should fail
	ok, err = cli1.GetLock("lock2", 5*time.Second)
	require.NoError(t, err)
	require.False(t, ok)

	// Release lock2 on the same client that got it
	err = cli2.ReleaseLock("lock2")
	require.NoError(t, err)

	// Try and get it again - OK
	ok, err = cli2.GetLock("lock2", 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = cli2.GetLock("lock2", 5*time.Second)
	require.NoError(t, err)
	require.False(t, ok)

	// Release lock2 on a different client
	err = cli1.ReleaseLock("lock2")
	require.NoError(t, err)

	ok, err = cli1.GetLock("lock2", 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestLockTimeout(t *testing.T) {
	t.Parallel()

	cleanUp(t)
	cli1 := clustmgr.NewClient(t.Name(), "test_cluster", 10,
		[]string{etcdAddress}, 1*time.Second, 1*time.Second, 5*time.Second,
		func(state map[int]int64) {}, func(cs clustmgr.ClusterState) {})
	err := cli1.Start()
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer cli1.Stop(false)

	timeout := 2 * time.Second
	// Get lock - OK
	start := time.Now()
	ok, err := cli1.GetLock("lock1", timeout)
	require.NoError(t, err)
	require.True(t, ok)

	// Get same lock again on same client, should fail
	ok, err = cli1.GetLock("lock1", timeout)
	require.NoError(t, err)
	require.False(t, ok)

	// Try and obtain it in a loop until lock times out and it succeeds
	for {
		ok, err = cli1.GetLock("lock1", timeout)
		require.NoError(t, err)
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Greater(t, time.Since(start), timeout)
}

func TestSetAndGetClusterState(t *testing.T) {
	t.Parallel()

	cleanUp(t)
	cli := clustmgr.NewClient(t.Name(), "test_cluster", 10,
		[]string{etcdAddress}, 1*time.Second, 1*time.Second, 5*time.Second,
		func(state map[int]int64) {}, func(cs clustmgr.ClusterState) {})
	err := cli.Start()
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer cli.Stop(false)

	csR, ns, ver, err := cli.GetClusterState()
	require.Nil(t, csR)
	require.Equal(t, 0, int(ver))
	require.Equal(t, 0, len(ns))

	cs := &clustmgr.ClusterState{
		Version: 23,
		GroupStates: [][]clustmgr.GroupNode{
			{clustmgr.GroupNode{0, false, true, 1}, clustmgr.GroupNode{1, true, true, 1}, clustmgr.GroupNode{2, false, true, 1}},
			{clustmgr.GroupNode{2, true, true, 1}, clustmgr.GroupNode{1, false, true, 1}, clustmgr.GroupNode{0, false, true, 1}},
			{clustmgr.GroupNode{2, false, true, 1}, clustmgr.GroupNode{0, false, true, 1}, clustmgr.GroupNode{1, true, true, 1}},
			{clustmgr.GroupNode{1, true, true, 1}, clustmgr.GroupNode{0, false, true, 1}, clustmgr.GroupNode{2, false, true, 1}},
		},
	}
	nsSet := map[int]int64{
		0: 100,
		1: 100,
		2: 100,
	}
	ok, err := cli.SetClusterState(cs, nsSet, 47)
	require.NoError(t, err)
	// Should fail as there's no previous cluster state with that version
	require.False(t, ok)

	csR, ns, ver, err = cli.GetClusterState()
	require.Nil(t, csR)
	require.Equal(t, 0, int(ver))
	require.Equal(t, 0, len(ns))

	ok, err = cli.SetClusterState(cs, nsSet, 0)
	require.NoError(t, err)
	// Should succeed as version 0 means no previous version
	require.True(t, ok)

	csR, ns, ver, err = cli.GetClusterState()
	require.NoError(t, err)
	require.NotNil(t, csR)
	require.Equal(t, 1, int(ver))
	require.Equal(t, cs, csR)
	require.Equal(t, nsSet, ns)

	nsSet2 := map[int]int64{
		0: 101,
		1: 101,
		2: 101,
	}

	cs2 := &clustmgr.ClusterState{
		Version: 24,
		GroupStates: [][]clustmgr.GroupNode{
			{clustmgr.GroupNode{0, true, true, 1}, clustmgr.GroupNode{1, true, true, 1}, clustmgr.GroupNode{2, false, true, 1}},
			{clustmgr.GroupNode{2, true, true, 1}, clustmgr.GroupNode{1, false, true, 1}, clustmgr.GroupNode{0, false, true, 1}},
			{clustmgr.GroupNode{2, false, false, 1}, clustmgr.GroupNode{0, false, false, 1}, clustmgr.GroupNode{1, true, true, 1}},
			{clustmgr.GroupNode{1, true, true, 1}, clustmgr.GroupNode{0, false, true, 1}, clustmgr.GroupNode{2, false, true, 1}},
		},
	}

	ok, err = cli.SetClusterState(cs2, nsSet2, 0)
	require.NoError(t, err)
	require.False(t, ok)

	csR, ns, ver, err = cli.GetClusterState()
	require.NoError(t, err)
	require.NotNil(t, csR)
	require.Equal(t, 1, int(ver))
	require.Equal(t, cs, csR)
	require.Equal(t, nsSet, ns)

	ok, err = cli.SetClusterState(cs2, nsSet2, 1)
	require.NoError(t, err)
	require.True(t, ok)

	csR, ns, ver, err = cli.GetClusterState()
	require.NoError(t, err)
	require.NotNil(t, csR)
	require.Equal(t, 2, int(ver))
	require.Equal(t, cs2, csR)
	require.Equal(t, nsSet2, ns)

	nsSet3 := map[int]int64{
		0: 102,
		1: 102,
		2: 102,
	}

	cs3 := &clustmgr.ClusterState{
		Version: 26,
		GroupStates: [][]clustmgr.GroupNode{
			{clustmgr.GroupNode{0, true, true, 1}, clustmgr.GroupNode{1, true, true, 1}, clustmgr.GroupNode{2, false, true, 1}},
			{clustmgr.GroupNode{1, false, true, 1}, clustmgr.GroupNode{1, false, true, 1}, clustmgr.GroupNode{0, false, true, 1}},
			{clustmgr.GroupNode{2, false, false, 1}, clustmgr.GroupNode{0, false, false, 1}, clustmgr.GroupNode{1, true, true, 1}},
			{clustmgr.GroupNode{1, true, true, 1}, clustmgr.GroupNode{0, true, true, 1}, clustmgr.GroupNode{2, false, true, 1}},
		},
	}
	ok, err = cli.SetClusterState(cs3, nsSet3, 2)
	require.NoError(t, err)
	require.True(t, ok)

	csR, ns, ver, err = cli.GetClusterState()
	require.NoError(t, err)
	require.NotNil(t, csR)
	require.Equal(t, 3, int(ver))
	require.Equal(t, cs3, csR)
	require.Equal(t, nsSet3, ns)
}

func TestSetAndReceiveClusterState(t *testing.T) {
	t.Parallel()

	cleanUp(t)
	numClients := 5
	var clients []clustmgr.Client
	var chans []chan clustmgr.ClusterState
	var nodeChanges int64

	for i := 0; i < numClients; i++ {
		ch := make(chan clustmgr.ClusterState, 1)
		cli := clustmgr.NewClient(t.Name(), "test_cluster", i,
			[]string{etcdAddress}, 1*time.Second, 1*time.Second, 5*time.Second,
			func(state map[int]int64) {
				atomic.AddInt64(&nodeChanges, 1)
			}, func(cs clustmgr.ClusterState) {
				ch <- cs
			})
		err := cli.Start()
		require.NoError(t, err)
		//goland:noinspection GoDeferInLoop
		defer func() {
			err := cli.Stop(false)
			require.NoError(t, err)
		}()
		clients = append(clients, cli)
		chans = append(chans, ch)
	}

	ns := getClientRevisions(clients)
	var ver int64
	for i, cli := range clients {

		cs := clustmgr.ClusterState{
			Version: 23 + i,
			GroupStates: [][]clustmgr.GroupNode{
				{clustmgr.GroupNode{0, false, false, 1}, clustmgr.GroupNode{1, true, false, 1}, clustmgr.GroupNode{2, false, false, 1}},
				{clustmgr.GroupNode{2, true, false, 1}, clustmgr.GroupNode{1, false, false, 1}, clustmgr.GroupNode{0, false, false, 1}},
				{clustmgr.GroupNode{2, false, false, 1}, clustmgr.GroupNode{0, false, false, 1}, clustmgr.GroupNode{1, true, false, 1}},
				{clustmgr.GroupNode{1, true, false, 1}, clustmgr.GroupNode{0, false, false, 1}, clustmgr.GroupNode{2, false, false, 1}},
			},
		}
		ok, err := cli.SetClusterState(&cs, ns, ver)
		require.NoError(t, err)
		require.True(t, ok)
		for _, ch := range chans {
			csReceived := <-ch
			require.Equal(t, cs, csReceived)
		}
		ver++
	}

	// Marking a replica as valid should also prompt the node change handler to be called
	nodeChangesStart := atomic.LoadInt64(&nodeChanges)
	ok, err := clients[0].MarkGroupAsValid(1, 3, 1000)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = testutils.WaitUntilWithError(func() (bool, error) {
		return atomic.LoadInt64(&nodeChanges) == nodeChangesStart+1, nil
	}, 5*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestClusterStateWithOldNodeVersionsNotReceived(t *testing.T) {
	t.Parallel()

	cleanUp(t)
	numClients := 5
	var clients []clustmgr.Client
	var chans []chan clustmgr.ClusterState
	var nodeChanges int64

	for i := 0; i < numClients; i++ {
		ch := make(chan clustmgr.ClusterState, 1)
		cli := clustmgr.NewClient(t.Name(), "test_cluster", i,
			[]string{etcdAddress}, 1*time.Second, 1*time.Second, 5*time.Second,
			func(state map[int]int64) {
				atomic.AddInt64(&nodeChanges, 1)
			}, func(cs clustmgr.ClusterState) {
				ch <- cs
			})
		err := cli.Start()
		require.NoError(t, err)
		//goland:noinspection GoDeferInLoop
		defer func() {
			err := cli.Stop(false)
			require.NoError(t, err)
		}()
		clients = append(clients, cli)
		chans = append(chans, ch)
	}

	// Create a map where all the revisions are old
	ns := getClientRevisions(clients)
	for nodeID, rev := range ns {
		ns[nodeID] = rev - 1
	}

	var ver int64
	for i, cli := range clients {

		cs := clustmgr.ClusterState{
			Version: 23 + i,
			GroupStates: [][]clustmgr.GroupNode{
				{clustmgr.GroupNode{0, false, false, 1}, clustmgr.GroupNode{1, true, false, 1}, clustmgr.GroupNode{2, false, false, 1}},
				{clustmgr.GroupNode{2, true, false, 1}, clustmgr.GroupNode{1, false, false, 1}, clustmgr.GroupNode{0, false, false, 1}},
				{clustmgr.GroupNode{2, false, false, 1}, clustmgr.GroupNode{0, false, false, 1}, clustmgr.GroupNode{1, true, false, 1}},
				{clustmgr.GroupNode{1, true, false, 1}, clustmgr.GroupNode{0, false, false, 1}, clustmgr.GroupNode{2, false, false, 1}},
			},
		}

		ok, err := cli.SetClusterState(&cs, ns, ver)
		require.NoError(t, err)
		require.True(t, ok)
		ver++

		ok, err = cli.SetClusterState(&cs, map[int]int64{}, ver)
		require.NoError(t, err)
		require.True(t, ok)
		ver++
	}

	for _, ch := range chans {
		select {
		case <-ch:
			require.Fail(t, "should not receive update")
		case <-time.After(250 * time.Millisecond):
			// OK
			break
		}
	}

}

func getClientRevisions(clients []clustmgr.Client) map[int]int64 {
	revisions := map[int]int64{}
	for i, client := range clients {
		revisions[i] = client.GetNodeRevision()
	}
	return revisions
}

func TestMarkGroupAsValidJoinedVersion(t *testing.T) {
	t.Parallel()

	cleanUp(t)
	cli := clustmgr.NewClient(t.Name(), "test_cluster", 10,
		[]string{etcdAddress}, 1*time.Second, 1*time.Second, 5*time.Second,
		func(state map[int]int64) {}, func(cs clustmgr.ClusterState) {})
	err := cli.Start()
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer cli.Stop(false)

	groups, err := cli.GetValidGroups()
	require.NoError(t, err)
	require.Equal(t, 0, len(groups))

	nodeID := 1
	groupID := 2

	ok, err := cli.MarkGroupAsValid(nodeID, groupID, 23)
	require.NoError(t, err)
	require.True(t, ok)

	// Should fail as 23 is not > 23
	ok, err = cli.MarkGroupAsValid(nodeID, groupID, 23)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = cli.MarkGroupAsValid(nodeID, groupID, 24)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = cli.MarkGroupAsValid(nodeID, groupID, 25)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = cli.MarkGroupAsValid(nodeID, groupID, 24)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = cli.MarkGroupAsValid(nodeID, groupID, 30)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = cli.MarkGroupAsValid(nodeID, groupID, 29)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = cli.MarkGroupAsValid(nodeID, groupID, 0)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestGetValidGroups(t *testing.T) {
	t.Parallel()

	cleanUp(t)
	cli := clustmgr.NewClient(t.Name(), "test_cluster", 10,
		[]string{etcdAddress}, 1*time.Second, 1*time.Second, 5*time.Second,
		func(state map[int]int64) {}, func(cs clustmgr.ClusterState) {})
	err := cli.Start()
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer cli.Stop(false)

	groups, err := cli.GetValidGroups()
	require.NoError(t, err)
	require.Equal(t, 0, len(groups))

	for jv := 0; jv < 5; jv++ {
		for nodeID := 0; nodeID < 3; nodeID++ {
			for groupID := 0; groupID < 10; groupID++ {
				ok, err := cli.MarkGroupAsValid(nodeID, groupID, jv)
				require.NoError(t, err)
				require.True(t, ok)
			}
		}
	}

	groups, err = cli.GetValidGroups()
	require.NoError(t, err)
	require.Equal(t, 30, len(groups))
	for i := 0; i < 10; i++ {
		for nodeID := 0; nodeID < 3; nodeID++ {
			for groupID := 0; groupID < 10; groupID++ {
				key := fmt.Sprintf("%d/%d", nodeID, groupID)
				ver, ok := groups[key]
				require.True(t, ok)
				require.Equal(t, 4, int(ver))
			}
		}
	}
}

func cleanUp(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddress},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cli.Delete(ctx, t.Name(), clientv3.WithPrefix())
	cancel()
	require.NoError(t, err)
}
