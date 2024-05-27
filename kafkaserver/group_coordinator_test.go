package kafkaserver

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	defaultSessionTimeout   = 30 * time.Second
	defaultRebalanceTimeout = 30 * time.Second
	defaultInitialJoinDelay = 100 * time.Millisecond
	defaultClientID         = "clientid1"
	defaultProtocolType     = "protocol_type1"
	defaultProtocolName     = "protocol1"
)

func TestFindCoordinator(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)
	for i := 0; i < 100; i++ {
		groupName := uuid.New().String()
		nodeID := -1
		// Should evaluate the same on all nodes
		for j, gc := range gcs {
			theNodeID := gc.FindCoordinator(groupName)
			if j > 0 {
				require.Equal(t, nodeID, theNodeID)
			} else {
				nodeID = theNodeID
			}
		}
		require.True(t, nodeID >= 0 && nodeID <= 2)
	}
}

func TestInitialJoinNoMemberID(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	protocolMetadata := []byte("protocol1_bytes")
	protocols := []ProtocolInfo{
		{defaultProtocolName, protocolMetadata},
	}

	start := time.Now()
	groupID := uuid.New().String()
	clientID := uuid.New().String()
	gc := findNode(groupID, gcs)
	// Only get this behaviour with API version >= 4
	res := callJoinGroupSyncWithApiVersion(gc, groupID, clientID, "", defaultProtocolType,
		protocols, defaultSessionTimeout, defaultRebalanceTimeout, 4)
	require.True(t, time.Now().Sub(start) < defaultInitialJoinDelay) // Should be no delay
	require.Equal(t, ErrorCodeUnknownMemberID, res.ErrorCode)
	require.True(t, strings.HasPrefix(res.MemberID, clientID))
	require.Equal(t, 37, len(res.MemberID)-len(clientID))
	require.Equal(t, "", res.LeaderMemberID)
	require.Equal(t, "", res.ProtocolName)
	require.Equal(t, 0, res.GenerationID)
	require.Equal(t, 0, len(res.Members))
}

func TestInitialMemberJoinAfterDelay(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	protocolMetadata := []byte("protocol1_bytes")
	protocols := []ProtocolInfo{
		{defaultProtocolName, protocolMetadata},
	}
	groupID := uuid.New().String()
	clientID := uuid.New().String()
	gc := findNode(groupID, gcs)
	start := time.Now()
	res := callJoinGroupSync(gc, groupID, clientID, "", defaultProtocolType,
		protocols, defaultSessionTimeout, defaultRebalanceTimeout)
	require.Equal(t, ErrorCodeNone, res.ErrorCode)
	dur := time.Now().Sub(start)
	require.True(t, dur >= defaultInitialJoinDelay)
	require.True(t, strings.HasPrefix(res.MemberID, clientID))
	require.Equal(t, 37, len(res.MemberID)-len(clientID))
	require.Equal(t, res.MemberID, res.LeaderMemberID)
	require.Equal(t, defaultProtocolName, res.ProtocolName)
	require.Equal(t, 1, res.GenerationID)
	require.Equal(t, 1, len(res.Members))
	require.Equal(t, res.MemberID, res.Members[0].MemberID)
	require.Equal(t, protocolMetadata, res.Members[0].MetaData)
}

func TestJoinMultipleMembersBeforeInitialDelay(t *testing.T) {

	initialJoinDelay := 250 * time.Millisecond
	gcs := createCoordinators(t, initialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	var memberMetaDataMap sync.Map

	numMembers := 10
	gc := findNode(groupID, gcs)
	chans := make([]chan JoinResult, numMembers)
	start := time.Now()
	joinWg := sync.WaitGroup{}
	joinWg.Add(numMembers)
	for i := 0; i < numMembers; i++ {
		ch := make(chan JoinResult, 1)
		protocolMetadata := []byte(fmt.Sprintf("metadata-%d", i))
		protocols := []ProtocolInfo{
			{defaultProtocolName, protocolMetadata},
		}
		gc.JoinGroup(0, groupID, defaultClientID, "", defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout, func(result JoinResult) {
			memberMetaDataMap.Store(result.MemberID, protocolMetadata)
			ch <- result
			joinWg.Done()
		})
		chans[i] = ch
	}

	joinWg.Wait()

	for _, ch := range chans {
		res := <-ch
		// It's twice defaultInitialJoinDelay because we delay once, and if any new members join since last delay we
		// delay again, until no new members join.
		require.True(t, time.Now().Sub(start) >= 2*initialJoinDelay)
		require.Equal(t, ErrorCodeNone, res.ErrorCode)
		require.True(t, strings.HasPrefix(res.MemberID, defaultClientID))
		require.Equal(t, 37, len(res.MemberID)-len(defaultClientID))
		require.Equal(t, defaultProtocolName, res.ProtocolName)
		require.Equal(t, 1, res.GenerationID)
		if res.MemberID == res.LeaderMemberID {
			require.Equal(t, numMembers, len(res.Members))
			for _, member := range res.Members {
				o, ok := memberMetaDataMap.Load(member.MemberID)
				require.True(t, ok)
				require.Equal(t, o.([]byte), member.MetaData)
			}
		} else {
			require.Equal(t, 0, len(res.Members))
		}
	}
}

func TestExtendInitialJoinDelayToRebalanceTimeout(t *testing.T) {

	initialJoinDelay := 100 * time.Millisecond
	rebalanceTimeout := 500 * time.Millisecond
	gcs := createCoordinators(t, initialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	var memberMetaDataMap sync.Map

	numMembers := 10
	gc := findNode(groupID, gcs)
	chans := make([]chan JoinResult, numMembers)
	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(numMembers)
	for i := 0; i < numMembers; i++ {
		ch := make(chan JoinResult, 1)
		protocolMetadata := []byte(fmt.Sprintf("metadata-%d", i))
		protocols := []ProtocolInfo{
			{defaultProtocolName, protocolMetadata},
		}
		// We pause half the initial join delay each time, this should have the effect of extending the delay
		time.Sleep(initialJoinDelay / 2)

		gc.JoinGroup(0, groupID, defaultClientID, "", defaultProtocolType, protocols, defaultSessionTimeout, rebalanceTimeout, func(result JoinResult) {
			memberMetaDataMap.Store(result.MemberID, protocolMetadata)
			ch <- result
			wg.Done()
		})

		chans[i] = ch
	}

	wg.Wait()

	for _, ch := range chans {
		res := <-ch
		delay := time.Now().Sub(start)
		require.True(t, delay >= 2*initialJoinDelay)
		require.True(t, delay > rebalanceTimeout)
		require.True(t, delay < rebalanceTimeout+initialJoinDelay)

		require.Equal(t, ErrorCodeNone, res.ErrorCode)
		require.True(t, strings.HasPrefix(res.MemberID, defaultClientID))
		require.Equal(t, 37, len(res.MemberID)-len(defaultClientID))
		require.Equal(t, defaultProtocolName, res.ProtocolName)
		require.Equal(t, 1, res.GenerationID)
		if res.MemberID == res.LeaderMemberID {
			require.Equal(t, numMembers, len(res.Members))
			for _, member := range res.Members {
				o, ok := memberMetaDataMap.Load(member.MemberID)
				require.True(t, ok)
				require.Equal(t, o.([]byte), member.MetaData)
			}
		} else {
			require.Equal(t, 0, len(res.Members))
		}
	}
}

func TestChooseProtocol(t *testing.T) {
	testChooseProtocol(t, [][]ProtocolInfo{
		{{Name: "prot1", Metadata: []byte("meta")}, {Name: "prot2", Metadata: []byte("meta")}, {Name: "prot3", Metadata: []byte("meta")}},
		{{Name: "prot1", Metadata: []byte("meta")}, {Name: "prot2", Metadata: []byte("meta")}, {Name: "prot3", Metadata: []byte("meta")}},
	}, "prot1")
	testChooseProtocol(t, [][]ProtocolInfo{
		{{Name: "prot1", Metadata: []byte("meta")}, {Name: "prot2", Metadata: []byte("meta")}, {Name: "prot3", Metadata: []byte("meta")}},
		{{Name: "prot2", Metadata: []byte("meta")}, {Name: "prot3", Metadata: []byte("meta")}},
	}, "prot2")
	testChooseProtocol(t, [][]ProtocolInfo{
		{{Name: "prot1", Metadata: []byte("meta")}, {Name: "prot2", Metadata: []byte("meta")}, {Name: "prot3", Metadata: []byte("meta")}},
		{{Name: "prot4", Metadata: []byte("meta")}, {Name: "prot3", Metadata: []byte("meta")}, {Name: "prot5", Metadata: []byte("meta")}},
		{{Name: "prot6", Metadata: []byte("meta")}, {Name: "prot7", Metadata: []byte("meta")}, {Name: "prot3", Metadata: []byte("meta")}},
	}, "prot3")
	testChooseProtocol(t, [][]ProtocolInfo{
		{{Name: "prot1", Metadata: []byte("meta")}},
		{{Name: "prot1", Metadata: []byte("meta")}},
		{{Name: "prot1", Metadata: []byte("meta")}},
	}, "prot1")
	testChooseProtocol(t, [][]ProtocolInfo{
		{{Name: "prot1", Metadata: []byte("meta")}, {Name: "prot2", Metadata: []byte("meta")}, {Name: "prot3", Metadata: []byte("meta")}},
		{{Name: "prot2", Metadata: []byte("meta")}, {Name: "prot3", Metadata: []byte("meta")}, {Name: "prot4", Metadata: []byte("meta")}},
		{{Name: "prot6", Metadata: []byte("meta")}, {Name: "prot3", Metadata: []byte("meta")}, {Name: "prot2", Metadata: []byte("meta")}},
	}, "prot2")
}

func testChooseProtocol(t *testing.T, infos [][]ProtocolInfo, expectedProtocol string) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)
	groupID := uuid.New().String()

	gc := findNode(groupID, gcs)
	chans := make([]chan JoinResult, len(infos))
	for i, protocolInfos := range infos {
		ch := make(chan JoinResult, 1)
		thePIs := protocolInfos
		gc.JoinGroup(0, groupID, defaultClientID, "", defaultProtocolType, thePIs, defaultSessionTimeout, defaultRebalanceTimeout, func(result JoinResult) {
			ch <- result
		})
		chans[i] = ch
	}
	for _, ch := range chans {
		res := <-ch
		require.Equal(t, ErrorCodeNone, res.ErrorCode)
		require.Equal(t, expectedProtocol, res.ProtocolName)
	}
}

func TestJoinUnsupportedProtocol(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)
	groupID := uuid.New().String()
	gc := findNode(groupID, gcs)
	protocols1 := []ProtocolInfo{{"prot1", []byte("foo")}, {"prot2", []byte("foo")}}
	res := callJoinGroupSync(gc, groupID, defaultClientID, "", "pt1", protocols1, defaultSessionTimeout, defaultRebalanceTimeout)
	res = callJoinGroupSync(gc, groupID, defaultClientID, res.MemberID, "pt1", protocols1, defaultSessionTimeout, defaultRebalanceTimeout)

	protocols2 := []ProtocolInfo{{"prot3", []byte("foo")}, {"prot4", []byte("foo")}}
	res = callJoinGroupSync(gc, groupID, defaultClientID, "", "pt1", protocols2, defaultSessionTimeout, defaultRebalanceTimeout)
	require.Equal(t, ErrorCodeInconsistentGroupProtocol, res.ErrorCode)
}

func TestJoinNotController(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)
	for i := 0; i < 10; i++ {
		groupID := uuid.New().String()
		coordinator := findNode(groupID, gcs)
		for _, gc := range gcs {
			if gc != coordinator {
				res := callJoinGroupSync(gc, groupID, defaultClientID, "", defaultProtocolType,
					nil, 0, 0)
				require.Equal(t, ErrorCodeNotCoordinator, res.ErrorCode)
			}
		}
	}
}

func TestSyncGroup(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()

	// First join
	numMembers := 10
	members, _ := setupJoinedGroup(t, numMembers, groupID, gcs)

	var assignments []AssignmentInfo
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		assignments = append(assignments, AssignmentInfo{
			MemberID:   memberID,
			Assignment: []byte(fmt.Sprintf("assigment-%s", memberID)),
		})
		return true
	})

	var syncResults sync.Map
	wg := sync.WaitGroup{}
	wg.Add(numMembers)

	gc := findNode(groupID, gcs)
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		isLeader := value.(bool)
		var theAssignments []AssignmentInfo
		if isLeader {
			theAssignments = assignments
		}
		gc.SyncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
			syncResults.Store(memberID, syncResult{
				errorCode:  errorCode,
				assignment: assignment,
			})
			wg.Done()
		})
		return true
	})
	wg.Wait()

	for _, expectedAssignment := range assignments {
		r, ok := syncResults.Load(expectedAssignment.MemberID)
		require.True(t, ok)
		res := r.(syncResult)
		require.Equal(t, ErrorCodeNone, res.errorCode)
		require.Equal(t, expectedAssignment.Assignment, res.assignment)
	}
}

func TestJoinNewMemberWhileAwaitingRebalance(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	numMembers := 10
	members, _ := setupJoinedGroup(t, numMembers, groupID, gcs)

	// Call all members into sync except one member which is not the leader, at this point the
	// sync should not complete

	var assignments []AssignmentInfo
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		assignments = append(assignments, AssignmentInfo{
			MemberID:   memberID,
			Assignment: []byte(fmt.Sprintf("assigment-%s", memberID)),
		})
		return true
	})

	gc := findNode(groupID, gcs)
	nonLeadersCount := 0
	chans := make([]chan syncResult, 0, numMembers-1)
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		isLeader := value.(bool)
		var theAssignments []AssignmentInfo
		if isLeader {
			theAssignments = assignments
		} else {
			if nonLeadersCount == numMembers-2 {
				// Skip the final non leader so sync does not complete
				return true
			}
			nonLeadersCount++
		}
		ch := make(chan syncResult, 1)
		chans = append(chans, ch)
		gc.SyncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
			ch <- syncResult{
				errorCode:  errorCode,
				assignment: assignment,
			}
		})
		return true
	})

	// Now join a new member
	expectedMeta := map[string][]byte{}
	var chans2 []chan JoinResult
	i := 0
	protocols := []ProtocolInfo{{defaultProtocolName, []byte(fmt.Sprintf("metadata2-%d", i))}}
	ch := make(chan JoinResult, 1)
	chans2 = append(chans2, ch)
	gc.JoinGroup(0, groupID, defaultClientID, "", defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
		func(result JoinResult) {
			ch <- result
		})

	// This should cause all waiting sync members to get a response with error code rebalance-in-progress and empty
	// assignment
	for _, ch := range chans {
		sr := <-ch
		require.Equal(t, ErrorCodeRebalanceInProgress, sr.errorCode)
		require.Equal(t, 0, len(sr.assignment))
	}

	// Now we rejoin all the original members
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		ch := make(chan JoinResult, 1)
		chans2 = append(chans2, ch)
		protocols := []ProtocolInfo{{defaultProtocolName, []byte(fmt.Sprintf("metadata2-%d", i))}}
		expectedMeta[memberID] = protocols[0].Metadata
		gc.JoinGroup(0, groupID, defaultClientID, memberID, defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
			func(result JoinResult) {
				ch <- result
			})
		i++
		return true
	})

	// And the join should complete successfully
	for _, ch := range chans2 {
		jr := <-ch
		require.Equal(t, ErrorCodeNone, jr.ErrorCode)
		require.Equal(t, defaultProtocolName, jr.ProtocolName)
		// generation should have incremented
		require.Equal(t, 2, jr.GenerationID)
		isLeader := jr.MemberID == jr.LeaderMemberID
		if isLeader {
			require.True(t, len(jr.Members) > 0)
			for _, memberInfo := range jr.Members {
				_, isOldMember := members.Load(memberInfo.MemberID)
				if isOldMember {
					expected, ok := expectedMeta[memberInfo.MemberID]
					require.True(t, ok)
					require.Equal(t, expected, memberInfo.MetaData)
				}
			}
		} else {
			require.Equal(t, 0, len(jr.Members))
		}
	}
}

func TestExistingMembersRejoinWithDifferentProtocolMetadataWhileAwaitingRebalance(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	numMembers := 10
	members, _ := setupJoinedGroup(t, numMembers, groupID, gcs)

	// Call all members into sync except one member which is not the leader, at this point the
	// sync should not complete

	var assignments []AssignmentInfo
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		assignments = append(assignments, AssignmentInfo{
			MemberID:   memberID,
			Assignment: []byte(fmt.Sprintf("assigment-%s", memberID)),
		})
		return true
	})

	gc := findNode(groupID, gcs)
	nonLeadersCount := 0
	chans := make([]chan syncResult, 0, numMembers-1)
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		isLeader := value.(bool)
		var theAssignments []AssignmentInfo
		if isLeader {
			theAssignments = assignments
		} else {
			if nonLeadersCount == numMembers-2 {
				// Skip the final non leader so sync does not complete
				return true
			}
			nonLeadersCount++
		}
		ch := make(chan syncResult, 1)
		chans = append(chans, ch)
		gc.SyncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
			ch <- syncResult{
				errorCode:  errorCode,
				assignment: assignment,
			}
		})
		return true
	})

	// Now we rejoin all the members - the first one should trigger a rebalance as group is still waiting on sync stage
	// and the protocol metadata has changed
	i := 0
	expectedMeta := map[string][]byte{}
	var chans2 []chan JoinResult
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		ch := make(chan JoinResult, 1)
		chans2 = append(chans2, ch)
		protocols := []ProtocolInfo{{defaultProtocolName, []byte(fmt.Sprintf("metadata2-%d", i))}}
		expectedMeta[memberID] = protocols[0].Metadata
		gc.JoinGroup(0, groupID, defaultClientID, memberID, defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
			func(result JoinResult) {
				ch <- result
			})
		return true
	})

	// The sync results should all return re-balance in progress and have empty assignments
	for _, ch := range chans {
		sr := <-ch
		require.Equal(t, ErrorCodeRebalanceInProgress, sr.errorCode)
		require.Equal(t, 0, len(sr.assignment))
	}

	// And the re-joins should complete successfully
	for _, ch := range chans2 {
		jr := <-ch
		require.Equal(t, ErrorCodeNone, jr.ErrorCode)
		require.Equal(t, defaultProtocolName, jr.ProtocolName)
		// generation should have incremented
		require.Equal(t, 2, jr.GenerationID)
		isLeader := jr.MemberID == jr.LeaderMemberID
		if isLeader {
			require.True(t, len(jr.Members) > 0)
			for _, memberInfo := range jr.Members {
				expected, ok := expectedMeta[memberInfo.MemberID]
				require.True(t, ok)
				require.Equal(t, expected, memberInfo.MetaData)
			}
		} else {
			require.Equal(t, 0, len(jr.Members))
		}
	}

}

func TestExistingMembersRejoinWithSameProtocolMetadataWhileAwaitingRebalance(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	numMembers := 10
	members, memberProtocols := setupJoinedGroup(t, numMembers, groupID, gcs)

	// Call all members into sync except one member which is not the leader, at this point the
	// sync should not complete

	var assignments []AssignmentInfo
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		assignments = append(assignments, AssignmentInfo{
			MemberID:   memberID,
			Assignment: []byte(fmt.Sprintf("assignment-%s", memberID)),
		})
		return true
	})

	gc := findNode(groupID, gcs)
	nonLeadersCount := 0
	chans := make([]chan syncResult, 0, numMembers-1)
	var memberIDs []string
	var skippedMember string
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		isLeader := value.(bool)
		var theAssignments []AssignmentInfo
		if isLeader {
			theAssignments = assignments
		} else {
			if nonLeadersCount == numMembers-2 {
				// Skip the final non leader so sync does not complete
				skippedMember = memberID
				return true
			}
			nonLeadersCount++
		}
		ch := make(chan syncResult, 1)
		chans = append(chans, ch)
		memberIDs = append(memberIDs, memberID)
		gc.SyncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
			ch <- syncResult{
				errorCode:  errorCode,
				assignment: assignment,
			}
		})
		return true
	})

	// Now we rejoin all the members with same metadata
	var chans2 []chan JoinResult
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		ch := make(chan JoinResult, 1)
		chans2 = append(chans2, ch)
		p, ok := memberProtocols.Load(memberID)
		require.True(t, ok)
		protocols := p.([]ProtocolInfo)
		gc.JoinGroup(0, groupID, defaultClientID, memberID, defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
			func(result JoinResult) {
				ch <- result
			})
		return true
	})

	// And the joins should return straight-away with the current state as no metadata change
	for _, ch := range chans2 {
		jr := <-ch
		require.Equal(t, ErrorCodeNone, jr.ErrorCode)
		require.Equal(t, defaultProtocolName, jr.ProtocolName)
		// generation should be same
		require.Equal(t, 1, jr.GenerationID)
		isLeader := jr.MemberID == jr.LeaderMemberID
		if isLeader {
			require.True(t, len(jr.Members) > 0)
			for _, memberInfo := range jr.Members {
				p, ok := memberProtocols.Load(memberInfo.MemberID)
				require.True(t, ok)
				expectedProtocols := p.([]ProtocolInfo)
				expected := expectedProtocols[0].Metadata
				require.True(t, ok)
				require.Equal(t, expected, memberInfo.MetaData)
			}
		} else {
			require.Equal(t, 0, len(jr.Members))
		}
	}

	// Now sync the last remaining member
	ch := make(chan syncResult, 1)
	chans = append(chans, ch)
	memberIDs = append(memberIDs, skippedMember)
	gc.SyncGroup(groupID, skippedMember, 1, nil, func(errorCode int, assignment []byte) {
		ch <- syncResult{
			errorCode:  errorCode,
			assignment: assignment,
		}
	})

	assignmentMap := map[string][]byte{}
	for _, assignment := range assignments {
		assignmentMap[assignment.MemberID] = assignment.Assignment
	}

	// The sync results should all return ok
	for i, ch := range chans {
		memberID := memberIDs[i]
		sr := <-ch
		require.Equal(t, ErrorCodeNone, sr.errorCode)
		require.Equal(t, assignmentMap[memberID], sr.assignment)
	}

}

func TestJoinNewMemberWhileActive(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	numMembers := 10
	members, _ := setupJoinedGroup(t, numMembers, groupID, gcs)

	// Sync all
	var assignments []AssignmentInfo
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		assignments = append(assignments, AssignmentInfo{
			MemberID:   memberID,
			Assignment: []byte(fmt.Sprintf("assigment-%s", memberID)),
		})
		return true
	})

	gc := findNode(groupID, gcs)
	chans := make([]chan syncResult, 0, numMembers-1)
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		isLeader := value.(bool)
		var theAssignments []AssignmentInfo
		if isLeader {
			theAssignments = assignments
		}
		ch := make(chan syncResult, 1)
		chans = append(chans, ch)
		gc.SyncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
			ch <- syncResult{
				errorCode:  errorCode,
				assignment: assignment,
			}
		})
		return true
	})

	// The sync results should all return ok
	for _, ch := range chans {
		sr := <-ch
		require.Equal(t, ErrorCodeNone, sr.errorCode)
	}

	// Now join a new member
	expectedMeta := map[string][]byte{}
	var chans2 []chan JoinResult
	i := 0
	protocols := []ProtocolInfo{{defaultProtocolName, []byte(fmt.Sprintf("metadata2-%d", i))}}
	ch := make(chan JoinResult, 1)
	chans2 = append(chans2, ch)
	gc.JoinGroup(0, groupID, defaultClientID, "", defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
		func(result JoinResult) {
			ch <- result
		})

	// Now we rejoin all the original members
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		ch := make(chan JoinResult, 1)
		chans2 = append(chans2, ch)
		protocols := []ProtocolInfo{{defaultProtocolName, []byte(fmt.Sprintf("metadata2-%d", i))}}
		expectedMeta[memberID] = protocols[0].Metadata
		gc.JoinGroup(0, groupID, defaultClientID, memberID, defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
			func(result JoinResult) {
				ch <- result
			})
		i++
		return true
	})

	// And the join should complete successfully
	for _, ch := range chans2 {
		jr := <-ch
		require.Equal(t, ErrorCodeNone, jr.ErrorCode)
		require.Equal(t, defaultProtocolName, jr.ProtocolName)
		// generation should have incremented
		require.Equal(t, 2, jr.GenerationID)
		isLeader := jr.MemberID == jr.LeaderMemberID
		if isLeader {
			require.True(t, len(jr.Members) > 0)
			for _, memberInfo := range jr.Members {
				_, originalMember := members.Load(memberInfo.MemberID)
				if originalMember {
					expected, ok := expectedMeta[memberInfo.MemberID]
					require.True(t, ok)
					require.Equal(t, expected, memberInfo.MetaData)
				}
			}
		} else {
			require.Equal(t, 0, len(jr.Members))
		}
	}
}

func TestRejoinLeaderWhileActive(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	numMembers := 10
	members, _ := setupJoinedGroup(t, numMembers, groupID, gcs)

	// Sync all
	var assignments []AssignmentInfo
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		assignments = append(assignments, AssignmentInfo{
			MemberID:   memberID,
			Assignment: []byte(fmt.Sprintf("assigment-%s", memberID)),
		})
		return true
	})

	gc := findNode(groupID, gcs)
	chans := make([]chan syncResult, 0, numMembers-1)
	var leader string
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		isLeader := value.(bool)
		var theAssignments []AssignmentInfo
		if isLeader {
			theAssignments = assignments
			leader = memberID
		}
		ch := make(chan syncResult, 1)
		chans = append(chans, ch)
		gc.SyncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
			ch <- syncResult{
				errorCode:  errorCode,
				assignment: assignment,
			}
		})
		return true
	})

	// The sync results should all return ok
	for _, ch := range chans {
		sr := <-ch
		require.Equal(t, ErrorCodeNone, sr.errorCode)
	}

	// Now rejoin the leader

	// Now join a new member
	expectedMeta := map[string][]byte{}
	var chans2 []chan JoinResult
	i := 0
	protocols := []ProtocolInfo{{defaultProtocolName, []byte(fmt.Sprintf("metadata2-%d", i))}}
	ch := make(chan JoinResult, 1)
	chans2 = append(chans2, ch)
	expectedMeta[leader] = protocols[0].Metadata
	gc.JoinGroup(0, groupID, defaultClientID, leader, defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
		func(result JoinResult) {
			ch <- result
		})

	// This should trigger a rebalance
	errorCode := gc.HeartbeatGroup(groupID, leader, 1)
	require.Equal(t, ErrorCodeRebalanceInProgress, errorCode)

	// Now we rejoin all the others members
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		if memberID == leader {
			return true
		}
		ch := make(chan JoinResult, 1)
		chans2 = append(chans2, ch)
		protocols := []ProtocolInfo{{defaultProtocolName, []byte(fmt.Sprintf("metadata2-%d", i))}}
		expectedMeta[memberID] = protocols[0].Metadata
		gc.JoinGroup(0, groupID, defaultClientID, memberID, defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
			func(result JoinResult) {
				ch <- result
			})
		i++
		return true
	})

	// And the join should complete successfully
	for _, ch := range chans2 {
		jr := <-ch
		require.Equal(t, ErrorCodeNone, jr.ErrorCode)
		require.Equal(t, defaultProtocolName, jr.ProtocolName)
		// generation should have incremented
		require.Equal(t, 2, jr.GenerationID)
		isLeader := jr.MemberID == jr.LeaderMemberID
		if isLeader {
			require.True(t, len(jr.Members) > 0)
			for _, memberInfo := range jr.Members {
				expected, ok := expectedMeta[memberInfo.MemberID]
				require.True(t, ok)
				require.Equal(t, expected, memberInfo.MetaData)
			}
		} else {
			require.Equal(t, 0, len(jr.Members))
		}
	}

}

func TestRejoinNonLeaderWhileActive(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	numMembers := 10
	members, memberProtocols := setupJoinedGroup(t, numMembers, groupID, gcs)

	// Sync all
	var assignments []AssignmentInfo
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		assignments = append(assignments, AssignmentInfo{
			MemberID:   memberID,
			Assignment: []byte(fmt.Sprintf("assigment-%s", memberID)),
		})
		return true
	})

	gc := findNode(groupID, gcs)
	chans := make([]chan syncResult, 0, numMembers-1)
	var leader string
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		isLeader := value.(bool)
		var theAssignments []AssignmentInfo
		if isLeader {
			theAssignments = assignments
			leader = memberID
		}
		ch := make(chan syncResult, 1)
		chans = append(chans, ch)
		gc.SyncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
			ch <- syncResult{
				errorCode:  errorCode,
				assignment: assignment,
			}
		})
		return true
	})

	// The sync results should all return ok
	for _, ch := range chans {
		sr := <-ch
		require.Equal(t, ErrorCodeNone, sr.errorCode)
	}

	// Now rejoin the non leaders - should just return current state

	expectedMeta := map[string][]byte{}
	var chans2 []chan JoinResult

	i := 0
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		if memberID == leader {
			return true
		}
		ch := make(chan JoinResult, 1)
		chans2 = append(chans2, ch)
		p, ok := memberProtocols.Load(memberID)
		require.True(t, ok)
		protocols := p.([]ProtocolInfo)
		expectedMeta[memberID] = protocols[0].Metadata
		gc.JoinGroup(0, groupID, defaultClientID, memberID, defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
			func(result JoinResult) {
				ch <- result
			})
		i++
		return true
	})

	// And the join should complete successfully
	for _, ch := range chans2 {
		jr := <-ch
		require.Equal(t, ErrorCodeNone, jr.ErrorCode)
		require.Equal(t, defaultProtocolName, jr.ProtocolName)
		require.Equal(t, 1, jr.GenerationID)
		isLeader := jr.MemberID == jr.LeaderMemberID
		if isLeader {
			require.True(t, len(jr.Members) > 0)
			for _, memberInfo := range jr.Members {
				expected, ok := expectedMeta[memberInfo.MemberID]
				require.True(t, ok)
				require.Equal(t, expected, memberInfo.MetaData)
			}
		} else {
			require.Equal(t, 0, len(jr.Members))
		}
	}

}

func TestSyncInJoinPhaseFails(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	gc := findNode(groupID, gcs)

	protocols := []ProtocolInfo{
		{defaultProtocolName, []byte("protocol1_bytes")},
	}
	gc.JoinGroup(0, groupID, defaultClientID, "", defaultProtocolType, protocols,
		defaultSessionTimeout, defaultRebalanceTimeout, func(result JoinResult) {
		})

	// The group will now be in state statePreRebalance

	ch := make(chan int, 1)
	gc.SyncGroup(groupID, "some-member-id", 0, nil, func(errorCode int, assignment []byte) {
		ch <- errorCode
	})
	errorCode := <-ch
	require.Equal(t, ErrorCodeRebalanceInProgress, errorCode)
}

func TestSyncWhenActiveReturnsCurrentState(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	gc := findNode(groupID, gcs)

	numMembers := 10
	members, _ := setupJoinedGroup(t, numMembers, groupID, gcs)
	assignments := syncGroup(groupID, numMembers, members, gcs)
	assignmentMap := map[string][]byte{}
	for _, assignment := range assignments {
		assignmentMap[assignment.MemberID] = assignment.Assignment
	}

	// Now everything is synced

	// Sync with unknown member id
	ch := make(chan int, 1)
	gc.SyncGroup(groupID, "unknown", 1, nil, func(errorCode int, assignment []byte) {
		ch <- errorCode
	})
	errorCode := <-ch
	require.Equal(t, ErrorCodeUnknownMemberID, errorCode)

	// Sync with actual members - should just return current state
	members.Range(func(key, value any) bool {
		memberID := key.(string)

		ch := make(chan syncResult, 1)
		gc.SyncGroup(groupID, memberID, 1, nil, func(errorCode int, assignment []byte) {
			ch <- syncResult{
				errorCode:  errorCode,
				assignment: assignment,
			}
		})
		res := <-ch
		require.Equal(t, ErrorCodeNone, res.errorCode)
		assignment, ok := assignmentMap[memberID]
		require.True(t, ok)
		require.Equal(t, assignment, res.assignment)

		return true
	})

}

func setupJoinedGroup(t *testing.T, numMembers int, groupID string, gcs []*GroupCoordinator) (*sync.Map, *sync.Map) {
	return setupJoinedGroupWithArgs(t, numMembers, groupID, gcs, defaultRebalanceTimeout)
}

func setupJoinedGroupWithArgs(t *testing.T, numMembers int, groupID string, gcs []*GroupCoordinator, rebalanceTimeout time.Duration) (*sync.Map, *sync.Map) {
	gc := findNode(groupID, gcs)
	wg := sync.WaitGroup{}
	wg.Add(numMembers)
	members := sync.Map{}
	memberProtocols := sync.Map{}
	for i := 0; i < numMembers; i++ {
		protocols := []ProtocolInfo{{defaultProtocolName, []byte(fmt.Sprintf("metadata-%d", i))}}
		gc.JoinGroup(4, groupID, defaultClientID, "", defaultProtocolType, protocols, defaultSessionTimeout, rebalanceTimeout, func(result JoinResult) {
			require.Equal(t, ErrorCodeUnknownMemberID, result.ErrorCode)
			go func() {
				gc.JoinGroup(0, groupID, defaultClientID, result.MemberID, defaultProtocolType, protocols, defaultSessionTimeout, rebalanceTimeout, func(result JoinResult) {
					if result.ErrorCode != ErrorCodeNone {
						panic(fmt.Sprintf("join returned error %d", result.ErrorCode))
					}
					isLeader := result.LeaderMemberID == result.MemberID
					members.Store(result.MemberID, isLeader)
					memberProtocols.Store(result.MemberID, protocols)
					wg.Done()
				})
			}()
		})
	}
	wg.Wait()
	return &members, &memberProtocols
}

type syncResult struct {
	errorCode  int
	assignment []byte
}

func syncGroup(groupID string, numMembers int, members *sync.Map, gcs []*GroupCoordinator) []AssignmentInfo {
	var assignments []AssignmentInfo
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		assignments = append(assignments, AssignmentInfo{
			MemberID:   memberID,
			Assignment: []byte(fmt.Sprintf("assigment-%s", memberID)),
		})
		return true
	})

	wg := sync.WaitGroup{}
	wg.Add(numMembers)

	gc := findNode(groupID, gcs)
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		isLeader := value.(bool)
		var theAssignments []AssignmentInfo
		if isLeader {
			theAssignments = assignments
		}
		gc.SyncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
			if errorCode != ErrorCodeNone {
				panic(fmt.Sprintf("sync returned error %d", errorCode))
			}
			wg.Done()
		})
		return true
	})
	wg.Wait()
	return assignments
}

func TestSyncWrongGeneration(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	gc := findNode(groupID, gcs)
	members, _ := setupJoinedGroup(t, 1, groupID, gcs)
	var memberID string
	members.Range(func(key, value any) bool {
		memberID = key.(string)
		return true
	})

	ch := make(chan int, 1)
	gc.SyncGroup(groupID, memberID, 23, []AssignmentInfo{}, func(errorCode int, assignment []byte) {
		ch <- errorCode
	})
	err := <-ch
	require.Equal(t, ErrorCodeIllegalGeneration, err)
}

func TestAddNewMembersAfterSync(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()

	numInitialMembers := 10
	members, memberProtocols := setupJoinedGroup(t, numInitialMembers, groupID, gcs)
	syncGroup(groupID, numInitialMembers, members, gcs)

	// Add another member
	gc := findNode(groupID, gcs)

	numNewMembers := 10
	newMembersMap := sync.Map{}
	chans := make([]chan JoinResult, 0, numInitialMembers+numNewMembers)
	for i := 0; i < numNewMembers; i++ {
		ch := make(chan JoinResult, 1)
		chans = append(chans, ch)
		protocols := []ProtocolInfo{{defaultProtocolName, []byte(fmt.Sprintf("metadata-%d", i+numInitialMembers))}}
		gc.JoinGroup(0, groupID, defaultClientID, "", "protocoltype1", protocols, defaultSessionTimeout, defaultRebalanceTimeout, func(result JoinResult) {
			go func() {
				// First should trigger a rebalance
				gc.JoinGroup(0, groupID, defaultClientID, result.MemberID, "protocoltype1", protocols, defaultSessionTimeout, defaultRebalanceTimeout, func(result JoinResult) {
					newMembersMap.Store(result.MemberID, struct{}{})
					memberProtocols.Store(result.MemberID, protocols)
					ch <- result
				})
			}()
		})
	}

	var leader string
	cnt := 0
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		isLeader := value.(bool)
		if isLeader {
			leader = memberID
		}

		// Now rejoin the original members
		ch := make(chan JoinResult, 1)
		chans = append(chans, ch)
		o, ok := memberProtocols.Load(memberID)
		require.True(t, ok)
		protocols := o.([]ProtocolInfo)
		gc.JoinGroup(0, groupID, defaultClientID, memberID, "protocoltype1", protocols, defaultSessionTimeout, defaultRebalanceTimeout, func(result JoinResult) {
			ch <- result
		})
		cnt++
		return true
	})

	// This should make all the joins return
	for _, ch := range chans {
		jr := <-ch
		require.Equal(t, ErrorCodeNone, jr.ErrorCode)
		require.Equal(t, leader, jr.LeaderMemberID)
		_, ok := members.Load(jr.MemberID)
		if !ok {
			_, ok := newMembersMap.Load(jr.MemberID)
			require.True(t, ok)
		}
		if jr.MemberID == leader {
			require.True(t, len(jr.Members) > 0)
			for _, member := range jr.Members {
				e, ok := memberProtocols.Load(member.MemberID)
				require.True(t, ok)
				expectedProts := e.([]ProtocolInfo)
				require.Equal(t, expectedProts[0].Metadata, member.MetaData)
			}
		} else {
			require.Equal(t, 0, len(jr.Members))
		}
		require.Equal(t, 2, jr.GenerationID)
		require.Equal(t, defaultProtocolName, jr.ProtocolName)
	}

}

func TestSyncNotController(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	for i := 0; i < 10; i++ {
		groupID := uuid.New().String()
		coordinator := findNode(groupID, gcs)
		for _, gc := range gcs {
			if gc != coordinator {

				ch := make(chan int, 1)
				gc.SyncGroup(groupID, "", 1, nil, func(errorCode int, assignment []byte) {
					ch <- errorCode
				})
				errorCode := <-ch
				require.Equal(t, ErrorCodeNotCoordinator, errorCode)
			}
		}
	}
}

func TestSyncEmptyMemberID(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	coordinator := findNode(groupID, gcs)

	ch := make(chan int, 1)
	coordinator.SyncGroup(groupID, "", 1, nil, func(errorCode int, assignment []byte) {
		ch <- errorCode
	})
	errorCode := <-ch
	require.Equal(t, ErrorCodeUnknownMemberID, errorCode)
}

func TestSyncUnknownGroupID(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	coordinator := findNode(groupID, gcs)

	ch := make(chan int, 1)
	coordinator.SyncGroup(groupID, "foo", 1, nil, func(errorCode int, assignment []byte) {
		ch <- errorCode
	})
	errorCode := <-ch
	require.Equal(t, ErrorCodeGroupIDNotFound, errorCode)
}

func TestHeartbeatNonCoordinator(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	for i := 0; i < 10; i++ {
		groupID := uuid.New().String()
		coordinator := findNode(groupID, gcs)
		for _, gc := range gcs {
			if gc != coordinator {
				errorCode := gc.HeartbeatGroup(groupID, "foo", 1)
				require.Equal(t, ErrorCodeNotCoordinator, errorCode)
			}
		}
	}
}

func TestHeartbeatEmptyMemberID(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	coordinator := findNode(groupID, gcs)
	errorCode := coordinator.HeartbeatGroup(groupID, "", 1)
	require.Equal(t, ErrorCodeUnknownMemberID, errorCode)
}

func TestHeartbeatUnknownGroupID(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	coordinator := findNode(groupID, gcs)
	errorCode := coordinator.HeartbeatGroup(groupID, "foo", 1)
	require.Equal(t, ErrorCodeGroupIDNotFound, errorCode)
}

func TestHeartbeatIllegalGeneration(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	protocolMetadata := []byte("protocol1_bytes")
	protocols := []ProtocolInfo{
		{defaultProtocolName, protocolMetadata},
	}
	groupID := uuid.New().String()
	gc := findNode(groupID, gcs)
	ch := make(chan JoinResult, 1)
	gc.JoinGroup(0, groupID, defaultClientID, "", defaultProtocolType, protocols, defaultSessionTimeout,
		defaultRebalanceTimeout, func(result JoinResult) {
			ch <- result
		})
	res := <-ch

	// Group should now be in state stateAwaitingRebalance - waiting for initial timeout before completing join
	require.Equal(t, stateAwaitingRebalance, gc.getState(groupID))

	errorCode := gc.HeartbeatGroup(groupID, res.MemberID, 100)
	require.Equal(t, ErrorCodeIllegalGeneration, errorCode)
}

func TestHeartbeatAwaitingRebalance(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	members, _ := setupJoinedGroup(t, 10, groupID, gcs)
	gc := findNode(groupID, gcs)
	var memberID string
	members.Range(func(key, value any) bool {
		memberID = key.(string)
		return false
	})

	// Group should now be in state stateAwaitingRebalance
	require.Equal(t, stateAwaitingRebalance, gc.getState(groupID))

	errorCode := gc.HeartbeatGroup(groupID, memberID, 1)
	require.Equal(t, ErrorCodeNone, errorCode)
}

func TestHeartbeatWhileActive(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	numMembers := 10
	members, _ := setupJoinedGroup(t, numMembers, groupID, gcs)
	syncGroup(groupID, numMembers, members, gcs)

	// Now group should be in active state

	gc := findNode(groupID, gcs)
	var memberID string
	members.Range(func(key, value any) bool {
		memberID = key.(string)
		return false
	})

	// Group should now be in state stateAwaitingRebalance
	require.Equal(t, stateActive, gc.getState(groupID))

	errorCode := gc.HeartbeatGroup(groupID, memberID, 1)
	require.Equal(t, ErrorCodeNone, errorCode)
}

func TestJoinTimeoutMembersRemovedAndJoinCompletes(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	gc := findNode(groupID, gcs)
	numMembers := 10
	rebalanceTimeout := 1 * time.Second
	members, memberProts := setupJoinedGroupWithArgs(t, numMembers, groupID, gcs, rebalanceTimeout)
	syncGroup(groupID, numMembers, members, gcs)
	require.Equal(t, stateActive, gc.getState(groupID))

	// Add a new member to prompt a rebalance
	var chans []chan JoinResult
	protocols := []ProtocolInfo{{defaultProtocolName, []byte("metadata-11")}}
	ch := make(chan JoinResult, 1)
	chans = append(chans, ch)
	gc.JoinGroup(0, groupID, defaultClientID, "", defaultProtocolType, protocols, defaultSessionTimeout,
		rebalanceTimeout, func(result JoinResult) {
			ch <- result
		})

	// Now we're going to rejoin all members, apart from two non leaders
	start := time.Now()
	skippedCount := 0
	skippedMembers := map[string]struct{}{}
	var leader string
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		isLeader := value.(bool)
		if isLeader {
			leader = memberID
		}
		if !isLeader && skippedCount < 2 {
			skippedCount++
			skippedMembers[memberID] = struct{}{}
			return true
		}
		p, ok := memberProts.Load(memberID)
		require.True(t, ok)
		protocols := p.([]ProtocolInfo)
		ch := make(chan JoinResult, 1)
		chans = append(chans, ch)
		gc.JoinGroup(0, groupID, defaultClientID, memberID, defaultProtocolType, protocols, defaultSessionTimeout,
			rebalanceTimeout, func(result JoinResult) {
				ch <- result
			})
		return true
	})

	// The group should rejoin, without the members that did not rejoin, and with the new member
	for _, ch := range chans {
		res := <-ch
		delay := time.Now().Sub(start)
		// The re-join timeout is the rebalance timeout
		require.True(t, delay > rebalanceTimeout)
		require.Equal(t, ErrorCodeNone, res.ErrorCode)
		require.Equal(t, 2, res.GenerationID)
		_, ok := skippedMembers[res.MemberID]
		require.False(t, ok)
		// leader should be the same
		require.Equal(t, leader, res.LeaderMemberID)

		if res.MemberID == res.LeaderMemberID {
			require.Equal(t, 1+numMembers-2, len(res.Members))
			for _, member := range res.Members {
				_, isOriginalMember := members.Load(member.MemberID)
				if isOriginalMember {
					o, ok := memberProts.Load(member.MemberID)
					require.True(t, ok)
					require.Equal(t, o.([]ProtocolInfo)[0].Metadata, member.MetaData)
					_, ok = skippedMembers[member.MemberID]
					require.False(t, ok)
				}
			}
		} else {
			require.Equal(t, 0, len(res.Members))
		}
	}
}

func TestJoinTimeoutMembersRemovedIncludingLeaderAndJoinCompletes(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	gc := findNode(groupID, gcs)
	numMembers := 10
	rebalanceTimeout := 1 * time.Second
	members, memberProts := setupJoinedGroupWithArgs(t, numMembers, groupID, gcs, rebalanceTimeout)
	syncGroup(groupID, numMembers, members, gcs)
	require.Equal(t, stateActive, gc.getState(groupID))

	// Add a new member to prompt a rebalance
	var chans []chan JoinResult
	protocols := []ProtocolInfo{{defaultProtocolName, []byte("metadata-11")}}
	ch := make(chan JoinResult, 1)
	chans = append(chans, ch)

	gc.JoinGroup(0, groupID, defaultClientID, "", defaultProtocolType, protocols, defaultSessionTimeout,
		rebalanceTimeout, func(result JoinResult) {
			ch <- result
		})

	// Now we're going to rejoin all members, excecpt two including the leader
	start := time.Now()
	skipped := false
	skippedMembers := map[string]struct{}{}
	var leader string
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		isLeader := value.(bool)
		if isLeader {
			leader = memberID
			skippedMembers[memberID] = struct{}{}
			return true
		}
		if !skipped {
			skipped = true
			skippedMembers[memberID] = struct{}{}
			return true
		}
		p, ok := memberProts.Load(memberID)
		require.True(t, ok)
		protocols := p.([]ProtocolInfo)
		ch := make(chan JoinResult, 1)
		chans = append(chans, ch)
		gc.JoinGroup(0, groupID, defaultClientID, memberID, defaultProtocolType, protocols, defaultSessionTimeout,
			rebalanceTimeout, func(result JoinResult) {
				ch <- result
			})
		return true
	})

	// The group should rejoin, without the members that did not rejoin, and with a new leader
	for _, ch := range chans {
		res := <-ch
		delay := time.Now().Sub(start)
		// The re-join timeout is the rebalance timeout
		require.True(t, delay > rebalanceTimeout)
		require.Equal(t, ErrorCodeNone, res.ErrorCode)
		require.Equal(t, 2, res.GenerationID)
		_, ok := skippedMembers[res.MemberID]
		require.False(t, ok)
		// leader should be different
		require.NotEqual(t, leader, res.LeaderMemberID)

		if res.MemberID == res.LeaderMemberID {
			require.Equal(t, 1+numMembers-2, len(res.Members))
			for _, member := range res.Members {
				_, isOriginalMember := members.Load(member.MemberID)
				if isOriginalMember {
					o, ok := memberProts.Load(member.MemberID)
					require.True(t, ok)
					require.Equal(t, o.([]ProtocolInfo)[0].Metadata, member.MetaData)
					_, ok = skippedMembers[member.MemberID]
					require.False(t, ok)
				}
			}
		} else {
			require.Equal(t, 0, len(res.Members))
		}
	}

}

func TestJoinTimeoutNoMembersRejoinTransitionsToEmpty(t *testing.T) {
	newMemberJoinTimeout := 500 * time.Millisecond
	gcs := createCoordinatorsWithCfgSetter(t, 3, func(config *conf.Config) {
		config.KafkaInitialJoinDelay = 100 * time.Millisecond
		config.KafkaMinSessionTimeout = 1 * time.Millisecond
		config.KafkaNewMemberJoinTimeout = newMemberJoinTimeout
	})
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	gc := findNode(groupID, gcs)
	numMembers := 10
	rebalanceTimeout := 1 * time.Second
	members, _ := setupJoinedGroupWithArgs(t, numMembers, groupID, gcs, rebalanceTimeout)
	syncGroup(groupID, numMembers, members, gcs)
	require.Equal(t, stateActive, gc.getState(groupID))

	// Add a new member to prompt a rebalance
	var chans []chan JoinResult
	protocols := []ProtocolInfo{{defaultProtocolName, []byte("metadata-11")}}
	ch := make(chan JoinResult, 1)
	chans = append(chans, ch)
	gc.JoinGroup(0, groupID, defaultClientID, "", defaultProtocolType, protocols, defaultSessionTimeout,
		rebalanceTimeout, func(result JoinResult) {
			ch <- result
		})
	require.Equal(t, statePreRebalance, gc.getState(groupID))

	// Now wait until that new member's session expires
	time.Sleep(2 * newMemberJoinTimeout)

	// And wait for join timeout
	time.Sleep(rebalanceTimeout)

	// At this point there should be only the old members in the group, and none have rejoined, so the join timeout
	// should remove them all leaving an empty group
	require.Equal(t, stateEmpty, gc.getState(groupID))
}

func TestRejoinLeaderTriggersRebalance(t *testing.T) {
	gcs := createCoordinators(t, defaultInitialJoinDelay, 3)
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	gc := findNode(groupID, gcs)
	numMembers := 10
	rebalanceTimeout := 1 * time.Second
	members, memberProts := setupJoinedGroupWithArgs(t, numMembers, groupID, gcs, rebalanceTimeout)
	syncGroup(groupID, numMembers, members, gcs)
	require.Equal(t, stateActive, gc.getState(groupID))

	var leader string
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		isLeader := value.(bool)
		if isLeader {
			leader = memberID
			return false
		}
		return true
	})
	p, ok := memberProts.Load(leader)
	require.True(t, ok)
	gc.JoinGroup(0, groupID, defaultClientID, leader, defaultProtocolType, p.([]ProtocolInfo), defaultSessionTimeout,
		rebalanceTimeout, func(result JoinResult) {})
	require.Equal(t, statePreRebalance, gc.getState(groupID))
}

func addMemberWithSessionTimeout(gc *GroupCoordinator, groupID string, sessionTimeout time.Duration) chan JoinResult {
	protocols := []ProtocolInfo{{defaultProtocolName, []byte("foo")}}
	ch := make(chan JoinResult, 1)
	gc.JoinGroup(0, groupID, defaultClientID, "", defaultProtocolType, protocols, sessionTimeout,
		defaultRebalanceTimeout, func(result JoinResult) {
			ch <- result
		})
	return ch
}

func TestSessionTimeoutWhenActive(t *testing.T) {
	gcs := createCoordinatorsWithCfgSetter(t, 3, func(config *conf.Config) {
		config.KafkaInitialJoinDelay = 100 * time.Millisecond
		config.KafkaMinSessionTimeout = 1 * time.Millisecond
	})
	defer stopCoordinators(t, gcs)

	groupID := uuid.New().String()
	gc := findNode(groupID, gcs)

	var results []JoinResult

	chan1 := addMemberWithSessionTimeout(gc, groupID, 100*time.Millisecond)
	chan2 := addMemberWithSessionTimeout(gc, groupID, 500*time.Millisecond)
	chan3 := addMemberWithSessionTimeout(gc, groupID, 1*time.Second)

	res1 := <-chan1
	res2 := <-chan2
	res3 := <-chan3

	results = append(results, res1)
	results = append(results, res2)
	results = append(results, res3)
	member1 := res1.MemberID
	member2 := res2.MemberID
	member3 := res3.MemberID

	var members sync.Map
	for _, res := range results {
		require.Equal(t, ErrorCodeNone, res.ErrorCode)
		isLeader := res.LeaderMemberID == res.MemberID
		members.Store(res.MemberID, isLeader)
	}
	require.Equal(t, stateAwaitingRebalance, gc.getState(groupID))
	syncGroup(groupID, 3, &members, gcs)
	require.Equal(t, stateActive, gc.getState(groupID))
	require.True(t, gc.groupHasMember(groupID, member1))
	require.True(t, gc.groupHasMember(groupID, member2))
	require.True(t, gc.groupHasMember(groupID, member3))

	time.Sleep(300 * time.Millisecond)
	require.False(t, gc.groupHasMember(groupID, member1))
	require.True(t, gc.groupHasMember(groupID, member2))
	require.True(t, gc.groupHasMember(groupID, member3))

	time.Sleep(300 * time.Millisecond)
	require.False(t, gc.groupHasMember(groupID, member1))
	require.False(t, gc.groupHasMember(groupID, member2))
	require.True(t, gc.groupHasMember(groupID, member3))

	time.Sleep(500 * time.Millisecond)
	require.False(t, gc.groupHasMember(groupID, member1))
	require.False(t, gc.groupHasMember(groupID, member2))
	require.False(t, gc.groupHasMember(groupID, member3))

	require.Equal(t, stateEmpty, gc.getState(groupID))
}

func findNode(groupID string, gcs []*GroupCoordinator) *GroupCoordinator {
	nodeID := rand.Intn(len(gcs))
	leader := gcs[nodeID].FindCoordinator(groupID)
	return gcs[leader]
}

func callJoinGroupSync(gc *GroupCoordinator, groupID string, clientID string, memberID string, protocolType string, protocols []ProtocolInfo, sessionTimeout time.Duration,
	rebalanceTimeout time.Duration) JoinResult {
	return callJoinGroupSyncWithApiVersion(gc, groupID, clientID, memberID, protocolType, protocols, sessionTimeout, rebalanceTimeout,
		0)
}

func callJoinGroupSyncWithApiVersion(gc *GroupCoordinator, groupID string, clientID string, memberID string, protocolType string, protocols []ProtocolInfo, sessionTimeout time.Duration,
	rebalanceTimeout time.Duration, apiVersion int16) JoinResult {
	ch := make(chan JoinResult, 1)
	gc.JoinGroup(apiVersion, groupID, clientID, memberID, protocolType, protocols, sessionTimeout, rebalanceTimeout, func(result JoinResult) {
		ch <- result
	})
	res := <-ch
	return res
}

type testConsumerInfoProvider struct {
	slabID int
}

func (t *testConsumerInfoProvider) SlabID() int {
	return t.slabID
}

func (t *testConsumerInfoProvider) EarliestOffset(int) (int64, int64, bool) {
	return 0, 0, false
}

func (t *testConsumerInfoProvider) LatestOffset(int) (int64, int64, bool, error) {
	return 0, 0, false, nil
}

func (t *testConsumerInfoProvider) OffsetByTimestamp(types.Timestamp, int) (int64, int64, bool) {
	return 0, 0, false
}

func createCoordinators(t *testing.T, initialJoinDelay time.Duration, numNodes int) []*GroupCoordinator {
	return createCoordinatorsWithCfgSetter(t, numNodes, func(config *conf.Config) {
		config.KafkaInitialJoinDelay = initialJoinDelay
	})
}

func createCoordinatorsWithCfgSetter(t *testing.T, numNodes int, cfgSetter func(config *conf.Config)) []*GroupCoordinator {
	procProvider := &testProcessorProvider{
		partitionNodeMap: map[int]int{
			0: 0,
			1: 1,
			2: 2,
			3: 0,
			4: 1,
			5: 2,
			6: 0,
			7: 1,
			8: 2,
		},
	}
	metaProvider := &testMetadataProvider{}
	forwarder := &testBatchForwarder{}
	streamMgr := &testStreamMgr{}
	gcs := make([]*GroupCoordinator, numNodes)
	for i := 0; i < numNodes; i++ {
		cfg := &conf.Config{}
		cfg.ApplyDefaults()
		cfg.NodeID = i
		if cfgSetter != nil {
			cfgSetter(cfg)
		}
		var err error
		gcs[i], err = NewGroupCoordinator(cfg, procProvider, streamMgr, metaProvider, nil, forwarder)
		require.NoError(t, err)
		err = gcs[i].Start()
		require.NoError(t, err)
	}
	return gcs
}

func stopCoordinators(t *testing.T, gcs []*GroupCoordinator) {
	for _, gc := range gcs {
		err := gc.Stop()
		require.NoError(t, err)
	}
}
