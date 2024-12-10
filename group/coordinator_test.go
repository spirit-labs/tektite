package group

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/acls"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"math"
	"sort"
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
	localTransports := transport.NewLocalTransports()
	gc, controlClient, _, _ := createCoordinatorWithConnFactoryAndCfgSetter(t, localTransports.CreateConnection, nil)
	defer stopCoordinator(t, gc)
	memberID := int32(333)
	controlClient.groupCoordinatorMemberID = memberID
	controlClient.groupCoordinatorAddress = "someaddress:777"
	groupID := uuid.New().String()
	req := kafkaprotocol.FindCoordinatorRequest{
		Key: common.StrPtr(groupID),
	}
	respCh := make(chan *kafkaprotocol.FindCoordinatorResponse, 1)
	err := gc.HandleFindCoordinatorRequest(&req, func(resp *kafkaprotocol.FindCoordinatorResponse) error {
		respCh <- resp
		return nil
	})
	require.NoError(t, err)
	resp := <-respCh
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.ErrorCode))
	require.Equal(t, memberID, resp.NodeId)
	require.Equal(t, "someaddress", *resp.Host)
	require.Equal(t, 777, int(resp.Port))
}

func TestInitialJoinNoMemberID(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)
	protocolMetadata := []byte("protocol1_bytes")
	protocols := []ProtocolInfo{
		{defaultProtocolName, protocolMetadata},
	}
	start := time.Now()
	groupID := uuid.New().String()
	clientID := uuid.New().String()
	// Only get this behaviour with API version >= 4
	res := callJoinGroupSyncWithApiVersion(gc, groupID, clientID, "", defaultProtocolType,
		protocols, defaultSessionTimeout, defaultRebalanceTimeout, 4)
	require.True(t, time.Now().Sub(start) < defaultInitialJoinDelay) // Should be no delay
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownMemberID, res.ErrorCode)
	require.True(t, strings.HasPrefix(res.MemberID, clientID))
	require.Equal(t, 37, len(res.MemberID)-len(clientID))
	require.Equal(t, "", res.LeaderMemberID)
	require.Equal(t, "", res.ProtocolName)
	require.Equal(t, 0, res.GenerationID)
	require.Equal(t, 0, len(res.Members))
}

func TestInitialMemberJoinAfterDelay(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	protocolMetadata := []byte("protocol1_bytes")
	protocols := []ProtocolInfo{
		{defaultProtocolName, protocolMetadata},
	}
	groupID := uuid.New().String()
	clientID := uuid.New().String()
	start := time.Now()
	res := callJoinGroupSync(gc, groupID, clientID, "", defaultProtocolType,
		protocols, defaultSessionTimeout, defaultRebalanceTimeout)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, res.ErrorCode)
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
	gc, _, _, _ := createCoordinatorWithCfgSetter(t, func(cfg *Conf) {
		cfg.InitialJoinDelay = initialJoinDelay
	})
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	var memberMetaDataMap sync.Map

	numMembers := 10
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
		gc.joinGroup(0, groupID, defaultClientID, "some-host", "", defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout, func(result JoinResult) {
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
		require.Equal(t, kafkaprotocol.ErrorCodeNone, res.ErrorCode)
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
	gc, _, _, _ := createCoordinatorWithCfgSetter(t, func(cfg *Conf) {
		cfg.InitialJoinDelay = initialJoinDelay
	})
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	var memberMetaDataMap sync.Map

	numMembers := 10
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

		gc.joinGroup(0, groupID, defaultClientID, "some-host", "", defaultProtocolType, protocols, defaultSessionTimeout, rebalanceTimeout, func(result JoinResult) {
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

		require.Equal(t, kafkaprotocol.ErrorCodeNone, res.ErrorCode)
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
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)
	groupID := uuid.New().String()

	chans := make([]chan JoinResult, len(infos))
	for i, protocolInfos := range infos {
		ch := make(chan JoinResult, 1)
		thePIs := protocolInfos
		gc.joinGroup(0, groupID, defaultClientID, "some-host", "", defaultProtocolType, thePIs, defaultSessionTimeout, defaultRebalanceTimeout, func(result JoinResult) {
			ch <- result
		})
		chans[i] = ch
	}
	for _, ch := range chans {
		res := <-ch
		require.Equal(t, kafkaprotocol.ErrorCodeNone, res.ErrorCode)
		require.Equal(t, expectedProtocol, res.ProtocolName)
	}
}

func TestJoinUnsupportedProtocol(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)
	groupID := uuid.New().String()
	protocols1 := []ProtocolInfo{{"prot1", []byte("foo")}, {"prot2", []byte("foo")}}
	res := callJoinGroupSync(gc, groupID, defaultClientID, "", "pt1", protocols1, defaultSessionTimeout, defaultRebalanceTimeout)
	res = callJoinGroupSync(gc, groupID, defaultClientID, res.MemberID, "pt1", protocols1, defaultSessionTimeout, defaultRebalanceTimeout)

	protocols2 := []ProtocolInfo{{"prot3", []byte("foo")}, {"prot4", []byte("foo")}}
	res = callJoinGroupSync(gc, groupID, defaultClientID, "", "pt1", protocols2, defaultSessionTimeout, defaultRebalanceTimeout)
	require.Equal(t, kafkaprotocol.ErrorCodeInconsistentGroupProtocol, res.ErrorCode)
}

func TestJoinNotController(t *testing.T) {
	gc, controlClient, _, _ := createCoordinatorWithCfgSetter(t, nil)
	defer stopCoordinator(t, gc)

	controlClient.groupCoordinatorAddress = "foo"

	res := callJoinGroupSync(gc, uuid.New().String(), defaultClientID, "", defaultProtocolType,
		nil, defaultSessionTimeout, defaultRebalanceTimeout)
	require.Equal(t, kafkaprotocol.ErrorCodeNotCoordinator, res.ErrorCode)
}

func TestSyncGroup(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()

	// First join
	numMembers := 10
	members, _ := setupJoinedGroup(t, numMembers, groupID, gc)

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

	members.Range(func(key, value any) bool {
		memberID := key.(string)
		isLeader := value.(bool)
		var theAssignments []AssignmentInfo
		if isLeader {
			theAssignments = assignments
		}
		gc.syncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
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
		require.Equal(t, kafkaprotocol.ErrorCodeNone, res.errorCode)
		require.Equal(t, expectedAssignment.Assignment, res.assignment)
	}
}

func TestJoinNewMemberWhileAwaitingRebalance(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	numMembers := 10
	members, _ := setupJoinedGroup(t, numMembers, groupID, gc)

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
		gc.syncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
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
	gc.joinGroup(0, groupID, defaultClientID, "some-host", "", defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
		func(result JoinResult) {
			ch <- result
		})

	// This should cause all waiting sync members to get a response with error code rebalance-in-progress and empty
	// assignment
	for _, ch := range chans {
		sr := <-ch
		require.Equal(t, kafkaprotocol.ErrorCodeRebalanceInProgress, sr.errorCode)
		require.Equal(t, 0, len(sr.assignment))
	}

	// Now we rejoin all the original members
	members.Range(func(key, value any) bool {
		memberID := key.(string)
		ch := make(chan JoinResult, 1)
		chans2 = append(chans2, ch)
		protocols := []ProtocolInfo{{defaultProtocolName, []byte(fmt.Sprintf("metadata2-%d", i))}}
		expectedMeta[memberID] = protocols[0].Metadata
		gc.joinGroup(0, groupID, defaultClientID, "some-host", memberID, defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
			func(result JoinResult) {
				ch <- result
			})
		i++
		return true
	})

	// And the join should complete successfully
	for _, ch := range chans2 {
		jr := <-ch
		require.Equal(t, kafkaprotocol.ErrorCodeNone, jr.ErrorCode)
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
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	numMembers := 10
	members, _ := setupJoinedGroup(t, numMembers, groupID, gc)

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
		gc.syncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
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
		gc.joinGroup(0, groupID, defaultClientID, "some-host", memberID, defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
			func(result JoinResult) {
				ch <- result
			})
		return true
	})

	// The sync results should all return re-balance in progress and have empty assignments
	for _, ch := range chans {
		sr := <-ch
		require.Equal(t, kafkaprotocol.ErrorCodeRebalanceInProgress, sr.errorCode)
		require.Equal(t, 0, len(sr.assignment))
	}

	// And the re-joins should complete successfully
	for _, ch := range chans2 {
		jr := <-ch
		require.Equal(t, kafkaprotocol.ErrorCodeNone, jr.ErrorCode)
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
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	numMembers := 10
	members, memberProtocols := setupJoinedGroup(t, numMembers, groupID, gc)

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
		gc.syncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
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
		gc.joinGroup(0, groupID, defaultClientID, "some-host", memberID, defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
			func(result JoinResult) {
				ch <- result
			})
		return true
	})

	// And the joins should return straight-away with the current state as no metadata change
	for _, ch := range chans2 {
		jr := <-ch
		require.Equal(t, kafkaprotocol.ErrorCodeNone, jr.ErrorCode)
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
	gc.syncGroup(groupID, skippedMember, 1, nil, func(errorCode int, assignment []byte) {
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
		require.Equal(t, kafkaprotocol.ErrorCodeNone, sr.errorCode)
		require.Equal(t, assignmentMap[memberID], sr.assignment)
	}

}

func TestJoinNewMemberWhileActive(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	numMembers := 10
	members, _ := setupJoinedGroup(t, numMembers, groupID, gc)

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
		gc.syncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
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
		require.Equal(t, kafkaprotocol.ErrorCodeNone, sr.errorCode)
	}

	// Now join a new member
	expectedMeta := map[string][]byte{}
	var chans2 []chan JoinResult
	i := 0
	protocols := []ProtocolInfo{{defaultProtocolName, []byte(fmt.Sprintf("metadata2-%d", i))}}
	ch := make(chan JoinResult, 1)
	chans2 = append(chans2, ch)
	gc.joinGroup(0, groupID, defaultClientID, "some-host", "", defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
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
		gc.joinGroup(0, groupID, defaultClientID, "some-host", memberID, defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
			func(result JoinResult) {
				ch <- result
			})
		i++
		return true
	})

	// And the join should complete successfully
	for _, ch := range chans2 {
		jr := <-ch
		require.Equal(t, kafkaprotocol.ErrorCodeNone, jr.ErrorCode)
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
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	numMembers := 10
	members, _ := setupJoinedGroup(t, numMembers, groupID, gc)

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
		gc.syncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
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
		require.Equal(t, kafkaprotocol.ErrorCodeNone, sr.errorCode)
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
	gc.joinGroup(0, groupID, defaultClientID, "some-host", leader, defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
		func(result JoinResult) {
			ch <- result
		})

	// This should trigger a rebalance
	errorCode := gc.heartbeatGroup(groupID, leader, 1)
	require.Equal(t, kafkaprotocol.ErrorCodeRebalanceInProgress, errorCode)

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
		gc.joinGroup(0, groupID, defaultClientID, "some-host", memberID, defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
			func(result JoinResult) {
				ch <- result
			})
		i++
		return true
	})

	// And the join should complete successfully
	for _, ch := range chans2 {
		jr := <-ch
		require.Equal(t, kafkaprotocol.ErrorCodeNone, jr.ErrorCode)
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
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	numMembers := 10
	members, memberProtocols := setupJoinedGroup(t, numMembers, groupID, gc)

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
		gc.syncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
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
		require.Equal(t, kafkaprotocol.ErrorCodeNone, sr.errorCode)
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
		gc.joinGroup(0, groupID, defaultClientID, "some-host", memberID, defaultProtocolType, protocols, defaultSessionTimeout, defaultRebalanceTimeout,
			func(result JoinResult) {
				ch <- result
			})
		i++
		return true
	})

	// And the join should complete successfully
	for _, ch := range chans2 {
		jr := <-ch
		require.Equal(t, kafkaprotocol.ErrorCodeNone, jr.ErrorCode)
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
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()

	protocols := []ProtocolInfo{
		{defaultProtocolName, []byte("protocol1_bytes")},
	}
	gc.joinGroup(0, groupID, defaultClientID, "some-host", "", defaultProtocolType, protocols,
		defaultSessionTimeout, defaultRebalanceTimeout, func(result JoinResult) {
		})

	// The group will now be in state StatePreReBalance

	ch := make(chan int, 1)
	gc.syncGroup(groupID, "some-member-id", 0, nil, func(errorCode int, assignment []byte) {
		ch <- errorCode
	})
	errorCode := <-ch
	require.Equal(t, kafkaprotocol.ErrorCodeRebalanceInProgress, errorCode)
}

func TestSyncWhenActiveReturnsCurrentState(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()

	numMembers := 10
	members, _ := setupJoinedGroup(t, numMembers, groupID, gc)
	assignments := syncGroup(groupID, numMembers, members, gc)
	assignmentMap := map[string][]byte{}
	for _, assignment := range assignments {
		assignmentMap[assignment.MemberID] = assignment.Assignment
	}

	// Now everything is synced

	// Sync with unknown member id
	ch := make(chan int, 1)
	gc.syncGroup(groupID, "unknown", 1, nil, func(errorCode int, assignment []byte) {
		ch <- errorCode
	})
	errorCode := <-ch
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownMemberID, errorCode)

	// Sync with actual members - should just return current state
	members.Range(func(key, value any) bool {
		memberID := key.(string)

		ch := make(chan syncResult, 1)
		gc.syncGroup(groupID, memberID, 1, nil, func(errorCode int, assignment []byte) {
			ch <- syncResult{
				errorCode:  errorCode,
				assignment: assignment,
			}
		})
		res := <-ch
		require.Equal(t, kafkaprotocol.ErrorCodeNone, res.errorCode)
		assignment, ok := assignmentMap[memberID]
		require.True(t, ok)
		require.Equal(t, assignment, res.assignment)

		return true
	})

}

func setupJoinedGroup(t *testing.T, numMembers int, groupID string, gc *Coordinator) (*sync.Map, *sync.Map) {
	return setupJoinedGroupWithArgs(t, numMembers, groupID, gc, defaultRebalanceTimeout)
}

func setupJoinedGroupWithArgs(t *testing.T, numMembers int, groupID string, gc *Coordinator, rebalanceTimeout time.Duration) (*sync.Map, *sync.Map) {
	wg := sync.WaitGroup{}
	wg.Add(numMembers)
	members := sync.Map{}
	memberProtocols := sync.Map{}
	for i := 0; i < numMembers; i++ {
		protocols := []ProtocolInfo{{defaultProtocolName, []byte(fmt.Sprintf("metadata-%d", i))}}
		gc.joinGroup(4, groupID, defaultClientID, "some-host", "", defaultProtocolType, protocols, defaultSessionTimeout, rebalanceTimeout, func(result JoinResult) {
			require.Equal(t, kafkaprotocol.ErrorCodeUnknownMemberID, result.ErrorCode)
			go func() {
				gc.joinGroup(0, groupID, defaultClientID, "some-host", result.MemberID, defaultProtocolType, protocols, defaultSessionTimeout, rebalanceTimeout, func(result JoinResult) {
					if result.ErrorCode != kafkaprotocol.ErrorCodeNone {
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

func syncGroup(groupID string, numMembers int, members *sync.Map, gc *Coordinator) []AssignmentInfo {
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

	members.Range(func(key, value any) bool {
		memberID := key.(string)
		isLeader := value.(bool)
		var theAssignments []AssignmentInfo
		if isLeader {
			theAssignments = assignments
		}
		gc.syncGroup(groupID, memberID, 1, theAssignments, func(errorCode int, assignment []byte) {
			if errorCode != kafkaprotocol.ErrorCodeNone {
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
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	members, _ := setupJoinedGroup(t, 1, groupID, gc)
	var memberID string
	members.Range(func(key, value any) bool {
		memberID = key.(string)
		return true
	})

	ch := make(chan int, 1)
	gc.syncGroup(groupID, memberID, 23, []AssignmentInfo{}, func(errorCode int, assignment []byte) {
		ch <- errorCode
	})
	err := <-ch
	require.Equal(t, kafkaprotocol.ErrorCodeIllegalGeneration, err)
}

func TestAddNewMembersAfterSync(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()

	numInitialMembers := 10
	members, memberProtocols := setupJoinedGroup(t, numInitialMembers, groupID, gc)
	syncGroup(groupID, numInitialMembers, members, gc)

	// Add another member

	numNewMembers := 10
	newMembersMap := sync.Map{}
	chans := make([]chan JoinResult, 0, numInitialMembers+numNewMembers)
	for i := 0; i < numNewMembers; i++ {
		ch := make(chan JoinResult, 1)
		chans = append(chans, ch)
		protocols := []ProtocolInfo{{defaultProtocolName, []byte(fmt.Sprintf("metadata-%d", i+numInitialMembers))}}
		gc.joinGroup(0, groupID, defaultClientID, "some-host", "", "protocoltype1", protocols, defaultSessionTimeout, defaultRebalanceTimeout, func(result JoinResult) {
			go func() {
				// First should trigger a rebalance
				gc.joinGroup(0, groupID, defaultClientID, "some-host", result.MemberID, "protocoltype1", protocols, defaultSessionTimeout, defaultRebalanceTimeout, func(result JoinResult) {
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
		gc.joinGroup(0, groupID, defaultClientID, "some-host", memberID, "protocoltype1", protocols, defaultSessionTimeout, defaultRebalanceTimeout, func(result JoinResult) {
			ch <- result
		})
		cnt++
		return true
	})

	// This should make all the joins return
	for _, ch := range chans {
		jr := <-ch
		require.Equal(t, kafkaprotocol.ErrorCodeNone, jr.ErrorCode)
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

func TestSyncEmptyMemberID(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()

	ch := make(chan int, 1)
	gc.syncGroup(groupID, "", 1, nil, func(errorCode int, assignment []byte) {
		ch <- errorCode
	})
	errorCode := <-ch
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownMemberID, errorCode)
}

func TestSyncUnknownGroupID(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()

	ch := make(chan int, 1)
	gc.syncGroup(groupID, "foo", 1, nil, func(errorCode int, assignment []byte) {
		ch <- errorCode
	})
	errorCode := <-ch
	require.Equal(t, kafkaprotocol.ErrorCodeGroupIDNotFound, errorCode)
}

func TestHeartbeatEmptyMemberID(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	errorCode := gc.heartbeatGroup(groupID, "", 1)
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownMemberID, errorCode)
}

func TestHeartbeatUnknownGroupID(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	errorCode := gc.heartbeatGroup(groupID, "foo", 1)
	require.Equal(t, kafkaprotocol.ErrorCodeGroupIDNotFound, errorCode)
}

func TestHeartbeatIllegalGeneration(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	protocolMetadata := []byte("protocol1_bytes")
	protocols := []ProtocolInfo{
		{defaultProtocolName, protocolMetadata},
	}
	groupID := uuid.New().String()
	ch := make(chan JoinResult, 1)
	gc.joinGroup(0, groupID, defaultClientID, "some-host", "", defaultProtocolType, protocols, defaultSessionTimeout,
		defaultRebalanceTimeout, func(result JoinResult) {
			ch <- result
		})
	res := <-ch

	// Group should now be in state StateAwaitingReBalance - waiting for initial timeout before completing join
	require.Equal(t, StateAwaitingReBalance, gc.getState(groupID))

	errorCode := gc.heartbeatGroup(groupID, res.MemberID, 100)
	require.Equal(t, kafkaprotocol.ErrorCodeIllegalGeneration, errorCode)
}

func TestHeartbeatAwaitingRebalance(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	members, _ := setupJoinedGroup(t, 10, groupID, gc)
	var memberID string
	members.Range(func(key, value any) bool {
		memberID = key.(string)
		return false
	})

	// Group should now be in state StateAwaitingReBalance
	require.Equal(t, StateAwaitingReBalance, gc.getState(groupID))

	errorCode := gc.heartbeatGroup(groupID, memberID, 1)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, errorCode)
}

func TestHeartbeatWhileActive(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	numMembers := 10
	members, _ := setupJoinedGroup(t, numMembers, groupID, gc)
	syncGroup(groupID, numMembers, members, gc)

	// Now group should be in active state

	var memberID string
	members.Range(func(key, value any) bool {
		memberID = key.(string)
		return false
	})

	// Group should now be in state StateAwaitingReBalance
	require.Equal(t, StateActive, gc.getState(groupID))

	errorCode := gc.heartbeatGroup(groupID, memberID, 1)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, errorCode)
}

func TestJoinTimeoutMembersRemovedAndJoinCompletes(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	numMembers := 10
	rebalanceTimeout := 1 * time.Second
	members, memberProts := setupJoinedGroupWithArgs(t, numMembers, groupID, gc, rebalanceTimeout)
	syncGroup(groupID, numMembers, members, gc)
	require.Equal(t, StateActive, gc.getState(groupID))

	// Add a new member to prompt a rebalance
	var chans []chan JoinResult
	protocols := []ProtocolInfo{{defaultProtocolName, []byte("metadata-11")}}
	ch := make(chan JoinResult, 1)
	chans = append(chans, ch)
	gc.joinGroup(0, groupID, defaultClientID, "some-host", "", defaultProtocolType, protocols, defaultSessionTimeout,
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
		gc.joinGroup(0, groupID, defaultClientID, "some-host", memberID, defaultProtocolType, protocols, defaultSessionTimeout,
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
		require.Equal(t, kafkaprotocol.ErrorCodeNone, res.ErrorCode)
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
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	numMembers := 10
	rebalanceTimeout := 1 * time.Second
	members, memberProts := setupJoinedGroupWithArgs(t, numMembers, groupID, gc, rebalanceTimeout)
	syncGroup(groupID, numMembers, members, gc)
	require.Equal(t, StateActive, gc.getState(groupID))

	// Add a new member to prompt a rebalance
	var chans []chan JoinResult
	protocols := []ProtocolInfo{{defaultProtocolName, []byte("metadata-11")}}
	ch := make(chan JoinResult, 1)
	chans = append(chans, ch)

	gc.joinGroup(0, groupID, defaultClientID, "some-host", "", defaultProtocolType, protocols, defaultSessionTimeout,
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
		gc.joinGroup(0, groupID, defaultClientID, "some-host", memberID, defaultProtocolType, protocols, defaultSessionTimeout,
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
		require.Equal(t, kafkaprotocol.ErrorCodeNone, res.ErrorCode)
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

	gc, _, _, _ := createCoordinatorWithCfgSetter(t, func(cfg *Conf) {
		cfg.InitialJoinDelay = 100 * time.Millisecond
		cfg.MinSessionTimeout = 1 * time.Millisecond
		cfg.NewMemberJoinTimeout = newMemberJoinTimeout
	})
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	numMembers := 10
	rebalanceTimeout := 1 * time.Second
	members, _ := setupJoinedGroupWithArgs(t, numMembers, groupID, gc, rebalanceTimeout)
	syncGroup(groupID, numMembers, members, gc)
	require.Equal(t, StateActive, gc.getState(groupID))

	// Add a new member to prompt a rebalance
	var chans []chan JoinResult
	protocols := []ProtocolInfo{{defaultProtocolName, []byte("metadata-11")}}
	ch := make(chan JoinResult, 1)
	chans = append(chans, ch)
	gc.joinGroup(0, groupID, defaultClientID, "some-host", "", defaultProtocolType, protocols, defaultSessionTimeout,
		rebalanceTimeout, func(result JoinResult) {
			ch <- result
		})
	require.Equal(t, StatePreReBalance, gc.getState(groupID))

	// Now wait until that new member's session expires
	time.Sleep(2 * newMemberJoinTimeout)

	// And wait for join timeout
	time.Sleep(rebalanceTimeout)

	// At this point there should be only the old members in the group, and none have rejoined, so the join timeout
	// should remove them all leaving an empty group
	require.Equal(t, StateEmpty, gc.getState(groupID))
}

func TestRejoinLeaderTriggersRebalance(t *testing.T) {
	gc := createCoordinator(t)
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()
	numMembers := 10
	rebalanceTimeout := 1 * time.Second
	members, memberProts := setupJoinedGroupWithArgs(t, numMembers, groupID, gc, rebalanceTimeout)
	syncGroup(groupID, numMembers, members, gc)
	require.Equal(t, StateActive, gc.getState(groupID))

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
	gc.joinGroup(0, groupID, defaultClientID, "some-host", leader, defaultProtocolType, p.([]ProtocolInfo), defaultSessionTimeout,
		rebalanceTimeout, func(result JoinResult) {})
	require.Equal(t, StatePreReBalance, gc.getState(groupID))
}

func addMemberWithSessionTimeout(gc *Coordinator, groupID string, sessionTimeout time.Duration) chan JoinResult {
	protocols := []ProtocolInfo{{defaultProtocolName, []byte("foo")}}
	ch := make(chan JoinResult, 1)
	gc.joinGroup(0, groupID, defaultClientID, "some-host", "", defaultProtocolType, protocols, sessionTimeout,
		defaultRebalanceTimeout, func(result JoinResult) {
			ch <- result
		})
	return ch
}

func TestSessionTimeoutWhenActive(t *testing.T) {
	gc, _, _, _ := createCoordinatorWithCfgSetter(t, func(config *Conf) {
		config.InitialJoinDelay = 100 * time.Millisecond
		config.MinSessionTimeout = 1 * time.Millisecond
	})
	defer stopCoordinator(t, gc)

	groupID := uuid.New().String()

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
		require.Equal(t, kafkaprotocol.ErrorCodeNone, res.ErrorCode)
		isLeader := res.LeaderMemberID == res.MemberID
		members.Store(res.MemberID, isLeader)
	}
	require.Equal(t, StateAwaitingReBalance, gc.getState(groupID))
	syncGroup(groupID, 3, &members, gc)
	require.Equal(t, StateActive, gc.getState(groupID))
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

	require.Equal(t, StateEmpty, gc.getState(groupID))
}

func TestOffsetCommit(t *testing.T) {
	localTransports := transport.NewLocalTransports()
	gc, controlClient, topicProvider, _ := createCoordinatorWithConnFactoryAndCfgSetter(t, localTransports.CreateConnection, nil)
	defer stopCoordinator(t, gc)

	fp := &fakePusherSink{}
	transportServer, err := localTransports.NewLocalServer(uuid.New().String())
	require.NoError(t, err)
	transportServer.RegisterHandler(transport.HandlerIDTablePusherDirectWrite, fp.HandleDirectWrite)
	memberData := common.MembershipData{
		ClusterListenAddress: transportServer.Address(),
	}
	err = gc.MembershipChanged(0, cluster.MembershipState{
		LeaderVersion:  1,
		ClusterVersion: 1,
		Members: []cluster.MembershipEntry{
			{
				ID:   0,
				Data: memberData.Serialize(nil),
			},
		},
	})
	require.NoError(t, err)

	topicName1 := "test-topic1"
	topicName2 := "test-topic2"
	topicProvider.infos[topicName1] = topicmeta.TopicInfo{
		ID:             1234,
		Name:           topicName1,
		PartitionCount: 100,
	}
	topicProvider.infos[topicName2] = topicmeta.TopicInfo{
		ID:             2234,
		Name:           topicName2,
		PartitionCount: 100,
	}
	controlClient.groupEpoch = 23

	groupID := uuid.New().String()
	numMembers := 10
	rebalanceTimeout := 1 * time.Second
	members, _ := setupJoinedGroupWithArgs(t, numMembers, groupID, gc, rebalanceTimeout)
	syncGroup(groupID, numMembers, members, gc)
	require.Equal(t, StateActive, gc.getState(groupID))

	var memberID string
	members.Range(func(key, value any) bool {
		memberID = key.(string)
		return false
	})

	req := kafkaprotocol.OffsetCommitRequest{
		GroupId:                   common.StrPtr(groupID),
		MemberId:                  common.StrPtr(memberID),
		GenerationIdOrMemberEpoch: int32(1),
		Topics: []kafkaprotocol.OffsetCommitRequestOffsetCommitRequestTopic{
			{
				Name: common.StrPtr(topicName1),
				Partitions: []kafkaprotocol.OffsetCommitRequestOffsetCommitRequestPartition{
					{
						PartitionIndex:  1,
						CommittedOffset: 12345,
					},
					{
						PartitionIndex:  23,
						CommittedOffset: 456456,
					},
				},
			},
			{
				Name: common.StrPtr(topicName2),
				Partitions: []kafkaprotocol.OffsetCommitRequestOffsetCommitRequestPartition{
					{
						PartitionIndex:  7,
						CommittedOffset: 345345,
					},
				},
			},
		},
	}

	resp, err := gc.OffsetCommit(&req)
	require.NoError(t, err)

	require.Equal(t, 2, len(resp.Topics))
	require.Equal(t, topicName1, *resp.Topics[0].Name)

	require.Equal(t, 2, len(resp.Topics[0].Partitions))

	require.Equal(t, 1, int(resp.Topics[0].Partitions[0].PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Topics[0].Partitions[0].ErrorCode))
	require.Equal(t, 23, int(resp.Topics[0].Partitions[1].PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Topics[0].Partitions[1].ErrorCode))

	require.Equal(t, 1, len(resp.Topics[1].Partitions))

	require.Equal(t, 7, int(resp.Topics[1].Partitions[0].PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Topics[0].Partitions[0].ErrorCode))

	received, rpcVer := fp.getReceived()
	require.NotNil(t, received)

	require.Equal(t, 1, int(rpcVer))

	require.Equal(t, "g."+groupID, received.WriterKey)
	require.Equal(t, 23, received.WriterEpoch)

	partHash, err := parthash.CreateHash([]byte("g." + groupID))
	require.NoError(t, err)

	var expectedKVs []common.KV
	expectedKVs = append(expectedKVs, createExpectedCommitKV(partHash, 1234, 1, 12345))
	expectedKVs = append(expectedKVs, createExpectedCommitKV(partHash, 1234, 23, 456456))
	expectedKVs = append(expectedKVs, createExpectedCommitKV(partHash, 2234, 7, 345345))

	require.Equal(t, expectedKVs, received.KVs)
}

func createExpectedCommitKV(partHash []byte, topicID int, partitionID int, committedOffset int64) common.KV {
	key := createOffsetKey(partHash, OffsetKeyPublic, topicID, partitionID)
	val := make([]byte, 0, 9)
	val = binary.BigEndian.AppendUint64(val, uint64(committedOffset))
	val = common.AppendValueMetadata(val)
	return common.KV{
		Key:   key,
		Value: val,
	}
}

func TestOffsetDelete(t *testing.T) {
	localTransports := transport.NewLocalTransports()
	gc, controlClient, topicProvider, _ := createCoordinatorWithConnFactoryAndCfgSetter(t, localTransports.CreateConnection, nil)
	defer stopCoordinator(t, gc)

	fp := &fakePusherSink{}
	transportServer, err := localTransports.NewLocalServer(uuid.New().String())
	require.NoError(t, err)
	transportServer.RegisterHandler(transport.HandlerIDTablePusherDirectWrite, fp.HandleDirectWrite)
	memberData := common.MembershipData{
		ClusterListenAddress: transportServer.Address(),
	}
	err = gc.MembershipChanged(0, cluster.MembershipState{
		LeaderVersion:  1,
		ClusterVersion: 1,
		Members: []cluster.MembershipEntry{
			{
				ID:   0,
				Data: memberData.Serialize(nil),
			},
		},
	})
	require.NoError(t, err)

	topicName1 := "test-topic1"
	topicName2 := "test-topic2"
	topicProvider.infos[topicName1] = topicmeta.TopicInfo{
		ID:             1234,
		Name:           topicName1,
		PartitionCount: 100,
	}
	topicProvider.infos[topicName2] = topicmeta.TopicInfo{
		ID:             2234,
		Name:           topicName2,
		PartitionCount: 100,
	}
	controlClient.groupEpoch = 23

	groupID := uuid.New().String()
	numMembers := 10
	rebalanceTimeout := 1 * time.Second
	members, _ := setupJoinedGroupWithArgs(t, numMembers, groupID, gc, rebalanceTimeout)
	syncGroup(groupID, numMembers, members, gc)
	require.Equal(t, StateActive, gc.getState(groupID))

	req := kafkaprotocol.OffsetDeleteRequest{
		GroupId: common.StrPtr(groupID),
		Topics: []kafkaprotocol.OffsetDeleteRequestOffsetDeleteRequestTopic{
			{
				Name: common.StrPtr(topicName1),
				Partitions: []kafkaprotocol.OffsetDeleteRequestOffsetDeleteRequestPartition{
					{
						PartitionIndex: 1,
					},
					{
						PartitionIndex: 23,
					},
				},
			},
			{
				Name: common.StrPtr(topicName2),
				Partitions: []kafkaprotocol.OffsetDeleteRequestOffsetDeleteRequestPartition{
					{
						PartitionIndex: 7,
					},
				},
			},
		},
	}

	resp, err := gc.OffsetDelete(&req)
	require.NoError(t, err)

	require.Equal(t, 2, len(resp.Topics))
	require.Equal(t, topicName1, *resp.Topics[0].Name)

	require.Equal(t, 2, len(resp.Topics[0].Partitions))

	require.Equal(t, 1, int(resp.Topics[0].Partitions[0].PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Topics[0].Partitions[0].ErrorCode))
	require.Equal(t, 23, int(resp.Topics[0].Partitions[1].PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Topics[0].Partitions[1].ErrorCode))

	require.Equal(t, 1, len(resp.Topics[1].Partitions))

	require.Equal(t, 7, int(resp.Topics[1].Partitions[0].PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Topics[0].Partitions[0].ErrorCode))

	received, rpcVer := fp.getReceived()
	require.NotNil(t, received)

	require.Equal(t, 1, int(rpcVer))

	require.Equal(t, "g."+groupID, received.WriterKey)
	require.Equal(t, 23, received.WriterEpoch)

	partHash, err := parthash.CreateHash([]byte("g." + groupID))
	require.NoError(t, err)

	var expectedKVs []common.KV
	expectedKVs = append(expectedKVs, createExpectedDeleteKV(partHash, 1234, 1))
	expectedKVs = append(expectedKVs, createExpectedDeleteKV(partHash, 1234, 23))
	expectedKVs = append(expectedKVs, createExpectedDeleteKV(partHash, 2234, 7))

	require.Equal(t, expectedKVs, received.KVs)
}

func createExpectedDeleteKV(partHash []byte, topicID int, partitionID int) common.KV {
	key := createOffsetKey(partHash, OffsetKeyPublic, topicID, partitionID)
	return common.KV{
		Key:   key,
		Value: []byte{},
	}
}

func TestOffsetFetch(t *testing.T) {
	gc, controlClient, topicProvider, tableGetter := createCoordinatorWithCfgSetter(t, nil)
	defer stopCoordinator(t, gc)

	topicName1 := "test-topic1"
	topicName2 := "test-topic2"
	topicProvider.infos[topicName1] = topicmeta.TopicInfo{
		ID:             1234,
		Name:           topicName1,
		PartitionCount: 100,
	}
	topicProvider.infos[topicName2] = topicmeta.TopicInfo{
		ID:             2234,
		Name:           topicName2,
		PartitionCount: 100,
	}
	controlClient.groupEpoch = 23

	groupID := uuid.New().String()
	numMembers := 10
	rebalanceTimeout := 1 * time.Second
	members, _ := setupJoinedGroupWithArgs(t, numMembers, groupID, gc, rebalanceTimeout)
	syncGroup(groupID, numMembers, members, gc)
	require.Equal(t, StateActive, gc.getState(groupID))

	g, ok := gc.getGroup(groupID)
	require.True(t, ok)

	infos := []createOffsetsInfo{
		{
			topicID: 1234,
			partInfos: []createOffsetsPartitionInfo{
				{
					1, 123213,
				},
				{
					23, 2344,
				},
			},
		},
		{
			topicID: 2234,
			partInfos: []createOffsetsPartitionInfo{
				{
					7, 3455,
				},
				{
					11, 233,
				},
				{
					77, 456,
				},
			},
		},
	}
	table := createOffsetsBatch(t, infos, g.partHash)
	tableGetter.table = table
	tableID := sst.CreateSSTableId()

	controlClient.queryRes = []lsm.NonOverlappingTables{
		[]lsm.QueryTableInfo{
			{
				ID: []byte(tableID),
			},
		},
	}

	req := kafkaprotocol.OffsetFetchRequest{
		GroupId: common.StrPtr(groupID),
		Topics: []kafkaprotocol.OffsetFetchRequestOffsetFetchRequestTopic{
			{
				Name:             common.StrPtr(topicName1),
				PartitionIndexes: []int32{1, 333, 23},
			},
			{
				Name:             common.StrPtr(topicName2),
				PartitionIndexes: []int32{7, 11, 77, 777},
			},
		},
	}
	resp, err := gc.OffsetFetch(&req)
	require.NoError(t, err)
	require.Equal(t, 2, len(resp.Topics))
	require.Equal(t, topicName1, *resp.Topics[0].Name)

	require.Equal(t, 3, len(resp.Topics[0].Partitions))

	require.Equal(t, 1, int(resp.Topics[0].Partitions[0].PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Topics[0].Partitions[0].ErrorCode))
	require.Equal(t, 123213, int(resp.Topics[0].Partitions[0].CommittedOffset))

	require.Equal(t, 333, int(resp.Topics[0].Partitions[1].PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Topics[1].Partitions[1].ErrorCode))
	require.Equal(t, -1, int(resp.Topics[0].Partitions[1].CommittedOffset))

	require.Equal(t, 23, int(resp.Topics[0].Partitions[2].PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Topics[0].Partitions[2].ErrorCode))
	require.Equal(t, 2344, int(resp.Topics[0].Partitions[2].CommittedOffset))

	require.Equal(t, 4, len(resp.Topics[1].Partitions))

	require.Equal(t, 7, int(resp.Topics[1].Partitions[0].PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Topics[1].Partitions[0].ErrorCode))
	require.Equal(t, 3455, int(resp.Topics[1].Partitions[0].CommittedOffset))

	require.Equal(t, 11, int(resp.Topics[1].Partitions[1].PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Topics[1].Partitions[1].ErrorCode))
	require.Equal(t, 233, int(resp.Topics[1].Partitions[1].CommittedOffset))

	require.Equal(t, 77, int(resp.Topics[1].Partitions[2].PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Topics[1].Partitions[2].ErrorCode))
	require.Equal(t, 456, int(resp.Topics[1].Partitions[2].CommittedOffset))

	require.Equal(t, 777, int(resp.Topics[1].Partitions[3].PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Topics[1].Partitions[3].ErrorCode))
	require.Equal(t, -1, int(resp.Topics[1].Partitions[3].CommittedOffset))
}

var expectedGroups = []kafkaprotocol.ListGroupsResponseListedGroup{
	{
		GroupId:      common.StrPtr("group-1"),
		GroupState:   common.StrPtr("empty"),
		ProtocolType: common.StrPtr("protocol1"),
		GroupType:    common.StrPtr("consumer"),
	},
	{
		GroupId:      common.StrPtr("group-2"),
		GroupState:   common.StrPtr("assigning"),
		ProtocolType: common.StrPtr("protocol2"),
		GroupType:    common.StrPtr("consumer"),
	},
	{
		GroupId:      common.StrPtr("group-3"),
		GroupState:   common.StrPtr("reconciling"),
		ProtocolType: common.StrPtr("protocol3"),
		GroupType:    common.StrPtr("consumer"),
	},
	{
		GroupId:      common.StrPtr("group-4"),
		GroupState:   common.StrPtr("stable"),
		ProtocolType: common.StrPtr("protocol4"),
		GroupType:    common.StrPtr("consumer"),
	},
	{
		GroupId:      common.StrPtr("group-5"),
		GroupState:   common.StrPtr("stable"),
		ProtocolType: common.StrPtr("protocol5"),
		GroupType:    common.StrPtr("consumer"),
	},
	{
		GroupId:      common.StrPtr("group-6"),
		GroupState:   common.StrPtr("dead"),
		ProtocolType: common.StrPtr("protocol6"),
		GroupType:    common.StrPtr("consumer"),
	},
}

func TestListAllGroups(t *testing.T) {
	testListGroups(t, nil, nil, expectedGroups)
	testListGroups(t, []*string{}, []*string{}, expectedGroups)
}

func TestListSingleStateWithStateFilter(t *testing.T) {
	var expected = []kafkaprotocol.ListGroupsResponseListedGroup{
		{
			GroupId:      common.StrPtr("group-4"),
			GroupState:   common.StrPtr("stable"),
			ProtocolType: common.StrPtr("protocol4"),
			GroupType:    common.StrPtr("consumer"),
		},
		{
			GroupId:      common.StrPtr("group-5"),
			GroupState:   common.StrPtr("stable"),
			ProtocolType: common.StrPtr("protocol5"),
			GroupType:    common.StrPtr("consumer"),
		},
	}
	testListGroups(t, []*string{common.StrPtr("stable")}, nil, expected)
}

func TestListSingleStateWithStateFilterCaseInsensitive(t *testing.T) {
	var expected = []kafkaprotocol.ListGroupsResponseListedGroup{
		{
			GroupId:      common.StrPtr("group-4"),
			GroupState:   common.StrPtr("stable"),
			ProtocolType: common.StrPtr("protocol4"),
			GroupType:    common.StrPtr("consumer"),
		},
		{
			GroupId:      common.StrPtr("group-5"),
			GroupState:   common.StrPtr("stable"),
			ProtocolType: common.StrPtr("protocol5"),
			GroupType:    common.StrPtr("consumer"),
		},
	}
	testListGroups(t, []*string{common.StrPtr("STaBLE")}, nil, expected)
}

func TestListMultipleStatesWithStateFilter(t *testing.T) {
	var expected = []kafkaprotocol.ListGroupsResponseListedGroup{
		{
			GroupId:      common.StrPtr("group-2"),
			GroupState:   common.StrPtr("assigning"),
			ProtocolType: common.StrPtr("protocol2"),
			GroupType:    common.StrPtr("consumer"),
		},
		{
			GroupId:      common.StrPtr("group-4"),
			GroupState:   common.StrPtr("stable"),
			ProtocolType: common.StrPtr("protocol4"),
			GroupType:    common.StrPtr("consumer"),
		},
		{
			GroupId:      common.StrPtr("group-5"),
			GroupState:   common.StrPtr("stable"),
			ProtocolType: common.StrPtr("protocol5"),
			GroupType:    common.StrPtr("consumer"),
		},
		{
			GroupId:      common.StrPtr("group-6"),
			GroupState:   common.StrPtr("dead"),
			ProtocolType: common.StrPtr("protocol6"),
			GroupType:    common.StrPtr("consumer"),
		},
	}
	testListGroups(t, []*string{common.StrPtr("stable"), common.StrPtr("assigning"), common.StrPtr("dead")},
		nil, expected)
	testListGroups(t, []*string{common.StrPtr("stable"), common.StrPtr("assigning"), common.StrPtr("dead")},
		[]*string{common.StrPtr("consumer")}, expected)
	testListGroups(t, []*string{common.StrPtr("stable"), common.StrPtr("assigning"), common.StrPtr("dead")},
		[]*string{common.StrPtr("foo")}, nil)
}

func TestListGroupsWithTypeFilter(t *testing.T) {
	testListGroups(t, nil, []*string{common.StrPtr("consumer")}, expectedGroups)
	testListGroups(t, nil, []*string{common.StrPtr("COnsumer")}, expectedGroups)
	testListGroups(t, nil, []*string{common.StrPtr("foo")}, nil)
}

func testListGroups(t *testing.T, statesFilter []*string, typesFilter []*string, expected []kafkaprotocol.ListGroupsResponseListedGroup) {
	localTransports := transport.NewLocalTransports()
	gc, _, _, _ := createCoordinatorWithConnFactoryAndCfgSetter(t, localTransports.CreateConnection, nil)

	fp := &fakePusherSink{}
	transportServer, err := localTransports.NewLocalServer(uuid.New().String())
	require.NoError(t, err)
	transportServer.RegisterHandler(transport.HandlerIDTablePusherDirectWrite, fp.HandleDirectWrite)
	memberData := common.MembershipData{
		ClusterListenAddress: transportServer.Address(),
	}
	err = gc.MembershipChanged(0, cluster.MembershipState{
		LeaderVersion:  1,
		ClusterVersion: 1,
		Members: []cluster.MembershipEntry{
			{
				ID:   0,
				Data: memberData.Serialize(nil),
			},
		},
	})
	require.NoError(t, err)

	// Directly create some groups in different states
	gc.groups["group-1"] = &group{
		id:           "group-1",
		state:        StateEmpty,
		protocolType: "protocol1",
	}
	gc.groups["group-2"] = &group{
		id:           "group-2",
		state:        StatePreReBalance,
		protocolType: "protocol2",
	}
	gc.groups["group-3"] = &group{
		id:           "group-3",
		state:        StateAwaitingReBalance,
		protocolType: "protocol3",
	}
	gc.groups["group-4"] = &group{
		id:           "group-4",
		state:        StateActive,
		protocolType: "protocol4",
	}
	gc.groups["group-5"] = &group{
		id:           "group-5",
		state:        StateActive,
		protocolType: "protocol5",
	}
	gc.groups["group-6"] = &group{
		id:           "group-6",
		state:        StateDead,
		protocolType: "protocol6",
	}

	req := kafkaprotocol.ListGroupsRequest{
		StatesFilter: statesFilter,
		TypesFilter:  typesFilter,
	}

	resp, err := gc.ListGroups(&req)
	require.NoError(t, err)

	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.ErrorCode))
	require.Equal(t, expected, resp.Groups)
}

func TestDescribeGroups(t *testing.T) {
	localTransports := transport.NewLocalTransports()
	gc, _, _, _ := createCoordinatorWithConnFactoryAndCfgSetter(t, localTransports.CreateConnection, nil)

	fp := &fakePusherSink{}
	transportServer, err := localTransports.NewLocalServer(uuid.New().String())
	require.NoError(t, err)
	transportServer.RegisterHandler(transport.HandlerIDTablePusherDirectWrite, fp.HandleDirectWrite)
	memberData := common.MembershipData{
		ClusterListenAddress: transportServer.Address(),
	}
	err = gc.MembershipChanged(0, cluster.MembershipState{
		LeaderVersion:  1,
		ClusterVersion: 1,
		Members: []cluster.MembershipEntry{
			{
				ID:   0,
				Data: memberData.Serialize(nil),
			},
		},
	})
	require.NoError(t, err)

	g1 := &group{
		id:           "group-1",
		state:        StateEmpty,
		protocolName: "protocol1",
		protocolType: "protocoltype1",
	}
	addMembersToGroup(g1, 5)
	gc.groups["group-1"] = g1
	g2 := &group{
		id:           "group-2",
		state:        StateActive,
		protocolName: "protocol2",
		protocolType: "protocoltype2",
	}
	addMembersToGroup(g2, 5)
	gc.groups["group-2"] = g2
	g3 := &group{
		id:           "group-3",
		state:        StateActive,
		protocolName: "protocol3",
		protocolType: "protocoltype3",
	}
	addMembersToGroup(g3, 5)
	gc.groups["group-3"] = g3
	g4 := &group{
		id:           "group-4",
		state:        StateDead,
		protocolName: "protocol4",
		protocolType: "protocoltype4",
	}
	addMembersToGroup(g4, 5)
	gc.groups["group-4"] = g4

	req := kafkaprotocol.DescribeGroupsRequest{
		Groups: []*string{common.StrPtr(g1.id), common.StrPtr(g2.id), common.StrPtr(g3.id), common.StrPtr(g4.id)},
	}

	resp, err := gc.DescribeGroups(&req)
	require.NoError(t, err)

	verifyExpectedGroup(t, []*group{g1, g2, g3, g4}, resp.Groups)
}

func verifyExpectedGroup(t *testing.T, expectedGroups []*group, respGroups []kafkaprotocol.DescribeGroupsResponseDescribedGroup) {
	require.Equal(t, len(expectedGroups), len(respGroups))
	for i, g := range expectedGroups {
		respGroup := respGroups[i]
		require.Equal(t, kafkaprotocol.ErrorCodeNone, int(respGroup.ErrorCode))
		require.Equal(t, g.id, common.SafeDerefStringPtr(respGroup.GroupId))
		if g.state != StateDead {
			require.Equal(t, 5, len(respGroup.Members))
			members := respGroup.Members
			sort.SliceStable(members, func(i, j int) bool {
				return strings.Compare(common.SafeDerefStringPtr(members[i].MemberId),
					common.SafeDerefStringPtr(members[j].MemberId)) < 0
			})
			for i, m := range members {
				require.Equal(t, "clientID1", common.SafeDerefStringPtr(m.ClientId))
				require.Equal(t, "clientHost1", common.SafeDerefStringPtr(m.ClientHost))
				if g.state == StateActive {
					require.Equal(t, fmt.Sprintf("assignment-%d", i), string(m.MemberAssignment))
					require.Equal(t, "protocolmeta", string(m.MemberMetadata))
				} else {
					require.Nil(t, m.MemberAssignment)
					require.Nil(t, m.MemberMetadata)
				}
			}
		} else {
			require.Equal(t, 0, len(respGroup.Members))
		}
	}
}

func addMembersToGroup(g *group, numMembers int) {
	g.protocolType = "protocol1"
	g.members = map[string]*member{}
	for i := 0; i < numMembers; i++ {
		memberID := fmt.Sprintf("member-%d", i)
		g.members[memberID] = &member{
			clientID:   "clientID1",
			clientHost: "clientHost1",
			protocols: []ProtocolInfo{
				{
					Name:     g.protocolName,
					Metadata: []byte("protocolmeta"),
				},
			},
		}
		g.assignments = append(g.assignments, AssignmentInfo{
			MemberID:   memberID,
			Assignment: []byte(fmt.Sprintf("assignment-%d", i)),
		})
	}
}

func TestDescribeGroupsNoSuchGroup(t *testing.T) {
	localTransports := transport.NewLocalTransports()
	gc, _, _, _ := createCoordinatorWithConnFactoryAndCfgSetter(t, localTransports.CreateConnection, nil)

	fp := &fakePusherSink{}
	transportServer, err := localTransports.NewLocalServer(uuid.New().String())
	require.NoError(t, err)
	transportServer.RegisterHandler(transport.HandlerIDTablePusherDirectWrite, fp.HandleDirectWrite)
	memberData := common.MembershipData{
		ClusterListenAddress: transportServer.Address(),
	}
	err = gc.MembershipChanged(0, cluster.MembershipState{
		LeaderVersion:  1,
		ClusterVersion: 1,
		Members: []cluster.MembershipEntry{
			{
				ID:   0,
				Data: memberData.Serialize(nil),
			},
		},
	})
	require.NoError(t, err)

	g1 := &group{
		id:           "group-1",
		state:        StateActive,
		protocolName: "protocol1",
		protocolType: "protocoltype1",
	}
	addMembersToGroup(g1, 5)
	gc.groups["group-1"] = g1

	req := kafkaprotocol.DescribeGroupsRequest{
		Groups: []*string{common.StrPtr(g1.id), common.StrPtr("unknown")},
	}

	resp, err := gc.DescribeGroups(&req)
	require.NoError(t, err)

	require.Equal(t, 2, len(resp.Groups))
	verifyExpectedGroup(t, []*group{g1}, resp.Groups[:1])

	eGroup := resp.Groups[1]
	require.Equal(t, kafkaprotocol.ErrorCodeInvalidGroupID, int(eGroup.ErrorCode))
	require.Equal(t, "unknown", common.SafeDerefStringPtr(eGroup.GroupId))
}

func TestDeleteGroups(t *testing.T) {
	localTransports := transport.NewLocalTransports()
	gc, controlClient, _, _ := createCoordinatorWithConnFactoryAndCfgSetter(t, localTransports.CreateConnection, nil)
	defer stopCoordinator(t, gc)

	fp := &fakePusherSink{}
	transportServer, err := localTransports.NewLocalServer(uuid.New().String())
	require.NoError(t, err)
	transportServer.RegisterHandler(transport.HandlerIDTablePusherDirectWrite, fp.HandleDirectWrite)
	memberData := common.MembershipData{
		ClusterListenAddress: transportServer.Address(),
	}
	err = gc.MembershipChanged(0, cluster.MembershipState{
		LeaderVersion:  1,
		ClusterVersion: 1,
		Members: []cluster.MembershipEntry{
			{
				ID:   0,
				Data: memberData.Serialize(nil),
			},
		},
	})
	require.NoError(t, err)

	//topicName1 := "test-topic1"
	//topicName2 := "test-topic2"
	//topicProvider.infos[topicName1] = topicmeta.TopicInfo{
	//	ID:             1234,
	//	Name:           topicName1,
	//	PartitionCount: 100,
	//}
	//topicProvider.infos[topicName2] = topicmeta.TopicInfo{
	//	ID:             2234,
	//	Name:           topicName2,
	//	PartitionCount: 100,
	//}
	controlClient.groupEpoch = 23

	groupID := uuid.New().String()
	numMembers := 10
	rebalanceTimeout := 1 * time.Second
	members, _ := setupJoinedGroupWithArgs(t, numMembers, groupID, gc, rebalanceTimeout)
	syncGroup(groupID, numMembers, members, gc)
	require.Equal(t, StateActive, gc.getState(groupID))

	req := kafkaprotocol.DeleteGroupsRequest{
		GroupsNames: []*string{common.StrPtr(groupID)},
	}

	resp, err := gc.DeleteGroups(&req)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.Results))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.Results[0].ErrorCode))
	require.Equal(t, groupID, common.SafeDerefStringPtr(resp.Results[0].GroupId))

	received, rpcVer := fp.getReceived()
	require.NotNil(t, received)

	require.Equal(t, 1, int(rpcVer))

	require.Equal(t, "g."+groupID, received.WriterKey)
	require.Equal(t, 23, received.WriterEpoch)

	require.Equal(t, 2, len(received.KVs))

	var expectedKVs []common.KV

	partHash, err := parthash.CreateHash([]byte("g." + groupID))
	require.NoError(t, err)

	tombstoneKey := make([]byte, 0, 24)
	tombstoneKey = append(tombstoneKey, partHash...)
	tombstoneKey = encoding.EncodeVersion(tombstoneKey, math.MaxUint64)

	endMarker := make([]byte, 0, 24)
	endMarker = append(endMarker, common.IncBigEndianBytes(partHash)...)
	endMarker = encoding.EncodeVersion(endMarker, math.MaxUint64)

	expectedKVs = append(expectedKVs, common.KV{Key: tombstoneKey, Value: []byte{}}, common.KV{Key: endMarker, Value: []byte{'x'}})

	require.Equal(t, expectedKVs, received.KVs)
}

type createOffsetsInfo struct {
	topicID   int
	partInfos []createOffsetsPartitionInfo
}

type createOffsetsPartitionInfo struct {
	partitionID     int
	committedOffset int
}

func createOffsetsKvs(t *testing.T, infos []createOffsetsInfo, partHash []byte) []common.KV {
	var kvs []common.KV
	for _, topicData := range infos {
		for _, partitionData := range topicData.partInfos {
			offset := partitionData.committedOffset
			// key is [partition_hash, topic_id, partition_id] value is [offset]
			key := createOffsetKey(partHash, OffsetKeyPublic, topicData.topicID, partitionData.partitionID)
			value := make([]byte, 8)
			binary.BigEndian.PutUint64(value, uint64(offset))
			kvs = append(kvs, common.KV{
				Key:   key,
				Value: value,
			})
		}
	}
	if len(kvs) > 1 {
		// sort them
		sort.Slice(kvs, func(i, j int) bool {
			return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
		})
	}
	return kvs
}

func createOffsetsBatch(t *testing.T, infos []createOffsetsInfo, partHash []byte) *sst.SSTable {
	kvs := createOffsetsKvs(t, infos, partHash)
	iter := common.NewKvSliceIterator(kvs)
	table, _, _, _, _, err := sst.BuildSSTable(common.DataFormatV1, 0, 0, iter)
	require.NoError(t, err)
	return table
}

func callJoinGroupSync(gc *Coordinator, groupID string, clientID string, memberID string, protocolType string, protocols []ProtocolInfo, sessionTimeout time.Duration,
	rebalanceTimeout time.Duration) JoinResult {
	return callJoinGroupSyncWithApiVersion(gc, groupID, clientID, memberID, protocolType, protocols, sessionTimeout, rebalanceTimeout,
		0)
}
func callJoinGroupSyncWithApiVersion(gc *Coordinator, groupID string, clientID string, memberID string, protocolType string, protocols []ProtocolInfo, sessionTimeout time.Duration,
	rebalanceTimeout time.Duration, apiVersion int16) JoinResult {
	ch := make(chan JoinResult, 1)
	gc.joinGroup(apiVersion, groupID, clientID, "some-host", memberID, protocolType, protocols, sessionTimeout, rebalanceTimeout, func(result JoinResult) {
		ch <- result
	})
	return <-ch
}

func stopCoordinator(t *testing.T, gc *Coordinator) {
	err := gc.Stop()
	require.NoError(t, err)
}

func createCoordinator(t *testing.T) *Coordinator {
	gc, _, _, _ := createCoordinatorWithCfgSetter(t, func(cfg *Conf) {
		cfg.InitialJoinDelay = defaultInitialJoinDelay
	})
	return gc
}

func createCoordinatorWithCfgSetter(t *testing.T, cfgSetter func(cfg *Conf)) (*Coordinator, *testControlClient,
	*testTopicInfoProvider, *testTableGetter) {
	localTransports := transport.NewLocalTransports()
	return createCoordinatorWithConnFactoryAndCfgSetter(t, localTransports.CreateConnection, cfgSetter)
}

func createCoordinatorWithConnFactoryAndCfgSetter(t *testing.T, connFactory transport.ConnectionFactory,
	cfgSetter func(cfg *Conf)) (*Coordinator, *testControlClient, *testTopicInfoProvider, *testTableGetter) {
	topicProvider := &testTopicInfoProvider{
		infos: map[string]topicmeta.TopicInfo{},
	}
	address := uuid.New().String()
	tableGetter := &testTableGetter{}
	cfg := NewConf()
	if cfgSetter != nil {
		cfgSetter(&cfg)
	}
	controlClient := &testControlClient{
		groupCoordinatorAddress: address,
	}
	controlFactory := func() (control.Client, error) {
		return controlClient, nil
	}
	controlClientCache := control.NewClientCache(10, controlFactory)
	connCaches := transport.NewConnCaches(10, connFactory)
	gc, err := NewCoordinator(cfg, topicProvider, controlClientCache, connCaches, tableGetter.getTable)
	require.NoError(t, err)
	gc.SetKafkaAddress(address)
	err = gc.Start()
	require.NoError(t, err)

	return gc, controlClient, topicProvider, tableGetter
}

type fakePusherSink struct {
	lock               sync.Mutex
	receivedRPCVersion int16
	received           *common.DirectWriteRequest
}

func (f *fakePusherSink) HandleDirectWrite(_ *transport.ConnectionContext, request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.received = &common.DirectWriteRequest{}
	f.receivedRPCVersion = int16(binary.BigEndian.Uint16(request))
	f.received.Deserialize(request, 2)

	return responseWriter(responseBuff, nil)
}

func (f *fakePusherSink) getReceived() (*common.DirectWriteRequest, int16) {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.received, f.receivedRPCVersion
}

type testTopicInfoProvider struct {
	infos map[string]topicmeta.TopicInfo
}

func (t *testTopicInfoProvider) GetTopicInfo(topicName string) (topicmeta.TopicInfo, bool, error) {
	info, ok := t.infos[topicName]
	if !ok {
		return topicmeta.TopicInfo{}, false, nil
	}
	return info, true, nil
}

type testControlClient struct {
	groupCoordinatorMemberID int32
	groupCoordinatorAddress  string
	groupEpoch               int
	queryRes                 lsm.OverlappingTables
}

func (t *testControlClient) PrePush(infos []offsets.GenerateOffsetTopicInfo, epochInfos []control.EpochInfo) ([]offsets.OffsetTopicInfo, int64,
	[]bool, error) {
	panic("should not be called")
}

func (t *testControlClient) ApplyLsmChanges(regBatch lsm.RegistrationBatch) error {
	panic("should not be called")
}

func (t *testControlClient) RegisterL0Table(sequence int64, regEntry lsm.RegistrationEntry) error {
	panic("should not be called")
}

func (t *testControlClient) QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error) {
	return t.queryRes, nil
}

func (t *testControlClient) RegisterTableListener(topicID int, partitionID int, memberID int32, resetSequence int64) (int64, error) {
	panic("should not be called")
}

func (t *testControlClient) PollForJob() (lsm.CompactionJob, error) {
	panic("should not be called")
}

func (t *testControlClient) GetOffsetInfos(infos []offsets.GetOffsetTopicInfo) ([]offsets.OffsetTopicInfo, error) {
	panic("should not be called")
}

func (t *testControlClient) GetTopicInfo(topicName string) (topicmeta.TopicInfo, int, bool, error) {
	panic("should not be called")
}

func (t *testControlClient) GetTopicInfoByID(topicID int) (topicmeta.TopicInfo, bool, error) {
	panic("should not be called")
}

func (t *testControlClient) GetAllTopicInfos() ([]topicmeta.TopicInfo, error) {
	panic("should not be called")
}

func (t *testControlClient) CreateOrUpdateTopic(topicInfo topicmeta.TopicInfo, create bool) error {
	panic("should not be called")
}

func (t *testControlClient) DeleteTopic(topicName string) error {
	panic("should not be called")
}

func (t *testControlClient) GetCoordinatorInfo(key string) (memberID int32, address string, groupEpoch int, err error) {
	return t.groupCoordinatorMemberID, t.groupCoordinatorAddress, t.groupEpoch, nil
}

func (t *testControlClient) GenerateSequence(sequenceName string) (int64, error) {
	panic("should not be called")
}

func (t *testControlClient) PutUserCredentials(username string, storedKey []byte, serverKey []byte, salt string, iters int) error {
	panic("should not be called")
}

func (t *testControlClient) DeleteUserCredentials(username string) error {
	panic("should not be called")
}

func (t *testControlClient) Authorise(principal string, resourceType acls.ResourceType, resourceName string, operation acls.Operation) (bool, error) {
	panic("should not be called")
}

func (t *testControlClient) CreateAcls(aclEntries []acls.AclEntry) error {
	panic("should not be called")
}

func (t *testControlClient) ListAcls(resourceType acls.ResourceType, resourceNameFilter string, patternTypeFilter acls.ResourcePatternType, principal string, host string, operation acls.Operation, permission acls.Permission) ([]acls.AclEntry, error) {
	panic("should not be called")
}

func (t *testControlClient) DeleteAcls(resourceType acls.ResourceType, resourceNameFilter string, patternTypeFilter acls.ResourcePatternType, principal string, host string, operation acls.Operation, permission acls.Permission) error {
	panic("should not be called")
}

func (t *testControlClient) Close() error {
	panic("should not be called")
}

type testTableGetter struct {
	table *sst.SSTable
}

func (t *testTableGetter) getTable(tableID sst.SSTableID) (*sst.SSTable, error) {
	return t.table, nil
}

func TestCalcMemberForHash(t *testing.T) {
	h := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	require.Equal(t, 0, common.CalcMemberForHash(h, 10))
	h = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
	require.Equal(t, 9, common.CalcMemberForHash(h, 10))
	h = []byte{255, 255, 255, 254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
	require.Equal(t, 9, common.CalcMemberForHash(h, 10))
}
