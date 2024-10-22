package control

import (
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"sync"
)

/*
GroupCoordinatorController maintains the address of the agent hosting the group coordinator for a particular consumer group
along with it's leader epoch. The address information is used by the FindCoordinator Kafka API to tell a consumer which
agent hosts the group coordinator for a consumer group.
The leader epoch for a group goes up (monotonically but not necessarily consecutive) every time the agent hosting the
coordinator for the group changes. We actually use the cluster version for this so we don't have to maintain another
sequence persistently. The group coordinator requests this information when it is created, and when it writes offsets
to the table pushed the epoch is passed in. When the table pusher then attempts to write the table to S3 it first checks
it has the latest epoch for any offsets being written in the table, and if not, the offsets write is rejected. This
allows us to screen out zombie group coordinators from committing offsets after another agent has become coordinator
for the group.
*/
type GroupCoordinatorController struct {
	lock         sync.RWMutex
	groupLeaders map[string]groupInfo
	chooseSeq    int64
	memberState  *cluster.MembershipState
	memberGroups map[int32][]string
}

type groupInfo struct {
	kafkaAddress string
	memberID     int32
	leaderEpoch  int
}

func NewGroupCoordinatorController() *GroupCoordinatorController {
	return &GroupCoordinatorController{
		groupLeaders: make(map[string]groupInfo),
		memberGroups: make(map[int32][]string),
	}
}

func (m *GroupCoordinatorController) MembershipChanged(membershipState *cluster.MembershipState) {
	m.lock.Lock()
	defer m.lock.Unlock()
	newMembersMap := make(map[int32]struct{}, len(membershipState.Members))
	for _, member := range membershipState.Members {
		newMembersMap[member.ID] = struct{}{}
	}
	if m.memberState != nil {
		for _, member := range m.memberState.Members {
			_, exists := newMembersMap[member.ID]
			if !exists {
				// member has left the cluster - remove it's groups
				// when findController is called again after this cluster membership change, a new info will be created
				// with a new epoch
				groups, ok := m.memberGroups[member.ID]
				if ok {
					for _, group := range groups {
						delete(m.groupLeaders, group)
					}
					delete(m.memberGroups, member.ID)
				}
			}
		}
	}
	m.memberState = membershipState
}

func (m *GroupCoordinatorController) ClearMembers() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.memberGroups = make(map[int32][]string)
	m.memberState = nil
}

func (m *GroupCoordinatorController) CheckGroupEpochs(groupEpochs []GroupEpochInfo) []bool {
	res := make([]bool, len(groupEpochs))
	m.lock.RLock()
	defer m.lock.RUnlock()
	for i, info := range groupEpochs {
		inf, ok := m.groupLeaders[info.GroupID]
		if ok {
			if inf.leaderEpoch == info.GroupEpoch {
				res[i] = true
			}
		}
	}
	return res
}

func (m *GroupCoordinatorController) GetGroupCoordinatorInfo(groupID string) (address string, groupEpoch int, err error) {
	info, ok := m.getInfo(groupID)
	if ok {
		return info.kafkaAddress, info.leaderEpoch, nil
	}
	info, err = m.createInfo(groupID)
	if err != nil {
		return "", 0, err
	}
	return info.kafkaAddress, info.leaderEpoch, nil
}

func (m *GroupCoordinatorController) getInfo(groupID string) (groupInfo, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	info, ok := m.groupLeaders[groupID]
	return info, ok
}

func (m *GroupCoordinatorController) createInfo(groupID string) (groupInfo, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	info, ok := m.groupLeaders[groupID]
	if ok {
		return info, nil
	}
	if m.memberState == nil || len(m.memberState.Members) == 0 {
		return groupInfo{}, common.NewTektiteErrorf(common.Unavailable, "no members in cluster")
	}
	// choose an agent round robin
	index := int(m.chooseSeq) % len(m.memberState.Members)
	m.chooseSeq++
	member := m.memberState.Members[index]
	data := member.Data
	var memberData common.MembershipData
	memberData.Deserialize(data, 0)
	// Note, we just use the cluster version as the leader epoch. All that matters is it increments every time a new
	// groupInfo is created which occurs when an agent hosting a controller leaves the cluster.
	// Using the cluster version gives us the added advantage in that we don't have to persist
	// a sequence so it always goes up even after controller failure
	info = groupInfo{
		memberID:     member.ID,
		kafkaAddress: memberData.KafkaListenerAddress,
		leaderEpoch:  m.memberState.ClusterVersion,
	}
	m.groupLeaders[groupID] = info
	m.memberGroups[member.ID] = append(m.memberGroups[member.ID], groupID)
	return info, nil
}
