package control

import (
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"sort"
)

type AgentMeta struct {
	ID           int32
	KafkaAddress string
	Location     string
}

func (c *Controller) updateClusterMeta(newState *cluster.MembershipState) {
	agentMetas := make([]AgentMeta, 0, len(newState.Members))
	var agentsSameAz []AgentMeta
	for _, member := range newState.Members {
		var memberData common.MembershipData
		memberData.Deserialize(member.Data, 0)
		agentMeta := AgentMeta{
			ID:           member.ID,
			KafkaAddress: memberData.KafkaListenerAddress,
			Location:     memberData.Location,
		}
		agentMetas = append(agentMetas, agentMeta)
		if memberData.Location == c.cfg.AzInfo {
			agentsSameAz = append(agentsSameAz, agentMeta)
		}
	}
	sort.SliceStable(agentsSameAz, func(i, j int) bool {
		return agentMetas[i].ID < agentMetas[j].ID
	})
	sort.SliceStable(agentsSameAz, func(i, j int) bool {
		return agentsSameAz[i].ID < agentsSameAz[j].ID
	})
	c.clusterState = agentMetas
	c.clusterStateSameAZ = agentsSameAz
}

func (c *Controller) GetClusterMeta() []AgentMeta {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.clusterState
}

func (c *Controller) GetClusterMetaThisAz() []AgentMeta {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.clusterStateSameAZ
}
