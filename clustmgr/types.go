package clustmgr

import "github.com/spirit-labs/tektite/encoding"

type ClusterState struct {
	Version     int
	GroupStates [][]GroupNode
}

type GroupNode struct {
	NodeID        int
	Leader        bool
	Valid         bool
	JoinedVersion int
}

type GroupState struct {
	ClusterVersion int
	GroupNodes     []GroupNode
}

type ClusterStateHandler func(state ClusterState) error

type ClusterStateNotifier interface {
	RegisterStateHandler(stateHandler ClusterStateHandler)
}

func (cs *ClusterState) Serialize(buff []byte) []byte {
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(cs.Version))
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(len(cs.GroupStates)))
	for _, gs := range cs.GroupStates {
		buff = encoding.AppendUint64ToBufferLE(buff, uint64(len(gs)))
		for _, gn := range gs {
			buff = encoding.AppendUint64ToBufferLE(buff, uint64(gn.NodeID))
			if gn.Leader {
				buff = append(buff, 1)
			} else {
				buff = append(buff, 0)
			}
			if gn.Valid {
				buff = append(buff, 1)
			} else {
				buff = append(buff, 0)
			}
			buff = encoding.AppendUint64ToBufferLE(buff, uint64(gn.JoinedVersion))
		}
	}
	return buff
}

func (cs *ClusterState) Deserialize(buff []byte, offset int) int {
	var v uint64
	v, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	cs.Version = int(v)
	var lgs uint64
	lgs, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	groupStates := make([][]GroupNode, int(lgs))
	for i := 0; i < int(lgs); i++ {
		var lnids uint64
		lnids, offset = encoding.ReadUint64FromBufferLE(buff, offset)
		groupStates[i] = make([]GroupNode, lnids)
		for j := 0; j < int(lnids); j++ {
			var nid uint64
			nid, offset = encoding.ReadUint64FromBufferLE(buff, offset)
			leader := buff[offset] == 1
			offset++
			valid := buff[offset] == 1
			offset++
			var joinedVersion uint64
			joinedVersion, offset = encoding.ReadUint64FromBufferLE(buff, offset)
			groupStates[i][j] = GroupNode{
				NodeID:        int(nid),
				Leader:        leader,
				Valid:         valid,
				JoinedVersion: int(joinedVersion),
			}
		}
	}
	cs.GroupStates = groupStates
	return offset
}

func (cs *ClusterState) Copy() *ClusterState {
	groupStatesCopy := make([][]GroupNode, 0, len(cs.GroupStates))
	for _, groupState := range cs.GroupStates {
		groupStateCopy := make([]GroupNode, 0, len(groupState))
		for _, groupNode := range groupState {
			groupNodeCopy := GroupNode{
				NodeID:        groupNode.NodeID,
				Leader:        groupNode.Leader,
				Valid:         groupNode.Valid,
				JoinedVersion: groupNode.JoinedVersion,
			}
			groupStateCopy = append(groupStateCopy, groupNodeCopy)
		}
		groupStatesCopy = append(groupStatesCopy, groupStateCopy)
	}
	csCopy := &ClusterState{
		Version:     cs.Version,
		GroupStates: groupStatesCopy,
	}
	return csCopy
}
