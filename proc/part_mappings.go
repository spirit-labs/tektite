package proc

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"sync"
)

type PartitionMapper interface {
	NodePartitions(mappingID string, partitionCount int) (map[int][]int, error)
	NodeForPartition(partitionID int, mappingID string, partitionCount int) int
}

var _ PartitionMapper = &ProcessorManager{}

type partitionMapper struct {
	slabMappingsLock   sync.RWMutex
	slabNodePartitions map[string]map[int][]int
	slabPartitionNodes map[string]map[int]int
}

func (m *ProcessorManager) invalidatePartitionMappings() {
	m.slabMappingsLock.Lock()
	defer m.slabMappingsLock.Unlock()
	m.slabNodePartitions = map[string]map[int][]int{}
	m.slabPartitionNodes = map[string]map[int]int{}
}

func (m *ProcessorManager) NodePartitions(mappingID string, partitionCount int) (map[int][]int, error) {
	return common.CallWithRetryOnUnavailable[map[int][]int](func() (map[int][]int, error) {
		return m.nodePartitions0(mappingID, partitionCount)
	}, m.isStopped)
}

func (m *ProcessorManager) nodePartitions0(mappingID string, partitionCount int) (map[int][]int, error) {
	m.slabMappingsLock.RLock()
	// Create a key including the partition count and mapping id
	keyBytes := make([]byte, len(mappingID)+8)
	binary.LittleEndian.PutUint64(keyBytes, uint64(partitionCount))
	copy(keyBytes[8:], common.StringToByteSliceZeroCopy(mappingID))
	key := common.ByteSliceToStringZeroCopy(keyBytes)
	nodePartitions, ok := m.slabNodePartitions[key]
	if ok {
		m.slabMappingsLock.RUnlock()
		return nodePartitions, nil
	}
	m.slabMappingsLock.RUnlock()
	m.slabMappingsLock.Lock()
	defer m.slabMappingsLock.Unlock()
	nodePartitions, ok = m.slabNodePartitions[key]
	if ok {
		return nodePartitions, nil
	}
	nodePartitions, err := m.calculateNodePartitions(mappingID, partitionCount)
	if err != nil {
		return nil, err
	}
	m.slabNodePartitions[key] = nodePartitions
	return nodePartitions, nil
}

func (m *ProcessorManager) calculateNodePartitions(mappingID string, partitionCount int) (map[int][]int, error) {
	nodePartitions := make(map[int][]int)
	for partitionID := 0; partitionID < partitionCount; partitionID++ {
		processorID := CalcProcessorForPartition(mappingID, partitionID, *m.cfg.ProcessorCount)
		leaderNode, err := m.GetLeaderNode(processorID)
		if err != nil {
			return nil, err
		}
		nodePartitions[leaderNode] = append(nodePartitions[leaderNode], partitionID)
	}
	return nodePartitions, nil
}

// CalcProcessorForPartition - we include the mappingID so different streams have a different distribution of partitions to processors
// otherwise all partitions for all streams would be on the same processors resulting in hot processors when
// number of partitions < number of processors (which is common)
func CalcProcessorForPartition(mappingID string, partitionID int, processorCount int) int {
	bytes := []byte(mappingID)
	bytes = encoding.AppendUint64ToBufferLE(bytes, uint64(partitionID))
	hash := common.DefaultHash(bytes)
	return int(hash) % processorCount
}

func (m *ProcessorManager) NodeForPartition(partitionID int, mappingID string, partitionCount int) int {
	nodeID, err := common.CallWithRetryOnUnavailable[int](func() (int, error) {
		return m.nodeForPartition0(partitionID, mappingID, partitionCount)
	}, m.isStopped)
	if err != nil {
		// Should never happen as nodeForPartition0 only returns Unavailable error
		panic(err)
	}
	return nodeID
}

func (m *ProcessorManager) nodeForPartition0(partitionID int, mappingID string, partitionCount int) (int, error) {
	m.slabMappingsLock.RLock()
	partitionNodes, ok := m.slabPartitionNodes[mappingID]
	if ok {
		m.slabMappingsLock.RUnlock()
		return partitionNodes[partitionID], nil
	}
	m.slabMappingsLock.RUnlock()
	m.slabMappingsLock.Lock()
	defer m.slabMappingsLock.Unlock()
	partitionNodes, ok = m.slabPartitionNodes[mappingID]
	if ok {
		return partitionNodes[partitionID], nil
	}
	partitionNodes, err := m.calculatePartitionNodes(mappingID, partitionCount)
	if err != nil {
		return 0, err
	}
	m.slabPartitionNodes[mappingID] = partitionNodes
	return partitionNodes[partitionID], nil
}

func (m *ProcessorManager) calculatePartitionNodes(mappingID string, partitionCount int) (map[int]int, error) {
	partitionNodes := make(map[int]int, partitionCount)
	for partitionID := 0; partitionID < partitionCount; partitionID++ {
		processorID := CalcProcessorForPartition(mappingID, partitionID, *m.cfg.ProcessorCount)
		leaderNode, err := m.GetLeaderNode(processorID)
		if err != nil {
			return nil, err
		}
		partitionNodes[partitionID] = leaderNode
	}
	return partitionNodes, nil
}
