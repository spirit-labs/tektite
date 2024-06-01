package proc

import (
	"crypto/sha256"
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"math/big"
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
		processorID := CalcProcessorForPartition(mappingID, uint64(partitionID), m.cfg.ProcessorCount)
		leaderNode, err := m.GetLeaderNode(processorID)
		if err != nil {
			return nil, err
		}
		nodePartitions[leaderNode] = append(nodePartitions[leaderNode], partitionID)
	}
	return nodePartitions, nil
}

func CalcProcessorForPartition(mappingID string, partitionID uint64, processorCount int) int {
	partitionHash := CalcPartitionHash(mappingID, partitionID)
	return CalcProcessorForHash(partitionHash, processorCount)
}

func CalcPartitionHash(mappingID string, partitionID uint64) []byte {
	// we include the mappingID so different streams have a different distribution of partitions to processors
	// otherwise all partitions for all streams would be on the same processors resulting in hot processors when
	// number of partitions < number of processors (which is common)
	bytes := encoding.AppendUint64ToBufferLE([]byte(mappingID), partitionID)
	h := sha256.New()
	if _, err := h.Write(bytes); err != nil {
		panic(err)
	}
	out := h.Sum(nil)
	// we take the first 128 bits
	return out[:16]
}

func CalcProcessorForHash(hash []byte, processorCount int) int {
	// we  assign each processor a contiguous range out of the possible values of the hash, with processor n
	// ranges < processor n + 1 ranges, and all values associated with a processor.
	// why we do it this way:
	// we use the hash as the first element of the key for each entry in the store, this means that each processor
	// will write non overlapping sets of keys. this is important, as when we push sstables from a node, each processors
	// writes are in a different sstable. this means when the sstables arrive on L0 of the LSM, there is no key overlap
	// from different processors. this allows us to parallelise compaction of l0 rather than having to do a "stop the world"
	// large compaction. this improves throughput.

	hashInt := new(big.Int).SetBytes(hash)

	// Calculate range per processor
	rangePerProcessor := new(big.Int).Div(int128Max, big.NewInt(int64(processorCount)))

	// Choose processor based on hash value
	processorIndex := hashInt.Div(hashInt, rangePerProcessor)

	processorID := int(processorIndex.Uint64())

	if processorID == processorCount {
		// Due to rangePerProcessor not being exact processorID might not be in the range 0... (<processorCount> - 1), but
		// can return <processorCount> for a small number of valid has values right at the top of the range. In this
		// case we assign to <processorCount> - 1.
		// However, it is *very* unlikely we would get a hash result in this range, as the hash is well distributed
		// and there the amount of other values dwarves it. So if this code is actually executed something strange has
		// happened.
		processorID = processorCount - 1
	}
	return processorID
}

// generateProcessorDataKeys - usually we hash [mapping_id, partition_id] to create the data key prefix and assign key prefixes
// in contiguous ranges to processors so there is no overlap between - this makes L0 compaction parallelisable.
// However sometimes we want to store data against a processor, but not for any particular mapping_id or partition. If that
// data is stored by the processor it's key must be in the range that's assigned to that processor or there would be
// overlap in keys in L0 and it would slow compaction.
// So we need to calculate a key for each processor that's inside the processor's range
func generateProcessorDataKeys(processorCount int) map[int][]byte {
	rangePerProcessor := new(big.Int).Div(int128Max, big.NewInt(int64(processorCount)))
	keys := map[int][]byte{}
	for i := 0; i < processorCount; i++ {
		val := new(big.Int).Mul(rangePerProcessor, big.NewInt(int64(i)))
		key := val.Bytes()
		// Sanity check that it maps to the right processor
		p := CalcProcessorForHash(key, processorCount)
		if p != i {
			panic("incorrect processor data key calculated")
		}
		keys[i] = key
	}
	return keys
}

var int128Max = new(big.Int).SetBytes([]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255})

// CalcProcessorForPartition - we include the mappingID so different streams have a different distribution of partitions to processors
// otherwise all partitions for all streams would be on the same processors resulting in hot processors when
// number of partitions < number of processors (which is common)
//func CalcProcessorForPartition(mappingID string, partitionID int, processorCount int) int {
//	bytes := []byte(mappingID)
//	bytes = encoding.AppendUint64ToBufferLE(bytes, uint64(partitionID))
//	hash := common.DefaultHash(bytes)
//	return int(hash) % processorCount
//}

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
		processorID := CalcProcessorForPartition(mappingID, uint64(partitionID), m.cfg.ProcessorCount)
		leaderNode, err := m.GetLeaderNode(processorID)
		if err != nil {
			return nil, err
		}
		partitionNodes[partitionID] = leaderNode
	}
	return partitionNodes, nil
}
