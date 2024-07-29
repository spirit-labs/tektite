package iteration

import (
	"bytes"
	"github.com/spirit-labs/tektite/asl/encoding"
	"math"

	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
)

type MergingIterator struct {
	highestVersion             uint64
	iters                      []Iterator
	preserveTombstones         bool
	minNonCompactableVersion   uint64
	currentTombstone           []byte
	isPrefixTombstone          bool
	firstSameKey               []byte
	firstSameKeyNonCompactable bool
	heap                       []int
	heapTail                   int
}

func NewMergingIterator(iters []Iterator, preserveTombstones bool, highestVersion uint64) (*MergingIterator, error) {
	mi := &MergingIterator{
		highestVersion:           highestVersion,
		minNonCompactableVersion: math.MaxUint64,
		iters:                    iters,
		preserveTombstones:       preserveTombstones,
		heap:                     make([]int, len(iters)+1),
		heapTail:                 0,
	}
	for i := 0; i < len(mi.heap); i++ {
		mi.heap[i] = i - 1
	}
	return mi, nil
}

func NewCompactionMergingIterator(iters []Iterator, preserveTombstones bool, minNonCompactableVersion uint64) (*MergingIterator, error) {
	mi := &MergingIterator{
		highestVersion:           math.MaxUint64,
		minNonCompactableVersion: minNonCompactableVersion,
		iters:                    iters,
		preserveTombstones:       preserveTombstones,
		heap:                     make([]int, len(iters)+1),
		heapTail:                 0,
	}
	for i := 0; i < len(mi.heap); i++ {
		mi.heap[i] = i - 1
	}
	return mi, nil
}

func (m *MergingIterator) Next() (bool, common.KV, error) {
	var err error
	var valid bool

	var lastKeyNoVersion []byte
	var lastKeyVersion uint64
	var lastKV common.KV
	if m.heap[0] >= 0 {
		lastKV = m.iters[m.heap[0]].Current()
		// If a key is not compactable because version >= minNonCompactableVersion then all versions of that key
		// are also non compactable, even if the other versions are < minNonCompactableVersion
		// so, we need to keep track of when the last key changes then decide whether that key is compactable
		// then we won't skip past any entries that are non compactable
		lastKeyNoVersion = lastKV.Key[:len(lastKV.Key)-8]
		lastKeyVersion = encoding.DecodeKeyVersion(lastKV.Key)
		if m.firstSameKey == nil || !bytes.Equal(m.firstSameKey, lastKeyNoVersion) {
			m.firstSameKey = lastKeyNoVersion
			m.firstSameKeyNonCompactable = lastKeyVersion >= m.minNonCompactableVersion
		}
	}

	advanceHeapIter := -1
	inactiveIterToCheck := -1

	if m.heap[0] != -1 && m.heapTail >= 1 {
		// Replace/Remove the last returned value if the heap is not empty
		advanceHeapIter = 1
	}

	var chosenVersion uint64
	var chosenKeyNoVersion []byte
	var kv common.KV
	for {
		if advanceHeapIter >= 1 {
			for {
				valid, kv, err = m.iters[m.heap[advanceHeapIter]].Next()
				if err != nil {
					return false, kv, err
				}
				if !valid {
					break
				}
				version := encoding.DecodeKeyVersion(kv.Key)
				// Skip over same key (if it's compactable)
				// in same iterator we can have multiple versions of the same key
				keyNoVersion := kv.Key[:len(kv.Key)-8]
				if !m.firstSameKeyNonCompactable && bytes.Equal(lastKeyNoVersion, keyNoVersion) {
					if version >= m.minNonCompactableVersion {
						panic("compacting non compactable version") //sanity check
					}
					if log.DebugEnabled {
						lastVersion := encoding.DecodeKeyVersion(lastKV.Key)
						log.Debugf("%p mi: dropping key in next as same key: key %v (%s) value %v (%s) version:%d last key: %v (%s) last value %v (%s) last version %d minnoncompactableversion:%d",
							m, kv.Key, string(kv.Key), kv.Value, string(kv.Value), version, lastKV.Key, string(lastKV.Key), lastKV.Value, string(lastKV.Value), lastVersion, m.minNonCompactableVersion)
					}
					continue
				}
				// Skip past keys with too high a version
				// prefix tombstones always have version math.MaxUint64 and are never screened out
				if version > m.highestVersion && version != math.MaxUint64 {
					if log.DebugEnabled {
						log.Debugf("%p merging iter skipping past key %v (%s) as version %d too high - max version %d",
							m, kv.Key, string(kv.Key), version, m.highestVersion)
					}
					continue
				}
				break
			}

			if advanceHeapIter > m.heapTail {
				// Invalid iterator advanced. If it became valid, then move it to the end of valid region
				// and sifting it up
				if valid {
					m.heapTail++ // make room to the new valid kv at the end of the heap.
					if advanceHeapIter != m.heapTail {
						// if iter not in the tail, move it to the tail
						m.heap[advanceHeapIter], m.heap[m.heapTail] = m.heap[m.heapTail], m.heap[advanceHeapIter]
					}
					// Heap Sift Up operation
					child := m.heapTail
					for {
						parent := child / 2
						if parent < 1 {
							break
						}
						parentKV := m.iters[m.heap[parent]].Current()
						childKV := m.iters[m.heap[child]].Current()
						diff := bytes.Compare(childKV.Key[:len(childKV.Key)-8], parentKV.Key[:len(parentKV.Key)-8])
						if diff < 0 || (diff == 0 && encoding.DecodeKeyVersion(childKV.Key) > encoding.DecodeKeyVersion(parentKV.Key)) {
							// child is smaller, swap parent-child and keep sifting up.
							m.heap[parent], m.heap[child] = m.heap[child], m.heap[parent]
							child = parent
						} else {
							break
						}
					}
				}
			} else if advanceHeapIter == 1 {
				siftDownHead := false
				if valid {
					siftDownHead = true
				} else {
					// Valid iterator advanced. If it became invalid, then move it to the invalid region
					if m.heapTail >= 2 {
						m.heap[1], m.heap[m.heapTail] = m.heap[m.heapTail], m.heap[1]
					}
					m.heapTail--
					if m.heapTail >= 2 {
						siftDownHead = true
					}
				}
				if siftDownHead {
					// Heap Sift Down operation
					parent := 1
					smallest := parent
					for {
						smallestKV := m.iters[m.heap[smallest]].Current()
						smallestKeyWithoutVersion := smallestKV.Key[:len(smallestKV.Key)-8]
						smallestKeyVersion := encoding.DecodeKeyVersion(smallestKV.Key)

						for _, child := range [2]int{2 * parent, 2*parent + 1} {
							if child > m.heapTail {
								break
							}
							childKV := m.iters[m.heap[child]].Current()
							childKeyWithoutVersion := childKV.Key[:len(childKV.Key)-8]
							childKeyVersion := encoding.DecodeKeyVersion(childKV.Key)
							diff := bytes.Compare(childKeyWithoutVersion, smallestKeyWithoutVersion)

							// If keys are same, the smallest should be the one with the highest version
							// if version are the same, sort it based on iter index
							if diff < 0 || (diff == 0 && (childKeyVersion > smallestKeyVersion || (childKeyVersion == smallestKeyVersion &&
								m.heap[child] < m.heap[smallest]))) {
								smallest = child
								smallestKeyWithoutVersion = childKeyWithoutVersion
								smallestKeyVersion = childKeyVersion
							}
						}

						// parent is smallest key amongst its children - heap variant is satisfied
						if smallest == parent {
							break
						}
						// swap
						m.heap[smallest], m.heap[parent] = m.heap[parent], m.heap[smallest]
						parent = smallest // keep sinking down prev smaller
					}
				}
			} else {
				panic("Only allowed to advance iterator in the HEAD or in the invalid region")
			}
		}

		if inactiveIterToCheck == -1 { // if it hasn't check inactive iters, it starts from the first one
			inactiveIterToCheck = m.heapTail + 1
			if advanceHeapIter == 1 && !valid {
				// if a prev valid iter just became invalid, we don't need to check it. so we skip the first invalid
				inactiveIterToCheck++
			}
		} else {
			inactiveIterToCheck++
		}
		if inactiveIterToCheck < len(m.heap) {
			// keep checking inactive iterator
			advanceHeapIter = inactiveIterToCheck
			continue
		}

		if m.heapTail < 1 {
			// Nothing valid
			m.heap[0] = -1
			return false, common.KV{}, nil
		}

		choosenKV := m.iters[m.heap[1]].Current()
		chosenKeyNoVersion = choosenKV.Key[:len(choosenKV.Key)-8]
		chosenVersion = encoding.DecodeKeyVersion(choosenKV.Key)

		if bytes.Equal(chosenKeyNoVersion, m.firstSameKey) {
			// same key, same version as last returned, drop this one
			if chosenVersion == lastKeyVersion {
				advanceHeapIter = 1
				continue
			}
			// the previous highest version is higher than this version, so we can remove this version
			// as long as previous highest is compactable
			if !m.firstSameKeyNonCompactable {
				if log.DebugEnabled {
					log.Debugf("%p mi: dropping as key version %d less than minnoncompactable (%d) %d chosenKeyVersion %d: key %v (%s) value %v (%s)",
						m, lastKeyVersion, 1, m.minNonCompactableVersion, chosenVersion, lastKV.Key, lastKV.Key, lastKV.Value, string(lastKV.Value))
				}
				advanceHeapIter = 1
				continue
			}
		}

		if m.currentTombstone != nil {
			if bytes.Equal(m.currentTombstone, choosenKV.Key[:len(m.currentTombstone)]) {
				if m.isPrefixTombstone || chosenVersion < m.minNonCompactableVersion {
					// The key matches current prefix tombstone
					// skip past it if it is compactable - for prefixes we alwqys delete even if non compactable
					advanceHeapIter = 1
					continue
				}
			} else {
				// does not match - reset the prefix tombstone
				m.currentTombstone = nil
				m.isPrefixTombstone = false
			}
		}

		isTombstone := len(choosenKV.Value) == 0
		if isTombstone {
			// We have a tombstone, keep track of it. Prefix tombstones (used for deletes of partitions)
			// are identified by having a version of math.MaxUint64
			m.currentTombstone = chosenKeyNoVersion
			if chosenVersion == math.MaxUint64 {
				m.isPrefixTombstone = true
			}
		}
		if !m.preserveTombstones && (isTombstone || chosenVersion == math.MaxUint64) {
			// We have a tombstone or a prefix tombstone end marker - skip past it
			// End marker also is identified as having a version of math.MaxUint64
			advanceHeapIter = 1
			continue
		}

		m.heap[0] = m.heap[1]
		return true, choosenKV, nil
	}
}

func (m *MergingIterator) Current() common.KV {
	if m.heap[0] < 0 {
		return common.KV{}
	}
	return m.iters[m.heap[0]].Current()
}

func (m *MergingIterator) Close() {
	for _, iter := range m.iters {
		iter.Close()
	}
}
