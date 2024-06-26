package iteration

import (
	"bytes"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	log "github.com/spirit-labs/tektite/logger"
	"math"
)

type MergingIterator struct {
	highestVersion           uint64
	iters                    []Iterator
	iterHeads                []*common.KVV // Keep references to the latest Next() value returned from iters
	preserveTombstones       bool
	current                  common.KV
	currIndex                int
	minNonCompactableVersion uint64
	noDropOnNext             bool
	currentTombstone         []byte
	isPrefixTombstone        bool
}

func NewMergingIterator(iters []Iterator, preserveTombstones bool, highestVersion uint64) (*MergingIterator, error) {
	mi := &MergingIterator{
		highestVersion:           highestVersion,
		minNonCompactableVersion: math.MaxUint64,
		iters:                    iters,
		iterHeads:                make([]*common.KVV, len(iters)),
		preserveTombstones:       preserveTombstones,
	}
	return mi, nil
}

func NewCompactionMergingIterator(iters []Iterator, preserveTombstones bool, minNonCompactableVersion uint64) (*MergingIterator, error) {
	mi := &MergingIterator{
		highestVersion:           math.MaxUint64,
		minNonCompactableVersion: minNonCompactableVersion,
		iters:                    iters,
		iterHeads:                make([]*common.KVV, len(iters)),
		preserveTombstones:       preserveTombstones,
	}
	return mi, nil
}

func (m *MergingIterator) Next() (bool, common.KV, error) {
	repeat := true
	for repeat {
		// Now find the smallest key, choosing the highest version when keys draw
		var chosenKeyNoVersion []byte
		var smallestIndex int
		var choosen common.KVV
	outer:
		for i, iter := range m.iters {
			valid, c, err := m.readIterHeadOrNext(i, iter)
			if err != nil {
				return false, common.KV{}, err
			}
			if !valid {
				continue outer
			}

			keyNoVersion := c.Key[:len(c.Key)-8] // Key without version
			if chosenKeyNoVersion == nil {
				chosenKeyNoVersion = keyNoVersion
				smallestIndex = i
				choosen = c
			} else {
				diff := bytes.Compare(keyNoVersion, chosenKeyNoVersion)
				if diff < 0 {
					chosenKeyNoVersion = keyNoVersion
					smallestIndex = i
					choosen = c
				} else if diff == 0 {
					// Keys are same

					// choose the highest version, and drop the other one as long as the highest version < minNonCompactable
					if c.Version > choosen.Version {
						// the current version is higher so drop the previous highest if the current version is compactable
						// note we can only drop the previous highest if *this* version is compactable as dropping it
						// will leave this version, and if its non compactable it means that snapshot rollback could
						// remove it, which would leave nothing.
						if c.Version < m.minNonCompactableVersion {
							m.logKeyDroppingVersionLessThanMinNonCompactable(choosen.Version, choosen, 1)
							m.iterHeads[smallestIndex] = nil
						}
						chosenKeyNoVersion = keyNoVersion
						smallestIndex = i
						choosen = c
					} else if c.Version < choosen.Version {
						// the previous highest version is higher than this version, so we can remove this version
						// as long as previous highest is compactable
						if choosen.Version < m.minNonCompactableVersion {
							// drop this entry if the version is compactable
							m.iterHeads[i] = nil
							m.logKeyDroppingVersionLessThanMinNonCompactable(choosen.Version, c, 2)
						}
					} else {
						// same key, same version, drop this one, and keep the one we already found,
						m.logKeyDroppingSameKeyAndVersion(c)
						m.iterHeads[i] = nil
					}
				}
			}
		}

		if chosenKeyNoVersion == nil {
			// Nothing valid
			return false, common.KV{}, nil
		}

		if m.currentTombstone != nil {
			if bytes.Compare(m.currentTombstone, choosen.Key[:len(m.currentTombstone)]) == 0 {
				if m.isPrefixTombstone || choosen.Version < m.minNonCompactableVersion {
					// The key matches current prefix tombstone
					// skip past it if it is compactable - for prefixes we alwqays delete even if non compactable
					m.iterHeads[smallestIndex] = nil
					continue
				}
			} else {
				// does not match - reset the prefix tombstone
				m.currentTombstone = nil
				m.isPrefixTombstone = false
			}
		}

		isTombstone := len(choosen.Value) == 0
		if isTombstone {
			// We have a tombstone, keep track of it. Prefix tombstones (used for deletes of partitions)
			// are identified by having a version of math.MaxUint64
			m.currentTombstone = chosenKeyNoVersion
			if choosen.Version == math.MaxUint64 {
				m.isPrefixTombstone = true
			}
		}
		if !m.preserveTombstones && (isTombstone || choosen.Version == math.MaxUint64) {
			// We have a tombstone or a prefix tombstone end marker - skip past it
			// End marker also is identified as having a version of math.MaxUint64
			m.iterHeads[smallestIndex] = nil
		} else {
			// output the entry
			m.current.Key = choosen.Key
			m.current.Value = choosen.Value
			m.currIndex = smallestIndex
			repeat = false
		}
	}
	m.iterHeads[m.currIndex] = nil
	return true, m.current, nil
}

func (m *MergingIterator) readIterHeadOrNext(index int, iter Iterator) (bool, common.KVV, error) {
	head := m.iterHeads[index]
	if head != nil {
		return true, *head, nil
	}
	var ver uint64
	var lastKeyNoVersion []byte
	var lastKeyVersion uint64

	if len(m.current.Key) > 0 {
		lastKeyNoVersion = m.current.Key[:len(m.current.Key)-8]
		lastKeyVersion = encoding.DecodeKeyVersion(m.current.Key)
		if lastKeyVersion >= m.minNonCompactableVersion {
			// Cannot compact it
			// We set this flag to mark that we cannot drop any other proceeding same keys with lower versions either
			// If the first one is >= minNonCompactable but proceeding lower keys are < minNonCompactable they can't be
			// dropped either otherwise on rollback of snapshot we could be left with no versions of those keys.
			m.noDropOnNext = true
		}
	}

	for {
		valid, c, err := iter.Next()
		if err != nil || !valid {
			return valid, common.KVV{}, err
		}
		kvv := common.KVV{Key: c.Key, Value: c.Value, Version: encoding.DecodeKeyVersion(c.Key)}

		// Skip over same key (if it's compactable)
		// in same iterator we can have multiple versions of the same key
		if bytes.Equal(lastKeyNoVersion, kvv.Key[:len(kvv.Key)-8]) {
			if !m.noDropOnNext {
				m.logKeyDropping(kvv)
				continue
			}
		} else {
			m.noDropOnNext = false
		}

		// Skip past keys with too high a version
		// prefix tombstones always have version math.MaxUint64 and are never screened out
		if kvv.Version > m.highestVersion && kvv.Version != math.MaxUint64 {
			m.logKeyDropVersionTooHight(c.Key, ver)
			continue
		}

		m.iterHeads[index] = &kvv
		return true, kvv, nil
	}
}

func (m *MergingIterator) logKeyDropVersionTooHight(key []byte, version uint64) {
	if log.DebugEnabled {
		log.Debugf("%p merging iter skipping past key %v (%s) as version %d too high - max version %d",
			m, key, string(key), version, m.highestVersion)
	}
}

func (m *MergingIterator) logKeyDropping(c common.KVV) {
	if log.DebugEnabled {
		ver := encoding.DecodeKeyVersion(c.Key)
		lastKey := m.current.Key
		lastValue := m.current.Value
		lastVersion := encoding.DecodeKeyVersion(lastKey)
		log.Debugf("%p mi: dropping key in next as same key: key %v (%s) value %v (%s) version:%d last key: %v (%s) last value %v (%s) last version %d minnoncompactableversion:%d",
			m, c.Key, string(c.Key), c.Value, string(c.Value), ver, lastKey, string(lastKey), lastValue, string(lastValue), lastVersion, m.minNonCompactableVersion)
	}
}

func (m *MergingIterator) logKeyDroppingVersionLessThanMinNonCompactable(choosenVersion uint64, kv common.KVV, index int) {
	if log.DebugEnabled {
		log.Debugf("%p mi: dropping as key version %d less than minnoncompactable (%d) %d chosenKeyVersion %d: key %v (%s) value %v (%s)",
			m, kv.Version, index, m.minNonCompactableVersion, choosenVersion, kv.Key, string(kv.Key), kv.Value, string(kv.Value))
	}
}
func (m *MergingIterator) logKeyDroppingSameKeyAndVersion(kvv common.KVV) {
	if log.DebugEnabled {
		log.Debugf("%p mi: dropping key as same key and version: key %v (%s) value %v (%s) ver %d",
			m, kvv.Key, string(kvv.Key), kvv.Value, string(kvv.Value), kvv.Version)
	}
}

func (m *MergingIterator) Close() {
	for _, iter := range m.iters {
		iter.Close()
	}
}
