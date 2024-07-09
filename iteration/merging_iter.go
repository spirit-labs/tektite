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
	iterHeads                []common.KV // Keep references to the latest Next() value returned from iters
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
		iterHeads:                make([]common.KV, len(iters)),
		preserveTombstones:       preserveTombstones,
	}
	return mi, nil
}

func NewCompactionMergingIterator(iters []Iterator, preserveTombstones bool, minNonCompactableVersion uint64) (*MergingIterator, error) {
	mi := &MergingIterator{
		highestVersion:           math.MaxUint64,
		minNonCompactableVersion: minNonCompactableVersion,
		iters:                    iters,
		iterHeads:                make([]common.KV, len(iters)),
		preserveTombstones:       preserveTombstones,
	}
	return mi, nil
}

func (m *MergingIterator) Next() (bool, common.KV, error) {
	var lastKeyNoVersion []byte
	if len(m.current.Key) > 0 {
		lastKeyNoVersion = m.current.Key[:len(m.current.Key)-8]
		if encoding.DecodeKeyVersion(m.current.Key) >= m.minNonCompactableVersion {
			// Cannot compact it
			// We set this flag to mark that we cannot drop any other proceeding same keys with lower versions either
			// If the first one is >= minNonCompactable but proceeding lower keys are < minNonCompactable they can't be
			// dropped either otherwise on rollback of snapshot we could be left with no versions of those keys.
			m.noDropOnNext = true
		}
	}

	repeat := true
	for repeat {
		// Now find the smallest key from all iterators, choosing the highest version when keys draw
		var keyNoVersion, chosenKeyNoVersion []byte
		var smallestIndex int
		var choosen common.KV
		var version, choosenVersion uint64
		var err error
		var valid bool
	outer:
		for i := range m.iters {
			if len(m.iterHeads[i].Key) == 0 {
				// Find the next valid iter's KV that is not compactable and has a version that is not too high
				for {
					valid, m.iterHeads[i], err = m.iters[i].Next()
					if err != nil {
						return false, common.KV{}, err
					}
					if !valid {
						continue outer
					}

					// Skip over same key (if it's compactable)
					// in same iterator we can have multiple versions of the same key
					keyNoVersion = m.iterHeads[i].Key[:len(m.iterHeads[i].Key)-8]
					if bytes.Equal(lastKeyNoVersion, m.iterHeads[i].Key[:len(m.iterHeads[i].Key)-8]) {
						if !m.noDropOnNext {
							if log.DebugEnabled {
								lastVersion := encoding.DecodeKeyVersion(m.current.Key)
								log.Debugf("%p mi: dropping key in next as same key: key %v (%s) value %v (%s) version:%d last key: %v (%s) last value %v (%s) last version %d minnoncompactableversion:%d",
									m, m.iterHeads[i].Key, string(m.iterHeads[i].Key), m.iterHeads[i].Value, string(m.iterHeads[i].Value), version, m.current.Key, string(m.current.Key), m.current.Value, string(m.current.Value), lastVersion, m.minNonCompactableVersion)
							}
							continue
						}
					} else {
						m.noDropOnNext = false
					}
					// Skip past keys with too high a version
					// prefix tombstones always have version math.MaxUint64 and are never screened out
					version = encoding.DecodeKeyVersion(m.iterHeads[i].Key)
					if version > m.highestVersion && version != math.MaxUint64 {
						if log.DebugEnabled {
							log.Debugf("%p merging iter skipping past key %v (%s) as version %d too high - max version %d",
								m, m.iterHeads[i].Key, string(m.iterHeads[i].Key), version, m.highestVersion)
						}
						continue
					}
					break
				}
			} else {
				version = encoding.DecodeKeyVersion(m.iterHeads[i].Key)
				keyNoVersion = m.iterHeads[i].Key[:len(m.iterHeads[i].Key)-8]
			}

			if chosenKeyNoVersion == nil {
				chosenKeyNoVersion = keyNoVersion
				smallestIndex = i
				choosen = m.iterHeads[i]
				choosenVersion = version
			} else {
				diff := bytes.Compare(keyNoVersion, chosenKeyNoVersion)
				if diff < 0 {
					chosenKeyNoVersion = keyNoVersion
					smallestIndex = i
					choosen = m.iterHeads[i]
					choosenVersion = version
				} else if diff == 0 {
					// Keys are same

					// choose the highest version, and drop the other one as long as the highest version < minNonCompactable
					if version > choosenVersion {
						// the current version is higher so drop the previous highest if the current version is compactable
						// note we can only drop the previous highest if *this* version is compactable as dropping it
						// will leave this version, and if its non compactable it means that snapshot rollback could
						// remove it, which would leave nothing.
						if version < m.minNonCompactableVersion {
							if log.DebugEnabled {
								log.Debugf("%p mi: dropping as key version %d less than minnoncompactable (%d) %d chosenKeyVersion %d: key %v (%s) value %v (%s)",
									m, version, 1, m.minNonCompactableVersion, choosenVersion, m.iterHeads[i].Key, string(m.iterHeads[i].Key), m.iterHeads[i].Value, string(m.iterHeads[i].Value))
							}
							m.iterHeads[smallestIndex] = common.KV{}
						}
						chosenKeyNoVersion = keyNoVersion
						smallestIndex = i
						choosen = m.iterHeads[i]
						choosenVersion = version
					} else if version < choosenVersion {
						// the previous highest version is higher than this version, so we can remove this version
						// as long as previous highest is compactable
						if choosenVersion < m.minNonCompactableVersion {
							// drop this entry if the version is compactable
							m.iterHeads[i] = common.KV{}
							if log.DebugEnabled {
								log.Debugf("%p mi: dropping as key version %d less than minnoncompactable (%d) %d chosenKeyVersion %d: key %v (%s) value %v (%s)",
									m, version, 2, m.minNonCompactableVersion, choosenVersion, choosen.Key, string(choosen.Key), choosen.Value, string(choosen.Value))
							}
						}
					} else {
						// same key, same version, drop this one, and keep the one we already found,
						if log.DebugEnabled {
							log.Debugf("%p mi: dropping key as same key and version: key %v (%s) value %v (%s) ver %d",
								m, m.iterHeads[i].Key, string(m.iterHeads[i].Key), m.iterHeads[i].Value, string(m.iterHeads[i].Value), version)
						}
						m.iterHeads[i] = common.KV{}
					}
				}
			}
		}

		if chosenKeyNoVersion == nil {
			// Nothing valid
			return false, common.KV{}, nil
		}

		if m.currentTombstone != nil {
			if bytes.Equal(m.currentTombstone, choosen.Key[:len(m.currentTombstone)]) {
				if m.isPrefixTombstone || choosenVersion < m.minNonCompactableVersion {
					// The key matches current prefix tombstone
					// skip past it if it is compactable - for prefixes we alwqays delete even if non compactable
					m.iterHeads[smallestIndex] = common.KV{}
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
			if choosenVersion == math.MaxUint64 {
				m.isPrefixTombstone = true
			}
		}
		if !m.preserveTombstones && (isTombstone || choosenVersion == math.MaxUint64) {
			// We have a tombstone or a prefix tombstone end marker - skip past it
			// End marker also is identified as having a version of math.MaxUint64
			m.iterHeads[smallestIndex] = common.KV{}
		} else {
			// output the entry
			m.current.Key = choosen.Key
			m.current.Value = choosen.Value
			m.currIndex = smallestIndex
			repeat = false
		}
	}
	m.iterHeads[m.currIndex] = common.KV{}
	return true, m.current, nil
}

func (m *MergingIterator) Close() {
	for _, iter := range m.iters {
		iter.Close()
	}
}
