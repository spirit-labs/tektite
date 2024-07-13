package iteration

import (
	"bytes"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	log "github.com/spirit-labs/tektite/logger"
	"math"
)

type MergingIterator struct {
	highestVersion             uint64
	iters                      []Iterator
	dropIterCurrent            []bool
	preserveTombstones         bool
	current                    common.KV
	currIndex                  int
	minNonCompactableVersion   uint64
	currentTombstone           []byte
	isPrefixTombstone          bool
	firstSameKey               []byte
	firstSameKeyNonCompactable bool
}

func NewMergingIterator(iters []Iterator, preserveTombstones bool, highestVersion uint64) (*MergingIterator, error) {
	mi := &MergingIterator{
		highestVersion:           highestVersion,
		minNonCompactableVersion: math.MaxUint64,
		iters:                    iters,
		dropIterCurrent:          make([]bool, len(iters)),
		preserveTombstones:       preserveTombstones,
	}
	return mi, nil
}

func NewCompactionMergingIterator(iters []Iterator, preserveTombstones bool, minNonCompactableVersion uint64) (*MergingIterator, error) {
	mi := &MergingIterator{
		highestVersion:           math.MaxUint64,
		minNonCompactableVersion: minNonCompactableVersion,
		iters:                    iters,
		dropIterCurrent:          make([]bool, len(iters)),
		preserveTombstones:       preserveTombstones,
	}
	return mi, nil
}

func (m *MergingIterator) Next() (bool, common.KV, error) {

	var lastKeyNoVersion []byte
	if len(m.current.Key) > 0 {
		// If a key is not compactable because version >= minNonCompactableVersion then all versions of that key
		// are also non compactable, even if the other versions are < minNonCompactableVersion
		// so, we need to keep track of when the last key changes then decide whether that key is compactable
		// then we won't skip past any entries that are non compactable
		lastKeyNoVersion = m.current.Key[:len(m.current.Key)-8]
		if m.firstSameKey == nil || !bytes.Equal(m.firstSameKey, lastKeyNoVersion) {
			m.firstSameKey = lastKeyNoVersion
			m.firstSameKeyNonCompactable = encoding.DecodeKeyVersion(m.current.Key) >= m.minNonCompactableVersion
		}
	}

	repeat := true
	for repeat {
		// Now find the smallest key from all iterators, choosing the highest version when keys draw
		var keyNoVersion, chosenKeyNoVersion []byte
		var smallestIndex int
		var chosen common.KV
		var version, chosenVersion uint64
		var err error
		var valid bool
	outer:
		for i := range m.iters {
			c := m.iters[i].Current()
			if len(c.Key) == 0 || m.dropIterCurrent[i] {
				for {
					// Find the next valid iters KV that is compactable and has a version that is not too high
					valid, c, err = m.iters[i].Next()
					if err != nil {
						return false, common.KV{}, err
					}
					if !valid {
						continue outer
					}

					version = encoding.DecodeKeyVersion(c.Key)
					// Skip over same key (if it's compactable)
					// in same iterator we can have multiple versions of the same key
					keyNoVersion = c.Key[:len(c.Key)-8]
					if !m.firstSameKeyNonCompactable && bytes.Equal(lastKeyNoVersion, keyNoVersion) {
						if log.DebugEnabled {
							lastVersion := encoding.DecodeKeyVersion(m.current.Key)
							log.Debugf("%p mi: dropping key in next as same key: key %v (%s) value %v (%s) version:%d last key: %v (%s) last value %v (%s) last version %d minnoncompactableversion:%d",
								m, c.Key, string(c.Key), c.Value, string(c.Value), version, m.current.Key, string(m.current.Key), m.current.Value, string(m.current.Value), lastVersion, m.minNonCompactableVersion)
						}
						continue
					}
					// Skip past keys with too high a version
					// prefix tombstones always have version math.MaxUint64 and are never screened out
					if version > m.highestVersion && version != math.MaxUint64 {
						if log.DebugEnabled {
							log.Debugf("%p merging iter skipping past key %v (%s) as version %d too high - max version %d",
								m, c.Key, string(c.Key), version, m.highestVersion)
						}
						continue
					}
					m.dropIterCurrent[i] = false
					break
				}
			} else {
				version = encoding.DecodeKeyVersion(c.Key)
				keyNoVersion = c.Key[:len(c.Key)-8]
			}

			if chosenKeyNoVersion == nil {
				chosenKeyNoVersion = keyNoVersion
				smallestIndex = i
				chosen = c
				chosenVersion = version
			} else {
				diff := bytes.Compare(keyNoVersion, chosenKeyNoVersion)
				if diff < 0 {
					chosenKeyNoVersion = keyNoVersion
					smallestIndex = i
					chosen = c
					chosenVersion = version
				} else if diff == 0 {
					// Keys are same

					// choose the highest version, and drop the other one as long as the highest version < minNonCompactable
					if version > chosenVersion {
						// the current version is higher so drop the previous highest if the current version is compactable
						// note we can only drop the previous highest if *this* version is compactable as dropping it
						// will leave this version, and if its non compactable it means that snapshot rollback could
						// remove it, which would leave nothing.
						if version < m.minNonCompactableVersion {
							if log.DebugEnabled {
								log.Debugf("%p mi: dropping as key version %d less than minnoncompactable (%d) %d chosenKeyVersion %d: key %v (%s) value %v (%s)",
									m, version, 1, m.minNonCompactableVersion, chosenVersion, c.Key, string(c.Key), c.Value, string(c.Value))
							}
							m.dropIterCurrent[smallestIndex] = true
						}
						chosenKeyNoVersion = keyNoVersion
						smallestIndex = i
						chosen = c
						chosenVersion = version
					} else if version < chosenVersion {
						// the previous highest version is higher than this version, so we can remove this version
						// as long as previous highest is compactable
						if chosenVersion < m.minNonCompactableVersion {
							// drop this entry if the version is compactable
							if log.DebugEnabled {
								log.Debugf("%p mi: dropping as key version %d less than minnoncompactable (%d) %d chosenKeyVersion %d: key %v (%s) value %v (%s)",
									m, version, 2, m.minNonCompactableVersion, chosenVersion, chosen.Key, string(chosen.Key), chosen.Value, string(chosen.Value))
							}
							m.dropIterCurrent[i] = true
						}
					} else {
						// same key, same version, drop this one, and keep the one we already found,
						if log.DebugEnabled {
							log.Debugf("%p mi: dropping key as same key and version: key %v (%s) value %v (%s) ver %d",
								m, c.Key, string(c.Key), c.Value, string(c.Value), version)
						}
						m.dropIterCurrent[i] = true
					}
				}
			}
		}

		if chosenKeyNoVersion == nil {
			// Nothing valid
			m.current = common.KV{}
			return false, common.KV{}, nil
		}

		if m.currentTombstone != nil {
			if bytes.Equal(m.currentTombstone, chosen.Key[:len(m.currentTombstone)]) {
				if m.isPrefixTombstone || chosenVersion < m.minNonCompactableVersion {
					// The key matches current prefix tombstone
					// skip past it if it is compactable - for prefixes we alwqays delete even if non compactable
					m.dropIterCurrent[smallestIndex] = true
					continue
				}
			} else {
				// does not match - reset the prefix tombstone
				m.currentTombstone = nil
				m.isPrefixTombstone = false
			}
		}

		isTombstone := len(chosen.Value) == 0
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
			m.dropIterCurrent[smallestIndex] = true
		} else {
			// output the entry
			m.current.Key = chosen.Key
			m.current.Value = chosen.Value
			m.currIndex = smallestIndex
			repeat = false
		}
	}
	m.dropIterCurrent[m.currIndex] = true
	return true, m.current, nil
}

func (m *MergingIterator) Current() common.KV {
	return m.current
}

func (m *MergingIterator) Close() {
	for _, iter := range m.iters {
		iter.Close()
	}
}
