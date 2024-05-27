package iteration

import (
	"bytes"
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"math"
)

type MergingIterator struct {
	highestVersion           uint64
	iters                    []Iterator
	preserveTombstones       bool
	current                  common.KV
	currIndex                int
	minNonCompactableVersion uint64
	noDropOnNext             bool
}

func NewMergingIterator(iters []Iterator, preserveTombstones bool, highestVersion uint64) (*MergingIterator, error) {
	mi := &MergingIterator{
		highestVersion:           highestVersion,
		minNonCompactableVersion: math.MaxUint64,
		iters:                    iters,
		preserveTombstones:       preserveTombstones,
	}
	return mi, nil
}

func NewCompactionMergingIterator(iters []Iterator, preserveTombstones bool, minNonCompactableVersion uint64) (*MergingIterator, error) {
	mi := &MergingIterator{
		highestVersion:           math.MaxUint64,
		minNonCompactableVersion: minNonCompactableVersion,
		iters:                    iters,
		preserveTombstones:       preserveTombstones,
	}
	return mi, nil
}

func (m *MergingIterator) PrependIterator(iter Iterator) error {
	iters := make([]Iterator, 0, len(m.iters)+1)
	iters = append(iters, iter)
	iters = append(iters, m.iters...)
	m.iters = iters
	_, err := m.IsValid()
	return err
}

func (m *MergingIterator) IsValid() (bool, error) {
	repeat := true
	for repeat {
		// Now find the smallest key, choosing the highest version when keys draw
		var smallestKeyNoVersion []byte
		var smallestIndex int
		var highestVersionSameKey uint64
		var chosenKey []byte
		var chosenValue []byte
	outer:
		for i, iter := range m.iters {
			var c common.KV
			var ver uint64
			for {
				valid, err := iter.IsValid()
				if err != nil {
					return false, err
				}
				if !valid {
					continue outer
				}
				c = iter.Current()

				// Skip past keys with too high a version
				// - version is stored inverted so higher version comes before lower version for same key
				ver = math.MaxUint64 - binary.BigEndian.Uint64(c.Key[len(c.Key)-8:])
				if ver <= m.highestVersion {
					break
				}
				if log.DebugEnabled {
					log.Debugf("%p merging iter skipping past key %v (%s) as version %d too high - max version %d",
						m, c.Key, string(c.Key), ver, m.highestVersion)
				}
				if err := iter.Next(); err != nil {
					return false, err
				}
			}
			keyNoVersion := c.Key[:len(c.Key)-8] // Key without version
			if smallestKeyNoVersion == nil {
				smallestKeyNoVersion = keyNoVersion
				smallestIndex = i
				highestVersionSameKey = ver
				chosenKey = c.Key
				chosenValue = c.Value
			} else {
				diff := bytes.Compare(keyNoVersion, smallestKeyNoVersion)
				if diff < 0 {
					smallestKeyNoVersion = keyNoVersion
					smallestIndex = i
					highestVersionSameKey = ver
					chosenKey = c.Key
					chosenValue = c.Value
				} else if diff == 0 {
					// Keys are same

					// choose the highest version, and drop the other one as long as the highest version < minNonCompactable
					if ver > highestVersionSameKey {
						// the current version is higher so drop the previous highest if the current version is compactable
						// note we can only drop the previous highest if *this* version is compactable as dropping it
						// will leave this version, and if its non compactable it means that snapshot rollback could
						// remove it, which would leave nothing.
						if ver < m.minNonCompactableVersion {
							if log.DebugEnabled {
								log.Debugf("%p mi: dropping as key version %d less than minnoncompactable (1) %d highestVersionSameKey %d: key %v (%s) value %v (%s)",
									m, highestVersionSameKey, m.minNonCompactableVersion, highestVersionSameKey, chosenKey, string(chosenKey), chosenValue, string(chosenValue))
							}
							if err := m.iters[smallestIndex].Next(); err != nil {
								return false, err
							}
						}
						smallestKeyNoVersion = keyNoVersion
						smallestIndex = i
						highestVersionSameKey = ver
						chosenKey = c.Key
						chosenValue = c.Value
					} else if ver < highestVersionSameKey {
						// the previous highest version is higher than this version, so we can remove this version
						// as long as previous highest is compactable
						if highestVersionSameKey < m.minNonCompactableVersion {
							// drop this entry if the version is compactable
							if err := iter.Next(); err != nil { // Advance iter as not the highest version
								return false, err
							}
							if log.DebugEnabled {
								log.Debugf("%p mi: dropping as key version %d less than minnoncompactable (2) %d highestVersionSameKey %d: key %v (%s) value %v (%s)",
									m, ver, m.minNonCompactableVersion, highestVersionSameKey, c.Key, string(c.Key), c.Value, string(c.Value))
							}
						}
					} else {
						// same key, same version, drop this one, and keep the one we already found,
						if log.DebugEnabled {
							log.Debugf("%p mi: dropping key as same key and version: key %v (%s) value %v (%s) ver %d",
								m, c.Key, string(c.Key), c.Value, string(c.Value), ver)
						}
						if err := iter.Next(); err != nil {
							return false, err
						}
					}
				}
			}
		}

		if smallestKeyNoVersion == nil {
			// Nothing valid
			return false, nil
		}

		if len(chosenValue) == 0 && !m.preserveTombstones {
			// Tombstone - advance the iter
			if err := m.iters[smallestIndex].Next(); err != nil {
				return false, err
			}
			// We will repeat the loop
		} else {
			m.current.Key = chosenKey
			m.current.Value = chosenValue
			m.currIndex = smallestIndex
			repeat = false
		}
	}
	return true, nil
}

func (m *MergingIterator) Current() common.KV {
	return m.current
}

func (m *MergingIterator) Next() error {
	lastKeyNoVersion := m.current.Key[:len(m.current.Key)-8]
	lastKeyVersion := math.MaxUint64 - binary.BigEndian.Uint64(m.current.Key[len(m.current.Key)-8:])

	if err := m.iters[m.currIndex].Next(); err != nil {
		return err
	}

	if lastKeyVersion >= m.minNonCompactableVersion {
		// Cannot compact it
		// We set this flag to mark that we cannot drop any other proceeding same keys with lower versions either
		// If the first one is >= minNonCompactable but proceeding lower keys are < minNonCompactable they can't be
		// dropped either otherwise on rollback of snapshot we could be left with no versions of those keys.
		m.noDropOnNext = true
		return nil
	}

	/*
		Possibly can be improved? In the common case of keys with no runs of same key, then we evaluate
		isValid() below to see if key is same, and if not, isValid() will be called again in loop by user.
		We can move the logic of skipping past same key from here into the isValid method
	*/
	for _, iter := range m.iters {
		var c common.KV
		for {
			valid, err := iter.IsValid()
			if err != nil {
				return err
			}
			if !valid {
				break
			}
			c = iter.Current()
			// Skip over same key (if it's compactable)- in same iterator we can have multiple versions of the same key
			ver := math.MaxUint64 - binary.BigEndian.Uint64(c.Key[len(c.Key)-8:])
			if bytes.Equal(lastKeyNoVersion, c.Key[:len(c.Key)-8]) {
				if !m.noDropOnNext {
					if log.DebugEnabled {
						lastKey := m.current.Key
						lastValue := m.current.Value
						lastVersion := math.MaxUint64 - binary.BigEndian.Uint64(lastKey[len(lastKey)-8:])
						log.Debugf("%p mi: dropping key in next as same key: key %v (%s) value %v (%s) version:%d last key: %v (%s) last value %v (%s) last version %d minnoncompactableversion:%d",
							m, c.Key, string(c.Key), c.Value, string(c.Value), ver, lastKey, string(lastKey), lastValue, string(lastValue), lastVersion, m.minNonCompactableVersion)
					}
					if err := iter.Next(); err != nil {
						return err
					}
					continue
				}
			} else {
				m.noDropOnNext = false
			}
			break
		}
	}
	return nil
}

func (m *MergingIterator) Close() {
	for _, iter := range m.iters {
		iter.Close()
	}
}
