package mem

import (
	"bytes"
	"github.com/spirit-labs/tektite/arenaskl"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/iteration"
)

type MemtableIterator struct {
	it          *arenaskl.Iterator
	prevIt      *arenaskl.Iterator
	keyStart    []byte
	keyEnd      []byte
	curr        common.KV
	valid       bool
	initialSeek bool
}

func (m *Memtable) NewIterator(keyStart []byte, keyEnd []byte) iteration.Iterator {
	var it arenaskl.Iterator
	it.Init(m.sl)
	iter := &MemtableIterator{
		it:       &it,
		keyStart: keyStart,
		keyEnd:   keyEnd,
	}
	iter.doInitialSeek()
	return iter
}

func (m *MemtableIterator) Current() common.KV {
	if !m.valid {
		panic("not valid")
	}
	return m.curr
}

func (m *MemtableIterator) Next() error {
	// we make a copy of the iter before advancing in case we advance off the end (invalid) and later
	// more records arrive
	prevCopy := *m.it
	m.it.Next()
	m.prevIt = &prevCopy
	return nil
}

func (m *MemtableIterator) doInitialSeek() {
	if m.keyStart == nil {
		m.it.SeekToFirst()
	} else {
		m.it.Seek(m.keyStart)
	}
	if m.it.Valid() && (m.keyEnd == nil || bytes.Compare(m.it.Key(), m.keyEnd) < 0) {
		// We found a key in range, so we won't need to perform the initial seek again
		// If we didn't find a key in range we will try again the next time IsValid() is called - this allows for the
		// case where an iterator is created but no data in range, then data is written with data in range - this
		// should make the iterator valid
		m.initialSeek = true
	}
}

func (m *MemtableIterator) IsValid() (bool, error) {
	if !m.initialSeek {
		m.doInitialSeek()
	}

	if !m.it.Valid() {
		// Check the previous iter in case new entries were added
		if m.prevIt != nil {
			cp := *m.prevIt
			m.prevIt.Next()
			if m.prevIt.Valid() && (m.keyEnd == nil || bytes.Compare(m.prevIt.Key(), m.keyEnd) < 0) {
				// There are new entries - reset the iterator to prev.next
				m.it = m.prevIt
				m.prevIt = nil
				m.valid = true
				m.curr = common.KV{
					Key:   m.it.Key(),
					Value: m.it.Value(),
				}
				return true, nil
			} else {
				// Put the prevIter back - still not valid
				m.prevIt = &cp
				m.valid = false
				return false, nil
			}
		}
		m.valid = false
		return false, nil
	}

	if m.keyEnd == nil || bytes.Compare(m.it.Key(), m.keyEnd) < 0 {
		m.valid = true
		m.curr = common.KV{
			Key:   m.it.Key(),
			Value: m.it.Value(),
		}
		return true, nil
	}

	m.valid = false
	return false, nil
}

func (m *MemtableIterator) Close() {
}
