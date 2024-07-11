package mem

import (
	"bytes"
	"github.com/spirit-labs/tektite/arenaskl"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/iteration"
)

type MemtableIterator struct {
	it                         *arenaskl.Iterator
	keyStart                   []byte
	keyEnd                     []byte
	curr                       common.KV
	initialSeek                bool
	hasIterReturnedFirstResult bool
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

func (m *MemtableIterator) Next() (bool, common.KV, error) {
	if !m.initialSeek {
		m.doInitialSeek()
	}

	if m.it.Valid() && m.hasIterReturnedFirstResult {
		m.it.Next()
	}

	if !m.it.Valid() {
		return false, common.KV{}, nil
	}

	if m.keyEnd == nil || bytes.Compare(m.it.Key(), m.keyEnd) < 0 {
		m.curr = common.KV{
			Key:   m.it.Key(),
			Value: m.it.Value(),
		}
		m.hasIterReturnedFirstResult = true
		return true, m.curr, nil
	}

	m.curr = common.KV{}
	return false, m.curr, nil
}

func (m *MemtableIterator) Current() common.KV {
	return m.curr
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

func (m *MemtableIterator) Close() {
	m.it = nil
}
