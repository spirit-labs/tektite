package sst

import (
	"bytes"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/iteration"
	log "github.com/spirit-labs/tektite/logger"
)

func (s *SSTable) NewIterator(keyStart []byte, keyEnd []byte) (iteration.Iterator, error) {
	offset := s.findOffset(keyStart)
	si := &SSTableIterator{
		ss:         s,
		nextOffset: offset,
		keyEnd:     keyEnd,
	}
	return si, nil
}

type SSTableIterator struct {
	ss         *SSTable
	nextOffset int
	valid      bool
	currkV     common.KV
	keyEnd     []byte
}

func (si *SSTableIterator) Next() (bool, common.KV, error) {
	if si.nextOffset == -1 {
		si.valid = false
		return false, common.KV{}, nil
	}
	indexOffset := int(si.ss.indexOffset)
	var kl, vl uint32
	kl, si.nextOffset = encoding.ReadUint32FromBufferLE(si.ss.data, si.nextOffset)
	k := si.ss.data[si.nextOffset : si.nextOffset+int(kl)]
	if si.keyEnd != nil && bytes.Compare(k, si.keyEnd) >= 0 {
		// End of range
		si.nextOffset = -1
		si.valid = false
		return false, common.KV{}, nil
	} else {
		si.currkV.Key = k
		si.nextOffset += int(kl)
		vl, si.nextOffset = encoding.ReadUint32FromBufferLE(si.ss.data, si.nextOffset)
		if vl == 0 {
			si.currkV.Value = nil
		} else {
			si.currkV.Value = si.ss.data[si.nextOffset : si.nextOffset+int(vl)]
		}
		si.nextOffset += int(vl)
		if si.nextOffset >= indexOffset { // Start of index data marks end of entries data
			// Reached end of SSTable
			si.nextOffset = -1
		}
		si.valid = true
		return true, si.currkV, nil
	}
}

func (si *SSTableIterator) Current() common.KV {
	return si.currkV
}

func (si *SSTableIterator) Close() {
}

type tableGetter interface {
	GetSSTable(tableID SSTableID) (*SSTable, error)
}

type LazySSTableIterator struct {
	tableID     SSTableID
	tableCache  tableGetter
	keyStart    []byte
	keyEnd      []byte
	iter        iteration.Iterator
	iterFactory func(sst *SSTable, keyStart []byte, keyEnd []byte) (iteration.Iterator, error)
}

func NewLazySSTableIterator(tableID SSTableID, tableCache tableGetter,
	keyStart []byte, keyEnd []byte, factory func(sst *SSTable, keyStart []byte, keyEnd []byte) (iteration.Iterator, error)) (iteration.Iterator, error) {
	it := &LazySSTableIterator{
		tableID:     tableID,
		tableCache:  tableCache,
		keyStart:    keyStart,
		keyEnd:      keyEnd,
		iterFactory: factory,
	}
	return it, nil
}

func (l *LazySSTableIterator) Next() (bool, common.KV, error) {
	iter, err := l.getIter()
	if err != nil {
		return false, common.KV{}, err
	}
	return iter.Next()
}

func (l *LazySSTableIterator) Current() common.KV {
	if l.iter == nil {
		return common.KV{}
	}
	return l.iter.Current()
}

func (l *LazySSTableIterator) Close() {
	if l.iter != nil {
		l.iter.Close()
	}
}

//goland:noinspection GoUnusedFunction
func dumpSST(id SSTableID, sst *SSTable) {
	sstIter, err := sst.NewIterator(nil, nil)
	if err != nil {
		panic(err)
	}
	log.Debugf("==============Dumping sstable: %v", id)
	for {
		valid, current, err := sstIter.Next()
		if err != nil {
			panic(err)
		}
		if !valid {
			break
		}
		log.Debugf("key: %v (%s) value: %v (%s)", current.Key, string(current.Key),
			current.Value, string(current.Value))
	}
	log.Debugf("==============End Dumping sstable: %v", id)
}

func (l *LazySSTableIterator) getIter() (iteration.Iterator, error) {
	if l.iter == nil {
		ssTable, err := l.tableCache.GetSSTable(l.tableID)
		if err != nil {
			return nil, err
		}
		//if log.DebugEnabled {
		//	DumpLock.Lock()
		//	defer DumpLock.Unlock()
		//	dumpSST(l.tableID, ssTable)
		//}

		iter, err := l.iterFactory(ssTable, l.keyStart, l.keyEnd)
		if err != nil {
			return nil, err
		}
		l.iter = iter
		return iter, err
	}
	return l.iter, nil
}
