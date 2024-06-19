package mem

import (
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/arenaskl"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"math"
	"sync/atomic"
)

type DeleteRange struct {
	StartKey []byte
	EndKey   []byte
}

type Memtable struct {
	Uuid                 string
	nodeID               int
	maxSizeBytes         int
	arena                *arenaskl.Arena
	sl                   *arenaskl.Skiplist
	flushedCallbacksLock common.SpinLock
	flushedCallbacks     []func(error)
	hasWrites            atomic.Bool
	reservedSpace        int64
}

var MemtableSizeOverhead int64

func init() {
	arena := arenaskl.NewArena(8192)
	arenaskl.NewSkiplist(arena)
	MemtableSizeOverhead = int64(arena.Size())
}

func NewMemtable(arena *arenaskl.Arena, nodeID int, maxSizeBytes int) *Memtable {

	uu, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}

	log.Debugf("node %d creating memtable %s", nodeID, uu.String())

	sl := arenaskl.NewSkiplist(arena)
	mt := &Memtable{
		Uuid:         uu.String(),
		nodeID:       nodeID,
		maxSizeBytes: maxSizeBytes,
		arena:        arena,
		sl:           sl,
	}

	// The skiplist takes up a certain amount of space for head and tail nodes etc, we need to take this into account
	// in the reserved space
	mt.reservedSpace = MemtableSizeOverhead

	return mt
}

type writeBatch interface {
	MemTableBytes() int64
	Range(f func(key []byte, value []byte) bool)
}

func (m *Memtable) Write(batch writeBatch) (bool, error) {

	writeIter := arenaskl.Iterator{}
	writeIter.Init(m.sl)

	// Try and reserve some space - the memtable is arena based so has a hard bound on size and does not expand to
	// accommodate more entries, so we need to make sure there is enough space before we begin the write. And writes
	// can occur concurrently. If not enough room can be reserved then the memtable will be replaced and the caller will
	// retry.
	batchMemSize := batch.MemTableBytes()

	reserved := atomic.AddInt64(&m.reservedSpace, batchMemSize)
	if reserved > int64(m.arena.Cap()) {
		atomic.AddInt64(&m.reservedSpace, -batchMemSize)
		// Not enough room
		return false, nil
	}

	var err error
	batch.Range(func(key []byte, value []byte) bool {
		if err = writeIter.Add(key, value, 0); err != nil {
			if //goland:noinspection GoDirectComparisonOfErrors
			err == arenaskl.ErrRecordExists {
				err = writeIter.Set(value, 0)
				if err != nil {
					if //goland:noinspection GoDirectComparisonOfErrors
					err == arenaskl.ErrRecordUpdated {
						curr := writeIter.Value()
						// Should never occur as same key should always be written from same processor
						panic(fmt.Sprintf("concurrent update for key %s curr is %v", key, curr))
					}
					return false
				}
			}
		}
		if err != nil {
			return false
		}
		if log.DebugEnabled {
			ver := math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-8:])
			log.Debugf("key:%v version: %d value: %v stored in memtable %s node %d", key, ver, value, m.Uuid, m.nodeID)
		}
		return true
	})
	if err != nil {
		return false, err
	}
	if batchMemSize > 0 {
		m.hasWrites.Store(true)
	}
	return true, nil
}

func (m *Memtable) HasWrites() bool {
	return m.hasWrites.Load()
}

func (m *Memtable) AddFlushedCallback(flushedCallback func(error)) {
	m.flushedCallbacksLock.Lock()
	m.flushedCallbacks = append(m.flushedCallbacks, flushedCallback)
	m.flushedCallbacksLock.Unlock()
}

func (m *Memtable) Flushed(err error) {
	// If ok = true, the memtable has been successfully flushed to storage we now call all flushed callbacks
	// If ok = false, memtable not pushed - most likely store has been halted (in tests), we still call the callbacks
	// in this case, but with err
	m.flushedCallbacksLock.Lock()
	defer m.flushedCallbacksLock.Unlock()
	for _, cb := range m.flushedCallbacks {
		cb(err)
	}
	m.flushedCallbacks = nil
}

func (m *Memtable) GetLastKey() []byte {
	iterLast := arenaskl.Iterator{}
	iterLast.Init(m.sl)
	iterLast.SeekToLast()
	if !iterLast.Valid() {
		panic("no data in table")
	}
	return iterLast.Key()
}
