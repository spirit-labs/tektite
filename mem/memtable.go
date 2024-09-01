package mem

import (
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	arenaskl2 "github.com/spirit-labs/tektite/asl/arenaskl"
	log "github.com/spirit-labs/tektite/logger"
	"math"
	"sync"
	"sync/atomic"
)

type Memtable struct {
	Uuid                 string
	nodeID               int
	maxSizeBytes         int
	arena                *arenaskl2.Arena
	sl                   *arenaskl2.Skiplist
	flushedCallbacksLock sync.Mutex
	flushedCallbacks     []func(error)
	hasWrites            atomic.Bool
	usedSpace            int64
}

var MemtableSizeOverhead int64

func init() {
	arena := arenaskl2.NewArena(8192)
	arenaskl2.NewSkiplist(arena)
	MemtableSizeOverhead = int64(arena.Size())
}

func NewMemtable(arena *arenaskl2.Arena, nodeID int, maxSizeBytes int) *Memtable {

	uu, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}

	log.Debugf("node %d creating memtable %s", nodeID, uu.String())

	sl := arenaskl2.NewSkiplist(arena)
	mt := &Memtable{
		Uuid:         uu.String(),
		nodeID:       nodeID,
		maxSizeBytes: maxSizeBytes,
		arena:        arena,
		sl:           sl,
	}

	// The skiplist takes up a certain amount of space for head and tail nodes etc, we need to take this into account
	// in the reserved space
	mt.usedSpace = MemtableSizeOverhead

	return mt
}

func (m *Memtable) Close() {
	m.arena = nil
	m.sl = nil
	m.flushedCallbacks = nil
}

type writeBatch interface {
	MemTableBytes() int64
	Range(f func(key []byte, value []byte) bool)
}

func (m *Memtable) Write(batch writeBatch) (bool, error) {

	writeIter := arenaskl2.Iterator{}
	writeIter.Init(m.sl)

	// Try and reserve some space - the memtable is arena based so has a hard bound on size and does not expand to
	// accommodate more entries, so we need to make sure there is enough space before we begin the write. And writes
	// can occur concurrently. If not enough room can be reserved then the memtable will be replaced and the caller will
	// retry.
	batchMemSize := batch.MemTableBytes()

	used := m.usedSpace + batchMemSize
	if used > int64(m.arena.Cap()) {
		// Not enough room
		return false, nil
	}
	m.usedSpace = used

	var err error
	batch.Range(func(key []byte, value []byte) bool {
		if err = writeIter.Add(key, value, 0); err != nil {
			if //goland:noinspection GoDirectComparisonOfErrors
			err == arenaskl2.ErrRecordExists {
				err = writeIter.Set(value, 0)
				if err != nil {
					if //goland:noinspection GoDirectComparisonOfErrors
					err == arenaskl2.ErrRecordUpdated {
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
	defer m.flushedCallbacksLock.Unlock()
	m.flushedCallbacks = append(m.flushedCallbacks, flushedCallback)
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
