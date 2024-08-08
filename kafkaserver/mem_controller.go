package kafkaserver

import (
	"sync"
)

type MemController interface {
	Reserve(size int, evictCallback func())
}

func newDefaultMemController(maxSize int) *defaultMemController {
	return &defaultMemController{maxSize: uint64(maxSize)}
}

// defaultMemController keeps track of the memory used by all fetchers (there is one fetcher per partition), such that
// so that total cached batches for all partitions can be controlled, and we evict fairly with the oldest batches
// evicted first
type defaultMemController struct {
	entries []allocateEntry
	lock    sync.Mutex
	totSize uint64
	maxSize uint64
}

type allocateEntry struct {
	size          uint32
	evictCallback func()
}

func (d *defaultMemController) Foo() {

}

func (d *defaultMemController) Reserve(size int, evictCallback func()) {
	d.lock.Lock()
	d.entries = append(d.entries, allocateEntry{
		size:          uint32(size),
		evictCallback: evictCallback,
	})
	d.totSize += uint64(size)
	if d.totSize > d.maxSize {
		// remove entries until less than maxSize
		overhead := d.totSize - d.maxSize
		i := 0
		var tot uint64
		var entry allocateEntry
		for i, entry = range d.entries {
			tot += uint64(entry.size)
			entry.evictCallback()
			if tot >= overhead {
				break
			}
		}
		d.totSize -= tot
		d.entries = d.entries[i+1:]
	}
	d.lock.Unlock()
}
