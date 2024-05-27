package kafkaserver

import (
	"sync"
)

type Allocator interface {
	Allocate(size int, evictCallback func()) ([]byte, error)
}

func newDefaultAllocator(maxSize int) *defaultAllocator {
	return &defaultAllocator{maxSize: uint64(maxSize)}
}

type defaultAllocator struct {
	entries []allocateEntry
	lock    sync.Mutex
	totSize uint64
	maxSize uint64
}

type allocateEntry struct {
	size          uint32
	evictCallback func()
}

func (d *defaultAllocator) Allocate(size int, evictCallback func()) ([]byte, error) {
	buff := make([]byte, size) // allocate before the lock as we want to minimise critical section
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
	return buff, nil
}

type directAllocator struct {
}

func (d *directAllocator) Allocate(size int, _ func()) ([]byte, error) {
	return make([]byte, size), nil
}
