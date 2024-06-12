package dev

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"sync"
	"sync/atomic"
	"time"
)

func NewInMemStore(delay time.Duration) *InMemStore {
	return &InMemStore{delay: delay}
}

// InMemStore - LocalStore is a simple cloud store used for testing
type InMemStore struct {
	store       sync.Map
	delay       time.Duration
	unavailable atomic.Bool
}

func (f *InMemStore) Get(key []byte) ([]byte, error) {
	if err := f.checkUnavailable(); err != nil {
		return nil, err
	}
	f.maybeAddDelay()
	skey := common.ByteSliceToStringZeroCopy(key)
	b, ok := f.store.Load(skey)
	if !ok {
		return nil, nil
	}
	if b == nil {
		panic("nil value in obj store")
	}
	bytes := b.([]byte)
	if len(bytes) == 0 {
		panic("empty bytes in obj store")
	}
	return bytes, nil //nolint:forcetypeassert
}

func (f *InMemStore) Put(key []byte, value []byte) error {
	if err := f.checkUnavailable(); err != nil {
		return err
	}
	f.maybeAddDelay()
	skey := common.ByteSliceToStringZeroCopy(key)
	log.Debugf("local cloud store %p adding blob with key %v value length %d", f, key, len(value))
	f.store.Store(skey, value)
	return nil
}

func (f *InMemStore) Delete(key []byte) error {
	if err := f.checkUnavailable(); err != nil {
		return err
	}
	log.Debugf("local cloud store %p deleting obj with key %v", f, key)
	f.maybeAddDelay()
	skey := common.ByteSliceToStringZeroCopy(key)
	f.store.Delete(skey)
	return nil
}

func (f *InMemStore) SetUnavailable(unavailable bool) {
	f.unavailable.Store(unavailable)
}

func (f *InMemStore) checkUnavailable() error {
	if f.unavailable.Load() {
		return errors.NewTektiteErrorf(errors.Unavailable, "cloud store is unavailable")
	}
	return nil
}

func (f *InMemStore) maybeAddDelay() {
	if f.delay != 0 {
		time.Sleep(f.delay)
	}
}

func (f *InMemStore) Size() int {
	size := 0
	f.store.Range(func(_, _ interface{}) bool {
		size++
		return true
	})
	return size
}

func (f *InMemStore) ForEach(fun func(key string, value []byte)) {
	f.store.Range(func(k, v any) bool {
		fun(k.(string), v.([]byte))
		return true
	})
}

func (f *InMemStore) Start() error {
	return nil
}

func (f *InMemStore) Stop() error {
	return nil
}
