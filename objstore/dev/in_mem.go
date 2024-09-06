package dev

import (
	"context"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func NewInMemStore(delay time.Duration) *InMemStore {
	return &InMemStore{delay: delay}
}

var _ objstore.Client = &InMemStore{}

// InMemStore - LocalStore is a simple cloud store used for testing
type InMemStore struct {
	store       sync.Map
	delay       time.Duration
	unavailable atomic.Bool
	condLock    sync.Mutex
}

type valueHolder struct {
	value []byte
	info  objstore.ObjectInfo
}

func internalKey(bucket string, key string) string {
	return bucket + ":" + key
}

func (im *InMemStore) Get(_ context.Context, bucket string, key string) ([]byte, error) {
	if err := im.checkUnavailable(); err != nil {
		return nil, err
	}
	im.maybeAddDelay()
	skey := internalKey(bucket, key)
	v, ok := im.store.Load(skey)
	if !ok {
		return nil, nil
	}
	if v == nil {
		panic("nil value in obj store")
	}
	holder := v.(valueHolder)
	return holder.value, nil //nolint:forcetypeassert
}

func (im *InMemStore) Put(_ context.Context, bucket string, key string, value []byte) error {
	if err := im.checkUnavailable(); err != nil {
		return err
	}
	im.maybeAddDelay()
	skey := internalKey(bucket, key)
	log.Debugf("local cloud store %p adding blob with key %v value length %d", im, key, len(value))
	im.store.Store(skey, valueHolder{value: value, info: objstore.ObjectInfo{Key: key, LastModified: time.Now().UTC()}})
	return nil
}

func (im *InMemStore) PutIfNotExists(_ context.Context, bucket string, key string, value []byte) (bool, error) {
	if err := im.checkUnavailable(); err != nil {
		return false, err
	}
	im.maybeAddDelay()
	im.condLock.Lock()
	defer im.condLock.Unlock()
	skey := internalKey(bucket, key)
	_, exists := im.store.Load(skey)
	if exists {
		return false, nil
	}
	im.store.Store(skey, valueHolder{value: value, info: objstore.ObjectInfo{Key: key, LastModified: time.Now()}})
	return true, nil
}

func (im *InMemStore) ListObjectsWithPrefix(_ context.Context, bucket string, prefix string, maxKeys int) ([]objstore.ObjectInfo, error) {
	sPref := internalKey(bucket, prefix)
	var infos []objstore.ObjectInfo
	if maxKeys == -1 {
		maxKeys = math.MaxInt
	}
	lb := len(bucket)
	im.store.Range(func(k, v interface{}) bool {
		key := k.(string)
		if strings.HasPrefix(key, sPref) {
			infos = append(infos, objstore.ObjectInfo{
				Key:          key[lb + 1:],
				LastModified: v.(valueHolder).info.LastModified,
			})
		}
		return true
	})
	sort.SliceStable(infos, func(i, j int) bool {
		return strings.Compare(infos[i].Key, infos[j].Key) < 0
	})
	if len(infos) > maxKeys {
		infos = infos[:maxKeys]
	}
	return infos, nil
}

func (im *InMemStore) Delete(_ context.Context, bucket string, key string) error {
	if err := im.checkUnavailable(); err != nil {
		return err
	}
	log.Debugf("local cloud store %p deleting obj with key %v", im, key)
	im.maybeAddDelay()
	im.store.Delete(internalKey(bucket, key))
	return nil
}

func (im *InMemStore) DeleteAll(_ context.Context, bucket string, keys []string) error {
	if err := im.checkUnavailable(); err != nil {
		return err
	}
	im.maybeAddDelay()
	for _, key := range keys {
		im.store.Delete(internalKey(bucket, key))
	}
	return nil
}

func (im *InMemStore) SetUnavailable(unavailable bool) {
	im.unavailable.Store(unavailable)
}

func (im *InMemStore) checkUnavailable() error {
	if im.unavailable.Load() {
		return common.NewTektiteErrorf(common.Unavailable, "cloud store is unavailable")
	}
	return nil
}

func (im *InMemStore) maybeAddDelay() {
	if im.delay != 0 {
		time.Sleep(im.delay)
	}
}

func (im *InMemStore) Size() int {
	size := 0
	im.store.Range(func(_, _ interface{}) bool {
		size++
		return true
	})
	return size
}

func (im *InMemStore) ForEach(fun func(key string, value []byte)) {
	im.store.Range(func(k, v any) bool {
		fun(k.(string), v.([]byte))
		return true
	})
}

func (im *InMemStore) Start() error {
	return nil
}

func (im *InMemStore) Stop() error {
	return nil
}
