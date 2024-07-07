package levels

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAddLoadDeletePreFlush(t *testing.T) {
	segCache := newSegmentCache(1000)
	segMap := map[string]*segment{}
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("segment-%d", i)
		seg := &segment{}
		segMap[id] = seg
		segCache.put(id, seg)
	}
	for id, seg := range segMap {
		segReceived := segCache.get(id)
		require.NotNil(t, segReceived)
		require.Equal(t, seg, segReceived)
	}
	for id := range segMap {
		segCache.delete(id)
	}
	for id := range segMap {
		segReceived := segCache.get(id)
		require.Nil(t, segReceived)
	}
}

func TestAddLoadDeletePreFlushZeroSizeCache(t *testing.T) {
	segCache := newSegmentCache(0)
	segMap := map[string]*segment{}
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("segment-%d", i)
		seg := &segment{}
		segMap[id] = seg
		segCache.put(id, seg)
	}
	for id, seg := range segMap {
		segReceived := segCache.get(id)
		require.NotNil(t, segReceived)
		require.Equal(t, seg, segReceived)
	}
	for id := range segMap {
		segCache.delete(id)
	}
	for id := range segMap {
		segReceived := segCache.get(id)
		require.Nil(t, segReceived)
	}
}

func TestAddLoadDeletePostFlush(t *testing.T) {
	segCache := newSegmentCache(1000)
	segMap := map[string]*segment{}
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("segment-%d", i)
		seg := &segment{}
		segMap[id] = seg
		segCache.put(id, seg)
	}
	segCache.sealPreflushCache()
	segCache.flushSealedCache()

	for id, seg := range segMap {
		segReceived := segCache.get(id)
		require.NotNil(t, segReceived)
		require.Equal(t, seg, segReceived)
	}
	for id := range segMap {
		segCache.delete(id)
	}
	for id := range segMap {
		segReceived := segCache.get(id)
		require.Nil(t, segReceived)
	}
}

func TestNoEvictionPreFlush(t *testing.T) {
	segCache := newSegmentCache(5)
	segMap := map[string]*segment{}
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("segment-%d", i)
		seg := &segment{}
		segMap[id] = seg
		segCache.put(id, seg)
	}
	for id, seg := range segMap {
		segReceived := segCache.get(id)
		require.NotNil(t, segReceived)
		require.Equal(t, seg, segReceived)
	}
}

func TestEvictionPostFlush(t *testing.T) {
	segCache := newSegmentCache(5)
	segMap := map[string]*segment{}
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("segment-%d", i)
		seg := &segment{}
		segMap[id] = seg
		segCache.put(id, seg)
	}
	segCache.sealPreflushCache()
	segCache.flushSealedCache()
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("segment-%d", i)
		segReceived := segCache.get(id)
		if i < 5 {
			require.Nil(t, segReceived)
		} else {
			require.NotNil(t, segReceived)
		}
	}
}
