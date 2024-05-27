package mem

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestLinkedKVMap(t *testing.T) {
	lhm := NewLinkedKVMap()

	require.Equal(t, 0, lhm.Len())

	for i := 0; i < 10; i++ {
		lhm.Put(createKV(i, i))
	}

	require.Equal(t, 10, lhm.Len())

	checkGet(t, lhm, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

	val, ok := lhm.Get([]byte("notexists"))
	require.False(t, ok)
	require.Nil(t, val)

	checkRange(t, lhm, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

	// delete from middle of list
	kDel := createKeyNoVersion(5)
	lhm.Delete(kDel)

	val, ok = lhm.Get(kDel)
	require.False(t, ok)
	require.Nil(t, val)

	// make sure can still see the other ones

	checkGet(t, lhm, 0, 1, 2, 3, 4, 6, 7, 8, 9)

	checkRange(t, lhm, 0, 1, 2, 3, 4, 6, 7, 8, 9)

	// delete from head
	kDel = createKeyNoVersion(0)
	lhm.Delete(kDel)

	val, ok = lhm.Get(kDel)
	require.False(t, ok)
	require.Nil(t, val)

	checkGet(t, lhm, 1, 2, 3, 4, 6, 7, 8, 9)

	checkRange(t, lhm, 1, 2, 3, 4, 6, 7, 8, 9)

	// delete from tail
	kDel = createKeyNoVersion(9)
	lhm.Delete(kDel)

	val, ok = lhm.Get(kDel)
	require.False(t, ok)
	require.Nil(t, val)

	checkGet(t, lhm, 1, 2, 3, 4, 6, 7, 8)

	checkRange(t, lhm, 1, 2, 3, 4, 6, 7, 8)

	// add more entries

	for i := 10; i < 15; i++ {
		lhm.Put(createKV(i, i))
	}

	keys := []int{1, 2, 3, 4, 6, 7, 8, 10, 11, 12, 13, 14}

	checkGet(t, lhm, keys...)

	checkRange(t, lhm, keys...)

	src := rand.NewSource(time.Now().UnixMilli())
	rnd := rand.New(src)

	// delete randomly one by one, checking get and range after each delete
	for len(keys) > 0 {
		p := rnd.Intn(len(keys))
		lhm.Delete(createKeyNoVersion(keys[p]))
		keys = append(keys[:p], keys[p+1:]...)
		checkGet(t, lhm, keys...)
		checkRange(t, lhm, keys...)
	}

	require.Equal(t, 0, lhm.Len())

	// updating values
	for i := 0; i < 10; i++ {
		lhm.Put(createKV(i, i))
	}
	checkGet(t, lhm, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	checkRange(t, lhm, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

	for i := 5; i < 15; i++ {
		lhm.Put(createKV(i, 10+i))
	}
	checkGetWithVals(t, lhm, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
		[]int{0, 1, 2, 3, 4, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24})
	checkRangeWithVals(t, lhm, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
		[]int{0, 1, 2, 3, 4, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24})
}

func TestLinkedKVMapWithVersions(t *testing.T) {
	lhm := NewLinkedKVMap()

	// We only keep the latest version of a key
	for i := 0; i < 10; i++ {
		lhm.Put(common.KV{
			Key:   createKeyWithVersion(i, 10),
			Value: createVal(i),
		})
	}
	for i := 0; i < 10; i++ {
		lhm.Put(common.KV{
			Key:   createKeyWithVersion(i, 11),
			Value: createVal(i + 10),
		})
	}
	checkGetWithVals(t, lhm, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []int{10, 11, 12, 13, 14, 15, 16, 17, 18, 19})
	i := 0
	lhm.Range(func(key []byte, val []byte) bool {
		expectedKey := createKeyWithVersion(i, 11)
		expectedVal := createVal(i + 10)
		require.Equal(t, expectedKey, key)
		require.Equal(t, expectedVal, val)
		i++
		return true
	})
}

func checkGet(t *testing.T, lhm *LinkedKVMap, keys ...int) {
	checkGetWithVals(t, lhm, keys, keys)
}

func checkRange(t *testing.T, lhm *LinkedKVMap, keys ...int) {
	checkRangeWithVals(t, lhm, keys, keys)
}

func checkGetWithVals(t *testing.T, lhm *LinkedKVMap, keys []int, vals []int) {
	for i := 0; i < len(keys); i++ {
		expectedKey := createKeyNoVersion(keys[i])
		expectedVal := createVal(vals[i])
		val, ok := lhm.Get(expectedKey)
		require.True(t, ok)
		require.Equal(t, expectedVal, val)
	}
}

func checkRangeWithVals(t *testing.T, lhm *LinkedKVMap, keys []int, vals []int) {
	i := 0
	lhm.Range(func(key []byte, val []byte) bool {
		expectedKey := createKey(keys[i])
		expectedVal := createVal(vals[i])
		require.Equal(t, expectedKey, key)
		require.Equal(t, expectedVal, val)
		i++
		return true
	})
}

func createKV(k int, v int) common.KV {
	return common.KV{Key: createKey(k), Value: createVal(v)}
}

func createKey(i int) []byte {
	return createKeyWithVersion(i, 0)
}

func createKeyWithVersion(i int, v int) []byte {
	return encoding.EncodeVersion([]byte(fmt.Sprintf("key%d", i)), uint64(v))
}

func createKeyNoVersion(i int) []byte {
	return []byte(fmt.Sprintf("key%d", i))
}

func createVal(i int) []byte {
	return []byte(fmt.Sprintf("val%d", i))
}
