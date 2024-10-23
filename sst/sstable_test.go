package sst

import (
	"fmt"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	iteration2 "github.com/spirit-labs/tektite/iteration"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
	"time"
)

func TestBuildTable(t *testing.T) {
	commonPrefix := []byte("keyprefix/")
	numEntries := 1000
	iter := prepareInput(commonPrefix, []byte("valueprefix/"), numEntries)
	// Add some deletes too
	numDeletes := 1000
	for i := 0; i < numDeletes; i++ {
		key := fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), numEntries+i)
		iter.AddKV([]byte(key), nil)
	}
	now := uint64(time.Now().UTC().UnixMilli())
	sstable, smallestKey, largestKey, _, _, err := BuildSSTable(common.DataFormatV1, 0, 0, iter)
	require.NoError(t, err)
	require.Equal(t, common.DataFormatV1, sstable.format)
	require.Equal(t, numEntries+numDeletes, int(sstable.numEntries))
	require.Equal(t, numDeletes, int(sstable.numDeletes))
	expectedSmallestKey := []byte(fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), 0))
	expectedLargestKey := []byte(fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), numEntries+numDeletes-1))
	require.Equal(t, expectedSmallestKey, smallestKey)
	require.Equal(t, expectedLargestKey, largestKey)
	require.GreaterOrEqual(t, sstable.CreationTime(), now)
}

func TestBuildWithTombstones(t *testing.T) {
	gi := &iteration2.StaticIterator{}
	gi.AddKV([]byte("keyPrefix/key0"), nil)
	gi.AddKV([]byte("keyPrefix/key1"), []byte("val1"))
	gi.AddKV([]byte("keyPrefix/key2"), []byte("val2"))
	gi.AddKV([]byte("keyPrefix/key3"), nil)

	sstable, _, _, _, _, err := BuildSSTable(common.DataFormatV1, 0, 0, gi)
	require.NoError(t, err)

	iter, err := sstable.NewIterator([]byte("keyPrefix/"), nil)
	require.NoError(t, err)

	valid, curr, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, true, valid)
	require.Equal(t, "keyPrefix/key0", string(curr.Key))
	require.Nil(t, curr.Value)

	valid, curr, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, true, valid)
	require.Equal(t, "keyPrefix/key1", string(curr.Key))
	require.Equal(t, "val1", string(curr.Value))

	valid, curr, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, true, valid)
	require.Equal(t, "keyPrefix/key2", string(curr.Key))
	require.Equal(t, "val2", string(curr.Value))

	valid, curr, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, true, valid)
	require.Equal(t, "keyPrefix/key3", string(curr.Key))
	require.Nil(t, curr.Value)

	requireIterNextValid(t, iter, false)
}

func TestBuildWithPrefixTombstones(t *testing.T) {
	gi := &iteration2.StaticIterator{}
	gi.AddKV(encoding.EncodeVersion([]byte("................prefix1_"), 0), nil)
	gi.AddKV(encoding.EncodeVersion([]byte("................prefix2_"), 0), nil)
	sstable, _, _, _, _, err := BuildSSTable(common.DataFormatV1, 0, 0, gi)
	require.NoError(t, err)
	require.Equal(t, 2, sstable.NumPrefixDeletes())
}

func TestSeek(t *testing.T) {
	commonPrefix := []byte("keyprefix/")
	numEntries := 1000

	iter := prepareInput(commonPrefix, []byte("valueprefix/"), numEntries)

	// Add a few more entries so we can test seeking to next
	key := fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), 1500)
	value := fmt.Sprintf("%ssomevalue-%010d", "valueprefix/", 1500)
	iter.AddKVAsString(key, value)
	key = fmt.Sprintf("%ssomekey-%010d1234", string(commonPrefix), 1550)
	value = fmt.Sprintf("%ssomevalue-%010d1234", "valueprefix/", 1550)
	iter.AddKVAsString(key, value)
	key = fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), 1600)
	value = fmt.Sprintf("%ssomevalue-%010d", "valueprefix/", 1600)
	iter.AddKVAsString(key, value)

	sstable, _, _, _, _, err := BuildSSTable(common.DataFormatV1, 0, 0, iter)
	require.NoError(t, err)

	// Seek all the keys - exact match
	for i := 0; i < numEntries; i++ {
		k := []byte(fmt.Sprintf("keyprefix/somekey-%010d", i))
		v := []byte(fmt.Sprintf("valueprefix/somevalue-%010d", i))
		seek(t, k, k, v, true, sstable)
	}

	//boundary cases
	seek(t, []byte("keyprefix/somekey-0000000000"), []byte("keyprefix/somekey-0000000000"), []byte("valueprefix/somevalue-0000000000"), true, sstable)
	k := []byte(fmt.Sprintf("keyprefix/somekey-%010d", numEntries-1))
	v := []byte(fmt.Sprintf("valueprefix/somevalue-%010d", numEntries-1))
	seek(t, k, k, v, true, sstable)

	//not found - as keys all greater than keys in sstable
	seek(t, []byte("keyprefix/t"), nil, nil, false, sstable)
	seek(t, []byte("keyprefix/somekey-0000002000"), nil, nil, false, sstable)
	seek(t, []byte("keyprefix/uqwdiquwhdiuqwhdiuqhwdiuqhwdiuhqwd"), nil, nil, false, sstable)

	//should find next key greater than
	seek(t, nil, []byte("keyprefix/somekey-0000000000"), []byte("valueprefix/somevalue-0000000000"), true, sstable)
	seek(t, []byte("a"), []byte("keyprefix/somekey-0000000000"), []byte("valueprefix/somevalue-0000000000"), true, sstable)
	seek(t, []byte("aaaaaaaaa/"), []byte("keyprefix/somekey-0000000000"), []byte("valueprefix/somevalue-0000000000"), true, sstable)
	seek(t, []byte("keyprefix/"), []byte("keyprefix/somekey-0000000000"), []byte("valueprefix/somevalue-0000000000"), true, sstable)
	seek(t, []byte("keyprefix/r"), []byte("keyprefix/somekey-0000000000"), []byte("valueprefix/somevalue-0000000000"), true, sstable)
	seek(t, []byte("keyprefix/somekey"), []byte("keyprefix/somekey-0000000000"), []byte("valueprefix/somevalue-0000000000"), true, sstable)
	seek(t, []byte("keyprefix/somekey-00000005001"), []byte("keyprefix/somekey-0000000501"), []byte("valueprefix/somevalue-0000000501"), true, sstable)

	seek(t, []byte("keyprefix/somekey-0000001450"), []byte("keyprefix/somekey-0000001500"), []byte("valueprefix/somevalue-0000001500"), true, sstable)
	seek(t, []byte("keyprefix/somekey-0000001450999"), []byte("keyprefix/somekey-0000001500"), []byte("valueprefix/somevalue-0000001500"), true, sstable)
	seek(t, []byte("keyprefix/somekey-0000001549"), []byte("keyprefix/somekey-00000015501234"), []byte("valueprefix/somevalue-00000015501234"), true, sstable)
	seek(t, []byte("keyprefix/somekey-000000154999"), []byte("keyprefix/somekey-00000015501234"), []byte("valueprefix/somevalue-00000015501234"), true, sstable)
	seek(t, []byte("keyprefix/somekey-0000001550"), []byte("keyprefix/somekey-00000015501234"), []byte("valueprefix/somevalue-00000015501234"), true, sstable)
	seek(t, []byte("keyprefix/somekey-0000001599"), []byte("keyprefix/somekey-0000001600"), []byte("valueprefix/somevalue-0000001600"), true, sstable)
	seek(t, []byte("keyprefix/somekey-000000159999"), []byte("keyprefix/somekey-0000001600"), []byte("valueprefix/somevalue-0000001600"), true, sstable)
	seek(t, []byte("keyprefix/somekey-0000001600"), []byte("keyprefix/somekey-0000001600"), []byte("valueprefix/somevalue-0000001600"), true, sstable)
}

func seek(t *testing.T, seekKey []byte, expectedKey []byte, expectedValue []byte, expectedValid bool, sstable *SSTable) {
	t.Helper()
	iter, err := sstable.NewIterator(seekKey, nil)
	require.NoError(t, err)

	kv := requireIterNextValid(t, iter, expectedValid)
	if !expectedValid {
		return
	}
	require.Equal(t, string(expectedKey), string(kv.Key))
	require.Equal(t, string(expectedValue), string(kv.Value))
}

func TestIterateWithGaps(t *testing.T) {
	commonPrefix := []byte("keyprefix/")
	it := &iteration2.StaticIterator{}
	// Add a few more entries so we can test seeking to next
	key := fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), 1500)
	value := fmt.Sprintf("%ssomevalue-%010d", "valueprefix/", 1500)
	it.AddKVAsString(key, value)
	key = fmt.Sprintf("%ssomekey-%010d1234", string(commonPrefix), 1550)
	value = fmt.Sprintf("%ssomevalue-%010d1234", "valueprefix/", 1550)
	it.AddKVAsString(key, value)
	key = fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), 1600)
	value = fmt.Sprintf("%ssomevalue-%010d", "valueprefix/", 1600)
	it.AddKVAsString(key, value)

	sstable, _, _, _, _, err := BuildSSTable(common.DataFormatV1, 0, 0, it)
	require.NoError(t, err)
	iter, err := sstable.NewIterator([]byte("keyprefix/somekey-0000001501"), nil)
	require.NoError(t, err)

	kv := requireIterNextValid(t, iter, true)
	require.Equal(t, []byte("keyprefix/somekey-00000015501234"), kv.Key)
	require.Equal(t, []byte("valueprefix/somevalue-00000015501234"), kv.Value)
	kv = requireIterNextValid(t, iter, true)
	require.Equal(t, []byte("keyprefix/somekey-0000001600"), kv.Key)
	require.Equal(t, []byte("valueprefix/somevalue-0000001600"), kv.Value)
	requireIterNextValid(t, iter, false)
}

func TestIterate(t *testing.T) {
	commonPrefix := []byte("keyprefix/")
	testIterate(t, commonPrefix, nil, 0, 999)
	testIterate(t, commonPrefix, []byte("keyprefix/somekey-0000000450"), 0, 449)
	testIterate(t, []byte("keyprefix/somekey-0000000300"), nil, 300, 999)
	testIterate(t, []byte("keyprefix/somekey-0000000300999"), nil, 301, 999)
	testIterate(t, []byte("keyprefix/somekey-0000000300"), []byte("keyprefix/somekey-0000000900"), 300, 899)
	testIterate(t, []byte("keyprefix/somekey-0000000300"), []byte("keyprefix/somekey-0000000999"), 300, 998)
	testIterate(t, []byte("keyprefix/somekey-0000000300"), []byte("keyprefix/somekey-0000000999999"), 300, 999)
	testIterate(t, []byte("keyprefix/somekey-0000000300"), []byte("keyprefix/somekey-0000001000"), 300, 999)
	testIterate(t, []byte("keyprefix/somekey-0000000700"), []byte("keyprefix/somekey-0000000701"), 700, 700)
	testIterate(t, []byte("keyprefix/somekey-0000000700"), []byte("keyprefix/somekey-0000000700"), -1, -1)
	testIterate(t, []byte("keyprefix/somekey-0000001000"), []byte("keyprefix/somekey-0000001001"), -1, -1)
	testIterate(t, []byte("keyprefix/t"), []byte("keyprefix/u"), -1, -1)
}

func TestCurrentIterateAll(t *testing.T) {
	it := prepareInput(nil, nil, 10)
	sstable, _, _, _, _, err := BuildSSTable(common.DataFormatV1, 0, 0, it)
	require.NoError(t, err)
	iter, err := sstable.NewIterator(nil, nil)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		kv := requireIterNextValid(t, iter, true)
		require.Equal(t, kv, iter.Current())
	}
	kv := requireIterNextValid(t, iter, false)
	require.Equal(t, common.KV{}, kv)
	require.Equal(t, common.KV{}, iter.Current())
}

func TestCurrentIteratePartial(t *testing.T) {
	it := prepareInput(nil, nil, 10)
	sstable, _, _, _, _, err := BuildSSTable(common.DataFormatV1, 0, 0, it)
	require.NoError(t, err)
	startRange := []byte(fmt.Sprintf("somekey-%010d", 2))
	endRange := []byte(fmt.Sprintf("somekey-%010d", 7))
	iter, err := sstable.NewIterator(startRange, endRange)
	require.NoError(t, err)
	for i := 2; i < 7; i++ {
		kv := requireIterNextValid(t, iter, true)
		require.Equal(t, kv, iter.Current())
	}
	kv := requireIterNextValid(t, iter, false)
	require.Equal(t, common.KV{}, kv)
	require.Equal(t, common.KV{}, iter.Current())
}

func testIterate(t *testing.T, startKey []byte, endKey []byte, firstExpected int, lastExpected int) {
	t.Helper()
	commonPrefix := []byte("keyprefix/")
	numEntries := 1000
	it := prepareInput(commonPrefix, []byte("valueprefix/"), numEntries)
	sstable, _, _, _, _, err := BuildSSTable(common.DataFormatV1, 0, 0, it)
	require.NoError(t, err)

	iter, err := sstable.NewIterator(startKey, endKey)
	require.NoError(t, err)

	if firstExpected == -1 {
		requireIterNextValid(t, iter, false)
		return
	}

	i := firstExpected
	for i <= lastExpected {
		valid, kv, err := iter.Next()
		require.NoError(t, err)
		if !valid {
			break
		}
		k := []byte(fmt.Sprintf("keyprefix/somekey-%010d", i))
		v := []byte(fmt.Sprintf("valueprefix/somevalue-%010d", i))
		require.Equal(t, k, kv.Key)
		require.Equal(t, v, kv.Value)
		i++
	}
	requireIterNextValid(t, iter, false)
}

func TestSerializeDeserialize(t *testing.T) {
	commonPrefix := []byte("................prefix1_")
	numEntries := 1000

	gi := &iteration2.StaticIterator{}
	// add a prefix deletion
	gi.AddKV(encoding.EncodeVersion(commonPrefix, math.MaxUint64), nil)
	// add some entries
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), i)
		value := fmt.Sprintf("%ssomevalue-%010d", string(commonPrefix), i)
		gi.AddKV(encoding.EncodeVersion([]byte(key), 0), []byte(value))
	}
	// add some deletes too
	key1 := fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), numEntries)
	gi.AddKV(encoding.EncodeVersion([]byte(key1), 0), nil)
	key2 := fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), numEntries+1)
	gi.AddKV(encoding.EncodeVersion([]byte(key2), 0), nil)

	sstable, _, _, _, _, err := BuildSSTable(common.DataFormatV1, 0, 0, gi)
	require.NoError(t, err)
	require.Equal(t, 1, int(sstable.numPrefixDeletes))
	buff := sstable.Serialize()

	sstable2 := &SSTable{}
	sstable2.Deserialize(buff, 0)

	require.Equal(t, sstable.format, sstable2.format)
	require.Equal(t, sstable.indexOffset, sstable2.indexOffset)
	require.Equal(t, sstable.numEntries, sstable2.numEntries)
	require.Equal(t, sstable.numDeletes, sstable2.numDeletes)
	require.Equal(t, sstable.maxKeyLength, sstable2.maxKeyLength)
	require.Equal(t, sstable.data, sstable2.data)
	require.Equal(t, sstable.creationTime, sstable2.creationTime)
	require.Equal(t, sstable.numPrefixDeletes, sstable2.numPrefixDeletes)
}

func prepareInput(keyPrefix []byte, valuePrefix []byte, numEntries int) *iteration2.StaticIterator {
	gi := &iteration2.StaticIterator{}
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("%ssomekey-%010d", string(keyPrefix), i)
		value := fmt.Sprintf("%ssomevalue-%010d", string(valuePrefix), i)
		gi.AddKVAsString(key, value)
	}
	return gi
}

func requireIterNextValid(t require.TestingT, iter iteration2.Iterator, valid bool) common.KV {
	v, kv, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, valid, v)
	return kv
}
