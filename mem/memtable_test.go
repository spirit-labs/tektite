package mem

import (
	"fmt"
	"github.com/spirit-labs/tektite/arenaskl"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMTIteratorPicksUpNewRecordsGreaterKeyAlreadyExists(t *testing.T) {
	memTable := NewMemtable(arenaskl.NewArena(1024*1024), 0, 1024*1024)

	// We test the case where a greater than iterator range already exists, then create iterator, then add
	// data in range, then iterate

	iter := memTable.NewIterator([]byte("key2"), []byte("key3"))

	addToMemtable(t, memTable, "key3", "val3")
	addToMemtable(t, memTable, "key4", "val4")

	requireNextValid(t, iter, false)

	addToMemtable(t, memTable, "key2", "val2")
	addToMemtable(t, memTable, "key2.1", "val2.1")

	curr := requireNextValid(t, iter, true)
	require.Equal(t, "key2", string(curr.Key))
	require.Equal(t, "val2", string(curr.Value))
	curr = requireNextValid(t, iter, true)
	require.Equal(t, "key2.1", string(curr.Key))
	require.Equal(t, "val2.1", string(curr.Value))
	requireNextValid(t, iter, false)
}

func TestMTIteratorAddNonKeyOrder(t *testing.T) {
	memTable := NewMemtable(arenaskl.NewArena(1024*1024), 0, 1024*1024)

	addToMemtable(t, memTable, "key0", "val0")
	addToMemtable(t, memTable, "key1", "val1")
	addToMemtable(t, memTable, "key2", "val2")
	addToMemtable(t, memTable, "key3", "val3")
	addToMemtable(t, memTable, "key4", "val4")

	iter := memTable.NewIterator(nil, nil)
	for i := 0; i < 5; i++ {
		curr := requireNextValid(t, iter, true)
		require.Equal(t, fmt.Sprintf("key%d", i), string(curr.Key))
		require.Equal(t, fmt.Sprintf("val%d", i), string(curr.Value))
	}
}

func TestMTIteratorAddInNonKeyOrder(t *testing.T) {
	memTable := NewMemtable(arenaskl.NewArena(1024*1024), 0, 1024*1024)

	addToMemtable(t, memTable, "key3", "val3")
	addToMemtable(t, memTable, "key2", "val2")
	addToMemtable(t, memTable, "key1", "val1")
	addToMemtable(t, memTable, "key0", "val0")
	addToMemtable(t, memTable, "key4", "val4")

	iter := memTable.NewIterator(nil, nil)
	for i := 0; i < 5; i++ {
		curr := requireNextValid(t, iter, true)
		require.Equal(t, fmt.Sprintf("key%d", i), string(curr.Key))
		require.Equal(t, fmt.Sprintf("val%d", i), string(curr.Value))
	}
}

func TestMTIteratorOverwriteKeys(t *testing.T) {
	memTable := NewMemtable(arenaskl.NewArena(1024*1024), 0, 1024*1024)

	addToMemtable(t, memTable, "key3", "val3")
	addToMemtable(t, memTable, "key2", "val2")
	addToMemtable(t, memTable, "key1", "val1")
	addToMemtable(t, memTable, "key0", "val0")
	addToMemtable(t, memTable, "key2", "val5")
	addToMemtable(t, memTable, "key4", "val4")
	addToMemtable(t, memTable, "key0", "val6")

	iter := memTable.NewIterator(nil, nil)
	for i := 0; i < 5; i++ {
		curr := requireNextValid(t, iter, true)
		j := i
		if i == 0 {
			j = 6
		} else if i == 2 {
			j = 5
		}
		require.Equal(t, fmt.Sprintf("key%d", i), string(curr.Key))
		require.Equal(t, fmt.Sprintf("val%d", j), string(curr.Value))
	}
}

func TestMTIteratorTombstones(t *testing.T) {
	memTable := NewMemtable(arenaskl.NewArena(1024*1024), 0, 1024*1024)

	addToMemtable(t, memTable, "key3", "val3")
	addToMemtable(t, memTable, "key2", "val2")
	addToMemtableWithByteSlice(t, memTable, "key1", nil)
	addToMemtable(t, memTable, "key0", "val0")
	addToMemtableWithByteSlice(t, memTable, "key4", nil)

	iter := memTable.NewIterator(nil, nil)
	for i := 0; i < 5; i++ {
		curr := requireNextValid(t, iter, true)
		require.Equal(t, fmt.Sprintf("key%d", i), string(curr.Key))
		if i == 1 || i == 4 {
			require.Equal(t, 0, len(curr.Value))
		} else {
			require.Equal(t, fmt.Sprintf("val%d", i), string(curr.Value))
		}
	}
}

// TestMTIteratorCurrent - Make sure Current() has correct behaviour
func TestMTIteratorCurrentIterateFullRange(t *testing.T) {
	memTable := NewMemtable(arenaskl.NewArena(1024*1024), 0, 1024*1024)
	batch := &testBatch{}
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("prefix/key%010d", i))
		val := []byte(fmt.Sprintf("val%010d", i))
		batch.AddEntry(common.KV{
			Key:   key,
			Value: val,
		})
	}
	ok, err := memTable.Write(batch)
	require.NoError(t, err)
	require.True(t, ok)

	iter := memTable.NewIterator(nil, nil)
	for i := 0; i < 10; i++ {
		curr := requireNextValid(t, iter, true)
		require.Equal(t, fmt.Sprintf("prefix/key%010d", i), string(curr.Key))
		require.Equal(t, fmt.Sprintf("val%010d", i), string(curr.Value))
		require.Equal(t, curr, iter.Current())
	}
	requireNextValid(t, iter, false)
	require.Equal(t, common.KV{}, iter.Current())
}

// TestMTIteratorCurrent - Make sure Current() has correct behaviour on partial range
func TestMTIteratorCurrentIteratePartialRange(t *testing.T) {
	memTable := NewMemtable(arenaskl.NewArena(1024*1024), 0, 1024*1024)
	batch := &testBatch{}
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("prefix/key%010d", i))
		val := []byte(fmt.Sprintf("val%010d", i))
		batch.AddEntry(common.KV{
			Key:   key,
			Value: val,
		})
	}
	ok, err := memTable.Write(batch)
	require.NoError(t, err)
	require.True(t, ok)

	iter := memTable.NewIterator([]byte(fmt.Sprintf("prefix/key%010d", 3)), []byte(fmt.Sprintf("prefix/key%010d", 7)))
	for i := 3; i < 7; i++ {
		curr := requireNextValid(t, iter, true)
		require.Equal(t, fmt.Sprintf("prefix/key%010d", i), string(curr.Key))
		require.Equal(t, fmt.Sprintf("val%010d", i), string(curr.Value))
		require.Equal(t, curr, iter.Current())
	}
	requireNextValid(t, iter, false)
	require.Equal(t, common.KV{}, iter.Current())
}

func TestMTIteratorIterateInRange(t *testing.T) {
	testMTIteratorIterateInRange(t, nil, nil, 0, 99)
	testMTIteratorIterateInRange(t, []byte("prefix/key0000000033"), nil, 33, 99)
	testMTIteratorIterateInRange(t, []byte("prefix/key0000000033"), []byte("prefix/key0000000077"), 33, 76)
	testMTIteratorIterateInRange(t, nil, []byte("prefix/key0000000088"), 0, 87)
	testMTIteratorIterateInRange(t, []byte("prefix/key0000000100"), []byte("prefix/key0000000200"), 0, -1)
	testMTIteratorIterateInRange(t, []byte("prefix/key0000000100"), nil, 0, -1)
	//Important ones - test ranges that end before start of data
	testMTIteratorIterateInRange(t, []byte("prefix/j"), []byte("prefix/k"), 0, -1)
	testMTIteratorIterateInRange(t, []byte("prefix/j"), []byte("prefix/key0000000000"), 0, -1)
	// Single value
	testMTIteratorIterateInRange(t, []byte("prefix/key0000000066"), []byte("prefix/key0000000067"), 66, 66)
}

func testMTIteratorIterateInRange(t *testing.T, keyStart []byte, keyEnd []byte, expectedFirst int, expectedLast int) {
	t.Helper()
	memTable := NewMemtable(arenaskl.NewArena(1024*1024), 0, 1024*1024)
	numEntries := 100
	batch := &testBatch{}
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("prefix/key%010d", i))
		val := []byte(fmt.Sprintf("val%010d", i))
		batch.AddEntry(common.KV{
			Key:   key,
			Value: val,
		})
	}
	ok, err := memTable.Write(batch)
	require.NoError(t, err)
	require.True(t, ok)

	iter := memTable.NewIterator(keyStart, keyEnd)
	for i := expectedFirst; i <= expectedLast; i++ {
		curr := requireNextValid(t, iter, true)
		require.Equal(t, fmt.Sprintf("prefix/key%010d", i), string(curr.Key))
		require.Equal(t, fmt.Sprintf("val%010d", i), string(curr.Value))
	}
	requireNextValid(t, iter, false)
}

func addToMemtable(t *testing.T, memTable *Memtable, key string, value string) {
	t.Helper()
	batch := &testBatch{}
	batch.AddEntry(common.KV{
		Key:   []byte(key),
		Value: []byte(value),
	})
	ok, err := memTable.Write(batch)
	require.NoError(t, err)
	require.True(t, ok)
}

func addToMemtableWithByteSlice(t *testing.T, memTable *Memtable, key string, value []byte) {
	t.Helper()
	batch := &testBatch{}
	batch.AddEntry(common.KV{
		Key:   []byte(key),
		Value: value,
	})
	ok, err := memTable.Write(batch)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestBatchAddAndIterate(t *testing.T) {
	batch := &testBatch{}
	// Add them in reverse order to make sure insertion order is maintained
	for i := 9; i >= 0; i-- {
		key := encoding.EncodeVersion([]byte(fmt.Sprintf("prefix/key%010d", i)), 0)
		val := []byte(fmt.Sprintf("val%010d", i))
		batch.AddEntry(common.KV{
			Key:   key,
			Value: val,
		})
		require.Equal(t, 10-i, batch.Len())
	}
	j := 9
	batch.Range(func(key []byte, value []byte) bool {
		require.Equal(t, string(encoding.EncodeVersion([]byte(fmt.Sprintf("prefix/key%010d", j)), 0)), string(key))
		require.Equal(t, fmt.Sprintf("val%010d", j), string(value))
		j--
		return true
	})
}

func requireNextValid(t *testing.T, iter iteration.Iterator, expectedValid bool) common.KV {
	t.Helper()
	valid, kv, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, expectedValid, valid)
	return kv
}

// simple batch for testing that does not require versions
type testBatch struct {
	memtableBytes int64
	entries       []common.KV
}

func (t *testBatch) AddEntry(kv common.KV) {
	t.entries = append(t.entries, kv)
	t.memtableBytes += arenaskl.MaxEntrySize(int64(len(kv.Key)), int64(len(kv.Value)))
}

func (t *testBatch) MemTableBytes() int64 {
	return t.memtableBytes
}

func (t *testBatch) Range(f func(key []byte, value []byte) bool) {
	for _, entry := range t.entries {
		f(entry.Key, entry.Value)
	}
}

func (t *testBatch) Len() int {
	return len(t.entries)
}

func TestMemtableMaxSize(t *testing.T) {
	size := 10000
	memTable := NewMemtable(arenaskl.NewArena(uint32(size)), 0, size)

	batch := &testBatch{}
	i := 0
	for batch.memtableBytes < int64(size) {
		batch.AddEntry(common.KV{
			Key:   []byte(fmt.Sprintf("key-%06d", i)),
			Value: []byte(fmt.Sprintf("val-%06d", i)),
		})
		i++
	}
	ok, err := memTable.Write(batch)
	require.NoError(t, err)
	require.False(t, ok)
}
