package control

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/compress"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSequences(t *testing.T) {
	lsmRec := &testLsmReceiver{}
	objStore := dev.NewInMemStore(0)
	blockSize := int64(100)
	getter := &mapTableGetter{tables: map[string]*sst.SSTable{}}
	dataBucketName := "test-bucket"
	kvr := func(kvs []common.KV) error {
		return writeKvDirect(kvs, objStore, dataBucketName, common.DataFormatV1, lsmRec)
	}
	sequences := NewSequences(lsmRec, getter.getTable, objStore, dataBucketName, common.DataFormatV1, blockSize,
		kvr)

	sequences.Start()
	defer sequences.Stop()

	numSequences := 10
	numVals := 1000
	for i := 0; i < numVals; i++ {
		for j := 0; j < numSequences; j++ {
			seq, err := sequences.GenerateSequence(fmt.Sprintf("test-sequence%d", j))
			require.NoError(t, err)
			require.Equal(t, i, int(seq))
		}
	}
}

func TestSequencesLoad(t *testing.T) {
	sequenceName := "test-sequence"
	seqVal := int64(12323)
	table := createTableWithStoredSequence(t, sequenceName, seqVal)
	tableID := sst.CreateSSTableId()
	lsmRec := &testLsmReceiver{
		queryRes: []lsm.NonOverlappingTables{
			[]lsm.QueryTableInfo{
				{
					ID: []byte(tableID),
				},
			},
		},
	}
	objStore := dev.NewInMemStore(0)
	blockSize := int64(100)
	getter := &mapTableGetter{tables: map[string]*sst.SSTable{}}
	getter.tables[tableID] = table
	dataBucketName := "test-bucket"
	kvr := func(kvs []common.KV) error {
		return writeKvDirect(kvs, objStore, dataBucketName, common.DataFormatV1, lsmRec)
	}
	sequences := NewSequences(lsmRec, getter.getTable, objStore, "test-bucket", common.DataFormatV1, blockSize,
		kvr)

	sequences.Start()
	defer sequences.Stop()

	seq, err := sequences.GenerateSequence(sequenceName)
	require.NoError(t, err)

	require.Equal(t, seqVal+1, seq)
}

func TestSequencesStore(t *testing.T) {
	sequenceName := "test-sequence"
	seqVal := int64(12323)
	table := createTableWithStoredSequence(t, sequenceName, seqVal)
	tableID := sst.CreateSSTableId()
	lsmRec := &testLsmReceiver{
		queryRes: []lsm.NonOverlappingTables{
			[]lsm.QueryTableInfo{
				{
					ID: []byte(tableID),
				},
			},
		},
	}
	objStore := dev.NewInMemStore(0)
	blockSize := int64(10)
	getter := &mapTableGetter{tables: map[string]*sst.SSTable{}}
	getter.tables[tableID] = table
	dataBucketName := "test-bucket"
	kvr := func(kvs []common.KV) error {
		return writeKvDirect(kvs, objStore, dataBucketName, common.DataFormatV1, lsmRec)
	}
	sequences := NewSequences(lsmRec, getter.getTable, objStore, "test-bucket", common.DataFormatV1, blockSize,
		kvr)

	sequences.Start()
	defer sequences.Stop()

	expectedSeq := seqVal + 1
	for i := 0; i < int(blockSize)+1; i++ {
		seq, err := sequences.GenerateSequence(sequenceName)
		require.NoError(t, err)

		// First generate should force a load then a reserve (store) of new block
		if i == 0 {
			regBatch := lsmRec.receivedRegBatch
			require.Equal(t, 1, len(regBatch.Registrations))
			pushedTableID := regBatch.Registrations[0].TableID
			storedSeq := loadStoredSequence(t, sequenceName, pushedTableID, objStore)
			require.Equal(t, seqVal+blockSize, storedSeq)
		}

		require.Equal(t, expectedSeq, seq)
		expectedSeq++
	}
	// And then we should get a second store when first block is exhausted
	regBatch := lsmRec.receivedRegBatch
	require.Equal(t, 1, len(regBatch.Registrations))
	pushedTableID := regBatch.Registrations[0].TableID

	storedSeq := loadStoredSequence(t, sequenceName, pushedTableID, objStore)
	require.Equal(t, seqVal+2*blockSize, storedSeq)
}

func loadStoredSequence(t *testing.T, sequenceName string, tableID sst.SSTableID, objStore objstore.Client) int64 {
	tableBytes, err := objStore.Get(context.Background(), "test-bucket", string(tableID))
	require.NoError(t, err)
	pushedTable, err := sst.GetSSTableFromBytes(tableBytes)
	require.NoError(t, err)
	require.Equal(t, 1, pushedTable.NumEntries())
	iter, err := pushedTable.NewIterator(nil, nil)
	require.NoError(t, err)
	ok, kv, err := iter.Next()
	require.NoError(t, err)
	require.True(t, ok)
	ph, err := parthash.CreateHash([]byte("sequence." + sequenceName))
	require.NoError(t, err)
	expectedKey := make([]byte, 0, 24)
	expectedKey = append(expectedKey, ph...)
	expectedKey = encoding.EncodeVersion(expectedKey, 0)
	require.Equal(t, expectedKey, kv.Key)
	storedSeq := int64(binary.BigEndian.Uint64(kv.Value))
	return storedSeq
}

func createTableWithStoredSequence(t *testing.T, sequenceName string, seqVal int64) *sst.SSTable {
	key, err := parthash.CreateHash([]byte("sequence." + sequenceName))
	require.NoError(t, err)
	val := make([]byte, 16)
	binary.BigEndian.PutUint64(val, uint64(seqVal))
	kv := common.KV{
		Key:   key,
		Value: val,
	}
	iter := common.NewKvSliceIterator([]common.KV{kv})
	table, _, _, _, _, err := sst.BuildSSTable(common.DataFormatV1, 0, 0, iter)
	require.NoError(t, err)
	return table
}

type testLsmReceiver struct {
	queryRes         lsm.OverlappingTables
	receivedRegBatch lsm.RegistrationBatch
}

func (t *testLsmReceiver) QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error) {
	return t.queryRes, nil
}

func (t *testLsmReceiver) ApplyLsmChanges(regBatch lsm.RegistrationBatch, completionFunc func(error) error) error {
	t.receivedRegBatch = regBatch
	return completionFunc(nil)
}

type mapTableGetter struct {
	tables map[string]*sst.SSTable
}

func (t *mapTableGetter) getTable(tableID sst.SSTableID) (*sst.SSTable, error) {
	table, ok := t.tables[string(tableID)]
	if !ok {
		return nil, errors.New("cannot find table")
	}
	return table, nil
}

func writeKvDirect(kvs []common.KV, objStore objstore.Client, dataBucketName string, dataFormat common.DataFormat,
	lsmHolder lsmReceiver) error {
	iter := common.NewKvSliceIterator(kvs)
	// Build ssTable
	table, smallestKey, largestKey, minVersion, maxVersion, err := sst.BuildSSTable(dataFormat, 0,
		0, iter)
	if err != nil {
		return err
	}
	tableID := sst.CreateSSTableId()
	// Push ssTable to object store
	tableData, err := table.ToStorageBytes(compress.CompressionTypeNone)
	if err != nil {
		return err
	}
	if err := objstore.PutWithTimeout(objStore, dataBucketName, tableID, tableData, objStoreCallTimeout); err != nil {
		return nil
	}
	// Register table with LSM
	regEntry := lsm.RegistrationEntry{
		Level:            0,
		TableID:          []byte(tableID),
		MinVersion:       minVersion,
		MaxVersion:       maxVersion,
		KeyStart:         smallestKey,
		KeyEnd:           largestKey,
		DeleteRatio:      table.DeleteRatio(),
		AddedTime:        uint64(time.Now().UnixMilli()),
		NumEntries:       uint64(table.NumEntries()),
		TableSize:        uint64(table.SizeBytes()),
		NumPrefixDeletes: uint32(table.NumPrefixDeletes()),
	}
	batch := lsm.RegistrationBatch{
		Registrations: []lsm.RegistrationEntry{regEntry},
	}
	ch := make(chan error, 1)
	if err := lsmHolder.ApplyLsmChanges(batch, func(err error) error {
		ch <- err
		return nil
	}); err != nil {
		return err
	}
	return <-ch
}
