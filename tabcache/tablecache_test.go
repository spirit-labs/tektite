package tabcache

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/sst"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTableCache(t *testing.T) {
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	address := "localhost:6768"

	objStore := dev.NewDevStore(address)
	err := objStore.Start()
	require.NoError(t, err)
	objStoreClient := dev.NewDevStoreClient(address)

	defer func() {
		err := objStoreClient.Stop()
		require.NoError(t, err)
		err = objStore.Stop()
		require.NoError(t, err)
	}()

	tc, err := NewTableCache(objStoreClient, &cfg)
	require.NoError(t, err)

	table1 := createSSTable(t)
	err = tc.AddSSTable([]byte("sst1"), table1)
	require.NoError(t, err)

	res, err := tc.GetSSTable([]byte("sst1"))
	require.NoError(t, err)
	require.Equal(t, table1, res)

	table2 := createSSTable(t)
	err = tc.AddSSTable([]byte("sst2"), table2)
	require.NoError(t, err)

	res, err = tc.GetSSTable([]byte("sst1"))
	require.NoError(t, err)
	require.Equal(t, table1, res)

	res, err = tc.GetSSTable([]byte("sst2"))
	require.NoError(t, err)
	require.Equal(t, table2, res)

	tc.DeleteSSTable([]byte("sst1"))
	res, err = tc.GetSSTable([]byte("sst1"))
	require.NoError(t, err)
	require.Nil(t, res)

	tc.DeleteSSTable([]byte("sst2"))
	res, err = tc.GetSSTable([]byte("sst2"))
	require.NoError(t, err)
	require.Nil(t, res)
}

func TestGetFromCloudStore(t *testing.T) {
	cfg := conf.Config{}
	cfg.ApplyDefaults()

	objStoreClient := dev.NewInMemStore(0)

	defer func() {
		err := objStoreClient.Stop()
		require.NoError(t, err)
		require.NoError(t, err)
	}()

	tc, err := NewTableCache(objStoreClient, &cfg)
	require.NoError(t, err)

	res, err := tc.GetSSTable([]byte("sst1"))
	require.NoError(t, err)
	require.Nil(t, res)

	table1 := createSSTable(t)

	err = objStoreClient.Put([]byte("sst1"), table1.Serialize())
	require.NoError(t, err)

	res, err = tc.GetSSTable([]byte("sst1"))
	require.NoError(t, err)
	checkTable(t, res)

	tc.DeleteSSTable([]byte("sst1"))
	res2, err := tc.GetSSTable([]byte("sst1"))
	require.NoError(t, err)

	require.NotNil(t, res2)
	require.Equal(t, res, res2)
}

func checkTable(t *testing.T, table *sst.SSTable) {
	iter, err := table.NewIterator(nil, nil)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		valid, curr, err := iter.Next()
		require.NoError(t, err)
		require.True(t, valid)
		require.Equal(t, []byte(fmt.Sprintf("key%000005d", i)), curr.Key)
		require.Equal(t, []byte(fmt.Sprintf("val%000005d", i)), curr.Value)
	}
}

func createSSTable(t *testing.T) *sst.SSTable {
	iter := iteration.StaticIterator{}
	for i := 0; i < 10; i++ {
		iter.AddKVAsString(fmt.Sprintf("key%000005d", i), fmt.Sprintf("val%000005d", i))
	}
	table, _, _, _, _, err := sst.BuildSSTable(common.DataFormatV1, 0, 0, &iter)
	require.NoError(t, err)
	return table
}
