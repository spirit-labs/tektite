package dev

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func init() {
	common.EnableTestPorts()
}

func TestDevStore(t *testing.T) {

	address, err := common.AddressWithPort("localhost")
	require.NoError(t, err)

	devStore := NewDevStore(address)
	err = devStore.Start()
	require.NoError(t, err)

	devClient := NewDevStoreClient(address)

	defer func() {
		//goland:noinspection GoUnhandledErrorResult
		devClient.Stop()
		err := devStore.Stop()
		require.NoError(t, err)
	}()

	vb, err := devClient.Get([]byte("key1"))
	require.NoError(t, err)
	require.Nil(t, vb)

	err = devClient.Put([]byte("key1"), []byte("val1"))
	require.NoError(t, err)

	vb, err = devClient.Get([]byte("key1"))
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val1", string(vb))

	err = devClient.Put([]byte("key2"), []byte("val2"))
	require.NoError(t, err)

	vb, err = devClient.Get([]byte("key2"))
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val2", string(vb))

	err = devClient.Delete([]byte("key1"))
	require.NoError(t, err)
	vb, err = devClient.Get([]byte("key1"))
	require.NoError(t, err)
	require.Nil(t, vb)

	vb, err = devClient.Get([]byte("key2"))
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val2", string(vb))

	err = devClient.Delete([]byte("key2"))
	require.NoError(t, err)
	vb, err = devClient.Get([]byte("key2"))
	require.NoError(t, err)
	require.Nil(t, vb)
}

func TestDevStoreUnavailable(t *testing.T) {

	address, err := common.AddressWithPort("localhost")
	require.NoError(t, err)

	devStore := NewDevStore(address)
	err = devStore.Start()
	require.NoError(t, err)
	err = devStore.Stop()
	require.NoError(t, err)

	devClient := NewDevStoreClient(address)

	//goland:noinspection GoUnhandledErrorResult
	defer devClient.Stop()

	_, err = devClient.Get([]byte("key1"))
	require.Error(t, err)
	var perr errors.TektiteError
	if errors.As(err, &perr) {
		require.Equal(t, errors.Unavailable, int(perr.Code))
	} else {
		require.Fail(t, "not a TektiteError")
	}

	err = devClient.Put([]byte("key1"), []byte("val1"))
	require.Error(t, err)
	perr = errors.TektiteError{}
	if errors.As(err, &perr) {
		require.Equal(t, errors.Unavailable, int(perr.Code))
	} else {
		require.Fail(t, "not a TektiteError")
	}

	err = devClient.Delete([]byte("key1"))
	require.Error(t, err)
	perr = errors.TektiteError{}
	if errors.As(err, &perr) {
		require.Equal(t, errors.Unavailable, int(perr.Code))
	} else {
		require.Fail(t, "not a TektiteError")
	}
}
