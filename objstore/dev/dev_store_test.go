package dev

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/objstore"
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

	objstore.TestApi(t, devClient)
}
