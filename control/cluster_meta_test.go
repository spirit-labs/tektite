package control

import (
	"fmt"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGetClusterMetadata(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	numControllers := 6
	i := 0
	controllers, _, tearDown := setupControllersWithObjectStoreAndConfigSetter(t, numControllers, objStore,
		func(conf *Conf) {
			az := fmt.Sprintf("az-%d", i%3)
			i++
			conf.AzInfo = az
		})
	defer tearDown(t)
	updateMembership(t, 1, 1, controllers, 0, 1, 2, 3, 4, 5)

	for _, controller := range controllers {
		agentMetas := controller.GetClusterMeta()
		require.Equal(t, numControllers, len(agentMetas))
		for i, meta := range agentMetas {
			require.Equal(t, i, int(meta.ID))
			require.Equal(t, fmt.Sprintf("kafka-address-%d:1234", i), meta.KafkaAddress)
			require.Equal(t, fmt.Sprintf("az-%d", i%3), meta.Location)
		}
	}
}

func TestGetClusterMetadataThisAz(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	numControllers := 6
	i := 0
	controllers, _, tearDown := setupControllersWithObjectStoreAndConfigSetter(t, numControllers, objStore,
		func(conf *Conf) {
			az := fmt.Sprintf("az-%d", i%3)
			i++
			conf.AzInfo = az
		})
	defer tearDown(t)
	updateMembership(t, 1, 1, controllers, 0, 1, 2, 3, 4, 5)

	for _, controller := range controllers {
		agentMetas := controller.GetClusterMetaThisAz()
		require.Equal(t, 2, len(agentMetas))
		for _, meta := range agentMetas {
			require.Equal(t, controller.cfg.AzInfo, meta.Location)
		}
	}
}
