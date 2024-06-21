//go:build integration

package integration

import (
	"fmt"
	"github.com/spirit-labs/tektite/command"
	"github.com/spirit-labs/tektite/kafka/fake"
	"github.com/spirit-labs/tektite/server"
	"github.com/spirit-labs/tektite/shutdown"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func getManagers(servers []*server.Server) []command.ManagerTest {
	var mgrs []command.ManagerTest
	for _, s := range servers {
		mgrs = append(mgrs, s.GetCommandManager().(command.ManagerTest))
	}
	return mgrs
}

func TestManagerMultipleCommands(t *testing.T) {

	fakeKafka := &fake.Kafka{}
	_, err := fakeKafka.CreateTopic("test_topic", 16)
	require.NoError(t, err)

	servers, tearDown := startCluster(t, 3, fakeKafka)
	defer tearDown(t)
	mgrs := getManagers(servers)

	mgr := mgrs[0]
	numCommands := 10
	for i := 0; i < numCommands; i++ {
		tsl := fmt.Sprintf(`test_stream%d :=
		(bridge from test_topic partitions = 16) -> (store stream)`, i)
		err := mgr.ExecuteCommand(tsl)
		require.NoError(t, err)
	}

	// Wait until commands are processed on all managers
	for _, mgr := range mgrs {
		m := mgr
		testutils.WaitUntil(t, func() (bool, error) {
			return m.LastProcessedCommandID() == int64(numCommands-1), nil
		})
	}

	for _, s := range servers {
		for i := 0; i < 10; i++ {
			pi := s.GetStreamManager().GetStream(fmt.Sprintf("test_stream%d", i))
			require.NotNil(t, pi)
			require.Equal(t, int64(i), pi.CommandID)
		}
	}
}

func TestManagersReload(t *testing.T) {
	servers, tearDown := startCluster(t, 3, nil)
	defer tearDown(t)
	mgrs := getManagers(servers)

	mgr := mgrs[0]
	numCommands := 10
	for i := 0; i < numCommands; i++ {
		tsl := fmt.Sprintf(`test_stream%d :=
		(bridge from test_topic	partitions = 16) -> (store stream)`, i)
		err := mgr.ExecuteCommand(tsl)
		require.NoError(t, err)
	}

	// Wait until commands are processed on all managers
	for _, mgr := range mgrs {
		m := mgr
		testutils.WaitUntil(t, func() (bool, error) {
			return m.LastProcessedCommandID() == int64(numCommands-1), nil
		})
	}

	// Now shutdown
	cfg := servers[0].GetConfig()
	err := shutdown.PerformShutdown(&cfg, false)
	require.NoError(t, err)

	// Restart
	for i, s := range servers {
		s, err := server.NewServerWithClientFactory(s.GetConfig(), nil)
		require.NoError(t, err)
		servers[i] = s
	}
	startServers(t, servers)
	mgrs = getManagers(servers)

	// Commands should all be loaded on start
	for _, mgr := range mgrs {
		require.Equal(t, int64(numCommands-1), mgr.LastProcessedCommandID())
	}
	for _, s := range servers {
		for i := 0; i < 10; i++ {
			pi := s.GetStreamManager().GetStream(fmt.Sprintf("test_stream%d", i))
			require.NotNil(t, pi)
			require.Equal(t, int64(i), pi.CommandID)
		}
	}
}

func TestManagerStreamManagerErrors(t *testing.T) {
	servers, tearDown := startCluster(t, 3, nil)
	defer tearDown(t)
	mgrs := getManagers(servers)

	mgr := mgrs[0]
	numCommands := 10
	for i := 0; i < numCommands; i++ {
		// user errors from the stream manager will still be recorded as commands
		// we create deliberate errors by using the same stream name more than once
		tsl := `test_stream1 := 
		(bridge from test_topic	partitions = 16) -> (store stream)`
		err := mgr.ExecuteCommand(tsl)
		if i == 0 {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			require.Equal(t, `stream 'test_stream1' already exists (line 1 column 1):
test_stream1 := 
^`,
				err.Error())
		}
	}

	// It errored - but we store commands before processing so there will still be commands persisted
	for _, mgr := range mgrs {
		m := mgr
		testutils.WaitUntil(t, func() (bool, error) {
			return m.LastProcessedCommandID() == int64(numCommands-1), nil
		})
	}

	// And the stream should have been deployed
	for _, s := range servers {
		pi := s.GetStreamManager().GetStream("test_stream1")
		require.NotNil(t, pi)
	}
}

func TestManagerDeployAndUndeployStreams(t *testing.T) {
	servers, tearDown := startCluster(t, 3, nil)
	defer tearDown(t)
	mgrs := getManagers(servers)

	mgr := mgrs[0]
	numCommands := 10
	for i := 0; i < numCommands; i++ {
		tsl := fmt.Sprintf(`test_stream%d :=
		(bridge from test_topic	partitions = 16) -> (store stream)`, i)
		err := mgr.ExecuteCommand(tsl)
		require.NoError(t, err)
		tsl = fmt.Sprintf(`delete(test_stream%d)`, i)
		err = mgr.ExecuteCommand(tsl)
		require.NoError(t, err)
	}

	// Wait until commands are processed on all managers
	for _, mgr := range mgrs {
		m := mgr
		testutils.WaitUntil(t, func() (bool, error) {
			return m.LastProcessedCommandID() == int64(2*numCommands-1), nil
		})
	}

	for _, s := range servers {
		for i := 0; i < numCommands; i++ {
			pi := s.GetStreamManager().GetStream(fmt.Sprintf("test_stream%d", i))
			require.Nil(t, pi)
		}
	}
}

func TestManagerPrepareQuery(t *testing.T) {
	servers, tearDown := startCluster(t, 3, nil)
	defer tearDown(t)
	mgrs := getManagers(servers)

	mgr := mgrs[0]

	command := `test_stream := 
	(bridge from test_topic	partitions = 16) -> 
    (project json_int("v0",val) as k0,json_float("v1",val) as k1,json_bool("v2",val) as k2,to_decimal(json_string("v3",val),14,3) as k3,
			 json_string("v4",val) as k4,to_bytes(json_string("v5",val)) as k5,to_timestamp(json_int("v6",val)) as k6)->
	(store table by k0,k1,k2,k3,k4,k5,k6)`
	err := mgr.ExecuteCommand(command)
	require.NoError(t, err)

	tsl := "prepare test_query1 := (get $p1:int,$p2:float,$p3:bool,$p4:decimal(14,3),$p5:string,$p6:bytes,$p7:timestamp from test_stream)"
	err = mgr.ExecuteCommand(tsl)
	require.NoError(t, err)

	// Wait until commands are processed on all managers
	for _, mgr := range mgrs {
		m := mgr
		testutils.WaitUntil(t, func() (bool, error) {
			return m.LastProcessedCommandID() == 1, nil
		})
	}
	decType := &types.DecimalType{
		Precision: 14,
		Scale:     3,
	}
	for _, s := range servers {
		paramSchema := s.GetQueryManager().GetPreparedQueryParamSchema("test_query1")
		require.NotNil(t, paramSchema)
		require.Equal(t, []string{"$p1:int", "$p2:float", "$p3:bool", "$p4:decimal(14,3)", "$p5:string", "$p6:bytes", "$p7:timestamp"}, paramSchema.ColumnNames())
		require.Equal(t, []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool,
			decType, types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp}, paramSchema.ColumnTypes())
	}
}

func TestManagerPrepareQueryInvalidPSParamTypes(t *testing.T) {
	testManagerPrepareQueryError(t,
		"prepare foo := (scan $p1:fatint to $p2:varchar from test_stream)",
		`invalid statement (line 1 column 25):
prepare foo := (scan $p1:fatint to $p2:varchar from test_stream)
                        ^`)
}

func testManagerPrepareQueryError(t *testing.T, tsl string, expectedErr string) {
	servers, tearDown := startCluster(t, 3, nil)
	defer tearDown(t)
	mgrs := getManagers(servers)

	mgr := mgrs[0]

	command := `test_stream := 
		(bridge from test_topic	partitions = 16) -> (store stream)`
	err := mgr.ExecuteCommand(command)
	require.NoError(t, err)

	err = mgr.ExecuteCommand(tsl)
	require.Error(t, err)
	require.Equal(t, expectedErr, err.Error())
}

func TestManagerCompaction(t *testing.T) {
	servers, tearDown := startCluster(t, 3, nil)
	defer tearDown(t)
	mgrs := getManagers(servers)

	mgr := mgrs[0]
	numCommands := 10
	for i := 0; i < numCommands; i++ {
		tsl := fmt.Sprintf(`test_stream%d :=
		(bridge from test_topic	partitions = 16) -> (store stream)`, i)
		err := mgr.ExecuteCommand(tsl)
		require.NoError(t, err)
		if i != numCommands-1 {
			tsl = fmt.Sprintf(`delete(test_stream%d)`, i)
			err = mgr.ExecuteCommand(tsl)
			require.NoError(t, err)
		}
	}

	// Wait until commands are processed on all managers
	for _, mgr := range mgrs {
		m := mgr
		testutils.WaitUntil(t, func() (bool, error) {
			return m.LastProcessedCommandID() == int64(2*numCommands-2), nil
		})
	}

	for _, s := range servers {
		for i := 0; i < numCommands; i++ {
			pi := s.GetStreamManager().GetStream(fmt.Sprintf("test_stream%d", i))
			if i < numCommands-1 {
				require.Nil(t, pi)
			} else {
				require.NotNil(t, pi)
			}
		}
	}

	batch, err := mgr.LoadCommands(0)
	require.NoError(t, err)
	require.Equal(t, 2*numCommands-1, batch.RowCount)

	mgr.SetClusterCompactor(true)
	err = mgr.MaybeCompact()
	require.NoError(t, err)

	testutils.WaitUntil(t, func() (bool, error) {
		batch, err = mgr.LoadCommands(0)
		if err != nil {
			return false, err
		}
		return batch.RowCount == 1, nil
	})

	for _, s := range servers {
		for i := 0; i < numCommands; i++ {
			pi := s.GetStreamManager().GetStream(fmt.Sprintf("test_stream%d", i))
			if i < numCommands-1 {
				require.Nil(t, pi)
			} else {
				require.NotNil(t, pi)
			}
		}
	}

	// Now shutdown
	cfg := servers[0].GetConfig()
	err = shutdown.PerformShutdown(&cfg, false)
	require.NoError(t, err)

	// Restart
	for i, s := range servers {
		s, err := server.NewServerWithClientFactory(s.GetConfig(), nil)
		require.NoError(t, err)
		servers[i] = s
	}
	startServers(t, servers)
	mgrs = getManagers(servers)

	// stream should be there
	for _, s := range servers {
		for i := 0; i < numCommands; i++ {
			pi := s.GetStreamManager().GetStream(fmt.Sprintf("test_stream%d", i))
			if i < numCommands-1 {
				require.Nil(t, pi)
			} else {
				require.NotNil(t, pi)
			}
		}
	}

}
