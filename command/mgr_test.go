package command

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/lock"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/opers"
	parser2 "github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/query"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/retention"
	"github.com/spirit-labs/tektite/sequence"
	store2 "github.com/spirit-labs/tektite/store"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/tppm"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagerMultipleCommands(t *testing.T) {
	st := store2.TestStore()
	err := st.Start()
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer st.Stop()
	mgrs, pMgrs, _, _, tr := setupManagers(t, st)
	defer tr.stop()

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

	for _, pMgr := range pMgrs {
		for i := 0; i < 10; i++ {
			pi := pMgr.GetStream(fmt.Sprintf("test_stream%d", i))
			require.NotNil(t, pi)
			require.Equal(t, int64(i), pi.CommandID)
		}
	}
}

func TestManagersReload(t *testing.T) {
	st := store2.TestStore()
	err := st.Start()
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer st.Stop()
	mgrs, _, _, _, tr1 := setupManagers(t, st)
	defer tr1.stop()

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

	// Now recreate managers and stream managers
	mgrs, pMgrs, _, _, tr2 := setupManagers(t, st)
	defer tr2.stop()

	// Commands should all be loaded on start
	for _, mgr := range mgrs {
		require.Equal(t, int64(numCommands-1), mgr.LastProcessedCommandID())
	}
	for _, pMgr := range pMgrs {
		for i := 0; i < 10; i++ {
			pi := pMgr.GetStream(fmt.Sprintf("test_stream%d", i))
			require.NotNil(t, pi)
			require.Equal(t, int64(i), pi.CommandID)
		}
	}
}

func TestManagerStreamManagerErrors(t *testing.T) {
	st := store2.TestStore()
	err := st.Start()
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer st.Stop()
	mgrs, pMgrs, _, _, tr := setupManagers(t, st)
	defer tr.stop()

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
	for _, pMgr := range pMgrs {
		pi := pMgr.GetStream("test_stream1")
		require.NotNil(t, pi)
	}
}

func TestManagerDeployAndUndeployStreams(t *testing.T) {
	st := store2.TestStore()
	err := st.Start()
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer st.Stop()
	mgrs, pMgrs, _, _, tr := setupManagers(t, st)
	defer tr.stop()

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

	for _, pMgr := range pMgrs {
		for i := 0; i < numCommands; i++ {
			pi := pMgr.GetStream(fmt.Sprintf("test_stream%d", i))
			require.Nil(t, pi)
		}
	}
}

func TestManagerPrepareQuery(t *testing.T) {
	st := store2.TestStore()
	err := st.Start()
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer st.Stop()
	mgrs, _, qMgrs, _, tr := setupManagers(t, st)
	defer tr.stop()

	mgr := mgrs[0]

	command := `test_stream := 
	(bridge from test_topic	partitions = 16) -> 
    (project json_int("v0",val) as k0,json_float("v1",val) as k1,json_bool("v2",val) as k2,to_decimal(json_string("v3",val),14,3) as k3,
			 json_string("v4",val) as k4,to_bytes(json_string("v5",val)) as k5,to_timestamp(json_int("v6",val)) as k6)->
	(store table by k0,k1,k2,k3,k4,k5,k6)`
	err = mgr.ExecuteCommand(command)
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
	for _, qMgr := range qMgrs {
		paramSchema := qMgr.GetPreparedQueryParamSchema("test_query1")
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
	st := store2.TestStore()
	err := st.Start()
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer st.Stop()
	mgrs, _, _, _, tr := setupManagers(t, st)
	defer tr.stop()

	mgr := mgrs[0]

	command := `test_stream := 
		(bridge from test_topic	partitions = 16) -> (store stream)`
	err = mgr.ExecuteCommand(command)
	require.NoError(t, err)

	err = mgr.ExecuteCommand(tsl)
	require.Error(t, err)
	require.Equal(t, expectedErr, err.Error())
}

func TestManagerCompaction(t *testing.T) {
	st := store2.TestStore()
	err := st.Start()
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer st.Stop()
	mgrs, pMgrs, _, _, tr := setupManagers(t, st)
	defer tr.stop()

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

	for _, pMgr := range pMgrs {
		for i := 0; i < numCommands; i++ {
			pi := pMgr.GetStream(fmt.Sprintf("test_stream%d", i))
			if i < numCommands-1 {
				require.Nil(t, pi)
			} else {
				require.NotNil(t, pi)
			}
		}
	}

	batch, err := mgr.loadCommands(0)
	require.NoError(t, err)
	require.Equal(t, 2*numCommands-1, batch.RowCount)

	mgr.SetClusterCompactor(true)
	err = mgr.maybeCompact()
	require.NoError(t, err)

	testutils.WaitUntil(t, func() (bool, error) {
		batch, err = mgr.loadCommands(0)
		if err != nil {
			return false, err
		}
		return batch.RowCount == 1, nil
	})

	for _, pMgr := range pMgrs {
		for i := 0; i < numCommands; i++ {
			pi := pMgr.GetStream(fmt.Sprintf("test_stream%d", i))
			if i < numCommands-1 {
				require.Nil(t, pi)
			} else {
				require.NotNil(t, pi)
			}
		}
	}

	// Now restart
	mgrs, pMgrs, _, _, tr2 := setupManagers(t, st)
	defer tr2.stop()

	// stream should be there
	for _, pMgr := range pMgrs {
		for i := 0; i < numCommands; i++ {
			pi := pMgr.GetStream(fmt.Sprintf("test_stream%d", i))
			if i < numCommands-1 {
				require.Nil(t, pi)
			} else {
				require.NotNil(t, pi)
			}
		}
	}

}

func TestManagerCannotCompactWhenDeletedPrefixes(t *testing.T) {
	st := store2.TestStore()
	err := st.Start()
	require.NoError(t, err)
	//goland:noinspection GoUnhandledErrorResult
	defer st.Stop()
	mgrs, pMgrs, _, dps, tr := setupManagers(t, st)
	defer tr.stop()

	for _, dp := range dps {
		dp.(*testPrefixRetention).HasPending.Store(true)
	}

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

	for _, pMgr := range pMgrs {
		for i := 0; i < numCommands; i++ {
			pi := pMgr.GetStream(fmt.Sprintf("test_stream%d", i))
			if i < numCommands-1 {
				require.Nil(t, pi)
			} else {
				require.NotNil(t, pi)
			}
		}
	}

	batch, err := mgr.loadCommands(0)
	require.NoError(t, err)
	require.Equal(t, 2*numCommands-1, batch.RowCount)

	mgr.SetClusterCompactor(true)
	err = mgr.maybeCompact()
	require.NoError(t, err)

	time.Sleep(250 * time.Millisecond)

	batch, err = mgr.loadCommands(0)
	require.NoError(t, err)
	require.Equal(t, 2*numCommands-1, batch.RowCount)
}

func setUpManager(signaller Signaller, remoting *testRemoting, st *store2.Store,
	addresses []string, nodeID int) (*manager, opers.StreamManager, query.Manager, PrefixRetention) {

	seqMgr := sequence.NewInMemSequenceManager()
	lockMgr := lock.NewInMemLockManager()

	pm := tppm.NewTestProcessorManager(st)
	pm.SetWriteVersion(10)

	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.NodeID = nodeID

	pMgr := opers.NewStreamManager(nil, st, &dummyPrefixRetention{}, &expr.ExpressionFactory{}, cfg, true)

	pm.SetBatchHandler(pMgr)
	pMgr.SetProcessorManager(pm)
	pMgr.Loaded()

	nodePartitions := map[int][]int{}
	nodePartitions[0] = []int{0}
	npp := tppm.NewTestNodePartitionProvider(nodePartitions)

	parser := parser2.NewParser(nil)

	qMgr := query.NewManager(npp, &tppm.TestClustVersionProvider{ClustVersion: 1234}, cfg.NodeID, pMgr, st, st,
		remoting, addresses, 100, &expr.ExpressionFactory{}, parser)
	// Set the last completed versions to be less than the write version. Normally this would make the written commands
	// invisible. However, when reading commands we execute the query with highest version = 0 so we should see them
	// immediately. This is important so when a command is written from one node it is visible straight away from another
	// node without waiting for the version to roll. Snapshot consistency does not matter here.
	qMgr.SetLastCompletedVersion(8)

	pm.AddActiveProcessor(0)
	processor := pm.GetProcessor(0)

	dPrefixes := &testPrefixRetention{}

	mgr := NewCommandManager(pMgr, qMgr, seqMgr, lockMgr, &singleProcessorForwarder{processor: processor}, &dummyVmgrClient{}, parser, cfg)
	mgr.SetCommandSignaller(signaller)
	mgr.SetPrefixRetentionService(dPrefixes)
	return mgr.(*manager), pMgr, qMgr, dPrefixes
}

type testPrefixRetention struct {
	HasPending atomic.Bool
}

func (t *testPrefixRetention) HasPendingPrefixesToRegister() bool {
	return t.HasPending.Load()
}

type singleProcessorForwarder struct {
	processor proc.Processor
}

func (s *singleProcessorForwarder) ForwardBatch(batch *proc.ProcessBatch, _ bool, completionFunc func(error)) {
	s.processor.IngestBatch(batch, completionFunc)
}

func setupManagers(t *testing.T, st *store2.Store) ([]*manager, []opers.StreamManager, []query.Manager,
	[]PrefixRetention, *testRemoting) {
	signaller := &testCommandSignaller{}
	rem := newTestRemoting()

	numManagers := 5
	var addresses []string
	for i := 0; i < numManagers; i++ {
		addresses = append(addresses, fmt.Sprintf("addr-%d", i))
	}

	var mgrs []*manager
	var pMgrs []opers.StreamManager
	var qMgrs []query.Manager
	var dPrefixes []PrefixRetention
	mgrsMap := map[string]query.Manager{}

	for i := 0; i < numManagers; i++ {
		mgr, pMgr, qMgr, dPrefix := setUpManager(signaller, rem, st, addresses, i)
		mgrs = append(mgrs, mgr)
		pMgrs = append(pMgrs, pMgr)
		qMgrs = append(qMgrs, qMgr)
		dPrefixes = append(dPrefixes, dPrefix)
		mgrsMap[addresses[i]] = qMgr
		if i != 0 {
			signaller.addManager(mgr)
		}
		initID := mgr.lastProcessedCommandID
		require.Equal(t, int64(-1), initID)
	}
	rem.mgrsMap = mgrsMap
	rem.start()

	for _, mgr := range mgrs {
		err := mgr.Start()
		require.NoError(t, err)
	}
	return mgrs, pMgrs, qMgrs, dPrefixes, rem
}

type sendInfo struct {
	msg *clustermsgs.QueryMessage
	mgr query.Manager
	cf  func(remoting.ClusterMessage, error)
}

func newTestRemoting() *testRemoting {
	return &testRemoting{
		sendChannel: make(chan sendInfo, 10),
	}
}

type testRemoting struct {
	mgrsMap     map[string]query.Manager
	sendChannel chan sendInfo
	unavailable atomic.Bool
	lock        sync.Mutex
	closed      bool
}

func (t *testRemoting) SetUnavailable() {
	t.unavailable.Store(true)
}

func (t *testRemoting) start() {
	common.Go(t.sendLoop)
}

func (t *testRemoting) stop() {
	t.lock.Lock()
	defer t.lock.Unlock()
	close(t.sendChannel)
	t.closed = true
}

func (t *testRemoting) sendLoop() {
	for sendInfo := range t.sendChannel {
		err := sendInfo.mgr.ExecuteRemoteQuery(sendInfo.msg)
		sendInfo.cf(nil, err)
	}
}

func (t *testRemoting) SendQueryMessageAsync(completionFunc func(remoting.ClusterMessage, error),
	msg *clustermsgs.QueryMessage, address string) {
	if t.unavailable.Load() {
		completionFunc(nil, remoting.Error{Msg: "test_unavailability"})
		return
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.closed {
		return
	}
	mgr, ok := t.mgrsMap[address]
	if !ok {
		panic("can't find manager")
	}
	t.sendChannel <- sendInfo{
		mgr: mgr,
		msg: msg,
		cf:  completionFunc,
	}
}

func (t *testRemoting) SendQueryResponse(msg *clustermsgs.QueryResponse, serverAddress string) error {
	mgr, ok := t.mgrsMap[serverAddress]
	if !ok {
		panic("can't find manager")
	}
	mgr.ReceiveQueryResult(msg)
	return nil
}

func (t *testRemoting) Close() {
}

type testCommandSignaller struct {
	lock sync.Mutex
	mgrs []*manager
}

func (t *testCommandSignaller) addManager(mgr *manager) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.mgrs = append(t.mgrs, mgr)
}

func (t *testCommandSignaller) CommandAvailable() {
	t.lock.Lock()
	defer t.lock.Unlock()
	for _, mgr := range t.mgrs {
		m := mgr
		common.Go(func() {
			if err := m.CheckProcessCommands(); err != nil {
				log.Errorf("failed to execute remote commands %v", err)
			}
		})
	}
}

type dummyVmgrClient struct {
}

func (c dummyVmgrClient) FailureDetected(int, int) error {
	return nil
}

func (c dummyVmgrClient) GetLastFailureFlushedVersion(int) (int, error) {
	return -1, nil
}

func (c dummyVmgrClient) FailureComplete(int, int) error {
	return nil
}

func (c dummyVmgrClient) IsFailureComplete(int) (bool, error) {
	return false, nil
}

func (c dummyVmgrClient) VersionFlushed(int, int, int, int) error {
	return nil
}

func (c dummyVmgrClient) GetVersions() (int, int, int, error) {
	return 0, 0, 0, nil
}

func (c dummyVmgrClient) VersionComplete(int, int, int, bool, func(error)) {
}

func (c dummyVmgrClient) Start() error {
	return nil
}

func (c dummyVmgrClient) Stop() error {
	return nil
}

type dummyPrefixRetention struct {
}

func (d *dummyPrefixRetention) AddPrefixRetention(retention.PrefixRetention) {
}
