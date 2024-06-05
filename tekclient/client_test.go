package tekclient

import (
	"fmt"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/api"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/command"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/types"
	"github.com/spirit-labs/tektite/wasm"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	serverKeyPath  = "testdata/serverkey.pem"
	serverCertPath = "testdata/servercert.pem"
)

func init() {
	common.EnableTestPorts()
}

func TestExecuteCommand(t *testing.T) {
	tsl := `test_stream := (bridge from test_topic partitions = 23) -> (store stream)`
	testExecuteCommand(t, tsl)
	tsl = `delete(test_stream)`
	testExecuteCommand(t, tsl)
}

func TestCannotConnect(t *testing.T) {
	clientTLSConfig := TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
	cl, err := NewClient("127.0.0.1:6889", clientTLSConfig)
	require.NoError(t, err)
	defer cl.Close()

	tsl := `test_stream := (bridge from test_topic partitions = 23) -> (store stream)`
	err = cl.ExecuteStatement(tsl)
	require.Error(t, err)
	require.Equal(t, "connection error: dial tcp 127.0.0.1:6889: connect: connection refused", err.Error())
}

func testExecuteCommand(t *testing.T, command string) {
	server, _, commandMgr, _, cl := setup(t)
	defer func() {
		cl.Close()
		err := server.Stop()
		require.NoError(t, err)
	}()
	err := cl.ExecuteStatement(command)
	require.NoError(t, err)
	receivedCommand := commandMgr.getCommand()
	require.Equal(t, command, receivedCommand)
}

func TestExecuteCommandError(t *testing.T) {
	tsl := `test_stream := (broodge from test_topic partitions = 23) -> (store stream)`
	testExecuteCommandError(t, tsl,
		`expected one of: 'aggregate', 'backfill', 'bridge', 'filter', 'join', 'kafka', 'partition', 'producer', 'project', 'store', 'topic', 'union' (line 1 column 17):
test_stream := (broodge from test_topic partitions = 23) -> (store stream)
                ^`)
	testExecuteCommandError(t, "adasdasdasd", "reached end of statement")
	testExecuteCommandError(t, "", "statement is empty")
}

func testExecuteCommandError(t *testing.T, tsl string, errMsg string) {
	server, _, _, _, cl := setup(t)
	defer func() {
		cl.Close()
		err := server.Stop()
		require.NoError(t, err)
	}()
	err := cl.ExecuteStatement(tsl)
	require.Error(t, err)
	require.Equal(t, errMsg, err.Error())
}

func TestExecuteQuery(t *testing.T) {

	server, queryMgr, _, _, cl := setup(t)
	defer func() {
		cl.Close()
		err := server.Stop()
		require.NoError(t, err)
	}()

	batches := createBatches(t, 0, 100, 1)
	queryMgr.addBatch(batches[0], true)

	tsl := "(scan all from some_table)"

	res, err := cl.ExecuteQuery(tsl)
	require.NoError(t, err)

	require.Equal(t, tsl, queryMgr.getDirectQueryState())

	checkReceivedRows(t, res, 0)
}

func TestExecuteQueryError(t *testing.T) {
	testExecuteQueryError(t, "qwdqwdqwdqwd",
		`expected '(' but found 'qwdqwdqwdqwd' (line 1 column 1):
qwdqwdqwdqwd
^`)
	testExecuteQueryError(t, "(scran all from some_table)",
		`expected one of: 'get', 'scan', 'project', 'filter', 'sort' but found 'scran' (line 1 column 2):
(scran all from some_table)
 ^`)
}

func testExecuteQueryError(t *testing.T, query string, errMsg string) {

	server, queryMgr, _, _, cl := setup(t)
	defer func() {
		cl.Close()
		err := server.Stop()
		require.NoError(t, err)
	}()

	batches := createBatches(t, 0, 100, 1)
	queryMgr.addBatch(batches[0], true)

	_, err := cl.ExecuteQuery(query)
	require.Error(t, err)

	require.Equal(t, errMsg, err.Error())
}

func TestStreamExecuteQuery(t *testing.T) {
	server, queryMgr, _, _, cl := setup(t)
	defer func() {
		cl.Close()
		err := server.Stop()
		require.NoError(t, err)
	}()

	batches := createBatches(t, 0, 100, 10)
	for i, batch := range batches {
		queryMgr.addBatch(batch, i == len(batches)-1)
	}

	tsl := "(scan all from some_table)"

	ch, err := cl.StreamExecuteQuery(tsl)
	require.NoError(t, err)

	require.Equal(t, tsl, queryMgr.getDirectQueryState())

	c := 0
	for res := range ch {
		require.Nil(t, res.Err)
		res := res.Chunk

		c = checkReceivedRows(t, res, c)
	}
}

func TestStreamExecuteQueryError(t *testing.T) {
	testStreamExecuteQueryError(t, "qwdqwdqwdqwd",
		`expected '(' but found 'qwdqwdqwdqwd' (line 1 column 1):
qwdqwdqwdqwd
^`)
	testStreamExecuteQueryError(t, "(scran all from some_table)",
		`expected one of: 'get', 'scan', 'project', 'filter', 'sort' but found 'scran' (line 1 column 2):
(scran all from some_table)
 ^`)
}

func testStreamExecuteQueryError(t *testing.T, tsl string, errMsg string) {
	server, queryMgr, _, _, cl := setup(t)
	defer func() {
		cl.Close()
		err := server.Stop()
		require.NoError(t, err)
	}()

	batches := createBatches(t, 0, 100, 10)
	for i, batch := range batches {
		queryMgr.addBatch(batch, i == len(batches)-1)
	}

	_, err := cl.StreamExecuteQuery(tsl)
	require.Error(t, err)
	require.Equal(t, errMsg, err.Error())
}

func TestPreparedQuery(t *testing.T) {
	server, queryMgr, commandMgr, _, cl := setup(t)
	defer func() {
		cl.Close()
		err := server.Stop()
		require.NoError(t, err)
	}()

	batches := createBatches(t, 0, 100, 1)
	queryMgr.addBatch(batches[0], true)

	query := "(scan $p1:int,$p2:float,$p3:bool to $p4:decimal(23,5),$p5:string,$p6:bytes from some_table)->(filter by $p7:timestamp == to_timestamp(12345))"

	pq, err := cl.PrepareQuery("test_query", query)
	require.NoError(t, err)

	rTsl := commandMgr.getCommand()
	require.Equal(t, fmt.Sprintf("prepare test_query := %s", query), rTsl)

	decType := &types.DecimalType{
		Precision: 23,
		Scale:     5,
	}
	queryMgr.setParamMetaData([]string{"$p1:int", "$p2:float", "$p3:bool", "$p4:decimal(23,5)", "$p5:string", "$p6:bytes", "$p7:timestamp"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp})

	decNum, err := decimal128.FromString("1234.4321", int32(decType.Precision), int32(decType.Scale))
	require.NoError(t, err)
	decVal := types.Decimal{
		Num:       decNum,
		Precision: decType.Precision,
		Scale:     decType.Scale,
	}

	args := make([]any, 7)

	iVal := int64(23)
	pq.SetIntArg(0, iVal)
	args[0] = iVal

	fVal := float64(1.23)
	pq.SetFloatArg(1, fVal)
	args[1] = fVal

	pq.SetBoolArg(2, true)
	args[2] = true

	pq.SetDecimalArg(3, decVal)
	args[3] = decVal

	sVal := "aardvark"
	pq.SetStringArg(4, sVal)
	args[4] = sVal

	bytesVal := []byte("zebras")
	pq.SetBytesArg(5, bytesVal)
	args[5] = bytesVal

	tsVal := types.NewTimestamp(987654)
	pq.SetTimestampArg(6, tsVal)
	args[6] = tsVal

	res, err := pq.Execute()
	require.NoError(t, err)

	checkReceivedRows(t, res, 0)

	receivedQuery, receivedArgs := queryMgr.getExecState()
	require.Equal(t, "test_query", receivedQuery)
	require.Equal(t, args, receivedArgs)

	checkReceivedRows(t, res, 0)
}

func TestPreparedQueryWithNullArgs(t *testing.T) {
	server, queryMgr, commandMgr, _, cl := setup(t)
	defer func() {
		cl.Close()
		err := server.Stop()
		require.NoError(t, err)
	}()

	batches := createBatches(t, 0, 100, 1)
	queryMgr.addBatch(batches[0], true)

	query := "(scan $p1:int,$p2:float,$p3:bool to $p4:decimal(23,5),$p5:string,$p6:bytes from some_table)->(filter by $p7:timestamp == to_timestamp(12345))"

	pq, err := cl.PrepareQuery("test_query", query)
	require.NoError(t, err)

	rTsl := commandMgr.getCommand()
	require.Equal(t, fmt.Sprintf("prepare test_query := %s", query), rTsl)

	decType := &types.DecimalType{
		Precision: 23,
		Scale:     5,
	}
	queryMgr.setParamMetaData([]string{"$p1:int", "$p2:float", "$p3:bool", "$p4:decimal(23,5)", "$p5:string", "$p6:bytes", "$p7:timestamp"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp})

	args := make([]any, 7)
	pq.SetNullArg(0)
	pq.SetNullArg(1)
	pq.SetNullArg(2)
	pq.SetNullArg(3)
	pq.SetNullArg(4)
	pq.SetNullArg(5)
	pq.SetNullArg(6)

	res, err := pq.Execute()
	require.NoError(t, err)

	checkReceivedRows(t, res, 0)

	receivedQuery, receivedArgs := queryMgr.getExecState()
	require.Equal(t, "test_query", receivedQuery)
	require.Equal(t, args, receivedArgs)

	checkReceivedRows(t, res, 0)
}

func TestPrepareQueryTslError(t *testing.T) {
	testPrepareQueryError(t, "test_query", "(scran range $start to $end from some_table)",
		`expected one of: 'get', 'scan', 'project', 'filter', 'sort' but found 'scran' (line 1 column 24):
prepare test_query := (scran range $start to $end from some_table)
                       ^`)
}

func testPrepareQueryError(t *testing.T, queryName string, tsl string, errMsg string) {
	server, queryMgr, _, _, cl := setup(t)
	defer func() {
		cl.Close()
		err := server.Stop()
		require.NoError(t, err)
	}()

	batches := createBatches(t, 0, 100, 1)
	queryMgr.addBatch(batches[0], true)

	_, err := cl.PrepareQuery(queryName, tsl)
	require.Error(t, err)
	require.Equal(t, errMsg, err.Error())
}

func TestStreamExecutePreparedQuery(t *testing.T) {
	server, queryMgr, _, _, cl := setup(t)
	defer func() {
		cl.Close()
		err := server.Stop()
		require.NoError(t, err)
	}()

	batches := createBatches(t, 0, 100, 10)
	for i, batch := range batches {
		queryMgr.addBatch(batch, i == len(batches)-1)
	}

	query := "(scan $p1:int,$p2:float,$p3:bool to $p4:decimal(23,5),$p5:string,$p6:bytes from some_table)->(filter by $p7:timestamp == to_timestamp(12345))"

	queryMgr.setParamMetaData([]string{"$p1:int", "$p2:float", "$p3:bool", "$p4:decimal(23,5)", "$p5:string", "$p6:bytes", "$p7:timestamp"},
		[]types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp})

	pq, err := cl.PrepareQuery("test_query", query)
	require.NoError(t, err)

	decNum, err := decimal128.FromString("1234.4321", int32(decType.Precision), int32(decType.Scale))
	require.NoError(t, err)
	decVal := types.Decimal{
		Num:       decNum,
		Precision: decType.Precision,
		Scale:     decType.Scale,
	}

	pq.SetIntArg(0, 23)
	pq.SetFloatArg(1, 1.23)
	pq.SetBoolArg(2, true)
	pq.SetDecimalArg(3, decVal)
	pq.SetStringArg(4, "aardvark")
	pq.SetBytesArg(5, []byte("zebras"))
	pq.SetTimestampArg(6, types.NewTimestamp(987654))

	ch, err := pq.StreamExecute()
	require.NoError(t, err)

	c := 0
	for res := range ch {
		require.Nil(t, res.Err)
		res := res.Chunk
		c = checkReceivedRows(t, res, c)
	}
}

func TestExecuteRegisterUnregisterWasmModule(t *testing.T) {
	server, _, _, moduleManager, cl := setup(t)
	defer func() {
		cl.Close()
		err := server.Stop()
		require.NoError(t, err)
	}()
	err := cl.RegisterWasmModule("testdata/wasm/test_mod1.wasm")
	require.NoError(t, err)
	require.True(t, moduleManager.registerCalled.Load())

	err = cl.UnregisterWasmModule("test_mod1")
	require.NoError(t, err)
	require.True(t, moduleManager.unregisterCalled.Load())
}

func setup(t *testing.T) (*api.HTTPAPIServer, *testQueryManager, *testCommandManager, *testWasmModuleManager, Client) {
	queryMgr := &testQueryManager{}
	commandMgr := &testCommandManager{}
	tlsConf := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  serverKeyPath,
		CertPath: serverCertPath,
	}
	moduleManager := &testWasmModuleManager{}
	address, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	server := api.NewHTTPAPIServer(address, "/tektite", queryMgr, commandMgr,
		parser.NewParser(nil), moduleManager, tlsConf)
	err = server.Activate()
	require.NoError(t, err)
	clientTLSConfig := TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
	cl, err := NewClient(address, clientTLSConfig)
	require.NoError(t, err)
	return server, queryMgr, commandMgr, moduleManager, cl
}

type testQueryManager struct {
	lock      sync.Mutex
	queryName string
	args      []any

	batches []batchInfo
	numLast int

	paramsSchema *evbatch.EventSchema

	directQuerytsl string
}

func (t *testQueryManager) GetLastCompletedVersion() int {
	return 0
}

func (t *testQueryManager) GetLastFlushedVersion() int {
	return 0
}

func (t *testQueryManager) Activate() {
}

func (t *testQueryManager) ExecuteQueryDirect(tsl string, _ parser.QueryDesc, outputFunc func(last bool, numLastBatches int, batch *evbatch.Batch) error) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.directQuerytsl = tsl
	t.sendBatches(outputFunc)
	return nil
}

func (t *testQueryManager) getDirectQueryState() string {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.directQuerytsl
}

type batchInfo struct {
	batch *evbatch.Batch
	last  bool
}

func (t *testQueryManager) addBatch(batch *evbatch.Batch, last bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.batches = append(t.batches, batchInfo{
		batch: batch,
		last:  last,
	})
	if last {
		t.numLast++
	}
}

func (t *testQueryManager) clearBatches() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.batches = nil
	t.numLast = 0
}

func (t *testQueryManager) setParamMetaData(paramNames []string, paramTypes []types.ColumnType) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.paramsSchema = evbatch.NewEventSchema(paramNames, paramTypes)
}

func (t *testQueryManager) getExecState() (string, []any) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.queryName, t.args
}

func (t *testQueryManager) PrepareQuery(parser.PrepareQueryDesc) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	return nil
}

func (t *testQueryManager) ExecutePreparedQuery(queryName string, args []any, outputFunc func(last bool, numLastBatches int, batch *evbatch.Batch) error) (int, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.queryName = queryName
	t.args = args
	t.sendBatches(outputFunc)
	return 0, nil
}

func (t *testQueryManager) sendBatches(outputFunc func(last bool, numLastBatches int, batch *evbatch.Batch) error) {
	go func() {
		t.lock.Lock()
		defer t.lock.Unlock()
		for _, info := range t.batches {
			err := outputFunc(info.last, t.numLast, info.batch)
			if err != nil {
				panic(err)
			}
		}
	}()
}

func (t *testQueryManager) ExecutePreparedQueryWithHighestVersion(string, []any, int64, func(last bool, numLastBatches int, batch *evbatch.Batch) error) (int, error) {
	return 0, nil
}

func (t *testQueryManager) SetLastCompletedVersion(int64) {
}

func (t *testQueryManager) SetLastCompletedVersionAllNodes() {
}

func (t *testQueryManager) SetClusterMessageHandlers(remoting.Server, *remoting.TeeBlockingClusterMessageHandler) {
}

func (t *testQueryManager) Start() error {
	return nil
}

func (t *testQueryManager) Stop() error {
	return nil
}

func (t *testQueryManager) ExecuteRemoteQuery(*clustermsgs.QueryMessage) error {
	return nil
}

func (t *testQueryManager) ReceiveQueryResult(*clustermsgs.QueryResponse) {
}

func (t *testQueryManager) GetPreparedQueryParamSchema(string) *evbatch.EventSchema {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.paramsSchema
}

type testCommandManager struct {
	lock sync.Mutex
	tsl  string
}

func (t *testCommandManager) SetPrefixRetentionService(command.PrefixRetention) {
}

func (t *testCommandManager) HandleClusterState(clustmgr.ClusterState) error {
	return nil
}

func (t *testCommandManager) ExecuteCommand(tsl string) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.tsl = tsl
	return nil
}

func (t *testCommandManager) getCommand() string {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.tsl
}

func (t *testCommandManager) SetCommandSignaller(command.Signaller) {
}

func (t *testCommandManager) Start() error {
	return nil
}

func (t *testCommandManager) Stop() error {
	return nil
}

func createBatches(t require.TestingT, start int, numRowsPerBatch int, numBatches int) []*evbatch.Batch {
	var batches []*evbatch.Batch
	index := start
	for i := 0; i < numBatches; i++ {
		colBuilders := evbatch.CreateColBuilders(columnTypes)
		for rowIndex := 0; rowIndex < numRowsPerBatch; rowIndex++ {
			if index%3 == 0 {
				colBuilders[0].AppendNull()
			} else {
				colBuilders[0].(*evbatch.IntColBuilder).Append(int64(1000000 + index))
			}
			if (index+1)%3 == 0 {
				colBuilders[1].AppendNull()
			} else {
				colBuilders[1].(*evbatch.FloatColBuilder).Append(float64(index) + 0.12345)
			}
			if (index+2)%3 == 0 {
				colBuilders[2].AppendNull()
			} else {
				colBuilders[2].(*evbatch.BoolColBuilder).Append(index%2 == 0)
			}
			if (index+3)%3 == 0 {
				colBuilders[3].AppendNull()
			} else {
				decNum, err := decimal128.FromString(fmt.Sprintf("%d123456789.987654321", index), int32(decType.Precision),
					int32(decType.Scale))
				require.NoError(t, err)
				dec := types.Decimal{
					Num:       decNum,
					Precision: decType.Precision,
					Scale:     decType.Scale,
				}
				colBuilders[3].(*evbatch.DecimalColBuilder).Append(dec)
			}
			if (index+4)%3 == 0 {
				colBuilders[4].AppendNull()
			} else {
				colBuilders[4].(*evbatch.StringColBuilder).Append(fmt.Sprintf("foobar-%d", index))
			}
			if (index+5)%3 == 0 {
				colBuilders[5].AppendNull()
			} else {
				colBuilders[5].(*evbatch.BytesColBuilder).Append([]byte(fmt.Sprintf("quux-%d", index)))
			}
			if (index+6)%3 == 0 {
				colBuilders[6].AppendNull()
			} else {
				ts := types.NewTimestamp(int64(2000000 + index))
				colBuilders[6].(*evbatch.TimestampColBuilder).Append(ts)
			}
			index++
		}
		schema := evbatch.NewEventSchema(columnNames, columnTypes)
		batch := evbatch.NewBatchFromBuilders(schema, colBuilders...)
		batches = append(batches, batch)
	}
	return batches
}

func checkReceivedRows(t *testing.T, res QueryResult, index int) int {
	require.Equal(t, columnNames, res.Meta().ColumnNames())
	require.Equal(t, columnTypes, res.Meta().ColumnTypes())
	require.Equal(t, 100, res.RowCount())
	require.Equal(t, len(columnNames), res.ColumnCount())

	for rowIndex := 0; rowIndex < res.RowCount(); rowIndex++ {
		row := res.Row(rowIndex)

		if index%3 == 0 {
			require.True(t, row.IsNull(0))
		} else {
			iv := row.IntVal(0)
			require.Equal(t, int64(1000000+index), iv)
		}

		if (index+1)%3 == 0 {
			require.True(t, row.IsNull(1))
		} else {
			fv := row.FloatVal(1)
			require.Equal(t, float64(index)+0.12345, fv)
		}

		if (index+2)%3 == 0 {
			require.True(t, row.IsNull(2))
		} else {
			b := row.BoolVal(2)
			require.Equal(t, index%2 == 0, b)
		}

		if (index+3)%3 == 0 {
			require.True(t, row.IsNull(3))
		} else {
			d := row.DecimalVal(3)
			decNum, err := decimal128.FromString(fmt.Sprintf("%d123456789.987654321", index), int32(decType.Precision),
				int32(decType.Scale))
			require.NoError(t, err)
			dec := types.Decimal{
				Num:       decNum,
				Precision: decType.Precision,
				Scale:     decType.Scale,
			}
			require.Equal(t, dec, d)
		}

		if (index+4)%3 == 0 {
			require.True(t, row.IsNull(4))
		} else {
			s := row.StringVal(4)
			require.Equal(t, fmt.Sprintf("foobar-%d", index), s)
		}

		if (index+5)%3 == 0 {
			require.True(t, row.IsNull(5))
		} else {
			bytes := row.BytesVal(5)
			require.Equal(t, []byte(fmt.Sprintf("quux-%d", index)), bytes)
		}

		if (index+6)%3 == 0 {
			require.True(t, row.IsNull(6))
		} else {
			ts := row.TimestampVal(6)
			require.Equal(t, types.NewTimestamp(int64(2000000+index)), ts)
		}
		index++
	}
	return index
}

var columnNames = []string{"f0", "f1", "f2", "f3", "f4", "f5", "f6"}
var decType = &types.DecimalType{
	Precision: 28,
	Scale:     4,
}
var columnTypes = []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType,
	types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp}

type testWasmModuleManager struct {
	registerCalled   atomic.Bool
	unregisterCalled atomic.Bool
}

func (t *testWasmModuleManager) RegisterModule(wasm.ModuleMetadata, []byte) error {
	t.registerCalled.Store(true)
	return nil
}

func (t *testWasmModuleManager) UnregisterModule(string) error {
	t.unregisterCalled.Store(true)
	return nil
}
