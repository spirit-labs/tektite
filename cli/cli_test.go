// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

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
	"github.com/spirit-labs/tektite/tekclient"
	"github.com/spirit-labs/tektite/types"
	"github.com/spirit-labs/tektite/wasm"
	"github.com/stretchr/testify/require"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	caCertPath             = "testdata/cacert.pem"
	caSignedServerKeyPath  = "testdata/casignedserverkey.pem"
	caSignedServerCertPath = "testdata/casignedservercert.pem"

	caSignedClientKeyPath  = "testdata/casignedclientkey.pem"
	caSignedClientCertPath = "testdata/casignedclientcert.pem"

	selfSignedServerKeyPath  = "testdata/selfsignedserverkey.pem"
	selfSignedServerCertPath = "testdata/selfsignedservercert.pem"

	selfSignedClientKeyPath  = "testdata/selfsignedclientkey.pem"
	selfSignedClientCertPath = "testdata/selfsignedclientcert.pem"

	selfSignedClientKeyPath2  = "testdata/selfsignedclientkey2.pem"
	selfSignedClientCertPath2 = "testdata/selfsignedclientcert2.pem"
)

func init() {
	common.EnableTestPorts()
}

func TestHTTPWithTlsNoClientAuth(t *testing.T) {
	clientTLSConfig := tekclient.TLSConfig{
		TrustedCertsPath: caSignedServerCertPath,
	}
	serverTLSConfig := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  caSignedServerKeyPath,
		CertPath: caSignedServerCertPath,
	}
	testCli(t, clientTLSConfig, serverTLSConfig)
}

func TestHTTPWithTLSNoClientAuthNoServerTrust(t *testing.T) {
	t.Helper()
	clientTLSConfig := tekclient.TLSConfig{
		NoVerify: true,
	}
	serverTLSConfig := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  caSignedServerKeyPath,
		CertPath: caSignedServerCertPath,
	}
	testCli(t, clientTLSConfig, serverTLSConfig)
}

func TestHTTPWithTLSClientAuthSingleCA(t *testing.T) {
	t.Helper()
	// Use the same CA that signs both the server and client certificate
	clientTLSConfig := tekclient.TLSConfig{
		TrustedCertsPath: caCertPath,
		KeyPath:          caSignedClientKeyPath,
		CertPath:         caSignedClientCertPath,
	}
	serverTLSConfig := conf.TLSConfig{
		Enabled:         true,
		KeyPath:         caSignedServerKeyPath,
		CertPath:        caSignedServerCertPath,
		ClientCertsPath: caCertPath,
		ClientAuth:      "require-and-verify-client-cert",
	}
	testCli(t, clientTLSConfig, serverTLSConfig)
}

func TestHTTPWithTLSClientAuthSelfSignedCerts(t *testing.T) {
	// Using self-signed certs for server and client - no common CA
	clientTLSConfig := tekclient.TLSConfig{
		TrustedCertsPath: selfSignedServerCertPath,
		KeyPath:          selfSignedClientKeyPath,
		CertPath:         selfSignedClientCertPath,
	}
	serverTLSConfig := conf.TLSConfig{
		Enabled:         true,
		KeyPath:         selfSignedServerKeyPath,
		CertPath:        selfSignedServerCertPath,
		ClientCertsPath: selfSignedClientCertPath,
		ClientAuth:      "require-and-verify-client-cert",
	}
	testCli(t, clientTLSConfig, serverTLSConfig)
}

func TestHTTPWithTLSClientAuthFailNoClientCertProvided(t *testing.T) {
	t.Helper()
	// Clients certs required but not provided
	clientTLSConfig := tekclient.TLSConfig{
		TrustedCertsPath: selfSignedClientCertPath,
	}
	serverTLSConfig := conf.TLSConfig{
		Enabled:         true,
		KeyPath:         selfSignedServerKeyPath,
		CertPath:        selfSignedServerCertPath,
		ClientCertsPath: selfSignedClientCertPath,
		ClientAuth:      "require-and-verify-client-cert",
	}

	testCliFailure(t, clientTLSConfig, serverTLSConfig)
}

func TestHTTPWithTLSClientAuthFailUntrustedClientCertProvided(t *testing.T) {
	// Clients certs required but untrusted one provided
	clientTLSConfig := tekclient.TLSConfig{
		TrustedCertsPath: selfSignedServerCertPath,
		CertPath:         selfSignedClientCertPath2,
		KeyPath:          selfSignedClientKeyPath2,
	}
	serverTLSConfig := conf.TLSConfig{
		Enabled:         true,
		KeyPath:         selfSignedServerKeyPath,
		CertPath:        selfSignedServerCertPath,
		ClientCertsPath: selfSignedClientCertPath,
		ClientAuth:      "require-and-verify-client-cert",
	}
	testCliFailure(t, clientTLSConfig, serverTLSConfig)
}

func startServer(t *testing.T, serverAddress string, tlsConf conf.TLSConfig) (*api.HTTPAPIServer, *testQueryManager,
	*testCommandManager, *testWasmModuleManager) {
	queryMgr := &testQueryManager{}
	commandMgr := &testCommandManager{}
	moduleManager := &testWasmModuleManager{}
	server := api.NewHTTPAPIServer(serverAddress, "/tektite", queryMgr, commandMgr,
		parser.NewParser(nil), moduleManager, tlsConf)
	err := server.Activate()
	require.NoError(t, err)
	return server, queryMgr, commandMgr, moduleManager
}

func testCliFailure(t *testing.T, clientTLSConfig tekclient.TLSConfig, serverTLSConfig conf.TLSConfig) {
	serverAddress, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	server, _, commandMgr, _ := startServer(t, serverAddress, serverTLSConfig)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()

	cli := NewCli(serverAddress, clientTLSConfig)

	cli.SetExitOnError(false)
	err = cli.Start()
	require.NoError(t, err)
	defer func() {
		err := cli.Stop()
		require.NoError(t, err)
	}()

	var out strings.Builder

	stmt := `test_stream := (bridge from test_topic partitions = 23) -> (store stream)`

	err = execStatement(stmt, cli, &out)
	require.NoError(t, err)
	require.Equal(t, "", commandMgr.getTsl())
	actual := out.String()

	require.True(t, strings.HasPrefix(actual, "connection error:"))
}

func testCli(t *testing.T, clientTLSConfig tekclient.TLSConfig, serverTLSConfig conf.TLSConfig) {
	serverAddress, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	server, queryMgr, commandMgr, moduleManager := startServer(t, serverAddress, serverTLSConfig)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()

	cli := NewCli(serverAddress, clientTLSConfig)

	cli.SetExitOnError(false)
	err = cli.Start()
	require.NoError(t, err)
	defer func() {
		err := cli.Stop()
		require.NoError(t, err)
	}()

	var out strings.Builder

	stmt := "set max_line_width 200"
	out.WriteString(stmt)
	out.WriteRune('\n')
	err = execStatement(stmt, cli, &out)
	require.NoError(t, err)

	stmt = `test_stream := (bridge from test_topic partitions = 23) -> (store stream)`
	out.WriteString(stmt)
	out.WriteRune('\n')
	err = execStatement(stmt, cli, &out)
	require.NoError(t, err)
	require.Equal(t, stmt, commandMgr.getTsl())

	// Add some fake data
	batches := createBatches(t, 0, 5, 10)
	queryMgr.clearBatches()
	for i, batch := range batches {
		queryMgr.addBatch(batch, i == len(batches)-1)
	}

	stmt = "(scan all from test_stream)"
	out.WriteString(stmt)
	out.WriteRune('\n')
	err = execStatement(stmt, cli, &out)
	require.NoError(t, err)
	require.Equal(t, stmt, queryMgr.getDirectQueryState())

	stmt = "delete(test_stream)"
	out.WriteString(stmt)
	out.WriteRune('\n')
	err = execStatement(stmt, cli, &out)
	require.NoError(t, err)
	require.Equal(t, stmt, commandMgr.getTsl())

	stmt = `register_wasm("testdata/wasm/test_mod1.wasm")`
	out.WriteString(stmt)
	out.WriteRune('\n')
	err = execStatement(stmt, cli, &out)
	require.NoError(t, err)
	require.True(t, moduleManager.called.Load())

	actual := out.String()

	require.Equal(t, expectedOutput, actual)
}

func execStatement(stmt string, cli *Cli, sb *strings.Builder) error {
	ch, err := cli.ExecuteStatement(stmt)
	if err != nil {
		return err
	}
	for line := range ch {
		sb.WriteString(line)
		sb.WriteRune('\n')
	}
	return nil
}

var expectedOutput = `set max_line_width 200
OK
test_stream := (bridge from test_topic partitions = 23) -> (store stream)
OK
(scan all from test_stream)
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| f0                   | f1                         | f2                         | f3                         | f4                         | f5                         | f6                         |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| null                 | 0.123450                   | true                       | null                       | foobar-0                   | quux-0                     | null                       |
| 1000001              | 1.123450                   | null                       | 1123456789.9876            | foobar-1                   | null                       | 1970-01-01 00:33:20.001000 |
| 1000002              | null                       | true                       | 2123456789.9876            | null                       | quux-2                     | 1970-01-01 00:33:20.002000 |
| null                 | 3.123450                   | false                      | null                       | foobar-3                   | quux-3                     | null                       |
| 1000004              | 4.123450                   | null                       | 4123456789.9876            | foobar-4                   | null                       | 1970-01-01 00:33:20.004000 |
| 1000005              | null                       | false                      | 5123456789.9876            | null                       | quux-5                     | 1970-01-01 00:33:20.005000 |
| null                 | 6.123450                   | true                       | null                       | foobar-6                   | quux-6                     | null                       |
| 1000007              | 7.123450                   | null                       | 7123456789.9876            | foobar-7                   | null                       | 1970-01-01 00:33:20.007000 |
| 1000008              | null                       | true                       | 8123456789.9876            | null                       | quux-8                     | 1970-01-01 00:33:20.008000 |
| null                 | 9.123450                   | false                      | null                       | foobar-9                   | quux-9                     | null                       |
| 1000010              | 10.123450                  | null                       | 10123456789.9876           | foobar-10                  | null                       | 1970-01-01 00:33:20.010000 |
| 1000011              | null                       | false                      | 11123456789.9876           | null                       | quux-11                    | 1970-01-01 00:33:20.011000 |
| null                 | 12.123450                  | true                       | null                       | foobar-12                  | quux-12                    | null                       |
| 1000013              | 13.123450                  | null                       | 13123456789.9876           | foobar-13                  | null                       | 1970-01-01 00:33:20.013000 |
| 1000014              | null                       | true                       | 14123456789.9876           | null                       | quux-14                    | 1970-01-01 00:33:20.014000 |
| null                 | 15.123450                  | false                      | null                       | foobar-15                  | quux-15                    | null                       |
| 1000016              | 16.123450                  | null                       | 16123456789.9876           | foobar-16                  | null                       | 1970-01-01 00:33:20.016000 |
| 1000017              | null                       | false                      | 17123456789.9876           | null                       | quux-17                    | 1970-01-01 00:33:20.017000 |
| null                 | 18.123450                  | true                       | null                       | foobar-18                  | quux-18                    | null                       |
| 1000019              | 19.123450                  | null                       | 19123456789.9876           | foobar-19                  | null                       | 1970-01-01 00:33:20.019000 |
| 1000020              | null                       | true                       | 20123456789.9876           | null                       | quux-20                    | 1970-01-01 00:33:20.020000 |
| null                 | 21.123450                  | false                      | null                       | foobar-21                  | quux-21                    | null                       |
| 1000022              | 22.123450                  | null                       | 22123456789.9876           | foobar-22                  | null                       | 1970-01-01 00:33:20.022000 |
| 1000023              | null                       | false                      | 23123456789.9876           | null                       | quux-23                    | 1970-01-01 00:33:20.023000 |
| null                 | 24.123450                  | true                       | null                       | foobar-24                  | quux-24                    | null                       |
| 1000025              | 25.123450                  | null                       | 25123456789.9876           | foobar-25                  | null                       | 1970-01-01 00:33:20.025000 |
| 1000026              | null                       | true                       | 26123456789.9876           | null                       | quux-26                    | 1970-01-01 00:33:20.026000 |
| null                 | 27.123450                  | false                      | null                       | foobar-27                  | quux-27                    | null                       |
| 1000028              | 28.123450                  | null                       | 28123456789.9876           | foobar-28                  | null                       | 1970-01-01 00:33:20.028000 |
| 1000029              | null                       | false                      | 29123456789.9876           | null                       | quux-29                    | 1970-01-01 00:33:20.029000 |
| null                 | 30.123450                  | true                       | null                       | foobar-30                  | quux-30                    | null                       |
| 1000031              | 31.123450                  | null                       | 31123456789.9876           | foobar-31                  | null                       | 1970-01-01 00:33:20.031000 |
| 1000032              | null                       | true                       | 32123456789.9876           | null                       | quux-32                    | 1970-01-01 00:33:20.032000 |
| null                 | 33.123450                  | false                      | null                       | foobar-33                  | quux-33                    | null                       |
| 1000034              | 34.123450                  | null                       | 34123456789.9876           | foobar-34                  | null                       | 1970-01-01 00:33:20.034000 |
| 1000035              | null                       | false                      | 35123456789.9876           | null                       | quux-35                    | 1970-01-01 00:33:20.035000 |
| null                 | 36.123450                  | true                       | null                       | foobar-36                  | quux-36                    | null                       |
| 1000037              | 37.123450                  | null                       | 37123456789.9876           | foobar-37                  | null                       | 1970-01-01 00:33:20.037000 |
| 1000038              | null                       | true                       | 38123456789.9876           | null                       | quux-38                    | 1970-01-01 00:33:20.038000 |
| null                 | 39.123450                  | false                      | null                       | foobar-39                  | quux-39                    | null                       |
| 1000040              | 40.123450                  | null                       | 40123456789.9876           | foobar-40                  | null                       | 1970-01-01 00:33:20.040000 |
| 1000041              | null                       | false                      | 41123456789.9876           | null                       | quux-41                    | 1970-01-01 00:33:20.041000 |
| null                 | 42.123450                  | true                       | null                       | foobar-42                  | quux-42                    | null                       |
| 1000043              | 43.123450                  | null                       | 43123456789.9876           | foobar-43                  | null                       | 1970-01-01 00:33:20.043000 |
| 1000044              | null                       | true                       | 44123456789.9876           | null                       | quux-44                    | 1970-01-01 00:33:20.044000 |
| null                 | 45.123450                  | false                      | null                       | foobar-45                  | quux-45                    | null                       |
| 1000046              | 46.123450                  | null                       | 46123456789.9876           | foobar-46                  | null                       | 1970-01-01 00:33:20.046000 |
| 1000047              | null                       | false                      | 47123456789.9876           | null                       | quux-47                    | 1970-01-01 00:33:20.047000 |
| null                 | 48.123450                  | true                       | null                       | foobar-48                  | quux-48                    | null                       |
| 1000049              | 49.123450                  | null                       | 49123456789.9876           | foobar-49                  | null                       | 1970-01-01 00:33:20.049000 |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
50 rows returned
delete(test_stream)
OK
register_wasm("testdata/wasm/test_mod1.wasm")
OK
`

func TestClientNotExistentKeyFile(t *testing.T) {
	tlsConfig := tekclient.TLSConfig{
		KeyPath:  "nothing/here/client.key",
		CertPath: caSignedClientCertPath,
	}
	cl := NewCli("localhost:6584", tlsConfig)
	err := cl.Start()
	require.Error(t, err)
	require.Equal(t, "open nothing/here/client.key: no such file or directory", err.Error())
}

func TestClientNotExistentCertFile(t *testing.T) {
	tlsConfig := tekclient.TLSConfig{
		KeyPath:  caSignedClientKeyPath,
		CertPath: "nothing/here/client.crt",
	}
	cl := NewCli("localhost:6584", tlsConfig)
	err := cl.Start()
	require.Error(t, err)
	require.Equal(t, "open nothing/here/client.crt: no such file or directory", err.Error())
}

type testQueryManager struct {
	lock      sync.Mutex
	queryName string
	args      []any

	batches []batchInfo
	numLast int

	paramNames []string
	paramTypes []types.ColumnType

	receivedParamNames []string
	receivedParamTypes []types.ColumnType

	directQueryTsl string
}

func (t *testQueryManager) GetLastCompletedVersion() int {
	return 0
}

func (t *testQueryManager) GetLastFlushedVersion() int {
	return 0
}

func (t *testQueryManager) ExecuteQueryDirect(tsl string, _ parser.QueryDesc, outputFunc func(last bool, numLastBatches int, batch *evbatch.Batch) error) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.directQueryTsl = tsl
	t.sendBatches(outputFunc)
	return nil
}

func (t *testQueryManager) getDirectQueryState() string {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.directQueryTsl
}

func (t *testQueryManager) Activate() {
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
	t.paramNames = paramNames
	t.paramTypes = paramTypes
}

func (t *testQueryManager) getExecState() (string, []any) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.queryName, t.args
}

func (t *testQueryManager) getPrepareState() ([]string, []types.ColumnType) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.receivedParamNames, t.receivedParamTypes
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
	return nil
}

type testCommandManager struct {
	lock sync.Mutex
	tsl  string
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

func (t *testCommandManager) getTsl() string {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.tsl
}

func createBatches(t require.TestingT, start int, numRowsPerBatch int, numBatches int) []*evbatch.Batch {
	var batches []*evbatch.Batch
	decType := &types.DecimalType{
		Precision: 28,
		Scale:     4,
	}
	columnTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType,
		types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp}
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
		schema := evbatch.NewEventSchema([]string{"f0", "f1", "f2", "f3", "f4", "f5", "f6"}, columnTypes)
		batch := evbatch.NewBatchFromBuilders(schema, colBuilders...)
		batches = append(batches, batch)
	}
	return batches
}

func (t *testCommandManager) SetCommandSignaller(command.Signaller) {
}

func (t *testCommandManager) Start() error {
	return nil
}

func (t *testCommandManager) Activate() error {
	return nil
}

func (t *testCommandManager) Stop() error {
	return nil
}

type testWasmModuleManager struct {
	called atomic.Bool
}

func (t *testWasmModuleManager) RegisterModule(wasm.ModuleMetadata, []byte) error {
	t.called.Store(true)
	return nil
}

func (t *testWasmModuleManager) UnregisterModule(string) error {
	//TODO implement me
	panic("implement me")
}
