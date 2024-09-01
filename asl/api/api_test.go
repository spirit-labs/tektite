package api

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/asl/remoting"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/cmdmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/types"
	"github.com/spirit-labs/tektite/wasm"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
)

const (
	serverKeyPath  = "testdata/serverkey.pem"
	serverCertPath = "testdata/servercert.pem"
)

func init() {
	common.EnableTestPorts()
}

func TestExecuteCreateStream(t *testing.T) {
	stmt := `test_stream :=
		(bridge from test_topic
			partitions = 23
		) -> (store stream)`
	testExecuteStatement(t, stmt)
}

func TestExecuteDeleteStream(t *testing.T) {
	stmt := `delete(test_stream)`
	testExecuteStatement(t, stmt)
}

func testExecuteStatement(t *testing.T, statement string) {
	server, _, commandMgr, _ := startServer(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)
	defer client.CloseIdleConnections()
	uri := fmt.Sprintf("https://%s/tektite/statement", server.ListenAddress())
	resp := sendPostRequest(t, client, uri, statement)
	defer closeRespBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, statement, commandMgr.getCommand())
}

func TestStatementParseError(t *testing.T) {
	server, _, _, _ := startServer(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)
	defer client.CloseIdleConnections()

	com := `test_stream ::=== (bridge from test_topic partitions = 23) -> (store stream)`

	uri := fmt.Sprintf("https://%s/tektite/statement", server.ListenAddress())
	resp := sendPostRequest(t, client, uri, com)
	defer closeRespBody(t, resp)
	require.Equal(t, 400, resp.StatusCode)
	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyString := string(bodyBytes)
	require.Equal(t,
		`TEK1001 - invalid statement (line 1 column 13):
test_stream ::=== (bridge from test_topic partitions = 23) -> (store stream)
            ^
`, bodyString)
	require.NoError(t, err)
}

func TestExecutePreparedStatementSingleBatch(t *testing.T) {
	testExecutePreparedStatementWithArgsAllTypes(t, 1, 1, 10)
}

func TestExecutePreparedStatementMultipleBatches(t *testing.T) {
	testExecutePreparedStatementWithArgsAllTypes(t, 10, 10, 10)
}

func TestExecuteDirectQuerySingleBatch(t *testing.T) {
	testExecuteDirectQuery(t, 1, 1, 10)
}

func TestExecuteDirectQueryMultipleBatches(t *testing.T) {
	testExecuteDirectQuery(t, 10, 10, 10)
}

func testExecutePreparedStatementWithArgsAllTypes(t *testing.T, numPartitions int, numBatchesPerPartition int, numRowsPerBatch int) {
	decType := &types.DecimalType{
		Precision: 28,
		Scale:     4,
	}
	paramTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType,
		types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	paramNames := []string{"$p1:int", "$p2:float", "$p3:bool", "$p4:decimal(28, 4)", "$p5:string", "$p6:bytes", "$p7:timestamp"}
	args := []any{23, 1.23, true, "1234.4321", "aardvark", base64.StdEncoding.EncodeToString([]byte("zebras")), 987654}
	decNum, err := decimal128.FromString("1234.4321", int32(decType.Precision), int32(decType.Scale))
	require.NoError(t, err)
	decVal := types.Decimal{
		Num:       decNum,
		Precision: decType.Precision,
		Scale:     decType.Scale,
	}
	expectedArgs := []any{int64(23), float64(1.23), true, decVal, "aardvark", []byte("zebras"), types.NewTimestamp(987654)}
	testExecutePreparedStatement(t, numPartitions, numBatchesPerPartition, numRowsPerBatch, paramNames,
		paramTypes, args, expectedArgs)
}

func TestExecutePreparedStatementConvertIntArg(t *testing.T) {
	testExecutePreparedStatementArg(t, "$p1:int", types.ColumnTypeInt, int64(123456), int64(123456))
	testExecutePreparedStatementArg(t, "$p1:int", types.ColumnTypeInt, int64(-123456), int64(-123456))
	testExecutePreparedStatementArg(t, "$p1:int", types.ColumnTypeInt, float64(123456), int64(123456))
	testExecutePreparedStatementArg(t, "$p1:int", types.ColumnTypeInt, float64(-123456), int64(-123456))
	testExecutePreparedStatementArg(t, "$p1:int", types.ColumnTypeInt, "123456", int64(123456))
	testExecutePreparedStatementArg(t, "$p1:int", types.ColumnTypeInt, "-123456", int64(-123456))
}

func TestExecutePreparedStatementConvertFloatArg(t *testing.T) {
	testExecutePreparedStatementArg(t, "$p1:float", types.ColumnTypeFloat, float64(1234.56), float64(1234.56))
	testExecutePreparedStatementArg(t, "$p1:float", types.ColumnTypeFloat, float64(-1234.56), float64(-1234.56))
	testExecutePreparedStatementArg(t, "$p1:float", types.ColumnTypeFloat, int64(123456), float64(123456))
	testExecutePreparedStatementArg(t, "$p1:float", types.ColumnTypeFloat, int64(-123456), float64(-123456))
	testExecutePreparedStatementArg(t, "$p1:float", types.ColumnTypeFloat, "1234.56", float64(1234.56))
	testExecutePreparedStatementArg(t, "$p1:float", types.ColumnTypeFloat, "-1234.56", float64(-1234.56))
	testExecutePreparedStatementArg(t, "$p1:float", types.ColumnTypeFloat, "1.23e100", float64(1.23e100))
	testExecutePreparedStatementArg(t, "$p1:float", types.ColumnTypeFloat, "-1.23e100", float64(-1.23e100))
}

func TestExecutePreparedStatementConvertBoolArg(t *testing.T) {
	testExecutePreparedStatementArg(t, "$p1:bool", types.ColumnTypeBool, false, false)
	testExecutePreparedStatementArg(t, "$p1:bool", types.ColumnTypeBool, true, true)
	testExecutePreparedStatementArg(t, "$p1:bool", types.ColumnTypeBool, "false", false)
	testExecutePreparedStatementArg(t, "$p1:bool", types.ColumnTypeBool, "true", true)
	testExecutePreparedStatementArg(t, "$p1:bool", types.ColumnTypeBool, "FALSE", false)
	testExecutePreparedStatementArg(t, "$p1:bool", types.ColumnTypeBool, "TRUE", true)
}

func TestExecutePreparedStatementConvertDecimalArg(t *testing.T) {
	decType := &types.DecimalType{
		Precision: 23,
		Scale:     7,
	}
	testExecutePreparedStatementArg(t, "$p1:decimal(23, 7)", decType, "1234.4321", createDecimal("1234.4321", 23, 7))
	testExecutePreparedStatementArg(t, "$p1:decimal(23, 7)", decType, "-1234.4321", createDecimal("-1234.4321", 23, 7))
	testExecutePreparedStatementArg(t, "$p1:decimal(23, 7)", decType, int64(123456), createDecimal("123456", 23, 7))
	testExecutePreparedStatementArg(t, "$p1:decimal(23, 7)", decType, float64(1234.56), createDecimal("1234.56", 23, 7))
}

func TestExecutePreparedStatementConvertStringArg(t *testing.T) {
	testExecutePreparedStatementArg(t, "$p1:string", types.ColumnTypeString, "foo", "foo")
	testExecutePreparedStatementArg(t, "$p1:string", types.ColumnTypeString, int64(123456), "123456")
	testExecutePreparedStatementArg(t, "$p1:string", types.ColumnTypeString, int64(-123456), "-123456")
	testExecutePreparedStatementArg(t, "$p1:string", types.ColumnTypeString, float64(1234.56), "1234.56")
	testExecutePreparedStatementArg(t, "$p1:string", types.ColumnTypeString, float64(-1234.56), "-1234.56")
	testExecutePreparedStatementArg(t, "$p1:string", types.ColumnTypeString, float64(1.23e100), "1.23e+100")
	testExecutePreparedStatementArg(t, "$p1:string", types.ColumnTypeString, float64(-1.23e100), "-1.23e+100")
	testExecutePreparedStatementArg(t, "$p1:string", types.ColumnTypeString, false, "false")
	testExecutePreparedStatementArg(t, "$p1:string", types.ColumnTypeString, true, "true")
}

func TestExecutePreparedStatementConvertBytesArg(t *testing.T) {
	testExecutePreparedStatementArg(t, "$p1:bytes", types.ColumnTypeBytes, base64.StdEncoding.EncodeToString([]byte("foo")), []byte("foo"))
}

func TestExecutePreparedStatementConvertTimestampArg(t *testing.T) {
	testExecutePreparedStatementArg(t, "$p1:timestamp", types.ColumnTypeTimestamp, int64(123456), types.NewTimestamp(int64(123456)))
}

func TestExecutePreparedStatementNullArg(t *testing.T) {
	testExecutePreparedStatementArg(t, "$p1:int", types.ColumnTypeInt, nil, nil)
}

func testExecutePreparedStatementArg(t *testing.T, paramName string, paramType types.ColumnType, inArg any, expectedArg any) {

	server, queryMgr, _, _ := startServer(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)
	defer client.CloseIdleConnections()

	queryMgr.setParamMetaData([]string{paramName}, []types.ColumnType{paramType})

	uri := fmt.Sprintf("https://%s/tektite/exec", server.ListenAddress())

	invocation := &PreparedStatementInvocation{
		QueryName: "test_query",
		Args:      []any{inArg},
	}
	buff, err := json.Marshal(&invocation)
	require.NoError(t, err)

	resp := sendPostRequest(t, client, uri, string(buff))

	defer closeRespBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	queryName, rargs := queryMgr.getExecState()
	require.Equal(t, "test_query", queryName)

	expectedArgs := []any{expectedArg}

	require.Equal(t, expectedArgs, rargs)
}

func TestWasmRegister(t *testing.T) {

	server, _, _, moduleManager := startServer(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)
	defer client.CloseIdleConnections()

	uri := fmt.Sprintf("https://%s/tektite/wasm-register", server.ListenAddress())

	moduleData := []byte("quwhdquwhdi")
	encodedData := base64.StdEncoding.EncodeToString(moduleData)

	registration := &WasmRegistration{
		MetaData: wasm.ModuleMetadata{
			ModuleName: "test_mod",
			FunctionsMetadata: map[string]expr.FunctionMetadata{
				"func1": {
					ParamTypes: []types.ColumnType{types.ColumnTypeInt},
					ReturnType: types.ColumnTypeString,
				},
			},
		},
		ModuleData: encodedData,
	}

	buff, err := json.Marshal(registration)
	require.NoError(t, err)

	resp := sendPostRequest(t, client, uri, string(buff))

	defer closeRespBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	meta, modBytes := moduleManager.getRegistrationInfo()
	require.Equal(t, registration.MetaData, meta)
	require.Equal(t, moduleData, modBytes)
}

func TestWasmUnregister(t *testing.T) {

	server, _, _, moduleManager := startServer(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)
	defer client.CloseIdleConnections()

	uri := fmt.Sprintf("https://%s/tektite/wasm-unregister", server.ListenAddress())

	resp := sendPostRequest(t, client, uri, "test_mod23")

	defer closeRespBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	modName := moduleManager.getUnregistrationInfo()
	require.Equal(t, "test_mod23", modName)
}

func sendPostRequest(t *testing.T, client *http.Client, uri string, body string) *http.Response {
	req, err := http.NewRequest(http.MethodPost, uri, bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := client.Do(req)
	require.NoError(t, err)
	return resp
}

func testExecuteDirectQuery(t *testing.T, numPartitions int, numBatchesPerPartition int, numRowsPerBatch int) {
	server, queryMgr, _, _ := startServer(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)
	defer client.CloseIdleConnections()

	var partitionBatches [][]*evbatch.Batch
	cnt := 0
	for i := 0; i < numPartitions; i++ {
		batches := createBatches(t, cnt, numRowsPerBatch, numBatchesPerPartition)
		partitionBatches = append(partitionBatches, batches)
		cnt += numRowsPerBatch * numBatchesPerPartition
		for j, batch := range batches {
			queryMgr.addBatch(batch, j == len(batches)-1)
		}
	}

	query := "(scan all from some_table)"
	uri := fmt.Sprintf("https://%s/tektite/query", server.ListenAddress())
	resp := sendPostRequest(t, client, uri, query)

	defer closeRespBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyString := string(bodyBytes)

	expected := createExpectedRows(t, numPartitions*numBatchesPerPartition*numRowsPerBatch)
	require.Equal(t, expected, bodyString)

	require.Equal(t, "(scan all from some_table)", queryMgr.getDirectQueryTsl())
}

func testExecutePreparedStatement(t *testing.T, numPartitions int, numBatchesPerPartition int, numRowsPerBatch int,
	paramNames []string, paramTypes []types.ColumnType, args []any, expectedArgs []any) {
	server, queryMgr, _, _ := startServer(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)
	defer client.CloseIdleConnections()

	var partitionBatches [][]*evbatch.Batch
	cnt := 0
	for i := 0; i < numPartitions; i++ {
		batches := createBatches(t, cnt, numRowsPerBatch, numBatchesPerPartition)
		partitionBatches = append(partitionBatches, batches)
		cnt += numRowsPerBatch * numBatchesPerPartition
		for j, batch := range batches {
			queryMgr.addBatch(batch, j == len(batches)-1)
		}
	}

	queryMgr.setParamMetaData(paramNames, paramTypes)

	uri := fmt.Sprintf("https://%s/tektite/exec", server.ListenAddress())

	invocation := &PreparedStatementInvocation{
		QueryName: "test_query",
		Args:      args,
	}
	buff, err := json.Marshal(&invocation)
	require.NoError(t, err)

	resp := sendPostRequest(t, client, uri, string(buff))

	defer closeRespBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyString := string(bodyBytes)

	expected := createExpectedRows(t, numPartitions*numBatchesPerPartition*numRowsPerBatch)
	require.Equal(t, expected, bodyString)

	queryName, rargs := queryMgr.getExecState()
	require.Equal(t, "test_query", queryName)
	require.Equal(t, expectedArgs, rargs)
}

func TestExecutePreparedStatementWithColHeaders(t *testing.T) {
	server, queryMgr, _, _ := startServer(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)
	defer client.CloseIdleConnections()

	batches := createBatches(t, 0, 10, 1)
	queryMgr.addBatch(batches[0], true)

	queryMgr.setParamMetaData([]string{}, []types.ColumnType{})

	invocation := &PreparedStatementInvocation{
		QueryName: "test_query",
	}
	buff, err := json.Marshal(&invocation)
	require.NoError(t, err)

	uri := fmt.Sprintf("https://%s/tektite/exec?col_headers=true", server.ListenAddress())
	resp := sendPostRequest(t, client, uri, string(buff))

	defer closeRespBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyString := string(bodyBytes)

	expected := `["f0","f1","f2","f3","f4","f5","f6"]
["int","float","bool","decimal(28,4)","string","bytes","timestamp"]`
	expected += "\n" + createExpectedRows(t, 10)
	require.Equal(t, expected, bodyString)
}

func TestExecuteDirectQueryWithColHeaders(t *testing.T) {
	server, queryMgr, _, _ := startServer(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)
	defer client.CloseIdleConnections()

	batches := createBatches(t, 0, 10, 1)
	queryMgr.addBatch(batches[0], true)

	uri := fmt.Sprintf("https://%s/tektite/query?col_headers=true", server.ListenAddress())
	query := "(scan all from foo)"
	resp := sendPostRequest(t, client, uri, query)

	defer closeRespBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyString := string(bodyBytes)

	expected := `["f0","f1","f2","f3","f4","f5","f6"]
["int","float","bool","decimal(28,4)","string","bytes","timestamp"]`
	expected += "\n" + createExpectedRows(t, 10)
	require.Equal(t, expected, bodyString)
}

func TestExecutePreparedStatementWithArrowEncoding(t *testing.T) {
	server, queryMgr, _, _ := startServer(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)
	defer client.CloseIdleConnections()

	batches := createBatches(t, 0, 10, 1)
	queryMgr.addBatch(batches[0], true)

	queryMgr.setParamMetaData([]string{}, []types.ColumnType{})

	invocation := &PreparedStatementInvocation{
		QueryName: "test_query",
	}
	buff, err := json.Marshal(&invocation)
	require.NoError(t, err)

	uri := fmt.Sprintf("https://%s/tektite/exec?col_headers=true", server.ListenAddress())
	req, err := http.NewRequest(http.MethodPost, uri, bytes.NewBuffer(buff))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Accept", TektiteArrowMimeType)
	resp, err := client.Do(req)
	require.NoError(t, err)

	defer closeRespBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	receivedBatches := decodeReceivedBatches(bodyBytes)
	require.Equal(t, 1, len(receivedBatches))
	require.True(t, batches[0].Equal(receivedBatches[0]))
}

func TestExecuteDirectQueryWithArrowEncoding(t *testing.T) {
	server, queryMgr, _, _ := startServer(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)
	defer client.CloseIdleConnections()

	batches := createBatches(t, 0, 10, 1)
	queryMgr.addBatch(batches[0], true)

	uri := fmt.Sprintf("https://%s/tektite/query?col_headers=true", server.ListenAddress())
	query := "(scan all from foo)"
	req, err := http.NewRequest(http.MethodPost, uri, bytes.NewBufferString(query))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Accept", TektiteArrowMimeType)
	resp, err := client.Do(req)
	require.NoError(t, err)

	defer closeRespBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	receivedBatches := decodeReceivedBatches(bodyBytes)
	require.Equal(t, 1, len(receivedBatches))
	require.True(t, batches[0].Equal(receivedBatches[0]))
}

func decodeReceivedBatches(buff []byte) []*evbatch.Batch {
	schema, offset := DecodeArrowSchema(buff[8:]) // first 8 bytes is length of schema block
	buff = buff[8+offset:]
	var batches []*evbatch.Batch
	for len(buff) > 0 {
		bl, _ := encoding.ReadUint64FromBufferLE(buff, 0)
		be := 8 + int(bl)
		batch := DecodeArrowBatch(schema, buff[8:be])
		batches = append(batches, batch)
		buff = buff[be:]
	}
	return batches
}

func TestHttp2Only(t *testing.T) {
	testErrorResponse(t, "/tektite/query", "",
		"the tektite HTTP API supports HTTP2 only\n", http.StatusHTTPVersionNotSupported, false)
}

func TestWrongMethod(t *testing.T) {
	server, _, _, _ := startServer(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)
	defer client.CloseIdleConnections()

	uri := fmt.Sprintf("https://%s/tektite/query", server.ListenAddress())

	resp, err := client.Get(uri)
	require.NoError(t, err)
	defer closeRespBody(t, resp)

	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestWrongPath(t *testing.T) {
	testErrorResponse(t, "/some/other/path?stmt=foo&schema=test_schema", "",
		"404 page not found\n", http.StatusNotFound, true)
}

func TestExecutePreparedStatementInvalidArgs(t *testing.T) {
	testExecutePreparedStatementInvalidArg(t, types.ColumnTypeInt, "xyz", "TEK1003 - argument 0 (xyz) of type string cannot be converted to int\n")
	testExecutePreparedStatementInvalidArg(t, types.ColumnTypeInt, false, "TEK1003 - argument 0 (false) of type bool cannot be converted to int\n")

	testExecutePreparedStatementInvalidArg(t, types.ColumnTypeFloat, "xyz", "TEK1003 - argument 0 (xyz) of type string cannot be converted to float\n")
	testExecutePreparedStatementInvalidArg(t, types.ColumnTypeFloat, false, "TEK1003 - argument 0 (false) of type bool cannot be converted to float\n")

	testExecutePreparedStatementInvalidArg(t, types.ColumnTypeBool, "xyz", "TEK1003 - argument 0 (xyz) of type string cannot be converted to bool\n")
	testExecutePreparedStatementInvalidArg(t, types.ColumnTypeBool, float64(1.23), "TEK1003 - argument 0 (1.23) of type float64 cannot be converted to bool\n")

	decType := &types.DecimalType{
		Precision: 23,
		Scale:     5,
	}
	testExecutePreparedStatementInvalidArg(t, decType, "xyz", "TEK1003 - argument 0 (xyz) of type string cannot be converted to decimal(23,5)\n")
	testExecutePreparedStatementInvalidArg(t, decType, false, "TEK1003 - argument 0 (false) of type bool cannot be converted to decimal(23,5)\n")

	testExecutePreparedStatementInvalidArg(t, types.ColumnTypeBytes, float64(12345), "TEK1003 - argument 0 (12345) of type float64 cannot be converted to bytes\n")
	testExecutePreparedStatementInvalidArg(t, types.ColumnTypeBytes, false, "TEK1003 - argument 0 (false) of type bool cannot be converted to bytes\n")
}

func testExecutePreparedStatementInvalidArg(t *testing.T, paramType types.ColumnType, arg any, errMsg string) {

	server, queryMgr, _, _ := startServer(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)
	defer client.CloseIdleConnections()

	queryMgr.setParamMetaData([]string{"$p1"}, []types.ColumnType{paramType})

	uri := fmt.Sprintf("https://%s/tektite/exec", server.ListenAddress())

	invocation := &PreparedStatementInvocation{
		QueryName: "test_query",
		Args:      []any{arg},
	}
	buff, err := json.Marshal(invocation)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, uri, bytes.NewBuffer(buff))
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)

	defer closeRespBody(t, resp)
	require.Equal(t, 400, resp.StatusCode)
	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyString := string(bodyBytes)
	require.Equal(t, errMsg, bodyString)
	require.NoError(t, err)
}

func TestUnknownQuery(t *testing.T) {
	invocation := &PreparedStatementInvocation{
		QueryName: "unknown_query",
	}
	body, err := json.Marshal(invocation)
	require.NoError(t, err)
	testErrorResponse(t, "/tektite/exec", string(body), "TEK1003 - unknown prepared query 'unknown_query'\n", http.StatusBadRequest, true)
}

func testErrorResponse(t *testing.T, path string, body string, errorMsg string, statusCode int, http2 bool) {
	server, _, _, _ := startServer(t)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, http2)
	defer client.CloseIdleConnections()

	uri := fmt.Sprintf("https://%s%s", server.ListenAddress(), path)

	req, err := http.NewRequest(http.MethodPost, uri, bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer closeRespBody(t, resp)

	require.Equal(t, statusCode, resp.StatusCode)
	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyString := string(bodyBytes)
	require.Equal(t, errorMsg, bodyString)
	require.NoError(t, err)
}

func startServer(t *testing.T) (*HTTPAPIServer, *testQueryManager, *testCommandManager, *testWasmModuleManager) {
	t.Helper()
	tlsConf := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  serverKeyPath,
		CertPath: serverCertPath,
	}
	queryMgr := &testQueryManager{}
	commandMgr := &testCommandManager{}
	moduleManager := &testWasmModuleManager{}

	address, err := common.AddressWithPort("localhost")
	require.NoError(t, err)

	server := NewHTTPAPIServer(0, []string{address}, "/tektite", queryMgr,
		commandMgr, parser.NewParser(nil), moduleManager, nil, tlsConf, false, 0)
	err = server.Activate()
	require.NoError(t, err)
	return server, queryMgr, commandMgr, moduleManager
}

func createClient(t *testing.T, enableHTTP2 bool) *http.Client {
	t.Helper()
	// Create config for the test client
	caCert, err := os.ReadFile(serverCertPath)
	require.NoError(t, err)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: true, //nolint:gosec
	}
	client := &http.Client{}
	if enableHTTP2 {
		client.Transport = &http2.Transport{
			TLSClientConfig: tlsConfig,
		}
	} else {
		client.Transport = &http.Transport{
			TLSNextProto:    make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
			TLSClientConfig: tlsConfig,
		}
	}
	return client
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

func createExpectedRows(t *testing.T, rows int) string {
	sb := strings.Builder{}
	for i := 0; i < rows; i++ {
		row := make([]any, 7)
		if i%3 != 0 {
			row[0] = int64(1000000 + i)
		}
		if (i+1)%3 != 0 {
			row[1] = float64(i) + 0.12345
		}
		if (i+2)%3 != 0 {
			row[2] = i%2 == 0
		}
		if (i+3)%3 != 0 {
			if i == 0 {
				row[3] = "123456789.9876"
			} else {
				row[3] = fmt.Sprintf("%d123456789.9876", i)
			}
		}
		if (i+4)%3 != 0 {
			row[4] = fmt.Sprintf("foobar-%d", i)
		}
		if (i+5)%3 != 0 {
			row[5] = fmt.Sprintf("quux-%d", i)
		}
		if (i+6)%3 != 0 {
			row[6] = int64(2000000 + i)
		}

		b, err := json.Marshal(row)
		require.NoError(t, err)
		sb.Write(b)
		sb.WriteRune('\n')
	}
	return sb.String()
}

func closeRespBody(t *testing.T, resp *http.Response) {
	t.Helper()
	if resp != nil {
		err := resp.Body.Close()
		require.NoError(t, err)
	}
}

type testQueryManager struct {
	lock      sync.Mutex
	queryName string
	args      []any

	batches []batchInfo
	numLast int

	paramSchema *evbatch.EventSchema

	receiverPrepareQueryDesc *parser.PrepareQueryDesc
	directQueryTsl           string
}

func (t *testQueryManager) GetLastCompletedVersion() int {
	return 0
}

func (t *testQueryManager) GetLastFlushedVersion() int {
	return 0
}

func (t *testQueryManager) Activate() {
}

func (t *testQueryManager) SetClusterMessageHandlers(remoting.Server, *remoting.TeeBlockingClusterMessageHandler) {
}

func (t *testQueryManager) Start() error {
	return nil
}

func (t *testQueryManager) Stop() error {
	return nil
}

func (t *testQueryManager) ExecuteQueryDirect(tsl string, _ parser.QueryDesc, outputFunc func(last bool, numLastBatches int, batch *evbatch.Batch) error) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.directQueryTsl = tsl
	t.sendBatches(outputFunc)
	return nil
}

func (t *testQueryManager) getDirectQueryTsl() string {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.directQueryTsl
}

type batchInfo struct {
	batch *evbatch.Batch
	last  bool
}

func (t *testQueryManager) addBatch(batch *evbatch.Batch, last bool) {
	t.batches = append(t.batches, batchInfo{
		batch: batch,
		last:  last,
	})
	if last {
		t.numLast++
	}
}

func (t *testQueryManager) setParamMetaData(paramNames []string, paramTypes []types.ColumnType) {
	t.paramSchema = evbatch.NewEventSchema(paramNames, paramTypes)
}

func (t *testQueryManager) getExecState() (string, []any) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.queryName, t.args
}

func (t *testQueryManager) getPrepareState() *parser.PrepareQueryDesc {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.receiverPrepareQueryDesc
}

func (t *testQueryManager) DeleteQuery(deleteQuery parser.DeleteQueryDesc) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	return nil
}

func (t *testQueryManager) PrepareQuery(prepareQuery parser.PrepareQueryDesc) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.receiverPrepareQueryDesc = &prepareQuery
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
		if len(t.batches) == 0 {
			err := outputFunc(true, 1, nil)
			if err != nil {
				panic(err)
			}
			return
		}
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

func (t *testQueryManager) ExecuteRemoteQuery(*clustermsgs.QueryMessage) error {
	return nil
}

func (t *testQueryManager) ReceiveQueryResult(*clustermsgs.QueryResponse) {
}

func (t *testQueryManager) GetPreparedQueryParamSchema(string) (*evbatch.EventSchema, bool) {
	if t.paramSchema == nil {
		return nil, false
	}
	return t.paramSchema, true
}

type testCommandManager struct {
	lock    sync.Mutex
	command string
}

func (t *testCommandManager) HandleClusterState(clustmgr.ClusterState) error {
	return nil
}

func (t *testCommandManager) SetCommandSignaller(cmdmgr.Signaller) {
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

func (t *testCommandManager) ExecuteCommand(command string) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.command = command
	return nil
}

func (t *testCommandManager) getCommand() string {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.command
}

type testWasmModuleManager struct {
	lock        sync.Mutex
	metaData    wasm.ModuleMetadata
	moduleBytes []byte
	unregName   string
}

func (t *testWasmModuleManager) RegisterModule(metaData wasm.ModuleMetadata, moduleBytes []byte) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.metaData = metaData
	t.moduleBytes = moduleBytes
	return nil
}

func (t *testWasmModuleManager) getRegistrationInfo() (wasm.ModuleMetadata, []byte) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.metaData, t.moduleBytes
}

func (t *testWasmModuleManager) UnregisterModule(name string) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.unregName = name
	return nil
}

func (t *testWasmModuleManager) getUnregistrationInfo() string {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.unregName
}

func createDecimal(str string, prec int, scale int) types.Decimal {
	num, err := decimal128.FromString(str, int32(prec), int32(scale))
	if err != nil {
		panic(err)
	}
	return types.Decimal{
		Num:       num,
		Precision: prec,
		Scale:     scale,
	}
}
