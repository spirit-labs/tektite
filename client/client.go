package client

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/spirit-labs/tektite/asl/api"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/asl/errwrap"
	common3 "github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/types"
	"golang.org/x/net/http2"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	queryRetryTimeout = 30 * time.Second
	queryRetryDelay   = 50 * time.Millisecond
)

func NewClient(serverAddress string, tlsConfig TLSConfig) (Client, error) {
	tlsConf, err := tlsConfig.ToGoTlsConfig()
	if err != nil {
		return nil, err
	}
	httpCl := &http.Client{}
	httpCl.Transport = &http2.Transport{
		TLSClientConfig: tlsConf,
	}
	return &client{
		serverAddress:     serverAddress,
		statementURL:      fmt.Sprintf("https://%s/tektite/statement", serverAddress),
		queryURL:          fmt.Sprintf("https://%s/tektite/query?col_headers=true", serverAddress),
		execPSURL:         fmt.Sprintf("https://%s/tektite/exec?col_headers=true", serverAddress),
		registerWasmURL:   fmt.Sprintf("https://%s/tektite/wasm-register", serverAddress),
		unregisterWasmURL: fmt.Sprintf("https://%s/tektite/wasm-unregister", serverAddress),
		tlsConfig:         tlsConfig,
		httpCl:            httpCl,
	}, nil
}

type client struct {
	serverAddress     string
	statementURL      string
	queryURL          string
	execPSURL         string
	registerWasmURL   string
	unregisterWasmURL string
	tlsConfig         TLSConfig
	httpCl            *http.Client
	stopped           atomic.Bool
}

func (c *client) Close() {
	c.stopped.Store(true)
	c.httpCl.CloseIdleConnections()
}

func (c *client) ExecuteStatement(statement string) error {
	_, err := common3.CallWithRetryOnUnavailableWithTimeout[int](func() (int, error) {
		return 0, c.executeStatement(statement)
	}, c.isStopped, queryRetryDelay, queryRetryTimeout, "")
	return err
}

func (c *client) executeStatement(statement string) error {
	if statement == "" {
		return common3.NewTektiteErrorf(common3.StatementError, "statement is empty")
	}
	resp, err := c.sendPostRequest(c.statementURL, statement)
	if err != nil {
		return err
	}
	defer closeResponseBody(resp)
	return c.extractError(resp)
}

func (c *client) PrepareQuery(queryName string, tsl string) (PreparedQuery, error) {
	var builder strings.Builder
	builder.WriteString("prepare ")
	builder.WriteString(queryName)
	builder.WriteString(" := ")
	builder.WriteString(tsl)
	resp, err := c.sendPostRequest(c.statementURL, builder.String())
	if err != nil {
		return nil, err
	}
	defer closeResponseBody(resp)
	if err := c.extractError(resp); err != nil {
		return nil, err
	}
	return newPreparedQuery(c, queryName), nil
}

func (c *client) sendPostRequest(uri string, body string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, uri, bytes.NewBufferString(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("accept", "x-tektite-arrow")
	resp, err := c.httpCl.Do(req)
	return resp, maybeConvertConnectionError(err)
}

func (c *client) RegisterWasmModule(modulePath string) error {
	if !strings.HasSuffix(modulePath, ".wasm") {
		return common3.NewTektiteError(common3.WasmError, "wasm module must have '.wasm' suffix")
	}
	modBytes, err := os.ReadFile(modulePath)
	if err != nil {
		return common3.NewTektiteErrorf(common3.WasmError, "failed to read wasm file '%s: %v", modulePath, err)
	}
	metaFile := modulePath[:len(modulePath)-5] + ".json"
	jsonBytes, err := os.ReadFile(metaFile)
	if err != nil {
		return common3.NewTektiteErrorf(common3.WasmError, "failed to read wasm json file '%s: %v", metaFile, err)
	}
	encodedModBytes := base64.StdEncoding.EncodeToString(modBytes)
	registration := fmt.Sprintf(`{"MetaData":%s, "ModuleData":"%s"}`, string(jsonBytes), encodedModBytes)
	resp, err := c.sendPostRequest(c.registerWasmURL, registration)
	if err != nil {
		return err
	}
	defer closeResponseBody(resp)
	return c.extractError(resp)
}

func (c *client) UnregisterWasmModule(moduleName string) error {
	resp, err := c.sendPostRequest(c.unregisterWasmURL, moduleName)
	if err != nil {
		return err
	}
	defer closeResponseBody(resp)
	return c.extractError(resp)
}

func maybeConvertConnectionError(err error) error {
	if err != nil {
		var urlErr *url.Error
		if errwrap.As(err, &urlErr) && urlErr != nil {
			var netErr *net.OpError
			if errwrap.As(urlErr.Err, &netErr) && netErr != nil {
				return common3.NewTektiteErrorf(common3.ConnectionError, "connection error: %v", netErr)
			}
		}
		return err
	}
	return nil
}

type preparedQuery struct {
	c        *client
	name     string
	args     []any
	maxIndex int
}

func newPreparedQuery(c *client, name string) *preparedQuery {
	return &preparedQuery{
		c:        c,
		name:     name,
		maxIndex: 0,
		args:     make([]any, 0, 2),
	}
}

func (p *preparedQuery) checkIndex(index int) {
	largs := len(p.args)
	if index >= largs {
		newArgs := make([]any, (index+1)*2)
		copy(newArgs, p.args)
		p.args = newArgs
	}
	if index > p.maxIndex {
		p.maxIndex = index
	}
}

func (p *preparedQuery) getArgs() []any {
	return p.args[:p.maxIndex+1]
}

func (p *preparedQuery) Name() string {
	return p.name
}

func (p *preparedQuery) SetIntArg(index int, val int64) {
	p.checkIndex(index)
	p.args[index] = val
}

func (p *preparedQuery) SetFloatArg(index int, val float64) {
	p.checkIndex(index)
	p.args[index] = val
}

func (p *preparedQuery) SetBoolArg(index int, val bool) {
	p.checkIndex(index)
	p.args[index] = val
}

func (p *preparedQuery) SetDecimalArg(index int, val types.Decimal) {
	p.checkIndex(index)
	p.args[index] = val
}

func (p *preparedQuery) SetStringArg(index int, val string) {
	p.checkIndex(index)
	p.args[index] = val
}

func (p *preparedQuery) SetBytesArg(index int, val []byte) {
	p.checkIndex(index)
	p.args[index] = val
}

func (p *preparedQuery) SetTimestampArg(index int, val types.Timestamp) {
	p.checkIndex(index)
	p.args[index] = val
}

func (p *preparedQuery) SetNullArg(index int) {
	p.checkIndex(index)
	p.args[index] = nil
}

func (p *preparedQuery) Execute() (QueryResult, error) {
	return p.c.executePreparedQuery(p.name, p.getArgs()...)
}

func (p *preparedQuery) StreamExecute() (chan StreamChunk, error) {
	return p.c.streamExecutePreparedQuery(p.name, p.getArgs()...)
}

func (c *client) GetPreparedQuery(queryName string) PreparedQuery {
	return newPreparedQuery(c, queryName)
}

func (c *client) ExecuteQuery(query string) (QueryResult, error) {
	return c.executeQuery(c.queryURL, query)
}

func (c *client) executePreparedQuery(queryName string, args ...any) (QueryResult, error) {
	return c.executeQuery(c.execPSURL, createExecutePSBody(queryName, args...))
}

func createExecutePSBody(queryName string, args ...any) string {
	var builder strings.Builder
	builder.WriteString(`{"QueryName":"`)
	builder.WriteString(queryName)
	builder.WriteString(`","Args":[`)
	for i, arg := range args {
		if arg == nil {
			builder.WriteString("null")
		} else {
			switch a := arg.(type) {
			case int64:
				builder.WriteString(strconv.Itoa(int(a)))
			case float64:
				builder.WriteString(strconv.FormatFloat(a, 'g', 6, 64))
			case bool:
				if a {
					builder.WriteString("true")
				} else {
					builder.WriteString("false")
				}
			case types.Decimal:
				builder.WriteString(a.String())
			case string:
				builder.WriteString(strconv.Quote(a))
			case []byte:
				encoded := base64.StdEncoding.EncodeToString(a)
				builder.WriteRune('"')
				builder.WriteString(encoded)
				builder.WriteRune('"')
			case types.Timestamp:
				builder.WriteString(strconv.Itoa(int(a.Val)))
			default:
				panic("unexpected type")
			}
		}
		if i != len(args)-1 {
			builder.WriteRune(',')
		}
	}
	builder.WriteString(`]}`)
	return builder.String()
}

func (c *client) isStopped() bool {
	return c.stopped.Load()
}

func (c *client) executeQuery(url string, query string) (QueryResult, error) {
	return common3.CallWithRetryOnUnavailableWithTimeout[QueryResult](func() (QueryResult, error) {
		return c.executeQuery0(url, query)
	}, c.isStopped, queryRetryDelay, queryRetryTimeout, "")
}

func (c *client) executeQuery0(url string, query string) (QueryResult, error) {
	resp, err := c.sendPostRequest(url, query)
	if err != nil {
		return nil, err
	}
	defer closeResponseBody(resp)
	if err := c.extractError(resp); err != nil {
		return nil, err
	}
	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	schema, offset := api.DecodeArrowSchema(buff[8:]) // first 8 bytes is length of schema block
	buff = buff[8+offset:]
	var batches []*evbatch.Batch
	for len(buff) > 0 {
		bl, _ := encoding.ReadUint64FromBufferLE(buff, 0)
		be := 8 + int(bl)
		batch := api.DecodeArrowBatch(schema, buff[8:be])
		batches = append(batches, batch)
		buff = buff[be:]
	}
	var bigBatch *evbatch.Batch
	if len(batches) > 1 {
		bigBatch = combineBatches(schema, batches)
	} else {
		bigBatch = batches[0]
	}

	return &arrowBasedQueryResult{batch: bigBatch}, nil
}

func combineBatches(schema *evbatch.EventSchema, batches []*evbatch.Batch) *evbatch.Batch {
	builders := evbatch.CreateColBuilders(schema.ColumnTypes())
	for _, batch := range batches {
		rc := batch.RowCount
		for i := 0; i < rc; i++ {
			for colIndex, ft := range schema.ColumnTypes() {
				evbatch.CopyColumnEntry(ft, builders, colIndex, i, batch)
			}
		}
	}
	return evbatch.NewBatchFromBuilders(schema, builders...)
}

func (c *client) StreamExecuteQuery(query string) (chan StreamChunk, error) {
	return c.streamExecQueryWithRetry(c.queryURL, query)
}

func (c *client) streamExecutePreparedQuery(queryName string, args ...any) (chan StreamChunk, error) {
	return c.streamExecQueryWithRetry(c.execPSURL, createExecutePSBody(queryName, args...))
}

func (c *client) streamExecQueryWithRetry(url string, query string) (chan StreamChunk, error) {
	return common3.CallWithRetryOnUnavailableWithTimeout[chan StreamChunk](func() (chan StreamChunk, error) {
		return c.streamExecQuery(url, query)
	}, c.isStopped, queryRetryDelay, queryRetryTimeout, "")
}

func (c *client) streamExecQuery(url string, query string) (chan StreamChunk, error) {
	resp, err := c.sendPostRequest(url, query)
	if err != nil {
		return nil, err
	}
	if err := c.extractError(resp); err != nil {
		return nil, err
	}
	ch := make(chan StreamChunk, 1000)
	go func() {
		c.streamQueryResults(resp.Body, ch)
		closeResponseBody(resp)
	}()
	return ch, nil
}

func readLengthPrefixed(stream io.Reader, ch chan StreamChunk) ([]byte, bool) {
	hb := make([]byte, 8)
	_, err := io.ReadFull(stream, hb)
	if err != nil {
		if err == io.EOF {
			return nil, false
		}
		ch <- StreamChunk{Err: err}
		return nil, false
	}
	lenBuf := binary.LittleEndian.Uint64(hb)
	buf := make([]byte, int(lenBuf))
	_, err = io.ReadFull(stream, buf)
	if err != nil {
		ch <- StreamChunk{Err: err}
		return nil, false
	}
	return buf, true
}

func (c *client) streamQueryResults(bodyStream io.Reader, ch chan StreamChunk) {
	headersBuf, ok := readLengthPrefixed(bodyStream, ch)
	if !ok {
		return
	}
	schema, _ := api.DecodeArrowSchema(headersBuf)
	for {
		batchBuf, ok := readLengthPrefixed(bodyStream, ch)
		if batchBuf == nil {
			// EOF
			close(ch)
			break
		}
		if !ok {
			return
		}
		batch := api.DecodeArrowBatch(schema, batchBuf)
		ch <- StreamChunk{Chunk: &arrowBasedQueryResult{batch}}
	}
}

func (c *client) extractError(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		bodyString := common3.ByteSliceToStringZeroCopy(bodyBytes)
		bodyString = bodyString[:len(bodyString)-1] // remove trailing \n
		if len(bodyString) > 10 && strings.HasPrefix(bodyString, "TEK") {
			sNum := bodyString[3:7]
			errorCode, err := strconv.Atoi(sNum)
			if err != nil {
				return err
			}
			msg := bodyString[10:]
			return common3.NewTektiteErrorf(common3.ErrCode(errorCode), msg)
		}
		return errwrap.New(bodyString)
	}
	return nil
}

func closeResponseBody(resp *http.Response) {
	if resp != nil {
		if err := resp.Body.Close(); err != nil {
			log.Errorf("failed to close http response %v", err)
		}
	}
}

type arrowBasedQueryResult struct {
	batch *evbatch.Batch
}

func (a *arrowBasedQueryResult) ColumnNames() []string {
	return a.batch.Schema.ColumnNames()
}

func (a *arrowBasedQueryResult) ColumnTypes() []types.ColumnType {
	return a.batch.Schema.ColumnTypes()
}

func (a *arrowBasedQueryResult) Meta() Meta {
	return a
}

func (a *arrowBasedQueryResult) ColumnCount() int {
	return len(a.batch.Schema.ColumnTypes())
}

func (a *arrowBasedQueryResult) RowCount() int {
	return a.batch.RowCount
}

func (a *arrowBasedQueryResult) Column(colIndex int) Column {
	return &arrowBasedColumn{col: a.batch.Columns[colIndex]}
}

func (a *arrowBasedQueryResult) Row(rowIndex int) Row {
	return &arrowBasedRow{rowIndex: rowIndex, qr: a}
}

type arrowBasedColumn struct {
	col evbatch.Column
}

func (a *arrowBasedColumn) IsNull(rowIndex int) bool {
	return a.col.IsNull(rowIndex)
}

func (a *arrowBasedColumn) IntVal(rowIndex int) int64 {
	return a.col.(*evbatch.IntColumn).Get(rowIndex)
}

func (a *arrowBasedColumn) FloatVal(rowIndex int) float64 {
	return a.col.(*evbatch.FloatColumn).Get(rowIndex)
}

func (a *arrowBasedColumn) BoolVal(rowIndex int) bool {
	return a.col.(*evbatch.BoolColumn).Get(rowIndex)
}

func (a *arrowBasedColumn) DecimalVal(rowIndex int) types.Decimal {
	return a.col.(*evbatch.DecimalColumn).Get(rowIndex)
}

func (a *arrowBasedColumn) StringVal(rowIndex int) string {
	return a.col.(*evbatch.StringColumn).Get(rowIndex)
}

func (a *arrowBasedColumn) BytesVal(rowIndex int) []byte {
	return a.col.(*evbatch.BytesColumn).Get(rowIndex)
}

func (a *arrowBasedColumn) TimestampVal(rowIndex int) types.Timestamp {
	return a.col.(*evbatch.TimestampColumn).Get(rowIndex)
}

type arrowBasedRow struct {
	rowIndex int
	qr       *arrowBasedQueryResult
}

func (a *arrowBasedRow) IsNull(colIndex int) bool {
	return a.qr.batch.Columns[colIndex].IsNull(a.rowIndex)
}

func (a *arrowBasedRow) IntVal(colIndex int) int64 {
	return a.qr.batch.Columns[colIndex].(*evbatch.IntColumn).Get(a.rowIndex)
}

func (a *arrowBasedRow) FloatVal(colIndex int) float64 {
	return a.qr.batch.Columns[colIndex].(*evbatch.FloatColumn).Get(a.rowIndex)
}

func (a *arrowBasedRow) BoolVal(colIndex int) bool {
	return a.qr.batch.Columns[colIndex].(*evbatch.BoolColumn).Get(a.rowIndex)
}

func (a *arrowBasedRow) DecimalVal(colIndex int) types.Decimal {
	return a.qr.batch.Columns[colIndex].(*evbatch.DecimalColumn).Get(a.rowIndex)
}

func (a *arrowBasedRow) StringVal(colIndex int) string {
	return a.qr.batch.Columns[colIndex].(*evbatch.StringColumn).Get(a.rowIndex)
}

func (a *arrowBasedRow) BytesVal(colIndex int) []byte {
	return a.qr.batch.Columns[colIndex].(*evbatch.BytesColumn).Get(a.rowIndex)
}

func (a *arrowBasedRow) TimestampVal(colIndex int) types.Timestamp {
	return a.qr.batch.Columns[colIndex].(*evbatch.TimestampColumn).Get(a.rowIndex)
}
