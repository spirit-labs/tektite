package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/spirit-labs/tektite/command"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/evbatch"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/query"
	"github.com/spirit-labs/tektite/types"
	"github.com/spirit-labs/tektite/wasm"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type HTTPAPIServer struct {
	lock             sync.Mutex
	listenAddress    string
	apiPath          string
	listener         net.Listener
	httpServer       *http.Server
	closeWg          sync.WaitGroup
	queryManager     query.Manager
	commandManager   command.Manager
	parser           *parser.Parser
	moduleManager    wasmModuleManager
	tlsConf          conf.TLSConfig
	wasmRegisterPath string
}

type wasmModuleManager interface {
	RegisterModule(moduleMetadata wasm.ModuleMetadata, moduleBytes []byte) error
	UnregisterModule(name string) error
}

func NewHTTPAPIServer(listenAddress string, apiPath string, queryManager query.Manager, commandManager command.Manager,
	parser *parser.Parser, moduleManager wasmModuleManager, tlsConf conf.TLSConfig) *HTTPAPIServer {
	return &HTTPAPIServer{
		listenAddress:    listenAddress,
		apiPath:          apiPath,
		queryManager:     queryManager,
		commandManager:   commandManager,
		parser:           parser,
		moduleManager:    moduleManager,
		tlsConf:          tlsConf,
		wasmRegisterPath: fmt.Sprintf("%s/%s", apiPath, "wasm-register"),
	}
}

// Start - note that this does not actually start the server - we delay really starting this (with Activate)
// until the rest of the server is started.
func (s *HTTPAPIServer) Start() error {
	return nil
}

func (s *HTTPAPIServer) Activate() error {
	return s.start()
}

func (s *HTTPAPIServer) start() error {
	s.lock.Lock()

	defer s.lock.Unlock()
	tlsConf, err := conf.CreateServerTLSConfig(s.tlsConf)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc(fmt.Sprintf("%s/query", s.apiPath), s.handleQuery)
	mux.HandleFunc(fmt.Sprintf("%s/exec", s.apiPath), s.handleExecPreparedStatement)
	mux.HandleFunc(fmt.Sprintf("%s/statement", s.apiPath), s.handleStatement)
	mux.HandleFunc(fmt.Sprintf("%s/wasm-register", s.apiPath), s.handleWasmRegister)
	mux.HandleFunc(fmt.Sprintf("%s/wasm-unregister", s.apiPath), s.handleWasmUnregister)
	s.httpServer = &http.Server{
		Handler:     mux,
		IdleTimeout: 0,
		TLSConfig:   tlsConf,
	}
	s.listener, err = net.Listen("tcp", s.listenAddress)
	if err != nil {
		return err
	}
	s.closeWg = sync.WaitGroup{}
	s.closeWg.Add(1)
	common.Go(func() {
		defer s.closeWg.Done()
		err = s.httpServer.ServeTLS(s.listener, "", "")
		if !errors.Is(http.ErrServerClosed, err) {
			log.Errorf("Failed to start the HTTP API server: %v", err)
		}
	})
	return nil
}

func (s *HTTPAPIServer) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return err
	}
	if err := s.listener.Close(); err != nil {
		// Ignore
	}
	s.closeWg.Wait()
	return nil
}

func (s *HTTPAPIServer) handleQuery(writer http.ResponseWriter, request *http.Request) { //nolint:gocyclo
	defer common.PanicHandler()
	u := s.checkRequest(writer, request)
	if u == nil {
		return
	}
	batchWriter := getBatchWriter(writer, request)
	includeHeader := getIncludeHeader(u)
	queryString, ok := getBodyAsString(writer, request)
	if !ok {
		return
	}
	queryDesc, err := s.parser.ParseQuery(queryString)
	if err != nil {
		writeInvalidStatementError(err.Error(), writer)
		return
	}
	execQuery(writer, batchWriter, includeHeader, func(o outFunc) error {
		return s.queryManager.ExecuteQueryDirect(queryString, *queryDesc, o)
	})
}

func (s *HTTPAPIServer) handleExecPreparedStatement(writer http.ResponseWriter, request *http.Request) { //nolint:gocyclo
	defer common.PanicHandler()
	u := s.checkRequest(writer, request)
	if u == nil {
		return
	}
	batchWriter := getBatchWriter(writer, request)
	includeHeader := getIncludeHeader(u)
	body, ok := getBody(writer, request)
	if !ok {
		return
	}
	invocation := &PreparedStatementInvocation{}
	if err := json.Unmarshal(body, &invocation); err != nil {
		writeError("invalid JSON in body", writer, errors.ExecuteQueryError)
		return
	}
	paramsSchema := s.queryManager.GetPreparedQueryParamSchema(invocation.QueryName)
	if paramsSchema == nil {
		writeError(fmt.Sprintf("unknown prepared query '%s'", invocation.QueryName), writer, errors.ExecuteQueryError)
		return
	}
	if len(invocation.Args) != len(paramsSchema.ColumnTypes()) {
		writeError(fmt.Sprintf("prepared query takes %d arguments, but only %d arguments provided", len(paramsSchema.ColumnTypes()), len(invocation.Args)),
			writer, errors.ExecuteQueryError)
		return
	}
	args, err := convertPreparedStatementArgs(invocation.Args, paramsSchema.ColumnTypes())
	if err != nil {
		writeError(err.Error(), writer, errors.ExecuteQueryError)
		return
	}
	execQuery(writer, batchWriter, includeHeader, func(o outFunc) error {
		_, err := s.queryManager.ExecutePreparedQuery(invocation.QueryName, args, o)
		return err
	})
}

func convertPreparedStatementArgs(args []any, argTypes []types.ColumnType) ([]any, error) {
	for i, arg := range args {
		if arg == nil {
			continue
		}
		ok := true
		argType := argTypes[i]
		switch argType.ID() {
		case types.ColumnTypeIDInt:
			switch v := arg.(type) {
			case int:
				args[i] = int64(v)
			case float64:
				args[i] = int64(v)
			case string:
				iVal, err := strconv.Atoi(v)
				if err == nil {
					args[i] = int64(iVal)
				} else {
					ok = false
				}
			default:
				ok = false
			}
		case types.ColumnTypeIDFloat:
			switch v := arg.(type) {
			case int:
				args[i] = float64(v)
			case float64:
				// OK
			case string:
				fVal, err := strconv.ParseFloat(v, 64)
				if err == nil {
					args[i] = fVal
				} else {
					ok = false
				}
			default:
				ok = false
			}
		case types.ColumnTypeIDBool:
			switch v := arg.(type) {
			case bool:
				// OK
			case string:
				switch v {
				case "true", "TRUE":
					args[i] = true
				case "false", "FALSE":
					args[i] = false
				default:
					ok = false
				}
			default:
				ok = false
			}
		case types.ColumnTypeIDDecimal:
			decType := argType.(*types.DecimalType)
			switch v := arg.(type) {
			case string:
				dec, err := types.NewDecimalFromString(v, decType.Precision, decType.Scale)
				if err == nil {
					args[i] = dec
				} else {
					ok = false
				}
			case float64:
				dec, err := types.NewDecimalFromFloat64(v, decType.Precision, decType.Scale)
				if err == nil {
					args[i] = dec
				} else {
					ok = false
				}
			case int:
				dec := types.NewDecimalFromInt64(int64(v), decType.Precision, decType.Scale)
				args[i] = dec
			default:
				ok = false
			}
		case types.ColumnTypeIDString:
			switch v := arg.(type) {
			case string:
				// OK
			case float64:
				args[i] = strconv.FormatFloat(v, 'g', 6, 64)
			case int:
				args[i] = strconv.Itoa(v)
			case bool:
				if v {
					args[i] = "true"
				} else {
					args[i] = "false"
				}
			default:
				ok = false
			}
		case types.ColumnTypeIDBytes:
			var sVal string
			sVal, ok = arg.(string)
			if ok {
				bytes, err := base64.StdEncoding.DecodeString(sVal)
				if err != nil {
					return nil, errors.Errorf("argument %d (%v) of type %s cannot be base64 decoded", i, arg,
						reflect.TypeOf(arg).String())
				}
				args[i] = bytes
			}
		case types.ColumnTypeIDTimestamp:
			switch v := arg.(type) {
			case int:
				args[i] = types.NewTimestamp(int64(v))
			case float64:
				args[i] = types.NewTimestamp(int64(v))
			default:
				ok = false
			}
		default:
			panic("unexpected type")
		}
		if !ok {
			return nil, errors.Errorf("argument %d (%v) of type %s cannot be converted to %s", i, arg,
				reflect.TypeOf(arg).String(), argType.String())
		}
	}
	return args, nil
}

type PreparedStatementInvocation struct {
	QueryName string
	Args      []any
}

func getBatchWriter(writer http.ResponseWriter, request *http.Request) BatchWriter {
	var batchWriter BatchWriter
	accept := request.Header.Get("accept")
	if accept == "x-tektite-arrow" {
		batchWriter = &ArrowBatchWriter{}
	} else {
		batchWriter = &jsonLinesBatchWriter{}
	}
	batchWriter.WriteContentType(writer)
	return batchWriter
}

func getIncludeHeader(u *url.URL) bool {
	sIncludeHeader := u.Query().Get("col_headers")
	if sIncludeHeader != "" && strings.ToLower(sIncludeHeader) == "true" {
		return true
	}
	return false
}

func (s *HTTPAPIServer) handleStatement(writer http.ResponseWriter, request *http.Request) { //nolint:gocyclo
	defer common.PanicHandler()
	u := s.checkRequest(writer, request)
	if u == nil {
		return
	}
	com, ok := getBodyAsString(writer, request)
	if !ok {
		return
	}
	tsl, err := s.parser.ParseTSL(com)
	if err != nil {
		writeInvalidStatementError(err.Error(), writer)
		return
	}
	if tsl.CreateStream == nil && tsl.DeleteStream == nil && tsl.PrepareQuery == nil {
		writeError("invalid statement. must be create stream / delete stream / prepare query", writer, errors.StatementError)
		return
	}
	if err := s.commandManager.ExecuteCommand(com); err != nil {
		maybeConvertAndSendError(err, writer)
	}
}

func (s *HTTPAPIServer) checkRequest(writer http.ResponseWriter, request *http.Request) *url.URL {
	if request.ProtoMajor != 2 {
		http.Error(writer, "the tektite HTTP API supports HTTP2 only", http.StatusHTTPVersionNotSupported)
		return nil
	}
	u, err := url.ParseRequestURI(request.RequestURI)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return nil
	}
	if request.Method != http.MethodPost {
		http.Error(writer, "the HTTP method must be a POST", http.StatusMethodNotAllowed)
		return nil
	}
	return u
}

func getBody(writer http.ResponseWriter, request *http.Request) ([]byte, bool) {
	bodyBytes, err := io.ReadAll(request.Body)
	if err != nil {
		log.Errorf("failed to read message body %v", err)
		writeError("failed to read message body", writer, errors.InternalError)
		return nil, false
	}
	return bodyBytes, true
}

func getBodyAsString(writer http.ResponseWriter, request *http.Request) (string, bool) {
	bodyBytes, ok := getBody(writer, request)
	if !ok {
		return "", false
	}
	return common.ByteSliceToStringZeroCopy(bodyBytes), true
}

type WasmRegistration struct {
	MetaData   wasm.ModuleMetadata
	ModuleData string
}

func (s *HTTPAPIServer) handleWasmRegister(writer http.ResponseWriter, request *http.Request) {
	u := s.checkRequest(writer, request)
	if u == nil {
		return
	}
	body, ok := getBody(writer, request)
	if !ok {
		return
	}
	registration := &WasmRegistration{}
	if err := json.Unmarshal(body, registration); err != nil {
		writeError(fmt.Sprintf("failed to parse JSON: %v", err), writer, errors.WasmError)
		return
	}
	decoded, err := base64.StdEncoding.DecodeString(registration.ModuleData)
	if err != nil {
		writeError(fmt.Sprintf("failed to base64 decode module bytes: %v", err), writer, errors.WasmError)
		return
	}
	if err := s.moduleManager.RegisterModule(registration.MetaData, decoded); err != nil {
		maybeConvertAndSendError(err, writer)
	}
}

func (s *HTTPAPIServer) handleWasmUnregister(writer http.ResponseWriter, request *http.Request) {
	u := s.checkRequest(writer, request)
	if u == nil {
		return
	}
	moduleName, ok := getBodyAsString(writer, request)
	if !ok {
		return
	}
	if err := s.moduleManager.UnregisterModule(moduleName); err != nil {
		maybeConvertAndSendError(err, writer)
	}
}

func (s *HTTPAPIServer) ListenAddress() string {
	return s.listenAddress
}

type outFunc func(last bool, numLastBatches int, batch *evbatch.Batch) error

func execQuery(writer http.ResponseWriter, batchWriter BatchWriter, includeHeader bool, outFuncFunc func(outFunc) error) {
	lastCount := uint64(0)
	batchCh := make(chan *evbatch.Batch, 10)
	outFunc := func(last bool, numLastBatches int, batch *evbatch.Batch) error {
		if batch != nil {
			batchCh <- batch
		}
		if last && atomic.AddUint64(&lastCount, 1) == uint64(numLastBatches) {
			close(batchCh)
		}
		return nil
	}
	err := outFuncFunc(outFunc)
	if err != nil {
		maybeConvertAndSendError(err, writer)
		return
	}
	headersWritten := !includeHeader
	for batch := range batchCh {
		if !headersWritten {
			if err := batchWriter.WriteHeaders(batch.Schema.ColumnNames(), batch.Schema.ColumnTypes(), writer); err != nil {
				maybeConvertAndSendError(err, writer)
				return
			}
			headersWritten = true
		}
		if err := batchWriter.WriteBatch(batch, writer); err != nil {
			maybeConvertAndSendError(err, writer)
			return
		}
	}
}

func writeError(msg string, writer http.ResponseWriter, errorCode errors.ErrorCode) {
	// We write API errors with a suffix of "TEKxxxx" where xxxx is the error code, zero padded. The client
	// needs to know this information
	msg = fmt.Sprintf("TEK%04d - %s", errorCode, msg)
	http.Error(writer, msg, http.StatusBadRequest)
}

func writeInvalidStatementError(msg string, writer http.ResponseWriter) {
	writeError(msg, writer, errors.StatementError)
}

func maybeConvertAndSendError(err error, writer http.ResponseWriter) {
	perr := maybeConvertError(err)
	var statusCode int
	if perr.Code == errors.InternalError {
		statusCode = http.StatusInternalServerError
	} else {
		statusCode = http.StatusBadRequest
	}
	// We add TEK followed by the error code so the client can distinguish Tektite errors
	// e.g. client needs to know if it's an unavailable error in order to retry
	msg := fmt.Sprintf("TEK%04d - %s", perr.Code, perr.Msg)
	http.Error(writer, msg, statusCode)
}

func maybeConvertError(err error) errors.TektiteError {
	var perr errors.TektiteError
	if errors.As(err, &perr) {
		return perr
	}
	return common.LogInternalError(err)
}
