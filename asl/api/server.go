package api

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/asl/arista"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/auth"
	"github.com/spirit-labs/tektite/cmdmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/query"
	"github.com/spirit-labs/tektite/types"
	"github.com/spirit-labs/tektite/wasm"
	"golang.org/x/net/http2"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ScramAuthMethod int

type HTTPAPIServer struct {
	lock                   sync.Mutex
	nodeID                 int
	listenAddresses        []string
	apiPath                string
	listener               net.Listener
	httpServer             *http.Server
	closeWg                sync.WaitGroup
	queryManager           query.Manager
	commandManager         cmdmgr.Manager
	parser                 *parser.Parser
	moduleManager          wasmModuleManager
	tlsConf                conf.TLSConfig
	wasmRegisterPath       string
	scramConversations     sync.Map
	scramManager           *auth.ScramManager
	jwtPrivateKey          *rsa.PrivateKey
	jwtPublicKeys          map[int]JwtPublicKeyInfo
	jwtInstanceID          string
	jwtPubKeysLock         sync.RWMutex
	authenticationRequired bool
	httpClient             *http.Client
	authenticatedUsersLock sync.RWMutex
	authenticatedUsers     map[string]uint64
	authCacheTimeout       uint64
}

type scramConversationHolder struct {
	conv    *auth.ScramConversation
	timeout *time.Timer
}

type wasmModuleManager interface {
	RegisterModule(moduleMetadata wasm.ModuleMetadata, moduleBytes []byte) error
	UnregisterModule(name string) error
}

func NewHTTPAPIServer(nodeID int, listenAddresses []string, apiPath string, queryManager query.Manager, commandManager cmdmgr.Manager,
	parser *parser.Parser, moduleManager wasmModuleManager, scramManager *auth.ScramManager, tlsConf conf.TLSConfig, authRequired bool,
	authCacheTimeout time.Duration) *HTTPAPIServer {
	return &HTTPAPIServer{
		nodeID:                 nodeID,
		listenAddresses:        listenAddresses,
		apiPath:                apiPath,
		queryManager:           queryManager,
		commandManager:         commandManager,
		parser:                 parser,
		moduleManager:          moduleManager,
		scramManager:           scramManager,
		tlsConf:                tlsConf,
		wasmRegisterPath:       fmt.Sprintf("%s/%s", apiPath, "wasm-register"),
		jwtPublicKeys:          map[int]JwtPublicKeyInfo{},
		authenticationRequired: authRequired,
		authenticatedUsers:     map[string]uint64{},
		authCacheTimeout:       uint64(authCacheTimeout),
	}
}

const (
	ScramAuthTypeHeaderName       = "X-Tektite-Scram-Auth-Type"
	ScramConversationIdHeaderName = "X-Tektite-Scram-Conversation-Id"
	JwtHeaderName                 = "X-Tektite-JWT-Token"
)

const scramAuthenticationTimeout = 5 * time.Second

type JwtPublicKeyInfo struct {
	PubKey     *rsa.PublicKey
	InstanceID string // Every time a node starts it gets a new InstanceID so we can determine if the cached public key is the right one
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
	// Generate a private/public key for signing and verifying JWTs
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	s.jwtInstanceID = uuid.New().String()
	s.jwtPrivateKey = privateKey
	tlsConf, err := conf.CreateServerTLSConfig(s.tlsConf)
	if err != nil {
		return err
	}
	// Create an http client for calling to get public keys from other nodes
	httpCl := &http.Client{}
	clientGoTlsConf, err := toGoTlsConfig(s.tlsConf.CertPath)
	if err != nil {
		return err
	}
	httpCl.Transport = &http2.Transport{
		TLSClientConfig: clientGoTlsConf,
	}
	s.httpClient = httpCl
	mux := http.NewServeMux()
	mux.HandleFunc(fmt.Sprintf("%s/query", s.apiPath), s.handleQuery)
	mux.HandleFunc(fmt.Sprintf("%s/exec", s.apiPath), s.handleExecPreparedStatement)
	mux.HandleFunc(fmt.Sprintf("%s/statement", s.apiPath), s.handleStatement)
	mux.HandleFunc(fmt.Sprintf("%s/wasm-register", s.apiPath), s.handleWasmRegister)
	mux.HandleFunc(fmt.Sprintf("%s/wasm-unregister", s.apiPath), s.handleWasmUnregister)
	mux.HandleFunc(fmt.Sprintf("%s/scram-auth", s.apiPath), s.handleScramAuth)
	mux.HandleFunc(fmt.Sprintf("%s/pub-key", s.apiPath), s.handlePubKey)
	mux.HandleFunc(fmt.Sprintf("%s/put-user", s.apiPath), s.handlePutUser)
	mux.HandleFunc(fmt.Sprintf("%s/delete-user", s.apiPath), s.handleDeleteUser)
	s.httpServer = &http.Server{
		Handler:     mux,
		IdleTimeout: 0,
		TLSConfig:   tlsConf,
	}
	s.listener, err = common.Listen("tcp", s.listenAddresses[s.nodeID])
	if err != nil {
		return err
	}
	s.closeWg = sync.WaitGroup{}
	s.closeWg.Add(1)
	common.Go(func() {
		defer s.closeWg.Done()
		err = s.httpServer.ServeTLS(s.listener, "", "")
		if !errwrap.Is(http.ErrServerClosed, err) {
			log.Errorf("Failed to start the HTTP API server: %v", err)
		}
	})
	return nil
}

func toGoTlsConfig(serverCertPath string) (*tls.Config, error) {
	// Note, the Tektite HTTP2 HTTP API is HTTP2 only and requires TLS
	tlsConfig := &tls.Config{ // nolint: gosec
		MinVersion: tls.VersionTLS12,
	}
	rootCerts, err := os.ReadFile(serverCertPath)
	if err != nil {
		return nil, err
	}
	rootCertPool := x509.NewCertPool()
	if ok := rootCertPool.AppendCertsFromPEM(rootCerts); !ok {
		return nil, errwrap.Errorf("failed to append root certs PEM (invalid PEM block?)")
	}
	tlsConfig.RootCAs = rootCertPool
	return tlsConfig, nil
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
	s.httpClient.CloseIdleConnections()
	return nil
}

func (s *HTTPAPIServer) maybeAuthenticate(writer http.ResponseWriter, request *http.Request) bool {
	if !s.authenticationRequired {
		return true
	}
	authHeader := request.Header.Get("Authorization")
	if authHeader == "" {
		http.Error(writer, "missing Authorization header", http.StatusUnauthorized)
		return false
	}
	if strings.HasPrefix(authHeader, "Bearer ") {
		jwt := authHeader[7:]
		// Verify the JWT was created by our trusted server
		claims, valid, err := s.verifyJwt(jwt)
		if err != nil || !valid {
			http.Error(writer, "invalid JWT", http.StatusUnauthorized)
			return false
		}
		// Verifying the JWT is not sufficient as users can update their passwords or be deleted from the system while
		// but the JWT will remain valid. We cache authentications for a short amount of time, but when that time has
		// expired we need to check whether the JWT can still be used. When we store credentials we also store a sequence
		// number, this increments every time the credentials are updated. When we create thr JWT we encode the sequence
		// number as of that time in the JWT. When we present the JWT we check the sequence number in the JWT matches the
		// stored sequence number. If they are the same the user has not been updated. If no sequence can be found, then
		// the user has been deleted and authentication fails. If sequence number does not match then password has been
		// updated and authentication fails.
		p, ok := claims["principal"]
		if ok {
			principal, ok := p.(string)
			if ok {
				// check in auth cache
				if s.checkAuthCache(principal) {
					// There's a cached authentication in the auth cache so we don't have to check user sequence
					return true
				}
				seq, ok := claims["sequence"]
				if ok {
					sequence, ok := seq.(int)
					// We need to check the sequence number in the JWT against the sequence number in the stored user
					// creds - this sequence number is incremented every time the password is changed
					storedSeq, ok, err := s.scramManager.GetUserCredsSequence(principal)
					if err != nil {
						maybeConvertAndSendError(err, writer)
						return false
					}
					if !ok {
						// User was deleted
						http.Error(writer, "authentication failed", http.StatusUnauthorized)
						return false
					}
					if storedSeq == sequence {
						// Stored user has same sequence number as JWT, so password wasn't changed
						// Update cached authentication
						s.cacheUserAuthentication(principal)
						return true
					}
					// Password has been changed
					http.Error(writer, "authentication failed", http.StatusUnauthorized)
					return false
				}
			}
		}
		// If we get here then it's an invalid JWT
		http.Error(writer, "invalid JWT", http.StatusUnauthorized)
		return false
	} else if strings.HasPrefix(authHeader, "Basic ") {
		basicStr := authHeader[6:]
		payload, _ := base64.StdEncoding.DecodeString(basicStr)
		pair := strings.SplitN(string(payload), ":", 2)
		if len(pair) != 2 {
			http.Error(writer, "invalid authorization header", http.StatusUnauthorized)
			return false
		}
		username := pair[0]
		password := pair[1]
		seq, authenticated, err := s.scramManager.AuthenticateWithUserPwd(username, password)
		if err != nil {
			http.Error(writer, "authentication failed", http.StatusUnauthorized)
			return false
		}
		if authenticated {
			// Set a JWT on the response
			s.cacheUserAuthentication(username)
			return s.createAndSetJwt(writer, username, seq)
		}
		return authenticated
	} else {
		http.Error(writer, "invalid authorization method", http.StatusUnauthorized)
		return false
	}
}

func (s *HTTPAPIServer) handleQuery(writer http.ResponseWriter, request *http.Request) { //nolint:gocyclo
	defer common.TektitePanicHandler()
	u := s.checkRequest(writer, request)
	if u == nil {
		return
	}
	if !s.maybeAuthenticate(writer, request) {
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
	defer common.TektitePanicHandler()
	u := s.checkRequest(writer, request)
	if u == nil {
		return
	}
	if !s.maybeAuthenticate(writer, request) {
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
		writeError("invalid JSON in body", writer, common.ExecuteQueryError)
		return
	}
	paramsSchema, ok := s.queryManager.GetPreparedQueryParamSchema(invocation.QueryName)
	if !ok {
		writeError(fmt.Sprintf("unknown prepared query '%s'", invocation.QueryName), writer, common.ExecuteQueryError)
		return
	}
	numParams := 0
	if paramsSchema != nil {
		numParams = len(paramsSchema.ColumnTypes())
	}
	if len(invocation.Args) != numParams {
		writeError(fmt.Sprintf("prepared query takes %d arguments, but only %d arguments provided", numParams, len(invocation.Args)),
			writer, common.ExecuteQueryError)
		return
	}
	var args []any
	if numParams > 0 {
		var err error
		args, err = convertPreparedStatementArgs(invocation.Args, paramsSchema.ColumnTypes())
		if err != nil {
			writeError(err.Error(), writer, common.ExecuteQueryError)
			return
		}
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
					return nil, errwrap.Errorf("argument %d (%v) of type %s cannot be base64 decoded", i, arg,
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
			return nil, errwrap.Errorf("argument %d (%v) of type %s cannot be converted to %s", i, arg,
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
	defer common.TektitePanicHandler()
	u := s.checkRequest(writer, request)
	if u == nil {
		return
	}
	if !s.maybeAuthenticate(writer, request) {
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
	if tsl.CreateStream == nil && tsl.DeleteStream == nil && tsl.PrepareQuery == nil && tsl.DeleteQuery == nil {
		writeError("invalid statement. must be create stream / delete stream / prepare query / delete query", writer, common.StatementError)
		return
	}
	if err := s.commandManager.ExecuteCommand(com); err != nil {
		maybeConvertAndSendError(err, writer)
	}
}

func (s *HTTPAPIServer) checkRequest(writer http.ResponseWriter, request *http.Request) *url.URL {
	if !checkHttp2(writer, request) {
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

func checkHttp2(writer http.ResponseWriter, request *http.Request) bool {
	if request.ProtoMajor != 2 {
		http.Error(writer, "the tektite HTTP API supports HTTP2 only", http.StatusHTTPVersionNotSupported)
		return false
	}
	return true
}

func getBody(writer http.ResponseWriter, request *http.Request) ([]byte, bool) {
	bodyBytes, err := io.ReadAll(request.Body)
	if err != nil {
		log.Errorf("failed to read message body %v", err)
		writeError("failed to read message body", writer, common.InternalError)
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
	if !s.maybeAuthenticate(writer, request) {
		return
	}
	body, ok := getBody(writer, request)
	if !ok {
		return
	}
	registration := &WasmRegistration{}
	if err := json.Unmarshal(body, registration); err != nil {
		writeError(fmt.Sprintf("failed to parse JSON: %v", err), writer, common.WasmError)
		return
	}
	decoded, err := base64.StdEncoding.DecodeString(registration.ModuleData)
	if err != nil {
		writeError(fmt.Sprintf("failed to base64 decode module bytes: %v", err), writer, common.WasmError)
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
	if !s.maybeAuthenticate(writer, request) {
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

func (s *HTTPAPIServer) handleScramAuth(writer http.ResponseWriter, request *http.Request) {
	u := s.checkRequest(writer, request)
	if u == nil {
		return
	}
	scramAuthType := request.Header.Get(ScramAuthTypeHeaderName)
	if scramAuthType == "" {
		http.Error(writer, fmt.Sprintf("header %s missing in request", ScramAuthTypeHeaderName), http.StatusBadRequest)
		return
	}
	if s.scramManager.AuthType() != scramAuthType {
		http.Error(writer, fmt.Sprintf("SCRAM auth type: %s not supported. Please use %s", scramAuthType, s.scramManager.AuthType()), http.StatusBadRequest)
		return
	}
	conversationID := request.Header.Get(ScramConversationIdHeaderName)
	var conv *auth.ScramConversation
	firstRequest := false
	if conversationID == "" {
		// First request of authentication - create a conversation
		firstRequest = true
		conversationID = uuid.New().String()
		var err error
		conv, err = s.scramManager.NewConversation()
		if err != nil {
			maybeConvertAndSendError(err, writer)
			return
		}
		holder := scramConversationHolder{
			conv: conv,
			// timeout to delete conversation if client does not call in for second request
			timeout: time.AfterFunc(scramAuthenticationTimeout, func() {
				s.scramConversations.Delete(conversationID)
			}),
		}
		s.scramConversations.Store(conversationID, holder)
	} else {
		c, ok := s.scramConversations.Load(conversationID)
		if !ok {
			// Either the client sent a bad conversation id, or the conversation may have timed out if a large delay
			// between client sending first and second request
			http.Error(writer, "authentication failed - no such conversation id", http.StatusUnauthorized)
			return
		} else {
			holder := c.(scramConversationHolder)
			conv = holder.conv
			s.scramConversations.Delete(conversationID)
			holder.timeout.Stop()
		}
	}
	bodyBytes, err := io.ReadAll(request.Body)
	if err != nil {
		maybeConvertAndSendError(err, writer)
		return
	}
	resp, complete, failed := conv.Process(bodyBytes)
	if failed {
		http.Error(writer, "authentication failed", http.StatusUnauthorized)
		return
	}
	if complete {
		// Authenticated
		s.cacheUserAuthentication(conv.Principal())
		if !s.createAndSetJwt(writer, conv.Principal(), conv.CredentialsSequence()) {
			return
		}
	}
	writer.Header().Set("Content-Type", "text/plain")
	if firstRequest {
		writer.Header().Set(ScramConversationIdHeaderName, conversationID)
	}
	if _, err := writer.Write(resp); err != nil {
		log.Errorf("failed to write response: %v", err)
	}
}

func (s *HTTPAPIServer) cacheUserAuthentication(principal string) {
	s.authenticatedUsersLock.Lock()
	defer s.authenticatedUsersLock.Unlock()
	// We use NanoTime rather than time.Time as time.Time can jump when clock is adjusted
	s.authenticatedUsers[principal] = arista.NanoTime()
}

func (s *HTTPAPIServer) checkAuthCache(principal string) bool {
	s.authenticatedUsersLock.RLock()
	defer s.authenticatedUsersLock.RUnlock()
	now := arista.NanoTime()
	authTime, ok := s.authenticatedUsers[principal]
	if !ok {
		return false
	}
	if now-authTime >= s.authCacheTimeout {
		delete(s.authenticatedUsers, principal)
		return false
	}
	return true
}

func (s *HTTPAPIServer) InvalidateAuthCache() {
	s.authenticatedUsersLock.Lock()
	defer s.authenticatedUsersLock.Unlock()
	s.authenticatedUsers = map[string]uint64{}
}

func (s *HTTPAPIServer) InvalidateInstanceID() {
	s.jwtPubKeysLock.Lock()
	defer s.jwtPubKeysLock.Unlock()
	s.jwtInstanceID = uuid.New().String()
}

func (s *HTTPAPIServer) GetCachedPubKeyInfos() map[int]JwtPublicKeyInfo {
	s.jwtPubKeysLock.Lock()
	defer s.jwtPubKeysLock.Unlock()
	res := map[int]JwtPublicKeyInfo{}
	for nodeID, info := range s.jwtPublicKeys {
		res[nodeID] = info
	}
	return res
}

func (s *HTTPAPIServer) GetInstanceID() string {
	s.jwtPubKeysLock.Lock()
	defer s.jwtPubKeysLock.Unlock()
	return s.jwtInstanceID
}

func (s *HTTPAPIServer) createAndSetJwt(writer http.ResponseWriter, principal string, credsSequence int) bool {
	// Generate JWT token
	jwt, err := s.createJwt(principal, credsSequence)
	if err != nil {
		maybeConvertAndSendError(err, writer)
		return false
	}
	writer.Header().Set(JwtHeaderName, jwt)
	return true
}

func (s *HTTPAPIServer) handlePubKey(writer http.ResponseWriter, request *http.Request) {
	if !checkHttp2(writer, request) {
		return
	}
	if request.Method != http.MethodGet {
		http.Error(writer, "the HTTP method must be a GET", http.StatusMethodNotAllowed)
		return
	}
	publicKeyBytes := x509.MarshalPKCS1PublicKey(&s.jwtPrivateKey.PublicKey)
	pkResp := pubKeyResponse{
		PublicKey:  base64.StdEncoding.EncodeToString(publicKeyBytes),
		InstanceID: s.jwtInstanceID,
	}
	bytes, err := json.Marshal(&pkResp)
	if err != nil {
		maybeConvertAndSendError(err, writer)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	if _, err := writer.Write(bytes); err != nil {
		log.Errorf("failed to write response: %v", err)
	}
}

type pubKeyResponse struct {
	PublicKey  string `json:"pub_key"`
	InstanceID string `json:"instance_id"`
}

func (s *HTTPAPIServer) handlePutUser(writer http.ResponseWriter, request *http.Request) {
	u := s.checkRequest(writer, request)
	if u == nil {
		return
	}
	if !s.maybeAuthenticate(writer, request) {
		return
	}
	body, ok := getBody(writer, request)
	if !ok {
		return
	}
	putUser := &PutUserRequest{}
	if err := json.Unmarshal(body, putUser); err != nil {
		http.Error(writer, "malformed JSON", http.StatusBadRequest)
		return
	}
	storedKey, err := base64.StdEncoding.DecodeString(putUser.StoredKey)
	if err != nil {
		http.Error(writer, "StoredKey not base64 encoded", http.StatusBadRequest)
		return
	}
	serverKey, err := base64.StdEncoding.DecodeString(putUser.ServerKey)
	if err != nil {
		http.Error(writer, "ServerKey not base64 encoded", http.StatusBadRequest)
		return
	}
	if err := s.scramManager.PutUserCredentials(putUser.UserName, storedKey, serverKey, putUser.Salt, putUser.Iterations); err != nil {
		maybeConvertAndSendError(err, writer)
	}
}

func (s *HTTPAPIServer) handleDeleteUser(writer http.ResponseWriter, request *http.Request) {
	u := s.checkRequest(writer, request)
	if u == nil {
		return
	}
	if !s.maybeAuthenticate(writer, request) {
		return
	}
	username, ok := getBody(writer, request)
	if !ok {
		return
	}
	if err := s.scramManager.DeleteUserCredentials(string(username)); err != nil {
		maybeConvertAndSendError(err, writer)
	}
}

type PutUserRequest struct {
	UserName   string `json:"username"`
	StoredKey  string `json:"stored_key"`
	ServerKey  string `json:"server_key"`
	Salt       string `json:"salt"`
	Iterations int    `json:"iterations"`
}

func (s *HTTPAPIServer) ListenAddress() string {
	return s.listenAddresses[s.nodeID]
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

func writeError(msg string, writer http.ResponseWriter, errorCode common.ErrCode) {
	// We write API errors with a suffix of "TEKxxxx" where xxxx is the error code, zero padded. The client
	// needs to know this information
	msg = fmt.Sprintf("TEK%04d - %s", errorCode, msg)
	http.Error(writer, msg, http.StatusBadRequest)
}

func writeInvalidStatementError(msg string, writer http.ResponseWriter) {
	writeError(msg, writer, common.StatementError)
}

func maybeConvertAndSendError(err error, writer http.ResponseWriter) {
	perr := maybeConvertError(err)
	var statusCode int
	if perr.Code == common.InternalError {
		statusCode = http.StatusInternalServerError
	} else {
		statusCode = http.StatusBadRequest
	}
	// We add TEK followed by the error code so the client can distinguish Tektite errors
	// e.g. client needs to know if it's an unavailable error in order to retry
	msg := fmt.Sprintf("TEK%04d - %s", perr.Code, perr.Msg)
	http.Error(writer, msg, statusCode)
}

func maybeConvertError(err error) common.TektiteError {
	var perr common.TektiteError
	if errwrap.As(err, &perr) {
		return perr
	}
	return common.NewInternalError(err)
}
