package auth

import (
	"crypto/hmac"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/query"
	"github.com/spirit-labs/tektite/types"
	"github.com/xdg-go/pbkdf2"
	"github.com/xdg-go/scram"
	"math"
	"sync"
)

type SystemSlabRegistor interface {
	RegisterSystemSlab(slabName string, persistorReceiverID int, deleterReceiverID int, slabID int,
		schema *opers.OperatorSchema, keyCols []string) error
}

type batchForwarder interface {
	ForwardBatch(batch *proc.ProcessBatch, replicate bool, completionFunc func(error))
}

var UserCredsColumnNames = []string{"username", "creds"}
var UserCredsColumnTypes = []types.ColumnType{types.ColumnTypeString, types.ColumnTypeBytes}

type ScramAuthType int

const (
	ScramAuthTypeSHA256 = 1
	ScramAuthTypeSHA512 = 2
	UserCredsSlabName   = "sys.usercreds"
	GetCredsQueryName   = "sys.get_creds"
	NumIters            = 4096
)

func NewScramManager(slabRegistor SystemSlabRegistor, forwarder batchForwarder, parser *parser.Parser,
	queryManager query.Manager, cfg *conf.Config, authType ScramAuthType) (*ScramManager, error) {
	sm := &ScramManager{
		slabRegistor: slabRegistor,
		forwarder:    forwarder,
		parser:       parser,
		queryManager: queryManager,
		userCredsSchema: opers.OperatorSchema{
			EventSchema:     evbatch.NewEventSchema(UserCredsColumnNames, UserCredsColumnTypes),
			PartitionScheme: opers.NewPartitionScheme("_default_", 1, false, cfg.ProcessorCount),
		},
		credsSequenceLocal: common.NewGRLocal(),
	}
	var hashGenFunc scram.HashGeneratorFcn
	var authTypeStr string
	if authType == ScramAuthTypeSHA256 {
		hashGenFunc = scram.SHA256
		authTypeStr = AuthenticationSaslScramSha256
	} else if authType == ScramAuthTypeSHA512 {
		hashGenFunc = scram.SHA512
		authTypeStr = AuthenticationSaslScramSha512
	} else {
		return nil, errors.New("invalid auth type")
	}
	scramServer, err := hashGenFunc.NewServer(sm.lookupCredential)
	if err != nil {
		return nil, err
	}
	sm.scramServer = scramServer
	sm.hashGenFunc = hashGenFunc
	sm.authType = authTypeStr
	return sm, nil
}

type ScramManager struct {
	slabRegistor       SystemSlabRegistor
	forwarder          batchForwarder
	userCredsSchema    opers.OperatorSchema
	scramServer        *scram.Server
	hashGenFunc        scram.HashGeneratorFcn
	authType           string
	parser             *parser.Parser
	queryManager       query.Manager
	credsSequenceLocal common.GRLocal
}

type userCreds struct {
	Salt      string
	Iters     int
	StoredKey string
	ServerKey string
	Sequence  int
}

func (s *ScramManager) Start() error {
	// Register system slab receivers that will receive the process batch and store or delete usercreds as appropriate
	if err := s.slabRegistor.RegisterSystemSlab(UserCredsSlabName, common.UserCredsReceiverID, common.UserCredsDeleteReceiverID,
		common.UserCredsSlabID, &s.userCredsSchema, []string{"username"}); err != nil {
		return err
	}
	prepare := parser.NewPrepareQueryDesc()
	if err := s.parser.Parse(fmt.Sprintf("prepare %s := (get $user_name:string from %s)",
		GetCredsQueryName, UserCredsSlabName), prepare); err != nil {
		return err
	}

	return s.queryManager.PrepareQuery(*prepare)
}

func (s *ScramManager) Stop() error {
	return nil
}

func (s *ScramManager) PutUserCredentials(username string, storedKey []byte, serverKey []byte, salt string, iters int) error {
	if iters != 4096 {
		return errors.New("invalid iterations")
	}
	creds, ok, err := s.lookupUserCreds(username)
	if err != nil {
		return err
	}
	sequence := 0
	if ok {
		// User already exists - this is a password update, increment the sequence from the existing creds
		sequence = creds.Sequence + 1
	}
	// Create a new userCreds struct - this is JSON serializable
	creds = userCreds{
		Salt:      salt,
		Iters:     iters,
		StoredKey: base64.StdEncoding.EncodeToString(storedKey),
		ServerKey: base64.StdEncoding.EncodeToString(serverKey),
		Sequence:  sequence,
	}
	// Serialize creds to JSON
	buff, err := json.Marshal(&creds)
	if err != nil {
		return err
	}
	// Convert to an event batch
	builders := evbatch.CreateColBuilders(s.userCredsSchema.EventSchema.ColumnTypes())
	builders[0].(*evbatch.StringColBuilder).Append(username)
	builders[1].(*evbatch.BytesColBuilder).Append(buff)
	evBatch := evbatch.NewBatchFromBuilders(s.userCredsSchema.EventSchema, builders...)
	processorID := s.userCredsSchema.PartitionProcessorMapping[0]
	// Create a process batch and send it to be stored
	processBatch := proc.NewProcessBatch(processorID, evBatch, common.UserCredsReceiverID, 0, -1)
	return s.sendCredsBatch(processBatch)
}

func (s *ScramManager) DeleteUserCredentials(username string) error {
	// We create a batch with just the key col
	columnTypes := []types.ColumnType{types.ColumnTypeString}
	schema := evbatch.NewEventSchema([]string{"username"}, columnTypes)
	colBuilders := evbatch.CreateColBuilders(columnTypes)
	colBuilders[0].(*evbatch.StringColBuilder).Append(username)
	batch := evbatch.NewBatchFromBuilders(schema, colBuilders...)
	processorID := s.userCredsSchema.PartitionProcessorMapping[0]
	processBatch := proc.NewProcessBatch(processorID, batch, common.UserCredsDeleteReceiverID, 0, -1)
	return s.sendCredsBatch(processBatch)
}

// AuthenticateWithUserPwd is used e.g. with HTTP basic auth, when we need to auth on the server with a username and
// password
func (s *ScramManager) AuthenticateWithUserPwd(username string, password string) (int, bool, error) {
	client, err := s.hashGenFunc.NewClient(username, password, "")
	if err != nil {
		return 0, false, err
	}
	// We just enact a SCRAM client / server conversation
	clConv := client.NewConversation()
	sConv := s.scramServer.NewConversation()
	r, err := clConv.Step("")
	if err != nil {
		return 0, false, err
	}
	r, err = sConv.Step(r)
	if err != nil {
		return 0, false, err
	}
	r, err = clConv.Step(r)
	if err != nil {
		return 0, false, err
	}
	r, err = sConv.Step(r)
	if err != nil {
		return 0, false, err
	}
	_, err = clConv.Step(r)
	if err != nil {
		return 0, false, err
	}
	if !clConv.Done() {
		panic("client conv not done")
	}
	seq, ok := s.credsSequenceLocal.Get()
	if !ok {
		panic("creds sequence local not set")
	}
	s.credsSequenceLocal.Delete()
	return seq.(int), clConv.Valid(), nil
}

func (s *ScramManager) AuthType() string {
	return s.authType
}

func CalcHash(hg scram.HashGeneratorFcn, b []byte) []byte {
	hash := hg()
	hash.Write(b)
	return hash.Sum(nil)
}

func CalcHMAC(hg scram.HashGeneratorFcn, key, buff []byte) []byte {
	hm := hmac.New(hg, key)
	hm.Write(buff)
	return hm.Sum(nil)
}

func (s *ScramManager) lookupCredential(username string) (scram.StoredCredentials, error) {
	creds, ok, err := s.lookupUserCreds(username)
	if err != nil {
		return scram.StoredCredentials{}, err
	}
	if !ok {
		return scram.StoredCredentials{}, errors.New("unknown user")
	}
	var storedCreds scram.StoredCredentials
	v, err := base64.StdEncoding.DecodeString(creds.StoredKey)
	if err != nil {
		return scram.StoredCredentials{}, err
	}
	storedCreds.StoredKey = v
	v, err = base64.StdEncoding.DecodeString(creds.ServerKey)
	if err != nil {
		return scram.StoredCredentials{}, err
	}
	storedCreds.ServerKey = v
	storedCreds.KeyFactors.Iters = creds.Iters
	storedCreds.KeyFactors.Salt = creds.Salt
	return storedCreds, nil
}

func (s *ScramManager) GetUserCredsSequence(username string) (int, bool, error) {
	creds, ok, err := s.lookupUserCreds(username)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}
	return creds.Sequence, true, nil
}

func (s *ScramManager) lookupUserCreds(username string) (userCreds, bool, error) {
	batch, err := s.executeQuerySingleResultBatch(GetCredsQueryName, []any{username})
	if err != nil {
		return userCreds{}, false, err
	}
	if batch.RowCount == 0 {
		return userCreds{}, false, nil
	}
	user := batch.Columns[0].(*evbatch.StringColumn).Get(0)
	if user != username {
		// sanity check
		return userCreds{}, false, errors.New("returned username does not match requested username")
	}
	credsBuff := batch.Columns[1].(*evbatch.BytesColumn).Get(0)
	var creds userCreds
	if err := json.Unmarshal(credsBuff, &creds); err != nil {
		return userCreds{}, false, err
	}
	// We store the creds sequence on a GR local - this is because we have no obvious way to pass it back directly to
	// the caller in the authentication process as this goes throug xdg-go library code.
	s.credsSequenceLocal.Set(creds.Sequence)
	return creds, true, nil
}

func (s *ScramManager) executeQuerySingleResultBatch(queryName string, args []any) (*evbatch.Batch, error) {
	ch := make(chan *evbatch.Batch, 1)
	_, err := s.queryManager.ExecutePreparedQueryWithHighestVersion(queryName, args, math.MaxInt64,
		func(last bool, numLastBatches int, batch *evbatch.Batch) error {
			if numLastBatches != 1 {
				panic("sys query must have 1 partition")
			}
			if !last {
				panic("must be one result batch")
			}
			ch <- batch
			return nil
		})
	if err != nil {
		return nil, err
	}
	return <-ch, nil
}

func (s *ScramManager) sendCredsBatch(batch *proc.ProcessBatch) error {
	ch := make(chan error, 1)
	// We ingest this with replication, so the batch will not be lost if failure occurs.
	s.forwarder.ForwardBatch(batch, true, func(err error) {
		ch <- err
	})
	return <-ch
}

func (s *ScramManager) NewConversation() (*ScramConversation, error) {
	return &ScramConversation{
		mgr:  s,
		conv: s.scramServer.NewConversation(),
	}, nil
}

type ScramConversation struct {
	mgr           *ScramManager
	principal     string
	conv          *scram.ServerConversation
	lock          sync.Mutex
	credsSequence int
	step          int
}

func (s *ScramConversation) Principal() string {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.principal
}

func (s *ScramConversation) CredentialsSequence() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.credsSequence
}

func (s *ScramConversation) Process(request []byte) (resp []byte, complete bool, failed bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	r, err := s.conv.Step(string(request))
	if err != nil {
		// Log auth failures at info
		log.Infof("Kafka API SASL SCRAM authentication failure: %v", err)
		return nil, false, true
	}
	if s.step == 0 {
		// GRLocal for sequence is set in the credentials lookup which occurs in the first step
		// The credentials sequence number should have been set using a GR local
		credsSequence, ok := s.mgr.credsSequenceLocal.Get()
		if !ok {
			panic("creds sequence not set")
		}
		s.mgr.credsSequenceLocal.Delete()
		s.credsSequence = credsSequence.(int)
	}
	s.step++
	if s.conv.Valid() {
		// Authentication succeeded
		s.principal = s.conv.Username()
	}
	return []byte(r), s.conv.Valid(), false
}

func AlgoForAuthType(authType string) scram.HashGeneratorFcn {
	var algo scram.HashGeneratorFcn
	if authType == AuthenticationSaslScramSha256 {
		algo = scram.SHA256
	} else if authType == AuthenticationSaslScramSha512 {
		algo = scram.SHA512
	} else {
		panic("invalid auth type")
	}
	return algo
}

func CreateUserScramCreds(password string, authType string) (storedKey []byte, serverKey []byte, salt string) {
	salt = uuid.New().String()
	// These would be computed on the client side and only the (username, storedKey, serverKey, salt, iters) are sent
	// over the wire
	hashFunc := AlgoForAuthType(authType)
	saltedPassword := pbkdf2.Key([]byte(password), []byte(salt), NumIters, hashFunc().Size(), hashFunc)
	clientKey := CalcHMAC(hashFunc, saltedPassword, []byte("Client Key"))
	storedKey = CalcHash(hashFunc, clientKey)
	serverKey = CalcHMAC(hashFunc, saltedPassword, []byte("Server Key"))
	return storedKey, serverKey, salt
}
