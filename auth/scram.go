package auth

import (
	"crypto/hmac"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	queryManager query.Manager, cfg *conf.Config) (*ScramManager, error) {
	sm := &ScramManager{
		slabRegistor: slabRegistor,
		forwarder:    forwarder,
		parser:       parser,
		queryManager: queryManager,
		userCredsSchema: opers.OperatorSchema{
			EventSchema:     evbatch.NewEventSchema(UserCredsColumnNames, UserCredsColumnTypes),
			PartitionScheme: opers.NewPartitionScheme("_default_", 1, false, cfg.ProcessorCount),
		},
	}
	scramServerSha256, err := scram.SHA256.NewServer(sm.lookupCredential)
	if err != nil {
		return nil, err
	}
	sm.scramServerSha256 = scramServerSha256
	scramServerSha512, err := scram.SHA512.NewServer(sm.lookupCredential)
	if err != nil {
		return nil, err
	}
	sm.scramServerSha512 = scramServerSha512
	return sm, nil
}

type ScramManager struct {
	slabRegistor      SystemSlabRegistor
	forwarder         batchForwarder
	userCredsSchema   opers.OperatorSchema
	scramServerSha256 *scram.Server
	scramServerSha512 *scram.Server
	parser            *parser.Parser
	queryManager      query.Manager
}

type userCreds struct {
	Salt      string
	Iters     int
	StoredKey string
	ServerKey string
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
	// Convert to our userCreds struct which is JSON serializable
	creds := userCreds{
		Salt:      salt,
		Iters:     iters,
		StoredKey: base64.StdEncoding.EncodeToString(storedKey),
		ServerKey: base64.StdEncoding.EncodeToString(serverKey),
	}
	// Serialize creds to JSON
	buff, err := json.Marshal(&creds)
	if err != nil {
		return err
	}
	log.Infof("putting usercreds: %s", string(buff))
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
	batch, err := s.executeQuerySingleResultBatch(GetCredsQueryName, []any{username})
	if err != nil {
		return scram.StoredCredentials{}, err
	}
	user := batch.Columns[0].(*evbatch.StringColumn).Get(0)
	if user != username {
		// sanity check
		return scram.StoredCredentials{}, errors.New("returned username does not match requested username")
	}
	credsBuff := batch.Columns[1].(*evbatch.BytesColumn).Get(0)
	var creds userCreds
	if err := json.Unmarshal(credsBuff, &creds); err != nil {
		return scram.StoredCredentials{}, err
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

func (s *ScramManager) NewConversation(scramAuthType ScramAuthType) (*ScramConversion, error) {
	var conv *scram.ServerConversation
	if scramAuthType == ScramAuthTypeSHA256 {
		conv = s.scramServerSha256.NewConversation()
	} else if scramAuthType == ScramAuthTypeSHA512 {
		conv = s.scramServerSha512.NewConversation()
	} else {
		panic("unknown scram auth type")
	}
	return &ScramConversion{
		conv: conv,
	}, nil
}

type ScramConversion struct {
	principal string
	conv      *scram.ServerConversation
	lock      sync.Mutex
}

func (s *ScramConversion) Principal() string {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.principal
}

func (s *ScramConversion) Process(request []byte) (resp []byte, complete bool, failed bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	r, err := s.conv.Step(string(request))
	if err != nil {
		// Log auth failures at info
		log.Infof("Kafka API SASL SCRAM authentication failure: %v", err)
		return nil, false, true
	}
	if s.conv.Done() {
		// Authentication succeeded
		s.principal = s.conv.Username()
	}
	return []byte(r), s.conv.Done(), false
}
