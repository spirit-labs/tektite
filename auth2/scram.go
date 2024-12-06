package auth

import (
	"crypto/hmac"
	"encoding/base64"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/xdg-go/pbkdf2"
	"github.com/xdg-go/scram"
	"strings"
	"sync"
)

type ScramAuthType int

const (
	ScramAuthTypeSHA256 = 1
	ScramAuthTypeSHA512 = 2
	NumIters            = 4096
)

func NewScramManager(authType ScramAuthType, controlClientCache *control.ClientCache, tableGetter sst.TableGetter,
	allowNonceAsPrefix bool) (*ScramManager, error) {
	partHash, err := parthash.CreateHash([]byte("user.creds"))
	if err != nil {
		return nil, err
	}
	var hashGenFunc scram.HashGeneratorFcn
	if authType == ScramAuthTypeSHA256 {
		hashGenFunc = scram.SHA256
	} else if authType == ScramAuthTypeSHA512 {
		hashGenFunc = scram.SHA512
	} else {
		return nil, errors.New("invalid auth type")
	}
	sm := &ScramManager{
		controlClientCache: controlClientCache,
		tableGetter:        tableGetter,
		partHash:           partHash,
		credsSequenceLocal: common.NewGRLocal(),
		allowNonceAsPrefix: allowNonceAsPrefix,
	}
	scramServer, err := hashGenFunc.NewServer(sm.lookupCredential)
	if err != nil {
		return nil, err
	}
	sm.scramServer = scramServer
	sm.hashGenFunc = hashGenFunc
	return sm, nil
}

type ScramManager struct {
	partHash           []byte
	scramServer        *scram.Server
	controlClientCache *control.ClientCache
	tableGetter        sst.TableGetter
	hashGenFunc        scram.HashGeneratorFcn
	credsSequenceLocal common.GRLocal
	allowNonceAsPrefix bool
}

// AuthenticateWithUserPwd is used e.g. with SASL/PLAIN, when we need to auth on the server with a username and
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
	creds, ok, err := s.LookupUserCreds(username)
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
	creds, ok, err := s.LookupUserCreds(username)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}
	return creds.Sequence, true, nil
}

func (s *ScramManager) createKey(username string) []byte {
	var key []byte
	key = append(key, s.partHash...)
	return encoding.KeyEncodeString(key, username)
}

func (s *ScramManager) LookupUserCreds(username string) (control.UserCredentials, bool, error) {
	cl, err := s.controlClientCache.GetClient()
	if err != nil {
		return control.UserCredentials{}, false, err
	}
	creds, exists, err := control.LookupUserCredentials(username, cl, s.tableGetter)
	if err != nil {
		return control.UserCredentials{}, false, err
	}
	if !exists {
		return control.UserCredentials{}, false, nil
	}
	// We store the creds sequence on a GR local - this is because we have no obvious way to pass it back directly to
	// the caller in the authentication process as this goes throug xdg-go library code.
	s.credsSequenceLocal.Set(creds.Sequence)
	return creds, true, nil
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
	returnedNonce string
}

func (s *ScramConversation) Step() int {
	return s.step
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
	request = s.maybeTrimNonce(request)
	r, err := s.conv.Step(string(request))
	if err != nil {
		// Log auth failures at info
		log.Infof("Kafka API SASL SCRAM authentication failure: %v", err)
		return nil, false, true
	}
	if s.step == 0 {
		if s.mgr.allowNonceAsPrefix {
			// extract the nonce
			s.returnedNonce = extractNonce(r, 0)
		}
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

func extractNonce(resp string, index int) string {
	fields := strings.Split(resp, ",")
	nonceField := fields[index]
	if !strings.HasPrefix(nonceField, "r=") {
		panic("invalid scram field")
	}
	return strings.TrimPrefix(nonceField, "r=")
}

func (s *ScramConversation) maybeTrimNonce(request []byte) []byte {
	if s.mgr.allowNonceAsPrefix && s.step == 1 {
		nonce := extractNonce(string(request), 1)
		if strings.HasPrefix(nonce, s.returnedNonce) {
			// The nonce provided by the client is a prefix of the actual nonce returned by the server in step 1
			// According to the SCRAM RFC it should be exactly equal, but earlier versions of librdkafka had
			// a bug whereby the nonce sent back in step 2 had the server nonce as a prefix burt was not exactly equal.
			// To allow compatibility with older librdkafka versions we don't fail the handshake if the supplied nonce
			// has a prefix. By default we don't allow this but it can be configured on the agent if necessary.
			sRequest := string(request)
			fields := strings.Split(sRequest, ",")
			var newRequest strings.Builder
			for i, field := range fields {
				if i == 1 {
					// substitute the exact nonce so handshake will pass
					newRequest.WriteString("r=" + s.returnedNonce)
				} else {
					newRequest.WriteString(field)
				}
				if i != len(fields)-1 {
					newRequest.WriteRune(',')
				}
			}
			request = []byte(newRequest.String())
		}
	}
	return request
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
	// These are computed on the client side and only the (username, storedKey, serverKey, salt, iters) are sent
	// over the wire - password is not sent over the wire from client to agent
	hashFunc := AlgoForAuthType(authType)
	saltedPassword := pbkdf2.Key([]byte(password), []byte(salt), NumIters, hashFunc().Size(), hashFunc)
	clientKey := CalcHMAC(hashFunc, saltedPassword, []byte("Client Key"))
	storedKey = CalcHash(hashFunc, clientKey)
	serverKey = CalcHMAC(hashFunc, saltedPassword, []byte("Server Key"))
	return storedKey, serverKey, salt
}
