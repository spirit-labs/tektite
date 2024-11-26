package control

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/queryutils"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/transport"
)

type UserCredentials struct {
	Salt      string
	Iters     int
	StoredKey string
	ServerKey string
	Sequence  int
}

var credentialsPrefix []byte

func init() {
	pref, err := parthash.CreateHash([]byte("credentials"))
	if err != nil {
		panic(err)
	}
	credentialsPrefix = pref
}

func LookupUserCredentials(username string, querier queryutils.Querier, getter sst.TableGetter) (UserCredentials, bool, error) {
	key := createCredentialsKey(username)
	iter, err := queryutils.CreateIteratorForKeyRange(key, common.IncBigEndianBytes(key), querier, getter)
	if err != nil {
		return UserCredentials{}, false, err
	}
	ok, kv, err := iter.Next()
	if err != nil {
		return UserCredentials{}, false, err
	}
	if !ok || !bytes.Equal(key, kv.Key) {
		return UserCredentials{}, false, nil
	}
	val := common.RemoveValueMetadata(kv.Value)
	var creds UserCredentials
	if err := json.Unmarshal(val, &creds); err != nil {
		return UserCredentials{}, false, err
	}
	return creds, true, nil
}

func (c *Controller) lookupUserCreds(username string) (UserCredentials, bool, error) {
	return LookupUserCredentials(username, c.lsmHolder, c.tableGetter)
}

func (c *Controller) putUserCredentials(username string, storedKey []byte, serverKey []byte, salt string, iters int) error {
	c.credentialsLock.Lock()
	defer c.credentialsLock.Unlock()
	creds, ok, err := c.lookupUserCreds(username)
	if err != nil {
		return err
	}
	sequence := 0
	if ok {
		// User already exists - this is a password update, increment the sequence from the existing creds
		sequence = creds.Sequence + 1
	}
	// Create a new userCreds struct - this is JSON serializable
	creds = UserCredentials{
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
	key := createCredentialsKey(username)
	val := common.AppendValueMetadata(buff)
	kv := common.KV{
		Key:   key,
		Value: val,
	}
	return c.sendDirectWrite([]common.KV{kv}, credentialsPrefix)
}

func (c *Controller) sendDirectWrite(kvs []common.KV, partHash []byte) error {
	req := common.DirectWriteRequest{
		WriterKey:   controllerWriterKey,
		WriterEpoch: c.activateClusterVersion,
		KVs:         kvs,
	}
	reqBuff := req.Serialize(createRequestBuffer())
	pusherAddress, ok := cluster.ChooseMemberAddressForHash(credentialsPrefix, c.currentMembership.Members)
	if !ok {
		// No available pushers
		return common.NewTektiteErrorf(common.Unavailable, "cannot put user credentials as no members in cluster")
	}
	conn, err := c.connCaches.GetConnection(pusherAddress)
	if err != nil {
		return err
	}
	_, err = conn.SendRPC(transport.HandlerIDTablePusherDirectWrite, reqBuff)
	return err
}

func (c *Controller) deleteUserCredentials(username string) error {
	c.credentialsLock.Lock()
	defer c.credentialsLock.Unlock()
	_, ok, err := c.lookupUserCreds(username)
	if err != nil {
		return err
	}
	if !ok {
		return common.NewTektiteErrorf(common.NoSuchUser, "user does not exist")
	}
	key := createCredentialsKey(username)
	kv := common.KV{
		Key: key,
	}
	return c.sendDirectWrite([]common.KV{kv}, credentialsPrefix)
}

func createCredentialsKey(username string) []byte {
	var key []byte
	key = append(key, credentialsPrefix...)
	key = encoding.KeyEncodeString(key, username)
	return encoding.EncodeVersion(key, 0)
}
