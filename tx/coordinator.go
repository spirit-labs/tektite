package tx

import (
	"encoding/binary"
	"errors"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/pusher"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/transport"
	"math"
	"sync"
)

type Coordinator struct {
	lock           sync.RWMutex
	controlClient  control.Client
	cfg            Conf
	tableGetter    sst.TableGetter
	membership     cluster.MembershipState
	connFactory    transport.ConnectionFactory
	connCachesLock sync.RWMutex
	connCaches     map[string]*transport.ConnectionCache
	txInfos        map[int64]*txInfo
}

type Conf struct {
	MaxPusherConnectionsPerAddress int
}

func NewCoordinator(cfg Conf, controlClient control.Client, tableGetter sst.TableGetter, connFactory transport.ConnectionFactory) *Coordinator {
	return &Coordinator{
		cfg:           cfg,
		controlClient: controlClient,
		tableGetter:   tableGetter,
		connFactory:   connFactory,
		txInfos:       make(map[int64]*txInfo),
		connCaches:    map[string]*transport.ConnectionCache{},
	}
}

func NewConf() Conf {
	return Conf{
		MaxPusherConnectionsPerAddress: DefaultMaxPusherConnectionsPerAddress,
	}
}

const (
	DefaultMaxPusherConnectionsPerAddress = 10
	producerIDSequenceName                = "pid"
	transactionMetadataVersion            = uint16(1)
)

func (c *Coordinator) MembershipChanged(_ int32, memberState cluster.MembershipState) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.membership = memberState
	return nil
}

func (c *Coordinator) HandleInitProducerID(req *kafkaprotocol.InitProducerIdRequest) (*kafkaprotocol.InitProducerIdResponse, error) {
	resp := &kafkaprotocol.InitProducerIdResponse{}
	if err := c.handleInitProducerID(req, resp); err != nil {
		var kerr kafkaError
		if errwrap.As(err, &kerr) {
			log.Warn(err)
			resp.ErrorCode = int16(kerr.errorCode)
		} else if common.IsUnavailableError(err) {
			log.Warn(err)
			resp.ErrorCode = int16(kafkaprotocol.ErrorCodeCoordinatorNotAvailable)
		} else {
			log.Error(err)
			resp.ErrorCode = int16(kafkaprotocol.ErrorCodeUnknownServerError)
		}
	}
	return resp, nil
}

func (c *Coordinator) handleInitProducerID(req *kafkaprotocol.InitProducerIdRequest, resp *kafkaprotocol.InitProducerIdResponse) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	transactionalID := common.SafeDerefStringPtr(req.TransactionalId)

	// TODO try and understand under what circumstances the client would provide producerID and epoch in the request
	// and when epoch needs to be verified

	if transactionalID == "" {
		// The producer is idempotent but not configured for transactions - just generate a pid and return it
		pid, err := c.generatePidWithRetry()
		if err != nil {
			return err
		}
		resp.ProducerId = pid
		resp.ProducerEpoch = 0
		return nil
	}

	// Load any existing record
	key := "t." + transactionalID
	storedState, err := c.loadTxInfo(transactionalID)
	if err != nil {
		return err
	}
	if storedState == nil {
		// First time transactionalID was used or no transactional id - generate a pid
		pid, err := c.generatePidWithRetry()
		if err != nil {
			return err
		}
		storedState = &txStoredState{
			pid:           pid,
			producerEpoch: 0,
		}
	} else {
		// TODO resolve any prepared but not committed/aborted transaction state

		// bump the kafka epoch
		if storedState.producerEpoch == math.MaxInt16 {
			storedState.producerEpoch = 0
		} else {
			storedState.producerEpoch++
		}
	}
	resp.ProducerId = storedState.pid
	resp.ProducerEpoch = storedState.producerEpoch
	// load the tektite epoch
	_, _, tektiteEpoch, err := c.controlClient.GetCoordinatorInfo(key)
	if err != nil {
		return err
	}
	var info txInfo
	info.storedState = *storedState
	info.tektiteEpoch = int64(tektiteEpoch)
	// Store the tx state
	if err := c.storeTxInfo(&info, key); err != nil {
		return err
	}
	c.addTxInfo(&info)
	return nil
}

func (c *Coordinator) addTxInfo(info *txInfo) {
	c.lock.RUnlock()
	c.lock.Lock()
	defer func() {
		c.lock.Unlock()
		c.lock.RLock()
	}()
	c.txInfos[info.storedState.pid] = info
}

type kafkaError struct {
	errorCode int
	errorMsg  string
}

func (k kafkaError) Error() string {
	return k.errorMsg
}

func (c *Coordinator) storeTxInfo(info *txInfo, key string) error {
	// The key is part_hash of "t." + transactionalID, value is serialized txStoredState
	partHash, err := parthash.CreateHash([]byte(key))
	if err != nil {
		return err
	}
	pusherAddress, ok := pusher.ChooseTablePusherForGroup(partHash, c.membership.Members)
	if !ok {
		// No available pushers
		return &kafkaError{errorCode: kafkaprotocol.ErrorCodeCoordinatorNotAvailable,
			errorMsg: "cannot store transaction as no members in cluster"}
	}
	kvKey := encoding.EncodeVersion(partHash, 0)
	value := make([]byte, 0, 32)
	value = binary.BigEndian.AppendUint16(value, transactionMetadataVersion)
	value = info.storedState.Serialize(value)
	pusherReq := pusher.DirectWriteRequest{
		WriterKey:   key,
		WriterEpoch: int(info.tektiteEpoch),
		KVs: []common.KV{
			{Key: kvKey, Value: value},
		},
	}
	buff := pusherReq.Serialize(createRequestBuffer())
	conn, err := c.getConnection(pusherAddress)
	if err != nil {
		return err
	}
	_, err = conn.SendRPC(transport.HandlerIDTablePusherDirectWrite, buff)
	return err
}

func createRequestBuffer() []byte {
	buff := make([]byte, 0, 128)                  // Initial size guess
	buff = binary.BigEndian.AppendUint16(buff, 1) // rpc version - currently 1
	return buff
}

func (c *Coordinator) generatePidWithRetry() (int64, error) {
	pid, err := c.controlClient.GenerateSequence(producerIDSequenceName)
	if err != nil {
		// FIXME - retry on unavailable
		return 0, err
	}
	return pid, nil
}

func (c *Coordinator) loadTxInfo(transactionalID string) (*txStoredState, error) {
	key := "t." + transactionalID
	keyStart, err := parthash.CreateHash([]byte(key))
	if err != nil {
		return nil, err
	}
	val, err := c.getLatestValueWithKey(keyStart)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, nil
	}
	version := binary.BigEndian.Uint16(val)
	if version != transactionMetadataVersion {
		return nil, errors.New("invalid transactional metadata version")
	}
	info := &txStoredState{}
	info.Deserialize(val, 2)
	return info, nil
}

// FIXME combine with other similar methods (e.g. in table pusher, but do a search for all usage of QueryTablesInRange)
func (c *Coordinator) getLatestValueWithKey(key []byte) ([]byte, error) {
	keyEnd := common.IncBigEndianBytes(key)
	queryRes, err := c.controlClient.QueryTablesInRange(key, keyEnd)
	if err != nil {
		return nil, err
	}
	if len(queryRes) == 0 {
		// no stored txinfo
		return nil, nil
	}
	// We take the first one as that's the most recent
	nonOverlapping := queryRes[0]
	res := nonOverlapping[0]
	tableID := res.ID
	sstTable, err := c.tableGetter(tableID)
	if err != nil {
		return nil, err
	}
	iter, err := sstTable.NewIterator(key, keyEnd)
	if err != nil {
		return nil, err
	}
	ok, kv, err := iter.Next()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	if len(kv.Value) == 0 {
		// tombstone
		return nil, nil
	}
	return kv.Value, nil
}

func (c *Coordinator) getConnection(address string) (transport.Connection, error) {
	connCache, ok := c.getConnCache(address)
	if !ok {
		connCache = c.createConnCache(address)
	}
	return connCache.GetConnection()
}

func (c *Coordinator) getConnCache(address string) (*transport.ConnectionCache, bool) {
	c.connCachesLock.RLock()
	defer c.connCachesLock.RUnlock()
	connCache, ok := c.connCaches[address]
	return connCache, ok
}

func (c *Coordinator) createConnCache(address string) *transport.ConnectionCache {
	c.connCachesLock.Lock()
	defer c.connCachesLock.Unlock()
	connCache, ok := c.connCaches[address]
	if ok {
		return connCache
	}
	connCache = transport.NewConnectionCache(address, c.cfg.MaxPusherConnectionsPerAddress, c.connFactory)
	c.connCaches[address] = connCache
	return connCache
}

type txInfo struct {
	storedState  txStoredState
	tektiteEpoch int64
}

type txStoredState struct {
	pid           int64
	producerEpoch int16
}

func (t *txStoredState) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint64(buff, uint64(t.pid))
	buff = binary.BigEndian.AppendUint16(buff, uint16(t.producerEpoch))
	return buff
}

func (t *txStoredState) Deserialize(buff []byte, offset int) int {
	t.pid = int64(binary.BigEndian.Uint64(buff[offset:]))
	offset = offset + 8
	t.producerEpoch = int16(binary.BigEndian.Uint16(buff[offset:]))
	offset = offset + 2
	return offset
}
