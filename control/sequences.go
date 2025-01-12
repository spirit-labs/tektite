package control

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"sync"
	"sync/atomic"
)

type Sequences struct {
	lock            sync.RWMutex
	stopping        atomic.Bool
	lsmHolder       lsmReceiver
	tableGetter     sst.TableGetter
	objStore        objstore.Client
	dataBucketName  string
	dataFormat      common.DataFormat
	blockSize       int64
	cachedSequences map[string]*Sequence
	kvWriter        kvWriter
}

type Sequence struct {
	sequences    *Sequences
	lock         sync.Mutex
	name         string
	key          []byte
	nextVal      int64
	maxCachedVal int64
}

func NewSequences(lsmHolder lsmReceiver, tableGetter sst.TableGetter, objStore objstore.Client,
	dataBucketName string, dataFormat common.DataFormat,
	blockSize int64, kvWriter kvWriter) *Sequences {
	return &Sequences{
		lsmHolder:       lsmHolder,
		tableGetter:     tableGetter,
		objStore:        objStore,
		dataBucketName:  dataBucketName,
		dataFormat:      dataFormat,
		blockSize:       blockSize,
		cachedSequences: map[string]*Sequence{},
		kvWriter:        kvWriter,
	}
}

type lsmReceiver interface {
	ApplyLsmChanges(regBatch lsm.RegistrationBatch, completionFunc func(error) error) error
	QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error)
}

func (s *Sequences) Start() {
}

func (s *Sequences) Stop() {
	s.stopping.Store(true)
}

func (s *Sequences) GenerateSequence(sequenceName string) (int64, error) {
	seq := s.getSequence(sequenceName)
	if seq == nil {
		var err error
		seq, err = s.createSequence(sequenceName)
		if err != nil {
			return 0, err
		}
	}
	return seq.generateSequence()
}

func (s *Sequences) getSequence(sequenceName string) *Sequence {
	s.lock.RLock()
	defer s.lock.RUnlock()
	seq, ok := s.cachedSequences[sequenceName]
	if !ok {
		return nil
	}
	return seq
}

func (s *Sequences) createSequence(sequenceName string) (*Sequence, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	seq, ok := s.cachedSequences[sequenceName]
	if ok {
		return seq, nil
	}
	hash, err := parthash.CreateHash([]byte("sequence." + sequenceName))
	if err != nil {
		return nil, err
	}
	key := make([]byte, 0, 16)
	key = append(key, hash...)
	seq = &Sequence{
		sequences:    s,
		name:         sequenceName,
		key:          key,
		maxCachedVal: -1,
	}
	s.cachedSequences[sequenceName] = seq
	return seq, nil
}

func (s *Sequence) generateSequence() (int64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.maxCachedVal == -1 {
		seq, err := s.loadLastSequence()
		if err != nil {
			return 0, err
		}
		s.maxCachedVal = seq
		s.nextVal = seq + 1
	}
	if s.nextVal > s.maxCachedVal {
		if err := s.reserveSequenceBlock(); err != nil {
			return 0, err
		}
	}
	seqVal := s.nextVal
	s.nextVal++
	return seqVal, nil
}

func (s *Sequence) loadLastSequence() (int64, error) {
	val, err := s.sequences.getLatestValueWithKey(s.key)
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return -1, nil
	}
	seq := binary.BigEndian.Uint64(val)
	return int64(seq), nil
}

func (s *Sequence) reserveSequenceBlock() error {
	reservedVal := s.maxCachedVal + s.sequences.blockSize
	value := make([]byte, 0, 9)
	value = binary.BigEndian.AppendUint64(value, uint64(reservedVal))
	value = common.AppendValueMetadata(value)
	// TODO should we channel all writes through table pusher instead of writing direct?
	key := make([]byte, 0, 24)
	key = append(key, s.key...)
	key = encoding.EncodeVersion(key, 0)
	if err := s.sequences.kvWriter([]common.KV{{
		Key:   key,
		Value: value,
	}}); err != nil {
		return err
	}
	s.maxCachedVal = reservedVal
	return nil
}

func (c *Sequences) getLatestValueWithKey(key []byte) ([]byte, error) {
	keyEnd := common.IncBigEndianBytes(key)
	queryRes, err := c.lsmHolder.QueryTablesInRange(key, keyEnd)
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
