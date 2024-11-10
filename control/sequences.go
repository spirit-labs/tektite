package control

import (
	"encoding/binary"
	"errors"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"sync"
	"sync/atomic"
	"time"
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
	blockSize int64) *Sequences {
	return &Sequences{
		lsmHolder:       lsmHolder,
		tableGetter:     tableGetter,
		objStore:        objStore,
		dataBucketName:  dataBucketName,
		dataFormat:      dataFormat,
		blockSize:       blockSize,
		cachedSequences: map[string]*Sequence{},
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
	key, err := parthash.CreateHash([]byte("sequence." + sequenceName))
	if err != nil {
		return nil, err
	}
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
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(reservedVal))
	// TODO should we channel all writes through table pusher instead of writing direct?
	if err := s.sequences.writeKvDirect(common.KV{
		Key:   s.key,
		Value: value,
	}); err != nil {
		return err
	}
	s.maxCachedVal = reservedVal
	return nil
}

// TODO combine with similar in topicmeta manager?
func (s *Sequences) writeKvDirect(kv common.KV) error {
	iter := common.NewKvSliceIterator([]common.KV{kv})
	// Build ssTable
	table, smallestKey, largestKey, minVersion, maxVersion, err := sst.BuildSSTable(s.dataFormat, 0, 0, iter)
	if err != nil {
		return err
	}
	tableID := sst.CreateSSTableId()
	// Push ssTable to object store
	tableData := table.Serialize()
	if err := s.putWithRetry(tableID, tableData); err != nil {
		return err
	}
	// Register table with LSM
	regEntry := lsm.RegistrationEntry{
		Level:            0,
		TableID:          []byte(tableID),
		MinVersion:       minVersion,
		MaxVersion:       maxVersion,
		KeyStart:         smallestKey,
		KeyEnd:           largestKey,
		DeleteRatio:      table.DeleteRatio(),
		AddedTime:        uint64(time.Now().UnixMilli()),
		NumEntries:       uint64(table.NumEntries()),
		TableSize:        uint64(table.SizeBytes()),
		NumPrefixDeletes: uint32(table.NumPrefixDeletes()),
	}
	batch := lsm.RegistrationBatch{
		Registrations: []lsm.RegistrationEntry{regEntry},
	}
	ch := make(chan error, 1)
	if err := s.lsmHolder.ApplyLsmChanges(batch, func(err error) error {
		ch <- err
		return nil
	}); err != nil {
		return err
	}
	return <-ch
}

func (s *Sequences) putWithRetry(key string, value []byte) error {
	for {
		err := objstore.PutWithTimeout(s.objStore, s.dataBucketName, key, value, objStoreCallTimeout)
		if err == nil {
			return nil
		}
		if s.stopping.Load() {
			return errors.New("sequences is stopping")
		}
		if common.IsUnavailableError(err) {
			log.Warnf("Unable to write type info due to unavailability, will retry after delay: %v", err)
			time.Sleep(unavailabilityRetryDelay)
		}
	}
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
