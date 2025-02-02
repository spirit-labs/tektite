package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/compress"
	"github.com/spirit-labs/tektite/iteration"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func NewCompactionWorkerService(cfg CompactionWorkerServiceConf, objStoreClient objstore.Client,
	clientFactory ControllerClientFactory, tableGetter sst.TableGetter, partHashes *parthash.PartitionHashes,
	retentions bool) *CompactionWorkerService {
	return &CompactionWorkerService{
		cfg:            cfg,
		objStoreClient: objStoreClient,
		clientFactory:  clientFactory,
		tableGetter:    tableGetter,
		retentions:     retentions,
		partHashes:     partHashes,
	}
}

type CompactionWorkerService struct {
	cfg                 CompactionWorkerServiceConf
	objStoreClient      objstore.Client
	clientFactory       ControllerClientFactory
	controllerClient    ControllerClient
	workers             []*compactionWorker
	tableGetter         sst.TableGetter
	partHashes          *parthash.PartitionHashes
	started             bool
	lock                sync.RWMutex
	gotPrefixRetentions bool
	retentions          bool
}

type CompactionWorkerServiceConf struct {
	MaxSSTableSize         int
	WorkerCount            int
	SSTableBucketName      string
	SSTablePushRetryDelay  time.Duration
	TableCompressionType   compress.CompressionType
	TopicCompactionMaxKeys int
}

func (c *CompactionWorkerServiceConf) Validate() error {
	return nil
}

func NewCompactionWorkerServiceConf() CompactionWorkerServiceConf {
	return CompactionWorkerServiceConf{
		WorkerCount:            4,
		SSTableBucketName:      "tektite-data",
		SSTablePushRetryDelay:  1 * time.Second,
		MaxSSTableSize:         16 * 1024 * 1024,
		TopicCompactionMaxKeys: 1000000,
	}
}

type ControllerClientFactory func() (ControllerClient, error)

type ControllerClient interface {
	ApplyLsmChanges(regBatch RegistrationBatch) error
	PollForJob() (CompactionJob, error)
	GetRetentionForTopic(topicID int) (time.Duration, bool, error)
	IsCompactedTopic(topicID int) (bool, error)
	QueryTablesInRange(keyStart []byte, keyEnd []byte) (OverlappingTables, error)
	Close() error
}

const workerRetryInterval = 100 * time.Millisecond

func (c *CompactionWorkerService) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	for i := 0; i < c.cfg.WorkerCount; i++ {
		worker := &compactionWorker{
			cws:             c,
			slabRetentions:  map[int]time.Duration{},
			compactedTopics: map[int64]bool{},
		}
		c.workers = append(c.workers, worker)
		worker.start()
	}
	c.started = true
	return nil
}

func (c *CompactionWorkerService) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return nil
	}
	var chans []chan struct{}
	for _, worker := range c.workers {
		// stop them in parallel - for quicker shutdown
		theWorker := worker
		ch := make(chan struct{}, 1)
		chans = append(chans, ch)
		common.Go(func() {
			theWorker.stop()
			ch <- struct{}{}
		})
	}
	for _, ch := range chans {
		<-ch
	}
	c.started = false
	return nil
}

type compactionWorker struct {
	cws             *CompactionWorkerService
	started         atomic.Bool
	stopWg          sync.WaitGroup
	slabRetentions  map[int]time.Duration
	controlClient   ControllerClient
	ccLock          sync.Mutex
	stopped         bool
	compactedTopics map[int64]bool
}

func (c *compactionWorker) controllerClient() (ControllerClient, error) {
	c.ccLock.Lock()
	defer c.ccLock.Unlock()
	if c.stopped {
		return nil, errors.New("worker is stopped")
	}
	if c.controlClient != nil {
		return c.controlClient, nil
	}
	cl, err := c.cws.clientFactory()
	if err != nil {
		return nil, err
	}
	c.controlClient = cl
	return cl, nil
}

func (c *compactionWorker) closeControllerClient(stop bool) {
	c.ccLock.Lock()
	defer c.ccLock.Unlock()
	if c.controlClient != nil {
		if err := c.controlClient.Close(); err != nil {
			log.Errorf("failed to close control client: %v", err)
		}
		c.controlClient = nil
	}
	if stop {
		c.stopped = true
	}
}

func (c *compactionWorker) GetSlabRetention(slabID int) (time.Duration, error) {
	// we cache the slab retentions
	ret, ok := c.slabRetentions[slabID]
	if ok {
		return ret, nil
	}
	cl, err := c.controllerClient()
	if err != nil {
		return 0, err
	}
	retention, exists, err := cl.GetRetentionForTopic(slabID)
	if err != nil {
		c.closeControllerClient(false)
		return 0, err
	}
	if !exists {
		return 0, errors.Errorf("compaction worker failed to get retention: topic with id %d does not exist", slabID)
	}
	c.slabRetentions[slabID] = retention
	return retention, nil
}

func (c *compactionWorker) start() {
	c.started.Store(true)
	c.stopWg.Add(1)
	common.Go(c.loop)
}

func (c *compactionWorker) stop() {
	c.closeControllerClient(true)
	c.started.Store(false)
	c.stopWg.Wait()
}

func (c *compactionWorker) loop() {
	defer func() {
		c.closeControllerClient(true)
		c.stopWg.Done()
	}()
	for c.started.Load() {
		cl, err := c.controllerClient()
		if err != nil {
			if common.IsUnavailableError(err) {
				time.Sleep(workerRetryInterval)
				continue
			}
			log.Errorf("failed to create controller client %v", err)
			return
		}
		job, err := cl.PollForJob()
		if err != nil {
			c.closeControllerClient(false)
			var tErr common.TektiteError
			if errwrap.As(err, &tErr) {
				if tErr.Code == common.Unavailable {
					// transient unavailability
					time.Sleep(workerRetryInterval)
					continue
				}
				if tErr.Code == common.CompactionPollTimeout {
					// OK
					continue
				}
			}
			if !strings.Contains(err.Error(), "connection closed") {
				log.Errorf("error in polling for compaction job: %v", err)
			} else {
				// Normal to get these when shutting down - don't spam the logs
				log.Debugf("error in polling for compaction job: %v", err)
			}
			continue
		}
		for c.started.Load() {
			registrations, deRegistrations, err := c.processJob(&job)
			if err == nil {
				regBatch := RegistrationBatch{
					Compaction:      true,
					JobID:           job.id,
					Registrations:   registrations,
					DeRegistrations: deRegistrations,
				}
				err := cl.ApplyLsmChanges(regBatch)
				if err == nil {
					// success
					break
				}
			}
			c.closeControllerClient(false)
			var tErr common.TektiteError
			if errwrap.As(err, &tErr) {
				if tErr.Code == common.Unavailable {
					// transient unavailability - retry after delay
					log.Warnf("transient error in processing compaction job. will retry: %v", err)
					time.Sleep(workerRetryInterval)
					continue
				}
			}
			log.Warnf("failed to process compaction:%v", err)
			break
		}
	}
}

func (c *compactionWorker) isCompactedTopic(topicID int64) (bool, error) {
	compacted, ok := c.compactedTopics[topicID]
	if ok {
		return compacted, nil
	}
	cl, err := c.controllerClient()
	if err != nil {
		return false, err
	}
	compacted, err = cl.IsCompactedTopic(int(topicID))
	if err != nil {
		return false, err
	}
	c.compactedTopics[topicID] = compacted
	return compacted, nil
}

func (c *compactionWorker) lastOffsetForKey(topicID int64, partitionID int64, key []byte, lastOffsetCache map[string]int64) (int64, bool, error) {
	lookupKey := make([]byte, 17+len(key))
	partHash, err := c.cws.partHashes.GetPartitionHash(int(topicID), int(partitionID))
	copy(lookupKey, partHash)
	lookupKey[16] = common.EntryTypeCompactedTopicLastOffsetForKey
	copy(lookupKey[17:], key)
	// First look in per job cache
	sKey := common.ByteSliceToStringZeroCopy(lookupKey)
	offset, ok := lastOffsetCache[sKey]
	if ok {
		return offset, true, nil
	}
	// Lookup in store
	rangeEnd := common.IncBigEndianBytes(lookupKey)
	cl, err := c.controllerClient()
	if err != nil {
		return 0, false, err
	}
	iter, err := CreateIteratorForKeyRange(lookupKey, rangeEnd, cl, c.cws.tableGetter)
	if err != nil {
		return 0, false, err
	}
	ok, kv, err := iter.Next()
	if err != nil {
		return 0, false, err
	}
	if !ok || !bytes.Equal(kv.Key[:len(lookupKey)], lookupKey) {
		// not found
		return 0, false, nil
	}
	val := common.RemoveValueMetadata(kv.Value)
	offset = int64(binary.BigEndian.Uint64(val))
	lastOffsetCache[sKey] = offset
	return offset, true, nil
}

func (c *compactionWorker) processJob(job *CompactionJob) ([]RegistrationEntry, []RegistrationEntry, error) {
	log.Debugf("compaction worker processing job %s", job.id)
	if job.isMove {
		if len(job.tables) != 1 {
			panic("move requires single run to move")
		}
		registrations, deRegistrations := c.moveTables(job.tables[0], job.levelFrom, job.preserveTombstones)
		return registrations, deRegistrations, nil
	}
	start := time.Now()
	tablesInMerge := 0
	for _, overlapping := range job.tables {
		tablesInMerge += len(overlapping)
	}
	var maxAddedTime uint64
	tablesToMerge := make([][]tableToMerge, len(job.tables))
	for i, overlapping := range job.tables {
		tables := make([]tableToMerge, len(overlapping))
		for j, t := range overlapping {
			ssTable, err := c.getSSTable(t.table.SSTableID)
			if err != nil {
				return nil, nil, err
			}
			if ssTable == nil {
				return nil, nil, errwrap.Errorf("cannot process compaction job as cannot find sstable: %v (%s)", t.table.SSTableID,
					string(t.table.SSTableID))
			}
			tables[j] = tableToMerge{
				deadVersionRanges: t.table.DeadVersionRanges,
				sst:               ssTable,
				id:                t.table.SSTableID,
			}
			if t.table.AddedTime > maxAddedTime {
				// We compute the maxAddedTime time - this is used for the AddedTime of any new sstables created.
				maxAddedTime = t.table.AddedTime
			}
		}
		tablesToMerge[i] = tables
	}
	mergeStart := time.Now()
	var retProvider RetentionProvider
	if c.cws.retentions {
		retProvider = c
	}
	// Per job cache
	lastOffsetCacheMap := map[string]int64{}
	infos, err := mergeSSTables(common.DataFormatV1, tablesToMerge, job.preserveTombstones,
		c.cws.cfg.MaxSSTableSize, job.lastFlushedVersion, job.id, retProvider, job.serverTime, c.isCompactedTopic,
		func(topicID int64, partitionID int64, key []byte) (int64, bool, error) {
			return c.lastOffsetForKey(topicID, partitionID, key, lastOffsetCacheMap)
		})
	if err != nil {
		return nil, nil, err
	}
	mergeDur := time.Now().Sub(mergeStart)
	log.Debugf("merge for job %s took %d ms", job.id, mergeDur.Milliseconds())
	// Now push the tables to the cloud store
	var ids []sst.SSTableID
	for _, info := range infos {
		id := []byte(sst.CreateSSTableId())
		log.Debugf("compaction job %s created sstable %v", job.id, id)
		ids = append(ids, id)
		for {
			tableBytes, err := info.sst.ToStorageBytes(c.cws.cfg.TableCompressionType)
			if err != nil {
				return nil, nil, err
			}
			// Add to object store
			err = objstore.PutWithTimeout(c.cws.objStoreClient, c.cws.cfg.SSTableBucketName, string(id),
				tableBytes, objstore.DefaultCallTimeout)
			if err == nil {
				break
			}
			if !common.IsUnavailableError(err) {
				return nil, nil, err
			}
			log.Warnf("failed to push compacted table, will retry: %v", err)
			time.Sleep(c.cws.cfg.SSTablePushRetryDelay)
		}
	}
	var tableEntries []TableEntry
	for i, info := range infos {
		tableEntries = append(tableEntries, TableEntry{
			SSTableID:        ids[i],
			RangeStart:       info.rangeStart,
			RangeEnd:         info.rangeEnd,
			MinVersion:       info.minVersion,
			MaxVersion:       info.maxVersion,
			DeleteRatio:      info.deleteRatio,
			NumEntries:       uint64(info.sst.NumEntries()),
			Size:             uint64(info.sst.SizeBytes()),
			AddedTime:        maxAddedTime,
			NumPrefixDeletes: info.numPrefixDeletes,
		})
		log.Debugf("compaction %s created table %v delete ratio %f preserve tombstones %t", job.id, ids[i], info.deleteRatio,
			job.preserveTombstones)
	}
	registrations, deRegistrations := changesToApply(tableEntries, job)
	dur := time.Now().Sub(start)
	log.Debugf("compaction job %s took %d ms to process on worker %p", job.id, dur.Milliseconds(), c)
	return registrations, deRegistrations, nil
}

func (c *compactionWorker) getSSTable(tableID sst.SSTableID) (*sst.SSTable, error) {
	buff, err := objstore.GetWithTimeout(c.cws.objStoreClient, c.cws.cfg.SSTableBucketName, string(tableID),
		objstore.DefaultCallTimeout)
	if err != nil {
		return nil, err
	}
	if len(buff) == 0 {
		return nil, errors.Errorf("cannot find sstable %s", tableID)
	}
	return sst.GetSSTableFromBytes(buff)
}

func changesToApply(newTables []TableEntry, job *CompactionJob) ([]RegistrationEntry, []RegistrationEntry) {
	var registrations []RegistrationEntry
	for _, newTable := range newTables {
		registrations = append(registrations, RegistrationEntry{
			Level:            job.levelFrom + 1,
			TableID:          newTable.SSTableID,
			KeyStart:         newTable.RangeStart,
			KeyEnd:           newTable.RangeEnd,
			MinVersion:       newTable.MinVersion,
			MaxVersion:       newTable.MaxVersion,
			DeleteRatio:      newTable.DeleteRatio,
			NumEntries:       newTable.NumEntries,
			TableSize:        newTable.Size,
			AddedTime:        newTable.AddedTime,
			NumPrefixDeletes: newTable.NumPrefixDeletes,
		})
	}
	var deRegistrations []RegistrationEntry
	for _, overlapping := range job.tables {
		for _, ssTable := range overlapping {
			deRegistrations = append(deRegistrations, RegistrationEntry{
				Level:            ssTable.level,
				TableID:          ssTable.table.SSTableID,
				KeyStart:         ssTable.table.RangeStart,
				KeyEnd:           ssTable.table.RangeEnd,
				MinVersion:       ssTable.table.MinVersion,
				MaxVersion:       ssTable.table.MaxVersion,
				DeleteRatio:      ssTable.table.DeleteRatio,
				NumEntries:       ssTable.table.NumEntries,
				TableSize:        ssTable.table.Size,
				NumPrefixDeletes: ssTable.table.NumPrefixDeletes,
			})
		}
	}
	return registrations, deRegistrations
}

// no overlap, so we can move tables directly from one level to another without merging anything
func (c *compactionWorker) moveTables(tables []tableToCompact, fromLevel int, preserveTombstones bool) ([]RegistrationEntry, []RegistrationEntry) {
	var registrations []RegistrationEntry
	for _, toCompact := range tables {
		if toCompact.table.DeleteRatio == float64(1) && !preserveTombstones {
			// The table is just deletes, and we're moving it into the last level - we can just drop it
		} else {
			registrations = append(registrations, RegistrationEntry{
				Level:       fromLevel + 1,
				TableID:     toCompact.table.SSTableID,
				KeyStart:    toCompact.table.RangeStart,
				KeyEnd:      toCompact.table.RangeEnd,
				MinVersion:  toCompact.table.MinVersion,
				MaxVersion:  toCompact.table.MaxVersion,
				DeleteRatio: toCompact.table.DeleteRatio,
				NumEntries:  toCompact.table.NumEntries,
				TableSize:   toCompact.table.Size,
				AddedTime:   toCompact.table.AddedTime,
			})
		}
	}
	var deRegistrations []RegistrationEntry
	for _, toCompact := range tables {
		deRegistrations = append(deRegistrations, RegistrationEntry{
			Level:       fromLevel,
			TableID:     toCompact.table.SSTableID,
			KeyStart:    toCompact.table.RangeStart,
			KeyEnd:      toCompact.table.RangeEnd,
			DeleteRatio: toCompact.table.DeleteRatio,
			NumEntries:  toCompact.table.NumEntries,
			TableSize:   toCompact.table.Size,
		})
	}
	return registrations, deRegistrations
}

type tableToMerge struct {
	deadVersionRanges []VersionRange
	sst               *sst.SSTable
	id                sst.SSTableID
}

func mergeSSTables(format common.DataFormat, tables [][]tableToMerge, preserveTombstones bool, maxTableSize int,
	lastFlushedVersion int64, jobID string, retentionProvider RetentionProvider, serverTime uint64,
	topicFunc isCompactedTopicFunc, keyFunc lastOffsetForKeyFunc) ([]ssTableInfo, error) {

	totEntries := 0
	chainIters := make([]iteration.Iterator, len(tables))
	for i, overlapping := range tables {
		sourceIters := make([]iteration.Iterator, len(overlapping))
		for j, table := range overlapping {
			iter, err := table.sst.NewIterator(nil, nil)
			if len(table.deadVersionRanges) > 0 {
				iter = NewRemoveDeadVersionsIterator(iter, table.deadVersionRanges)
			}
			if retentionProvider != nil {
				iter = NewRemoveExpiredEntriesIterator(iter, table.sst.CreationTime(), serverTime, retentionProvider)
			}
			if topicFunc != nil {
				iter = NewCompactedTopicIterator(iter, topicFunc, keyFunc)
			}
			sourceIters[j] = iter
			if err != nil {
				return nil, err
			}
			totEntries += table.sst.NumEntries()
		}
		chainIter := iteration.NewChainingIterator(sourceIters)
		chainIters[i] = chainIter
	}

	var minNonCompactableVersion uint64
	if lastFlushedVersion == -1 {
		// Nothing flushed yet, no versions can be overwritten
		minNonCompactableVersion = 0
	} else {
		// We can only overwrite older versions of same key if version < lastFlushedVersion
		// This ensures we don't lose any keys that we need after rolling back to lastFlushedVersion on failure
		minNonCompactableVersion = uint64(lastFlushedVersion)
	}
	mi, err := iteration.NewCompactionMergingIterator(chainIters, preserveTombstones, minNonCompactableVersion)
	if err != nil {
		return nil, err
	}
	log.Debugf("compaction created merging iterator: %p with minnoncompactable version %d for job %s",
		mi, minNonCompactableVersion, jobID)

	mergeResults := make([]common.KV, 0, totEntries)
	for {
		valid, curr, err := mi.Next()
		if err != nil {
			return nil, err
		}
		if !valid {
			break
		}
		mergeResults = append(mergeResults, curr)
	}
	return compactedToTableInfos(format, mergeResults, maxTableSize)
}

func compactedToTableInfos(format common.DataFormat, mergeResults []common.KV, maxTableSize int) ([]ssTableInfo, error) {
	size := 0
	iLast := 0
	var outTables []ssTableInfo
	var lastKeyNoVersion []byte
	for i, curr := range mergeResults {

		k := curr.Key
		size += 12 + 2*len(k) + len(curr.Value)

		isLast := i == len(mergeResults)-1

		keyNoVersion := k[:len(k)-8]
		if lastKeyNoVersion != nil && bytes.Equal(lastKeyNoVersion, keyNoVersion) && !isLast {
			// If keys only differ by version they must not be split across different sstables
			continue
		}
		lastKeyNoVersion = keyNoVersion

		// estimate of how much space an entry takes up in the sstable (data and index)

		if size >= maxTableSize || isLast {
			iter := common.NewKvSliceIterator(mergeResults[iLast : i+1])
			ssTable, smallestKey, largestKey, minVersion, maxVersion, err := sst.BuildSSTable(format, size, i+1-iLast,
				iter)
			if err != nil {
				return nil, err
			}
			outTables = append(outTables, ssTableInfo{
				sst:              ssTable,
				rangeStart:       smallestKey,
				rangeEnd:         largestKey,
				minVersion:       minVersion,
				maxVersion:       maxVersion,
				deleteRatio:      ssTable.DeleteRatio(),
				numPrefixDeletes: uint32(ssTable.NumPrefixDeletes()),
			})
			iLast = i + 1
			size = 0
		}
	}

	return outTables, nil
}

func validateRegBatch(regBatch RegistrationBatch, objStore objstore.Client, bucketName string) {
	for _, entry := range regBatch.Registrations {
		validateRegEntry(entry, objStore, bucketName)
	}
	for _, entry := range regBatch.DeRegistrations {
		validateRegEntry(entry, objStore, bucketName)
	}
}

func validateRegEntry(entry RegistrationEntry, objStore objstore.Client, bucketName string) {
	buff, err := objstore.GetWithTimeout(objStore, bucketName, string(entry.TableID), objstore.DefaultCallTimeout)
	if err != nil {
		panic(err)
	}
	table, err := sst.GetSSTableFromBytes(buff)
	if err != nil {
		panic(err)
	}
	iter, err := table.NewIterator(nil, nil)
	if err != nil {
		panic(err)
	}
	var firstKey []byte
	var lastKey []byte
	for {
		valid, curr, err := iter.Next()
		if err != nil {
			panic(err)
		}
		if !valid {
			break
		}
		if lastKey != nil && bytes.Compare(curr.Key, lastKey) <= 0 {
			panic(fmt.Sprintf("key out of order or duplicate %s", string(curr.Key)))
		}
		if firstKey == nil {
			firstKey = curr.Key
		}
		lastKey = curr.Key
	}
	if !bytes.Equal(firstKey, entry.KeyStart) {
		panic(fmt.Sprintf("first key %v (%s) and range start %v (%s) are not equal",
			firstKey, string(firstKey), entry.KeyStart, string(entry.KeyStart)))
	}
	if !bytes.Equal(lastKey, entry.KeyEnd) {
		panic(fmt.Sprintf("last key %v (%s) and range end %v (%s) are not equal",
			lastKey, string(lastKey), entry.KeyEnd, string(entry.KeyEnd)))
	}
}

type querier interface {
	QueryTablesInRange(keyStart []byte, keyEnd []byte) (OverlappingTables, error)
}

// CreateIteratorForKeyRange Duplicated from queryutils to prevent circular dependency - improve this
func CreateIteratorForKeyRange(keyStart []byte, keyEnd []byte, querier querier, tableGetter sst.TableGetter) (iteration.Iterator, error) {
	ids, err := querier.QueryTablesInRange(keyStart, keyEnd)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return iteration.EmptyIterator{}, nil
	}
	var iters []iteration.Iterator
	for _, nonOverLapIDs := range ids {
		if len(nonOverLapIDs) == 1 {
			info := nonOverLapIDs[0]
			iter, err := sst.NewLazySSTableIterator(info.ID, tableGetter, keyStart, keyEnd)
			if err != nil {
				return nil, err
			}
			iters = append(iters, iter)
		} else {
			itersInChain := make([]iteration.Iterator, len(nonOverLapIDs))
			for j, nonOverlapID := range nonOverLapIDs {
				iter, err := sst.NewLazySSTableIterator(nonOverlapID.ID, tableGetter, keyStart, keyEnd)
				if err != nil {
					return nil, err
				}
				itersInChain[j] = iter
			}
			iters = append(iters, iteration.NewChainingIterator(itersInChain))
		}
	}
	return iteration.NewMergingIterator(iters, false, math.MaxUint64)
}
