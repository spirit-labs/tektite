package levels

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/errors"
	iteration2 "github.com/spirit-labs/tektite/iteration"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/retention"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/tabcache"
	"sync"
	"sync/atomic"
	"time"
)

func NewCompactionWorkerService(cfg *conf.Config, levelMgrClientFactory ClientFactory, tableCache *tabcache.Cache,
	objStoreClient objstore.Client) *CompactionWorkerService {
	return &CompactionWorkerService{
		cfg:                   cfg,
		levelMgrClientFactory: levelMgrClientFactory,
		tableCache:            tableCache,
		objStoreClient:        objStoreClient,
	}
}

type CompactionWorkerService struct {
	cfg                   *conf.Config
	levelMgrClientFactory ClientFactory
	tableCache            *tabcache.Cache
	objStoreClient        objstore.Client
	workers               []*compactionWorker
	started               bool
	lock                  sync.Mutex
	gotPrefixRetentions   bool
}

const workerRetryInterval = 500 * time.Millisecond

func (c *CompactionWorkerService) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	for i := 0; i < c.cfg.CompactionWorkerCount; i++ {
		worker := &compactionWorker{
			cws:    c,
			client: c.levelMgrClientFactory.CreateLevelManagerClient(),
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
	cws     *CompactionWorkerService
	started atomic.Bool
	stopWg  sync.WaitGroup
	client  Client
}

func (c *compactionWorker) start() {
	c.started.Store(true)
	c.stopWg.Add(1)
	common.Go(c.loop)
}

func (c *compactionWorker) stop() {
	c.started.Store(false)
	c.stopWg.Wait()
	if err := c.client.Stop(); err != nil {
		log.Warnf("failed to stop levelMgr client: %v", err)
	}
}

func (c *compactionWorker) loop() {
	defer c.stopWg.Done()
	for c.started.Load() {
		job, err := c.client.PollForJob()
		if err != nil {
			var tErr errors.TektiteError
			if errors.As(err, &tErr) {
				if tErr.Code == errors.Unavailable || tErr.Code == errors.LevelManagerNotLeaderNode {
					// transient unavailability
					time.Sleep(workerRetryInterval)
					continue
				}
				if tErr.Code == errors.CompactionPollTimeout {
					// OK
					continue
				}
			}
			log.Errorf("error in polling for compaction job: %v", err)
			continue
		}
		registrations, deRegistrations, err := c.processJob(job)
		if err != nil {
			log.Errorf("failure in processing job: %v", err)
			continue
		}
		regBatch := RegistrationBatch{
			Compaction:      true,
			JobID:           job.id,
			Registrations:   registrations,
			DeRegistrations: deRegistrations,
		}
		for c.started.Load() {
			err := c.client.ApplyChanges(regBatch)
			if err == nil {
				break
			}
			var tErr errors.TektiteError
			if errors.As(err, &tErr) {
				if tErr.Code == errors.Unavailable || tErr.Code == errors.LevelManagerNotLeaderNode {
					// transient unavailability - retry after delay
					time.Sleep(workerRetryInterval)
					continue
				}
			}
			log.Warnf("failed to apply compaction changes:%v", err)
			break
		}
	}
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
	tablesToMerge := make([][]tableToMerge, len(job.tables))
	for i, overlapping := range job.tables {
		tables := make([]tableToMerge, len(overlapping))
		for j, t := range overlapping {
			ssTable, err := c.cws.tableCache.GetSSTable(t.table.SSTableID)
			if err != nil {
				return nil, nil, err
			}
			if ssTable == nil {
				return nil, nil, errors.Errorf("cannot process compaction job as cannot find sstable: %v (%s)", t.table.SSTableID,
					string(t.table.SSTableID))
			}
			tables[j] = tableToMerge{
				prefixRetentions:  t.expiredPrefixes,
				deadVersionRanges: t.deadVersionRanges,
				sst:               ssTable,
				id:                t.table.SSTableID,
			}
		}
		tablesToMerge[i] = tables
	}
	mergeStart := time.Now()
	infos, err := mergeSSTables(common.DataFormatV1, tablesToMerge, job.preserveTombstones,
		c.cws.cfg.CompactionMaxSSTableSize, job.lastFlushedVersion, job.id)
	if err != nil {
		return nil, nil, err
	}
	mergeDur := time.Now().Sub(mergeStart)
	log.Debugf("merge for job %s took %d ms", job.id, mergeDur.Milliseconds())
	// Now push the tables to the cloud store
	var ids []sst.SSTableID
	for _, info := range infos {
		id := []byte(fmt.Sprintf("sst-%s", uuid.New().String()))
		log.Debugf("compaction job %s created sstable %v", job.id, id)
		ids = append(ids, id)
		for {
			tableBytes := info.sst.Serialize()
			// Add to object store
			err := c.cws.objStoreClient.Put(id, tableBytes)
			if err == nil {
				// Add to the local cache
				err = c.cws.tableCache.AddSSTable(id, info.sst)
				if err != nil {
					panic(err) // never happens
				}
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
			SSTableID:   ids[i],
			RangeStart:  info.rangeStart,
			RangeEnd:    info.rangeEnd,
			MinVersion:  info.minVersion,
			MaxVersion:  info.maxVersion,
			DeleteRatio: info.deleteRatio,
			NumEntries:  uint64(info.sst.NumEntries()),
			Size:        uint64(info.sst.SizeBytes()),
		})
		log.Debugf("compaction %s created table %v delete ratio %f preserve tombstones %t", job.id, ids[i], info.deleteRatio,
			job.preserveTombstones)
	}
	registrations, deRegistrations := changesToApply(tableEntries, job)
	dur := time.Now().Sub(start)
	log.Debugf("compaction job %s took %d ms to process on worker %p", job.id, dur.Milliseconds(), c)
	return registrations, deRegistrations, nil
}

func changesToApply(newTables []TableEntry, job *CompactionJob) ([]RegistrationEntry, []RegistrationEntry) {
	var registrations []RegistrationEntry
	for _, newTable := range newTables {
		registrations = append(registrations, RegistrationEntry{
			Level:       job.levelFrom + 1,
			TableID:     newTable.SSTableID,
			KeyStart:    newTable.RangeStart,
			KeyEnd:      newTable.RangeEnd,
			MinVersion:  newTable.MinVersion,
			MaxVersion:  newTable.MaxVersion,
			DeleteRatio: newTable.DeleteRatio,
			NumEntries:  newTable.NumEntries,
			TableSize:   newTable.Size,
		})
	}
	var deRegistrations []RegistrationEntry
	for _, overlapping := range job.tables {
		for _, ssTable := range overlapping {
			deRegistrations = append(deRegistrations, RegistrationEntry{
				Level:       ssTable.level,
				TableID:     ssTable.table.SSTableID,
				KeyStart:    ssTable.table.RangeStart,
				KeyEnd:      ssTable.table.RangeEnd,
				MinVersion:  ssTable.table.MinVersion,
				MaxVersion:  ssTable.table.MaxVersion,
				DeleteRatio: ssTable.table.DeleteRatio,
				NumEntries:  ssTable.table.NumEntries,
				TableSize:   ssTable.table.Size,
			})
		}
	}
	return registrations, deRegistrations
}

// no overlap, so we can move tables directly from one level to another without merging anything
func (c *compactionWorker) moveTables(tables []tableToCompact, fromLevel int, preserveTombstones bool) ([]RegistrationEntry, []RegistrationEntry) {
	var registrations []RegistrationEntry
	for _, tableToCompact := range tables {
		if tableToCompact.table.DeleteRatio == float64(1) && !preserveTombstones {
			// The table is just deletes, and we're moving it into the last level - we can just drop it
		} else {
			registrations = append(registrations, RegistrationEntry{
				Level:       fromLevel + 1,
				TableID:     tableToCompact.table.SSTableID,
				KeyStart:    tableToCompact.table.RangeStart,
				KeyEnd:      tableToCompact.table.RangeEnd,
				MinVersion:  tableToCompact.table.MinVersion,
				MaxVersion:  tableToCompact.table.MaxVersion,
				DeleteRatio: tableToCompact.table.DeleteRatio,
				NumEntries:  tableToCompact.table.NumEntries,
				TableSize:   tableToCompact.table.Size,
				// Note we do not set CreationTime as this is only used for newly added tables in order to determine
				// retention
			})
		}
	}
	var deRegistrations []RegistrationEntry
	for _, tableToCompact := range tables {
		deRegistrations = append(deRegistrations, RegistrationEntry{
			Level:       fromLevel,
			TableID:     tableToCompact.table.SSTableID,
			KeyStart:    tableToCompact.table.RangeStart,
			KeyEnd:      tableToCompact.table.RangeEnd,
			DeleteRatio: tableToCompact.table.DeleteRatio,
			NumEntries:  tableToCompact.table.NumEntries,
			TableSize:   tableToCompact.table.Size,
		})
	}
	return registrations, deRegistrations
}

type tableToMerge struct {
	prefixRetentions  []retention.PrefixRetention
	deadVersionRanges []VersionRange
	sst               *sst.SSTable
	id                sst.SSTableID
}

func mergeSSTables(format common.DataFormat, tables [][]tableToMerge, preserveTombstones bool, maxTableSize int,
	lastFlushedVersion int64, jobID string) ([]ssTableInfo, error) {

	totEntries := 0
	totDataSize := 0
	totTables := 0

	chainIters := make([]iteration2.Iterator, len(tables))
	for i, overlapping := range tables {
		sourceIters := make([]iteration2.Iterator, len(overlapping))
		for j, table := range overlapping {
			sstIter, err := table.sst.NewIterator(nil, nil)
			log.Debugf("mergingSSTables with dead version range %v", table.deadVersionRanges)
			if len(table.deadVersionRanges) > 0 {
				sstIter = NewRemoveDeadVersionsIterator(sstIter, table.deadVersionRanges)
			}
			if len(table.prefixRetentions) > 0 {
				// We pass in zero for creationTime and now, as we know that all the prefix retentions will have retention
				// set to zero anyway (expiration is calculated on the level manager),
				// so they are not used in the iterator
				sstIter = NewRemoveExpiredEntriesIterator(sstIter, table.prefixRetentions,
					0, 0)
			}

			sourceIters[j] = sstIter

			if err != nil {
				return nil, err
			}
			totEntries += table.sst.NumEntries()
			totDataSize += table.sst.SizeBytes()
			totTables++
		}
		chainIter := iteration2.NewChainingIterator(sourceIters)
		chainIters[i] = chainIter
	}

	numTables := float64(totDataSize) / float64(maxTableSize)
	numEntriesPerTableEstimate := int(float64(totEntries) / numTables)

	totMergedEntries := 0

	var minNonCompactableVersion uint64
	if lastFlushedVersion == -1 {
		// Nothing flushed yet, no versions can be overwritten
		minNonCompactableVersion = 0
	} else {
		// We can only overwrite older versions of same key if version < lastFlushedVersion
		// This ensures we don't lose any keys that we need after rolling back to lastFlushedVersion on failure
		minNonCompactableVersion = uint64(lastFlushedVersion)
	}
	mi, err := iteration2.NewCompactionMergingIterator(chainIters, preserveTombstones, minNonCompactableVersion)
	if err != nil {
		return nil, err
	}
	log.Debugf("compaction created merging iterator: %p with minnoncompactable version %d for job %s",
		mi, minNonCompactableVersion, jobID)
	var outTables []ssTableInfo
	for {
		v, err := mi.IsValid()
		if err != nil {
			return nil, err
		}
		if !v {
			break
		}
		meIter := newMaxSizeIterator(maxTableSize, mi)

		ssTable, smallestKey, largestKey, minVersion, maxVersion, err := sst.BuildSSTable(format, maxTableSize, numEntriesPerTableEstimate,
			meIter)
		if err != nil {
			return nil, err
		}
		outTables = append(outTables, ssTableInfo{
			sst:         ssTable,
			rangeStart:  smallestKey,
			rangeEnd:    largestKey,
			minVersion:  minVersion,
			maxVersion:  maxVersion,
			deleteRatio: ssTable.DeleteRatio(),
		})
		totMergedEntries += ssTable.NumEntries()
	}

	log.Debugf("compaction job merged %d entries into %d entries", totEntries, totMergedEntries)

	return outTables, nil
}

func validateRegBatch(regBatch RegistrationBatch, objStore objstore.Client) {
	for _, entry := range regBatch.Registrations {
		validateRegEntry(entry, objStore)
	}
	for _, entry := range regBatch.DeRegistrations {
		validateRegEntry(entry, objStore)
	}
}

func validateRegEntry(entry RegistrationEntry, objStore objstore.Client) {
	buff, err := objStore.Get(entry.TableID)
	if err != nil {
		panic(err)
	}
	table := &sst.SSTable{}
	table.Deserialize(buff, 0)
	iter, err := table.NewIterator(nil, nil)
	if err != nil {
		panic(err)
	}
	var firstKey []byte
	var lastKey []byte
	for {
		valid, err := iter.IsValid()
		if err != nil {
			panic(err)
		}
		if !valid {
			break
		}
		curr := iter.Current()
		if lastKey != nil && bytes.Compare(curr.Key, lastKey) <= 0 {
			panic(fmt.Sprintf("key out of order or duplicate %s", string(curr.Key)))
		}
		if firstKey == nil {
			firstKey = curr.Key
		}
		lastKey = curr.Key
		err = iter.Next()
		if err != nil {
			panic(err)
		}
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
