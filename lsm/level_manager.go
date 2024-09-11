package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/spirit-labs/tektite/asl/arista"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/sst"
	"sort"
	"strings"
	"sync"
	"time"
)

type Manager struct {
	compactionState
	lock                      sync.RWMutex
	started                   bool
	conf                      *conf.Config
	format                    common.MetadataFormat
	objStore                  objstore.Client
	masterRecord              *MasterRecord
	hasChanges                bool
	clusterVersions           map[string]int
	enableCompaction          bool
	validateOnEachStateChange bool
	pendingAddsQueue          []pendingL0Add
}

type pendingL0Add struct {
	regBatch       RegistrationBatch
	completionFunc func(error)
}

type deleteTableEntry struct {
	tableID   sst.SSTableID
	addedTime uint64
}

func NewManager(conf *conf.Config, cloudStore objstore.Client, enableCompaction bool,
	validateOnEachStateChange bool) *Manager {
	lm := &Manager{
		compactionState:           newCompactionState(),
		format:                    conf.RegistryFormat,
		objStore:                  cloudStore,
		conf:                      conf,
		clusterVersions:           map[string]int{},
		enableCompaction:          enableCompaction,
		validateOnEachStateChange: validateOnEachStateChange,
	}
	return lm
}

func (lm *Manager) Start(masterRecord *MasterRecord) error {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if lm.started {
		return nil
	}
	lm.masterRecord = masterRecord
	lm.scheduleTableDeleteTimer()
	// Maybe trigger a compaction as levels could be full
	if err := lm.maybeScheduleCompaction(); err != nil {
		return err
	}
	lm.started = true
	return nil
}

func (lm *Manager) Stop() error {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if !lm.started {
		return nil
	}
	lm.tableDeleteTimer.Stop()
	for _, inProg := range lm.inProgress {
		if inProg.timer != nil {
			inProg.timer.Stop()
		}
	}
	lm.started = false
	return nil
}

func (lm *Manager) QueryTablesInRange(keyStart []byte, keyEnd []byte) (OverlappingTables, error) {
	lm.lock.RLock()
	defer lm.lock.RUnlock()
	if !lm.started {
		return nil, errors.New("not started")
	}
	var overlapping OverlappingTables
	for level, entry := range lm.masterRecord.levelEntries {
		tables, err := lm.getOverlappingTables(keyStart, keyEnd, level, entry)
		if err != nil {
			return nil, err
		}
		if level == 0 {
			// Level 0 is overlapping
			for _, table := range tables {
				overlapping = append(overlapping, []QueryTableInfo{{ID: table.SSTableID, DeadVersions: table.DeadVersionRanges}})
			}
		} else if tables != nil {
			// Other levels are non overlapping
			tableInfos := make([]QueryTableInfo, len(tables))
			for i, table := range tables {
				tableInfos[i] = QueryTableInfo{
					ID:           table.SSTableID,
					DeadVersions: table.DeadVersionRanges,
				}
			}
			overlapping = append(overlapping, tableInfos)
		}
	}
	return overlapping, nil
}

func (lm *Manager) RegisterL0Tables(registrationBatch RegistrationBatch, completionFunc func(error)) {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if !lm.started {
		completionFunc(common.NewTektiteErrorf(common.Unavailable, "not started"))
		return
	}
	if !(len(registrationBatch.DeRegistrations) == 0 && len(registrationBatch.Registrations) == 1) ||
		registrationBatch.Registrations[0].Level != 0 || registrationBatch.Compaction {
		completionFunc(errwrap.Errorf("not an L0 registration %v", registrationBatch))
		return
	}
	// we check cluster version - this protects against network partition where node is lost but is still running, new
	// node takes over, but old store tries to register tables after new node is active.
	lowestVersion := lm.clusterVersions[registrationBatch.ClusterName]
	if registrationBatch.ClusterVersion < lowestVersion {
		completionFunc(common.NewTektiteErrorf(common.Unavailable, "registration batch version is too low"))
		return
	}
	lm.clusterVersions[registrationBatch.ClusterName] = registrationBatch.ClusterVersion
	if lm.getL0FreeSpace() >= 1 {
		log.Debugf("in lsm.Manager RegisterL0Tables - enough free space so applying now")
		completionFunc(lm.doApplyChangesAndMaybeCompact(registrationBatch))
		return
	}
	// queue the request
	log.Debugf("in lsm.Manager RegisterL0Tables - not enough free space so queuing- %d", lm.getL0FreeSpace())
	lm.pendingAddsQueue = append(lm.pendingAddsQueue, pendingL0Add{
		regBatch:       registrationBatch,
		completionFunc: completionFunc,
	})
}

func (lm *Manager) ApplyChanges(regBatch RegistrationBatch) error {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if !lm.started {
		return errors.New("not started")
	}
	if regBatch.Compaction {
		return lm.applyCompactionChanges(regBatch)
	}
	if err := lm.doApplyChanges(regBatch); err != nil {
		return err
	}
	log.Debugf("registered l0 table: %v now dumping, reprocess? %t", regBatch.Registrations[0].TableID)
	if log.DebugEnabled {
		lm.dump()
	}
	if lm.enableCompaction {
		return lm.maybeScheduleCompaction()
	}
	return nil
}

func (lm *Manager) ApplyChangesNoCheck(regBatch RegistrationBatch) error {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	return lm.doApplyChanges(regBatch)
}

func (lm *Manager) MaybeScheduleCompaction() error {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if !lm.started {
		return errors.New("not started")
	}
	return lm.maybeScheduleCompaction()
}

// RegisterDeadVersionRange - registers a range of versions as dead - versions in the dead range will be removed from
// the store via compaction, asynchronously. Note the version range is inclusive.
func (lm *Manager) RegisterDeadVersionRange(versionRange VersionRange, clusterName string, clusterVersion int) error {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if !lm.started {
		return errors.New("not started")
	}
	lowestVersion := lm.clusterVersions[clusterName]
	if clusterVersion < lowestVersion {
		// Note we send back RegisterDeadVersionWrongClusterVersion as we do not want the sender to retry - failure
		// should be cancelled
		return common.NewTektiteErrorf(common.RegisterDeadVersionWrongClusterVersion,
			"RegisterDeadVersionRange - cluster version is too low %d expected %d", clusterVersion, lowestVersion)
	}
	requiresMasterRecordUpdate := false
	// Find all tables that possibly might have entries in the dead range and update the table entry to include that
	for _, entry := range lm.masterRecord.levelEntries {
		if entry.maxVersion < versionRange.VersionStart {
			continue
		}
		for i, lte := range entry.tableEntries {
			te := lte.Get(entry)
			dontOverlapRight := versionRange.VersionStart > te.MaxVersion
			dontOverlapLeft := versionRange.VersionEnd < te.MinVersion
			overlaps := !(dontOverlapLeft || dontOverlapRight)
			if overlaps {
				dvrs := te.DeadVersionRanges
				// Make sure dvr is not already there
				exists := false
				for _, dvr := range dvrs {
					if dvr.VersionStart == versionRange.VersionStart && dvr.VersionEnd == versionRange.VersionEnd {
						exists = true
						break
					}
				}
				if !exists {
					dvrs = append(dvrs, versionRange)
					te.DeadVersionRanges = dvrs
					entry.SetAt(i, te)
					requiresMasterRecordUpdate = true
				}
			}
		}
	}
	// We update the cluster version - this prevents L0 tables with a dead version range being pushed after this has been
	// called - as we clear the local store when we get the new cluster version in proc mgr.
	lm.clusterVersions[clusterName] = clusterVersion
	if requiresMasterRecordUpdate {
		lm.hasChanges = true
	}
	return nil
}

func (lm *Manager) GetSlabRetention(slabID int) (time.Duration, error) {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if !lm.started {
		return 0, errors.New("not started")
	}
	ret := time.Duration(lm.masterRecord.slabRetentions[uint64(slabID)])
	return ret, nil
}

func (lm *Manager) RegisterSlabRetention(slabID int, retention time.Duration) error {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if !lm.started {
		return errors.New("not started")
	}
	lm.masterRecord.slabRetentions[uint64(slabID)] = uint64(retention)
	lm.masterRecord.version++
	lm.hasChanges = true
	return nil
}

func (lm *Manager) UnregisterSlabRetention(slabID int) error {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if !lm.started {
		return errors.New("not started")
	}
	delete(lm.masterRecord.slabRetentions, uint64(slabID))
	lm.masterRecord.version++
	lm.hasChanges = true
	return nil
}

func (lm *Manager) StoreLastFlushedVersion(lastFlushedVersion int64) error {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if !lm.started {
		return errors.New("not started")
	}
	lm.masterRecord.lastFlushedVersion = lastFlushedVersion
	lm.hasChanges = true
	return nil
}

func (lm *Manager) getOverlappingTables(keyStart []byte, keyEnd []byte, level int,
	levEntry *levelEntry) ([]*TableEntry, error) {
	numTableEntries := len(levEntry.tableEntries)
	// Note that keyEnd is exclusive!
	var tables []*TableEntry
	if level == 0 {
		// can't use binary search in L0 as not ordered
		for i := numTableEntries - 1; i >= 0; i-- {
			te := levEntry.tableEntries[i].Get(levEntry)
			// We must add the overlapping entries from newest to oldest
			if hasOverlap(keyStart, keyEnd, te.RangeStart, te.RangeEnd) {
				tables = append(tables, te)
			}
		}
	} else {
		// Find the start index of overlap with binary search
		startIndex := 0
		if keyEnd != nil {
			startIndex = sort.Search(numTableEntries, func(i int) bool {
				return bytes.Compare(levEntry.tableEntries[i].Get(levEntry).RangeEnd, keyStart) >= 0
			})
		}
		if startIndex == numTableEntries {
			// no overlap
			return nil, nil
		}
		for _, entry := range levEntry.tableEntries[startIndex:] {
			te := entry.Get(levEntry)
			if hasOverlap(keyStart, keyEnd, te.RangeStart, te.RangeEnd) {
				tables = append(tables, te)
			} else {
				break
			}
		}
	}
	return tables, nil
}

func (lm *Manager) levelMaxTablesTrigger(level int) int {
	if level == 0 {
		return lm.conf.L0CompactionTrigger
	}
	mt := lm.conf.L1CompactionTrigger
	for i := 1; i < level; i++ {
		mt *= lm.conf.LevelMultiplier
	}
	return mt
}

func (lm *Manager) getL0FreeSpace() int {
	return lm.conf.L0MaxTablesBeforeBlocking - lm.masterRecord.levelTableCounts[0]
}

func (lm *Manager) applyCompactionChanges(regBatch RegistrationBatch) error {
	jobExists := lm.jobInProgress(regBatch.JobID)
	if !jobExists {
		return common.NewTektiteErrorf(common.CompactionJobNotFound, "job not found %s", regBatch.JobID)
	}
	if err := lm.doApplyChanges(regBatch); err != nil {
		return err
	}
	registeredTables := make(map[string]struct{}, len(regBatch.Registrations))
	for _, registration := range regBatch.Registrations {
		registeredTables[string(registration.TableID)] = struct{}{}
	}
	tablesToDelete := make([]deleteTableEntry, 0, len(regBatch.DeRegistrations))
	now := arista.NanoTime()
	// For each deRegistration we add the table id to the tables to delete UNLESS the same table has also been
	// registered in the batch - this can happen when a table is moved from one level to the next - we do not want to
	// delete it then.
	l0DeRegs := 0
	for _, deRegistration := range regBatch.DeRegistrations {
		if deRegistration.Level == 0 {
			l0DeRegs++
		}
		_, registered := registeredTables[string(deRegistration.TableID)]
		if !registered {
			tablesToDelete = append(tablesToDelete, deleteTableEntry{
				tableID:   deRegistration.TableID,
				addedTime: now,
			})
		}
	}
	// ss-tables are deleted after a delay - this allows any queries currently in execution some time
	lm.tablesToDelete = append(lm.tablesToDelete, tablesToDelete...)
	if err := lm.compactionComplete(regBatch.JobID); err != nil {
		return err
	}
	lm.maybeDespatchPendingL0Adds()
	return nil
}

func (lm *Manager) maybeDespatchPendingL0Adds() {
	freeSpace := lm.getL0FreeSpace()
	log.Debugf("in levelmanager maybeDespatchPendingL0Adds, freespace is %d", freeSpace)
	if freeSpace <= 0 {
		return
	}
	toDespatch := freeSpace
	if len(lm.pendingAddsQueue) < toDespatch {
		toDespatch = len(lm.pendingAddsQueue)
	}
	log.Debugf("sending %d queueing l0 add", toDespatch)
	for i := 0; i < toDespatch; i++ {
		pending := lm.pendingAddsQueue[i]
		pending.completionFunc(lm.doApplyChangesAndMaybeCompact(pending.regBatch))
	}
	lm.pendingAddsQueue = lm.pendingAddsQueue[toDespatch:]
}

func (lm *Manager) getLastLevel() int {
	return len(lm.masterRecord.levelEntries) - 1
}

func (lm *Manager) doApplyChangesAndMaybeCompact(regBatch RegistrationBatch) error {
	if err := lm.doApplyChanges(regBatch); err != nil {
		return err
	}
	return lm.maybeScheduleCompaction()
}

func (lm *Manager) doApplyChanges(regBatch RegistrationBatch) error {
	if log.DebugEnabled {
		var dsb strings.Builder
		for _, dereg := range regBatch.DeRegistrations {
			dsb.WriteString(fmt.Sprintf("%v", []byte(dereg.TableID)))
			dsb.WriteRune(',')
		}
		var rsb strings.Builder
		for _, reg := range regBatch.Registrations {
			rsb.WriteString(fmt.Sprintf("%v", []byte(reg.TableID)))
			rsb.WriteRune(',')
		}
		log.Debugf("applychanges: deregistering: %s registering: %s", dsb.String(), rsb.String())
	}
	// We must process the de-registrations before registrations, or we can temporarily have overlapping keys
	if err := lm.applyDeRegistrations(regBatch.DeRegistrations); err != nil {
		return err
	}
	if err := lm.applyRegistrations(regBatch.Registrations); err != nil {
		return err
	}
	if lm.validateOnEachStateChange {
		if err := lm.Validate(false); err != nil {
			return err
		}
	}
	lm.masterRecord.version++
	lm.hasChanges = true
	return nil
}

func (lm *Manager) applyDeRegistrations(deRegistrations []RegistrationEntry) error { //nolint:gocyclo
	for _, deRegistration := range deRegistrations {
		if len(deRegistration.KeyStart) == 0 || len(deRegistration.KeyEnd) <= 8 {
			return errwrap.Errorf("deregistration, key start/end does not have a version: %v", deRegistration)
		}
		entry := lm.getLevelEntry(deRegistration.Level)
		if len(entry.tableEntries) == 0 {
			// Can occur if deRegistration applied more than once - we need to be idempotent
			continue
		}
		// Find the table entry in the level
		pos := getTableEntryForDeregistration(entry, deRegistration)
		if pos == -1 {
			// This can occur if deRegistration is applied more than once - we are idempotent.
			// E.g. during reprocessing or if the client resubmits after a network error but it had actually been
			// applied already
			continue
		}
		// Now remove it
		//newTableEntries := make([]*TableEntry, pos)
		//copy(newTableEntries, entry.tableEntries[:pos])
		//newTableEntries = append(newTableEntries, entry.tableEntries[pos+1:]...)
		entry.RemoveAt(pos)

		// Find the new overall start and end range
		var newStart, newEnd []byte
		if deRegistration.Level == 0 {
			// Level 0 is not ordered so we need to scan through all of them
			for _, lte := range entry.tableEntries {
				te := lte.Get(entry)
				if newStart == nil || bytes.Compare(te.RangeStart, newStart) < 0 {
					newStart = te.RangeStart
				}
				if newEnd == nil || bytes.Compare(te.RangeEnd, newEnd) > 0 {
					newEnd = te.RangeEnd
				}
			}
		} else if len(entry.tableEntries) > 0 {
			newStart = entry.tableEntries[0].Get(entry).RangeStart
			newEnd = entry.tableEntries[len(entry.tableEntries)-1].Get(entry).RangeEnd
		}
		entry.rangeStart = newStart
		entry.rangeEnd = newEnd

		// Update stats
		lm.masterRecord.levelTableCounts[deRegistration.Level]--
		lm.masterRecord.stats.TotTables--
		lm.masterRecord.stats.TotBytes -= int(deRegistration.TableSize)
		lm.masterRecord.stats.TotEntries -= int(deRegistration.NumEntries)
		levStats := lm.getLevelStats(deRegistration.Level)
		levStats.Tables--
		levStats.Bytes -= int(deRegistration.TableSize)
		levStats.Entries -= int(deRegistration.NumEntries)
	}
	return nil
}

// use pointers here - no need to copy
func (lm *Manager) getLevelEntry(level int) *levelEntry {
	lm.maybeResizeLevelEntries(level)
	return lm.masterRecord.levelEntries[level]
}

func (lm *Manager) maybeResizeLevelEntries(level int) {
	if level >= len(lm.masterRecord.levelEntries) {
		newEntries := make([]*levelEntry, level+1)
		copy(newEntries, lm.masterRecord.levelEntries)
		for j := len(lm.masterRecord.levelEntries); j < level+1; j++ {
			newEntries[j] = &levelEntry{}
		}
		lm.masterRecord.levelEntries = newEntries
	}
}

func (lm *Manager) applyRegistrations(registrations []RegistrationEntry) error { //nolint:gocyclo
	for _, registration := range registrations {
		log.Debugf("got reg keystart %v keyend %v", registration.KeyStart, registration.KeyEnd)
		if len(registration.KeyStart) == 0 || len(registration.KeyEnd) <= 8 {
			return errwrap.Errorf("registration, key start/end does not have a version: %v", registration)
		}
		log.Debugf("LevelManager registering new table %v (%s) from %s to %s in level %d",
			registration.TableID, string(registration.TableID), string(registration.KeyStart), string(registration.KeyEnd), registration.Level)
		// The new table entry that we're going to add
		tabEntry := &TableEntry{
			SSTableID:        registration.TableID,
			RangeStart:       registration.KeyStart,
			RangeEnd:         registration.KeyEnd,
			MinVersion:       registration.MinVersion,
			MaxVersion:       registration.MaxVersion,
			DeleteRatio:      registration.DeleteRatio,
			AddedTime:        registration.AddedTime,
			NumEntries:       registration.NumEntries,
			Size:             registration.TableSize,
			NumPrefixDeletes: registration.NumPrefixDeletes,
		}
		entry := lm.getLevelEntry(registration.Level)
		if registration.MaxVersion > entry.maxVersion {
			entry.maxVersion = registration.MaxVersion
		}
		// Note, that if the same table is added more than once by a client if it retries after transient error, this
		// does not matter, as when an iterator sees keys of the same version it will only take the first and ignore the
		// others
		if registration.Level == 0 {
			// We have overlapping keys in L0, so we just append to the last table entry
			entry.InsertAt(len(entry.tableEntries), tabEntry)
		} else {
			// L > 0
			// Table entries in these levels are non overlapping
			numTableEntries := len(entry.tableEntries)
			if numTableEntries == 0 {
				// The first table entry
				entry.InsertAt(0, tabEntry)
			} else {
				// Find the insert before point
				insertPoint := -1
				index := sort.Search(numTableEntries, func(i int) bool {
					te := entry.tableEntries[i].Get(entry)
					return bytes.Compare(registration.KeyEnd, te.RangeStart) < 0
				})
				if index < numTableEntries {
					insertPoint = index
				}
				if insertPoint > 0 {
					// check no overlap with previous table entry
					l := entry.tableEntries[insertPoint-1]
					te := l.Get(entry)
					if bytes.Compare(te.RangeEnd, registration.KeyStart) >= 0 {
						msg := fmt.Sprintf("got overlap with previous table id %s, prev key end %s inserting key start %s inserting key end %s",
							string(te.SSTableID), string(te.RangeEnd), string(registration.KeyStart),
							string(registration.KeyEnd))
						return common.Error(msg)
					}
				}
				if insertPoint == -1 {
					// insert at end
					insertPoint = len(entry.tableEntries)
				}
				// Insert the new entry in the table entries in the right place
				entry.InsertAt(insertPoint, tabEntry)
				//var newTableEntries []*TableEntry
				//if insertPoint >= 0 {
				//	left := entry.tableEntries[:insertPoint]
				//	right := entry.tableEntries[insertPoint:]
				//	newTableEntries = append(newTableEntries, left...)
				//	newTableEntries = append(newTableEntries, tabEntry)
				//	newTableEntries = append(newTableEntries, right...)
				//} else if insertPoint == -1 {
				//	newTableEntries = append(newTableEntries, entry.tableEntries...)
				//	newTableEntries = append(newTableEntries, tabEntry)
				//}
				//entry.tableEntries = newTableEntries
			}
		}
		// Update the ranges
		if entry.rangeStart == nil || bytes.Compare(registration.KeyStart, entry.rangeStart) < 0 {
			entry.rangeStart = registration.KeyStart
		}
		if bytes.Compare(registration.KeyEnd, entry.rangeEnd) > 0 {
			entry.rangeEnd = registration.KeyEnd
		}
		lm.masterRecord.levelTableCounts[registration.Level]++
		// Update stats
		if registration.Level == 0 {
			lm.masterRecord.stats.TablesIn++
			lm.masterRecord.stats.BytesIn += int(registration.TableSize)
			lm.masterRecord.stats.EntriesIn += int(registration.NumEntries)
		}
		lm.masterRecord.stats.TotTables++
		lm.masterRecord.stats.TotBytes += int(registration.TableSize)
		lm.masterRecord.stats.TotEntries += int(registration.NumEntries)
		levStats := lm.getLevelStats(registration.Level)
		levStats.Tables++
		levStats.Bytes += int(registration.TableSize)
		levStats.Entries += int(registration.NumEntries)
	}
	return nil
}

func (lm *Manager) getLevelStats(level int) *LevelStats {
	levStats, ok := lm.masterRecord.stats.LevelStats[level]
	if !ok {
		levStats = &LevelStats{}
		lm.masterRecord.stats.LevelStats[level] = levStats
	}
	return levStats
}

func (lm *Manager) scheduleTableDeleteTimer() {
	lm.tableDeleteTimer = time.AfterFunc(lm.conf.SSTableDeleteCheckInterval, func() {
		lm.maybeDeleteTables()
	})
}

func (lm *Manager) maybeDeleteTables() {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if !lm.started {
		return
	}
	pos := -1
	now := arista.NanoTime()
	for i, entry := range lm.tablesToDelete {
		age := time.Duration(now - entry.addedTime)
		if age < lm.conf.SSTableDeleteDelay {
			break
		}
		log.Debugf("deleted sstable %v", entry.tableID)
		if err := objstore.DeleteWithTimeout(lm.objStore, lm.conf.BucketName, string(entry.tableID), objstore.DefaultCallTimeout); err != nil {
			log.Errorf("failed to delete ss-table from cloud store: %v", err)
			break
		}
		pos = i
	}
	if pos != -1 {
		lm.tablesToDelete = lm.tablesToDelete[pos+1:]
	}
	lm.scheduleTableDeleteTimer()
}

func hasOverlap(keyStart []byte, keyEnd []byte, blockKeyStart []byte, blockKeyEnd []byte) bool {
	// Note! keyStart is inclusive, keyEnd is exclusive
	// LevelManager keyStart and keyEnd are inclusive!
	dontOverlapRight := bytes.Compare(keyStart, blockKeyEnd) > 0                  // Range starts after end of block
	dontOverlapLeft := keyEnd != nil && bytes.Compare(keyEnd, blockKeyStart) <= 0 // Range ends before beginning of block
	dontOverlap := dontOverlapLeft || dontOverlapRight
	return !dontOverlap
}

func (lm *Manager) DumpLevelInfo() {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	lm.dumpLevelInfo()
}

func (lm *Manager) dumpLevelInfo() {
	builder := strings.Builder{}
	for level := range lm.masterRecord.levelEntries {
		tableCount := lm.masterRecord.levelTableCounts[level]
		builder.WriteString(fmt.Sprintf("level:%d table_count:%d, ", level, tableCount))
	}
	log.Info(builder.String())
}

func (lm *Manager) Dump() {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	lm.dump()
}

func (lm *Manager) dump() {
	log.Infof("Dumping LevelManager ====================")
	for level, entry := range lm.masterRecord.levelEntries {
		log.Infof("Dumping level %d. Max version %d. Range Start: %v Range End: %v. There are %d table entries",
			level, entry.maxVersion, entry.rangeStart, entry.rangeEnd, len(entry.tableEntries))
		for _, lte := range entry.tableEntries {
			te := lte.Get(entry)
			log.Infof("table entry sstableid %v (%s) range start %s range end %s deleteRatio %.2f hasDeletes %t", te.SSTableID, string(te.SSTableID),
				string(te.RangeStart), string(te.RangeEnd), te.DeleteRatio, te.DeleteRatio > 0)
		}
	}
	for prefix := range lm.masterRecord.slabRetentions {
		log.Infof("prefix %v", prefix)
	}
}

func getTableEntryForDeregistration(levEntry *levelEntry, deRegistration RegistrationEntry) int {
	pos := -1
	if deRegistration.Level == 0 {
		// Level 0 is not ordered so we need to scan
		for i, lte := range levEntry.tableEntries {
			te := lte.Get(levEntry)
			if bytes.Equal(te.SSTableID, deRegistration.TableID) {
				pos = i
				break
			}
		}
		return pos
	}
	// Search in L > 0 - these are ordered so we can use binary search
	n := len(levEntry.tableEntries)
	// if the ids are equal their ranges will be as well
	pos = sort.Search(n, func(i int) bool {
		lte := levEntry.tableEntries[i].Get(levEntry)
		return bytes.Compare(lte.RangeStart, deRegistration.KeyStart) >= 0
	})
	if pos >= n {
		pos = -1
	}
	// it's also possible for multiple table entries to have the same range
	for i := pos; i < n; i++ {
		lte := levEntry.tableEntries[i]
		te := lte.Get(levEntry)
		if bytes.Equal(te.SSTableID, deRegistration.TableID) {
			pos = i
			break
		}
	}
	return pos
}

func (lm *Manager) GetObjectStore() objstore.Client {
	return lm.objStore
}

func (lm *Manager) GetLevelTableCounts() map[int]int {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	counts := map[int]int{}
	for level, count := range lm.masterRecord.levelTableCounts {
		counts[level] = count
	}
	return counts
}

func (lm *Manager) GetStats() Stats {
	lm.lock.RLock()
	defer lm.lock.RUnlock()
	statsCopy := lm.masterRecord.stats.copy()
	return *statsCopy
}

// Only used in testing
func (lm *Manager) reset() {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	lm.started = false
	lm.clusterVersions = map[string]int{}
	lm.masterRecord = nil
}

func (lm *Manager) GetLevelEntry(level int) *levelEntry {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	return lm.getLevelEntry(level)
}

// Used in testing only
func (lm *Manager) getMasterRecord() *MasterRecord {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	// Serialize/Deserialize so not to expose internal MasterRecord directly
	buff := lm.masterRecord.Serialize(nil)
	mr := &MasterRecord{}
	mr.Deserialize(buff, 0)
	return mr
}
