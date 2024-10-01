package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/spirit-labs/tektite/asl/arista"
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
	cfg                       Conf
	format                    common.MetadataFormat
	objStore                  objstore.Client
	l0FreeCallback            func()
	masterRecord              *MasterRecord
	hasChanges                bool
	enableCompaction          bool
	validateOnEachStateChange bool
}

type Conf struct {
	RegistryFormat             common.MetadataFormat
	SSTableBucketName          string
	L0CompactionTrigger        int
	L1CompactionTrigger        int
	LevelMultiplier            int
	L0MaxTablesBeforeBlocking  int
	SSTableDeleteDelay         time.Duration
	SSTableDeleteCheckInterval time.Duration
	CompactionPollerTimeout    time.Duration
	CompactionJobTimeout       time.Duration
}

func NewConf() Conf {
	return Conf{
		RegistryFormat:             common.MetadataFormatV1,
		SSTableBucketName:          "tektite-data",
		L0CompactionTrigger:        4,
		L1CompactionTrigger:        4,
		LevelMultiplier:            10,
		L0MaxTablesBeforeBlocking:  10,
		SSTableDeleteDelay:         10 * time.Second,
		SSTableDeleteCheckInterval: 2 * time.Second,
		CompactionPollerTimeout:    1 * time.Second,
		CompactionJobTimeout:       30 * time.Second,
	}
}

func (c *Conf) Validate() error {
	// TODO
	return nil
}

func NewManager(objStore objstore.Client, l0FreeCallback func(), enableCompaction bool, validateOnEachStateChange bool,
	opts Conf) *Manager {
	lm := &Manager{
		compactionState:           newCompactionState(),
		objStore:                  objStore,
		l0FreeCallback:            l0FreeCallback,
		cfg:                       opts,
		enableCompaction:          enableCompaction,
		validateOnEachStateChange: validateOnEachStateChange,
	}
	return lm
}

func (m *Manager) Start(masterRecordBytes []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.started {
		return nil
	}
	if len(masterRecordBytes) > 0 {
		m.masterRecord = &MasterRecord{}
		m.masterRecord.Deserialize(masterRecordBytes, 0)
	} else {
		m.masterRecord = NewMasterRecord(m.cfg.RegistryFormat)
	}
	m.scheduleTableDeleteTimer()
	// Maybe trigger a compaction as levels could be full
	if err := m.maybeScheduleCompaction(); err != nil {
		return err
	}
	m.started = true
	return nil
}

func (m *Manager) Stop() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return nil
	}
	m.tableDeleteTimer.Stop()
	for _, inProg := range m.inProgress {
		if inProg.timer != nil {
			inProg.timer.Stop()
		}
	}
	m.started = false
	return nil
}

func (m *Manager) QueryTablesInRange(keyStart []byte, keyEnd []byte) (OverlappingTables, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if !m.started {
		return nil, errors.New("not started")
	}
	var overlapping OverlappingTables
	for level, entry := range m.masterRecord.levelEntries {
		tables, err := m.getOverlappingTables(keyStart, keyEnd, level, entry)
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

func (m *Manager) GetTablesForHighestKeyWithPrefix(prefix []byte) ([]sst.SSTableID, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if !m.started {
		return nil, errors.New("not started")
	}
	var tableIDs []sst.SSTableID
	for level, entry := range m.masterRecord.levelEntries {
		entries, err := m.getTablesForHighestKeyWithPrefix(prefix, level, entry)
		if err != nil {
			return nil, err
		}
		tableIDs = append(tableIDs, entries...)
	}
	return tableIDs, nil
}

func (m *Manager) ApplyChanges(regBatch RegistrationBatch, noCompaction bool) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return false, errors.New("not started")
	}
	l0FreeSpaceBefore := m.getL0FreeSpace()
	isL0Reg := false
	for _, reg := range regBatch.Registrations {
		if reg.Level == 0 {
			isL0Reg = true
			break
		}
	}
	if isL0Reg && m.getL0FreeSpace() <= 0 {
		// L0 is full - push back
		return false, nil
	}
	if regBatch.Compaction {
		if err := m.applyCompactionChanges(regBatch); err != nil {
			return false, err
		}
	} else {
		if err := m.doApplyChanges(regBatch); err != nil {
			return false, err
		}
		if !noCompaction && m.enableCompaction && (isL0Reg || regBatch.Compaction) {
			if err := m.maybeScheduleCompaction(); err != nil {
				return false, err
			}
		}
	}
	if l0FreeSpaceBefore <= 0 && m.getL0FreeSpace() > 0 {
		// Space has been freed in L0, call the callback so any waiting writers can retry their writes
		go m.l0FreeCallback()
	}
	return true, nil
}

func (m *Manager) MaybeScheduleCompaction() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return errors.New("not started")
	}
	return m.maybeScheduleCompaction()
}

// RegisterDeadVersionRange - registers a range of versions as dead - versions in the dead range will be removed from
// the store via compaction, asynchronously. Note the version range is inclusive.
func (m *Manager) RegisterDeadVersionRange(versionRange VersionRange) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return errors.New("not started")
	}
	requiresMasterRecordUpdate := false
	// Find all tables that possibly might have entries in the dead range and update the table entry to include that
	for _, entry := range m.masterRecord.levelEntries {
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
	if requiresMasterRecordUpdate {
		m.hasChanges = true
	}
	return nil
}

func (m *Manager) GetSlabRetention(slabID int) (time.Duration, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return 0, errors.New("not started")
	}
	ret := time.Duration(m.masterRecord.slabRetentions[uint64(slabID)])
	return ret, nil
}

func (m *Manager) RegisterSlabRetention(slabID int, retention time.Duration) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return errors.New("not started")
	}
	m.masterRecord.slabRetentions[uint64(slabID)] = uint64(retention)
	m.masterRecord.version++
	m.hasChanges = true
	return nil
}

func (m *Manager) UnregisterSlabRetention(slabID int) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return errors.New("not started")
	}
	delete(m.masterRecord.slabRetentions, uint64(slabID))
	m.masterRecord.version++
	m.hasChanges = true
	return nil
}

func (m *Manager) StoreLastFlushedVersion(lastFlushedVersion int64) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return errors.New("not started")
	}
	m.masterRecord.lastFlushedVersion = lastFlushedVersion
	m.hasChanges = true
	return nil
}

func (m *Manager) GetMasterRecordBytes() []byte {
	m.lock.Lock()
	defer m.lock.Unlock()
	buffSize := m.masterRecord.SerializedSize()
	buff := make([]byte, 0, buffSize)
	buff = m.masterRecord.Serialize(buff)
	if len(buff) != buffSize {
		log.Warnf("master record serialized size calculation is incorrect - this could result in unnecessary buffer reallocation")
	}
	return buff
}

func (m *Manager) getTablesForHighestKeyWithPrefix(prefix []byte, level int, levEntry *levelEntry) ([]sst.SSTableID, error) {
	// Find all tables that might contain the highest key with the specified prefix
	nextPrefix := common.IncBigEndianBytes(prefix)
	numTableEntries := len(levEntry.tableEntries)
	var tables []sst.SSTableID
	if level == 0 {
		// can't use binary search in L0 as not ordered
		for i := numTableEntries - 1; i >= 0; i-- {
			te := levEntry.tableEntries[i].Get(levEntry)
			// We must add the overlapping entries from newest to oldest
			if HasOverlap(prefix, nextPrefix, te.RangeStart, te.RangeEnd) {
				tables = append(tables, te.SSTableID)
			}
		}
	} else {
		// Find the highest table index - where we know any tables with a greater index cannot contain the prefix
		highestIndex := sort.Search(numTableEntries, func(i int) bool {
			return bytes.Compare(levEntry.tableEntries[i].Get(levEntry).RangeEnd, nextPrefix) >= 0
		})
		if highestIndex == numTableEntries {
			// It might be in the last one
			highestIndex = numTableEntries - 1
		}
		for highestIndex >= 0 {
			te := levEntry.tableEntries[highestIndex].Get(levEntry)
			if bytes.Compare(te.RangeEnd, prefix) < 0 {
				// No keys with prefix
				return nil, nil
			}
			tablePrefixStart := te.RangeStart[:len(prefix)]
			if bytes.Compare(tablePrefixStart, prefix) > 0 {
				// if the first key in the table has a prefix which is greater than the prefix we are looking for
				// then the largest key cannot be in this table, but it might be in the table to the left, so we continue there
				highestIndex--
				continue
			}
			//log.Infof("found in table with keystart %v id %v", te.RangeStart, te.SSTableID)
			// The last key in the table is greater or equal to the next prefix
			tables = append(tables, te.SSTableID)
			break
		}
	}
	return tables, nil
}

func (m *Manager) getOverlappingTables(keyStart []byte, keyEnd []byte, level int,
	levEntry *levelEntry) ([]*TableEntry, error) {
	numTableEntries := len(levEntry.tableEntries)
	// Note that keyEnd is exclusive!
	var tables []*TableEntry
	if level == 0 {
		// can't use binary search in L0 as not ordered
		for i := numTableEntries - 1; i >= 0; i-- {
			te := levEntry.tableEntries[i].Get(levEntry)
			// We must add the overlapping entries from newest to oldest
			if HasOverlap(keyStart, keyEnd, te.RangeStart, te.RangeEnd) {
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
			if HasOverlap(keyStart, keyEnd, te.RangeStart, te.RangeEnd) {
				tables = append(tables, te)
			} else {
				break
			}
		}
	}
	return tables, nil
}

func (m *Manager) levelMaxTablesTrigger(level int) int {
	if level == 0 {
		return m.cfg.L0CompactionTrigger
	}
	mt := m.cfg.L1CompactionTrigger
	for i := 1; i < level; i++ {
		mt *= m.cfg.LevelMultiplier
	}
	return mt
}

func (m *Manager) getL0FreeSpace() int {
	return m.cfg.L0MaxTablesBeforeBlocking - m.masterRecord.levelTableCounts[0]
}

func (m *Manager) applyCompactionChanges(regBatch RegistrationBatch) error {
	jobExists := m.jobInProgress(regBatch.JobID)
	if !jobExists {
		return common.NewTektiteErrorf(common.CompactionJobNotFound, "job not found %s", regBatch.JobID)
	}
	if err := m.doApplyChanges(regBatch); err != nil {
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
	m.tablesToDelete = append(m.tablesToDelete, tablesToDelete...)
	if err := m.compactionComplete(regBatch.JobID); err != nil {
		return err
	}
	return nil
}

func (m *Manager) getLastLevel() int {
	return len(m.masterRecord.levelEntries) - 1
}

func (m *Manager) doApplyChanges(regBatch RegistrationBatch) error {
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
	if err := m.applyDeRegistrations(regBatch.DeRegistrations); err != nil {
		return err
	}
	if err := m.applyRegistrations(regBatch.Registrations); err != nil {
		return err
	}
	if m.validateOnEachStateChange {
		if err := m.Validate(false); err != nil {
			return err
		}
	}
	m.masterRecord.version++
	m.hasChanges = true
	return nil
}

func (m *Manager) applyDeRegistrations(deRegistrations []RegistrationEntry) error { //nolint:gocyclo
	for _, deRegistration := range deRegistrations {
		if len(deRegistration.KeyStart) == 0 || len(deRegistration.KeyEnd) <= 8 {
			return errwrap.Errorf("deregistration, key start/end does not have a version: %v", deRegistration)
		}
		entry := m.levelEntry(deRegistration.Level)
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
		m.masterRecord.levelTableCounts[deRegistration.Level]--
		m.masterRecord.stats.TotTables--
		m.masterRecord.stats.TotBytes -= int(deRegistration.TableSize)
		m.masterRecord.stats.TotEntries -= int(deRegistration.NumEntries)
		levStats := m.getLevelStats(deRegistration.Level)
		levStats.Tables--
		levStats.Bytes -= int(deRegistration.TableSize)
		levStats.Entries -= int(deRegistration.NumEntries)
	}
	return nil
}

func (m *Manager) levelEntry(level int) *levelEntry {
	m.maybeResizeLevelEntries(level)
	return m.masterRecord.levelEntries[level]
}

func (m *Manager) maybeResizeLevelEntries(level int) {
	if level >= len(m.masterRecord.levelEntries) {
		newEntries := make([]*levelEntry, level+1)
		copy(newEntries, m.masterRecord.levelEntries)
		for j := len(m.masterRecord.levelEntries); j < level+1; j++ {
			newEntries[j] = &levelEntry{}
		}
		m.masterRecord.levelEntries = newEntries
	}
}

func (m *Manager) applyRegistrations(registrations []RegistrationEntry) error { //nolint:gocyclo
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
		entry := m.levelEntry(registration.Level)
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
			}
		}
		// Update the ranges
		if entry.rangeStart == nil || bytes.Compare(registration.KeyStart, entry.rangeStart) < 0 {
			entry.rangeStart = registration.KeyStart
		}
		if bytes.Compare(registration.KeyEnd, entry.rangeEnd) > 0 {
			entry.rangeEnd = registration.KeyEnd
		}
		m.masterRecord.levelTableCounts[registration.Level]++
		// Update stats
		if registration.Level == 0 {
			m.masterRecord.stats.TablesIn++
			m.masterRecord.stats.BytesIn += int(registration.TableSize)
			m.masterRecord.stats.EntriesIn += int(registration.NumEntries)
		}
		m.masterRecord.stats.TotTables++
		m.masterRecord.stats.TotBytes += int(registration.TableSize)
		m.masterRecord.stats.TotEntries += int(registration.NumEntries)
		levStats := m.getLevelStats(registration.Level)
		levStats.Tables++
		levStats.Bytes += int(registration.TableSize)
		levStats.Entries += int(registration.NumEntries)
	}
	return nil
}

func (m *Manager) getLevelStats(level int) *LevelStats {
	levStats, ok := m.masterRecord.stats.LevelStats[level]
	if !ok {
		levStats = &LevelStats{}
		m.masterRecord.stats.LevelStats[level] = levStats
	}
	return levStats
}

func (m *Manager) scheduleTableDeleteTimer() {
	m.tableDeleteTimer = time.AfterFunc(m.cfg.SSTableDeleteCheckInterval, func() {
		m.maybeDeleteTables()
	})
}

func (m *Manager) maybeDeleteTables() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return
	}
	pos := -1
	now := arista.NanoTime()
	for i, entry := range m.tablesToDelete {
		age := time.Duration(now - entry.addedTime)
		if age < m.cfg.SSTableDeleteDelay {
			break
		}
		log.Debugf("deleted sstable %v", entry.tableID)
		if err := objstore.DeleteWithTimeout(m.objStore, m.cfg.SSTableBucketName, string(entry.tableID), objstore.DefaultCallTimeout); err != nil {
			log.Errorf("failed to delete ss-table from cloud store: %v", err)
			break
		}
		pos = i
	}
	if pos != -1 {
		m.tablesToDelete = m.tablesToDelete[pos+1:]
	}
	m.scheduleTableDeleteTimer()
}

func HasOverlap(keyStart []byte, keyEnd []byte, blockKeyStart []byte, blockKeyEnd []byte) bool {
	// Note! keyStart is inclusive, keyEnd is exclusive
	// LevelManager keyStart and keyEnd are inclusive!
	dontOverlapRight := bytes.Compare(keyStart, blockKeyEnd) > 0                  // Range starts after end of block
	dontOverlapLeft := keyEnd != nil && bytes.Compare(keyEnd, blockKeyStart) <= 0 // Range ends before beginning of block
	dontOverlap := dontOverlapLeft || dontOverlapRight
	return !dontOverlap
}

func (m *Manager) DumpLevelInfo() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.dumpLevelInfo()
}

func (m *Manager) dumpLevelInfo() {
	builder := strings.Builder{}
	for level := range m.masterRecord.levelEntries {
		tableCount := m.masterRecord.levelTableCounts[level]
		builder.WriteString(fmt.Sprintf("level:%d table_count:%d, ", level, tableCount))
	}
	log.Info(builder.String())
}

func (m *Manager) Dump() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.dump()
}

func (m *Manager) dump() {
	log.Infof("Dumping LevelManager ====================")
	for level, entry := range m.masterRecord.levelEntries {
		log.Infof("Dumping level %d. Max version %d. Range Start: %v Range End: %v. There are %d table entries",
			level, entry.maxVersion, entry.rangeStart, entry.rangeEnd, len(entry.tableEntries))
		for _, lte := range entry.tableEntries {
			te := lte.Get(entry)
			log.Infof("table entry sstableid %v (%s) range start %s range end %s deleteRatio %.2f hasDeletes %t", te.SSTableID, string(te.SSTableID),
				string(te.RangeStart), string(te.RangeEnd), te.DeleteRatio, te.DeleteRatio > 0)
		}
	}
	for prefix := range m.masterRecord.slabRetentions {
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

func (m *Manager) GetObjectStore() objstore.Client {
	return m.objStore
}

func (m *Manager) GetLevelTableCounts() map[int]int {
	m.lock.Lock()
	defer m.lock.Unlock()
	counts := map[int]int{}
	for level, count := range m.masterRecord.levelTableCounts {
		counts[level] = count
	}
	return counts
}

func (m *Manager) GetStats() Stats {
	m.lock.RLock()
	defer m.lock.RUnlock()
	statsCopy := m.masterRecord.stats.copy()
	return *statsCopy
}

// Only used in testing
func (m *Manager) reset() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.started = false
	m.masterRecord = nil
}

func (m *Manager) getLevelEntry(level int) *levelEntry {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.levelEntry(level)
}

// Used in testing only
func (m *Manager) getMasterRecord() *MasterRecord {
	m.lock.Lock()
	defer m.lock.Unlock()
	// Serialize/Deserialize so not to expose internal MasterRecord directly
	buff := m.masterRecord.Serialize(nil)
	mr := &MasterRecord{}
	mr.Deserialize(buff, 0)
	return mr
}

type deleteTableEntry struct {
	tableID   sst.SSTableID
	addedTime uint64
}
