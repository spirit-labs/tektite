package lsm

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/asl/arista"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/sst"
	"math"
	"strings"
	"time"
)

type compactionState struct {
	tableDeleteTimer   *time.Timer
	tablesToDelete     []deleteTableEntry
	jobQueue           []jobHolder
	inProgress         map[string]inProgressCompaction
	pendingCompactions map[int]int
	lockedRanges       map[int][]lockedRange
	pollers            *pollerQueue
	stats              CompactionStats
}

func newCompactionState() compactionState {
	return compactionState{
		inProgress:         make(map[string]inProgressCompaction),
		pendingCompactions: map[int]int{},
		lockedRanges:       map[int][]lockedRange{},
		pollers:            &pollerQueue{},
	}
}

type jobHolder struct {
	job            CompactionJob
	completionFunc func(error)
}

type lockedRange struct {
	level int
	start []byte
	end   []byte
}

func (lr *lockedRange) overlaps(rng *lockedRange) bool {
	dontOverlapRight := bytes.Compare(rng.start, lr.end) > 0
	dontOverlapLeft := bytes.Compare(rng.end, lr.start) < 0
	dontOverlap := dontOverlapLeft || dontOverlapRight
	return !dontOverlap
}

func (m *Manager) maybeScheduleCompaction() error {
	// Get a level to compact (if any)
	level, numTables := m.chooseLevelToCompact()
	if level == -1 {
		if log.DebugEnabled {
			m.dumpLevelInfo()
		}
		// nothing to do
		return nil
	}
	tables, err := m.chooseTablesToCompact(level, numTables)
	if err != nil {
		return err
	}
	log.Debugf("in maybeScheduleCompaction - chose level %d num tables to compact: %d", level, len(tables))
	if len(tables) == 0 {
		return nil
	}
	_, _, err = m.scheduleCompaction(level, tables, nil)
	return err
}

func (m *Manager) getAllL0Tables() ([][]*TableEntry, error) {
	entry := m.levelEntry(0)
	tableEntries := make([]*TableEntry, len(entry.tableEntries))
	for i, lte := range entry.tableEntries {
		tableEntries[i] = lte.Get(entry)
	}
	return [][]*TableEntry{tableEntries}, nil
}

func (m *Manager) scheduleCompaction(level int, tableSlices [][]*TableEntry, completionFunc func(error)) (int, bool, error) {
	// If we are compacting into the last level, then we delete tombstones
	var jobs []CompactionJob
	destLevelEntries := m.levelEntry(level + 1)
	destLevelExists := len(destLevelEntries.tableEntries) > 0
	hasLocked := false
	now := uint64(time.Now().UTC().UnixMilli())
	lfv := m.masterRecord.lastFlushedVersion
outer:
	for _, tables := range tableSlices {
		// We compact each inner slice in its own job
		var tablesToCompact [][]tableToCompact
		// Calculate overall range of source tables
		sourceRangeStart, sourceRangeEnd := m.calculateOverallRange(tables)
		sourceRange := lockedRange{
			level: level,
			start: sourceRangeStart,
			end:   sourceRangeEnd,
		}
		// First check if this range is already locked
		if m.isRangeLocked(sourceRange) {
			// there's already a job that includes this range - we can't compact this slice
			hasLocked = true
			continue outer
		}
		// find overlap with tables in next level
		var overlapping []*TableEntry
		if destLevelExists {
			// note: rangeEnd param to getOverlappingTables is exclusive, so we need to increment
			rangeEnd := common.IncBigEndianBytes(sourceRangeEnd)
			var err error
			overlapping, err = m.getOverlappingTables(sourceRangeStart, rangeEnd, level+1, destLevelEntries)
			if err != nil {
				return 0, false, err
			}
		}
		// calculate maximum possible overall range of results of compaction
		destRangeStart := sourceRangeStart
		destRangeEnd := sourceRangeEnd
		if len(overlapping) > 0 {
			destRangeStart, destRangeEnd = m.calculateOverallRange(append(tables, overlapping...))
		}
		destRange := lockedRange{
			level: level + 1,
			start: destRangeStart,
			end:   destRangeEnd,
		}
		// Now check if overall result range is already locked in destination level - note that we lock ranges instead
		// of locking destination tables, as when compacting into an empty level we still need to lock the destination
		// range to prevent more than once concurrent compaction compacting into the same destination range
		if m.isRangeLocked(destRange) {
			// there's already a job that includes this table - we can't compact this slice
			hasLocked = true
			continue outer
		}
		hasDeadVersionRanges := false
		canCompact := true
		// create the job
		var tableIDs []sst.SSTableID
		// Note that tables in a slice must be added in order from newest to earliest - this is critical as the exact same key
		// can be in different tables, and when a compaction merging iterator is created and finds same keys it will
		// take the leftmost one - this must be the latest one!
		hasPotentialExpiredEntries := false
		hasDeletes := false
		for i := len(tables) - 1; i >= 0; i-- {
			st := tables[i].copy()
			tablesToCompact = append(tablesToCompact, []tableToCompact{{
				level: level,
				table: st,
			}})
			tableIDs = append(tableIDs, st.SSTableID)
			if !hasPotentialExpiredEntries {
				hasPotentialExpiredEntries = m.hasPotentialExpiredEntries(st, now)
			}
			if !hasDeletes {
				hasDeletes = st.DeleteRatio > 0
			}
			if int64(st.MaxVersion) > lfv {
				// can't remove tombstones if there are any entries with version that's not flushed yet, otherwise
				// when compacting into last level could end up not removing key as non compactable but removing
				// tombstone as preserveTombstones = false as last level, thus ending up with data not getting deleted
				canCompact = false
			}
			if len(st.DeadVersionRanges) > 0 {
				hasDeadVersionRanges = true
			}
		}
		if len(overlapping) > 0 {
			var nextLevelTables []tableToCompact
			for _, st := range overlapping {
				nextLevelTables = append(nextLevelTables, tableToCompact{
					level: level + 1,
					table: st.copy(),
				})
				if int64(st.MaxVersion) > lfv {
					canCompact = false
				}
				if len(st.DeadVersionRanges) > 0 {
					hasDeadVersionRanges = true
				}
			}
			tablesToCompact = append(tablesToCompact, nextLevelTables)
		}
		// We move tables directly if all the following are true:
		// 1. There's only a single source table in the job (otherwise there could be overlap between source tables)
		// 2. There are definitely no expired entries that would need removing
		// 3. There are no dead version ranges to remove
		// 4. There is no overlap with tables in the next level
		// 5. We're not moving to the last level or there are no deletes in the table (we want to remove deletes on the last
		// level, so we can't move in that case)
		move := len(tables) == 1 && !hasPotentialExpiredEntries && !hasDeadVersionRanges && len(overlapping) == 0 &&
			(level+1 != m.getLastLevel() || !hasDeletes)
		id := uuid.New().String()
		destLevel := level + 1
		// We preserve tombstones if we're not compacting into the last level or there are entries in any table
		// in the compaction with a non compactable version (> last flushed version)
		preserveTombstones := !canCompact || m.getLastLevel() > destLevel
		job := CompactionJob{
			id:                 id,
			levelFrom:          level,
			tables:             tablesToCompact,
			isMove:             move,
			preserveTombstones: preserveTombstones,
			scheduleTime:       arista.NanoTime(),
			serverTime:         uint64(time.Now().UTC().UnixMilli()),
			lastFlushedVersion: m.masterRecord.lastFlushedVersion,
			sourceRange:        sourceRange,
			destRange:          destRange,
		}
		log.Debugf("created compaction job %s from level %d last level is %d, preserve tombstones is %t",
			id, level, m.getLastLevel(), preserveTombstones)
		jobs = append(jobs, job)
		m.lockTablesForJob(job)
	}
	var complFunc func(error)
	if completionFunc != nil {
		complFunc = common.NewCountDownFuture(len(jobs), completionFunc).CountDown
	}
	for _, job := range jobs {
		if log.DebugEnabled {
			sb := strings.Builder{}
			for _, no := range job.tables {
				for _, ttc := range no {
					sb.WriteString(fmt.Sprintf("level:%d table:%v, ", ttc.level, ttc.table.SSTableID))
				}
			}
			log.Debugf("compaction created job %s %s", job.id, sb.String())
		}
		m.queueOrDespatchJob(job, complFunc)
	}
	// return number of jobs, whether any tables were locked
	return len(jobs), hasLocked, nil
}

func (m *Manager) isRangeLocked(rng lockedRange) bool {
	rngs, ok := m.lockedRanges[rng.level]
	if !ok {
		return false
	}
	for _, r := range rngs {
		if r.overlaps(&rng) {
			return true
		}
	}
	return false
}

func (m *Manager) hasPotentialExpiredEntries(te *TableEntry, now uint64) bool {
	if len(m.masterRecord.slabRetentions) == 0 {
		return false
	}
	// If all the entries in the table are for the same partition hash then we can directly look at the slab id
	// to see if entries are expired
	partitionHash1 := te.RangeStart[:16]
	partitionHash2 := te.RangeEnd[:16]
	same := bytes.Equal(partitionHash1, partitionHash2)
	if !same {
		// Might not have expired entries but we err on the side of caution and return true, which will prevent a move
		return true
	}
	slabID1 := binary.BigEndian.Uint64(te.RangeStart[16:])
	slabID2 := binary.BigEndian.Uint64(te.RangeEnd[16:])
	for slabID, ret := range m.masterRecord.slabRetentions {
		retMillis := uint64(time.Duration(ret).Milliseconds())
		if slabID >= slabID1 && slabID <= slabID2 {
			age := now - te.AddedTime
			if age >= retMillis {
				return true
			}
		}
	}
	return false
}

func (m *Manager) queueOrDespatchJob(job CompactionJob, complFunc func(error)) {
	if m.pollers.Len() > 0 {
		// We have a waiting poller - hand the job to the poller straightaway
		holder := jobHolder{
			job:            job,
			completionFunc: complFunc,
		}
		m.stats.InProgressJobs++
		poller := m.pollers.pop()
		poller.timer.Stop()
		poller.timer = nil
		timer := m.scheduleJobTimeout(holder, poller.connectionID)
		m.inProgress[job.id] = inProgressCompaction{
			timer:        timer,
			jobHolder:    holder,
			connectionID: poller.connectionID,
		}
		theJob := job
		poller.completionFunc(&theJob, nil)
	} else {
		// append the job to the job queue
		m.jobQueue = append(m.jobQueue, jobHolder{
			job:            job,
			completionFunc: complFunc,
		})
		m.stats.QueuedJobs++
	}
	m.pendingCompactions[job.levelFrom]++
}

func (m *Manager) lockTablesForJob(job CompactionJob) {
	m.lockRange(job.sourceRange)
	m.lockRange(job.destRange)
}

func (m *Manager) unlockTablesForJob(job CompactionJob) {
	m.unlockRange(job.sourceRange)
	m.unlockRange(job.destRange)
}

func (m *Manager) LockRange(rng lockedRange) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.lockRange(rng)
}

func (m *Manager) lockRange(rng lockedRange) {
	m.lockedRanges[rng.level] = append(m.lockedRanges[rng.level], rng)
}

func (m *Manager) UnlockRange(rng lockedRange) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.unlockRange(rng)
}

func (m *Manager) unlockRange(r lockedRange) {
	levelRanges, ok := m.lockedRanges[r.level]
	if ok {
		var newRanges []lockedRange
		found := false
		for _, rng := range levelRanges {
			if !(bytes.Equal(r.start, rng.start) && bytes.Equal(r.end, rng.end)) {
				newRanges = append(newRanges, rng)
			} else {
				found = true
			}
		}
		if found {
			m.lockedRanges[r.level] = newRanges
			return
		}
	}
	panic(fmt.Sprintf("failed to unlock range %v - not found", r))
}

func (m *Manager) calculateOverallRange(tables []*TableEntry) ([]byte, []byte) {
	first := true
	var rangeStart, rangeEnd []byte
	for _, table := range tables {
		if first {
			rangeStart = table.RangeStart
			rangeEnd = table.RangeEnd
			first = false
		} else {
			if bytes.Compare(rangeStart, table.RangeStart) > 0 {
				rangeStart = table.RangeStart
			}
			if bytes.Compare(table.RangeEnd, rangeEnd) > 0 {
				rangeEnd = table.RangeEnd
			}
		}
	}
	// it's critical we calculate the overlap to exclude versions, so we make
	// sure we capture overlapping keys of any version
	return rangeStart[:len(rangeStart)-8], rangeEnd[:len(rangeEnd)-8]
}

func (m *Manager) chooseLevelToCompact() (int, int) {
	// We choose a level to compact based on ratio of number of tables / max tables trigger
	toCompact := -1
	var maxRatio float64
	var numTables int
	for level := range m.masterRecord.levelTableCounts {
		trigger := m.levelMaxTablesTrigger(level)
		tableCount := m.tableCount(level)
		// we take any already scheduled compactions for the level into account
		pending := m.pendingCompactions[level]
		availableTables := tableCount - pending
		if availableTables > trigger {
			ratio := float64(availableTables) / float64(trigger)
			if ratio > maxRatio {
				maxRatio = ratio
				toCompact = level
				numTables = availableTables - trigger
			}
		}
	}
	return toCompact, numTables
}

func (m *Manager) tableCount(level int) int {
	return m.masterRecord.levelTableCounts[level]
}

func (m *Manager) chooseTablesToCompact(level int, maxTables int) ([][]*TableEntry, error) {
	if level == 0 {
		// We compact the whole level -this is done as a single job, as there can be overlap between L0 tables
		return m.getAllL0Tables()
	}
	levEntry := m.levelEntry(level)
	tables, err := chooseTablesToCompactFromLevel(levEntry, maxTables)
	if err != nil {
		return nil, err
	}
	// L > 0, no overlap between tables, so convert to one job per table as they can be processed in parallel
	tableSlices := make([][]*TableEntry, len(tables))
	for i, table := range tables {
		tableSlices[i] = []*TableEntry{table}
	}
	return tableSlices, nil
}

func chooseTablesToCompactFromLevel(levelEntry *levelEntry, maxTables int) ([]*TableEntry, error) {
	// Iterate through once to get min and max added time
	var minAddedTime uint64 = math.MaxUint64
	var maxAddedTime uint64
	for _, lte := range levelEntry.tableEntries {
		te := lte.Get(levelEntry)
		if te.AddedTime < minAddedTime {
			minAddedTime = te.AddedTime
		}
		if te.AddedTime > maxAddedTime {
			maxAddedTime = te.AddedTime
		}
	}
	// Iterate through again to calculate scores
	h := scoreHeap{}
	heap.Init(&h)
	for _, lte := range levelEntry.tableEntries {
		te := lte.Get(levelEntry)
		heap.Push(&h, scoreEntry{
			tableEntry: te,
			score:      computeScore(te, minAddedTime, maxAddedTime),
		})
		if h.Len() > maxTables {
			heap.Pop(&h)
		}
	}
	entries := make([]*TableEntry, h.Len())
	for i := len(entries) - 1; i >= 0; i-- {
		scoreEntry := heap.Pop(&h).(scoreEntry)
		entries[i] = scoreEntry.tableEntry
	}
	return entries, nil
}

func computeScore(te *TableEntry, minAddedTime uint64, maxAddedTime uint64) float64 {
	/*
		The score has three components.
		1. From 0-1 as AddedTime varies linearly between maxAddedTime and minAddedTime
		2. DeleteRatio, from 0-1
		3. If there is one or more prefix tombstones then contribute 3 (this happens when table is dropped)
	*/
	var ageContrib float64
	if maxAddedTime > minAddedTime {
		// if AddedTime = minAddedTime then + 1, if AddedTime = maxAddedTime then -1
		// So we prioritise compaction of older tables
		ageContrib = 1 - float64(te.AddedTime-minAddedTime)/float64(maxAddedTime-minAddedTime)
	}
	var prefixDeleteContrib float64
	if te.NumPrefixDeletes > 0 {
		prefixDeleteContrib = 3
	}
	return ageContrib + te.DeleteRatio + prefixDeleteContrib
}

type scoreEntry struct {
	tableEntry *TableEntry
	score      float64
}

type scoreHeap []scoreEntry

//goland:noinspection GoMixedReceiverTypes
func (h scoreHeap) Len() int { return len(h) }

//goland:noinspection GoMixedReceiverTypes
func (h scoreHeap) Less(i, j int) bool { return h[i].score < h[j].score }

//goland:noinspection GoMixedReceiverTypes
func (h scoreHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

//goland:noinspection GoMixedReceiverTypes
func (h *scoreHeap) Push(x interface{}) {
	*h = append(*h, x.(scoreEntry))
}

//goland:noinspection GoMixedReceiverTypes
func (h *scoreHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (m *Manager) jobInProgress(jobID string) bool {
	_, ok := m.inProgress[jobID]
	return ok
}

func (m *Manager) compactionComplete(jobID string) error {
	compactionJob, ok := m.inProgress[jobID]
	if !ok {
		panic("cannot find compactionJob")
	}
	job := compactionJob.jobHolder.job
	delete(m.inProgress, job.id)
	m.pendingCompactions[job.levelFrom]--
	if compactionJob.timer != nil {
		compactionJob.timer.Stop()
	}
	m.unlockTablesForJob(job)
	m.stats.InProgressJobs--
	m.stats.CompletedJobs++
	dur := time.Duration(arista.NanoTime() - job.scheduleTime)
	log.Debugf("compaction complete job %s - time from schedule %d ms", job.id, dur.Milliseconds())
	cf := compactionJob.jobHolder.completionFunc
	if cf != nil {
		log.Debugf("in compactionComplete %s calling completion", jobID)
		cf(nil)
	}
	if log.DebugEnabled {
		m.dumpLevelInfo()
	}
	// After compaction, the dest level might need compaction, or we might have more dead entries to remove,
	// so we trigger a check
	return m.maybeScheduleCompaction()
}

func (m *Manager) PollForJob(connectionID int, completionFunc func(job *CompactionJob, err error)) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if len(m.jobQueue) > 0 {
		holder := m.jobQueue[0]
		m.jobQueue = m.jobQueue[1:]
		m.stats.QueuedJobs--
		job := holder.job
		timer := m.scheduleJobTimeout(holder, connectionID)
		m.inProgress[job.id] = inProgressCompaction{
			timer:        timer,
			jobHolder:    holder,
			connectionID: connectionID,
		}
		m.stats.InProgressJobs++
		jobCopy := job
		completionFunc(&jobCopy, nil)
		return
	}
	p := &poller{
		addedTime:      arista.NanoTime(),
		completionFunc: completionFunc,
		connectionID:   connectionID,
	}
	m.schedulePollerTimeout(p)
	m.pollers.add(p)
}

func (m *Manager) schedulePollerTimeout(poller *poller) {
	timer := common.ScheduleTimer(m.cfg.CompactionPollerTimeout, false, func() {
		// run on separate GR to avoid deadlock with stopping timer when job dispatched and level manager lock
		common.Go(func() {
			m.lock.Lock()
			defer m.lock.Unlock()
			if poller.timer == nil {
				// already complete
				return
			}
			m.pollers.remove(poller)
			poller.completionFunc(nil, common.NewTektiteErrorf(common.CompactionPollTimeout, "no job available"))
		})
	})
	poller.timer = timer
}

func (m *Manager) scheduleJobTimeout(holder jobHolder, connectionID int) *time.Timer {
	return time.AfterFunc(m.cfg.CompactionJobTimeout, func() {
		m.lock.Lock()
		defer m.lock.Unlock()
		m.cancelInProgressJob(holder)
	})
}

func (m *Manager) cancelInProgressJob(holder jobHolder) {
	log.Debugf("cancelling in progress job: %s", holder.job.id)
	job := holder.job
	_, ok := m.inProgress[job.id]
	if !ok {
		return // already complete
	}
	log.Debugf("compaction job: %s timed out, will be made available to pollers again", holder.job.id)
	delete(m.inProgress, job.id)
	m.pendingCompactions[job.levelFrom]--
	m.stats.InProgressJobs--
	m.stats.TimedOutJobs++
	m.queueOrDespatchJob(job, holder.completionFunc)
}

func (m *Manager) connectionClosed(connectionID int) {
	m.lock.Lock()
	defer m.lock.Unlock()
	log.Debugf("connectionClosed %d num inprog jobs:%d", connectionID, len(m.inProgress))
	// Cancel any in progress compactions that were polled for on the connection that was closed. This indicates
	// the node has failed, cancelling them on close of connection is quicker than waiting for job timeout which can
	// be a significant time. We don't want compaction to stall for a long time when a node dies, as this can cause
	// L0 to reach max size and registrations to block.
	for _, inProg := range m.inProgress {
		log.Debugf("inprogress job: %s connection id:%d", inProg.jobHolder.job.id, inProg.connectionID)
		if inProg.connectionID == connectionID {
			log.Debugf("cancelling inprogress job %s on connection close", inProg.jobHolder.job.id)
			m.cancelInProgressJob(inProg.jobHolder)
		}
	}
}

func (m *Manager) checkForDeadEntries(rng VersionRange) bool {
	for level, entry := range m.masterRecord.levelEntries {
		for _, lte := range entry.tableEntries {
			te := lte.Get(entry)
			// Only add the tables that match
			if te.MaxVersion >= rng.VersionStart && te.MinVersion <= rng.VersionEnd {
				log.Errorf("entry with dead version in sstable %s level %d", string(te.SSTableID), level)
				return true
			}
		}
	}
	return false
}

func (m *Manager) forceCompaction(level int, maxTables int) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	entries := m.levelEntry(level)
	if len(entries.tableEntries) == 0 {
		return nil
	}
	tables, err := m.chooseTablesToCompact(level, maxTables)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}
	_, _, err = m.scheduleCompaction(level, tables, nil)
	return err
}

func (m *Manager) GetCompactionStats() CompactionStats {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.stats
}

type CompactionStats struct {
	QueuedJobs     int
	InProgressJobs int
	CompletedJobs  int
	TimedOutJobs   int
}

type LevelIterator interface {
	Next() (*TableEntry, error)
	Reset() error
}

type tableToCompact struct {
	level int
	table *TableEntry
}

type inProgressCompaction struct {
	timer        *time.Timer
	jobHolder    jobHolder
	connectionID int
}

type CompactionJob struct {
	id                 string
	levelFrom          int
	tables             [][]tableToCompact
	isMove             bool
	preserveTombstones bool
	scheduleTime       uint64 // Used for timing jobs - we use nanoTime to avoid errors if clocks change
	serverTime         uint64 // Unix millis past epoch - Used on compaction workers to determine if entries are expired
	lastFlushedVersion int64
	sourceRange        lockedRange // Not used on compaction worker so doesn't need to be serialized
	destRange          lockedRange // Not used on compaction worker so doesn't need to be serialized
}

func (c *CompactionJob) Serialize(buff []byte) []byte {
	buff = encoding.AppendStringToBufferLE(buff, c.id)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(c.levelFrom))
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(c.tables)))
	for _, tablesToCompact := range c.tables {
		buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(tablesToCompact)))
		for _, tableToCompact := range tablesToCompact {
			buff = encoding.AppendUint32ToBufferLE(buff, uint32(tableToCompact.level))
			buff = tableToCompact.table.serialize(buff)
		}
	}
	buff = encoding.AppendBoolToBuffer(buff, c.isMove)
	buff = encoding.AppendBoolToBuffer(buff, c.preserveTombstones)
	buff = encoding.AppendUint64ToBufferLE(buff, c.scheduleTime)
	buff = encoding.AppendUint64ToBufferLE(buff, c.serverTime)
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(c.lastFlushedVersion))
	return buff
}

func (c *CompactionJob) Deserialize(buff []byte, offset int) int {
	c.id, offset = encoding.ReadStringFromBufferLE(buff, offset)
	var lf uint32
	lf, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	c.levelFrom = int(lf)
	var nt uint32
	nt, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	c.tables = make([][]tableToCompact, int(nt))
	for i := 0; i < int(nt); i++ {
		var nt2 uint32
		nt2, offset = encoding.ReadUint32FromBufferLE(buff, offset)
		tables2 := make([]tableToCompact, int(nt2))
		for j := 0; j < int(nt2); j++ {
			var l uint32
			l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
			te := &TableEntry{}
			offset = te.deserialize(buff, offset)
			tables2[j] = tableToCompact{
				level: int(l),
				table: te,
			}
		}
		c.tables[i] = tables2
	}
	c.isMove, offset = encoding.ReadBoolFromBuffer(buff, offset)
	c.preserveTombstones, offset = encoding.ReadBoolFromBuffer(buff, offset)
	c.scheduleTime, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	c.serverTime, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	var lfv uint64
	lfv, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	c.lastFlushedVersion = int64(lfv)
	return offset
}

type CompactionResult struct {
	id        string
	newTables []TableEntry
}

func (c *CompactionResult) Serialize(buff []byte) []byte {
	buff = encoding.AppendStringToBufferLE(buff, c.id)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(c.newTables)))
	for _, nt := range c.newTables {
		buff = nt.serialize(buff)
	}
	return buff
}

func (c *CompactionResult) Deserialize(buff []byte, offset int) int {
	c.id, offset = encoding.ReadStringFromBufferLE(buff, offset)
	var nt uint32
	nt, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	c.newTables = make([]TableEntry, int(nt))
	for i := 0; i < int(nt); i++ {
		offset = c.newTables[i].deserialize(buff, offset)
	}
	return offset
}

// MergeSSTables takes a list of SSTables, and merges them to produce one or more output SSTables
// Tables lower in the list take precedence to tables higher in the list when a common key is found

type ssTableInfo struct {
	sst              *sst.SSTable
	rangeStart       []byte
	rangeEnd         []byte
	minVersion       uint64
	maxVersion       uint64
	deleteRatio      float64
	numPrefixDeletes uint32
}

type poller struct {
	addedTime      uint64
	connectionID   int
	completionFunc func(job *CompactionJob, err error)
	index          int
	timer          *common.TimerHandle
}

type pollerQueue []*poller

//goland:noinspection GoMixedReceiverTypes
func (pq pollerQueue) Len() int { return len(pq) }

//goland:noinspection GoMixedReceiverTypes
func (pq pollerQueue) Less(i, j int) bool {
	return pq[i].addedTime < pq[j].addedTime
}

//goland:noinspection GoMixedReceiverTypes
func (pq pollerQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

//goland:noinspection GoMixedReceiverTypes
func (pq *pollerQueue) Push(x interface{}) {
	item := x.(*poller)
	item.index = len(*pq)
	*pq = append(*pq, item)
}

//goland:noinspection GoMixedReceiverTypes
func (pq *pollerQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	item.index = -1
	return item
}

//goland:noinspection GoMixedReceiverTypes
func (pq *pollerQueue) add(item *poller) {
	heap.Push(pq, item)
}

//goland:noinspection GoMixedReceiverTypes
func (pq *pollerQueue) remove(item *poller) {
	heap.Remove(pq, item.index)
}

//goland:noinspection GoMixedReceiverTypes
func (pq *pollerQueue) pop() *poller {
	item := pq.Pop()
	return item.(*poller)
}
