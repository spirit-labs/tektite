package levels

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/sst"
	"math"
	"strings"
	"time"
)

type jobHolder struct {
	job            CompactionJob
	completionFunc func(error)
}

func (lm *LevelManager) maybeScheduleCompaction() error {

	// If there are any dead version ranges that need to be removed these always take priority.
	if len(lm.masterRecord.deadVersionRanges) > 0 {
		if err := lm.maybeScheduleRemoveDeadVersionEntries(); err != nil {
			return err
		}
	}

	// Get a level to compact (if any)
	level, numTables := lm.chooseLevelToCompact()
	if level == -1 {
		if log.DebugEnabled {
			lm.dumpLevelInfo()
		}
		// nothing to do
		return nil
	}
	tables, err := lm.chooseTablesToCompact(level, numTables)
	if err != nil {
		return err
	}

	log.Debugf("in levelmanager maybeScheduleCompaction - chose level %d num tables to compact: %d", level, len(tables))
	if len(tables) == 0 {
		return nil
	}

	_, _, err = lm.scheduleCompaction(level, tables, nil, nil)
	return err
}

func (lm *LevelManager) chooseL0TablesToCompact() ([][]*TableEntry, bool) {
	if lm.compactingStartupL0Group {
		// If we're already compacting startup L0 group (processor id = -1) then we cannot compact any more L0 tables
		// until that is complete. That's because the startup L0 group can have key overlap with other groups so
		// if we processed a compaction for a non startup L0 group before the startup compaction completed it could result
		// in moves to L1 with overlapping keys.
		return nil, false
	}
	// If we have table entries for processor -1, these are ones added on startup where we do not know the processor id.
	// So we must compact this in a single job, before compact any others.
	startupEntries, ok := lm.level0Groups[-1]
	if ok {
		return [][]*TableEntry{startupEntries}, true
	}
	// We currently compact the whole level - a job for each group
	var tableSlices [][]*TableEntry
	for _, g := range lm.level0Groups {
		tableSlices = append(tableSlices, g)
	}
	return tableSlices, true
}

func (lm *LevelManager) scheduleCompaction(level int, tableSlices [][]*TableEntry, deadVersionRanges []VersionRange,
	completionFunc func(error)) (int, bool, error) {
	// If we are compacting into the last level, then we delete tombstones
	destLevel := level + 1
	preserveTombstones := lm.getLastLevel() > destLevel

	var jobs []CompactionJob

	destLevelEntries := lm.getLevelSegmentEntries(level + 1)
	segmentEntries := destLevelEntries.segmentEntries
	destLevelExists := len(segmentEntries) > 0
	hasLocked := false
	now := uint64(time.Now().UTC().UnixMilli())

outer:
	for _, tables := range tableSlices {

		// We compact each slice in its own job
		var tablesToCompact [][]tableToCompact
		for _, table := range tables {
			_, locked := lm.lockedTables[string(table.SSTableID)]
			if locked {
				// there's already a job that includes this table - we can't compact this slice
				hasLocked = true
				continue outer
			}
		}

		// find overlap with next level
		var overlapping []*TableEntry
		if destLevelExists {
			// it's critical we calculate the overlap to exclude versions, so we make
			// sure we capture overlapping keys of any version
			rangeStart, rangeEnd := lm.calculateOverallRange(tables)
			rangeStartNoVersion := rangeStart[:len(rangeStart)-8]

			// rangeEnd param to getOverlappingTables is exclusive, so we need to increment
			rangeEndNoVersion := rangeEnd[:len(rangeEnd)-8]
			rangeEndNoVersion = common.IncrementBytesBigEndian(rangeEndNoVersion)
			var err error
			overlapping, err = lm.getOverlappingTables(rangeStartNoVersion, rangeEndNoVersion, level+1, segmentEntries)
			if err != nil {
				return 0, false, err
			}
		}
		for _, overlap := range overlapping {
			_, locked := lm.lockedTables[string(overlap.SSTableID)]
			if locked {
				// there's already a job that includes this table - we can't compact this slice
				hasLocked = true
				continue outer
			}
		}
		// create the job
		var tableIDs []sst.SSTableID
		// Note that tables in a slice must be added in order from newest to earliest - this is critical as the exact same key
		// can be in different tables, and when a compaction merging iterator is created and finds same keys it will
		// take the leftmost one - this must be the latest one!
		hasPotentialExpiredEntries := false
		hasDeletes := false
		for i := len(tables) - 1; i >= 0; i-- {
			st := tables[i]
			tablesToCompact = append(tablesToCompact, []tableToCompact{{
				level:             level,
				table:             st,
				deadVersionRanges: deadVersionRanges,
			}})
			tableIDs = append(tableIDs, st.SSTableID)
			if !hasPotentialExpiredEntries {
				hasPotentialExpiredEntries = lm.hasPotentialExpiredEntries(st, now)
			}
			if !hasDeletes {
				hasDeletes = st.DeleteRatio > 0
			}
		}
		if len(overlapping) > 0 {
			var nextLevelTables []tableToCompact
			for _, st := range overlapping {
				nextLevelTables = append(nextLevelTables, tableToCompact{
					level: level + 1,
					table: st,
				})
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
		move := len(tables) == 1 && !hasPotentialExpiredEntries && deadVersionRanges == nil && len(overlapping) == 0 &&
			(level+1 != lm.getLastLevel() || !hasDeletes)

		id := uuid.New().String()

		job := CompactionJob{
			id:                 id,
			levelFrom:          level,
			tables:             tablesToCompact,
			isMove:             move,
			preserveTombstones: preserveTombstones,
			scheduleTime:       common.NanoTime(),
			serverTime:         uint64(time.Now().UTC().UnixMilli()),
			lastFlushedVersion: lm.masterRecord.lastFlushedVersion,
		}

		log.Debugf("created compaction job %s from level %d last level is %d, preserve tombstones is %t",
			id, level, lm.getLastLevel(), preserveTombstones)
		jobs = append(jobs, job)

		lm.lockTablesForJob(job)
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
		lm.queueOrDespatchJob(job, complFunc)
	}

	// return number of jobs, whether any tables were locked
	return len(jobs), hasLocked, nil
}

func (lm *LevelManager) hasPotentialExpiredEntries(te *TableEntry, now uint64) bool {
	if len(lm.masterRecord.slabRetentions) == 0 {
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
	for slabID, ret := range lm.masterRecord.slabRetentions {
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

func (lm *LevelManager) queueOrDespatchJob(job CompactionJob, complFunc func(error)) {
	if lm.pollers.Len() > 0 {
		// We have a waiting poller - hand the job to the poller straightaway
		holder := jobHolder{
			job:            job,
			completionFunc: complFunc,
		}
		lm.stats.InProgressJobs++
		poller := lm.pollers.pop()
		poller.timer.Stop()
		poller.timer = nil
		timer := lm.scheduleJobTimeout(holder, poller.connectionID)
		lm.inProgress[job.id] = inProgressCompaction{
			timer:        timer,
			jobHolder:    holder,
			connectionID: poller.connectionID,
		}
		theJob := job
		poller.completionFunc(&theJob, nil)
	} else {
		// append the job to the job queue
		lm.jobQueue = append(lm.jobQueue, jobHolder{
			job:            job,
			completionFunc: complFunc,
		})
		lm.stats.QueuedJobs++
	}
	lm.pendingCompactions[job.levelFrom]++
}

func (lm *LevelManager) lockTablesForJob(job CompactionJob) {
	for _, overlapping := range job.tables {
		for _, st := range overlapping {
			lm.lockedTables[string(st.table.SSTableID)] = struct{}{}
		}
	}
}

func (lm *LevelManager) unlockTablesForJob(job CompactionJob) {
	for _, overlapping := range job.tables {
		for _, st := range overlapping {
			delete(lm.lockedTables, string(st.table.SSTableID))
		}
	}
}

func (lm *LevelManager) calculateOverallRange(tables []*TableEntry) ([]byte, []byte) {
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
	return rangeStart, rangeEnd
}

func (lm *LevelManager) chooseLevelToCompact() (int, int) {
	// We choose a level to compact based on ratio of number of tables / max tables trigger
	toCompact := -1
	var maxRatio float64
	var numTables int
	for level := range lm.masterRecord.levelTableCounts {
		trigger := lm.levelMaxTablesTrigger(level)
		tableCount := lm.tableCount(level)
		// we take any already scheduled compactions for the level into account
		pending := lm.pendingCompactions[level]
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

func (lm *LevelManager) tableCount(level int) int {
	return lm.masterRecord.levelTableCounts[level]
}

func (lm *LevelManager) chooseTablesToCompact(level int, maxTables int) ([][]*TableEntry, error) {
	if level == 0 {
		tables, ok := lm.chooseL0TablesToCompact()
		if !ok {
			// L0 startup group compaction in progress
			return nil, nil
		}
		return tables, nil
	}
	iter, err := lm.levelIterator(level)
	if err != nil {
		return nil, err
	}
	// convert to one job per table
	tables, err := chooseTablesToCompactFromLevel(iter, maxTables)
	if err != nil {
		return nil, err
	}
	tableSlices := make([][]*TableEntry, len(tables))
	for i, table := range tables {
		tableSlices[i] = []*TableEntry{table}
	}
	return tableSlices, nil
}

func chooseTablesToCompactFromLevel(iter LevelIterator, maxTables int) ([]*TableEntry, error) {
	// Iterate through once to get min and max added time
	var minAddedTime uint64 = math.MaxUint64
	var maxAddedTime uint64
	for {
		te, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if te == nil {
			break
		}
		if te.AddedTime < minAddedTime {
			minAddedTime = te.AddedTime
		}
		if te.AddedTime > maxAddedTime {
			maxAddedTime = te.AddedTime
		}
	}
	// Iterate through again to calculate scores
	if err := iter.Reset(); err != nil {
		return nil, err
	}
	h := scoreHeap{}
	heap.Init(&h)
	for {
		te, err := iter.Next()

		if err != nil {
			return nil, err
		}
		if te == nil {
			break
		}
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

func (lm *LevelManager) jobInProgress(jobID string) bool {
	_, ok := lm.inProgress[jobID]
	return ok
}

func (lm *LevelManager) compactionComplete(jobID string) error {
	log.Debugf("in compactionComplete %s", jobID)
	compactionJob, ok := lm.inProgress[jobID]
	if !ok {
		panic("cannot find compactionJob")
	}
	job := compactionJob.jobHolder.job
	delete(lm.inProgress, job.id)
	lm.pendingCompactions[job.levelFrom]--
	if compactionJob.timer != nil {
		compactionJob.timer.Stop()
	}
	lm.unlockTablesForJob(job)
	lm.stats.InProgressJobs--
	lm.stats.CompletedJobs++
	dur := time.Duration(common.NanoTime() - job.scheduleTime)
	log.Debugf("compaction complete job %s - time from schedule %d ms", job.id, dur.Milliseconds())
	cf := compactionJob.jobHolder.completionFunc
	if cf != nil {
		log.Debugf("in compactionComplete %s calling completion", jobID)
		cf(nil)
	}
	if log.DebugEnabled {
		lm.dumpLevelInfo()
	}
	// After compaction, the dest level might need compaction, or we might have more dead entries to remove,
	// so we trigger a check
	return lm.maybeScheduleCompaction()
}

func (lm *LevelManager) pollForJob(connectionID int, completionFunc func(job *CompactionJob, err error)) {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if len(lm.jobQueue) > 0 {
		holder := lm.jobQueue[0]
		lm.jobQueue = lm.jobQueue[1:]
		lm.stats.QueuedJobs--
		job := holder.job
		timer := lm.scheduleJobTimeout(holder, connectionID)
		lm.inProgress[job.id] = inProgressCompaction{
			timer:        timer,
			jobHolder:    holder,
			connectionID: connectionID,
		}
		lm.stats.InProgressJobs++
		jobCopy := job
		completionFunc(&jobCopy, nil)
		return
	}
	poller := &poller{
		addedTime:      common.NanoTime(),
		completionFunc: completionFunc,
		connectionID:   connectionID,
	}
	lm.schedulePollerTimeout(poller)
	lm.pollers.add(poller)
}

func (lm *LevelManager) schedulePollerTimeout(poller *poller) {
	timer := common.ScheduleTimer(lm.conf.CompactionPollerTimeout, false, func() {
		// run on separate GR to avoid deadlock with stopping timer when job dispatched and level manager lock
		common.Go(func() {
			lm.lock.Lock()
			defer lm.lock.Unlock()
			if poller.timer == nil {
				// already complete
				return
			}
			lm.pollers.remove(poller)
			poller.completionFunc(nil, errors.NewTektiteErrorf(errors.CompactionPollTimeout, "no job available"))
		})
	})
	poller.timer = timer
}

func (lm *LevelManager) scheduleJobTimeout(holder jobHolder, connectionID int) *common.TimerHandle {
	return common.ScheduleTimer(lm.conf.CompactionJobTimeout, false, func() {
		lm.lock.Lock()
		defer lm.lock.Unlock()
		log.Debugf("compaction job timedout %s with connection id %d", holder.job.id, connectionID)
		lm.cancelInProgressJob(holder)
	})
}

func (lm *LevelManager) cancelInProgressJob(holder jobHolder) {
	log.Debugf("cancelling in progress job: %s", holder.job.id)
	job := holder.job
	_, ok := lm.inProgress[job.id]
	if !ok {
		return // already complete
	}
	log.Debugf("compaction job: %s timed out, will be made available to pollers again", holder.job.id)
	delete(lm.inProgress, job.id)

	lm.pendingCompactions[job.levelFrom]--
	lm.stats.InProgressJobs--
	lm.stats.TimedOutJobs++

	lm.queueOrDespatchJob(job, holder.completionFunc)
}

func (lm *LevelManager) connectionClosed(connectionID int) {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	log.Debugf("levelmanager.connectionClosed %d num inprog jobs:%d", connectionID, len(lm.inProgress))
	// Cancel any in progress compactions that were polled for on the connection that was closed. This indicates
	// the node has failed, cancelling them on close of connection is quicker than waiting for job timeout which can
	// be a significant time. We don't want compaction to stall for a long time when a node dies, as this can cause
	// L0 to reach max size and registrations to block.
	for _, inProg := range lm.inProgress {
		log.Debugf("inprogress job: %s connection id:%d", inProg.jobHolder.job.id, inProg.connectionID)
		if inProg.connectionID == connectionID {
			log.Debugf("cancelling inprogress job %s on connection close", inProg.jobHolder.job.id)
			lm.cancelInProgressJob(inProg.jobHolder)
		}
	}
}

func (lm *LevelManager) maybeScheduleRemoveDeadVersionEntries() error {
	log.Debugf("levelmanager.maybeScheduleRemoveDeadVersionEntries")
	if lm.removeDeadVersionsInProgress {
		log.Debugf("levelmanager.maybeScheduleRemoveDeadVersionEntries - already in progress")

		// We only process one remove dead versions at a time
		return nil
	}

	for {
		versionRange := lm.masterRecord.deadVersionRanges[0]

		// Scan for any tables that potentially have data that needs to be compacted out
		type compactInfo struct {
			level        int
			tableEntries [][]*TableEntry
		}
		scheduledAll := true
		var infos []compactInfo
		for level, entries := range lm.masterRecord.levelSegmentEntries {
			if entries.maxVersion < versionRange.VersionStart {
				continue
			}
			var tableEntries [][]*TableEntry
			iter, err := lm.levelIterator(level)
			if err != nil {
				return err
			}
			if level == 0 {
				// We compact the entire level, if at least one table matches dead version
				hasMatch := false
				entries := lm.getLevelSegmentEntries(0)
				if len(entries.segmentEntries) > 0 {
					l0SegmentEntry := entries.segmentEntries[0]
					l0Seg, err := lm.getSegment(l0SegmentEntry.segmentID)
					if err != nil {
						return err
					}
					tables := l0Seg.tableEntries
					for _, tableEntry := range tables {
						if tableEntry.MaxVersion >= versionRange.VersionStart && tableEntry.MinVersion <= versionRange.VersionEnd {
							hasMatch = true
							break
						}
					}
				}
				if hasMatch {
					var ok bool
					tableEntries, ok = lm.chooseL0TablesToCompact()
					if !ok {
						// We can't choose any L0 tables as there is a startup L0 compaction in progress, this doesn't
						// mean there are no dead version entries, so we won't remove the entry on completion
						scheduledAll = false
					}
				}
			} else {
				for {
					te, err := iter.Next()
					if err != nil {
						return err
					}
					if te == nil {
						break
					}
					// Only add the tables that match
					if te.MaxVersion >= versionRange.VersionStart && te.MinVersion <= versionRange.VersionEnd {
						tableEntries = append(tableEntries, []*TableEntry{te})
					}
				}
			}
			log.Debugf("level %d job has %d table entries", level, len(tableEntries))
			if len(tableEntries) > 0 {
				infos = append(infos, compactInfo{
					level:        level,
					tableEntries: tableEntries,
				})
			}
		}

		if len(infos) == 0 {
			log.Debugf("removal of dead version range: %v - no data to remove so doing nothing", versionRange)
			// Nothing to do, we can remove the dead version now
			lm.masterRecord.deadVersionRanges = lm.masterRecord.deadVersionRanges[1:]
			if len(lm.masterRecord.deadVersionRanges) > 0 {
				// Try with the next version range
				continue
			}
			return nil
		}

		cf := common.NewCountDownFuture(0, func(err error) {

			// This will be called with the compaction and lm locks held

			// Never called with error
			if err != nil {
				panic(err)
			}

			log.Debugf("removal of dead version range: %v completed scheduled all? %t", versionRange, scheduledAll)

			if scheduledAll {
				// We managed to schedule all compactions required to remove this version range so no more data for the
				// range can remain - we remove the dead version
				log.Debugf("dead version range %v removed on level manager", lm.masterRecord.deadVersionRanges[0])
				lm.masterRecord.deadVersionRanges = lm.masterRecord.deadVersionRanges[1:]
			}
			lm.removeDeadVersionsInProgress = false
		})

		scheduledCount := 0
		for _, info := range infos {
			count, hasLocked, err := lm.scheduleCompaction(info.level, info.tableEntries, []VersionRange{versionRange}, cf.CountDown)
			if err != nil {
				return err
			}
			if hasLocked {
				// If we couldn't compact everything we wanted because tables were locked then we didn't schedule all - there
				// is more compaction to do in order to remove dead version range.
				scheduledAll = false
			}
			scheduledCount += count
		}
		cf.SetCount(scheduledCount)

		if scheduledCount > 0 {
			log.Debugf("scheduled removal of dead version range: %v", versionRange)
			lm.removeDeadVersionsInProgress = true
		}

		return nil
	}
}

func (lm *LevelManager) lockTable(tableName string) {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	lm.lockedTables[tableName] = struct{}{}
}

func (lm *LevelManager) unlockTable(tableName string) {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	delete(lm.lockedTables, tableName)
}

func (lm *LevelManager) forceCompaction(level int, maxTables int) error {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	entries := lm.getLevelSegmentEntries(level)
	if len(entries.segmentEntries) == 0 {
		return nil
	}
	tables, err := lm.chooseTablesToCompact(level, maxTables)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}
	_, _, err = lm.scheduleCompaction(level, tables, nil, nil)
	return err
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
	level             int
	table             *TableEntry
	deadVersionRanges []VersionRange
}

type inProgressCompaction struct {
	timer        *common.TimerHandle
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
			buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(tableToCompact.deadVersionRanges)))
			for _, rng := range tableToCompact.deadVersionRanges {
				buff = rng.Serialize(buff)
			}
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

			var lvr uint32
			lvr, offset = encoding.ReadUint32FromBufferLE(buff, offset)
			var deadVersionRanges []VersionRange
			if lvr > 0 {
				deadVersionRanges = make([]VersionRange, lvr)
				for i := 0; i < int(lvr); i++ {
					offset = deadVersionRanges[i].Deserialize(buff, offset)
				}
			}

			tables2[j] = tableToCompact{
				level:             int(l),
				table:             te,
				deadVersionRanges: deadVersionRanges,
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
