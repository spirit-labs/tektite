package levels

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/retention"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
)

const maxTableSize = 1300

func TestPollerTimeout(t *testing.T) {
	pollerTimeout := 250 * time.Millisecond
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		cfg.CompactionPollerTimeout = &pollerTimeout
	})
	defer tearDown(t)

	start := time.Now()
	numPolls := 10
	chans := make([]chan error, 0, numPolls)
	for i := 0; i < numPolls; i++ {
		ch := make(chan error, 1)
		chans = append(chans, ch)
		lm.pollForJob(-1, func(job *CompactionJob, err error) {
			ch <- err
		})
	}
	for _, ch := range chans {
		err := <-ch
		require.Error(t, err)
		require.True(t, time.Now().Sub(start) >= pollerTimeout)
		var perr errors.TektiteError
		isTektiteError := errors.As(err, &perr)
		require.True(t, isTektiteError)
		require.Equal(t, errors.CompactionPollTimeout, int(perr.Code))
	}
}

func TestJobWithExpiredPrefixes(t *testing.T) {
	testJobWithExpiredPrefixes(t, 0)
	testJobWithExpiredPrefixes(t, 1)
}

func testJobWithExpiredPrefixes(t *testing.T, fromLevel int) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l0CompactionTrigger := 1
		l1CompactionTrigger := 1
		cfg.L0CompactionTrigger = &l0CompactionTrigger
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)
	// First register the prefixes
	prefix1 := []byte("prefix1")
	prefix2 := []byte("prefix2")
	prefix3 := []byte("prefix3")
	prefix4 := []byte("prefix4")
	prefixRetentions := []retention.PrefixRetention{
		{Prefix: prefix1, Retention: 1},
		{Prefix: prefix2, Retention: 100000},
		{Prefix: prefix3, Retention: 1},
		{Prefix: prefix4, Retention: 100000},
	}
	err := lm.RegisterPrefixRetentions(prefixRetentions, false, 0)
	require.NoError(t, err)

	now := uint64(time.Now().UTC().UnixMilli())

	registrations := []RegistrationEntry{
		{
			Level:        fromLevel,
			TableID:      []byte("sst1"),
			KeyStart:     []byte("prefix0/somekey321"),
			KeyEnd:       []byte("prefix9/somekey123"),
			CreationTime: now,
		},
		{
			Level:        fromLevel,
			TableID:      []byte("sst2"),
			KeyStart:     []byte("prefix0/somekey321"),
			KeyEnd:       []byte("prefix9/somekey123"),
			CreationTime: now,
		},
		{
			Level:        fromLevel + 1,
			TableID:      []byte("sst3"),
			KeyStart:     []byte("prefix0/somekey321"),
			KeyEnd:       []byte("prefix9/somekey123"),
			CreationTime: now,
		},
	}
	regBatch := RegistrationBatch{Registrations: registrations}
	err = lm.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)

	// Now wait a little while for the shorter expiration to expire
	time.Sleep(2 * time.Millisecond)

	expectedPrefixes := map[string]struct{}{
		string(prefix1): {},
		string(prefix3): {},
	}

	err = lm.MaybeScheduleCompaction()
	require.NoError(t, err)
	job, err := getJob(lm)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, fromLevel, job.levelFrom)
	if fromLevel == 0 {
		require.Equal(t, 3, len(job.tables))
		require.Equal(t, 1, len(job.tables[0]))
		require.Equal(t, 1, len(job.tables[1]))
		require.Equal(t, 1, len(job.tables[2]))
		tableToCompact1 := job.tables[0][0]
		checkPrefixExtensions(t, expectedPrefixes, tableToCompact1.expiredPrefixes)
		tableToCompact2 := job.tables[1][0]
		checkPrefixExtensions(t, expectedPrefixes, tableToCompact2.expiredPrefixes)
		tableToCompact3 := job.tables[2][0]
		checkPrefixExtensions(t, expectedPrefixes, tableToCompact3.expiredPrefixes)
	} else {
		require.Equal(t, 2, len(job.tables))
		require.Equal(t, 1, len(job.tables[0]))
		require.Equal(t, 1, len(job.tables[1]))
		tableToCompact1 := job.tables[0][0]
		checkPrefixExtensions(t, expectedPrefixes, tableToCompact1.expiredPrefixes)
		tableToCompact2 := job.tables[1][0]
		checkPrefixExtensions(t, expectedPrefixes, tableToCompact2.expiredPrefixes)
	}
}

func checkPrefixExtensions(t *testing.T, expected map[string]struct{}, prefixRetentions []retention.PrefixRetention) {
	require.Equal(t, len(expected), len(prefixRetentions))
	for _, pr := range prefixRetentions {
		require.Equal(t, 0, int(pr.Retention))
		_, ok := expected[string(pr.Prefix)]
		require.True(t, ok)
	}
}

func TestPollForJobWhenAlreadyInQueue(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 1
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)
	// populate level 1 and 2 with an overlap and level 1 having reached L1CompactionTrigger
	populateLevel(t, lm, 1, createTableEntry("sst1", 0, 9),
		createTableEntry("sst2", 10, 19))
	populateLevel(t, lm, 2, createTableEntry("sst3", 0, 19))
	err := lm.MaybeScheduleCompaction()
	require.NoError(t, err)
	stats := lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)
	require.Equal(t, 0, stats.InProgressJobs)
	job, err := getJob(lm)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, 1, job.levelFrom)
	require.Equal(t, 2, len(job.tables))
	require.Equal(t, 1, len(job.tables[0]))
	stats = lm.GetCompactionStats()
	require.Equal(t, 0, stats.QueuedJobs)
	require.Equal(t, 1, stats.InProgressJobs)
}

func TestPollForJobWhenNotAlreadyInQueue(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 1
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)
	// populate level 1 and 2 with an overlap and level 1 having reached L1CompactionTrigger
	populateLevel(t, lm, 1, createTableEntry("sst1", 0, 9),
		createTableEntry("sst2", 10, 19))
	populateLevel(t, lm, 2, createTableEntry("sst3", 0, 19))

	ch := make(chan pollResult, 1)
	lm.pollForJob(-1, func(job *CompactionJob, err error) {
		ch <- pollResult{job, err}
	})
	stats := lm.GetCompactionStats()
	require.Equal(t, 0, stats.QueuedJobs)
	require.Equal(t, 0, stats.InProgressJobs)

	go func() {
		time.Sleep(conf.DefaultCompactionPollerTimeout / 4)
		err := lm.MaybeScheduleCompaction()
		if err != nil {
			panic(err)
		}
	}()

	res := <-ch
	require.NoError(t, res.err)
	require.NotNil(t, res.job)
	require.Equal(t, 1, res.job.levelFrom)
	require.Equal(t, 2, len(res.job.tables))
	require.Equal(t, 1, len(res.job.tables[0]))

	stats = lm.GetCompactionStats()
	require.Equal(t, 0, stats.QueuedJobs)
	require.Equal(t, 1, stats.InProgressJobs)
}

func TestPollersGetJobsInOrder(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 1
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)

	populateLevel(t, lm, 1, createTableEntry("sst1", 0, 9))

	numPollers := 8

	for i := 0; i < numPollers; i++ {
		populateLevel(t, lm, 2, createTableEntry(fmt.Sprintf("sst-dest-%d", i+2), (i+1)*10,
			(i+1)*10+9))
	}

	var chans []chan pollResult
	for i := 0; i < numPollers; i++ {
		ch := make(chan pollResult, 1)
		lm.pollForJob(-1, func(job *CompactionJob, err error) {
			ch <- pollResult{job, err}
		})
		chans = append(chans, ch)
	}

	// Add more tables and schedule compaction - this should create jobs - these should be given direct to
	// the waiting pollers, so nothing should be in the job queue
	for i := 0; i < numPollers; i++ {
		populateLevel(t, lm, 1, createTableEntry(fmt.Sprintf("sst-%d", i+2),
			(i+1)*10, (i+1)*10+9))
	}
	err := lm.MaybeScheduleCompaction()
	require.NoError(t, err)
	stats := lm.GetCompactionStats()
	require.Equal(t, 0, stats.QueuedJobs)
	require.Equal(t, numPollers, stats.InProgressJobs)

	for _, ch := range chans {
		res := <-ch
		require.NoError(t, res.err)
		require.NotNil(t, res.job)
		require.Equal(t, 1, res.job.levelFrom)
		require.Equal(t, 2, len(res.job.tables))
		require.Equal(t, 1, len(res.job.tables[0]))
	}
}

func TestPollJobAndCompleteItLevel0To1(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l0CompactionTrigger := 1
		l1CompactionTrigger := 10
		cfg.L0CompactionTrigger = &l0CompactionTrigger
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)

	// create a bunch of overlapping tables on L0

	sst1_1 := createTableEntryWithDeleteRatio("sst1-1", 11, 50, 0.1)

	sst1_2 := createTableEntryWithDeleteRatio("sst1-2", 35, 40, 0.1)

	sst1_3 := createTableEntryWithDeleteRatio("sst1-3", 9, 22, 0.1)

	sst1_4 := createTableEntryWithDeleteRatio("sst1-4", 33, 48, 0.1)

	sst1_5 := createTableEntryWithDeleteRatio("sst1-5", 8, 31, 0.11)

	populateLevel(t, lm, 0, sst1_1, sst1_2, sst1_3, sst1_4, sst1_5)

	sst2_1 := createTableEntryWithDeleteRatio("sst2-1", 0, 3, 0.5)

	sst2_2 := createTableEntryWithDeleteRatio("sst2-2", 4, 7, 0.5)

	sst2_3 := createTableEntryWithDeleteRatio("sst2-3", 15, 30, 0.5)

	sst2_4 := createTableEntryWithDeleteRatio("sst2-4", 33, 45, 0.5)

	sst2_5 := createTableEntryWithDeleteRatio("sst2-5", 52, 99, 0.5)

	populateLevel(t, lm, 1, sst2_1, sst2_2, sst2_3, sst2_4, sst2_5)
	err := lm.MaybeScheduleCompaction()
	// this should create job with everything in L0 merging into sst2_3 and sst2_4
	require.NoError(t, err)

	stats := lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)

	job, err := getJob(lm)
	require.NoError(t, err)

	stats = lm.GetCompactionStats()
	require.Equal(t, 1, stats.InProgressJobs)
	require.Equal(t, 0, stats.QueuedJobs)

	require.False(t, job.isMove)
	require.Equal(t, 0, job.levelFrom)

	require.Equal(t, 6, len(job.tables))

	require.Equal(t, 1, len(job.tables[0]))
	require.Equal(t, "sst1-5", string(job.tables[0][0].table.SSTableID))
	require.Equal(t, []byte("key00008"), trimVersion(job.tables[0][0].table.RangeStart))
	require.Equal(t, []byte("key00031"), trimVersion(job.tables[0][0].table.RangeEnd))

	require.Equal(t, 1, len(job.tables[1]))
	require.Equal(t, "sst1-4", string(job.tables[1][0].table.SSTableID))
	require.Equal(t, []byte("key00033"), trimVersion(job.tables[1][0].table.RangeStart))
	require.Equal(t, []byte("key00048"), trimVersion(job.tables[1][0].table.RangeEnd))

	require.Equal(t, 1, len(job.tables[2]))
	require.Equal(t, "sst1-3", string(job.tables[2][0].table.SSTableID))
	require.Equal(t, []byte("key00009"), trimVersion(job.tables[2][0].table.RangeStart))
	require.Equal(t, []byte("key00022"), trimVersion(job.tables[2][0].table.RangeEnd))

	require.Equal(t, 1, len(job.tables[3]))
	require.Equal(t, "sst1-2", string(job.tables[3][0].table.SSTableID))
	require.Equal(t, []byte("key00035"), trimVersion(job.tables[3][0].table.RangeStart))
	require.Equal(t, []byte("key00040"), trimVersion(job.tables[3][0].table.RangeEnd))

	require.Equal(t, 1, len(job.tables[4]))
	require.Equal(t, "sst1-1", string(job.tables[4][0].table.SSTableID))
	require.Equal(t, []byte("key00011"), trimVersion(job.tables[4][0].table.RangeStart))
	require.Equal(t, []byte("key00050"), trimVersion(job.tables[4][0].table.RangeEnd))

	require.Equal(t, 2, len(job.tables[5]))
	require.Equal(t, "sst2-3", string(job.tables[5][0].table.SSTableID))
	require.Equal(t, []byte("key00015"), trimVersion(job.tables[5][0].table.RangeStart))
	require.Equal(t, []byte("key00030"), trimVersion(job.tables[5][0].table.RangeEnd))

	require.Equal(t, "sst2-4", string(job.tables[5][1].table.SSTableID))
	require.Equal(t, []byte("key00033"), trimVersion(job.tables[5][1].table.RangeStart))
	require.Equal(t, []byte("key00045"), trimVersion(job.tables[5][1].table.RangeEnd))

	newTables := []TableEntry{
		{
			SSTableID:   []byte("sst2-6"),
			RangeStart:  encoding.EncodeVersion([]byte("key00008"), 0),
			RangeEnd:    encoding.EncodeVersion([]byte("key00033"), 0),
			DeleteRatio: 1.23,
		},
		{
			SSTableID:   []byte("sst2-7"),
			RangeStart:  encoding.EncodeVersion([]byte("key00034"), 0),
			RangeEnd:    encoding.EncodeVersion([]byte("key00050"), 0),
			DeleteRatio: 1.33,
		},
	}

	sendCompactionComplete(t, lm, job, newTables)

	stats = lm.GetCompactionStats()
	require.Equal(t, 0, stats.InProgressJobs)
	require.Equal(t, 0, stats.QueuedJobs)

	// should be nothing left in level 0
	checkLevelEntries(t, lm, 0)

	expectedTables := []TableEntry{sst2_1, sst2_2, newTables[0], newTables[1], sst2_5}

	checkLevelEntries(t, lm, 1, expectedTables...)
}

func sendCompactionComplete(t *testing.T, lm *LevelManager, job *CompactionJob, newTables []TableEntry) {
	registrations, deRegistrations := changesToApply(newTables, job)
	regBatch := RegistrationBatch{
		ClusterName:     "test_cluster",
		Compaction:      true,
		JobID:           job.id,
		Registrations:   registrations,
		DeRegistrations: deRegistrations,
	}
	err := lm.ApplyChanges(regBatch, false, 0)
	require.NoError(t, err)
}

func TestPollJobAndCompleteItLevel0To1EmptyL1(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 1
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)
	// create a bunch of overlapping tables on L0

	sst1_1 := TableEntry{
		SSTableID:   []byte("sst1-1"),
		RangeStart:  encoding.EncodeVersion([]byte("key00011"), 0),
		RangeEnd:    encoding.EncodeVersion([]byte("key00050"), 0),
		DeleteRatio: 0.1,
	}
	sst1_2 := TableEntry{
		SSTableID:   []byte("sst1-2"),
		RangeStart:  encoding.EncodeVersion([]byte("key00035"), 0),
		RangeEnd:    encoding.EncodeVersion([]byte("key00040"), 0),
		DeleteRatio: 0.1,
	}
	sst1_3 := TableEntry{
		SSTableID:   []byte("sst1-3"),
		RangeStart:  encoding.EncodeVersion([]byte("key00009"), 0),
		RangeEnd:    encoding.EncodeVersion([]byte("key00022"), 0),
		DeleteRatio: 0.1,
	}
	sst1_4 := TableEntry{
		SSTableID:   []byte("sst1-4"),
		RangeStart:  encoding.EncodeVersion([]byte("key00033"), 0),
		RangeEnd:    encoding.EncodeVersion([]byte("key00048"), 0),
		DeleteRatio: 0.1,
	}
	sst1_5 := TableEntry{
		SSTableID:   []byte("sst1-5"),
		RangeStart:  encoding.EncodeVersion([]byte("key00008"), 0),
		RangeEnd:    encoding.EncodeVersion([]byte("key00031"), 0),
		DeleteRatio: 0.11,
	}
	populateLevel(t, lm, 0, sst1_1, sst1_2, sst1_3, sst1_4, sst1_5)

	err := lm.MaybeScheduleCompaction()
	// this should create job with everything in L0
	require.NoError(t, err)

	stats := lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)

	job, err := getJob(lm)
	require.NoError(t, err)

	stats = lm.GetCompactionStats()
	require.Equal(t, 1, stats.InProgressJobs)
	require.Equal(t, 0, stats.QueuedJobs)

	require.False(t, job.isMove)
	require.Equal(t, 0, job.levelFrom)

	require.Equal(t, 5, len(job.tables))

	require.Equal(t, 1, len(job.tables[0]))
	require.Equal(t, "sst1-5", string(job.tables[0][0].table.SSTableID))
	require.Equal(t, encoding.EncodeVersion([]byte("key00008"), 0), job.tables[0][0].table.RangeStart)
	require.Equal(t, encoding.EncodeVersion([]byte("key00031"), 0), job.tables[0][0].table.RangeEnd)

	require.Equal(t, 1, len(job.tables[1]))
	require.Equal(t, "sst1-4", string(job.tables[1][0].table.SSTableID))
	require.Equal(t, encoding.EncodeVersion([]byte("key00033"), 0), job.tables[1][0].table.RangeStart)
	require.Equal(t, encoding.EncodeVersion([]byte("key00048"), 0), job.tables[1][0].table.RangeEnd)

	require.Equal(t, 1, len(job.tables[2]))
	require.Equal(t, "sst1-3", string(job.tables[2][0].table.SSTableID))
	require.Equal(t, encoding.EncodeVersion([]byte("key00009"), 0), job.tables[2][0].table.RangeStart)
	require.Equal(t, encoding.EncodeVersion([]byte("key00022"), 0), job.tables[2][0].table.RangeEnd)

	require.Equal(t, 1, len(job.tables[3]))
	require.Equal(t, "sst1-2", string(job.tables[3][0].table.SSTableID))
	require.Equal(t, encoding.EncodeVersion([]byte("key00035"), 0), job.tables[3][0].table.RangeStart)
	require.Equal(t, encoding.EncodeVersion([]byte("key00040"), 0), job.tables[3][0].table.RangeEnd)

	require.Equal(t, 1, len(job.tables[4]))
	require.Equal(t, "sst1-1", string(job.tables[4][0].table.SSTableID))
	require.Equal(t, encoding.EncodeVersion([]byte("key00011"), 0), job.tables[4][0].table.RangeStart)
	require.Equal(t, encoding.EncodeVersion([]byte("key00050"), 0), job.tables[4][0].table.RangeEnd)

	newTables := []TableEntry{
		createTableEntryWithDeleteRatio("sst2-6", 8, 33, 1.23),
		createTableEntryWithDeleteRatio("sst2-7", 34, 50, 1.33),
	}

	sendCompactionComplete(t, lm, job, newTables)

	stats = lm.GetCompactionStats()
	require.Equal(t, 0, stats.InProgressJobs)
	require.Equal(t, 0, stats.QueuedJobs)

	// should be nothing left in level 0
	checkLevelEntries(t, lm, 0)

	checkLevelEntries(t, lm, 1, newTables...)
}

func TestPollJobAndCompleteItLevel1To2(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l0CompactionTrigger := 2
		levelMultiplier := 2
		cfg.L0CompactionTrigger = &l0CompactionTrigger
		cfg.LevelMultiplier = &levelMultiplier
	})
	defer tearDown(t)

	sst1_1 := createTableEntryWithDeleteRatio("sst1-1", 0, 1, 0.1)
	sst1_2 := createTableEntryWithDeleteRatio("sst1-2", 2, 3, 0.1)
	sst1_3 := createTableEntryWithDeleteRatio("sst1-3", 4, 5, 0.1)
	sst1_4 := createTableEntryWithDeleteRatio("sst1-4", 6, 7, 0.1)
	sst1_5 := createTableEntryWithDeleteRatio("sst1-5", 10, 19, 0.11)

	populateLevel(t, lm, 1, sst1_1, sst1_2, sst1_3, sst1_4, sst1_5)

	sst2_1 := createTableEntryWithDeleteRatio("sst2-1", 0, 4, 0.5)
	sst2_2 := createTableEntryWithDeleteRatio("sst2-2", 5, 9, 0.3)
	sst2_3 := createTableEntryWithDeleteRatio("sst2-3", 10, 14, 0.45)
	sst2_4 := createTableEntryWithDeleteRatio("sst2-4", 15, 20, 0.8)
	sst2_5 := createTableEntryWithDeleteRatio("sst2-5", 21, 24, 0.34)

	populateLevel(t, lm, 2, sst2_1, sst2_2, sst2_3, sst2_4, sst2_5)
	err := lm.MaybeScheduleCompaction()
	// this should create job from sst1-5 as has highest delete ratio and overlaps with next level
	require.NoError(t, err)

	stats := lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)

	job, err := getJob(lm)
	require.NoError(t, err)

	stats = lm.GetCompactionStats()
	require.Equal(t, 1, stats.InProgressJobs)
	require.Equal(t, 0, stats.QueuedJobs)

	require.False(t, job.isMove)
	require.Equal(t, 2, len(job.tables))
	require.Equal(t, 1, len(job.tables[0]))
	require.Equal(t, "sst1-5", string(job.tables[0][0].table.SSTableID))
	require.Equal(t, []byte("key00010"), trimVersion(job.tables[0][0].table.RangeStart))
	require.Equal(t, []byte("key00019"), trimVersion(job.tables[0][0].table.RangeEnd))

	require.Equal(t, 2, len(job.tables[1]))

	require.Equal(t, "sst2-3", string(job.tables[1][0].table.SSTableID))
	require.Equal(t, []byte("key00010"), trimVersion(job.tables[1][0].table.RangeStart))
	require.Equal(t, []byte("key00014"), trimVersion(job.tables[1][0].table.RangeEnd))

	require.Equal(t, "sst2-4", string(job.tables[1][1].table.SSTableID))
	require.Equal(t, []byte("key00015"), trimVersion(job.tables[1][1].table.RangeStart))
	require.Equal(t, []byte("key00020"), trimVersion(job.tables[1][1].table.RangeEnd))

	// try and complete with wrong id
	regBatch := RegistrationBatch{
		ClusterName: "test_cluster",
		Compaction:  true,
		JobID:       "unknown",
	}
	err = lm.ApplyChanges(regBatch, false, 0)
	require.Error(t, err)

	newTables := []TableEntry{
		createTableEntryWithDeleteRatio("sst2-6", 10, 15, 1.23),
		createTableEntryWithDeleteRatio("sst2-7", 16, 20, 1.23),
	}

	sendCompactionComplete(t, lm, job, newTables)

	stats = lm.GetCompactionStats()
	require.Equal(t, 0, stats.InProgressJobs)
	require.Equal(t, 0, stats.QueuedJobs)

	checkLevelEntries(t, lm, 1, sst1_1, sst1_2, sst1_3, sst1_4)

	expectedTables := []TableEntry{sst2_1, sst2_2, newTables[0], newTables[1], sst2_5}

	checkLevelEntries(t, lm, 2, expectedTables...)
}

func TestCompactionTimeout(t *testing.T) {
	compactionTimeout := 100 * time.Millisecond
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l0CompactionTrigger := 1
		l1CompactionTrigger := 1
		levelMultiplier := 1
		cfg.L0CompactionTrigger = &l0CompactionTrigger
		cfg.L1CompactionTrigger = &l1CompactionTrigger
		cfg.LevelMultiplier = &levelMultiplier
		cfg.CompactionJobTimeout = &compactionTimeout
	})
	defer tearDown(t)

	sst1 := TableEntry{
		SSTableID:   []byte("sst1"),
		RangeStart:  encoding.EncodeVersion([]byte("key00000"), 0),
		RangeEnd:    encoding.EncodeVersion([]byte("key00009"), 0),
		DeleteRatio: 0.1,
	}
	sst2 := TableEntry{
		SSTableID:   []byte("sst2"),
		RangeStart:  encoding.EncodeVersion([]byte("key00010"), 0),
		RangeEnd:    encoding.EncodeVersion([]byte("key00019"), 0),
		DeleteRatio: 0.11,
	}
	populateLevel(t, lm, 1, sst1, sst2)

	sst3 := TableEntry{
		SSTableID:   []byte("sst3"),
		RangeStart:  encoding.EncodeVersion([]byte("key00010"), 0),
		RangeEnd:    encoding.EncodeVersion([]byte("key00019"), 0),
		DeleteRatio: 0.5,
	}
	populateLevel(t, lm, 2, sst3)
	err := lm.MaybeScheduleCompaction()
	require.NoError(t, err)

	ch := make(chan pollResult, 1)
	lm.pollForJob(-1, func(job *CompactionJob, err error) {
		ch <- pollResult{job, err}
	})
	res := <-ch
	require.NoError(t, res.err)

	stats := lm.GetCompactionStats()
	require.Equal(t, 1, stats.InProgressJobs)
	require.Equal(t, 0, stats.QueuedJobs)

	// Make job timeout
	time.Sleep(2 * compactionTimeout)

	// should be added back on to queue
	stats = lm.GetCompactionStats()
	require.Equal(t, 0, stats.InProgressJobs)
	require.Equal(t, 1, stats.QueuedJobs)
	require.Equal(t, 1, stats.TimedOutJobs)
}

func TestTablesMovedToLastLevelWhenNoOverlapAndNoDeletes(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 1
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)

	sst1 := createTableEntryWithDeleteRatio("sst1", 0, 9, 0.0)
	sst2 := createTableEntryWithDeleteRatio("sst2", 10, 19, 0.0)

	populateLevel(t, lm, 1, sst1, sst2)

	err := lm.MaybeScheduleCompaction()
	require.NoError(t, err)

	stats := lm.GetCompactionStats()

	require.Equal(t, 1, stats.QueuedJobs)
	require.Equal(t, 0, stats.InProgressJobs)

	job, err := getJob(lm)
	require.NoError(t, err)

	require.Equal(t, true, job.isMove)
}

func TestTablesNotMovedToLastLevelWhenNoOverlapAndDeletes(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 1
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)

	sst1 := createTableEntryWithDeleteRatio("sst1", 0, 9, 0.1)
	sst2 := createTableEntryWithDeleteRatio("sst2", 10, 19, 0.1)

	populateLevel(t, lm, 1, sst1, sst2)

	err := lm.MaybeScheduleCompaction()
	require.NoError(t, err)

	stats := lm.GetCompactionStats()

	require.Equal(t, 1, stats.QueuedJobs)
	require.Equal(t, 0, stats.InProgressJobs)

	job, err := getJob(lm)
	require.NoError(t, err)

	require.Equal(t, false, job.isMove)
}

func TestTablesMovedToNonLastLevelWhenNoOverlapAndDeletes(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 1
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)

	sst1 := createTableEntryWithDeleteRatio("sst1", 0, 9, 0.1)
	sst2 := createTableEntryWithDeleteRatio("sst2", 10, 19, 0.1)
	populateLevel(t, lm, 1, sst1, sst2)

	sst3 := createTableEntryWithDeleteRatio("sst3", 20, 29, 0.0)
	populateLevel(t, lm, 2, sst3)

	sst4 := createTableEntryWithDeleteRatio("sst4", 30, 39, 0.0)
	populateLevel(t, lm, 3, sst4)

	err := lm.MaybeScheduleCompaction()
	require.NoError(t, err)

	stats := lm.GetCompactionStats()

	require.Equal(t, 1, stats.QueuedJobs)
	require.Equal(t, 0, stats.InProgressJobs)

	job, err := getJob(lm)
	require.NoError(t, err)

	require.Equal(t, true, job.isMove)
}

func TestFileLockingOnNextLevel(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 1
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)

	sst1_1 := createTableEntryWithDeleteRatio("sst1-1", 0, 9, 0.1)
	// level 1
	// overlaps with sst2_1 and sst2_2
	sst1_2 := createTableEntryWithDeleteRatio("sst1-2", 10, 19, 0.3)
	// overlaps with sst2_2
	sst1_3 := createTableEntryWithDeleteRatio("sst1-3", 20, 29, 0.5)

	populateLevel(t, lm, 1, sst1_1, sst1_2, sst1_3)

	sst2_1 := createTableEntryWithDeleteRatio("sst2-1", 5, 17, 0.4)

	sst2_2 := createTableEntryWithDeleteRatio("sst2-2", 18, 35, 0.4)

	populateLevel(t, lm, 2, sst2_1, sst2_2)

	err := lm.MaybeScheduleCompaction()
	require.NoError(t, err)

	// should be a single job from for sst1-3, as sst1-2 overlaps with same table and sst1-3 has higher delete ratio
	stats := lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)
	require.Equal(t, 0, stats.InProgressJobs)

	job, err := getJob(lm)
	require.NoError(t, err)
	require.Equal(t, 2, len(job.tables))
	require.Equal(t, 1, len(job.tables[0]))
	require.Equal(t, "sst1-3", string(job.tables[0][0].table.SSTableID))

	// complete the job - this should release locks and then sst1_3 will have a job created as next highest delete ratio
	sendCompactionComplete(t, lm, job, nil)

	job, err = getJob(lm)
	require.NoError(t, err)
	require.Equal(t, 2, len(job.tables))
	require.Equal(t, 1, len(job.tables[0]))
	require.Equal(t, "sst1-2", string(job.tables[0][0].table.SSTableID))
}

func TestFileLocking(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 1
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)

	sst1_1 := createTableEntryWithDeleteRatio("sst1-1", 0, 9, 0.1)
	// level 1
	// overlaps with sst2_1 and sst2_2
	sst1_2 := createTableEntryWithDeleteRatio("sst1-2", 10, 19, 0.3)
	// overlaps with sst2_2
	sst1_3 := createTableEntryWithDeleteRatio("sst1-3", 20, 29, 0.5)
	populateLevel(t, lm, 1, sst1_1, sst1_2, sst1_3)

	sst2_1 := createTableEntryWithDeleteRatio("sst2-1", 5, 17, 0.4)
	sst2_2 := createTableEntryWithDeleteRatio("sst2-2", 18, 35, 0.4)

	populateLevel(t, lm, 2, sst2_1, sst2_2)

	err := lm.MaybeScheduleCompaction()
	require.NoError(t, err)

	// should be a single job from for sst1-3, as sst1-2 overlaps with same table and sst1-3 has higher delete ratio
	stats := lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)
	require.Equal(t, 0, stats.InProgressJobs)

	job, err := getJob(lm)
	require.NoError(t, err)
	require.Equal(t, 2, len(job.tables))
	require.Equal(t, 1, len(job.tables[0]))
	require.Equal(t, "sst1-3", string(job.tables[0][0].table.SSTableID))

	// complete the job - this should release locks and then sst1_3 will have a job created as next highest delete ratio
	sendCompactionComplete(t, lm, job, nil)

	job, err = getJob(lm)
	require.NoError(t, err)
	require.Equal(t, 2, len(job.tables))
	require.Equal(t, 1, len(job.tables[0]))
	require.Equal(t, "sst1-2", string(job.tables[0][0].table.SSTableID))
}

func TestDeleteSSTablesAfterCompaction(t *testing.T) {
	deleteCheckPeriod := 100 * time.Millisecond
	deleteDelay := 100 * time.Millisecond
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l0CompactionTrigger := 2
		levelMultiplier := 2
		cfg.L0CompactionTrigger = &l0CompactionTrigger
		cfg.LevelMultiplier = &levelMultiplier
		cfg.SSTableDeleteCheckInterval = &deleteCheckPeriod
		cfg.SSTableDeleteDelay = &deleteDelay
	})
	defer tearDown(t)

	err := lm.objStore.Put([]byte("sst1-1"), []byte("foo"))
	require.NoError(t, err)
	err = lm.objStore.Put([]byte("sst1-2"), []byte("foo"))
	require.NoError(t, err)
	err = lm.objStore.Put([]byte("sst1-3"), []byte("foo"))
	require.NoError(t, err)
	err = lm.objStore.Put([]byte("sst1-4"), []byte("foo"))
	require.NoError(t, err)
	err = lm.objStore.Put([]byte("sst1-5"), []byte("foo"))
	require.NoError(t, err)

	err = lm.objStore.Put([]byte("sst2-1"), []byte("foo"))
	require.NoError(t, err)
	err = lm.objStore.Put([]byte("sst2-2"), []byte("foo"))
	require.NoError(t, err)
	err = lm.objStore.Put([]byte("sst2-3"), []byte("foo"))
	require.NoError(t, err)
	err = lm.objStore.Put([]byte("sst2-4"), []byte("foo"))
	require.NoError(t, err)
	err = lm.objStore.Put([]byte("sst2-5"), []byte("foo"))
	require.NoError(t, err)

	sst1_1 := createTableEntryWithDeleteRatio("sst1-1", 0, 1, 0.1)
	sst1_2 := createTableEntryWithDeleteRatio("sst1-2", 2, 3, 0.1)
	sst1_3 := createTableEntryWithDeleteRatio("sst1-3", 4, 5, 0.1)
	sst1_4 := createTableEntryWithDeleteRatio("sst1-4", 6, 7, 0.1)
	sst1_5 := createTableEntryWithDeleteRatio("sst1-5", 10, 19, 0.11)

	populateLevel(t, lm, 1, sst1_1, sst1_2, sst1_3, sst1_4, sst1_5)

	sst2_1 := createTableEntryWithDeleteRatio("sst2-1", 0, 4, 0.5)
	sst2_2 := createTableEntryWithDeleteRatio("sst2-2", 5, 9, 0.3)
	sst2_3 := createTableEntryWithDeleteRatio("sst2-3", 10, 14, 0.45)
	sst2_4 := createTableEntryWithDeleteRatio("sst2-4", 15, 20, 0.8)
	sst2_5 := createTableEntryWithDeleteRatio("sst2-5", 21, 24, 0.34)

	populateLevel(t, lm, 2, sst2_1, sst2_2, sst2_3, sst2_4, sst2_5)
	err = lm.MaybeScheduleCompaction()
	// this should create job from sst1-5 as has highest delete ratio and overlaps with next level
	require.NoError(t, err)
	stats := lm.GetCompactionStats()

	require.Equal(t, 1, stats.QueuedJobs)

	job, err := getJob(lm)
	require.NoError(t, err)

	newTables := []TableEntry{
		createTableEntryWithDeleteRatio("sst2-6", 10, 15, 1.23),
		createTableEntryWithDeleteRatio("sst2-7", 16, 20, 1.23),
	}

	sendCompactionComplete(t, lm, job, newTables)

	stats = lm.GetCompactionStats()
	require.Equal(t, 0, stats.InProgressJobs)
	require.Equal(t, 0, stats.QueuedJobs)

	checkLevelEntries(t, lm, 1, sst1_1, sst1_2, sst1_3, sst1_4)

	expectedTables := []TableEntry{sst2_1, sst2_2, newTables[0], newTables[1], sst2_5}

	checkLevelEntries(t, lm, 2, expectedTables...)

	// sst1_5, sst2_3, sst2_4 should be deleted after a delay

	time.Sleep(2*deleteCheckPeriod + 2*deleteDelay)

	v, err := lm.objStore.Get([]byte("sst1-5"))
	require.NoError(t, err)
	require.Nil(t, v)

	v, err = lm.objStore.Get([]byte("sst2-3"))
	require.NoError(t, err)
	require.Nil(t, v)

	v, err = lm.objStore.Get([]byte("sst2-4"))
	require.NoError(t, err)
	require.Nil(t, v)

	// others should still be there

	v, err = lm.objStore.Get([]byte("sst1-1"))
	require.NoError(t, err)
	require.NotNil(t, v)

	v, err = lm.objStore.Get([]byte("sst1-2"))
	require.NoError(t, err)
	require.NotNil(t, v)

	v, err = lm.objStore.Get([]byte("sst1-3"))
	require.NoError(t, err)
	require.NotNil(t, v)

	v, err = lm.objStore.Get([]byte("sst1-4"))
	require.NoError(t, err)
	require.NotNil(t, v)

	v, err = lm.objStore.Get([]byte("sst2-1"))
	require.NoError(t, err)
	require.NotNil(t, v)

	v, err = lm.objStore.Get([]byte("sst2-2"))
	require.NoError(t, err)
	require.NotNil(t, v)

	v, err = lm.objStore.Get([]byte("sst2-5"))
	require.NoError(t, err)
	require.NotNil(t, v)
}

func TestChooseLevelToCompact0(t *testing.T) {
	testChooseLevelToCompact(t, 0, func(lm *LevelManager) {
		addTablesToLevel(t, lm, 0, 10, 0, 10)
		addTablesToLevel(t, lm, 1, 8, 0, 10)
		addTablesToLevel(t, lm, 2, 5, 0, 10)
	})
}

func TestChooseLevelToCompact1(t *testing.T) {
	testChooseLevelToCompact(t, 1, func(lm *LevelManager) {
		addTablesToLevel(t, lm, 0, 5, 0, 10)
		addTablesToLevel(t, lm, 1, 11, 0, 10)
		addTablesToLevel(t, lm, 2, 4, 0, 10)
	})
}

func testChooseLevelToCompact(t *testing.T, level int, adderFunc func(lm *LevelManager)) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l0CompactionTrigger := 1
		l1CompactionTrigger := 1
		levelMultiplier := 1
		cfg.L0CompactionTrigger = &l0CompactionTrigger
		cfg.L1CompactionTrigger = &l1CompactionTrigger
		cfg.LevelMultiplier = &levelMultiplier
	})
	defer tearDown(t)

	adderFunc(lm)

	err := lm.MaybeScheduleCompaction()
	require.NoError(t, err)
	job, err := getJob(lm)
	require.NoError(t, err)

	require.Equal(t, level, job.levelFrom)
}

func TestChooseTableToCompact(t *testing.T) {
	testChooseTableToCompact(t, 0.7, 0.3, 0.1, 0.7, 0.4, 0.25)
	testChooseTableToCompact(t, 0.8, 0.8, 0.2, 0.6, 0.3, 0.25)
	testChooseTableToCompact(t, 0.5, 0.1, 0.2, 0.5, 0.3, 0.25)
}

func testChooseTableToCompact(t *testing.T, expectedRatio float64, deleteRatios ...float64) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l0CompactionTrigger := 1
		l1CompactionTrigger := 1
		levelMultiplier := 1
		cfg.L0CompactionTrigger = &l0CompactionTrigger
		cfg.L1CompactionTrigger = &l1CompactionTrigger
		cfg.LevelMultiplier = &levelMultiplier
	})
	defer tearDown(t)

	addTablesToLevel(t, lm, 0, 3, 0, 10)
	addTablesToLevel(t, lm, 1, 5, 0, 10, deleteRatios...)
	addTablesToLevel(t, lm, 2, 4, 0, 10)

	err := lm.MaybeScheduleCompaction()
	require.NoError(t, err)
	job, err := getJob(lm)
	require.NoError(t, err)

	require.Equal(t, 1, job.levelFrom)
	require.Equal(t, expectedRatio, job.tables[0][0].table.DeleteRatio)
}

func addTablesToLevel(t *testing.T, lm *LevelManager, level int, numTables int, rangeStart int, rangePerLevel int, deleteRatios ...float64) {
	var entries []TableEntry
	for i := 0; i < numTables; i++ {
		var dr float64
		if len(deleteRatios) == 0 {
			dr = 0.0
		} else {
			dr = deleteRatios[i]
		}
		te := TableEntry{
			SSTableID:   []byte(fmt.Sprintf("sst-%d-%d", level, i)),
			RangeStart:  encoding.EncodeVersion([]byte(fmt.Sprintf("key-%05d", rangeStart)), 0),
			RangeEnd:    encoding.EncodeVersion([]byte(fmt.Sprintf("key-%05d", rangeStart+rangePerLevel-1)), 0),
			DeleteRatio: dr,
		}
		entries = append(entries, te)
	}
	populateLevel(t, lm, level, entries...)
}

func getJob(lm *LevelManager) (*CompactionJob, error) {
	ch := make(chan pollResult, 1)
	lm.pollForJob(-1, func(job *CompactionJob, err error) {
		ch <- pollResult{job, err}
	})
	res := <-ch
	return res.job, res.err
}

func checkLevelEntries(t *testing.T, lm *LevelManager, level int, entries ...TableEntry) {
	iter, err := lm.LevelIterator(level)
	require.NoError(t, err)
	if iter == nil {
		require.Equal(t, 0, len(entries))
		return
	}
	for _, expectedTe := range entries {
		te, err := iter.Next()
		require.NoError(t, err)
		require.NotNil(t, te)
		require.Equal(t, expectedTe, *te)
	}
	te, err := iter.Next()
	require.NoError(t, err)
	require.Nil(t, te)
}

type pollResult struct {
	job *CompactionJob
	err error
}

func populateLevel(t *testing.T, lm *LevelManager, level int, entries ...TableEntry) {
	var registrations []RegistrationEntry
	for _, te := range entries {
		registrations = append(registrations, RegistrationEntry{
			Level:       level,
			TableID:     te.SSTableID,
			KeyStart:    te.RangeStart,
			KeyEnd:      te.RangeEnd,
			DeleteRatio: te.DeleteRatio,
		})
	}
	regBatch := RegistrationBatch{Registrations: registrations}
	err := lm.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
}

func TestMergeInterleaved(t *testing.T) {
	builder1 := newSSTableBuilder()
	for i := 0; i < 50; i += 2 {
		builder1.addEntry(fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i))
	}
	sst1, err := builder1.build()
	require.NoError(t, err)

	builder2 := newSSTableBuilder()
	for i := 50; i < 100; i += 2 {
		builder2.addEntry(fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i))
	}
	sst2, err := builder2.build()
	require.NoError(t, err)

	builder3 := newSSTableBuilder()
	for i := 1; i < 50; i += 2 {
		builder3.addEntry(fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i))
	}
	sst3, err := builder3.build()
	require.NoError(t, err)

	builder4 := newSSTableBuilder()
	for i := 51; i < 100; i += 2 {
		builder4.addEntry(fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i))
	}
	sst4, err := builder4.build()
	require.NoError(t, err)

	res, err := mergeSSTables(common.DataFormatV1,
		[][]tableToMerge{{{sst: sst1}, {sst: sst2}}, {{sst: sst3}, {sst: sst4}}}, true,
		1300, math.MaxInt64, "")
	require.NoError(t, err)
	require.Equal(t, 4, len(res))
	for i := 0; i < 4; i++ {
		require.Equal(t, 25, res[i].sst.NumEntries())
	}

	checkKVsInRange(t, "val", res[0].sst, 0, 25)
	checkKVsInRange(t, "val", res[1].sst, 25, 50)
	checkKVsInRange(t, "val", res[2].sst, 50, 75)
	checkKVsInRange(t, "val", res[3].sst, 75, 100)
}

func TestMergeNoOverlap(t *testing.T) {
	builder1 := newSSTableBuilder()
	for i := 0; i < 25; i++ {
		builder1.addEntry(fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i))
	}
	sst1, err := builder1.build()
	require.NoError(t, err)

	builder2 := newSSTableBuilder()
	for i := 25; i < 50; i++ {
		builder2.addEntry(fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i))
	}
	sst2, err := builder2.build()
	require.NoError(t, err)

	builder3 := newSSTableBuilder()
	for i := 50; i < 75; i++ {
		builder3.addEntry(fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i))
	}
	sst3, err := builder3.build()
	require.NoError(t, err)

	builder4 := newSSTableBuilder()
	for i := 75; i < 100; i++ {
		builder4.addEntry(fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i))
	}
	sst4, err := builder4.build()
	require.NoError(t, err)

	res, err := mergeSSTables(common.DataFormatV1,
		[][]tableToMerge{{{sst: sst1}, {sst: sst2}}, {{sst: sst3}, {sst: sst4}}}, true,
		1300, math.MaxInt64, "")
	require.NoError(t, err)
	require.Equal(t, 4, len(res))
	for i := 0; i < 4; i++ {
		require.Equal(t, 25, res[i].sst.NumEntries())
	}

	checkKVsInRange(t, "val", res[0].sst, 0, 25)
	checkKVsInRange(t, "val", res[1].sst, 25, 50)
	checkKVsInRange(t, "val", res[2].sst, 50, 75)
	checkKVsInRange(t, "val", res[3].sst, 75, 100)
}

func TestOverwriteEntriesWithLaterVersionFirst(t *testing.T) {
	builder1 := newSSTableBuilder()
	for i := 0; i < 25; i++ {
		builder1.addEntryWithVersion(fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i), 10)
	}
	sst1, err := builder1.build()
	require.NoError(t, err)

	builder2 := newSSTableBuilder()
	for i := 25; i < 50; i++ {
		builder2.addEntryWithVersion(fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i), 10)
	}
	sst2, err := builder2.build()
	require.NoError(t, err)

	builder3 := newSSTableBuilder()
	for i := 25; i < 50; i++ {
		builder3.addEntryWithVersion(fmt.Sprintf("key%05d", i), fmt.Sprintf("bal%05d", i), 20)
	}
	sst3, err := builder3.build()
	require.NoError(t, err)

	builder4 := newSSTableBuilder()
	for i := 50; i < 75; i++ {
		builder4.addEntryWithVersion(fmt.Sprintf("key%05d", i), fmt.Sprintf("bal%05d", i), 20)
	}
	sst4, err := builder4.build()
	require.NoError(t, err)

	res, err := mergeSSTables(common.DataFormatV1,
		[][]tableToMerge{{{sst: sst1}, {sst: sst2}}, {{sst: sst3}, {sst: sst4}}}, true,
		maxTableSize, math.MaxInt64, "")
	require.NoError(t, err)
	require.Equal(t, 3, len(res))
	for i := 0; i < 3; i++ {
		require.Equal(t, 25, res[i].sst.NumEntries())
	}

	checkKVsInRange(t, "val", res[0].sst, 0, 25)
	checkKVsInRange(t, "bal", res[1].sst, 25, 50)
	checkKVsInRange(t, "bal", res[2].sst, 50, 75)
}

func TestOverwriteEntriesWithLaterVersionLast(t *testing.T) {
	builder1 := newSSTableBuilder()
	for i := 0; i < 25; i++ {
		builder1.addEntryWithVersion(fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i), 10)
	}
	sst1, err := builder1.build()
	require.NoError(t, err)

	builder2 := newSSTableBuilder()
	for i := 25; i < 50; i++ {
		builder2.addEntryWithVersion(fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i), 10)
	}
	sst2, err := builder2.build()
	require.NoError(t, err)

	builder3 := newSSTableBuilder()
	for i := 25; i < 50; i++ {
		builder3.addEntryWithVersion(fmt.Sprintf("key%05d", i), fmt.Sprintf("bal%05d", i), 20)
	}
	sst3, err := builder3.build()
	require.NoError(t, err)

	builder4 := newSSTableBuilder()
	for i := 50; i < 75; i++ {
		builder4.addEntryWithVersion(fmt.Sprintf("key%05d", i), fmt.Sprintf("bal%05d", i), 20)
	}
	sst4, err := builder4.build()
	require.NoError(t, err)

	res, err := mergeSSTables(common.DataFormatV1, [][]tableToMerge{{{sst: sst1}, {sst: sst2}}, {{sst: sst3}, {sst: sst4}}},
		true, maxTableSize, math.MaxInt64, "")
	require.NoError(t, err)
	require.Equal(t, 3, len(res))
	for i := 0; i < 3; i++ {
		require.Equal(t, 25, res[i].sst.NumEntries())
	}

	checkKVsInRange(t, "val", res[0].sst, 0, 25)
	checkKVsInRange(t, "bal", res[1].sst, 25, 50)
	checkKVsInRange(t, "bal", res[2].sst, 50, 75)
}

func TestMergePreserveTombstones(t *testing.T) {
	builder1 := newSSTableBuilder()
	builder1.addEntry("key00000", "val00000")
	builder1.addTombstone("key00001")
	sst1, err := builder1.build()
	require.NoError(t, err)

	builder2 := newSSTableBuilder()
	builder2.addEntry("key00002", "val00002")
	builder2.addTombstone("key00003")
	sst2, err := builder2.build()
	require.NoError(t, err)

	builder3 := newSSTableBuilder()
	builder3.addTombstone("key00000")
	builder3.addEntry("key00001", "val00001")
	sst3, err := builder3.build()
	require.NoError(t, err)

	builder4 := newSSTableBuilder()
	builder4.addTombstone("key00002")
	builder4.addEntry("key00003", "val00003")
	sst4, err := builder4.build()
	require.NoError(t, err)

	res, err := mergeSSTables(common.DataFormatV1,
		[][]tableToMerge{{{sst: sst1}, {sst: sst2}}, {{sst: sst3}, {sst: sst4}}}, true, maxTableSize,
		math.MaxInt64, "")
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	checkKVs(t, res[0].sst, "val", 0, 0, 1, -1, 2, 2, 3, -1)
}

func TestMergePreserveTombstonesAllEntriesRemoved(t *testing.T) {
	builder1 := newSSTableBuilder()
	builder1.addEntryWithVersion("key00000", "val00000", 1)
	builder1.addTombstoneWithVersion("key00001", 3)
	sst1, err := builder1.build()
	require.NoError(t, err)

	builder2 := newSSTableBuilder()
	builder2.addEntryWithVersion("key00002", "val00002", 1)
	builder2.addTombstoneWithVersion("key00003", 3)
	sst2, err := builder2.build()
	require.NoError(t, err)

	builder3 := newSSTableBuilder()
	builder3.addTombstoneWithVersion("key00000", 2)
	builder3.addEntryWithVersion("key00001", "val00001", 2)
	sst3, err := builder3.build()
	require.NoError(t, err)

	builder4 := newSSTableBuilder()
	builder4.addTombstoneWithVersion("key00002", 2)
	builder4.addEntryWithVersion("key00003", "val00003", 2)
	sst4, err := builder4.build()
	require.NoError(t, err)

	res, err := mergeSSTables(common.DataFormatV1, [][]tableToMerge{{{sst: sst1}, {sst: sst2}}, {{sst: sst3}, {sst: sst4}}},
		true, maxTableSize, math.MaxInt64, "")
	require.NoError(t, err)
	require.Equal(t, 1, len(res))

	// just the tombstones should remain
	checkKVs(t, res[0].sst, "val", 0, -1, 1, -1, 2, -1, 3, -1)
}

func TestMergePreserveTombstonesNotAllEntriesRemoved(t *testing.T) {
	builder1 := newSSTableBuilder()
	builder1.addEntryWithVersion("key00000", "val00000", 1)
	builder1.addTombstoneWithVersion("key00001", 1)
	sst1, err := builder1.build()
	require.NoError(t, err)

	builder2 := newSSTableBuilder()
	builder2.addEntryWithVersion("key00002", "val00002", 1)
	builder2.addTombstoneWithVersion("key00003", 1)
	sst2, err := builder2.build()
	require.NoError(t, err)

	builder3 := newSSTableBuilder()
	builder3.addTombstoneWithVersion("key00000", 2)
	builder3.addEntryWithVersion("key00001", "val00001", 2)
	sst3, err := builder3.build()
	require.NoError(t, err)

	builder4 := newSSTableBuilder()
	builder4.addTombstoneWithVersion("key00002", 2)
	builder4.addEntryWithVersion("key00003", "val00003", 2)
	sst4, err := builder4.build()
	require.NoError(t, err)

	res, err := mergeSSTables(common.DataFormatV1, [][]tableToMerge{{{sst: sst1}, {sst: sst2}}, {{sst: sst3}, {sst: sst4}}},
		true, maxTableSize, math.MaxInt64, "")
	require.NoError(t, err)
	require.Equal(t, 1, len(res))

	checkKVs(t, res[0].sst, "val", 0, -1, 1, 1, 2, -1, 3, 3)
}

func TestMergeNotPreserveTombstonesAllEntriesRemoved(t *testing.T) {
	builder1 := newSSTableBuilder()
	builder1.addEntryWithVersion("key00000", "val00000", 1)
	builder1.addTombstoneWithVersion("key00001", 3)
	sst1, err := builder1.build()
	require.NoError(t, err)

	builder2 := newSSTableBuilder()
	builder2.addEntryWithVersion("key00002", "val00002", 1)
	builder2.addTombstoneWithVersion("key00003", 3)
	sst2, err := builder2.build()
	require.NoError(t, err)

	builder3 := newSSTableBuilder()
	builder3.addTombstoneWithVersion("key00000", 2)
	builder3.addEntryWithVersion("key00001", "val00001", 2)
	sst3, err := builder3.build()
	require.NoError(t, err)

	builder4 := newSSTableBuilder()
	builder4.addTombstoneWithVersion("key00002", 2)
	builder4.addEntryWithVersion("key00003", "val00003", 2)
	sst4, err := builder4.build()
	require.NoError(t, err)

	res, err := mergeSSTables(common.DataFormatV1, [][]tableToMerge{{{sst: sst1}, {sst: sst2}}, {{sst: sst3}, {sst: sst4}}},
		false, maxTableSize, math.MaxInt64, "")
	require.NoError(t, err)
	require.Equal(t, 0, len(res))
}

func TestMergeNotPreserveTombstonesNotAllEntriesRemoved(t *testing.T) {
	builder1 := newSSTableBuilder()
	builder1.addEntryWithVersion("key00000", "val00000", 1)
	builder1.addTombstoneWithVersion("key00001", 1)
	sst1, err := builder1.build()
	require.NoError(t, err)

	builder2 := newSSTableBuilder()
	builder2.addEntryWithVersion("key00002", "val00002", 1)
	builder2.addTombstoneWithVersion("key00003", 1)
	sst2, err := builder2.build()
	require.NoError(t, err)

	builder3 := newSSTableBuilder()
	builder3.addTombstoneWithVersion("key00000", 2)
	builder3.addEntryWithVersion("key00001", "val00001", 2)
	sst3, err := builder3.build()
	require.NoError(t, err)

	builder4 := newSSTableBuilder()
	builder4.addTombstoneWithVersion("key00002", 2)
	builder4.addEntryWithVersion("key00003", "val00003", 2)
	sst4, err := builder4.build()
	require.NoError(t, err)

	res, err := mergeSSTables(common.DataFormatV1, [][]tableToMerge{{{sst: sst1}, {sst: sst2}}, {{sst: sst3}, {sst: sst4}}},
		false, maxTableSize, math.MaxInt64, "")
	require.NoError(t, err)
	require.Equal(t, 1, len(res))

	checkKVs(t, res[0].sst, "val", 1, 1, 3, 3)
}

func TestMergeAndDeletePrefixes(t *testing.T) {
	builder1 := newSSTableBuilder()
	var prefixes []retention.PrefixRetention
	for i := 0; i < 10; i++ {
		prefix := fmt.Sprintf("prefix-%05d", i)
		prefixes = append(prefixes, retention.PrefixRetention{Prefix: []byte(prefix)})
		for j := 0; j < 10; j += 2 {
			builder1.addEntry(fmt.Sprintf("%s-key-%05d", prefix, j), fmt.Sprintf("%s-val-%05d", prefix, j))
		}
	}
	builder2 := newSSTableBuilder()
	for i := 0; i < 10; i++ {
		prefix := fmt.Sprintf("prefix-%05d", i)
		for j := 1; j < 10; j += 2 {
			builder2.addEntry(fmt.Sprintf("%s-key-%05d", prefix, j), fmt.Sprintf("%s-val-%05d", prefix, j))
		}
	}
	sst1, err := builder1.build()
	require.NoError(t, err)
	sst2, err := builder2.build()
	require.NoError(t, err)

	var prefixesToKeep = []retention.PrefixRetention{prefixes[0], prefixes[2], prefixes[3], prefixes[4], prefixes[6], prefixes[8], prefixes[9]}
	var prefixesToRemove = []retention.PrefixRetention{prefixes[1], prefixes[5], prefixes[7]}

	tableToMerge1 := tableToMerge{
		prefixRetentions: prefixesToRemove,
		sst:              sst1,
	}
	tableToMerge2 := tableToMerge{
		prefixRetentions: prefixesToRemove,
		sst:              sst2,
	}

	res, err := mergeSSTables(common.DataFormatV1, [][]tableToMerge{{tableToMerge1}, {tableToMerge2}},
		false, 3500, math.MaxInt64, "")
	require.NoError(t, err)
	require.Equal(t, 2, len(res))

	iter1, err := res[0].sst.NewIterator(nil, nil)
	require.NoError(t, err)
	iter2, err := res[1].sst.NewIterator(nil, nil)
	require.NoError(t, err)
	iter := iteration.NewChainingIterator([]iteration.Iterator{iter1, iter2})

	for _, prefix := range prefixesToKeep {
		for j := 0; j < 10; j++ {
			valid, err := iter.IsValid()
			require.NoError(t, err)
			require.True(t, valid)
			curr := iter.Current()
			require.Equal(t, fmt.Sprintf("%s-key-%05d", prefix.Prefix, j), string(curr.Key[:len(curr.Key)-8]))
			err = iter.Next()
			require.NoError(t, err)
		}
	}
	valid, err := iter.IsValid()
	require.NoError(t, err)
	require.False(t, valid)
}

func TestMergeDeadVersions(t *testing.T) {
	builder1 := newSSTableBuilder()
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%05d", i)
		val := fmt.Sprintf("val-%05d", i)
		builder1.addEntryWithVersion(key, val, uint64(i))
	}
	deadRange1 := VersionRange{
		VersionStart: 3,
		VersionEnd:   5,
	}
	deadRange2 := VersionRange{
		VersionStart: 13,
		VersionEnd:   17,
	}
	builder2 := newSSTableBuilder()
	for i := 10; i < 20; i++ {
		key := fmt.Sprintf("key-%05d", i)
		val := fmt.Sprintf("val-%05d", i)
		builder2.addEntryWithVersion(key, val, uint64(i))
	}
	sst1, err := builder1.build()
	require.NoError(t, err)
	sst2, err := builder2.build()
	require.NoError(t, err)

	tableToMerge1 := tableToMerge{
		deadVersionRanges: []VersionRange{deadRange1, deadRange2},
		sst:               sst1,
	}
	tableToMerge2 := tableToMerge{
		deadVersionRanges: []VersionRange{deadRange1, deadRange2},
		sst:               sst2,
	}

	res, err := mergeSSTables(common.DataFormatV1, [][]tableToMerge{{tableToMerge1}, {tableToMerge2}},
		false, 3500, math.MaxInt64, "")
	require.NoError(t, err)
	require.Equal(t, 1, len(res))

	iter, err := res[0].sst.NewIterator(nil, nil)
	require.NoError(t, err)
	i := 0
	for i < 20 {
		valid, err := iter.IsValid()
		require.NoError(t, err)
		if !valid {
			break
		}
		entry := iter.Current()
		ver := math.MaxUint64 - binary.BigEndian.Uint64(entry.Key[len(entry.Key)-8:])
		require.Equal(t, uint64(i), ver)

		keyNoVer := entry.Key[:len(entry.Key)-8]
		expectedKey := []byte(fmt.Sprintf("key-%05d", i))

		require.Equal(t, expectedKey, keyNoVer)

		i++

		if i == 3 {
			i = 6
		} else if i == 13 {
			i = 18
		}

		err = iter.Next()
		require.NoError(t, err)
	}
	valid, err := iter.IsValid()
	require.NoError(t, err)
	require.False(t, valid)
}

func TestScoreHeap(t *testing.T) {
	testScoreHeap(t, []float64{0.1, 0.5, 0.4, 0, 0.2, 0.3}, []float64{0.5, 0.4, 0.3}, 3)
	testScoreHeap(t, []float64{0.1, 0.5, 0.4, 0, 0.2, 0.3, 0.7, 0.25, 0.6}, []float64{0.7, 0.6}, 2)
	testScoreHeap(t, []float64{0.1, 0.5, 0.4, 0, 0.2, 0.3, 0.7, 0.25, 0.6}, []float64{0.7, 0.6, 0.5, 0.4, 0.3, 0.25, 0.2, 0.1, 0}, 9)
}

func testScoreHeap(t *testing.T, in []float64, expected []float64, n int) {
	h := &scoreHeap{}
	heap.Init(h)
	for _, ratio := range in {
		heap.Push(h, scoreEntry{
			tableEntry: &TableEntry{DeleteRatio: ratio},
			score:      ratio,
		})
		if h.Len() > n {
			heap.Pop(h)
		}
	}
	out := make([]float64, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		se := heap.Pop(h).(scoreEntry)
		out[i] = se.score
	}

	require.Equal(t, expected, out)
}

func TestMaxSizeIterator(t *testing.T) {
	si := &iteration.StaticIterator{}
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		si.AddKVAsString(fmt.Sprintf("key-%05d", i), fmt.Sprintf("val-%05d", i))
	}
	maxSize := 500
	me := newMaxSizeIterator(maxSize, si)
	for i := 0; i < numEntries; i++ {
		valid, err := me.IsValid()
		require.NoError(t, err)
		if !valid {
			break
		}
		entry := me.Current()
		require.Equal(t, fmt.Sprintf("key-%05d", i), string(entry.Key))
		require.Equal(t, fmt.Sprintf("val-%05d", i), string(entry.Value))
		err = me.Next()
		require.NoError(t, err)
	}
}

func TestMaxSizeIteratorDontSplitVersions(t *testing.T) {
	si := &iteration.StaticIterator{}
	numEntries := 1000
	numVersions := 10
	for i := 0; i < numEntries; i++ {
		for j := 0; j < numVersions; j++ {
			k := encoding.EncodeVersion([]byte(fmt.Sprintf("key-%05d", i)), uint64(j))
			si.AddKV(k, []byte(fmt.Sprintf("val-%05d", i)))
		}
	}
	maxSize := 2000

	me := newMaxSizeIterator(maxSize, si)

	for i := 0; i < numEntries; i++ {
		for j := 0; j < numVersions; j++ {
			valid, err := me.IsValid()
			require.NoError(t, err)
			if !valid {
				// We can't split in middle of version
				require.Equal(t, 0, j)
				me = newMaxSizeIterator(maxSize, si)
			}
			entry := me.Current()
			k := encoding.EncodeVersion([]byte(fmt.Sprintf("key-%05d", i)), uint64(j))
			require.Equal(t, k, entry.Key)
			require.Equal(t, fmt.Sprintf("val-%05d", i), string(entry.Value))
			err = me.Next()
			require.NoError(t, err)
		}
	}
}

func TestRemoveExpiredEntriesIteratorZeroRetention(t *testing.T) {
	si := &iteration.StaticIterator{}

	var prefixes []retention.PrefixRetention
	for i := 0; i < 10; i++ {
		prefix := fmt.Sprintf("prefix-%05d", i)
		prefixes = append(prefixes, retention.PrefixRetention{Prefix: []byte(prefix)})
		for j := 0; j < 100; j++ {
			si.AddKVAsString(fmt.Sprintf("%s-key-%05d", prefix, j), fmt.Sprintf("%s-val-%05d", prefix, j))
		}
	}

	var prefixesToKeep = []retention.PrefixRetention{prefixes[0], prefixes[2], prefixes[3], prefixes[4], prefixes[6], prefixes[8], prefixes[9]}
	var prefixesToRemove = []retention.PrefixRetention{prefixes[1], prefixes[5], prefixes[7]}

	creationTime := uint64(time.Now().UTC().UnixMilli())
	now := creationTime
	rdpi := NewRemoveExpiredEntriesIterator(si, prefixesToRemove, creationTime, now)

	for _, prefix := range prefixesToKeep {
		for i := 0; i < 100; i++ {
			valid, err := rdpi.IsValid()
			require.NoError(t, err)
			require.True(t, valid)
			entry := rdpi.Current()
			require.Equal(t, fmt.Sprintf("%s-key-%05d", prefix.Prefix, i), string(entry.Key))
			require.Equal(t, fmt.Sprintf("%s-val-%05d", prefix.Prefix, i), string(entry.Value))
			err = rdpi.Next()
			require.NoError(t, err)
		}
	}
	valid, err := rdpi.IsValid()
	require.NoError(t, err)
	require.False(t, valid)
}

func TestRemoveExpiredEntriesIteratorNonZeroRetention(t *testing.T) {
	si := &iteration.StaticIterator{}

	var prefixesShortExpiration []retention.PrefixRetention
	var prefixesLongExpiration []retention.PrefixRetention
	for i := 0; i < 10; i++ {
		prefix := fmt.Sprintf("prefix-%05d", i)
		if i < 5 {
			prefixesShortExpiration = append(prefixesShortExpiration,
				retention.PrefixRetention{Prefix: []byte(prefix), Retention: 1})
		} else {
			prefixesLongExpiration = append(prefixesLongExpiration,
				retention.PrefixRetention{Prefix: []byte(prefix), Retention: 1000000})
		}
		for j := 0; j < 100; j++ {
			si.AddKVAsString(fmt.Sprintf("%s-key-%05d", prefix, j), fmt.Sprintf("%s-val-%05d", prefix, j))
		}
	}

	allPrefixes := append(prefixesShortExpiration, prefixesLongExpiration...)

	creationTime := uint64(time.Now().UTC().UnixMilli())
	now := creationTime + 1

	rdpi := NewRemoveExpiredEntriesIterator(si, allPrefixes, creationTime, now)

	for _, prefix := range prefixesLongExpiration {
		for i := 0; i < 100; i++ {
			valid, err := rdpi.IsValid()
			require.NoError(t, err)
			require.True(t, valid)
			entry := rdpi.Current()
			require.Equal(t, fmt.Sprintf("%s-key-%05d", prefix.Prefix, i), string(entry.Key))
			require.Equal(t, fmt.Sprintf("%s-val-%05d", prefix.Prefix, i), string(entry.Value))
			err = rdpi.Next()
			require.NoError(t, err)
		}
	}
	valid, err := rdpi.IsValid()
	require.NoError(t, err)
	require.False(t, valid)
}

func TestRemovedDeadVersionsIterator(t *testing.T) {
	si := &iteration.StaticIterator{}
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		val := []byte(fmt.Sprintf("val-%05d", i))
		key = encoding.EncodeVersion(key, uint64(i))
		si.AddKV(key, val)
	}
	deadRange1 := VersionRange{
		VersionStart: 333,
		VersionEnd:   555,
	}
	deadRange2 := VersionRange{
		VersionStart: 777,
		VersionEnd:   888,
	}
	me := NewRemoveDeadVersionsIterator(si, []VersionRange{deadRange1, deadRange2})
	i := 0
	for i < numEntries {
		valid, err := me.IsValid()
		require.NoError(t, err)
		if !valid {
			break
		}
		entry := me.Current()
		ver := math.MaxUint64 - binary.BigEndian.Uint64(entry.Key[len(entry.Key)-8:])
		require.Equal(t, uint64(i), ver)

		keyNoVer := entry.Key[:len(entry.Key)-8]
		expectedKey := []byte(fmt.Sprintf("key-%05d", i))

		require.Equal(t, expectedKey, keyNoVer)

		i++

		if i == 333 {
			i = 556
		} else if i == 777 {
			i = 889
		}

		err = me.Next()
		require.NoError(t, err)
	}
}

func TestSerializeDeserializeCompactionJob(t *testing.T) {
	job1 := CompactionJob{
		id:        uuid.New().String(),
		levelFrom: 3,
		tables: [][]tableToCompact{
			{
				{level: 2, table: &TableEntry{
					SSTableID:   []byte("sst1"),
					RangeStart:  []byte("key0"),
					RangeEnd:    []byte("key9"),
					DeleteRatio: 0.43,
				}, expiredPrefixes: []retention.PrefixRetention{{
					Prefix:    []byte("prefix1"),
					Retention: 1234,
				}, {
					Prefix:    []byte("prefix2"),
					Retention: 5678,
				}},
					deadVersionRanges: []VersionRange{
						{
							VersionStart: 1234,
							VersionEnd:   4321,
						},
						{
							VersionStart: 3456,
							VersionEnd:   4567,
						},
					},
				},
				{level: 2, table: &TableEntry{
					SSTableID:   []byte("sst2"),
					RangeStart:  []byte("key9"),
					RangeEnd:    []byte("key10"),
					DeleteRatio: 0.12,
				}},
			},
			{
				{level: 3, table: &TableEntry{
					SSTableID:   []byte("sst3"),
					RangeStart:  []byte("key3"),
					RangeEnd:    []byte("key12"),
					DeleteRatio: 0.32,
				}},
				{level: 3, table: &TableEntry{
					SSTableID:   []byte("sst4"),
					RangeStart:  []byte("key20"),
					RangeEnd:    []byte("key30"),
					DeleteRatio: 0.14,
				}},
			},
		},
		isMove:             true,
		preserveTombstones: true,
		scheduleTime:       123456,
	}

	buff := job1.Serialize(nil)
	var job2 CompactionJob
	job2.Deserialize(buff, 0)
	require.Equal(t, job1, job2)

	job1.isMove = false
	buff = job1.Serialize(nil)
	var job3 CompactionJob
	job3.Deserialize(buff, 0)
	require.Equal(t, job1, job3)
}

func TestSerializeDeserializeCompactionResult(t *testing.T) {
	res := CompactionResult{
		id: uuid.New().String(),
		newTables: []TableEntry{
			{
				SSTableID:   []byte("sst1"),
				RangeStart:  []byte("key0"),
				RangeEnd:    []byte("key9"),
				DeleteRatio: 0.11,
			},
			{
				SSTableID:   []byte("sst2"),
				RangeStart:  []byte("key10"),
				RangeEnd:    []byte("key19"),
				DeleteRatio: 0.21,
			},
		},
	}
	var buff []byte
	buff = res.Serialize(buff)

	var res2 CompactionResult
	res2.Deserialize(buff, 0)

	require.Equal(t, res, res2)
}

func TestCompactionTriggers(t *testing.T) {
	l0Trigger := 8
	l1Trigger := 10
	levelMult := 10
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		cfg.L0CompactionTrigger = &l0Trigger
		cfg.L1CompactionTrigger = &l1Trigger
		cfg.LevelMultiplier = &levelMult
	})
	defer tearDown(t)

	require.Equal(t, l0Trigger, lm.levelMaxTablesTrigger(0))
	require.Equal(t, l1Trigger, lm.levelMaxTablesTrigger(1))
	expected := l1Trigger
	for i := 2; i < 10; i++ {
		expected *= levelMult
		require.Equal(t, expected, lm.levelMaxTablesTrigger(i))
	}
}

func checkKVsInRange(t *testing.T, valPrefix string, sst *sst.SSTable, keyStart int, keyEnd int) {
	var r []int
	for i := keyStart; i < keyEnd; i++ {
		r = append(r, i, i)
	}
	checkKVs(t, sst, valPrefix, r...)
}

func checkKVs(t *testing.T, sst *sst.SSTable, valPrefix string, keys ...int) {
	iter, err := sst.NewIterator(nil, nil)
	require.NoError(t, err)
	for i := 0; i < len(keys); i += 2 {
		k := keys[i]
		v := keys[i+1]
		valid, err := iter.IsValid()
		require.NoError(t, err)
		require.True(t, valid)
		curr := iter.Current()
		require.Equal(t, fmt.Sprintf("key%05d", k), string(curr.Key[:len(curr.Key)-8]))
		if v == -1 {
			// tombstone
			require.Nil(t, curr.Value)
		} else {
			require.Equal(t, fmt.Sprintf("%s%05d", valPrefix, v), string(curr.Value))
		}
		err = iter.Next()
		require.NoError(t, err)
	}
}

func newSSTableBuilder() *ssTableBuilder {
	return &ssTableBuilder{
		si: &iteration.StaticIterator{},
	}
}

type ssTableBuilder struct {
	si *iteration.StaticIterator
}

func (tb *ssTableBuilder) addEntry(key string, val string) {
	kb := encoding.EncodeVersion([]byte(key), 0)
	tb.si.AddKV(kb, []byte(val))
}

func (tb *ssTableBuilder) addEntryWithVersion(key string, val string, version uint64) {
	kb := encoding.EncodeVersion([]byte(key), version)
	tb.si.AddKV(kb, []byte(val))
}

func (tb *ssTableBuilder) addTombstone(key string) {
	kb := encoding.EncodeVersion([]byte(key), 0)
	tb.si.AddKV(kb, nil)
}

func (tb *ssTableBuilder) addTombstoneWithVersion(key string, version uint64) {
	kb := encoding.EncodeVersion([]byte(key), version)
	tb.si.AddKV(kb, nil)
}

func (tb *ssTableBuilder) build() (*sst.SSTable, error) {
	ssTable, _, _, _, _, err := sst.BuildSSTable(common.DataFormatV1, 0, 0, tb.si)
	if err != nil {
		return nil, err
	}
	return ssTable, nil
}

func createTableEntry(tableName string, rangeStart int, rangeEnd int) TableEntry {
	return createTableEntryWithDeleteRatio(tableName, rangeStart, rangeEnd, 0)
}

func createTableEntryWithDeleteRatio(tableName string, rangeStart int, rangeEnd int, deleteRatio float64) TableEntry {
	return TableEntry{
		SSTableID:   []byte(tableName),
		RangeStart:  encoding.EncodeVersion([]byte(fmt.Sprintf("key%05d", rangeStart)), 0),
		RangeEnd:    encoding.EncodeVersion([]byte(fmt.Sprintf("key%05d", rangeEnd)), 0),
		DeleteRatio: deleteRatio,
	}
}

func trimVersion(key []byte) []byte {
	return key[:len(key)-8]
}

func TestRemoveDeadVersionsL0(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l0CompactionTrigger := 10
		cfg.L0CompactionTrigger = &l0CompactionTrigger
	})
	defer tearDown(t)

	// this won't match
	regEntry1 := RegistrationEntry{
		Level:      0,
		TableID:    []byte("sst1"),
		MinVersion: 11,
		MaxVersion: 24,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	// this will match
	regEntry2 := RegistrationEntry{
		Level:      0,
		TableID:    []byte("sst2"),
		MinVersion: 17,
		MaxVersion: 25,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	regBatch := RegistrationBatch{
		Registrations: []RegistrationEntry{regEntry1, regEntry2},
	}
	err := lm.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	rng := VersionRange{
		VersionStart: 25,
		VersionEnd:   33,
	}
	err = lm.RegisterDeadVersionRange(rng, "test_cluster", 123, false, 0)
	require.NoError(t, err)

	stats := lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)

	// For L0 we compact the entire level so even non-matching ones should be there

	job, err := getJob(lm)
	require.NoError(t, err)
	require.False(t, job.isMove)

	require.Equal(t, "sst1", string(job.tables[0][0].table.SSTableID))
	require.Equal(t, []VersionRange{rng}, job.tables[0][0].deadVersionRanges)

	require.Equal(t, "sst2", string(job.tables[1][0].table.SSTableID))
	require.Equal(t, []VersionRange{rng}, job.tables[1][0].deadVersionRanges)
}

func TestRemoveDeadVersionsL0NoRemoveNoMatch(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l0CompactionTrigger := 10
		cfg.L0CompactionTrigger = &l0CompactionTrigger
	})
	defer tearDown(t)

	// this won't match
	regEntry1 := RegistrationEntry{
		Level:      0,
		TableID:    []byte("sst1"),
		MinVersion: 11,
		MaxVersion: 24,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	// this won't match
	regEntry2 := RegistrationEntry{
		Level:      0,
		TableID:    []byte("sst2"),
		MinVersion: 34,
		MaxVersion: 57,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	regBatch := RegistrationBatch{
		Registrations: []RegistrationEntry{regEntry1, regEntry2},
	}
	err := lm.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	rng := VersionRange{
		VersionStart: 25,
		VersionEnd:   33,
	}
	err = lm.RegisterDeadVersionRange(rng, "test_cluster", 123, false, 0)
	require.NoError(t, err)

	stats := lm.GetCompactionStats()
	require.Equal(t, 0, stats.QueuedJobs)
}

func TestRemoveDeadVersionsL1(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 10
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)

	// this won't match
	regEntry1 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst1"),
		MinVersion: 11,
		MaxVersion: 24,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	// this will match
	regEntry2 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst2"),
		MinVersion: 17,
		MaxVersion: 25,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	// this will match
	regEntry3 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst3"),
		MinVersion: 22,
		MaxVersion: 39,
		KeyStart:   encoding.EncodeVersion([]byte("key-0004"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0007"), 0),
	}
	// this will match
	regEntry4 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst4"),
		MinVersion: 33,
		MaxVersion: 38,
		KeyStart:   encoding.EncodeVersion([]byte("key-0008"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0010"), 0),
	}
	// this won't match
	regEntry5 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst5"),
		MinVersion: 34,
		MaxVersion: 53,
		KeyStart:   encoding.EncodeVersion([]byte("key-0008"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0010"), 0),
	}
	// this won't match
	regEntry6 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst6"),
		MinVersion: 42,
		MaxVersion: 65,
		KeyStart:   encoding.EncodeVersion([]byte("key-0008"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0010"), 0),
	}
	regBatch := RegistrationBatch{
		Registrations: []RegistrationEntry{regEntry1, regEntry2, regEntry3, regEntry4, regEntry5, regEntry6},
	}
	err := lm.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	rng := VersionRange{
		VersionStart: 25,
		VersionEnd:   33,
	}
	err = lm.RegisterDeadVersionRange(rng, "test_cluster", 123, false, 0)
	require.NoError(t, err)

	stats := lm.GetCompactionStats()
	require.Equal(t, 3, stats.QueuedJobs)

	job1, err := getJob(lm)
	require.NoError(t, err)

	require.Equal(t, "sst2", string(job1.tables[0][0].table.SSTableID))
	require.Equal(t, []VersionRange{rng}, job1.tables[0][0].deadVersionRanges)
	require.False(t, job1.isMove)

	job2, err := getJob(lm)
	require.NoError(t, err)

	require.Equal(t, "sst3", string(job2.tables[0][0].table.SSTableID))
	require.Equal(t, []VersionRange{rng}, job2.tables[0][0].deadVersionRanges)
	require.False(t, job2.isMove)

	job3, err := getJob(lm)
	require.NoError(t, err)

	require.Equal(t, "sst4", string(job3.tables[0][0].table.SSTableID))
	require.Equal(t, []VersionRange{rng}, job3.tables[0][0].deadVersionRanges)
	require.False(t, job3.isMove)
}

func TestRemoveDeadVersionsL1NoRemoveNoMatch(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 10
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)

	// this won't match
	regEntry1 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst1"),
		MinVersion: 11,
		MaxVersion: 24,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	// this won't match
	regEntry5 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst5"),
		MinVersion: 34,
		MaxVersion: 53,
		KeyStart:   encoding.EncodeVersion([]byte("key-0008"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0010"), 0),
	}
	// this won't match
	regEntry6 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst6"),
		MinVersion: 42,
		MaxVersion: 65,
		KeyStart:   encoding.EncodeVersion([]byte("key-0008"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0010"), 0),
	}
	regBatch := RegistrationBatch{
		Registrations: []RegistrationEntry{regEntry1, regEntry5, regEntry6},
	}
	err := lm.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	rng := VersionRange{
		VersionStart: 25,
		VersionEnd:   33,
	}
	err = lm.RegisterDeadVersionRange(rng, "test_cluster", 123, false, 0)
	require.NoError(t, err)

	stats := lm.GetCompactionStats()
	require.Equal(t, 0, stats.QueuedJobs)
}

func TestCompleteDeadVersionRemovalAllScheduledNoneWaiting(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 10
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)

	regEntry1 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst1"),
		MinVersion: 17,
		MaxVersion: 25,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	regBatch := RegistrationBatch{
		Registrations: []RegistrationEntry{regEntry1},
	}
	err := lm.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	rng := VersionRange{
		VersionStart: 25,
		VersionEnd:   33,
	}
	err = lm.RegisterDeadVersionRange(rng, "test_cluster", 123, false, 0)
	require.NoError(t, err)

	require.Equal(t, rng, lm.getDeadVersions()[0])

	stats := lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)

	job, err := getJob(lm)
	require.NoError(t, err)

	require.Equal(t, "sst1", string(job.tables[0][0].table.SSTableID))
	require.Equal(t, []VersionRange{rng}, job.tables[0][0].deadVersionRanges)
	require.False(t, job.isMove)

	sendCompactionComplete(t, lm, job, []TableEntry{})

	// Make sure deleted version is removed
	require.Equal(t, 0, len(lm.getDeadVersions()))
}

func TestCompleteDeadVersionRemovalAllScheduledOneWaiting(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 10
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)

	regEntry1 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst1"),
		MinVersion: 17,
		MaxVersion: 25,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	regEntry2 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst2"),
		MinVersion: 37,
		MaxVersion: 53,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	regBatch := RegistrationBatch{
		Registrations: []RegistrationEntry{regEntry1, regEntry2},
	}
	err := lm.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	rng1 := VersionRange{
		VersionStart: 25,
		VersionEnd:   33,
	}
	rng2 := VersionRange{
		VersionStart: 45,
		VersionEnd:   53,
	}
	err = lm.RegisterDeadVersionRange(rng1, "test_cluster", 123, false, 0)
	require.NoError(t, err)

	require.Equal(t, rng1, lm.getDeadVersions()[0])

	stats := lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)

	// Now register another range - this shouldn't create a new job yet, as we only process one dead version range at
	// a time
	err = lm.RegisterDeadVersionRange(rng2, "test_cluster", 123, false, 0)
	require.NoError(t, err)

	require.Equal(t, rng2, lm.getDeadVersions()[1])

	stats = lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)

	job, err := getJob(lm)
	require.NoError(t, err)

	stats = lm.GetCompactionStats()
	require.Equal(t, 0, stats.QueuedJobs)

	require.Equal(t, "sst1", string(job.tables[0][0].table.SSTableID))
	require.Equal(t, []VersionRange{rng1}, job.tables[0][0].deadVersionRanges)
	require.False(t, job.isMove)

	sendCompactionComplete(t, lm, job, []TableEntry{})

	// Make sure deleted version is removed
	require.Equal(t, 1, len(lm.getDeadVersions()))
	require.Equal(t, rng2, lm.getDeadVersions()[0])

	// Now, a new job should have been created
	stats = lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)

	job, err = getJob(lm)
	require.NoError(t, err)

	require.Equal(t, "sst2", string(job.tables[0][0].table.SSTableID))
	require.Equal(t, []VersionRange{rng2}, job.tables[0][0].deadVersionRanges)
	require.False(t, job.isMove)

	sendCompactionComplete(t, lm, job, []TableEntry{})

	// Make sure deleted version is removed
	require.Equal(t, 0, len(lm.getDeadVersions()))
}

func TestCompleteDeadVersionRemovalNotAllScheduledOneWaiting(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 2
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)

	regEntry1 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst1"),
		MinVersion: 17,
		MaxVersion: 25,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	regEntry2 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst2"),
		MinVersion: 19,
		MaxVersion: 53,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	regBatch := RegistrationBatch{
		Registrations: []RegistrationEntry{regEntry1, regEntry2},
	}
	err := lm.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)
	rng1 := VersionRange{
		VersionStart: 25,
		VersionEnd:   33,
	}
	rng2 := VersionRange{
		VersionStart: 45,
		VersionEnd:   53,
	}

	// lock one of the tables - this simulates another compaction job in progress which has it locked
	lm.lockTable("sst1")

	err = lm.RegisterDeadVersionRange(rng1, "test_cluster", 123, false, 0)
	require.NoError(t, err)

	require.Equal(t, rng1, lm.getDeadVersions()[0])

	stats := lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)

	// Now register another range - this shouldn't create a new job yet, as we only process one dead version range at
	// a time
	err = lm.RegisterDeadVersionRange(rng2, "test_cluster", 123, false, 1)
	require.NoError(t, err)

	require.Equal(t, rng2, lm.getDeadVersions()[1])

	stats = lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)

	job, err := getJob(lm)
	require.NoError(t, err)

	stats = lm.GetCompactionStats()
	require.Equal(t, 0, stats.QueuedJobs)

	// Only sst2 is in job even though sst1 and sst2 match - as sst1 is locked
	require.Equal(t, "sst2", string(job.tables[0][0].table.SSTableID))
	require.Equal(t, []VersionRange{rng1}, job.tables[0][0].deadVersionRanges)
	require.False(t, job.isMove)

	// Now unlock the table
	lm.unlockTable("sst1")

	sendCompactionComplete(t, lm, job, []TableEntry{})

	// dead version won't be deleted as didn't manage to lock all matching tables in first job
	require.Equal(t, 2, len(lm.getDeadVersions()))
	require.Equal(t, []VersionRange{rng1, rng2}, lm.getDeadVersions())

	// Now, a new job should have been created
	stats = lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)

	job, err = getJob(lm)
	require.NoError(t, err)

	// should contain sst1

	require.Equal(t, "sst1", string(job.tables[0][0].table.SSTableID))
	require.Equal(t, []VersionRange{rng1}, job.tables[0][0].deadVersionRanges)
	require.False(t, job.isMove)

	sendCompactionComplete(t, lm, job, []TableEntry{{
		SSTableID:  []byte("sst3"),
		RangeStart: encoding.EncodeVersion([]byte("key-0000"), 0),
		RangeEnd:   encoding.EncodeVersion([]byte("key-0003"), 0),
		MinVersion: 40,
		MaxVersion: 60,
	}})

	require.Equal(t, 1, len(lm.getDeadVersions()))
	require.Equal(t, rng2, lm.getDeadVersions()[0])
}

func TestCompleteDeadVersionMultipleMatchingLevels(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l0CompactionTrigger := 10
		l1CompactionTrigger := 10
		levelMultiplier := 10
		cfg.L0CompactionTrigger = &l0CompactionTrigger
		cfg.L1CompactionTrigger = &l1CompactionTrigger
		cfg.LevelMultiplier = &levelMultiplier
	})
	defer tearDown(t)

	regEntry1 := RegistrationEntry{
		Level:      0,
		TableID:    []byte("sst1"),
		MinVersion: 17,
		MaxVersion: 25,
		KeyStart:   encoding.EncodeVersion([]byte("key-0000"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0003"), 0),
	}
	regBatch := RegistrationBatch{
		Registrations: []RegistrationEntry{regEntry1},
	}
	err := lm.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)

	regEntry2 := RegistrationEntry{
		Level:      1,
		TableID:    []byte("sst2"),
		MinVersion: 17,
		MaxVersion: 25,
		KeyStart:   encoding.EncodeVersion([]byte("key-0004"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0008"), 0),
	}
	regBatch = RegistrationBatch{
		Registrations: []RegistrationEntry{regEntry2},
	}
	err = lm.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)

	regEntry3 := RegistrationEntry{
		Level:      2,
		TableID:    []byte("sst3"),
		MinVersion: 17,
		MaxVersion: 25,
		KeyStart:   encoding.EncodeVersion([]byte("key-0009"), 0),
		KeyEnd:     encoding.EncodeVersion([]byte("key-0011"), 0),
	}
	regBatch = RegistrationBatch{
		Registrations: []RegistrationEntry{regEntry3},
	}
	err = lm.ApplyChangesNoCheck(regBatch)
	require.NoError(t, err)

	rng1 := VersionRange{
		VersionStart: 25,
		VersionEnd:   33,
	}

	err = lm.RegisterDeadVersionRange(rng1, "test_cluster", 123, false, 0)
	require.NoError(t, err)

	stats := lm.GetCompactionStats()
	require.Equal(t, 3, stats.QueuedJobs)

	job, err := getJob(lm)
	require.NoError(t, err)

	require.Equal(t, "sst1", string(job.tables[0][0].table.SSTableID))
	require.Equal(t, []VersionRange{rng1}, job.tables[0][0].deadVersionRanges)
	require.False(t, job.isMove)

	job, err = getJob(lm)
	require.NoError(t, err)

	require.Equal(t, "sst2", string(job.tables[0][0].table.SSTableID))
	require.Equal(t, []VersionRange{rng1}, job.tables[0][0].deadVersionRanges)
	require.False(t, job.isMove)

	job, err = getJob(lm)
	require.NoError(t, err)

	require.Equal(t, "sst3", string(job.tables[0][0].table.SSTableID))
	require.Equal(t, []VersionRange{rng1}, job.tables[0][0].deadVersionRanges)
	require.False(t, job.isMove)
}

func TestNoPreserveTombstonesWhenTableCompactedToLastLevel(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 1
		cfg.L1CompactionTrigger = &l1CompactionTrigger
	})
	defer tearDown(t)

	sst1 := createTableEntryWithDeleteRatio("sst1", 0, 9, 0.5)
	sst2 := createTableEntryWithDeleteRatio("sst2", 10, 19, 0.5)

	populateLevel(t, lm, 1, sst1, sst2)

	err := lm.MaybeScheduleCompaction()
	require.NoError(t, err)

	job, err := getJob(lm)
	require.NoError(t, err)

	require.Equal(t, 1, job.levelFrom)
	require.Equal(t, false, job.preserveTombstones)
}

func TestClosePollersForConnectionID(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 1
		compactionJobTimeout := time.Hour
		cfg.L1CompactionTrigger = &l1CompactionTrigger
		cfg.CompactionJobTimeout = &compactionJobTimeout
	})
	defer tearDown(t)

	sst1 := createTableEntryWithDeleteRatio("sst1", 0, 9, 0.5)
	sst2 := createTableEntryWithDeleteRatio("sst2", 10, 19, 0.5)

	populateLevel(t, lm, 1, sst1, sst2)

	err := lm.MaybeScheduleCompaction()
	require.NoError(t, err)

	ch := make(chan pollResult, 1)
	connectionID := 100
	lm.pollForJob(connectionID, func(job *CompactionJob, err error) {
		ch <- pollResult{job, err}
	})

	res := <-ch
	require.NoError(t, res.err)
	require.NotNil(t, res.job)

	stats := lm.GetCompactionStats()
	require.Equal(t, 1, stats.InProgressJobs)
	require.Equal(t, 0, stats.QueuedJobs)

	lm.connectionClosed(connectionID)

	// job should be cancelled and returned to queue
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		stats := lm.GetCompactionStats()
		return stats.InProgressJobs == 0 && stats.QueuedJobs == 1, nil
	}, 5*time.Second, 1*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestJobCreatedWithLastFlushedVersion(t *testing.T) {
	lm, tearDown := setupLevelManagerWithConfigSetter(t, true, func(cfg *conf.Config) {
		l1CompactionTrigger := 1
		compactionJobTimeout := time.Hour
		cfg.L1CompactionTrigger = &l1CompactionTrigger
		cfg.CompactionJobTimeout = &compactionJobTimeout
	})
	defer tearDown(t)

	sst1 := createTableEntryWithDeleteRatio("sst1", 0, 9, 0.5)
	sst2 := createTableEntryWithDeleteRatio("sst2", 10, 19, 0.5)
	populateLevel(t, lm, 1, sst1, sst2)

	err := lm.MaybeScheduleCompaction()
	require.NoError(t, err)

	stats := lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)
	job, err := getJob(lm)
	require.NoError(t, err)
	require.Equal(t, int64(-1), job.lastFlushedVersion)

	err = lm.StoreLastFlushedVersion(100, false, 0)
	require.NoError(t, err)

	sst3 := createTableEntryWithDeleteRatio("sst3", 10, 19, 0.5)
	populateLevel(t, lm, 1, sst3)
	err = lm.MaybeScheduleCompaction()
	require.NoError(t, err)

	stats = lm.GetCompactionStats()
	require.Equal(t, 1, stats.QueuedJobs)
	job, err = getJob(lm)
	require.NoError(t, err)
	require.Equal(t, int64(100), job.lastFlushedVersion)
}
