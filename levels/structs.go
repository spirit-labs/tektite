//lint:file-ignore U1000 Ignore all unused code
package levels

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/sst"
)

type QueryTableInfo struct {
	ID           sst.SSTableID
	DeadVersions []VersionRange
}

type NonOverlappingTables []QueryTableInfo

type OverlappingTables []NonOverlappingTables

func (ot OverlappingTables) Serialize(bytes []byte) []byte {
	bytes = encoding.AppendUint32ToBufferLE(bytes, uint32(len(ot)))
	for _, tableInfos := range ot {
		bytes = encoding.AppendUint32ToBufferLE(bytes, uint32(len(tableInfos)))
		for _, tableInfo := range tableInfos {
			bytes = encoding.AppendUint32ToBufferLE(bytes, uint32(len(tableInfo.ID)))
			bytes = append(bytes, tableInfo.ID...)
			bytes = encoding.AppendUint32ToBufferLE(bytes, uint32(len(tableInfo.DeadVersions)))
			for _, dv := range tableInfo.DeadVersions {
				bytes = dv.Serialize(bytes)
			}
		}
	}
	return bytes
}

func DeserializeOverlappingTables(bytes []byte, offset int) OverlappingTables {
	nn, offset := encoding.ReadUint32FromBufferLE(bytes, offset)
	otids := make([]NonOverlappingTables, nn)
	for i := 0; i < int(nn); i++ {
		var no uint32
		no, offset = encoding.ReadUint32FromBufferLE(bytes, offset)
		tableInfos := make([]QueryTableInfo, no)
		otids[i] = tableInfos
		for j := 0; j < int(no); j++ {
			var l uint32
			l, offset = encoding.ReadUint32FromBufferLE(bytes, offset)
			tabID := bytes[offset : offset+int(l)]
			offset += int(l)
			tableInfos[j].ID = tabID
			l, offset = encoding.ReadUint32FromBufferLE(bytes, offset)
			deadVersions := make([]VersionRange, l)
			for i := 0; i < int(l); i++ {
				offset = deadVersions[i].Deserialize(bytes, offset)
			}
			tableInfos[j].DeadVersions = deadVersions
		}
	}
	return otids
}

type RegistrationEntry struct {
	Level            int
	TableID          sst.SSTableID
	MinVersion       uint64
	MaxVersion       uint64
	KeyStart, KeyEnd []byte
	DeleteRatio      float64
	AddedTime        uint64
	NumEntries       uint64
	TableSize        uint64
	NumPrefixDeletes uint32
}

func (re *RegistrationEntry) serialize(buff []byte) []byte {
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(re.Level))
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(re.TableID)))
	buff = append(buff, re.TableID...)
	buff = encoding.AppendUint64ToBufferLE(buff, re.MinVersion)
	buff = encoding.AppendUint64ToBufferLE(buff, re.MaxVersion)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(re.KeyStart)))
	buff = append(buff, re.KeyStart...)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(re.KeyEnd)))
	buff = append(buff, re.KeyEnd...)
	buff = encoding.AppendFloat64ToBufferLE(buff, re.DeleteRatio)
	buff = encoding.AppendUint64ToBufferLE(buff, re.AddedTime)
	buff = encoding.AppendUint64ToBufferLE(buff, re.NumEntries)
	buff = encoding.AppendUint64ToBufferLE(buff, re.TableSize)
	buff = encoding.AppendUint32ToBufferLE(buff, re.NumPrefixDeletes)
	return buff
}

func (re *RegistrationEntry) deserialize(buff []byte, offset int) int {
	var lev uint32
	lev, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	re.Level = int(lev)
	var l uint32
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	re.TableID = buff[offset : offset+int(l)]
	offset += int(l)
	re.MinVersion, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	re.MaxVersion, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	re.KeyStart = buff[offset : offset+int(l)]
	offset += int(l)
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	re.KeyEnd = buff[offset : offset+int(l)]
	offset += int(l)
	re.DeleteRatio, offset = encoding.ReadFloat64FromBufferLE(buff, offset)
	re.AddedTime, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	re.NumEntries, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	re.TableSize, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	re.NumPrefixDeletes, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	return offset
}

type RegistrationBatch struct {
	ClusterName     string
	ClusterVersion  int
	Compaction      bool
	JobID           string
	ProcessorID     int
	Registrations   []RegistrationEntry
	DeRegistrations []RegistrationEntry
}

func (rb *RegistrationBatch) Serialize(buff []byte) []byte {
	buff = encoding.AppendStringToBufferLE(buff, rb.ClusterName)
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(rb.ClusterVersion))
	buff = encoding.AppendBoolToBuffer(buff, rb.Compaction)
	buff = encoding.AppendStringToBufferLE(buff, rb.JobID)
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(rb.ProcessorID))
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(rb.Registrations)))
	for _, reg := range rb.Registrations {
		buff = reg.serialize(buff)
	}
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(rb.DeRegistrations)))
	for _, dereg := range rb.DeRegistrations {
		buff = dereg.serialize(buff)
	}
	return buff
}

func (rb *RegistrationBatch) Deserialize(buff []byte, offset int) int {
	var s string
	s, offset = encoding.ReadStringFromBufferLE(buff, offset)
	rb.ClusterName = s
	var ver uint64
	ver, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	rb.ClusterVersion = int(ver)
	rb.Compaction, offset = encoding.ReadBoolFromBuffer(buff, offset)
	rb.JobID, offset = encoding.ReadStringFromBufferLE(buff, offset)
	var pid uint64
	pid, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	rb.ProcessorID = int(pid)
	var l uint32
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	rb.Registrations = make([]RegistrationEntry, l)
	for i := 0; i < int(l); i++ {
		offset = rb.Registrations[i].deserialize(buff, offset)
	}
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	rb.DeRegistrations = make([]RegistrationEntry, l)
	for i := 0; i < int(l); i++ {
		offset = rb.DeRegistrations[i].deserialize(buff, offset)
	}
	return offset
}

type segmentID []byte

type segment struct {
	format       byte // Placeholder to allow us to change format later on
	tableEntries []*TableEntry
}

func (s *segment) serialize(buff []byte) []byte {
	buff = append(buff, s.format)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(s.tableEntries)))
	for _, te := range s.tableEntries {
		buff = te.serialize(buff)
	}
	return buff
}

func (s *segment) deserialize(buff []byte) {
	s.format = buff[0]
	offset := 1
	var l uint32
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	s.tableEntries = make([]*TableEntry, int(l))
	for i := 0; i < int(l); i++ {
		te := &TableEntry{}
		offset = te.deserialize(buff, offset)
		s.tableEntries[i] = te
	}
}

type TableEntry struct {
	SSTableID         sst.SSTableID
	RangeStart        []byte
	RangeEnd          []byte
	MinVersion        uint64
	MaxVersion        uint64
	DeleteRatio       float64
	AddedTime         uint64 // The time the table was first added to the database - used to calculate retention
	NumEntries        uint64
	Size              uint64
	NumPrefixDeletes  uint32
	DeadVersionRanges []VersionRange
}

func (te *TableEntry) copy() *TableEntry {
	cp := *te
	cp.DeadVersionRanges = make([]VersionRange, len(te.DeadVersionRanges))
	copy(cp.DeadVersionRanges, te.DeadVersionRanges)
	return &cp
}

func (te *TableEntry) serialize(buff []byte) []byte {
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(te.SSTableID)))
	buff = append(buff, te.SSTableID...)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(te.RangeStart)))
	buff = append(buff, te.RangeStart...)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(te.RangeEnd)))
	buff = append(buff, te.RangeEnd...)
	buff = encoding.AppendUint64ToBufferLE(buff, te.MinVersion)
	buff = encoding.AppendUint64ToBufferLE(buff, te.MaxVersion)
	buff = encoding.AppendFloat64ToBufferLE(buff, te.DeleteRatio)
	buff = encoding.AppendUint64ToBufferLE(buff, te.NumEntries)
	buff = encoding.AppendUint64ToBufferLE(buff, te.Size)
	buff = encoding.AppendUint32ToBufferLE(buff, te.NumPrefixDeletes)
	// We encode DeadVersionRanges as varint to save space as most TableEntry instances won't have any DeadVersionRanges
	ldvps := len(te.DeadVersionRanges)
	buff = binary.AppendUvarint(buff, uint64(ldvps))
	for i := 0; i < ldvps; i++ {
		buff = te.DeadVersionRanges[i].Serialize(buff)
	}
	return buff
}

func (te *TableEntry) deserialize(buff []byte, offset int) int {
	var l uint32
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	te.SSTableID = buff[offset : offset+int(l)]
	offset += int(l)
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	te.RangeStart = buff[offset : offset+int(l)]
	offset += int(l)
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	te.RangeEnd = buff[offset : offset+int(l)]
	offset += int(l)
	te.MinVersion, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	te.MaxVersion, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	te.DeleteRatio, offset = encoding.ReadFloat64FromBufferLE(buff, offset)
	te.NumEntries, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	te.Size, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	te.NumPrefixDeletes, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	ldvps, bytesRead := binary.Uvarint(buff[offset:])
	offset += bytesRead
	if ldvps > 0 {
		te.DeadVersionRanges = make([]VersionRange, ldvps)
		for i := 0; i < int(ldvps); i++ {
			offset = te.DeadVersionRanges[i].Deserialize(buff, offset)
		}
	}
	return offset
}

type segmentEntry struct {
	format     byte // Placeholder to allow us to change format later on, e.g. add bloom filter in the future
	segmentID  segmentID
	rangeStart []byte
	rangeEnd   []byte
}

func (se *segmentEntry) serialize(buff []byte) []byte {
	buff = append(buff, se.format)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(se.segmentID)))
	buff = append(buff, se.segmentID...)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(se.rangeStart)))
	buff = append(buff, se.rangeStart...)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(se.rangeEnd)))
	buff = append(buff, se.rangeEnd...)
	return buff
}

func (se *segmentEntry) deserialize(buff []byte, offset int) int {
	se.format = buff[offset]
	offset++
	var l uint32
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	se.segmentID = buff[offset : offset+int(l)]
	offset += int(l)
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	se.rangeStart = buff[offset : offset+int(l)]
	offset += int(l)
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	se.rangeEnd = buff[offset : offset+int(l)]
	offset += int(l)
	return offset
}

type Stats struct {
	TotBytes       int
	TotEntries     int
	TotTables      int
	BytesIn        int
	EntriesIn      int
	TablesIn       int
	TotCompactions int
	LevelStats     map[int]*LevelStats
}

func (l *Stats) Serialize(buff []byte) []byte {
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(l.TotBytes))
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(l.TotEntries))
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(l.TotTables))
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(l.BytesIn))
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(l.EntriesIn))
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(l.TablesIn))
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(l.TotCompactions))
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(l.LevelStats)))
	for level, levStats := range l.LevelStats {
		buff = encoding.AppendUint32ToBufferLE(buff, uint32(level))
		buff = levStats.serialize(buff)
	}
	return buff
}

func (l *Stats) Deserialize(buff []byte, offset int) int {
	var u uint64
	u, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	l.TotBytes = int(u)
	u, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	l.TotEntries = int(u)
	u, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	l.TotTables = int(u)
	u, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	l.BytesIn = int(u)
	u, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	l.EntriesIn = int(u)
	u, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	l.TablesIn = int(u)
	u, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	l.TotCompactions = int(u)
	var nl uint32
	nl, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	numLevels := int(nl)
	levStatsMap := make(map[int]*LevelStats, numLevels)
	for i := 0; i < numLevels; i++ {
		var l uint32
		l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
		levStats := &LevelStats{}
		offset = levStats.deserialize(buff, offset)
		levStatsMap[int(l)] = levStats
	}
	l.LevelStats = levStatsMap
	return offset
}

func (l *Stats) copy() *Stats {
	statsCopy := &Stats{}
	statsCopy.TotBytes = l.TotBytes
	statsCopy.TotEntries = l.TotEntries
	statsCopy.TotTables = l.TotTables
	statsCopy.BytesIn = l.BytesIn
	statsCopy.EntriesIn = l.EntriesIn
	statsCopy.TablesIn = l.TablesIn
	statsCopy.TotCompactions = l.TotCompactions
	statsCopy.LevelStats = make(map[int]*LevelStats, len(l.LevelStats))
	for level, levStats := range l.LevelStats {
		statsCopy.LevelStats[level] = &LevelStats{
			Bytes:   levStats.Bytes,
			Entries: levStats.Entries,
			Tables:  levStats.Tables,
		}
	}
	return statsCopy
}

type LevelStats struct {
	Bytes   int
	Entries int
	Tables  int
}

func (l *LevelStats) serialize(buff []byte) []byte {
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(l.Bytes))
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(l.Entries))
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(l.Tables))
	return buff
}

func (l *LevelStats) deserialize(buff []byte, offset int) int {
	var u uint64
	u, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	l.Bytes = int(u)
	u, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	l.Entries = int(u)
	u, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	l.Tables = int(u)
	return offset
}

type masterRecord struct {
	format               common.MetadataFormat
	version              uint64
	levelSegmentEntries  []levelEntries
	levelTableCounts     map[int]int
	slabRetentions       map[uint64]uint64 // this is a map as we can get duplicate registrations
	lastFlushedVersion   int64
	lastProcessedReplSeq int
	stats                *Stats
}

func copyLevelSegmentEntries(levEntries []levelEntries) []levelEntries {
	lseCopy := make([]levelEntries, len(levEntries))
	for i, entries := range levEntries {
		copiedEntries := make([]segmentEntry, len(entries.segmentEntries))
		copy(copiedEntries, entries.segmentEntries)
		lseCopy[i] = levelEntries{
			segmentEntries: copiedEntries,
			maxVersion:     entries.maxVersion,
		}
	}
	return lseCopy
}

func (mr *masterRecord) copy() *masterRecord {
	lseCopy := copyLevelSegmentEntries(mr.levelSegmentEntries)

	prefixRetentionsCopy := make(map[uint64]uint64, len(mr.slabRetentions))
	for slabID, retention := range mr.slabRetentions {
		prefixRetentionsCopy[slabID] = retention
	}

	tableCountCopy := make(map[int]int, len(mr.levelTableCounts))
	for level, cnt := range mr.levelTableCounts {
		tableCountCopy[level] = cnt
	}
	return &masterRecord{
		format:               mr.format,
		version:              mr.version,
		levelSegmentEntries:  lseCopy,
		levelTableCounts:     tableCountCopy,
		slabRetentions:       prefixRetentionsCopy,
		lastFlushedVersion:   mr.lastFlushedVersion,
		lastProcessedReplSeq: mr.lastProcessedReplSeq,
		stats:                mr.stats.copy(),
	}
}

func (mr *masterRecord) serialize(buff []byte) []byte {
	buff = append(buff, byte(mr.format))
	buff = encoding.AppendUint64ToBufferLE(buff, mr.version)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(mr.levelSegmentEntries)))
	for _, entries := range mr.levelSegmentEntries {
		buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(entries.segmentEntries)))
		for _, segEntry := range entries.segmentEntries {
			buff = segEntry.serialize(buff)
		}
		buff = encoding.AppendUint64ToBufferLE(buff, entries.maxVersion)
	}
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(mr.levelTableCounts)))
	for level, cnt := range mr.levelTableCounts {
		buff = encoding.AppendUint32ToBufferLE(buff, uint32(level))
		buff = encoding.AppendUint64ToBufferLE(buff, uint64(cnt))
	}
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(mr.slabRetentions)))
	for slabID, retention := range mr.slabRetentions {
		buff = encoding.AppendUint64ToBufferLE(buff, slabID)
		buff = encoding.AppendUint64ToBufferLE(buff, retention)
	}
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(mr.lastFlushedVersion))
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(mr.lastProcessedReplSeq))
	return mr.stats.Serialize(buff)
}

func (mr *masterRecord) deserialize(buff []byte, offset int) int {
	mr.format = common.MetadataFormat(buff[offset])
	offset++
	mr.version, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	var nl uint32
	nl, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	mr.levelSegmentEntries = make([]levelEntries, nl)
	for level := 0; level < int(nl); level++ {
		var numEntries uint32
		numEntries, offset = encoding.ReadUint32FromBufferLE(buff, offset)
		segEntries := make([]segmentEntry, numEntries)
		for j := 0; j < int(numEntries); j++ {
			segEntry := segmentEntry{}
			offset = segEntry.deserialize(buff, offset)
			segEntries[j] = segEntry
		}
		entries := levelEntries{
			segmentEntries: segEntries,
		}
		entries.maxVersion, offset = encoding.ReadUint64FromBufferLE(buff, offset)
		mr.levelSegmentEntries[level] = entries
	}
	var ncnts uint32
	ncnts, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	mr.levelTableCounts = make(map[int]int, ncnts)
	for i := 0; i < int(ncnts); i++ {
		var lev uint32
		lev, offset = encoding.ReadUint32FromBufferLE(buff, offset)
		var cnt uint64
		cnt, offset = encoding.ReadUint64FromBufferLE(buff, offset)
		mr.levelTableCounts[int(lev)] = int(cnt)
	}
	var np uint32
	np, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	mr.slabRetentions = make(map[uint64]uint64, np)
	for i := 0; i < int(np); i++ {
		var slabID uint64
		slabID, offset = encoding.ReadUint64FromBufferLE(buff, offset)
		var retention uint64
		retention, offset = encoding.ReadUint64FromBufferLE(buff, offset)
		mr.slabRetentions[slabID] = retention
	}
	var lfv uint64
	lfv, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	mr.lastFlushedVersion = int64(lfv)
	var lpr uint64
	lpr, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	mr.lastProcessedReplSeq = int(lpr)
	mr.stats = &Stats{}
	return mr.stats.Deserialize(buff, offset)
}

type VersionRange struct {
	VersionStart uint64
	VersionEnd   uint64
}

func (v *VersionRange) Serialize(buff []byte) []byte {
	buff = encoding.AppendUint64ToBufferLE(buff, v.VersionStart)
	return encoding.AppendUint64ToBufferLE(buff, v.VersionEnd)
}

func (v *VersionRange) Deserialize(buff []byte, offset int) int {
	v.VersionStart, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	v.VersionEnd, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	return offset
}

type levelEntries struct {
	segmentEntries []segmentEntry
	maxVersion     uint64 // Note this is the max version ever stored in the level, not necessarily the current max version in the level
}
