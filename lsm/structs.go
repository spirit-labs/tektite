//lint:file-ignore U1000 Ignore all unused code
package lsm

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
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

func (re *RegistrationEntry) Serialize(buff []byte) []byte {
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

func (re *RegistrationEntry) Deserialize(buff []byte, offset int) int {
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
	Compaction      bool
	JobID           string
	Registrations   []RegistrationEntry
	DeRegistrations []RegistrationEntry
}

func (rb *RegistrationBatch) Serialize(buff []byte) []byte {
	buff = encoding.AppendBoolToBuffer(buff, rb.Compaction)
	buff = encoding.AppendStringToBufferLE(buff, rb.JobID)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(rb.Registrations)))
	for _, reg := range rb.Registrations {
		buff = reg.Serialize(buff)
	}
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(rb.DeRegistrations)))
	for _, dereg := range rb.DeRegistrations {
		buff = dereg.Serialize(buff)
	}
	return buff
}

func (rb *RegistrationBatch) Deserialize(buff []byte, offset int) int {
	rb.Compaction, offset = encoding.ReadBoolFromBuffer(buff, offset)
	rb.JobID, offset = encoding.ReadStringFromBufferLE(buff, offset)
	var l uint32
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	rb.Registrations = make([]RegistrationEntry, l)
	for i := 0; i < int(l); i++ {
		offset = rb.Registrations[i].Deserialize(buff, offset)
	}
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	rb.DeRegistrations = make([]RegistrationEntry, l)
	for i := 0; i < int(l); i++ {
		offset = rb.DeRegistrations[i].Deserialize(buff, offset)
	}
	return offset
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
	buff = encoding.AppendUint64ToBufferLE(buff, te.AddedTime)
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
	te.AddedTime, offset = encoding.ReadUint64FromBufferLE(buff, offset)
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

func (l *Stats) SerializedSize() int {
	size := 7*8 + 4
	size += len(l.LevelStats) * (4 + 3*8)
	return size
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

type MasterRecord struct {
	format             common.MetadataFormat
	version            uint64
	levelEntries       []*levelEntry
	levelTableCounts   map[int]int
	slabRetentions     map[uint64]uint64 // this is a map as we can get duplicate registrations
	lastFlushedVersion int64
	stats              *Stats
}

func NewMasterRecord(format common.MetadataFormat) *MasterRecord {
	return &MasterRecord{
		format:             format,
		version:            0,
		slabRetentions:     map[uint64]uint64{},
		stats:              &Stats{LevelStats: map[int]*LevelStats{}},
		levelTableCounts:   map[int]int{},
		lastFlushedVersion: -1,
	}
}

func (mr *MasterRecord) SerializedSize() int {
	size :=
		1 + // format
			8 + // version
			4 + // num levels
			4 + // num level table counts
			len(mr.levelTableCounts)*(4+8) + // level table counts
			4 + // num slab retentions
			len(mr.slabRetentions)*(8+8) + // slab retentions
			8 // last flushed version
	for _, levEntry := range mr.levelEntries {
		size += levEntry.serializedSize()
	}
	if mr.stats != nil {
		size += mr.stats.SerializedSize()
	}
	return size
}

func (mr *MasterRecord) Serialize(buff []byte) []byte {
	buff = append(buff, byte(mr.format))
	buff = encoding.AppendUint64ToBufferLE(buff, mr.version)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(mr.levelEntries)))
	for _, entry := range mr.levelEntries {
		buff = entry.Serialize(buff)
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
	return mr.stats.Serialize(buff)
}

func (mr *MasterRecord) Deserialize(buff []byte, offset int) int {
	mr.format = common.MetadataFormat(buff[offset])
	offset++
	mr.version, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	var nl uint32
	nl, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	mr.levelEntries = make([]*levelEntry, nl)
	for level := 0; level < int(nl); level++ {
		mr.levelEntries[level] = &levelEntry{}
		offset = mr.levelEntries[level].Deserialize(buff, offset)
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
	mr.stats = &Stats{}
	return mr.stats.Deserialize(buff, offset)
}

type levelEntry struct {
	maxVersion       uint64 // Note this is the max version ever stored in the level, not necessarily the current max version in the level
	rangeStart       []byte
	rangeEnd         []byte
	tableEntries     []levelTableEntry
	tableEntriesBuff []byte
	addedTablesBuff  []byte
	totEntrySizes    int
}

func (le *levelEntry) SetAt(index int, te *TableEntry) {
	prevLength := le.tableEntries[index].length
	pos := len(le.addedTablesBuff)
	le.addedTablesBuff = te.serialize(le.addedTablesBuff)
	length := uint32(len(le.addedTablesBuff) - pos)
	lte := levelTableEntry{
		pos:    pos + len(le.tableEntriesBuff),
		length: length,
	}
	le.tableEntries[index] = lte
	le.totEntrySizes += int(length - prevLength)
}

func (le *levelEntry) InsertAt(index int, te *TableEntry) {
	pos := len(le.addedTablesBuff)
	le.addedTablesBuff = te.serialize(le.addedTablesBuff)
	length := uint32(len(le.addedTablesBuff) - pos)
	// When adding new entries we set pos to be pos in the addedTablesBuff + length of the existing tableEntriesBuff
	// this allows us to distinguish between newly inserted/set entries and ones that are backed by the existing buffer
	// without having to store an extra field in levelTableEntry which would take more memory
	lte := levelTableEntry{
		pos:    pos + len(le.tableEntriesBuff),
		length: length,
	}
	le.tableEntries = insertInSlice(le.tableEntries, index, lte)
	le.totEntrySizes += int(length)
}

func (le *levelEntry) RemoveAt(index int) {
	prevLength := le.tableEntries[index].length
	le.tableEntries = append(le.tableEntries[:index], le.tableEntries[index+1:]...)
	le.totEntrySizes -= int(prevLength)
}

var eightZeroBytes = []byte{0, 0, 0, 0, 0, 0, 0, 0}

func (le *levelEntry) serializedSize() int {
	return 8 + //  max version
		4 + // len(rangeStart)
		len(le.rangeStart) +
		4 + // len(rangeEnd)
		len(le.rangeEnd) +
		8 + // positionPos
		8 + // num table entries
		le.totEntrySizes +
		len(le.tableEntries)*8 // positions
}

func (le *levelEntry) Serialize(buff []byte) []byte {
	buff = encoding.AppendUint64ToBufferLE(buff, le.maxVersion)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(le.rangeStart)))
	buff = append(buff, le.rangeStart...)
	buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(le.rangeEnd)))
	buff = append(buff, le.rangeEnd...)
	// The positions go at the end of the serialized state, and this is the position in the buffer of the start of the
	// positions. We fill it in first with zeros (as we don't know the value until we have serialized all the entries),
	// then we fill it in at the end.
	positionsPos := len(buff)
	buff = append(buff, eightZeroBytes...)
	blockStart := -1
	blockEnd := 0
	newEntries := make([]levelTableEntry, len(le.tableEntries))
	newPosStart := len(buff)
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(len(le.tableEntries)))
	for i, lte := range le.tableEntries {
		if lte.pos >= len(le.tableEntriesBuff) {
			// Added entry
			// Append any block
			if blockStart != -1 {
				buff = append(buff, le.tableEntriesBuff[blockStart:blockEnd]...)
			}
			// Then append the added entry
			addedTablePos := lte.pos - len(le.tableEntriesBuff)
			newPosStart = len(buff)
			block := le.addedTablesBuff[addedTablePos : addedTablePos+int(lte.length)]
			buff = append(buff, block...)
			blockStart = -1
		} else {
			if blockStart != -1 && lte.pos > blockEnd {
				// discontinuity - entry was removed here - append previous block
				buff = append(buff, le.tableEntriesBuff[blockStart:blockEnd]...)
				newPosStart = len(buff)
				blockStart = lte.pos
				blockEnd = lte.pos + int(lte.length)
			} else {
				if blockStart == -1 {
					blockStart = lte.pos
					newPosStart = len(buff)
				} else {
					newPosStart += int(newEntries[i-1].length)
				}
				blockEnd = lte.pos + int(lte.length)
			}
		}
		newEntries[i] = levelTableEntry{
			pos:    newPosStart,
			length: lte.length,
		}
	}
	if blockStart != -1 {
		// Append any final block
		buff = append(buff, le.tableEntriesBuff[blockStart:blockEnd]...)
	}
	// Append the positions
	// TODO if most positions haven't changed no need to compute them again...
	binary.LittleEndian.PutUint64(buff[positionsPos:], uint64(len(buff)))
	for _, ne := range newEntries {
		buff = encoding.AppendUint64ToBufferLE(buff, uint64(ne.pos))
	}
	le.addedTablesBuff = nil // reset
	le.tableEntriesBuff = buff
	le.tableEntries = newEntries
	return buff
}

func (le *levelEntry) Deserialize(buff []byte, offset int) int {
	totEntriesSize := 0
	le.maxVersion, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	var l uint32
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	if l > 0 {
		le.rangeStart = buff[offset : offset+int(l)]
		offset += int(l)
	}
	l, offset = encoding.ReadUint32FromBufferLE(buff, offset)
	if l > 0 {
		le.rangeEnd = buff[offset : offset+int(l)]
		offset += int(l)
	}
	var positionPos uint64
	positionPos, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	var numEntries uint64
	numEntries, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	le.tableEntries = make([]levelTableEntry, numEntries)
	le.tableEntriesBuff = buff
	entriesSize := int(positionPos) - offset
	// Skip over the encoded entries to the positions
	offset = int(positionPos)
	ne := int(numEntries)
	var prevPos uint64
	for i := 0; i < ne; i++ {
		var pos uint64
		pos, offset = encoding.ReadUint64FromBufferLE(buff, offset)
		le.tableEntries[i].pos = int(pos)
		if i > 0 {
			l := pos - prevPos
			totEntriesSize += int(l)
			le.tableEntries[i-1].length = uint32(l)
		}
		prevPos = pos
	}
	if numEntries == 1 {
		totEntriesSize += entriesSize
		le.tableEntries[0].length = uint32(entriesSize)
	} else if numEntries > 1 {
		lastButOne := le.tableEntries[numEntries-2]
		l := int(positionPos) - lastButOne.pos - int(lastButOne.length)
		totEntriesSize += l
		le.tableEntries[numEntries-1].length = uint32(l)
	}
	le.addedTablesBuff = nil
	le.totEntrySizes = totEntriesSize
	return offset
}

func insertInSlice[T any](s []T, index int, value T) []T {
	if index < 0 || index > len(s) {
		panic("index out of bounds")
	}
	ls := len(s)
	cs := cap(s)
	if ls == cs {
		var newCap int
		if cs == 0 {
			// start at 8 so we don't reallocate too much at the beginning
			newCap = 8
		} else {
			newCap = cs * 2
		}
		newSlice := make([]T, ls+1, newCap)
		copy(newSlice, s[:index])
		copy(newSlice[index+1:], s[index:])
		s = newSlice
	} else {
		s = s[:ls+1]
		copy(s[index+1:], s[index:])
	}
	s[index] = value
	return s
}

/*
levelTableEntry - holds the position and length of the table-entry in the underlying buffer.
Note, that we do not Deserialize all table entries when the master record is deserialized, instead we keep them
as a byte slice. This is more memory efficient than deserializing them all. When the database is large, it's important
we keep the in-memory size small. Also, during a typical operation only a small number of table entries are
updated/added/removed, this means the majority do not need to get serialized - we can use the original buffer.
*/
type levelTableEntry struct {
	pos    int
	length uint32
}

func (lte *levelTableEntry) Get(levEntry *levelEntry) *TableEntry {
	var buff []byte
	var offset int
	if lte.pos >= len(levEntry.tableEntriesBuff) {
		buff = levEntry.addedTablesBuff
		// The pos is relative to the tableEntriesBuff
		offset = lte.pos - len(levEntry.tableEntriesBuff)
	} else {
		buff = levEntry.tableEntriesBuff
		offset = lte.pos
	}
	te := &TableEntry{}
	te.deserialize(buff, offset)
	return te
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
