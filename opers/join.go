package opers

import (
	"fmt"
	"github.com/alecthomas/participle/v2/lexer"
	encoding2 "github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/iteration"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"time"
)

type JoinOperator struct {
	BaseOperator
	outSchema                   *OperatorSchema
	leftTable                   *StoreTableOperator
	rightTable                  *StoreTableOperator
	joinType                    JoinType
	leftTableSlabID             int
	rightTableSlabID            int
	leftStoreKeyCols            []int
	rightStoreKeyCols           []int
	leftStoreRowCols            []int
	rightStoreRowCols           []int
	leftLookupKeyCols           []int
	leftLookupKeyTypes          []types.ColumnType
	leftLookupRowCols           []int
	rightLookupRowCols          []int
	leftLookupRowTypes          []types.ColumnType
	rightLookupRowTypes         []types.ColumnType
	leftColsToKeep              []int
	rightColsToKeep             []int
	leftEventTimeColIndex       int
	rightEventTimeColIndex      int
	rightLookupOffsetInOutput   int
	isStreamTableJoin           bool
	externalTableID             int
	nodeID                      int
	withinMillis                int64
	receiverID                  int
	batchReceiver               *batchReceiver
	keySequences                []uint64
	leftHandleCtx               *handleIncomingCtx
	rightHandleCtx              *handleIncomingCtx
	forwardProcIDs              []int
	procReceiverBarrierVersions []map[int]int
	leftInput                   Operator
	rightInput                  Operator
	hashCache                   *partitionHashCache
}

type JoinType int

const JoinTypeUnknown = JoinType(-1)
const JoinTypeInner = JoinType(0)
const JoinTypeLeftOuter = JoinType(1)
const JoinTypeRightOuter = JoinType(2)

const keyInitialBufferSize = 48

var leftIndicator = []byte{0}
var rightIndicator = []byte{1}

type handleIncomingCtx struct {
	incomingLeft           bool
	incomingSlabID         int
	incomingKeyCols        []int
	incomingRowCols        []int
	incomingOffsetInOutput int
	incomingColsToKeep     []int
	lookupSlabID           int
	lookupKeyCols          []int
	lookupRowCols          []int
	lookupKeyTypes         []types.ColumnType
	lookupRowTypes         []types.ColumnType
	lookupOffsetInOutput   int
	eventTimeColIndex      int
}

func NewJoinOperator(leftTableSlabID int, rightTableSlabID int, left Operator, right Operator,
	leftIsTable bool, rightIsTable bool, leftSlab *SlabInfo, rightSlab *SlabInfo, joinElements []parser.JoinElement,
	within time.Duration, nodeID int, receiverID int, op *parser.JoinDesc) (*JoinOperator, error) {

	isStreamTableJoin := leftIsTable || rightIsTable

	joinType := JoinTypeUnknown

	s1 := left.OutSchema()
	s2 := right.OutSchema()

	// Calculate unique set of incoming processor ids
	procIDs := map[int]struct{}{}
	for _, ids := range [][]int{s1.ProcessorIDs, s2.ProcessorIDs} {
		for _, procID := range ids {
			procIDs[procID] = struct{}{}
		}
	}
	maxProcID := -1
	var forwardProcIDs []int
	for procID := range procIDs {
		forwardProcIDs = append(forwardProcIDs, procID)
		if procID > maxProcID {
			maxProcID = procID
		}
	}
	forwardingProcCount := len(procIDs)

	// For each sending processor - we maintain a map of receiver id to last barrier version forwarded
	procReceiverBarrierVersions := make([]map[int]int, maxProcID+1)
	for _, procID := range forwardProcIDs {
		procReceiverBarrierVersions[procID] = map[int]int{}
	}

	keySequences := make([]uint64, 1+maxProcID)

	leftCols := map[string]struct{}{}
	for _, colName := range s1.EventSchema.ColumnNames() {
		leftCols[colName] = struct{}{}
	}
	rightCols := map[string]struct{}{}
	for _, colName := range s2.EventSchema.ColumnNames() {
		rightCols[colName] = struct{}{}
	}

	var leftKeyCols []string
	var rightKeyCols []string
	rightKeyMap := map[string]struct{}{}
	for _, elem := range joinElements {
		var jt JoinType
		switch elem.JoinType {
		case "=":
			jt = JoinTypeInner
		case "*=":
			jt = JoinTypeLeftOuter
		case "=*":
			jt = JoinTypeRightOuter
		default:
			panic("invalid joinType")
		}
		if joinType != JoinTypeUnknown && joinType != jt {
			return nil, statementErrorAtPositionf(elem.JoinTypeToken, op, "the same join type (one of `=`, `*=` or `=*`) must be used for all join expression")
		}
		joinType = jt
		if err := checkKeyColumn(elem.LeftCol, leftCols, op, elem.LeftToken); err != nil {
			return nil, err
		}
		if err := checkKeyColumn(elem.RightCol, rightCols, op, elem.RightToken); err != nil {
			return nil, err
		}
		leftKeyCols = append(leftKeyCols, elem.LeftCol)
		rightKeyCols = append(rightKeyCols, elem.RightCol)
		rightKeyMap[elem.RightCol] = struct{}{}
	}

	outerSideTable := false
	var outerToken lexer.Token
	if leftIsTable && joinType == JoinTypeLeftOuter {
		outerSideTable = true
		outerToken = op.LeftStreamToken
	} else if rightIsTable && joinType == JoinTypeRightOuter {
		outerSideTable = true
		outerToken = op.RightStreamToken
	}
	if outerSideTable {
		return nil, statementErrorAtPositionf(outerToken, op, "with an outer join, the outer side of the join cannot be a table")
	}

	if !isStreamTableJoin {
		leftKeyCols = append(leftKeyCols, EventTimeColName)
		rightKeyCols = append(rightKeyCols, EventTimeColName)
	}

	leftTable, err := NewStoreTableOperator(left.OutSchema(), leftTableSlabID, leftKeyCols, nodeID, op)
	if err != nil {
		return nil, err
	}
	rightTable, err := NewStoreTableOperator(right.OutSchema(), rightTableSlabID, rightKeyCols, nodeID, op)
	if err != nil {
		return nil, err
	}

	rightLookupRowCols := make([]int, len(rightTable.outRowCols))
	// We don't include key columns, other than event_time which is always at position 0, so the rowcols will
	// just be ascending by 1 starting at 1 or 0 depending on whether stream-stream or stream-table join
	for i := range rightTable.outRowCols {
		if rightIsTable {
			// If we're looking up in external table, then we don't include event_time in the key being looked up,
			// instead it's retrieved from the row cols of the looked up row
			rightLookupRowCols[i] = i
		} else {
			// We're going to lookup in internal table, and we will get event_time from the retrieved key, so we skip
			// the event_time in the output schema as that will be filled in from the key not the row cols
			rightLookupRowCols[i] = i + 1
		}
	}

	var leftKeyColumnTypes []types.ColumnType
	for _, col := range leftTable.outKeyCols {
		leftType := leftTable.outSchema.EventSchema.ColumnTypes()[col]
		leftKeyColumnTypes = append(leftKeyColumnTypes, leftType)
	}
	var rightKeyColumnTypes []types.ColumnType
	for i, col := range rightTable.outKeyCols {
		rightType := rightTable.outSchema.EventSchema.ColumnTypes()[col]
		if rightType.ID() != leftKeyColumnTypes[i].ID() {
			joinElem := joinElements[i]
			return nil, statementErrorAtPositionf(joinElem.JoinTypeToken, op, "cannot join columns '%s' and '%s' - they have different types %s and %s", leftKeyCols[i], rightKeyCols[i],
				leftKeyColumnTypes[i].String(), rightType.String())
		}
		rightKeyColumnTypes = append(rightKeyColumnTypes, rightType)
	}

	// We check that the key cols in the external table are compatible with the join cols. They are compatible if the
	// join cols are the same as the table key cols
	// Note that we do not currently allow something like:  join cols being [cust_id, country]
	// and key cols in external table being [cust_id, country, city]
	// which looks like it could allow looking up multiple rows for each incoming. But the problem is that the join table
	// will likely have been partitioned on [cust_id, country] whereas the stream would be partitioned on [customer_id]
	// this means not all table rows for same cust_id will necessarily be in the same partition as the incoming row
	// for that cust_id, so the lookup may miss joining matching rows.
	// The user cannot just partition the table only on [cust_id] as that would mean get queries would fail as they
	// would hash [cust_id, country] to find the target partition, not [cust_id].
	// In the future we would allow looking up matching rows using a query when not in same partition, but this adds
	// significant complexity and would have negative performance implications.
	if leftIsTable && (leftSlab == nil || !keyColsCompatible(leftSlab.KeyColIndexes, leftTable.outKeyCols)) {
		return nil, statementErrorAtTokenNamef("", op, "specified join columns for table on the left of the join do not match the stored key columns in the table")
	}
	if rightIsTable && (rightSlab == nil || !keyColsCompatible(rightSlab.KeyColIndexes, rightTable.outKeyCols)) {
		return nil, statementErrorAtTokenNamef("", op, "specified join columns for table on the right of the join do not match the stored key columns in the table")
	}

	var externalTableID int
	if leftIsTable {
		externalTableID = leftSlab.SlabID
	}
	if rightIsTable {
		externalTableID = rightSlab.SlabID
	}

	var leftRowColumnTypes []types.ColumnType
	for _, col := range leftTable.outRowCols {
		leftRowColumnTypes = append(leftRowColumnTypes, leftTable.outSchema.EventSchema.ColumnTypes()[col])
	}
	var rightRowColumnTypes []types.ColumnType
	for _, col := range rightTable.outRowCols {
		rightRowColumnTypes = append(rightRowColumnTypes, rightTable.outSchema.EventSchema.ColumnTypes()[col])
	}

	// The output schema is event_time, followed by all the columns from the left followed by all the columns from the right without the right key cols
	// columns are prefixed with l_ and r_ to disambiguate
	outFNames := []string{EventTimeColName}
	outFTypes := []types.ColumnType{types.ColumnTypeTimestamp}

	var leftColsToKeep []int
	for i, leftColName := range left.OutSchema().EventSchema.ColumnNames() {
		if leftColName == OffsetColName {
			continue
		}
		if isStreamTableJoin && !leftIsTable && leftColName == EventTimeColName {
			// for a stream-table join we omit the event_time column from the incoming stream side as this will be present
			// as the overall event_time
			continue
		}
		outFNames = append(outFNames, fmt.Sprintf("l_%s", leftColName))
		outFTypes = append(outFTypes, left.OutSchema().EventSchema.ColumnTypes()[i])
		leftColsToKeep = append(leftColsToKeep, i)
	}
	var rightColsToKeep []int
	for i, rightColName := range right.OutSchema().EventSchema.ColumnNames() {
		if rightColName == OffsetColName {
			continue
		}
		if isStreamTableJoin && !rightIsTable && rightColName == EventTimeColName {
			// for a stream-table join we omit the event_time column from the incoming stream side as this will be present
			// as the overall event_time
			continue
		}
		_, isKey := rightKeyMap[rightColName]
		if !isKey {
			outFNames = append(outFNames, fmt.Sprintf("r_%s", rightColName))
			outFTypes = append(outFTypes, right.OutSchema().EventSchema.ColumnTypes()[i])
			rightColsToKeep = append(rightColsToKeep, i)
		}
	}
	outEvSchema := evbatch.NewEventSchema(outFNames, outFTypes)
	outSchema := left.OutSchema().Copy()
	outSchema.EventSchema = outEvSchema

	var leftEventTimeColIndex int
	if HasOffsetColumn(left.OutSchema().EventSchema) {
		leftEventTimeColIndex = 1
	}
	var rightEventTimeColIndex int
	if HasOffsetColumn(right.OutSchema().EventSchema) {
		rightEventTimeColIndex = 1
	}

	// stream-table - incoming on left
	// we don't include event-time as will be present on overall event-time
	leftStoreRowCols := leftTable.rowCols
	if isStreamTableJoin && !leftIsTable {
		leftStoreRowCols = leftStoreRowCols[1:]
	}
	rightStoreRowCols := rightTable.rowCols
	if isStreamTableJoin && !rightIsTable {
		rightStoreRowCols = rightStoreRowCols[1:]
	}

	rightLookupOffsetInOutput := len(leftTable.outSchema.EventSchema.ColumnTypes()) + 1
	if isStreamTableJoin && rightIsTable {
		// We don't include event-time so offset is one less
		rightLookupOffsetInOutput--
	}

	jo := &JoinOperator{
		outSchema:                   outSchema,
		leftTable:                   leftTable,
		rightTable:                  rightTable,
		joinType:                    joinType,
		leftStoreKeyCols:            leftTable.inKeyCols,
		rightStoreKeyCols:           rightTable.inKeyCols,
		leftLookupKeyCols:           leftTable.outKeyCols,
		leftLookupRowCols:           leftTable.outRowCols,
		rightLookupRowCols:          rightLookupRowCols,
		leftStoreRowCols:            leftStoreRowCols,
		rightStoreRowCols:           rightStoreRowCols,
		leftLookupKeyTypes:          leftKeyColumnTypes,
		leftLookupRowTypes:          leftRowColumnTypes,
		rightLookupRowTypes:         rightRowColumnTypes,
		leftTableSlabID:             leftTableSlabID,
		rightTableSlabID:            rightTableSlabID,
		leftColsToKeep:              leftColsToKeep,
		rightColsToKeep:             rightColsToKeep,
		isStreamTableJoin:           isStreamTableJoin,
		rightLookupOffsetInOutput:   rightLookupOffsetInOutput,
		externalTableID:             externalTableID,
		nodeID:                      nodeID,
		withinMillis:                within.Milliseconds(),
		leftEventTimeColIndex:       leftEventTimeColIndex,
		rightEventTimeColIndex:      rightEventTimeColIndex,
		receiverID:                  receiverID,
		keySequences:                keySequences,
		forwardProcIDs:              forwardProcIDs,
		procReceiverBarrierVersions: procReceiverBarrierVersions,
		hashCache:                   newPartitionHashCache(outSchema.MappingID, outSchema.Partitions),
	}
	if !leftIsTable {
		jo.leftInput = &inputOper{
			left: true,
			jo:   jo,
		}
	}
	if !rightIsTable {
		jo.rightInput = &inputOper{
			left: false,
			jo:   jo,
		}
	}
	jo.batchReceiver = &batchReceiver{
		j:                   jo,
		forwardingProcCount: forwardingProcCount,
	}
	// We create these context objects to capture the non changing arguments that we use when processing an incoming batch
	// to avoid passing many args into the method every time.
	jo.leftHandleCtx = &handleIncomingCtx{
		incomingLeft:           true,
		incomingSlabID:         jo.leftTableSlabID,
		incomingKeyCols:        jo.leftStoreKeyCols,
		incomingRowCols:        jo.leftStoreRowCols,
		incomingOffsetInOutput: 1,
		lookupSlabID:           jo.rightTableSlabID,
		lookupKeyCols:          nil,
		lookupRowCols:          jo.rightLookupRowCols,
		lookupKeyTypes:         []types.ColumnType{types.ColumnTypeTimestamp},
		lookupRowTypes:         jo.rightLookupRowTypes,
		lookupOffsetInOutput:   jo.rightLookupOffsetInOutput,
		incomingColsToKeep:     jo.leftColsToKeep,
		eventTimeColIndex:      jo.leftEventTimeColIndex,
	}
	jo.rightHandleCtx = &handleIncomingCtx{
		incomingLeft:           false,
		incomingSlabID:         jo.rightTableSlabID,
		incomingKeyCols:        jo.rightStoreKeyCols,
		incomingRowCols:        jo.rightStoreRowCols,
		incomingOffsetInOutput: len(jo.leftTable.outSchema.EventSchema.ColumnTypes()) + 1,
		lookupSlabID:           jo.leftTableSlabID,
		lookupKeyCols:          jo.leftLookupKeyCols,
		lookupRowCols:          jo.leftLookupRowCols,
		lookupKeyTypes:         jo.leftLookupKeyTypes,
		lookupRowTypes:         jo.leftLookupRowTypes,
		lookupOffsetInOutput:   1,
		incomingColsToKeep:     jo.rightColsToKeep,
		eventTimeColIndex:      jo.rightEventTimeColIndex,
	}
	return jo, nil
}

func keyColsCompatible(colsExternal []int, joinCols []int) bool {
	if len(joinCols) != len(colsExternal) {
		return false
	}
	for i, joinCol := range joinCols {
		if joinCol != colsExternal[i] {
			return false
		}
	}
	return true
}

func checkKeyColumn(columnName string, colNames map[string]struct{}, desc *parser.JoinDesc, token lexer.Token) error {
	if columnName == OffsetColName || columnName == EventTimeColName {
		return statementErrorAtPositionf(token, desc, "joining with column '%s' is not allowed", columnName)
	}
	_, available := colNames[columnName]
	if !available {
		return statementErrorAtPositionf(token, desc, "cannot join with column '%s' - it is not a known column in the input stream", columnName)
	}
	return nil
}

func (j *JoinOperator) receiveBatch(batch *evbatch.Batch, execCtx StreamExecContext) error {
	var resBatch *evbatch.Batch
	var err error
	incomingLeft := execCtx.EventBatchBytes()[0] == 0
	if incomingLeft {
		resBatch, err = j.handleIncoming(batch, j.leftHandleCtx, execCtx)
	} else {
		resBatch, err = j.handleIncoming(batch, j.rightHandleCtx, execCtx)
	}
	if err != nil {
		return err
	}
	_, err = j.dispatchBatch(resBatch, execCtx)
	return err
}

func (j *JoinOperator) handleIncoming(batch *evbatch.Batch, ctx *handleIncomingCtx, execCtx StreamExecContext) (*evbatch.Batch, error) {
	includeNonMatched := (ctx.incomingLeft && j.joinType == JoinTypeLeftOuter) || (!ctx.incomingLeft && j.joinType == JoinTypeRightOuter)
	if j.isStreamTableJoin {
		return j.handleIncomingStreamTable(batch, execCtx, ctx, includeNonMatched)
	} else {
		return j.handleIncomingStreamStream(batch, execCtx, ctx, includeNonMatched)
	}
}

func (j *JoinOperator) handleIncomingStreamTable(batch *evbatch.Batch, execCtx StreamExecContext, ctx *handleIncomingCtx,
	includeNonMatched bool) (*evbatch.Batch, error) {

	// It's a stream-table/table-stream join, we look up in the table that's external to the join.
	partitionHash := j.hashCache.getHash(execCtx.PartitionID())
	rc := batch.RowCount
	eventTimeCol := batch.GetTimestampColumn(ctx.eventTimeColIndex)
	var outBuilders []evbatch.ColumnBuilder
	for i := 0; i < rc; i++ {
		lookupStart := encoding2.EncodeEntryPrefix(partitionHash, uint64(j.externalTableID), keyInitialBufferSize)
		lookupStart = evbatch.EncodeKeyCols(batch, i, ctx.incomingKeyCols, lookupStart)
		// If all key values are null then there will just be a 0 null marker for each key col plus the prefix
		if len(lookupStart) == 24+len(ctx.incomingKeyCols) {
			// We don't join on null (result of null == null is false, same in SQL)
			continue
		}
		lookupEnd := common.IncBigEndianBytes(lookupStart)
		log.Debugf("looking up row in external table start %v end %v version %d", lookupStart, lookupEnd, execCtx.WriteVersion())
		incomingET := eventTimeCol.Get(i).Val
		iter, err := execCtx.Processor().NewIterator(lookupStart, lookupEnd, uint64(execCtx.WriteVersion()), false)
		if err != nil {
			return nil, err
		}
		outBuilders, err = j.appendRows(iter, batch, i, ctx, includeNonMatched, outBuilders, eventTimeCol, true, incomingET)
		if err != nil {
			return nil, err
		}
	}

	if outBuilders != nil {
		batch := evbatch.NewBatchFromBuilders(j.outSchema.EventSchema, outBuilders...)
		return batch, nil
	}

	return nil, nil
}

func (j *JoinOperator) handleIncomingStreamStream(batch *evbatch.Batch, execCtx StreamExecContext,
	ctx *handleIncomingCtx, includeNonMatched bool) (*evbatch.Batch, error) {

	// stream-stream join

	partitionHash := j.hashCache.getHash(execCtx.PartitionID())
	rc := batch.RowCount
	eventTimeCol := batch.GetTimestampColumn(ctx.eventTimeColIndex)
	var outBuilders []evbatch.ColumnBuilder
	for i := 0; i < rc; i++ {
		// We store the incoming in the internal table
		persistKeyBuff := encoding2.EncodeEntryPrefix(partitionHash, uint64(ctx.incomingSlabID), keyInitialBufferSize)
		persistKeyBuff = evbatch.EncodeKeyCols(batch, i, ctx.incomingKeyCols, persistKeyBuff)

		// If all key values are null then there will just be a 0 null marker for each key col plus the prefix plus the
		// encoded event time
		if len(persistKeyBuff) == 24+9+len(ctx.incomingKeyCols)-1 {
			// We don't join on null (result of null == null is false, same in SQL)
			continue
		}

		// It's possible that two events might arrive with the exact same event_time and with exact same join columns
		// but other columns are different. If we just keyed the table with the join columns and event_time then later
		// stored events would overwrite earlier ones. We therefore add an incrementing sequence on the key to make the
		// stored keys unique.
		procID := execCtx.Processor().ID()
		persistKeyBuff = encoding2.AppendUint64ToBufferBE(persistKeyBuff, j.keySequences[procID])
		j.keySequences[procID]++ // Safe to increment with no lock, as processor always on same GR

		persistKeyBuff = encoding2.EncodeVersion(persistKeyBuff, uint64(execCtx.WriteVersion()))

		persistRowBuff := evbatch.EncodeRowCols(batch, i, ctx.incomingRowCols, make([]byte, 0, rowInitialBufferSize))

		if execCtx.WriteVersion() < 0 {
			panic(fmt.Sprintf("invalid write version: %d", execCtx.WriteVersion()))
		}

		log.Debugf("node %d storing key %v value %v with version %d", j.nodeID, persistKeyBuff,
			persistRowBuff, execCtx.WriteVersion())

		execCtx.StoreEntry(common.KV{
			Key:   persistKeyBuff,
			Value: persistRowBuff,
		}, false)

		// Lookup in the other table
		lpkb := len(persistKeyBuff)

		// We lookup from (et - within) to (et + within)

		lookupStart := encoding2.EncodeEntryPrefix(partitionHash, uint64(ctx.lookupSlabID), lpkb)
		lookupStart = append(lookupStart, persistKeyBuff[24:lpkb-24]...) // append the key without the prefix and event_time, sequence and version
		incomingET := eventTimeCol.Get(i).Val

		// note that within upper bound is inclusive, this ensures the join gives the same results irrespective of
		// whether left was processed before right or vice versa. We verify this in script test.
		etBoundFrom := incomingET - j.withinMillis
		etBoundTo := incomingET + j.withinMillis + 1
		if log.DebugEnabled {
			tFrom := time.UnixMilli(etBoundFrom)
			tTo := time.UnixMilli(etBoundTo)
			log.Debugf("looking up from et %s to et %s (excl)", tFrom.Format(time.RFC3339), tTo.Format(time.RFC3339))
		}
		// The event time part of the key is always the last element on the key
		lookupStart = encoding2.KeyEncodeInt(lookupStart, etBoundFrom) // note iterator upper bound is exclusive

		lookupEnd := make([]byte, lpkb-24, lpkb-16)
		copy(lookupEnd, lookupStart)
		lookupEnd = encoding2.KeyEncodeInt(lookupEnd, etBoundTo)

		log.Debugf("lookup start %v end %v maxVersion %d", lookupStart, lookupEnd, execCtx.WriteVersion())
		iter, err := execCtx.Processor().NewIterator(lookupStart, lookupEnd, uint64(execCtx.WriteVersion()), false)
		if err != nil {
			return nil, err
		}
		outBuilders, err = j.appendRows(iter, batch, i, ctx, includeNonMatched, outBuilders, eventTimeCol, false,
			incomingET)
		if err != nil {
			return nil, err
		}
	}

	if outBuilders != nil {
		batch := evbatch.NewBatchFromBuilders(j.outSchema.EventSchema, outBuilders...)
		return batch, nil
	}

	return nil, nil
}

func (j *JoinOperator) appendRows(iter iteration.Iterator, batch *evbatch.Batch, rowIndex int, ctx *handleIncomingCtx,
	includeNonMatched bool, outBuilders []evbatch.ColumnBuilder, eventTimeCol *evbatch.TimestampColumn, streamTableJoin bool,
	incomingET int64) ([]evbatch.ColumnBuilder, error) {
	lookedUp := false
	for {
		valid, curr, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if !valid {
			break
		}

		// combine the column values from the incoming batch with the column vals from the looked up row
		if outBuilders == nil {
			outBuilders = evbatch.CreateColBuilders(j.outSchema.EventSchema.ColumnTypes())
		}

		// copy from the incoming batch
		j.copyFromIncomingBatch(batch, rowIndex, ctx.incomingColsToKeep, ctx.incomingOffsetInOutput, outBuilders)

		var etLookup int64
		if !streamTableJoin {
			// stream-stream join - lookup includes event_time on the key
			// The event_time is always the last entry on the key, so we can extract it directly
			lk := len(curr.Key)
			etLookup, _ = encoding2.KeyDecodeInt(curr.Key, lk-24) // after the event_time is sequence then version
		}

		if ctx.incomingLeft {
			if !streamTableJoin {
				// stream-stream join
				// We're looking up the right columns from store, we're only interested in event time here as we don't
				// include the other key columns as they're already included on the left
				rightEventTimeColBuilder := outBuilders[ctx.lookupOffsetInOutput].(*evbatch.TimestampColBuilder)
				rightEventTimeColBuilder.Append(types.NewTimestamp(etLookup))
				// for a stream-table join we don't include any key columns
			}
		} else {
			// get key columns from looked up key
			if err := LoadColsFromKey(outBuilders[ctx.lookupOffsetInOutput:], ctx.lookupKeyTypes, ctx.lookupKeyCols, curr.Key); err != nil {
				return nil, err
			}
		}

		LoadColsFromValue(outBuilders[ctx.lookupOffsetInOutput:], ctx.lookupRowTypes, ctx.lookupRowCols,
			curr.Value)

		// for a stream-table join we always use the event time from the stream side
		// for a stream-stream join, we use the maximum et
		etToUse := incomingET
		if !streamTableJoin && etLookup > etToUse {
			etToUse = etLookup
		}

		outBuilders[0].(*evbatch.TimestampColBuilder).Append(types.NewTimestamp(etToUse))

		lookedUp = true
	}
	if !lookedUp && includeNonMatched {

		// It's an outer join, and we need to include the incoming row, even if it matched nothing
		// combine the column values from the incoming batch with the column vals from the looked up row

		if outBuilders == nil {
			outBuilders = evbatch.CreateColBuilders(j.outSchema.EventSchema.ColumnTypes())
		}
		// copy from the incoming batch
		j.copyFromIncomingBatch(batch, rowIndex, ctx.incomingColsToKeep, ctx.incomingOffsetInOutput, outBuilders)

		// Set rest to null
		for _, k := range ctx.lookupKeyCols {
			outBuilders[ctx.lookupOffsetInOutput+k].AppendNull()
		}
		for _, k := range ctx.lookupRowCols {
			outBuilders[ctx.lookupOffsetInOutput+k].AppendNull()
		}

		if ctx.incomingLeft && !j.isStreamTableJoin {
			// Set the right event-time to null - this won't be in the lookupKeyCols
			outBuilders[ctx.lookupOffsetInOutput].AppendNull()
		}

		// Fill in overall event-time
		incomingET := eventTimeCol.Get(rowIndex).Val
		outBuilders[0].(*evbatch.TimestampColBuilder).Append(types.NewTimestamp(incomingET))
	}
	return outBuilders, nil
}

func (j *JoinOperator) copyFromIncomingBatch(batch *evbatch.Batch, rowIndex int, storeColsToKeep []int, storeOffsetInOutput int,
	outBuilders []evbatch.ColumnBuilder) {
	for index, k := range storeColsToKeep {
		incomingType := batch.Schema.ColumnTypes()[k]
		col := batch.Columns[k]
		indexInOutput := index + storeOffsetInOutput
		evbatch.CopyColumnEntryWithCol(incomingType, col, outBuilders[indexInOutput], rowIndex)
	}
}

func (j *JoinOperator) HandleStreamBatch(*evbatch.Batch, StreamExecContext) (*evbatch.Batch, error) {
	panic("not supported")
}

// When we receive a batch on either left or right input we do not process it directly, instead we forward it to
// the same (current) processor where it is received by BatchReceiver and processed. We do this so barriers
// can be handled properly. If we handled left and right directly, then we would receive two barriers for each version
// one on left and one on right. These could arrive at different times. We would not know what version the output batch
// should be emitted at. We would need somehow to delay batches in the operator when barriers arrive, in a similar way
// to how barriers and batches are delayed in the Processor. This would duplicate the barrier logic and be hard to
// implement as the HandleStreamBatch signature is synchronous not asynchronous - i.e. processing must complete by
// the time it has been executed.
func (j *JoinOperator) forwardBatchToReceiver(left bool, batch *evbatch.Batch, execCtx StreamExecContext) {
	pid := execCtx.Processor().ID()
	pb := proc.NewProcessBatch(pid, batch, j.receiverID, execCtx.PartitionID(), pid)
	pb.Version = execCtx.WriteVersion()
	// We hijack eventBatchBytes here to mark whether from left or right, the batch is always sent locally
	// so, it won't be used to deserialize batch
	if left {
		pb.EvBatchBytes = leftIndicator
	} else {
		pb.EvBatchBytes = rightIndicator
	}
	execCtx.Processor().IngestBatch(pb, func(err error) {
		if err != nil {
			log.Errorf("failed to forward batch for join: %v", err)
		}
	})
}

func (j *JoinOperator) dispatchBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	if batch != nil {
		return nil, j.sendBatchDownStream(batch, execCtx)
	}
	return nil, nil
}

func (j *JoinOperator) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not supported")
}

func (j *JoinOperator) HandleBarrier(execCtx StreamExecContext) error {
	// See note in UnionOperator.HandleBarrier
	sendingProcessorID := execCtx.Processor().ID()
	lastSentMap := j.procReceiverBarrierVersions[sendingProcessorID]
	lastSent, ok := lastSentMap[execCtx.ReceiverID()]
	if ok && lastSent == execCtx.WriteVersion() {
		return nil
	}
	for _, processorID := range j.forwardProcIDs {
		execCtx.ForwardBarrier(processorID, j.receiverID)
	}
	lastSentMap[execCtx.ReceiverID()] = execCtx.WriteVersion()
	return nil
}

func (j *JoinOperator) InSchema() *OperatorSchema {
	// Join has no single in schema - it joins two inputs which can have different schemas
	return nil
}

func (j *JoinOperator) OutSchema() *OperatorSchema {
	return j.outSchema
}

func (j *JoinOperator) Setup(mgr StreamManagerCtx) error {
	mgr.RegisterReceiver(j.receiverID, j.batchReceiver)
	return nil
}

func (j *JoinOperator) Teardown(mgr StreamManagerCtx, completeCB func(error)) {
	mgr.UnregisterReceiver(j.receiverID)
	completeCB(nil)
}

type inputOper struct {
	left bool
	jo   *JoinOperator
}

func (r *inputOper) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	r.jo.forwardBatchToReceiver(r.left, batch, execCtx)
	return nil, nil
}

func (r *inputOper) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not supported")
}

func (r *inputOper) HandleBarrier(execCtx StreamExecContext) error {
	return r.jo.HandleBarrier(execCtx)
}

func (r *inputOper) InSchema() *OperatorSchema {
	return nil
}

func (r *inputOper) OutSchema() *OperatorSchema {
	return nil
}

func (r *inputOper) Setup(StreamManagerCtx) error {
	return nil
}

func (r *inputOper) AddDownStreamOperator(Operator) {
}

func (r *inputOper) GetDownStreamOperators() []Operator {
	return []Operator{r.jo}
}

func (r *inputOper) RemoveDownStreamOperator(Operator) {
}

func (r *inputOper) GetParentOperator() Operator {
	return nil
}

func (r *inputOper) SetParentOperator(Operator) {
}

func (r *inputOper) SetStreamInfo(*StreamInfo) {
}

func (r *inputOper) GetStreamInfo() *StreamInfo {
	return nil
}

func (r *inputOper) Teardown(mgr StreamManagerCtx, completeCB func(error)) {
	completeCB(nil)
}

type batchReceiver struct {
	j                   *JoinOperator
	forwardingProcCount int
}

func (b *batchReceiver) InSchema() *OperatorSchema {
	return nil
}

func (b *batchReceiver) OutSchema() *OperatorSchema {
	return nil
}

func (b *batchReceiver) ForwardingProcessorCount() int {
	return b.forwardingProcCount
}

func (b *batchReceiver) ReceiveBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	return nil, b.j.receiveBatch(batch, execCtx)
}

func (b *batchReceiver) ReceiveBarrier(execCtx StreamExecContext) error {
	return b.j.BaseOperator.HandleBarrier(execCtx)
}

func (b *batchReceiver) RequiresBarriersInjection() bool {
	return false
}
