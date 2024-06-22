package opers

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"math"
	"sort"
	"sync"
	"time"
)

func NewAggregateOperator(inSchema *OperatorSchema, aggDesc *parser.AggregateDesc,
	aggStateSlabID int, openWindowsSlabID int, resultsSlabID int, closedWindowReceiverID int,
	size time.Duration, hop time.Duration, lateness time.Duration, storeResults bool,
	includeWindowCols bool, expressionFactory *expr.ExpressionFactory) (*AggregateOperator, error) {

	hasOffset := HasOffsetColumn(inSchema.EventSchema)
	windowed := size != 0
	processSchema := inSchema
	keyExprDescs := aggDesc.KeyExprs
	keyExprStrs := aggDesc.KeyExprsStrings
	if windowed {
		processSchema = inSchema.Copy()
		var startIndex int
		if hasOffset {
			// we remove offset
			startIndex = 1
		}
		fNames := append([]string{windowStartColName, windowEndColName}, inSchema.EventSchema.ColumnNames()[startIndex:]...)
		fTypes := append([]types.ColumnType{types.ColumnTypeTimestamp, types.ColumnTypeTimestamp}, inSchema.EventSchema.ColumnTypes()[startIndex:]...)
		processSchema.EventSchema = evbatch.NewEventSchema(fNames, fTypes)
		keyExprDescs = append([]parser.ExprDesc{&parser.IdentifierExprDesc{IdentifierName: windowStartColName},
			&parser.IdentifierExprDesc{IdentifierName: windowEndColName}}, keyExprDescs...)
		keyExprStrs = append([]string{windowStartColName, windowEndColName}, keyExprStrs...)
	}

	var aggFuncHolders []aggFuncHolder
	var extraStateAggs []int
	var keyColHolders []keyColHolder
	aggExprDescs := aggDesc.AggregateExprs
	numCols := len(aggExprDescs) + len(keyExprDescs) + 1
	aggStateColumnNames := make([]string, numCols)
	aggStateColumnTypes := make([]types.ColumnType, numCols)
	var keyColIndexes []int
	var keyColTypes []types.ColumnType
	var aggColIndexes []int
	var aggColTypes []types.ColumnType

	// The output schema is event_time, followed by key_exprs, followed by agg_exprs
	aggStateColumnNames[0] = EventTimeColName
	aggStateColumnTypes[0] = types.ColumnTypeTimestamp

	colIndex := 1
	for i, keyExprDesc := range keyExprDescs {
		aggStateColumnNames[colIndex] = keyExprStrs[i]
		e, err := expressionFactory.CreateExpression(keyExprDesc, processSchema.EventSchema)
		if err != nil {
			return nil, err
		}
		aggStateColumnTypes[colIndex] = e.ResultType()
		keyColIndexes = append(keyColIndexes, colIndex)
		keyColTypes = append(keyColTypes, aggStateColumnTypes[colIndex])
		keyColHolders = append(keyColHolders, keyColHolder{
			colIndex: colIndex,
			expr:     e,
		})
		colIndex++
	}

	createAggFunc := func(index int, desc parser.ExprDesc, aggExprStr string, allowReservedAlias bool) error {

		ok, aggExprDesc, alias, aliasExprDesc := parser.ExtractAlias(desc)
		if !ok {
			return desc.ErrorAtPosition("invalid alias - must be an identifier")
		}
		fo, ok := aggExprDesc.(*parser.FunctionExprDesc)
		if !ok {
			return aggExprDesc.ErrorAtPosition(
				"'%s' is not a valid aggregate expression. must be one of 'count(<expr>)', 'sum(<expr>)', 'min(<expr>)', 'max(<expr)' or 'avg(<expr>)'",
				aggExprStr)
		}
		aggFuncName := fo.FunctionName
		aggFunc, ok := aggFuncsMap[aggFuncName]
		if !ok {
			return aggExprDesc.ErrorAtPosition("unknown aggregate function '%s'. must be one of 'count', 'sum', 'min' or 'avg'", aggFuncName)
		}
		innerExpr := fo.ArgExprs[0]

		if alias != "" {
			if !allowReservedAlias {
				if isReservedIdentifierName(alias) {
					return aliasExprDesc.ErrorAtPosition("cannot use column alias '%s', it is a reserved name", alias)
				}
			}
			aggStateColumnNames[index] = alias
		} else {
			aggStateColumnNames[index] = aggExprStr
		}

		e, err := expressionFactory.CreateExpression(innerExpr, processSchema.EventSchema)
		if err != nil {
			return err
		}
		aggFuncHolders = append(aggFuncHolders, aggFuncHolder{
			aggFunc:   aggFunc,
			innerExpr: e,
			colIndex:  index,
		})
		if aggFunc.RequiresExtraData() {
			extraStateAggs = append(extraStateAggs, len(aggFuncHolders)-1)
		}

		rt := aggFunc.ReturnTypeForExpressionType(e.ResultType())
		aggStateColumnTypes[index] = rt
		aggColIndexes = append(aggColIndexes, index)
		aggColTypes = append(aggColTypes, rt)
		return nil
	}

	// We create an agg expr for the output event_time - this is max of the event_time values on the group.
	maxEventTimeDesc := parser.BinaryOperatorExprDesc{
		Left: &parser.FunctionExprDesc{
			FunctionName: "max",
			Aggregate:    true,
			ArgExprs:     []parser.ExprDesc{&parser.IdentifierExprDesc{IdentifierName: "event_time"}},
		},
		Right: &parser.IdentifierExprDesc{IdentifierName: "event_time"},
		Op:    "as",
	}
	if err := createAggFunc(0, &maxEventTimeDesc, "max(event_time) as event_time", true); err != nil {
		return nil, err
	}

	for i, aggExprDesc := range aggExprDescs {
		if err := createAggFunc(colIndex, aggExprDesc, aggDesc.AggregateExprStrings[i], false); err != nil {
			return nil, err
		}
		colIndex++
	}

	aggStateSchema := evbatch.NewEventSchema(aggStateColumnNames, aggStateColumnTypes)

	var outEventSchema *evbatch.EventSchema
	var outKeyColIndexes []int
	var outKeyColTypes []types.ColumnType
	var outAggColIndexes []int
	var outAggColTypes []types.ColumnType
	if windowed && !includeWindowCols {
		// remove window start and end cols from the aggStateSchema, these are always at indexes 1 and 2
		var oNames []string
		oNames = append(oNames, aggStateColumnNames[0])
		oNames = append(oNames, aggStateColumnNames[3:]...)
		var oTypes []types.ColumnType
		oTypes = append(oTypes, aggStateColumnTypes[0])
		oTypes = append(oTypes, aggStateColumnTypes[3:]...)
		outEventSchema = evbatch.NewEventSchema(oNames, oTypes)
		// And we need to adjust the key and agg colindexes so they work when loading from the agg state into the output
		// schema
		outKeyColIndexes = make([]int, len(keyColIndexes)-2)
		copy(outKeyColIndexes, keyColIndexes[2:])
		for i, okc := range outKeyColIndexes {
			outKeyColIndexes[i] = okc - 2 // shift left by 2 as we're omitting ws and we
		}
		outKeyColTypes = keyColTypes[2:]
		outAggColIndexes = make([]int, len(aggColIndexes))
		copy(outAggColIndexes, aggColIndexes)
		for i, oac := range outAggColIndexes {
			if oac != 0 { // zero is the event_time agg column, we don't want to shift that
				outAggColIndexes[i] = oac - 2 // shift left by 2 as we're omitting ws and we
			}
		}
		outAggColTypes = aggColTypes
	} else {
		outEventSchema = aggStateSchema
		outKeyColIndexes = keyColIndexes
		outKeyColTypes = keyColTypes
		outAggColIndexes = aggColIndexes
		outAggColTypes = aggColTypes
	}
	outSchema := inSchema.Copy()
	outSchema.EventSchema = outEventSchema

	var processorWatermarks []int64
	if windowed {
		processorWatermarks = make([]int64, processSchema.PartitionScheme.MaxProcessorID+1)
	}

	eventTimeColIndex := 0
	if hasOffset {
		eventTimeColIndex = 1
	}
	processingEventTimeColIndex := eventTimeColIndex
	if windowed {
		processingEventTimeColIndex = 2
	}

	return &AggregateOperator{
		processSchema:               processSchema,
		inSchema:                    inSchema,
		outSchema:                   outSchema,
		aggStateSchema:              aggStateSchema,
		aggFuncHolders:              aggFuncHolders,
		keyColHolders:               keyColHolders,
		keyColIndexes:               keyColIndexes,
		aggColIndexes:               aggColIndexes,
		keyColTypes:                 keyColTypes,
		aggColTypes:                 aggColTypes,
		outKeyColTypes:              outKeyColTypes,
		outKeyColIndexes:            outKeyColIndexes,
		outAggColIndexes:            outAggColIndexes,
		outAggColTypes:              outAggColTypes,
		extraStateAggs:              extraStateAggs,
		hasExtraStateAggs:           len(extraStateAggs) > 0,
		closedWindowReceiverID:      closedWindowReceiverID,
		openWindowsSlabID:           uint64(openWindowsSlabID),
		aggStateSlabID:              uint64(aggStateSlabID),
		resultsSlabID:               uint64(resultsSlabID),
		openWindows:                 make([][]windowEntry, inSchema.PartitionScheme.Partitions),
		windowsLoaded:               make([]bool, inSchema.PartitionScheme.Partitions),
		processorWatermarks:         processorWatermarks,
		lateness:                    lateness.Milliseconds(),
		windowed:                    windowed,
		size:                        int(size.Milliseconds()),
		hop:                         int(hop.Milliseconds()),
		eventTimeColIndex:           eventTimeColIndex,
		processingEventTimeColIndex: processingEventTimeColIndex,
		hasOffset:                   hasOffset,
		storeResults:                storeResults,
		includeWindowCols:           includeWindowCols,
		aggDesc:                     aggDesc,
		hashCache:                   newPartitionHashCache(inSchema.MappingID, inSchema.Partitions),
	}, nil
}

const windowStartColName = "ws"
const windowEndColName = "we"

type AggregateOperator struct {
	BaseOperator
	processSchema               *OperatorSchema
	inSchema                    *OperatorSchema
	outSchema                   *OperatorSchema
	aggStateSchema              *evbatch.EventSchema
	windowed                    bool
	size                        int
	hop                         int
	aggFuncHolders              []aggFuncHolder
	keyColHolders               []keyColHolder
	keyColIndexes               []int
	keyColTypes                 []types.ColumnType
	outKeyColIndexes            []int
	outKeyColTypes              []types.ColumnType
	outAggColTypes              []types.ColumnType
	outAggColIndexes            []int
	aggColIndexes               []int
	aggColTypes                 []types.ColumnType
	extraStateAggs              []int
	hasExtraStateAggs           bool
	closedWindowReceiverID      int
	aggStateSlabID              uint64
	resultsSlabID               uint64
	openWindowsSlabID           uint64
	openWindows                 [][]windowEntry
	windowsLoaded               []bool
	processorWatermarks         []int64
	lateness                    int64
	eventTimeColIndex           int
	processingEventTimeColIndex int
	hasOffset                   bool
	storeResults                bool
	includeWindowCols           bool
	aggDesc                     *parser.AggregateDesc
	hashCache                   *partitionHashCache
}

type windowEntry struct {
	ws int64
	we int64
}

type aggFuncHolder struct {
	aggFunc            AggFunc
	innerExpr          expr.Expression
	colIndex           int
	requiredSourceCols []int
}

type keyColHolder struct {
	colIndex int
	expr     expr.Expression
}

type aggState struct {
	data      []any
	extraData [][]byte
}

func (a *AggregateOperator) HandleQueryBatch(_ *evbatch.Batch, _ QueryExecContext) (*evbatch.Batch, error) {
	panic("not supported in queries")
}

func (a *AggregateOperator) augmentWithWindows(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	if batch == nil || batch.RowCount == 0 {
		return nil, nil
	}

	eventTimeCol := batch.GetTimestampColumn(a.eventTimeColIndex)
	colBuilders := evbatch.CreateColBuilders(a.processSchema.EventSchema.ColumnTypes())
	wsColBuilder := colBuilders[0].(*evbatch.TimestampColBuilder)
	weColBuilder := colBuilders[1].(*evbatch.TimestampColBuilder)

	partitionID := execCtx.PartitionID()
	openWindows, err := a.getOpenWindows(partitionID, execCtx.Processor())
	if err != nil {
		return nil, err
	}

	for i := 0; i < batch.RowCount; i++ {
		eventTime64 := eventTimeCol.Get(i).Val

		lastWatermark := a.processorWatermarks[execCtx.Processor().ID()]

		if eventTime64+a.lateness <= lastWatermark {
			// drop the row - the window is closed and gone
			continue
		}

		eventTime := int(eventTime64)
		// Find the smallest window end which is > eventTime - this gives us the leftmost window that overlaps
		we := a.hop * (1 + (eventTime / a.hop))
		ws := we - a.size

		// Now we find all overlapping windows by incrementing window hop until ws > eventTime
		for ws <= eventTime {

			entry := findWindow(ws, openWindows)
			if entry == nil {
				// create a new window
				entry := windowEntry{ws: int64(ws), we: int64(we)}
				openWindows = addWindow(entry, openWindows)
				// store the open window - we need to do this, so if we crash and restart, we don't end up with open windows
				// never being closed
				partitionHash := a.hashCache.getHash(partitionID)
				key := encoding.EncodeEntryPrefix(partitionHash, a.openWindowsSlabID, 40)
				key = encoding.KeyEncodeTimestamp(key, types.NewTimestamp(int64(ws)))
				key = encoding.EncodeVersion(key, uint64(execCtx.WriteVersion()))
				val := make([]byte, 8)
				binary.LittleEndian.PutUint64(val, uint64(we))
				execCtx.StoreEntry(common.KV{Key: key, Value: val}, false)
				a.windowsLoaded[execCtx.PartitionID()] = true
			}

			// add row to window
			wsColBuilder.Append(types.NewTimestamp(int64(ws)))
			weColBuilder.Append(types.NewTimestamp(int64(we)))
			var startCol int
			if a.hasOffset {
				startCol = 1
			}
			batchSchema := batch.Schema
			for k := startCol; k < len(batchSchema.ColumnTypes()); k++ {
				ft := batchSchema.ColumnTypes()[k]
				col := batch.Columns[k]
				colBuilder := colBuilders[k-startCol+2]
				evbatch.CopyColumnEntryWithCol(ft, col, colBuilder, i)
			}
			ws += a.hop
			we += a.hop
		}
	}
	a.openWindows[partitionID] = openWindows
	batch = evbatch.NewBatchFromBuilders(a.processSchema.EventSchema, colBuilders...)
	return batch, nil
}

func (a *AggregateOperator) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	if a.windowed {
		var err error
		batch, err = a.augmentWithWindows(batch, execCtx)
		if err != nil {
			return nil, err
		}
	}
	return a.handleStreamBatch(batch, execCtx)
}

func (a *AggregateOperator) handleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	defer batch.Release()
	cols, err := a.createCols(batch)
	if err != nil {
		return nil, err
	}

	grouped := a.groupData(cols, batch)

	writtenEntries, err := a.computeAggs(grouped, execCtx)
	if err != nil {
		return nil, err
	}
	if !a.windowed {
		// Create a batch from the written entries and send it downstream
		colBuilders := evbatch.CreateColBuilders(a.aggStateSchema.ColumnTypes())
		for _, entry := range writtenEntries {
			if err := LoadColsFromKey(colBuilders, a.keyColTypes, a.keyColIndexes, entry.Key); err != nil {
				return nil, err
			}
			LoadColsFromValue(colBuilders, a.aggColTypes, a.aggColIndexes, entry.Value)
		}
		batch := evbatch.NewBatchFromBuilders(a.aggStateSchema, colBuilders...)
		return nil, a.sendBatchDownStream(batch, execCtx)
	}
	return nil, nil
}

func findWindow(ws int, windows []windowEntry) *windowEntry {
	// we could binary search here?
	ws64 := int64(ws)
	for _, entry := range windows {
		if entry.ws == ws64 {
			return &entry
		}
	}
	return nil
}

func addWindow(toAdd windowEntry, windows []windowEntry) []windowEntry {
	insertPos := 0
	for _, entry := range windows {
		if entry.ws == toAdd.ws {
			// sanity check
			panic("duplicate window")
		}
		if entry.ws > toAdd.ws {
			break
		}
		insertPos++
	}
	newWindows := make([]windowEntry, len(windows)+1)
	copy(newWindows, windows[:insertPos])
	newWindows[insertPos] = toAdd
	copy(newWindows[insertPos+1:], windows[insertPos:])
	return newWindows
}

func (a *AggregateOperator) loadOpenWindows(partitionID int, processor proc.Processor) ([]windowEntry, error) {
	partitionHash := a.hashCache.getHash(partitionID)
	key := encoding.EncodeEntryPrefix(partitionHash, a.openWindowsSlabID, 24)
	iter, err := processor.NewIterator(key, common.IncrementBytesBigEndian(key), math.MaxUint64, false)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var openWindows []windowEntry
	for {
		valid, err := iter.IsValid()
		if err != nil {
			return nil, err
		}
		if !valid {
			break
		}
		curr := iter.Current()
		ws, _ := encoding.KeyDecodeTimestamp(curr.Key, 24)
		we, _ := encoding.ReadUint64FromBufferLE(curr.Value, 0)
		entry := windowEntry{
			ws: ws.Val,
			we: int64(we),
		}
		openWindows = append(openWindows, entry)
		if err := iter.Next(); err != nil {
			return nil, err
		}
	}
	sort.SliceStable(openWindows, func(i, j int) bool {
		return openWindows[i].ws < openWindows[j].ws
	})
	return openWindows, nil
}

func (a *AggregateOperator) getOpenWindows(partitionID int, processor proc.Processor) ([]windowEntry, error) {
	// Note, we can access windowsLoaded and openWindows without a memory barrier.
	// This is because this method is called on the processor thread that all these partitions always run on.
	// In other words windowsLoaded[x] and openWindows[x] are always accessed by the same goroutine.
	var openWindows []windowEntry
	if !a.windowsLoaded[partitionID] {
		// The first time we receive a barrier for the partition we load any open windows that are persisted.
		var err error
		openWindows, err = a.loadOpenWindows(partitionID, processor)
		if err != nil {
			return nil, err
		}
		a.openWindows[partitionID] = openWindows
		a.windowsLoaded[partitionID] = true
	} else {
		openWindows = a.openWindows[partitionID]
	}
	return openWindows, nil
}

func (a *AggregateOperator) HandleBarrier(execCtx StreamExecContext) error {
	wm := int64(execCtx.WaterMark())
	if a.windowed && wm > 0 {
		a.processorWatermarks[execCtx.Processor().ID()] = wm
		// find any closed windows
		partitionIDs := a.processSchema.PartitionScheme.ProcessorPartitionMapping[execCtx.Processor().ID()]
		for _, partitionID := range partitionIDs {
			// Note, we can access windowsLoaded and openWindows without a memory barrier.
			// This is because this method is called on the processor thread that all these partitions always run on.
			// In other words windowsLoaded[x] and openWindows[x] are always accessed by the same goroutine.
			openWindows, err := a.getOpenWindows(partitionID, execCtx.Processor())
			if err != nil {
				return err
			}
			closedPos := 0
			for _, entry := range openWindows {
				lastDataInWindow := entry.we - 1
				if lastDataInWindow+a.lateness <= wm {
					closed, err := a.closeWindow(entry, partitionID, execCtx)
					if err != nil {
						return err
					}
					if !closed {
						// It's possible the window didn't get closed as the data wasn't found because it hasn't yet been
						// flushed from the processor write cache. This is ok, it will be flushed when this barrier completes
						// and the next barrier will close the window
						break
					}
					closedPos++
				} else {
					break
				}
			}
			if closedPos > 0 {
				// remove the closed windows
				openWindows = openWindows[closedPos:]
				a.openWindows[partitionID] = openWindows
			}
		}
	}
	return a.BaseOperator.HandleBarrier(execCtx)
}

func (a *AggregateOperator) closeWindow(entry windowEntry, partitionID int, execCtx StreamExecContext) (bool, error) {
	// We load the aggregation for the window and then send it as a batch to the receiver, where it will be picked
	// up and stored.
	// Note that `ws` must be the first column for us to be able to load the window efficiently

	partitionHash := a.hashCache.getHash(partitionID)
	keyStart := encoding.EncodeEntryPrefix(partitionHash, a.aggStateSlabID, 33)
	keyStart = append(keyStart, 1) // not null
	keyStart = encoding.KeyEncodeTimestamp(keyStart, types.NewTimestamp(entry.ws))
	keyEnd := common.IncrementBytesBigEndian(keyStart)
	iter, err := execCtx.Processor().NewIterator(keyStart, keyEnd, math.MaxUint64, false)
	if err != nil {
		return false, err
	}
	if iter == nil {
		log.Info("foo")
	}
	defer iter.Close()
	colBuilders := evbatch.CreateColBuilders(a.outSchema.EventSchema.ColumnTypes())
	hasData := false
	for {
		valid, err := iter.IsValid()
		if err != nil {
			return false, err
		}
		if !valid {
			break
		}
		curr := iter.Current()
		key := curr.Key
		if !a.includeWindowCols {
			key = curr.Key[18:] // first part of key is ws, we, so we truncate that part
		}
		if err := LoadColsFromKey(colBuilders, a.outKeyColTypes, a.outKeyColIndexes, key); err != nil {
			return false, err
		}
		LoadColsFromValue(colBuilders, a.outAggColTypes, a.outAggColIndexes, curr.Value)
		if err := iter.Next(); err != nil {
			return false, err
		}
		hasData = true
	}
	if !hasData {
		return false, nil
	}
	batch := evbatch.NewBatchFromBuilders(a.outSchema.EventSchema, colBuilders...)
	pb := proc.NewProcessBatch(execCtx.Processor().ID(), batch, a.closedWindowReceiverID, partitionID, -1)
	pb.Version = execCtx.WriteVersion()
	pb.EvBatchBytes = keyStart
	execCtx.Processor().IngestBatch(pb, func(err error) {
		if err != nil {
			log.Errorf("failed to ingest closed window batch: %v", err)
		}
	})
	return true, nil
}

func (a *AggregateOperator) createCols(batch *evbatch.Batch) ([]evbatch.Column, error) {
	cols := make([]evbatch.Column, len(a.aggStateSchema.ColumnTypes()))
	cols[0] = batch.GetTimestampColumn(a.processingEventTimeColIndex) // event-time col
	// First evaluate the key col expressions
	for _, keyColHolder := range a.keyColHolders {
		col, err := expr.EvalColumn(keyColHolder.expr, batch)
		if err != nil {
			return nil, err
		}
		cols[keyColHolder.colIndex] = col
	}
	// Then evaluate the inner expressions of the aggregate columns
	for _, aggHolder := range a.aggFuncHolders {
		col, err := expr.EvalColumn(aggHolder.innerExpr, batch)
		if err != nil {
			return nil, err
		}
		cols[aggHolder.colIndex] = col
	}
	return cols, nil
}

func (a *AggregateOperator) groupData(cols []evbatch.Column, batch *evbatch.Batch) map[string][]any {
	// Now for each agg func, we first group all the values by the key cols
	var keyCache []string
	if len(a.aggFuncHolders) > 1 {
		// As we will be iterating over the cols for each agg function, we cache the key, so we don't calculate it
		// each time
		keyCache = make([]string, batch.RowCount)
	}
	grouped := map[string][]any{}
	for i, aggHolder := range a.aggFuncHolders {
		a.groupDataForAggFunc(cols, batch.RowCount, keyCache, i, aggHolder.colIndex, grouped,
			aggHolder.innerExpr.ResultType().ID())
	}
	return grouped
}

func (a *AggregateOperator) groupDataForAggFunc(cols []evbatch.Column, rc int, keyCache []string, aggIndex int, aggColIndex int,
	grouped map[string][]any, ftID types.ColumnTypeID) {
	for row := 0; row < rc; row++ {
		var sKey string
		if keyCache != nil {
			sKey = keyCache[row]
		}
		if sKey == "" {
			key := a.createKey(cols, row)
			sKey = common.ByteSliceToStringZeroCopy(key)
			if keyCache != nil {
				keyCache[row] = sKey
			}
		}
		gArr := grouped[sKey]
		if gArr == nil {
			gArr = make([]any, len(a.aggFuncHolders))
			grouped[sKey] = gArr
		}
		col := cols[aggColIndex]
		if col.IsNull(row) {
			continue
		}
		vals := gArr[aggIndex]
		switch ftID {
		case types.ColumnTypeIDInt:
			vals = groupIntData(col, row, vals)
		case types.ColumnTypeIDFloat:
			vals = groupFloatData(col, row, vals)
		case types.ColumnTypeIDBool:
			vals = groupBoolData(col, row, vals)
		case types.ColumnTypeIDDecimal:
			vals = groupDecimalData(col, row, vals)
		case types.ColumnTypeIDString:
			vals = groupStringData(col, row, vals)
		case types.ColumnTypeIDBytes:
			vals = groupBytesData(col, row, vals)
		case types.ColumnTypeIDTimestamp:
			vals = groupTimestampData(col, row, vals)
		default:
			panic("unknown type")
		}
		gArr[aggIndex] = vals
	}
}

func groupIntData(col evbatch.Column, row int, vals any) any {
	val := col.(*evbatch.IntColumn).Get(row)
	var intVals []int64
	if vals != nil {
		intVals = vals.([]int64)
	} else {
		intVals = make([]int64, 0, 8)
	}
	return append(intVals, val)
}

func groupFloatData(col evbatch.Column, row int, vals any) any {
	val := col.(*evbatch.FloatColumn).Get(row)
	var floatVals []float64
	if vals != nil {
		floatVals = vals.([]float64)
	} else {
		floatVals = make([]float64, 0, 8)
	}
	return append(floatVals, val)
}

func groupBoolData(col evbatch.Column, row int, vals any) any {
	val := col.(*evbatch.BoolColumn).Get(row)
	var boolVals []bool
	if vals != nil {
		boolVals = vals.([]bool)
	} else {
		boolVals = make([]bool, 0, 8)
	}
	return append(boolVals, val)
}

func groupDecimalData(col evbatch.Column, row int, vals any) any {
	val := col.(*evbatch.DecimalColumn).Get(row)
	var decimalVals []types.Decimal
	if vals != nil {
		decimalVals = vals.([]types.Decimal)
	} else {
		decimalVals = make([]types.Decimal, 0, 8)
	}
	return append(decimalVals, val)
}

func groupStringData(col evbatch.Column, row int, vals any) any {
	val := col.(*evbatch.StringColumn).Get(row)
	var stringVals []string
	if vals != nil {
		stringVals = vals.([]string)
	} else {
		stringVals = make([]string, 0, 8)
	}
	return append(stringVals, val)
}

func groupBytesData(col evbatch.Column, row int, vals any) any {
	val := col.(*evbatch.BytesColumn).Get(row)
	var bytesVals [][]byte
	if vals != nil {
		bytesVals = vals.([][]byte)
	} else {
		bytesVals = make([][]byte, 0, 8)
	}
	return append(bytesVals, val)
}

func groupTimestampData(col evbatch.Column, row int, vals any) any {
	val := col.(*evbatch.TimestampColumn).Get(row)
	var timestampVals []types.Timestamp
	if vals != nil {
		timestampVals = vals.([]types.Timestamp)
	} else {
		timestampVals = make([]types.Timestamp, 0, 8)
	}
	return append(timestampVals, val)
}

func (a *AggregateOperator) computeAggs(grouped map[string][]any, execCtx StreamExecContext) ([]common.KV, error) {
	var writtenEntries []common.KV
	numAggs := len(a.aggColTypes)
	for key, groupedArr := range grouped {
		rowBytes := make([]byte, 0, 64)
		partitionHash := a.hashCache.getHash(execCtx.PartitionID())
		storeKey := encoding.EncodeEntryPrefix(partitionHash, a.aggStateSlabID, 24+len(key))
		storeKey = append(storeKey, common.StringToByteSliceZeroCopy(key)...)
		state, err := a.maybeLoadState(storeKey, execCtx)
		if err != nil {
			return nil, err
		}
		if state == nil {
			state = &aggState{
				data: make([]any, numAggs),
			}
			if a.hasExtraStateAggs {
				state.extraData = make([][]byte, numAggs)
			}
		}
		for i, v := range groupedArr {
			aggHolder := a.aggFuncHolders[i]
			prev := state.data[i]
			var extra []byte
			if a.hasExtraStateAggs {
				extra = state.extraData[i]
			}
			var res any
			var extraRes []byte
			switch aggHolder.innerExpr.ResultType().ID() {
			case types.ColumnTypeIDInt:
				if v == nil {
					res, extraRes, err = aggHolder.aggFunc.ComputeInt(prev, extra, nil)
				} else {
					res, extraRes, err = aggHolder.aggFunc.ComputeInt(prev, extra, v.([]int64))
				}
			case types.ColumnTypeIDFloat:
				if v == nil {
					res, extraRes, err = aggHolder.aggFunc.ComputeFloat(prev, extra, nil)
				} else {
					res, extraRes, err = aggHolder.aggFunc.ComputeFloat(prev, extra, v.([]float64))
				}
			case types.ColumnTypeIDBool:
				if v == nil {
					res, extraRes, err = aggHolder.aggFunc.ComputeBool(prev, extra, nil)
				} else {
					res, extraRes, err = aggHolder.aggFunc.ComputeBool(prev, extra, v.([]bool))
				}
			case types.ColumnTypeIDDecimal:
				if v == nil {
					res, extraRes, err = aggHolder.aggFunc.ComputeDecimal(prev, extra, nil)
				} else {
					res, extraRes, err = aggHolder.aggFunc.ComputeDecimal(prev, extra, v.([]types.Decimal))
				}
			case types.ColumnTypeIDString:
				if v == nil {
					res, extraRes, err = aggHolder.aggFunc.ComputeString(prev, extra, nil)
				} else {
					res, extraRes, err = aggHolder.aggFunc.ComputeString(prev, extra, v.([]string))
				}
			case types.ColumnTypeIDBytes:
				if v == nil {
					res, extraRes, err = aggHolder.aggFunc.ComputeBytes(prev, extra, nil)
				} else {
					res, extraRes, err = aggHolder.aggFunc.ComputeBytes(prev, extra, v.([][]byte))
				}
			case types.ColumnTypeIDTimestamp:
				if v == nil {
					res, extraRes, err = aggHolder.aggFunc.ComputeTimestamp(prev, extra, nil)
				} else {
					res, extraRes, err = aggHolder.aggFunc.ComputeTimestamp(prev, extra, v.([]types.Timestamp))
				}
			default:
				panic("unknown type")
			}
			if err != nil {
				return nil, err
			}
			state.data[i] = res
			if a.hasExtraStateAggs {
				state.extraData[i] = extraRes
			}
		}
		for i, res := range state.data {
			rowBytes = encodeAggResult(a.aggColTypes[i], rowBytes, res)
		}
		if a.hasExtraStateAggs {
			for _, index := range a.extraStateAggs {
				extra := state.extraData[index]
				rowBytes = encoding.AppendUint32ToBufferLE(rowBytes, uint32(len(extra)))
				rowBytes = append(rowBytes, extra...)
			}
		}
		storeKey = encoding.EncodeVersion(storeKey, uint64(execCtx.WriteVersion()))
		kv := common.KV{
			Key:   storeKey,
			Value: rowBytes,
		}
		if !a.windowed {
			writtenEntries = append(writtenEntries, kv)
		}
		execCtx.StoreEntry(kv, false)
	}
	return writtenEntries, nil
}

func encodeAggResult(aggColType types.ColumnType, rowBytes []byte, res any) []byte {
	rowBytes = append(rowBytes, 1) // Not null
	switch aggColType.ID() {
	case types.ColumnTypeIDInt:
		rowBytes = encoding.AppendUint64ToBufferLE(rowBytes, uint64(res.(int64)))
	case types.ColumnTypeIDFloat:
		rowBytes = encoding.AppendFloat64ToBufferLE(rowBytes, res.(float64))
	case types.ColumnTypeIDBool:
		rowBytes = encoding.AppendBoolToBuffer(rowBytes, res.(bool))
	case types.ColumnTypeIDDecimal:
		rowBytes = encoding.AppendDecimalToBuffer(rowBytes, res.(types.Decimal))
	case types.ColumnTypeIDString:
		rowBytes = encoding.AppendStringToBufferLE(rowBytes, res.(string))
	case types.ColumnTypeIDBytes:
		rowBytes = encoding.AppendBytesToBufferLE(rowBytes, res.([]byte))
	case types.ColumnTypeIDTimestamp:
		rowBytes = encoding.AppendUint64ToBufferLE(rowBytes, uint64(res.(types.Timestamp).Val))
	default:
		panic("unknown type")
	}
	return rowBytes
}

func (a *AggregateOperator) maybeLoadState(key []byte, execCtx StreamExecContext) (*aggState, error) {
	v, err := execCtx.Get(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	data, offset := encoding.DecodeRowToSlice(v, 0, a.aggColTypes)
	var extraData [][]byte
	if a.hasExtraStateAggs {
		extraData = make([][]byte, len(a.aggFuncHolders))
		for _, index := range a.extraStateAggs {
			var el uint32
			el, offset = encoding.ReadUint32FromBufferLE(v, offset)
			extra := v[offset : offset+int(el)]
			extraData[index] = common.CopyByteSlice(extra)
			offset += int(el)
		}
	}
	return &aggState{
		data:      data,
		extraData: extraData,
	}, nil
}

func (a *AggregateOperator) createKey(cols []evbatch.Column, row int) []byte {
	keyBuff := make([]byte, 0, 32)
	for i, index := range a.keyColIndexes {
		keyBuff = evbatch.EncodeKeyCol(row, cols[index], a.keyColTypes[i], keyBuff)
	}
	return keyBuff
}

func (a *AggregateOperator) InSchema() *OperatorSchema {
	return a.inSchema
}

func (a *AggregateOperator) OutSchema() *OperatorSchema {
	return a.outSchema
}

func (a *AggregateOperator) Setup(mgr StreamManagerCtx) error {
	mgr.RegisterReceiver(a.closedWindowReceiverID, a)
	return nil
}

func (a *AggregateOperator) Teardown(mgr StreamManagerCtx, _ *sync.RWMutex) {
	mgr.UnregisterReceiver(a.closedWindowReceiverID)
}

func (a *AggregateOperator) ReceiveBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	partitionHash := a.hashCache.getHash(execCtx.PartitionID())
	if a.storeResults {
		// store the batch
		prefix := encoding.EncodeEntryPrefix(partitionHash, a.resultsSlabID, 64)
		storeBatchInTable(batch, a.outKeyColIndexes, a.outAggColIndexes, prefix, execCtx, -1, false)
	}
	keyPrefix := execCtx.EventBatchBytes()
	ws, _ := encoding.KeyDecodeInt(keyPrefix, 25)
	// delete the open window from storage
	key := encoding.EncodeEntryPrefix(partitionHash, a.openWindowsSlabID, 40)
	key = encoding.KeyEncodeTimestamp(key, types.NewTimestamp(ws))
	key = encoding.EncodeVersion(key, uint64(execCtx.WriteVersion()))
	execCtx.StoreEntry(common.KV{
		Key: key,
	}, false)
	// create a tombstone and endmarker to delete the data from the window
	endMarker := append(common.IncrementBytesBigEndian(keyPrefix), 0)
	execCtx.StoreEntry(common.KV{
		Key: keyPrefix,
	}, false)
	execCtx.StoreEntry(common.KV{
		Key:   endMarker,
		Value: []byte{'x'},
	}, false)
	return nil, a.sendBatchDownStream(batch, execCtx)
}

func (a *AggregateOperator) ReceiveBarrier(execCtx StreamExecContext) error {
	return a.BaseOperator.HandleBarrier(execCtx)
}

func (a *AggregateOperator) ForwardingProcessorCount() int {
	return len(a.processSchema.PartitionScheme.ProcessorIDs)
}

func (a *AggregateOperator) RequiresBarriersInjection() bool {
	return true
}
