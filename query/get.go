package query

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/iteration"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"math"
	"sync"
)

type GetOperator struct {
	opers.BaseOperator
	schema                 *opers.OperatorSchema
	isRange                bool
	rangeStartExprs        []expr.Expression
	rangeEndExprs          []expr.Expression
	startInclusive         bool
	endInclusive           bool
	keyColIndexes          []int
	rowColIndexes          []int
	keyColumnTypes         []types.ColumnType
	rowColumnTypes         []types.ColumnType
	slabID                 int
	emptyBatch             *evbatch.Batch
	nodeID                 int
	keySchema              *evbatch.EventSchema
	streamMetaIterProvider iteratorProvider
}

type KeyColExpr interface {
	Eval(args map[string]any) (any, error)
}

func NewGetOperator(isRange bool, rangeStartExprs []expr.Expression, rangeEndExprs []expr.Expression, startInclusive bool, endInclusive bool,
	slabID int, keyColIndexes []int, tableSchema *opers.OperatorSchema, nodeID int, streamMetaIterProvider iteratorProvider) *GetOperator {

	keyColsSet := map[int]struct{}{}
	var keyColumnTypes []types.ColumnType
	var keyColNames []string
	for _, colIndex := range keyColIndexes {
		keyColumnTypes = append(keyColumnTypes, tableSchema.EventSchema.ColumnTypes()[colIndex])
		keyColNames = append(keyColNames, tableSchema.EventSchema.ColumnNames()[colIndex])
		keyColsSet[colIndex] = struct{}{}
	}
	var rowColumnTypes []types.ColumnType
	var rowColIndexes []int
	for i, ft := range tableSchema.EventSchema.ColumnTypes() {
		_, keyCol := keyColsSet[i]
		if !keyCol {
			rowColIndexes = append(rowColIndexes, i)
			rowColumnTypes = append(rowColumnTypes, ft)
		}
	}
	keySchema := evbatch.NewEventSchema(keyColNames, keyColumnTypes)
	return &GetOperator{
		isRange:         isRange,
		rangeStartExprs: rangeStartExprs,
		rangeEndExprs:   rangeEndExprs,
		startInclusive:  startInclusive,
		endInclusive:    endInclusive,
		schema:          tableSchema,
		keyColIndexes:   keyColIndexes,
		rowColIndexes:   rowColIndexes,
		keyColumnTypes:  keyColumnTypes,
		rowColumnTypes:  rowColumnTypes,
		slabID:          slabID,
		emptyBatch:      evbatch.CreateEmptyBatch(tableSchema.EventSchema),
		nodeID:          nodeID,
		keySchema:       keySchema,
	}
}

func (g *GetOperator) CreateIterator(mappingID string, partID uint64, args *evbatch.Batch, highestVersion uint64,
	processor proc.Processor) (iteration.Iterator, error) {
	partitionHash := proc.CalcPartitionHash(mappingID, partID)
	var start, end []byte
	if !g.isRange {
		// get
		keyStart, err := g.CreateRangeStartKey(args)
		if err != nil {
			return nil, err
		}
		prefix := encoding.EncodeEntryPrefix(partitionHash, uint64(g.slabID), 24+len(keyStart))
		start = append(prefix, keyStart...)
		end = common.IncrementBytesBigEndian(start)
	} else if g.rangeStartExprs == nil && g.rangeEndExprs == nil {
		// scan all
		start = encoding.EncodeEntryPrefix(partitionHash, uint64(g.slabID), 24)
		end = common.IncrementBytesBigEndian(start)
	} else {
		// scan range
		if g.rangeStartExprs != nil {
			keyStart, err := g.CreateRangeStartKey(args)
			if err != nil {
				return nil, err
			}
			if !g.startInclusive {
				keyStart = common.IncrementBytesBigEndian(keyStart)
			}
			start = encoding.EncodeEntryPrefix(partitionHash, uint64(g.slabID), 24+len(keyStart))
			start = append(start, keyStart...)
		} else {
			start = encoding.EncodeEntryPrefix(partitionHash, uint64(g.slabID), 24)
		}
		if g.rangeEndExprs != nil {
			keyEnd, err := g.CreateRangeEndKey(args)
			if err != nil {
				return nil, err
			}
			if g.endInclusive {
				keyEnd = common.IncrementBytesBigEndian(keyEnd)
			}
			end = encoding.EncodeEntryPrefix(partitionHash, uint64(g.slabID), 24+len(keyEnd))
			end = append(end, keyEnd...)
		} else {
			end = encoding.EncodeEntryPrefix(partitionHash, uint64(g.slabID+1), 24)
		}
	}
	log.Debugf("node:%d creating query iterator start:%v end:%v with max version:%d", g.nodeID, start, end, highestVersion)
	if g.streamMetaIterProvider != nil {
		return g.streamMetaIterProvider.NewIterator(start, end, highestVersion, false)
	} else {
		return processor.NewIterator(start, end, highestVersion, false)
	}
}

func (g *GetOperator) CreateRangeStartKey(args *evbatch.Batch) ([]byte, error) {
	return g.createKey(g.rangeStartExprs, args)
}

func (g *GetOperator) CreateRangeEndKey(args *evbatch.Batch) ([]byte, error) {
	return g.createKey(g.rangeEndExprs, args)
}

func (g *GetOperator) createKey(exprs []expr.Expression, args *evbatch.Batch) ([]byte, error) {
	buff := make([]byte, 0, 32)
	for _, e := range exprs {
		switch e.ResultType().ID() {
		case types.ColumnTypeIDInt:
			val, null, err := e.EvalInt(0, args)
			if err != nil {
				return nil, err
			}
			if null {
				buff = append(buff, 0)
				continue
			}
			buff = append(buff, 1)
			buff = encoding.KeyEncodeInt(buff, val)
		case types.ColumnTypeIDFloat:
			val, null, err := e.EvalFloat(0, args)
			if err != nil {
				return nil, err
			}
			if null {
				buff = append(buff, 0)
				continue
			}
			buff = append(buff, 1)
			buff = encoding.KeyEncodeFloat(buff, val)
		case types.ColumnTypeIDBool:
			val, null, err := e.EvalBool(0, args)
			if err != nil {
				return nil, err
			}
			if null {
				buff = append(buff, 0)
				continue
			}
			buff = append(buff, 1)
			buff = encoding.AppendBoolToBuffer(buff, val)
		case types.ColumnTypeIDDecimal:
			val, null, err := e.EvalDecimal(0, args)
			if err != nil {
				return nil, err
			}
			if null {
				buff = append(buff, 0)
				continue
			}
			buff = append(buff, 1)
			buff = encoding.KeyEncodeDecimal(buff, val)
		case types.ColumnTypeIDString:
			val, null, err := e.EvalString(0, args)
			if err != nil {
				return nil, err
			}
			if null {
				buff = append(buff, 0)
				continue
			}
			buff = append(buff, 1)
			buff = encoding.KeyEncodeString(buff, val)
		case types.ColumnTypeIDBytes:
			val, null, err := e.EvalBytes(0, args)
			if err != nil {
				return nil, err
			}
			if null {
				buff = append(buff, 0)
				continue
			}
			buff = append(buff, 1)
			buff = encoding.KeyEncodeBytes(buff, val)
		case types.ColumnTypeIDTimestamp:
			val, null, err := e.EvalTimestamp(0, args)
			if err != nil {
				return nil, err
			}
			if null {
				buff = append(buff, 0)
				continue
			}
			buff = append(buff, 1)
			buff = encoding.KeyEncodeTimestamp(buff, val)
		default:
			panic("unknown type")
		}
	}
	return buff, nil
}

func (g *GetOperator) CreateRawPartitionKey(args *evbatch.Batch) ([]byte, error) {
	if len(g.rangeStartExprs) != 1 {
		return nil, errors.NewQueryErrorf("query on a raw partition key must have a single expression")
	}
	e := g.rangeStartExprs[0]
	if e.ResultType() != types.ColumnTypeBytes {
		return nil, errors.NewTektiteErrorf(errors.ExecuteQueryError,
			"invalid get expression type %s for lookup in raw partition - the get expression must return type bytes",
			e.ResultType().String())
	}
	// A raw partition is chosen by hashing the bytes of the Kafka message key
	val, null, err := e.EvalBytes(0, args)
	if err != nil {
		return nil, err
	}
	if null {
		panic("unexpected null")
	}
	return val, nil
}

func (g *GetOperator) LoadBatch(iter iteration.Iterator, maxRows int) (*evbatch.Batch, bool, error) {
	colBuilders := evbatch.CreateColBuilders(g.schema.EventSchema.ColumnTypes())
	rc := 0
	valid := false
	for {
		var err error
		valid, err = iter.IsValid()
		if err != nil {
			return nil, false, err
		}
		if rc == maxRows {
			break
		}
		if !valid {
			break
		}
		k := iter.Current().Key
		if log.DebugEnabled {
			version := math.MaxUint64 - binary.BigEndian.Uint64(k[len(k)-8:])
			v := iter.Current().Value
			log.Debugf("query loader loaded key %v (%s) value %v (%s) version %d", k, string(k), v, string(v), version)
		}
		if err := opers.LoadColsFromKey(colBuilders, g.keyColumnTypes, g.keyColIndexes, k); err != nil {
			return nil, false, err
		}
		opers.LoadColsFromValue(colBuilders, g.rowColumnTypes, g.rowColIndexes, iter.Current().Value)
		if err = iter.Next(); err != nil {
			return nil, false, err
		}
		rc++
	}
	if rc == 0 {
		return g.emptyBatch, false, nil
	}
	return evbatch.NewBatchFromBuilders(g.schema.EventSchema, colBuilders...), valid, nil
}

func (g *GetOperator) HandleStreamBatch(*evbatch.Batch, opers.StreamExecContext) (*evbatch.Batch, error) {
	panic("not supported in streams")
}

func (g *GetOperator) HandleBarrier(opers.StreamExecContext) error {
	panic("not supported in streams")
}

func (g *GetOperator) AddDownStreamOperator(downstream opers.Operator) {
	g.BaseOperator.AddDownStreamOperator(downstream)
}

func (g *GetOperator) HandleQueryBatch(batch *evbatch.Batch, execCtx opers.QueryExecContext) (*evbatch.Batch, error) {
	return nil, g.SendQueryBatchDownStream(batch, execCtx)
}

func (g *GetOperator) InSchema() *opers.OperatorSchema {
	return nil
}

func (g *GetOperator) OutSchema() *opers.OperatorSchema {
	return g.schema
}

func (g *GetOperator) Setup(opers.StreamManagerCtx) error {
	return nil
}

func (g *GetOperator) Teardown(opers.StreamManagerCtx, *sync.RWMutex) {
}

func (g *GetOperator) GetKeyColExprs() []expr.Expression {
	return g.rangeStartExprs
}
