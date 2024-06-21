// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opers

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/proc"
	"sync"
)

type StoreTableOperator struct {
	BaseOperator
	inSchema   *OperatorSchema
	outSchema  *OperatorSchema
	inKeyCols  []int
	outKeyCols []int
	rowCols    []int
	outRowCols []int
	keyPrecfix []byte
	store      store
	nodeID     int
	noCache    bool
	hasKey     bool
	slabID     uint64
	hasOffset  bool
	hashCache  *partitionHashCache
}

func NewStoreTableOperator(schema *OperatorSchema, slabID int, store store, keyCols []string, nodeID int, noCache bool,
	desc errMsgAtPositionProvider) (*StoreTableOperator, error) {
	var inKeyCols []int
	var outKeyCols []int
	var rowCols []int
	var outRowCols []int
	colMap := createInColIndexMap(schema.EventSchema)
	hasOffset := HasOffsetColumn(schema.EventSchema)
	for _, keyCol := range keyCols {
		index, ok := colMap[keyCol]
		if !ok {
			return nil, statementErrorAtTokenNamef("", desc, "cannot use key column '%s' - it is not a known column in the incoming schema",
				keyCol)
		}
		inKeyCols = append(inKeyCols, index)
		if hasOffset {
			// no offset in output
			outKeyCols = append(outKeyCols, index-1)
		} else {
			outKeyCols = append(outKeyCols, index)
		}
	}
	keyColSet := make(map[string]struct{}, len(keyCols))
	for _, keyCol := range keyCols {
		keyColSet[keyCol] = struct{}{}
	}
	for i, colName := range schema.EventSchema.ColumnNames() {
		_, ok := keyColSet[colName]
		if !ok {
			if colName != OffsetColName {
				// Note, we do not store the offset column in a table
				rowCols = append(rowCols, i)
				if hasOffset {
					outRowCols = append(outRowCols, i-1)
				} else {
					outRowCols = append(outRowCols, i)
				}
			}
		}
	}
	var outSchema *OperatorSchema
	if hasOffset {
		// Remove the offset column - we do not store this in the table
		outEvSchema := evbatch.NewEventSchema(schema.EventSchema.ColumnNames()[1:], schema.EventSchema.ColumnTypes()[1:])
		outSchema = schema.Copy()
		outSchema.EventSchema = outEvSchema
	} else {
		outSchema = schema
	}
	return &StoreTableOperator{
		inSchema:   schema,
		outSchema:  outSchema,
		store:      store,
		inKeyCols:  inKeyCols,
		outKeyCols: outKeyCols,
		rowCols:    rowCols,
		outRowCols: outRowCols,
		nodeID:     nodeID,
		noCache:    noCache,
		hasKey:     len(keyCols) > 0,
		slabID:     uint64(slabID),
		hasOffset:  hasOffset,
		hashCache:  newPartitionHashCache(schema.MappingID, schema.Partitions),
	}, nil
}

func (s *StoreTableOperator) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not supported in queries")
}

func (s *StoreTableOperator) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	s.storeBatchInTable(batch, execCtx)
	if s.hasOffset {
		// remove offset col
		schema := batch.Schema
		batch = &evbatch.Batch{
			Schema:   evbatch.NewEventSchema(schema.ColumnNames()[1:], schema.ColumnTypes()[1:]),
			Columns:  batch.Columns[1:],
			RowCount: batch.RowCount,
		}
	}
	return batch, s.sendBatchDownStream(batch, execCtx)
}

func (s *StoreTableOperator) storeBatchInTable(batch *evbatch.Batch, execCtx StreamExecContext) {
	if s.hasKey {
		prefix := s.createTableKeyPrefix(s.slabID, execCtx.PartitionID(), 64)
		storeBatchInTable(batch, s.inKeyCols, s.rowCols, prefix, execCtx, s.nodeID, s.noCache)
	} else {
		// No key cols, so we store the row with a constant key - we just use the table/partition here
		key := s.createTableKeyPrefix(s.slabID, execCtx.PartitionID(), 32)
		key = encoding.EncodeVersion(key, uint64(execCtx.WriteVersion()))
		// They will all overwrite, so just take the last one
		row := make([]byte, 0, rowInitialBufferSize)
		row = evbatch.EncodeRowCols(batch, batch.RowCount-1, s.rowCols, row)
		execCtx.StoreEntry(common.KV{
			Key:   key,
			Value: row,
		}, s.noCache)
	}
}

func (s *StoreTableOperator) createTableKeyPrefix(slabID uint64, partID int, cap int) []byte {
	partitionHash := s.hashCache.getHash(partID)
	return encoding.EncodeEntryPrefix(partitionHash, slabID, cap)
}

func (s *StoreTableOperator) InSchema() *OperatorSchema {
	return s.inSchema
}

func (s *StoreTableOperator) OutSchema() *OperatorSchema {
	return s.outSchema
}

func (s *StoreTableOperator) Setup(StreamManagerCtx) error {
	return nil
}

func (s *StoreTableOperator) Teardown(StreamManagerCtx, *sync.RWMutex) {
}

func createTableKeyPrefix(mappingID string, slabID uint64, partID uint64, cap int) []byte {
	partitionHash := proc.CalcPartitionHash(mappingID, partID)
	return encoding.EncodeEntryPrefix(partitionHash, slabID, cap)
}
