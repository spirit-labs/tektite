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
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/types"
	"sync"
)

func NewKafkaOutOperator(ts *StoreStreamOperator, slabID int, offsetsSlabID int, schema *OperatorSchema, store store,
	storeOffset bool) (*KafkaOutOperator, error) {
	return &KafkaOutOperator{
		slabID:              slabID,
		offsetsSlabID:       offsetsSlabID,
		storeOffset:         storeOffset,
		schema:              schema,
		store:               store,
		offsets:             make([]partitionOffsets, schema.PartitionScheme.Partitions),
		storeStreamOperator: ts,
		hashCache:           newPartitionHashCache(schema.MappingID, schema.Partitions),
	}, nil
}

type KafkaOutOperator struct {
	BaseOperator
	offsets             []partitionOffsets
	slabID              int
	offsetsSlabID       int
	storeOffset         bool
	schema              *OperatorSchema
	store               store
	storeStreamOperator *StoreStreamOperator
	hashCache           *partitionHashCache
}

type partitionOffsets struct {
	lock           sync.Mutex
	loaded         bool
	firstOffset    int64
	firstTimestamp int64
	lastOffset     int64
	lastTimestamp  int64
}

func (k *KafkaOutOperator) GetPartitionProcessorMapping() map[int]int {
	return k.schema.PartitionScheme.PartitionProcessorMapping
}

func (k *KafkaOutOperator) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {

	// First we send it to the store operator - which will add the offset column if not already there.
	batch, err := k.storeStreamOperator.HandleStreamBatch(batch, execCtx)
	if err != nil {
		return nil, err
	}

	lr := batch.RowCount - 1
	offset := batch.GetIntColumn(0).Get(lr)
	ts := batch.GetTimestampColumn(1).Get(lr)
	lastTimestamp := ts.Val
	off := &k.offsets[execCtx.PartitionID()]
	off.lock.Lock()
	defer off.lock.Unlock()
	off.lastOffset = offset + 1
	off.lastTimestamp = lastTimestamp
	off.loaded = true
	if k.storeOffset {
		partitionHash := k.hashCache.getHash(execCtx.PartitionID())
		storeOffset(execCtx, off.lastOffset, partitionHash, k.offsetsSlabID, execCtx.WriteVersion())
	}
	return nil, k.sendBatchDownStream(batch, execCtx)
}

func (k *KafkaOutOperator) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not supported for stream")
}

func (k *KafkaOutOperator) HandleBarrier(execCtx StreamExecContext) error {
	return k.BaseOperator.HandleBarrier(execCtx)
}

func (k *KafkaOutOperator) InSchema() *OperatorSchema {
	return k.schema
}

func (k *KafkaOutOperator) OutSchema() *OperatorSchema {
	return k.schema
}

func (k *KafkaOutOperator) Setup(StreamManagerCtx) error {
	return nil
}

func (k *KafkaOutOperator) Teardown(StreamManagerCtx, *sync.RWMutex) {
}

func (k *KafkaOutOperator) SlabID() int {
	return k.slabID
}

func (k *KafkaOutOperator) PartitionMapping() string {
	return k.schema.MappingID
}

func (k *KafkaOutOperator) EarliestOffset(int) (int64, int64, bool) {
	// currently always zero
	return 0, 0, true
}

// LatestOffset - note that this returns 1 + the msg with the highest offset in the partition
func (k *KafkaOutOperator) LatestOffset(partitionID int) (int64, int64, bool, error) {
	if partitionID < 0 || partitionID >= len(k.offsets) {
		return 0, 0, false, nil
	}
	off := &k.offsets[partitionID]
	off.lock.Lock()
	defer off.lock.Unlock()
	if off.loaded {
		return off.lastOffset, off.lastTimestamp, true, nil
	}
	// load from store
	partitionHash := k.hashCache.getHash(partitionID)
	offset, err := loadOffset(partitionHash, k.offsetsSlabID, k.store)
	if err != nil {
		return 0, 0, false, err
	}
	off.loaded = true
	off.lastOffset = offset
	// Timestamp?
	return off.lastOffset, 0, true, nil
}

func (k *KafkaOutOperator) OffsetByTimestamp(types.Timestamp, int) (int64, int64, bool) {
	return -1, -1, false
}
