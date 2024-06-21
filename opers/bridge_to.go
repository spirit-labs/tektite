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
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/kafka"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"sync"
	"time"
)

func NewBridgeToOperator(desc *parser.BridgeToDesc, storeStreamOperator *StoreStreamOperator,
	backFillOperator *BackfillOperator, clientFactory kafka.ClientFactory) (*BridgeToOperator, error) {
	msgClient, err := clientFactory(desc.TopicName, desc.Props)
	if err != nil {
		return nil, err
	}
	schema := storeStreamOperator.OutSchema()
	maxPartition := 0
	for partId := range schema.PartitionProcessorMapping {
		if partId > maxPartition {
			maxPartition = partId
		}
	}
	offsetsToCommit := make([]int64, maxPartition+1)
	for i := 0; i < len(offsetsToCommit); i++ {
		offsetsToCommit[i] = -1
	}
	var initialRetryDelay time.Duration
	if desc.InitialRetryDelay != nil {
		initialRetryDelay = *desc.InitialRetryDelay
	} else {
		initialRetryDelay = defaultInitialRetryDelay
	}
	if initialRetryDelay < 1*time.Millisecond {
		return nil, statementErrorAtTokenNamef("initial_retry_delay", desc, "'initial_retry_delay' must be >= 1ms")
	}
	var maxRetryDelay time.Duration
	if desc.MaxRetryDelay != nil {
		maxRetryDelay = *desc.MaxRetryDelay
	} else {
		maxRetryDelay = defaultMaxRetryDelay
	}
	if maxRetryDelay < initialRetryDelay {
		return nil, statementErrorAtTokenNamef("max_retry_delay", desc, "'max_retry_delay' must be >= 'initial_retry_delay'")
	}
	var connectTimeout time.Duration
	if desc.ConnectTimeout != nil {
		connectTimeout = *desc.ConnectTimeout
	} else {
		connectTimeout = defaultConnectTimeout
	}
	var sendTimeout time.Duration
	if desc.SendTimeout != nil {
		sendTimeout = *desc.SendTimeout
	} else {
		sendTimeout = defaultSendTimeout
	}
	producers := make([]kafka.MessageProducer, maxPartition+1)
	for partId := range schema.PartitionProcessorMapping {
		producers[partId], err = msgClient.NewMessageProducer(partId, connectTimeout, sendTimeout)
		if err != nil {
			return nil, err
		}
	}
	maxProcessorID := 0
	for procID := range schema.ProcessorPartitionMapping {
		if procID > maxProcessorID {
			maxProcessorID = procID
		}
	}
	bto := &BridgeToOperator{
		desc:                desc,
		schema:              schema,
		storeStreamOperator: storeStreamOperator,
		backFillOperator:    backFillOperator,
		offsetsToCommit:     offsetsToCommit,
		storeMode:           make([]bool, maxProcessorID+1),
		producers:           producers,
		msgClient:           msgClient,
		initialRetryDelay:   initialRetryDelay,
		maxRetryDelay:       maxRetryDelay,
		lastRetryDuration:   make([]time.Duration, maxProcessorID+1),
	}
	backFillOperator.AddDownStreamOperator(&backfillSink{b: bto})
	return bto, nil
}

const (
	defaultInitialRetryDelay = 5 * time.Second
	defaultMaxRetryDelay     = 30 * time.Second
	retryBackoffFactor       = float64(1.25)
	defaultConnectTimeout    = 5 * time.Second
	defaultSendTimeout       = 2 * time.Second
)

type BridgeToOperator struct {
	BaseOperator
	desc                *parser.BridgeToDesc
	schema              *OperatorSchema
	slabID              int
	storeStreamOperator *StoreStreamOperator
	backFillOperator    *BackfillOperator
	offsetsToCommit     []int64
	msgClient           kafka.MessageClient
	storeMode           []bool
	producers           []kafka.MessageProducer
	timers              sync.Map
	lastRetryDuration   []time.Duration
	initialRetryDelay   time.Duration
	maxRetryDelay       time.Duration
}

func (b *BridgeToOperator) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	if b.storeMode[execCtx.Processor().ID()] || !b.backFillOperator.IsLive(execCtx.PartitionID()) {
		// We are either in store mode as previous send/connect to kafka timed out - or the backfill is not initialised
		// yet or still loading after startup - we therefore just store the batch, and it will be loaded either by backfill
		// or when the operator exits store mode on a timer
		_, err := b.storeStreamOperator.HandleStreamBatch(batch, execCtx)
		return nil, err
	}
	return b.backFillOperator.HandleStreamBatch(batch, execCtx)
}

func (b *BridgeToOperator) enterStoreMode(execCtx StreamExecContext, batch *evbatch.Batch) error {
	execCtx.Processor().CheckInProcessorLoop()
	processor := execCtx.Processor()
	processorID := processor.ID()
	log.Debugf("bridge to entering store mode for processor %d", processorID)
	processor.CheckInProcessorLoop()
	// store the batch
	if _, err := b.storeStreamOperator.HandleStreamBatch(batch, execCtx); err != nil {
		return err
	}
	b.storeMode[processor.ID()] = true
	partIDs, ok := b.schema.ProcessorPartitionMapping[processorID]
	if !ok {
		panic("cannot find partitions for processor")
	}
	b.flushLastCommitted(execCtx)
	for _, partID := range partIDs {
		b.backFillOperator.pauseBackfill(partID)
	}
	// We set a timer to exit store mode and reload after a timeout
	delay := b.getRetryDelay(processorID)
	tz := common.ScheduleTimer(delay, true, func() {
		b.exitStoreMode(processor, partIDs)
	})
	b.timers.Store(processorID, tz)
	return nil
}

func (b *BridgeToOperator) getRetryDelay(processorID int) time.Duration {
	lastDelay := b.lastRetryDuration[processorID]
	var delay time.Duration
	if lastDelay == 0 {
		delay = b.initialRetryDelay
	} else if lastDelay == b.maxRetryDelay {
		delay = lastDelay
	} else {
		// retry delay backs off by retry factor each time, up to a maximum
		delay = time.Duration(float64(lastDelay) * retryBackoffFactor)
		if delay > b.maxRetryDelay {
			delay = b.maxRetryDelay
		}
	}
	b.lastRetryDuration[processorID] = delay
	log.Debugf("bridge to retrying after delay of %d ms", delay.Milliseconds())
	return delay
}

func (b *BridgeToOperator) exitStoreMode(processor proc.Processor, partIDs []int) {
	processor.SubmitAction(func() error {
		processor.CheckInProcessorLoop()
		procID := processor.ID()
		// Note that we exit store mode on a processor action, this ensures any batches already queued on the processor
		// when we went into store mode get processed, and ignored, before this.
		b.timers.Delete(procID)
		b.storeMode[procID] = false
		b.lastRetryDuration[procID] = 0 // reset retry delay
		// We must flush write cache before loading as version might not have completed and data could still be in cache
		if err := processor.WriteCache().MaybeWriteToStore(); err != nil {
			return err
		}
		for _, partID := range partIDs {
			log.Debugf("bridge to exiting store mode for partition %d - reloading", partID)
			// Tell the backfill operator to start back-filling from last committed offset.
			if err := b.backFillOperator.restartBackfill(partID, processor); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BridgeToOperator) HandleBarrier(execCtx StreamExecContext) error {
	// We store offsets on receipt of barrier
	execCtx.Processor().CheckInProcessorLoop()
	b.flushLastCommitted(execCtx)
	return b.BaseOperator.HandleBarrier(execCtx)
}

func (b *BridgeToOperator) flushLastCommitted(execCtx StreamExecContext) {
	partitionIDs := b.schema.ProcessorPartitionMapping[execCtx.Processor().ID()]
	for _, partitionID := range partitionIDs {
		offsetToCommit := b.offsetsToCommit[partitionID]
		if offsetToCommit != -1 {
			b.backFillOperator.storeCommittedOffSetForPartition(offsetToCommit, execCtx)
			b.offsetsToCommit[partitionID] = -1
		}
	}
}

func (b *BridgeToOperator) InSchema() *OperatorSchema {
	return b.schema
}

func (b *BridgeToOperator) OutSchema() *OperatorSchema {
	return b.schema
}

func (b *BridgeToOperator) Setup(mgr StreamManagerCtx) error {
	for _, producer := range b.producers {
		if producer != nil {
			if err := producer.Start(); err != nil {
				return err
			}
		}
	}
	return b.backFillOperator.Setup(mgr)
}

func (b *BridgeToOperator) Teardown(mgr StreamManagerCtx, _ *sync.RWMutex) {
	b.timers.Range(func(_, value any) bool {
		value.(*common.TimerHandle).Stop()
		return true
	})
	b.backFillOperator.Teardown(mgr, nil)
	for _, producer := range b.producers {
		if producer != nil {
			if err := producer.Stop(); err != nil {
				log.Warnf("failed to stop producer: %v", err)
			}
		}
	}
}

func (b *BridgeToOperator) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not supported")
}

func (b *BridgeToOperator) sendBatch(batch *evbatch.Batch, execCtx StreamExecContext) error {
	partitionID := execCtx.PartitionID()
	if b.storeMode[execCtx.Processor().ID()] {
		// Ignore. If batches are already queued when we go into store mode then when they get processed we ignore them.
		return nil
	}
	producer := b.producers[partitionID]
	if err := producer.SendBatch(batch); err != nil {
		// failed to send batch. we will go into "store mode" which means we won't attempt to
		// send messages, we will store them. after a delay we will exit "store mode" and attempt delivery again.
		if err := b.enterStoreMode(execCtx, batch); err != nil {
			return err
		}
		log.Warnf("'bridge to' operator failed to send to topic %s. Will backoff and retry send after delay - error: %v",
			b.desc.TopicName, err)
		return nil
	}
	// Successfully sent batch
	lastOffset := batch.GetIntColumn(0).Get(batch.RowCount - 1)
	b.offsetsToCommit[partitionID] = lastOffset
	log.Debugf("bridge to, message batch delivered ok on partition %d", partitionID)
	return nil
}

type backfillSink struct {
	BaseOperator
	b *BridgeToOperator
}

func (s *backfillSink) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	return nil, s.b.sendBatch(batch, execCtx)
}

func (s *backfillSink) HandleBarrier(execCtx StreamExecContext) error {
	return s.b.HandleBarrier(execCtx)
}

func (s *backfillSink) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not supported")
}

func (s *backfillSink) InSchema() *OperatorSchema {
	return nil
}

func (s *backfillSink) OutSchema() *OperatorSchema {
	return nil
}

func (s *backfillSink) Setup(StreamManagerCtx) error {
	return nil
}

func (s *backfillSink) Teardown(StreamManagerCtx, *sync.RWMutex) {
}
