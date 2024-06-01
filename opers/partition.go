package opers

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/evbatch"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parser"
	"sync"
)

const initialKeyBufferSize = 16

const constKeyColName = "const"

func NewPartitionOperator(inSchema *OperatorSchema, keyCols []string, partitions int, forwardReceiverID int,
	mappingID string, cfg *conf.Config, desc *parser.PartitionDesc) (*PartitionOperator, error) {

	partitionScheme := NewPartitionScheme(mappingID, partitions, false, *cfg.ProcessorCount)

	var outEventSchema *evbatch.EventSchema
	removeOffset := HasOffsetColumn(inSchema.EventSchema)
	if removeOffset {
		// We remove the offset column as it does not make sense after partitioning
		outEventSchema = evbatch.NewEventSchema(inSchema.EventSchema.ColumnNames()[1:], inSchema.EventSchema.ColumnTypes()[1:])
	} else {
		outEventSchema = inSchema.EventSchema
	}
	outSchema := &OperatorSchema{
		EventSchema:     outEventSchema,
		PartitionScheme: partitionScheme,
	}

	keyIndexes, err := getKeyColIndexes(keyCols, outSchema.EventSchema, desc)
	if err != nil {
		return nil, err
	}

	po := &PartitionOperator{
		keyIndexes:        keyIndexes,
		inSchema:          inSchema,
		outSchema:         outSchema,
		forwardReceiverID: forwardReceiverID,
		removeOffset:      removeOffset,
	}
	procCount := *cfg.ProcessorCount
	if *cfg.LevelManagerEnabled {
		procCount++
	}
	expectedSequences := make([]map[int]int, procCount)
	for i := 0; i < procCount; i++ {
		expectedSequences[i] = map[int]int{}
	}
	po.receiver = &partitionReceiver{
		po:                po,
		expectedSequences: expectedSequences,
	}
	return po, nil
}

type PartitionOperator struct {
	BaseOperator
	keyIndexes        []int
	inSchema          *OperatorSchema
	outSchema         *OperatorSchema
	forwardReceiverID int
	removeOffset      bool
	receiver          *partitionReceiver
}

func getKeyColIndexes(keyCols []string, inSchema *evbatch.EventSchema, desc *parser.PartitionDesc) ([]int, error) {
	var keyIndexes []int
	if len(keyCols) == 1 && keyCols[0] == constKeyColName {
		return keyIndexes, nil
	}
	colMap := createInColIndexMap(inSchema)
	for _, keyCol := range keyCols {
		index, ok := colMap[keyCol]
		if !ok {
			return nil, statementErrorAtTokenNamef("", desc, "cannot use key column '%s' in partition operator - it is not a known column in the incoming schema",
				keyCol)
		}
		keyIndexes = append(keyIndexes, index)
	}
	return keyIndexes, nil
}

func (po *PartitionOperator) GetPartitionProcessorMapping() map[int]int {
	return po.outSchema.PartitionScheme.PartitionProcessorMapping
}

func (po *PartitionOperator) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not supported in queries")
}

func (po *PartitionOperator) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	defer batch.Release()
	if po.removeOffset {
		// remove offset col
		batch = evbatch.NewBatch(po.outSchema.EventSchema, batch.Columns[1:]...)
	}
	pScheme := po.outSchema.PartitionScheme
	for i := 0; i < batch.RowCount; i++ {
		var partID uint32
		if po.keyIndexes == nil {
			// Group by const - always goes in same partition
			partID = 0
		} else {
			keyBuff := make([]byte, 0, initialKeyBufferSize)
			keyBuff = evbatch.EncodeKeyCols(batch, i, po.keyIndexes, keyBuff)
			hash := common.DefaultHash(keyBuff)
			partID = common.CalcPartition(hash, pScheme.Partitions)
		}
		processorID, ok := pScheme.PartitionProcessorMapping[int(partID)]
		if !ok {
			panic(fmt.Sprintf("no processor for partition ppm:%v partID: %d", pScheme.PartitionProcessorMapping, partID))
		}
		execCtx.ForwardEntry(processorID, po.forwardReceiverID, int(partID), i, batch, po.outSchema.EventSchema)
	}
	return nil, nil
}

func (po *PartitionOperator) HandleBarrier(execCtx StreamExecContext) error {
	procIDs := po.outSchema.PartitionScheme.ProcessorIDs
	for _, processorID := range procIDs {
		execCtx.ForwardBarrier(processorID, po.forwardReceiverID)
	}
	return nil
}

func (po *PartitionOperator) Schema() *evbatch.EventSchema {
	return po.inSchema.EventSchema
}

func (po *PartitionOperator) InSchema() *OperatorSchema {
	return po.inSchema
}

func (po *PartitionOperator) OutSchema() *OperatorSchema {
	return po.outSchema
}

func (po *PartitionOperator) Setup(mgr StreamManagerCtx) error {
	mgr.RegisterReceiver(po.forwardReceiverID, po.receiver)
	return nil
}

func (po *PartitionOperator) Teardown(mgr StreamManagerCtx, _ *sync.RWMutex) {
	mgr.UnregisterReceiver(po.forwardReceiverID)
}

type partitionReceiver struct {
	po                *PartitionOperator
	expectedSequences []map[int]int
}

func (p *partitionReceiver) ForwardingProcessorCount() int {
	return len(p.po.inSchema.PartitionScheme.ProcessorIDs)
}

func (p *partitionReceiver) InSchema() *OperatorSchema {
	// The batch that gets passed to the receiver already has the offset removed (if applicable)
	return p.po.outSchema
}

func (p *partitionReceiver) OutSchema() *OperatorSchema {
	return p.po.outSchema
}

func (p *partitionReceiver) ReceiveBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	forwardingProcID := execCtx.ForwardingProcessorID()
	// Screen out duplicates that could have been resent from the forward queue of the source processor, e.g. due
	// to network issues, or from old node after fail-over
	seq := execCtx.ForwardSequence()
	log.Debugf("processor %d received batch from processor %d with seq %d", execCtx.Processor().ID(),
		execCtx.ForwardingProcessorID(), execCtx.ForwardSequence())
	seqMap := p.expectedSequences[execCtx.Processor().ID()]
	if seq == -1 {
		// reset - next one should be 1
		log.Debugf("processor %d resetting sequence from processor %d", execCtx.Processor().ID(), execCtx.ForwardingProcessorID())
		seqMap[forwardingProcID] = 1
	} else {
		expected := seqMap[forwardingProcID]
		if seq < expected {
			log.Warnf("node %d received duplicate forwarded batch, will be ignored. processor id %d partition %d forwarding processor id %d sequence %d expected sequence %d version %d",
				execCtx.Processor().ID(), execCtx.PartitionID(), forwardingProcID, seq, expected, execCtx.WriteVersion())
			return nil, nil
		}
		seqMap[forwardingProcID] = seq + 1
	}
	return nil, p.po.sendBatchDownStream(batch, execCtx)
}

func (p *partitionReceiver) ReceiveBarrier(execCtx StreamExecContext) error {
	return p.po.BaseOperator.HandleBarrier(execCtx)
}

func (p *partitionReceiver) RequiresBarriersInjection() bool {
	return false
}
