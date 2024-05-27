package proc

import (
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/mem"
)

type BatchHandler interface {
	HandleProcessBatch(processor Processor, processBatch *ProcessBatch, reprocess bool) (bool, *mem.Batch, []*ProcessBatch, error)
}

type ReceiverInfoProvider interface {
	// GetForwardingProcessorCount returns the number of processors that forward to the specified receiver
	GetForwardingProcessorCount(receiverID int) (int, bool)
	// GetInjectableReceivers returns all the receiver ids that can be injected into a specific processor. In other
	// words it returns the top level receiver ids that run on the specified processor.
	GetInjectableReceivers(processorID int) []int
	// GetRequiredCompletions returns the total number of barriers that need to arrive at terminal receivers for
	// a version to be complete
	GetRequiredCompletions() int
}

type ProcessBatch struct {
	ProcessorID           int
	ReceiverID            int
	PartitionID           int
	ForwardingProcessorID int
	ForwardSequence       int
	Version               int
	Watermark             int
	ReplSeq               int
	CommandID             int
	Barrier               bool
	BackFill              bool
	EvBatch               *evbatch.Batch
	EvBatchBytes          []byte
}

func (pb *ProcessBatch) Copy() *ProcessBatch {
	return &ProcessBatch{
		ProcessorID:           pb.ProcessorID,
		ReceiverID:            pb.ReceiverID,
		PartitionID:           pb.PartitionID,
		ForwardingProcessorID: pb.ForwardingProcessorID,
		ForwardSequence:       pb.ForwardSequence,
		Version:               pb.Version,
		Watermark:             pb.Watermark,
		ReplSeq:               pb.ReplSeq,
		CommandID:             pb.CommandID,
		Barrier:               pb.Barrier,
		BackFill:              pb.BackFill,
		EvBatch:               pb.EvBatch,
		EvBatchBytes:          pb.EvBatchBytes,
	}
}

func NewProcessBatch(processorID int, evBatch *evbatch.Batch, receiverID int, partitionID int, forwardingProcessorID int) *ProcessBatch {
	return &ProcessBatch{
		ProcessorID:           processorID,
		ReceiverID:            receiverID,
		PartitionID:           partitionID,
		ForwardingProcessorID: forwardingProcessorID,
		EvBatch:               evBatch,
		ReplSeq:               -1,
	}
}

func NewForwardedProcessBatch(processorID int, evBatchBytes []byte, receiverID int, version int, partitionID int,
	forwardingProcessorID int, forwardSequence int) *ProcessBatch {
	return &ProcessBatch{
		ProcessorID:           processorID,
		ReceiverID:            receiverID,
		Version:               version,
		PartitionID:           partitionID,
		ForwardingProcessorID: forwardingProcessorID,
		ForwardSequence:       forwardSequence,
		EvBatchBytes:          evBatchBytes,
		ReplSeq:               -1,
	}
}

func NewBarrierProcessBatch(processorID int, receiverID int, version int, waterMark int, forwardingProcessorID int, commandID int) *ProcessBatch {
	return &ProcessBatch{
		ProcessorID:           processorID,
		ReceiverID:            receiverID,
		Version:               version,
		Watermark:             waterMark,
		Barrier:               true,
		ForwardingProcessorID: forwardingProcessorID,
		ReplSeq:               -1,
		CommandID:             commandID,
	}
}

func (pb *ProcessBatch) CheckDeserializeEvBatch(schema *evbatch.EventSchema) {
	if pb.EvBatch == nil && pb.EvBatchBytes != nil {
		pb.EvBatch = evbatch.NewBatchFromSingleBuff(schema, pb.EvBatchBytes)
	}
}

func (pb *ProcessBatch) GetBatchBytes() []byte {
	if pb.EvBatchBytes == nil {
		if pb.EvBatch != nil {
			pb.EvBatchBytes = pb.EvBatch.Serialize(nil)
		}
	}
	return pb.EvBatchBytes
}
