package opers

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/proc"
)

type StreamExecContext interface {
	StoreEntry(kv common.KV, noCache bool)
	ForwardEntry(processorID int, receiverID int, remotePartitionID int, rowIndex int, batch *evbatch.Batch, schema *evbatch.EventSchema)
	ForwardBarrier(processorID int, receiverID int)
	CheckInProcessorLoop()
	WriteVersion() int
	PartitionID() int
	ForwardingProcessorID() int
	ForwardSequence() int
	Processor() proc.Processor
	Get(key []byte) ([]byte, error)
	BackFill() bool
	WaterMark() int
	SetWaterMark(waterMark int)
	EventBatchBytes() []byte
	ReceiverID() int
}

type QueryExecContext interface {
	ExecID() string
	ResultAddress() string
	Last() bool
	ExecState() any
}
