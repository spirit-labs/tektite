package proc

import (
	"fmt"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
)

type forwardMessageHandler struct {
	m *ProcessorManager
}

func (f *forwardMessageHandler) HandleMessage(messageHolder remoting.MessageHolder, completionFunc func(remoting.ClusterMessage, error)) {
	forwardMsg := messageHolder.Message.(*clustermsgs.ForwardBatchMessage)
	o, ok := f.m.processors.Load(int(forwardMsg.ProcessorId))
	if !ok {
		completionFunc(nil, errors.NewTektiteErrorf(errors.Unavailable, "no processor available on receipt of forward message"))
		return
	}
	processor := o.(Processor)
	var pb *ProcessBatch
	if !forwardMsg.Barrier {
		pb = NewForwardedProcessBatch(int(forwardMsg.ProcessorId), forwardMsg.BatchBytes, int(forwardMsg.ReceiverId),
			int(forwardMsg.Version), int(forwardMsg.PartitionId), int(forwardMsg.ForwardingProcessorId), int(forwardMsg.ForwardingSequence))
	} else {
		pb = NewBarrierProcessBatch(int(forwardMsg.ProcessorId), int(forwardMsg.ReceiverId), int(forwardMsg.Version),
			int(forwardMsg.WaterMark), int(forwardMsg.ForwardingProcessorId), int(forwardMsg.CommandId))
	}
	if forwardMsg.Replicate {
		processor.GetReplicator().ReplicateBatch(pb, func(err error) {
			completionFunc(nil, err)
		})
	} else {
		processor.IngestBatch(pb, func(err error) {
			completionFunc(nil, err)
		})
	}
}

func (m *ProcessorManager) ForwardBatch(batch *ProcessBatch, replicate bool, completionFunc func(error)) {

	if batch.ProcessorID < 0 {
		panic(fmt.Sprintf("invalid processor id for forwarding %d", batch.ProcessorID))
	}

	leaderNode, err := m.GetLeaderNode(batch.ProcessorID)
	if err != nil {
		completionFunc(err)
		return
	}

	if leaderNode == m.cfg.NodeID {
		// Processor is local
		processor, ok := m.GetProcessor(batch.ProcessorID)
		if !ok {
			completionFunc(errors.NewTektiteErrorf(errors.Unavailable, "processor not available locally"))
			return
		}
		if replicate {
			processor.GetReplicator().ReplicateBatch(batch, completionFunc)
		} else {
			processor.IngestBatch(batch, completionFunc)
		}
		return
	}

	message := &clustermsgs.ForwardBatchMessage{
		ProcessorId:           uint64(batch.ProcessorID),
		ReceiverId:            uint64(batch.ReceiverID),
		PartitionId:           uint64(batch.PartitionID),
		ForwardingProcessorId: int64(batch.ForwardingProcessorID),
		ForwardingSequence:    int64(batch.ForwardSequence),
		CommandId:             int64(batch.CommandID),
		Version:               uint64(batch.Version),
		WaterMark:             int64(batch.Watermark),
		Barrier:               batch.Barrier,
		BatchBytes:            batch.GetBatchBytes(),
		Replicate:             replicate,
	}

	serverAddress := m.cfg.ClusterAddresses[leaderNode]
	m.remotingClient.SendRPCAsync(func(_ remoting.ClusterMessage, err error) {
		checkErrorAndCallCompletion(completionFunc, err)
	}, message, serverAddress)
}

func checkErrorAndCallCompletion(completionFunc func(error), err error) {
	if err == nil {
		completionFunc(nil)
		return
	}
	completionFunc(remoting.MaybeConvertError(err))
}
