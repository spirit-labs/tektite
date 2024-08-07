package repli

import (
	"fmt"
	"github.com/spirit-labs/tektite/asl/remoting"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
)

func SetClusterMessageHandlers(remotingServer remoting.Server, manager proc.Manager) {
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageReplicateMessage, &replicateMessageHandler{manager: manager})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLastCommittedRequestMessage, &lastCommittedRequestHandler{manager: manager})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageSetLastCommittedMessage, &setLastCommittedHandler{manager: manager})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageFlushMessage, &flushMessageHandler{manager: manager})
}

type replicateMessageHandler struct {
	manager proc.Manager
}

func (r *replicateMessageHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	msg := messageHolder.Message.(*clustermsgs.ReplicateMessage) //nolint:forcetypeassert
	processorID := int(msg.ProcessorId)
	replic, err := getReplicator(r.manager, processorID)
	if err != nil {
		return nil, err
	}
	switch int(msg.ReplicationType) {
	case replicationTypeBatch:
		processBatch := proc.NewForwardedProcessBatch(processorID, msg.BatchBytes, int(msg.ReceiverId),
			0, int(msg.PartitionId), int(msg.ForwardingProcessorId), 0)
		processBatch.ReplSeq = int(msg.SequenceNumber)
		return nil, replic.receiveReplicatedBatch(processBatch, int(msg.ClusterVersion), int(msg.JoinedClusterVersion))
	case replicationTypeCommit:
		return nil, replic.receiveReplicatedCommit(int(msg.SequenceNumber), int(msg.ClusterVersion), int(msg.JoinedClusterVersion))
	case replicationTypeSync, replicationTypeSyncStart, replicationTypeSyncEnd:
		processBatch := proc.NewForwardedProcessBatch(processorID, msg.BatchBytes, int(msg.ReceiverId),
			0, int(msg.PartitionId), int(msg.ForwardingProcessorId), 0)
		processBatch.ReplSeq = int(msg.SequenceNumber)
		return nil, replic.receiveReplicatedSync(processBatch, int(msg.ClusterVersion), int(msg.JoinedClusterVersion),
			int(msg.ReplicationType))
	default:
		panic(fmt.Sprintf("unexpected replication type %d", msg.ReplicationType))
	}
}

type lastCommittedRequestHandler struct {
	manager proc.Manager
}

func (l *lastCommittedRequestHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	msg := messageHolder.Message.(*clustermsgs.LastCommittedRequest)
	replic, err := getReplicator(l.manager, int(msg.ProcessorId))
	if err != nil {
		return nil, err
	}
	if !replic.IsValid() {
		return nil, common.NewTektiteErrorf(common.Unavailable, "not valid")
	}
	lastCommitted, err := replic.GetLastCommitted(int(msg.ClusterVersion), int(msg.JoinedVersion))
	if err != nil {
		return nil, err
	}
	return &clustermsgs.LastCommittedResponse{LastCommitted: int64(lastCommitted)}, nil
}

type setLastCommittedHandler struct {
	manager proc.Manager
}

func (s *setLastCommittedHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	msg := messageHolder.Message.(*clustermsgs.SetLastCommittedMessage)
	replic, err := getReplicator(s.manager, int(msg.ProcessorId))
	if err != nil {
		return nil, err
	}
	if !replic.IsValid() {
		return nil, common.NewTektiteErrorf(common.Unavailable, "not valid")
	}
	return nil, replic.SetLastCommittedSequence(int(msg.LastCommitted), int(msg.JoinedVersion))
}

type flushMessageHandler struct {
	manager proc.Manager
}

func (f *flushMessageHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	msg := messageHolder.Message.(*clustermsgs.FlushMessage) //nolint:forcetypeassert
	replic, err := getReplicator(f.manager, int(msg.ProcessorId))
	if err != nil {
		return nil, err
	}
	return nil, replic.receiveFlushMessage(int(msg.BatchSeq), int(msg.ClusterVersion), int(msg.JoinedClusterVersion))
}

func (r *replicator) sendReplication(processorID int, groupNodes []clustmgr.GroupNode, clusterVersion int,
	batch *proc.ProcessBatch, completionFunc func(error)) {

	msgs := make([]remoting.ClusterMessage, 0, len(groupNodes))
	serverAddresses := make([]string, 0, len(groupNodes))

	for _, groupNode := range groupNodes {
		if groupNode.NodeID != r.cfg.NodeID {
			serverAddresses = append(serverAddresses, r.cfg.ClusterAddresses[groupNode.NodeID])

			msg := &clustermsgs.ReplicateMessage{
				ReplicationType:       replicationTypeBatch,
				ProcessorId:           uint64(processorID),
				ReceiverId:            uint64(batch.ReceiverID),
				PartitionId:           uint64(batch.PartitionID),
				ForwardingProcessorId: int64(batch.ForwardingProcessorID),
				BatchBytes:            batch.GetBatchBytes(),
				SequenceNumber:        uint64(batch.ReplSeq),
				ClusterVersion:        uint64(clusterVersion),
				JoinedClusterVersion:  uint64(groupNode.JoinedVersion),
				SendingNode:           uint32(r.cfg.NodeID),
			}
			msgs = append(msgs, msg)
		}
	}
	// We don't retry here - errors propagated back to the ingestor (e.g. the source) which does the retry
	r.remotingClient.BroadcastAsyncMultipleRequests(func(err error) {
		if err == nil {
			completionFunc(nil)
		} else {
			checkErrorAndCallCompletion(completionFunc, err)
		}
	}, msgs, serverAddresses...)
}

func (r *replicator) sendCommit(processorID int, groupNodes []clustmgr.GroupNode, clusterVersion int,
	batchSeq int, completionFunc func(error)) {
	msgs := make([]remoting.ClusterMessage, 0, len(groupNodes))
	serverAddresses := make([]string, 0, len(groupNodes))
	for _, groupNode := range groupNodes {
		if groupNode.NodeID != r.cfg.NodeID {
			serverAddresses = append(serverAddresses, r.cfg.ClusterAddresses[groupNode.NodeID])
			msg := &clustermsgs.ReplicateMessage{
				ReplicationType:      replicationTypeCommit,
				ProcessorId:          uint64(processorID),
				ClusterVersion:       uint64(clusterVersion),
				JoinedClusterVersion: uint64(groupNode.JoinedVersion),
				SequenceNumber:       uint64(batchSeq),
				SendingNode:          uint32(r.cfg.NodeID),
			}
			msgs = append(msgs, msg)
		}
	}
	// We don't retry here - errors propagated back to the ingestor (e.g. the source) which does the retry
	r.remotingClient.BroadcastAsyncMultipleRequests(func(err error) {
		if err == nil {
			completionFunc(nil)
		} else {
			checkErrorAndCallCompletion(completionFunc, err)
		}
	}, msgs, serverAddresses...)
}

func (r *replicator) sendFlush(processorID int, clusterVersion int, batchSeq int, completionFunc func(error)) {
	groupState, ok := r.manager.GetGroupState(processorID)
	if !ok {
		// This can occur if cluster not ready - client will retry and it should resolve
		completionFunc(common.NewTektiteErrorf(common.Unavailable, "no processor available when sending flush"))
		return
	}
	groupNodes := groupState.GroupNodes

	msgs := make([]remoting.ClusterMessage, 0, len(groupNodes))
	serverAddresses := make([]string, 0, len(groupNodes))
	for _, groupNode := range groupNodes {
		if groupNode.NodeID != r.cfg.NodeID {
			serverAddresses = append(serverAddresses, r.cfg.ClusterAddresses[groupNode.NodeID])
			msg := &clustermsgs.FlushMessage{
				ProcessorId:          uint64(processorID),
				BatchSeq:             uint64(batchSeq),
				ClusterVersion:       uint64(clusterVersion),
				JoinedClusterVersion: uint64(groupNode.JoinedVersion),
			}
			msgs = append(msgs, msg)
		}
	}
	// We don't retry here - errors propagated back to the ingestor (e.g. the source) which does the retry
	r.remotingClient.BroadcastAsyncMultipleRequests(func(err error) {
		if err == nil {
			completionFunc(nil)
		} else {
			checkErrorAndCallCompletion(completionFunc, err)
		}
	}, msgs, serverAddresses...)
}

func (r *replicator) sendSyncBatchToNode(processorID int, nodeID int, clusterVersion int, joinedClusterVersion int,
	replType replicationType, seq int, batch *proc.ProcessBatch, completionFunc func(error)) {

	serverAddress := r.cfg.ClusterAddresses[nodeID]
	msg := &clustermsgs.ReplicateMessage{
		ReplicationType:       uint32(replType),
		ProcessorId:           uint64(processorID),
		SequenceNumber:        uint64(batch.ReplSeq),
		ClusterVersion:        uint64(clusterVersion),
		JoinedClusterVersion:  uint64(joinedClusterVersion),
		BatchBytes:            batch.GetBatchBytes(),
		ForwardingProcessorId: int64(batch.ForwardingProcessorID),
		SendingNode:           uint32(r.cfg.NodeID),
		ReplSeq:               uint64(seq),
	}
	r.remotingClient.SendRPCAsync(func(_ remoting.ClusterMessage, err error) {
		if err == nil {
			completionFunc(nil)
		} else {
			checkErrorAndCallCompletion(completionFunc, err)
		}
	}, msg, serverAddress)
}

func checkErrorAndCallCompletion(completionFunc func(error), err error) {
	if err == nil {
		completionFunc(nil)
		return
	}
	completionFunc(remoting.MaybeConvertError(err))
}

func getReplicator(manager proc.Manager, processorID int) (*replicator, error) {
	processor := manager.GetProcessor(processorID)
	if processor == nil {
		return nil, common.NewTektiteErrorf(common.Unavailable, "no processor available when getting replicator")
	}
	if processor.IsLeader() {
		repl := processor.GetReplicator().(*replicator)

		return nil, common.NewTektiteErrorf(common.Unavailable, "not a replica on node %d",
			repl.cfg.NodeID)
	}
	return processor.GetReplicator().(*replicator), nil
}
