package kafkaserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaserver/protocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/types"
	"net"
	"strconv"
	"time"
)

type NewHandler struct {
	conn *connection
}

func (n *NewHandler) HandleProduceRequest(_ *protocol.RequestHeader, req *protocol.ProduceRequest, completionFunc func(resp *protocol.ProduceResponse) error) error {
	var resp protocol.ProduceResponse
	resp.Responses = make([]protocol.ProduceResponseTopicProduceResponse, len(req.TopicData))
	toComplete := len(req.TopicData) * len(req.TopicData[0].PartitionData)
	// Note, the CountDownFuture provides a memory barrier so different goroutines can safely write into the ProduceResponse
	// (they never write into the same array indexes and the arrays are created before ingesting)
	cf := common.NewCountDownFuture(toComplete, func(_ error) {
		if err := completionFunc(&resp); err != nil {
			log.Errorf("failed to send produce response: %v", err)
		}
	})
	for i, topicData := range req.TopicData {
		resp.Responses[i].Name = topicData.Name
		partitionResponses := make([]protocol.ProduceResponsePartitionProduceResponse, len(topicData.PartitionData))
		resp.Responses[i].PartitionResponses = partitionResponses
		topicInfo, topicExists := n.conn.s.metadataProvider.GetTopicInfo(*topicData.Name)
		for j, partitionData := range topicData.PartitionData {
			partitionResponses[j].Index = partitionData.Index
			if !topicExists {
				partitionResponses[j].ErrorCode = protocol.ErrorCodeUnknownTopicOrPartition
				msg := fmt.Sprintf("unknown topic:%s", *topicData.Name)
				partitionResponses[j].ErrorMessage = &msg
				cf.CountDown(nil)
				continue
			}
			processorID, ok := topicInfo.ProduceInfoProvider.PartitionScheme().PartitionProcessorMapping[int(partitionData.Index)]
			if !ok {
				panic("no processor for partition")
			}
			processor := n.conn.s.procProvider.GetProcessor(processorID)
			if processor == nil {
				partitionResponses[j].ErrorCode = protocol.ErrorCodeUnknownTopicOrPartition
				cf.CountDown(nil)
				continue
			}
			if !processor.IsLeader() {
				partitionResponses[j].ErrorCode = protocol.ErrorCodeNotLeaderOrFollower
				cf.CountDown(nil)
				continue
			}
			var partitionFetcher *PartitionFetcher
			if topicInfo.ConsumeEnabled {
				// can be nil for produce only topic
				var err error
				partitionFetcher, err = n.conn.s.fetcher.GetPartitionFetcher(&topicInfo, partitionData.Index)
				if err != nil {
					log.Errorf("failed to fetch partition fetcher for partition %d: %v", partitionData.Index, err)
					partitionResponses[j].ErrorCode = protocol.ErrorCodeUnknownTopicOrPartition
					cf.CountDown(nil)
					continue
				}
			}
			if len(partitionData.Records) > 1 {
				log.Errorf("unexpected more than one records produced: %d", len(partitionData.Records))
				partitionResponses[j].ErrorCode = protocol.ErrorCodeUnknownServerError
				cf.CountDown(nil)
				continue
			}

			producedRecords := partitionData.Records[0]
			producerID := int(binary.BigEndian.Uint64(producedRecords[43:51]))
			baseSequence := int(binary.BigEndian.Uint32(producedRecords[53:57]))
			lastOffsetDelta := int(binary.BigEndian.Uint32(producedRecords[23:27]))
			sequenceNumber := baseSequence + lastOffsetDelta
			// if producerID is 0 then idempotency is disabled on the producer
			if producerID > 0 {
				lastSequenceNumber, exists := topicInfo.ProduceInfoProvider.GetIdempotentProducerMetadata(producerID)
				if exists && sequenceNumber <= lastSequenceNumber {
					partitionResponses[j].ErrorCode = protocol.ErrorCodeDuplicateSequenceNumber
					cf.CountDown(nil)
					continue
				}
				topicInfo.ProduceInfoProvider.SetIdempotentProducerMetadata(producerID, sequenceNumber)
			}

			numRecords := int(binary.BigEndian.Uint32(producedRecords[57:]))
			magic := producedRecords[16]
			if magic != 2 {
				partitionResponses[j].ErrorCode = protocol.ErrorCodeUnsupportedForMessageFormat
				cf.CountDown(nil)
				continue
			}
			partitionID := int(partitionData.Index)
			index := j
			topicInfo.ProduceInfoProvider.IngestBatch(producedRecords, processor, partitionID,
				func(err error) {
					processor.CheckInProcessorLoop()
					if err != nil {
						var errorCode int16
						if common.IsUnavailableError(err) {
							log.Warnf("failed to replicate produce batch %v", err)
							// This can occur, e.g. due to insufficient replicas available, or replicas are currently
							// syncing. It should resolve when sufficient nodes become available or sync completes.
							errorCode = protocol.ErrorCodeLeaderNotAvailable
						} else {
							log.Errorf("failed to replicate produce batch %v", err)
							errorCode = protocol.ErrorCodeUnknownServerError
						}
						partitionResponses[index].ErrorCode = errorCode
						cf.CountDown(nil)
						return
					}
					offset, appendTime := topicInfo.ProduceInfoProvider.GetLastProducedInfo(int(partitionID))
					partitionResponses[index].BaseOffset = offset
					partitionResponses[index].LogAppendTimeMs = appendTime
					if partitionFetcher != nil && topicInfo.CanCache {
						partitionFetcher.AddBatch(offset-int64(numRecords)+1, offset, producedRecords)
					}
					cf.CountDown(nil)
				})
		}
	}
	return nil
}

func (n *NewHandler) ProduceRequestErrorResponse(errorCode int16, errorMsg string, req *protocol.ProduceRequest) *protocol.ProduceResponse {
	var resp protocol.ProduceResponse
	resp.Responses = make([]protocol.ProduceResponseTopicProduceResponse, len(req.TopicData))
	for i, topicData := range req.TopicData {
		resp.Responses[i].Name = topicData.Name
		partitionResponses := make([]protocol.ProduceResponsePartitionProduceResponse, len(topicData.PartitionData))
		resp.Responses[i].PartitionResponses = partitionResponses
		for j, partitionData := range topicData.PartitionData {
			partitionResponses[j].Index = partitionData.Index
			partitionResponses[j].ErrorCode = errorCode
			if errorMsg != "" {
				partitionResponses[j].ErrorMessage = &errorMsg
			}
		}
	}
	return &resp
}

func (n *NewHandler) HandleFetchRequest(_ *protocol.RequestHeader, req *protocol.FetchRequest, completionFunc func(resp *protocol.FetchResponse) error) error {
	var resp protocol.FetchResponse
	resp.Responses = make([]protocol.FetchResponseFetchableTopicResponse, len(req.Topics))
	toComplete := len(req.Topics) * len(req.Topics[0].Partitions)
	// Note, the CountDownFuture provides a memory barrier so different goroutines can safely write into the ProduceResponse
	// (they never write into the same array indexes and the arrays are created before ingesting)
	cf := common.NewCountDownFuture(toComplete, func(_ error) {
		if err := completionFunc(&resp); err != nil {
			log.Errorf("failed to send produce response: %v", err)
		}
	})
	var waiters []*Waiter
	hasData := false
	for i, topic := range req.Topics {
		resp.Responses[i].Topic = topic.Topic
		partitionResponses := make([]protocol.FetchResponsePartitionData, len(topic.Partitions))
		resp.Responses[i].Partitions = partitionResponses
		topicInfo, topicExists := n.conn.s.metadataProvider.GetTopicInfo(*topic.Topic)
		for j, partitionData := range topic.Partitions {
			partitionResponses[j].PartitionIndex = partitionData.Partition
			if !topicExists {
				partitionResponses[j].ErrorCode = protocol.ErrorCodeUnknownTopicOrPartition
				cf.CountDown(nil)
				continue
			}
			// Note that fetchMaxBytes is not a hard limit - total bytes returned can be greater than this
			// depending on number of partitions in fetch request and size of first batch available in partition
			fetchMaxBytes := req.MaxBytes
			if partitionData.PartitionMaxBytes < fetchMaxBytes {
				fetchMaxBytes = partitionData.PartitionMaxBytes
			}
			partitionFetcher, err := n.conn.s.fetcher.GetPartitionFetcher(&topicInfo, partitionData.Partition)
			if err != nil {
				log.Errorf("failed to find partition fetcher for topic:%s partition:%d", *topic.Topic, partitionData.Partition)
				partitionResponses[j].ErrorCode = protocol.ErrorCodeUnknownTopicOrPartition
				cf.CountDown(nil)
				continue
			} else {
				index := j
				waiter := partitionFetcher.Fetch(partitionData.FetchOffset, int(req.MinBytes), int(fetchMaxBytes), time.Duration(req.MaxWaitMs)*time.Millisecond,
					func(batches [][]byte, hwm int64, err error) {
						if err != nil {
							var errorCode int16
							var kerr KafkaProtocolError
							if errors.As(err, &kerr) {
								errorCode = kerr.ErrorCode
							} else {
								errorCode = protocol.ErrorCodeUnknownServerError
							}
							log.Errorf("failed to execute fetch %v", err)
							partitionResponses[index].ErrorCode = errorCode
							cf.CountDown(nil)
						} else {
							partitionResponses[index].HighWatermark = hwm
							partitionResponses[index].Records = batches
							cf.CountDown(nil)
						}
					})
				if waiter != nil {
					waiters = append(waiters, waiter)
				} else {
					hasData = true
				}
			}
		}
	}
	if !hasData {
		for _, waiter := range waiters {
			waiter.schedule()
		}
	} else {
		// If any of the fetches returned data then we complete the response now
		for _, waiter := range waiters {
			waiter.complete()
		}
	}
	return nil
}

func (n *NewHandler) FetchRequestErrorResponse(errorCode int16, _ string, req *protocol.FetchRequest) *protocol.FetchResponse {
	var resp protocol.FetchResponse
	resp.ErrorCode = errorCode
	resp.Responses = make([]protocol.FetchResponseFetchableTopicResponse, len(req.Topics))
	for i, topic := range req.Topics {
		resp.Responses[i].Topic = topic.Topic
		resp.Responses[i].TopicId = topic.TopicId
		partitionResponses := make([]protocol.FetchResponsePartitionData, len(topic.Partitions))
		resp.Responses[i].Partitions = partitionResponses
		for j, partitionData := range topic.Partitions {
			partitionResponses[j].PartitionIndex = partitionData.Partition
			partitionResponses[j].ErrorCode = errorCode
		}
	}
	return &resp
}

func (n *NewHandler) HandleMetadataRequest(_ *protocol.RequestHeader, req *protocol.MetadataRequest, completionFunc func(resp *protocol.MetadataResponse) error) error {
	var resp protocol.MetadataResponse
	brokerInfos := n.conn.s.metadataProvider.BrokerInfos()
	resp.Brokers = make([]protocol.MetadataResponseMetadataResponseBroker, len(brokerInfos))
	for i, brokerInfo := range brokerInfos {
		var broker protocol.MetadataResponseMetadataResponseBroker
		broker.Host = &brokerInfo.Host
		broker.Port = int32(brokerInfo.Port)
		broker.NodeId = int32(brokerInfo.NodeID)
		resp.Brokers[i] = broker
	}
	resp.ControllerId = int32(n.conn.s.metadataProvider.ControllerNodeID())
	if len(req.Topics) == 0 {
		// request for all topics
		topicInfos := n.conn.s.metadataProvider.GetAllTopics()
		resp.Topics = make([]protocol.MetadataResponseMetadataResponseTopic, len(topicInfos))
		for i, topicInfo := range topicInfos {
			topic := populateTopic(topicInfo)
			resp.Topics[i] = topic
		}
	} else {
		resp.Topics = make([]protocol.MetadataResponseMetadataResponseTopic, len(req.Topics))
		for i, top := range req.Topics {
			topicInfo, ok := n.conn.s.metadataProvider.GetTopicInfo(*top.Name)
			if !ok {
				resp.Topics[i].Name = top.Name
				resp.Topics[i].ErrorCode = protocol.ErrorCodeUnknownTopicOrPartition
			} else {
				top := populateTopic(&topicInfo)
				resp.Topics[i] = top
			}
		}
	}
	return completionFunc(&resp)
}

func (n *NewHandler) MetadataRequestErrorResponse(errorCode int16, _ string, req *protocol.MetadataRequest) *protocol.MetadataResponse {
	var resp protocol.MetadataResponse
	if len(req.Topics) >= 0 {
		resp.Topics = make([]protocol.MetadataResponseMetadataResponseTopic, len(req.Topics))
		for i, top := range req.Topics {
			resp.Topics[i].Name = top.Name
			resp.Topics[i].ErrorCode = errorCode
		}
	}
	return &resp
}

func populateTopic(topicInfo *TopicInfo) protocol.MetadataResponseMetadataResponseTopic {
	var topic protocol.MetadataResponseMetadataResponseTopic
	topic.Name = &topicInfo.Name
	topic.Partitions = make([]protocol.MetadataResponseMetadataResponsePartition, len(topicInfo.Partitions))
	for i, partitionInfo := range topicInfo.Partitions {
		var part protocol.MetadataResponseMetadataResponsePartition
		part.PartitionIndex = int32(partitionInfo.ID)
		part.LeaderId = int32(partitionInfo.LeaderNodeID)
		for _, replicaNodeID := range partitionInfo.ReplicaNodeIDs {
			part.ReplicaNodes = append(part.ReplicaNodes, int32(replicaNodeID))
		}
		// isr nodes
		part.IsrNodes = part.ReplicaNodes
		topic.Partitions[i] = part
	}
	return topic
}

func (n *NewHandler) OffsetCommitRequestErrorResponse(errorCode int16, _ string, req *protocol.OffsetCommitRequest) *protocol.OffsetCommitResponse {
	var resp protocol.OffsetCommitResponse
	resp.Topics = make([]protocol.OffsetCommitResponseOffsetCommitResponseTopic, len(req.Topics))
	for i, topic := range req.Topics {
		resp.Topics[i].Name = topic.Name
		partitionResponses := make([]protocol.OffsetCommitResponseOffsetCommitResponsePartition, len(topic.Partitions))
		resp.Topics[i].Partitions = partitionResponses
		for j, partitionData := range topic.Partitions {
			partitionResponses[j].PartitionIndex = partitionData.PartitionIndex
			partitionResponses[j].ErrorCode = errorCode
		}
	}
	return &resp
}

func (n *NewHandler) HandleOffsetCommitRequest(_ *protocol.RequestHeader, req *protocol.OffsetCommitRequest, completionFunc func(resp *protocol.OffsetCommitResponse) error) error {
	var resp protocol.OffsetCommitResponse
	topicNames := make([]string, len(req.Topics))
	partitionIDs := make([][]int32, len(req.Topics))
	offsets := make([][]int64, len(req.Topics))
	resp.Topics = make([]protocol.OffsetCommitResponseOffsetCommitResponseTopic, len(req.Topics))
	for i, topicInfo := range req.Topics {
		resp.Topics[i].Partitions = make([]protocol.OffsetCommitResponseOffsetCommitResponsePartition, len(topicInfo.Partitions))
		resp.Topics[i].Name = topicInfo.Name
		topicNames[i] = *topicInfo.Name
		pids := make([]int32, len(topicInfo.Partitions))
		offs := make([]int64, len(topicInfo.Partitions))
		for j, partitionInfo := range topicInfo.Partitions {
			resp.Topics[i].Partitions[j].PartitionIndex = partitionInfo.PartitionIndex
			pids[j] = partitionInfo.PartitionIndex
			offs[j] = partitionInfo.CommittedOffset
		}
		partitionIDs[i] = pids
		offsets[i] = offs
	}
	// TODO why not pass the req straight into the OffsetCommit method?
	errorCodes := n.conn.s.groupCoordinator.OffsetCommit(*req.GroupId, *req.MemberId,
		int(req.GenerationIdOrMemberEpoch), topicNames, partitionIDs, offsets)
	for i, errs := range errorCodes {
		for j, errCode := range errs {
			resp.Topics[i].Partitions[j].ErrorCode = errCode
		}
	}
	return completionFunc(&resp)
}

func (n *NewHandler) OffsetFetchRequestErrorResponse(errorCode int16, _ string, req *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {
	var resp protocol.OffsetFetchResponse
	if len(req.Groups) > 0 {
		resp.Groups = make([]protocol.OffsetFetchResponseOffsetFetchResponseGroup, len(req.Groups))
		for i, g := range req.Groups {
			resp.Groups[i].GroupId = g.GroupId
			resp.Groups[i].ErrorCode = errorCode
		}
	} else {
		resp.ErrorCode = errorCode
	}
	return &resp
}

func (n *NewHandler) HandleOffsetFetchRequest(_ *protocol.RequestHeader, req *protocol.OffsetFetchRequest, completionFunc func(resp *protocol.OffsetFetchResponse) error) error {
	var resp protocol.OffsetFetchResponse
	topicNames := make([]string, len(req.Topics))
	partitionIDs := make([][]int32, len(req.Topics))
	for i, topicInfo := range req.Topics {
		topicNames[i] = *topicInfo.Name
		partitionIDs[i] = topicInfo.PartitionIndexes
	}
	offsets, errorCodes, topLevelErrorCode := n.conn.s.groupCoordinator.OffsetFetch(*req.GroupId, topicNames, partitionIDs)
	resp.Topics = make([]protocol.OffsetFetchResponseOffsetFetchResponseTopic, len(req.Topics))
	for i, topicName := range topicNames {
		resp.Topics[i].Name = &topicName
		resp.Topics[i].Partitions = make([]protocol.OffsetFetchResponseOffsetFetchResponsePartition, len(partitionIDs[i]))
		partitions := partitionIDs[i]
		for j, partitionID := range partitions {
			errorCode := errorCodes[i][j]
			if topLevelErrorCode != protocol.ErrorCodeNone {
				errorCode = topLevelErrorCode
			}
			resp.Topics[i].Partitions[j].PartitionIndex = partitionID
			if errorCode == protocol.ErrorCodeNone {
				resp.Topics[i].Partitions[j].CommittedOffset = offsets[i][j]
			} else {
				resp.Topics[i].Partitions[j].ErrorCode = errorCode
			}
		}
	}
	return completionFunc(&resp)
}

func (n *NewHandler) ListOffsetsRequestErrorResponse(errorCode int16, _ string, req *protocol.ListOffsetsRequest) *protocol.ListOffsetsResponse {
	var resp protocol.ListOffsetsResponse
	resp.Topics = make([]protocol.ListOffsetsResponseListOffsetsTopicResponse, len(req.Topics))
	for i, topic := range req.Topics {
		resp.Topics[i].Name = topic.Name
		partitionResponses := make([]protocol.ListOffsetsResponseListOffsetsPartitionResponse, len(topic.Partitions))
		resp.Topics[i].Partitions = partitionResponses
		for j, partitionData := range topic.Partitions {
			partitionResponses[j].PartitionIndex = partitionData.PartitionIndex
			partitionResponses[j].ErrorCode = errorCode
		}
	}
	return &resp
}

func (n *NewHandler) HandleListOffsetsRequest(_ *protocol.RequestHeader, req *protocol.ListOffsetsRequest, completionFunc func(resp *protocol.ListOffsetsResponse) error) error {
	var resp protocol.ListOffsetsResponse
	topicNames := make([]string, len(req.Topics))
	type partitionOffset struct {
		partitionID  int32
		timestamp    int64
		resOffset    int64
		resTimestamp int64
		errorCode    int16
	}
	partitionOffsets := make([][]partitionOffset, len(req.Topics))
	resp.Topics = make([]protocol.ListOffsetsResponseListOffsetsTopicResponse, len(req.Topics))
	for i, topicInfo := range req.Topics {
		resp.Topics[i].Partitions = make([]protocol.ListOffsetsResponseListOffsetsPartitionResponse, len(topicInfo.Partitions))
		resp.Topics[i].Name = topicInfo.Name
		topicNames[i] = *topicInfo.Name
		partitionOffsets[i] = make([]partitionOffset, len(topicInfo.Partitions))
		for j, partitionInfo := range topicInfo.Partitions {
			partitionOffsets[i][j].partitionID = partitionInfo.PartitionIndex
			partitionOffsets[i][j].timestamp = partitionInfo.Timestamp
		}
	}
	for i, topicName := range topicNames {
		topicInfo, topicExists := n.conn.s.metadataProvider.GetTopicInfo(topicName)
		for j, partitionOff := range partitionOffsets[i] {
			resp.Topics[i].Partitions[j].PartitionIndex = partitionOff.partitionID
			if !topicExists {
				resp.Topics[i].Partitions[j].ErrorCode = protocol.ErrorCodeUnknownTopicOrPartition
				continue
			}
			timestamp := partitionOff.timestamp
			var resOffset, resTimestamp int64
			var ok bool
			if timestamp == -2 || timestamp == -4 {
				resOffset, resTimestamp, ok = topicInfo.ConsumerInfoProvider.EarliestOffset(int(partitionOff.partitionID))
			} else if timestamp == -1 {
				var err error
				resOffset, resTimestamp, ok, err = topicInfo.ConsumerInfoProvider.LatestOffset(int(partitionOff.partitionID))
				if err != nil {
					log.Errorf("failed to get latest offset %v", err)
					resp.Topics[i].Partitions[j].ErrorCode = protocol.ErrorCodeUnknownServerError
					continue
				}
			} else {
				resOffset, resTimestamp, ok = topicInfo.ConsumerInfoProvider.OffsetByTimestamp(types.NewTimestamp(timestamp), int(partitionOff.partitionID))
			}
			if !ok {
				resp.Topics[i].Partitions[j].ErrorCode = protocol.ErrorCodeUnknownTopicOrPartition
			} else {
				resp.Topics[i].Partitions[j].Offset = resOffset
				resp.Topics[i].Partitions[j].Timestamp = resTimestamp
			}
		}
	}
	return completionFunc(&resp)
}

func (n *NewHandler) FindCoordinatorRequestErrorResponse(errorCode int16, errorMsg string, req *protocol.FindCoordinatorRequest) *protocol.FindCoordinatorResponse {
	var resp protocol.FindCoordinatorResponse
	resp.ErrorCode = errorCode
	if errorMsg != "" {
		resp.ErrorMessage = &errorMsg
	}
	return &resp
}

func (n *NewHandler) HandleFindCoordinatorRequest(_ *protocol.RequestHeader, req *protocol.FindCoordinatorRequest, completionFunc func(resp *protocol.FindCoordinatorResponse) error) error {
	nodeID := n.conn.s.groupCoordinator.FindCoordinator(*req.Key)
	address := n.conn.s.cfg.KafkaServerListenerConfig.Addresses[nodeID]
	if len(n.conn.s.cfg.KafkaServerListenerConfig.AdvertisedAddresses) > 0 {
		address = n.conn.s.cfg.KafkaServerListenerConfig.AdvertisedAddresses[nodeID]
	}
	host, sPort, err := net.SplitHostPort(address)
	var port int
	if err == nil {
		port, err = strconv.Atoi(sPort)
	}
	if err != nil {
		// Should never happen as addresses will have been verified when returning broker infos in metadata request
		panic(err)
	}
	var resp protocol.FindCoordinatorResponse
	resp.NodeId = int32(nodeID)
	resp.Host = &host
	resp.Port = int32(port)
	return completionFunc(&resp)
}

func (n *NewHandler) JoinGroupRequestErrorResponse(errorCode int16, errorMsg string, req *protocol.JoinGroupRequest) *protocol.JoinGroupResponse {
	var resp protocol.JoinGroupResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (n *NewHandler) HandleJoinGroupRequest(hdr *protocol.RequestHeader, req *protocol.JoinGroupRequest, completionFunc func(resp *protocol.JoinGroupResponse) error) error {
	infos := make([]ProtocolInfo, len(req.Protocols))
	for i, protoInfo := range req.Protocols {
		infos[i] = ProtocolInfo{
			Name:     *protoInfo.Name,
			Metadata: common.ByteSliceCopy(protoInfo.Metadata),
		}
	}
	rebalanceTimeout := 5 * time.Minute
	sessionTimeout := time.Duration(req.SessionTimeoutMs) * time.Millisecond
	n.conn.s.groupCoordinator.JoinGroup(hdr.RequestApiVersion, *req.GroupId, *hdr.ClientId, *req.MemberId, *req.ProtocolType, infos, sessionTimeout, rebalanceTimeout, func(result JoinResult) {
		var resp protocol.JoinGroupResponse
		resp.ErrorCode = int16(result.ErrorCode)
		if resp.ErrorCode == protocol.ErrorCodeNone {
			resp.GenerationId = int32(result.GenerationID)
			resp.ProtocolName = &result.ProtocolName
			resp.Leader = &result.LeaderMemberID
			resp.MemberId = &result.MemberID
			resp.Members = make([]protocol.JoinGroupResponseJoinGroupResponseMember, len(result.Members))
			for i, m := range result.Members {
				memberID := m.MemberID // create new variable as taking address of loop variable will be incorrect
				resp.Members[i].MemberId = &memberID
				resp.Members[i].Metadata = m.MetaData
			}
		}
		if err := completionFunc(&resp); err != nil {
			log.Errorf("failed to send join group response %v", err)
		}
	})
	return nil
}

func (n *NewHandler) HeartbeatRequestErrorResponse(errorCode int16, _ string, _ *protocol.HeartbeatRequest) *protocol.HeartbeatResponse {
	var resp protocol.HeartbeatResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (n *NewHandler) HandleHeartbeatRequest(_ *protocol.RequestHeader, req *protocol.HeartbeatRequest, completionFunc func(resp *protocol.HeartbeatResponse) error) error {
	errorCode := n.conn.s.groupCoordinator.HeartbeatGroup(*req.GroupId, *req.MemberId, int(req.GenerationId))
	var resp protocol.HeartbeatResponse
	resp.ErrorCode = int16(errorCode)
	return completionFunc(&resp)
}

func (n *NewHandler) LeaveGroupRequestErrorResponse(errorCode int16, _ string, _ *protocol.LeaveGroupRequest) *protocol.LeaveGroupResponse {
	var resp protocol.LeaveGroupResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (n *NewHandler) HandleLeaveGroupRequest(_ *protocol.RequestHeader, req *protocol.LeaveGroupRequest, completionFunc func(resp *protocol.LeaveGroupResponse) error) error {
	leaveInfos := []MemberLeaveInfo{{MemberID: *req.MemberId}}
	errorCode := n.conn.s.groupCoordinator.LeaveGroup(*req.GroupId, leaveInfos)
	var resp protocol.LeaveGroupResponse
	resp.ErrorCode = errorCode
	return completionFunc(&resp)
}

func (n *NewHandler) SyncGroupRequestErrorResponse(errorCode int16, _ string, _ *protocol.SyncGroupRequest) *protocol.SyncGroupResponse {
	var resp protocol.SyncGroupResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (n *NewHandler) HandleSyncGroupRequest(_ *protocol.RequestHeader, req *protocol.SyncGroupRequest, completionFunc func(resp *protocol.SyncGroupResponse) error) error {
	assignments := make([]AssignmentInfo, len(req.Assignments))
	for i, assignment := range req.Assignments {
		mem := *assignment.MemberId
		assignments[i] = AssignmentInfo{
			MemberID:   mem,
			Assignment: assignment.Assignment,
		}
	}
	n.conn.s.groupCoordinator.SyncGroup(*req.GroupId, *req.MemberId, int(req.GenerationId), assignments, func(errorCode int, assignment []byte) {
		var resp protocol.SyncGroupResponse
		resp.ErrorCode = int16(errorCode)
		if resp.ErrorCode == protocol.ErrorCodeNone {
			resp.Assignment = assignment
		}
		if err := completionFunc(&resp); err != nil {
			log.Errorf("failed to send sync group response %v", err)
		}
	})
	return nil
}

func (n *NewHandler) ApiVersionsRequestErrorResponse(errorCode int16, _ string, req *protocol.ApiVersionsRequest) *protocol.ApiVersionsResponse {
	var resp protocol.ApiVersionsResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (n *NewHandler) HandleApiVersionsRequest(_ *protocol.RequestHeader, _ *protocol.ApiVersionsRequest, completionFunc func(resp *protocol.ApiVersionsResponse) error) error {
	var resp protocol.ApiVersionsResponse
	resp.ApiKeys = protocol.SupportedAPIVersions
	return completionFunc(&resp)
}

func (n *NewHandler) SaslHandshakeRequestErrorResponse(errorCode int16, _ string, req *protocol.SaslHandshakeRequest) *protocol.SaslHandshakeResponse {
	var resp protocol.SaslHandshakeResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (n *NewHandler) HandleSaslHandshakeRequest(_ *protocol.RequestHeader, _ *protocol.SaslHandshakeRequest, completionFunc func(resp *protocol.SaslHandshakeResponse) error) error {
	var resp protocol.SaslHandshakeResponse
	// For now, we don't implement any mechanisms, this is just a placeholder
	return completionFunc(&resp)
}

func (n *NewHandler) SaslAuthenticateRequestErrorResponse(errorCode int16, _ string, req *protocol.SaslAuthenticateRequest) *protocol.SaslAuthenticateResponse {
	var resp protocol.SaslAuthenticateResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (n *NewHandler) HandleSaslAuthenticateRequest(_ *protocol.RequestHeader, req *protocol.SaslAuthenticateRequest, completionFunc func(resp *protocol.SaslAuthenticateResponse) error) error {
	var resp protocol.SaslAuthenticateResponse
	// Always fail, just a placeholder
	resp.ErrorCode = protocol.ErrorCodeSaslAuthenticationFailed
	return completionFunc(&resp)
}
