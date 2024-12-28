package kafkaserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/types"
	"net"
	"strconv"
	"time"
)

func (c *connection) HandleCreateTopicsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.CreateTopicsRequest, completionFunc func(resp *kafkaprotocol.CreateTopicsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleDeleteTopicsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.DeleteTopicsRequest, completionFunc func(resp *kafkaprotocol.DeleteTopicsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleProduceRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.ProduceRequest, completionFunc func(resp *kafkaprotocol.ProduceResponse) error) error {
	var resp kafkaprotocol.ProduceResponse
	resp.Responses = make([]kafkaprotocol.ProduceResponseTopicProduceResponse, len(req.TopicData))
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
		partitionResponses := make([]kafkaprotocol.ProduceResponsePartitionProduceResponse, len(topicData.PartitionData))
		resp.Responses[i].PartitionResponses = partitionResponses
		topicInfo, topicExists := c.s.metadataProvider.GetTopicInfo(*topicData.Name)
		for j, partitionData := range topicData.PartitionData {
			partitionResponses[j].Index = partitionData.Index
			if !topicExists {
				partitionResponses[j].ErrorCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
				msg := fmt.Sprintf("unknown topic:%s", *topicData.Name)
				partitionResponses[j].ErrorMessage = &msg
				cf.CountDown(nil)
				continue
			}
			processorID, ok := topicInfo.ProduceInfoProvider.PartitionScheme().PartitionProcessorMapping[int(partitionData.Index)]
			if !ok {
				panic("no processor for partition")
			}
			processor := c.s.procProvider.GetProcessor(processorID)
			if processor == nil {
				partitionResponses[j].ErrorCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
				cf.CountDown(nil)
				continue
			}
			if !processor.IsLeader() {
				partitionResponses[j].ErrorCode = kafkaprotocol.ErrorCodeNotLeaderOrFollower
				cf.CountDown(nil)
				continue
			}
			var partitionFetcher *PartitionFetcher
			if topicInfo.ConsumeEnabled {
				// can be nil for produce only topic
				var err error
				partitionFetcher, err = c.s.fetcher.GetPartitionFetcher(&topicInfo, partitionData.Index)
				if err != nil {
					log.Errorf("failed to fetch partition fetcher for partition %d: %v", partitionData.Index, err)
					partitionResponses[j].ErrorCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
					cf.CountDown(nil)
					continue
				}
			}
			producedRecords := partitionData.Records
			numRecords := int(binary.BigEndian.Uint32(producedRecords[57:]))
			magic := producedRecords[16]
			if magic != 2 {
				partitionResponses[j].ErrorCode = kafkaprotocol.ErrorCodeUnsupportedForMessageFormat
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
							errorCode = kafkaprotocol.ErrorCodeLeaderNotAvailable
						} else {
							var kafkaInErr *opers.KafkaInError
							if errors.As(err, &kafkaInErr) {
								log.Warn(kafkaInErr.Error())
								errorCode = kafkaInErr.ErrCode
							} else {
								log.Errorf("failed to replicate produce batch %v", err)
								errorCode = kafkaprotocol.ErrorCodeUnknownServerError
							}
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

func (c *connection) ProduceRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.ProduceRequest) *kafkaprotocol.ProduceResponse {
	var resp kafkaprotocol.ProduceResponse
	resp.Responses = make([]kafkaprotocol.ProduceResponseTopicProduceResponse, len(req.TopicData))
	for i, topicData := range req.TopicData {
		resp.Responses[i].Name = topicData.Name
		partitionResponses := make([]kafkaprotocol.ProduceResponsePartitionProduceResponse, len(topicData.PartitionData))
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

func (c *connection) HandleFetchRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.FetchRequest, completionFunc func(resp *kafkaprotocol.FetchResponse) error) error {
	var resp kafkaprotocol.FetchResponse
	resp.Responses = make([]kafkaprotocol.FetchResponseFetchableTopicResponse, len(req.Topics))
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
		partitionResponses := make([]kafkaprotocol.FetchResponsePartitionData, len(topic.Partitions))
		resp.Responses[i].Partitions = partitionResponses
		topicInfo, topicExists := c.s.metadataProvider.GetTopicInfo(*topic.Topic)
		for j, partitionData := range topic.Partitions {
			partitionResponses[j].PartitionIndex = partitionData.Partition
			if !topicExists {
				partitionResponses[j].ErrorCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
				cf.CountDown(nil)
				continue
			}
			// Note that fetchMaxBytes is not a hard limit - total bytes returned can be greater than this
			// depending on number of partitions in fetch request and size of first batch available in partition
			fetchMaxBytes := req.MaxBytes
			if partitionData.PartitionMaxBytes < fetchMaxBytes {
				fetchMaxBytes = partitionData.PartitionMaxBytes
			}
			partitionFetcher, err := c.s.fetcher.GetPartitionFetcher(&topicInfo, partitionData.Partition)
			if err != nil {
				log.Errorf("failed to find partition fetcher for topic:%s partition:%d", *topic.Topic, partitionData.Partition)
				partitionResponses[j].ErrorCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
				cf.CountDown(nil)
				continue
			} else {
				index := j
				waiter := partitionFetcher.Fetch(partitionData.FetchOffset, int(req.MinBytes), int(fetchMaxBytes), time.Duration(req.MaxWaitMs)*time.Millisecond,
					func(batches [][]byte, hwm int64, err error) {
						if err != nil {
							var errorCode int16
							var kerr kafkaencoding.KafkaError
							if errors.As(err, &kerr) {
								errorCode = int16(kerr.ErrorCode)
							} else {
								errorCode = kafkaprotocol.ErrorCodeUnknownServerError
							}
							log.Errorf("failed to execute fetch %v", err)
							partitionResponses[index].ErrorCode = errorCode
							cf.CountDown(nil)
						} else {
							partitionResponses[index].HighWatermark = hwm
							var records []byte
							for _, batch := range batches {
								records = append(records, batch...)
							}
							partitionResponses[index].Records = records
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

func (c *connection) FetchRequestErrorResponse(errorCode int16, _ string, req *kafkaprotocol.FetchRequest) *kafkaprotocol.FetchResponse {
	var resp kafkaprotocol.FetchResponse
	resp.ErrorCode = errorCode
	resp.Responses = make([]kafkaprotocol.FetchResponseFetchableTopicResponse, len(req.Topics))
	for i, topic := range req.Topics {
		resp.Responses[i].Topic = topic.Topic
		resp.Responses[i].TopicId = topic.TopicId
		partitionResponses := make([]kafkaprotocol.FetchResponsePartitionData, len(topic.Partitions))
		resp.Responses[i].Partitions = partitionResponses
		for j, partitionData := range topic.Partitions {
			partitionResponses[j].PartitionIndex = partitionData.Partition
			partitionResponses[j].ErrorCode = errorCode
		}
	}
	return &resp
}

func (c *connection) HandleMetadataRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.MetadataRequest, completionFunc func(resp *kafkaprotocol.MetadataResponse) error) error {
	var resp kafkaprotocol.MetadataResponse
	brokerInfos := c.s.metadataProvider.BrokerInfos()
	resp.Brokers = make([]kafkaprotocol.MetadataResponseMetadataResponseBroker, len(brokerInfos))
	for i, brokerInfo := range brokerInfos {
		var broker kafkaprotocol.MetadataResponseMetadataResponseBroker
		broker.Host = &brokerInfo.Host
		broker.Port = int32(brokerInfo.Port)
		broker.NodeId = int32(brokerInfo.NodeID)
		resp.Brokers[i] = broker
	}
	resp.ControllerId = int32(c.s.metadataProvider.ControllerNodeID())
	if len(req.Topics) == 0 {
		// request for all topics
		topicInfos := c.s.metadataProvider.GetAllTopics()
		resp.Topics = make([]kafkaprotocol.MetadataResponseMetadataResponseTopic, len(topicInfos))
		for i, topicInfo := range topicInfos {
			topic := populateTopic(topicInfo)
			resp.Topics[i] = topic
		}
	} else {
		resp.Topics = make([]kafkaprotocol.MetadataResponseMetadataResponseTopic, len(req.Topics))
		for i, top := range req.Topics {
			topicInfo, ok := c.s.metadataProvider.GetTopicInfo(*top.Name)
			if !ok {
				resp.Topics[i].Name = top.Name
				resp.Topics[i].ErrorCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
			} else {
				top := populateTopic(&topicInfo)
				resp.Topics[i] = top
			}
		}
	}
	return completionFunc(&resp)
}

func (c *connection) MetadataRequestErrorResponse(errorCode int16, _ string, req *kafkaprotocol.MetadataRequest) *kafkaprotocol.MetadataResponse {
	var resp kafkaprotocol.MetadataResponse
	if len(req.Topics) >= 0 {
		resp.Topics = make([]kafkaprotocol.MetadataResponseMetadataResponseTopic, len(req.Topics))
		for i, top := range req.Topics {
			resp.Topics[i].Name = top.Name
			resp.Topics[i].ErrorCode = errorCode
		}
	}
	return &resp
}

func populateTopic(topicInfo *TopicInfo) kafkaprotocol.MetadataResponseMetadataResponseTopic {
	var topic kafkaprotocol.MetadataResponseMetadataResponseTopic
	topic.Name = &topicInfo.Name
	topic.Partitions = make([]kafkaprotocol.MetadataResponseMetadataResponsePartition, len(topicInfo.Partitions))
	for i, partitionInfo := range topicInfo.Partitions {
		var part kafkaprotocol.MetadataResponseMetadataResponsePartition
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

func (c *connection) OffsetCommitRequestErrorResponse(errorCode int16, _ string, req *kafkaprotocol.OffsetCommitRequest) *kafkaprotocol.OffsetCommitResponse {
	var resp kafkaprotocol.OffsetCommitResponse
	resp.Topics = make([]kafkaprotocol.OffsetCommitResponseOffsetCommitResponseTopic, len(req.Topics))
	for i, topic := range req.Topics {
		resp.Topics[i].Name = topic.Name
		partitionResponses := make([]kafkaprotocol.OffsetCommitResponseOffsetCommitResponsePartition, len(topic.Partitions))
		resp.Topics[i].Partitions = partitionResponses
		for j, partitionData := range topic.Partitions {
			partitionResponses[j].PartitionIndex = partitionData.PartitionIndex
			partitionResponses[j].ErrorCode = errorCode
		}
	}
	return &resp
}

func (c *connection) HandleOffsetCommitRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.OffsetCommitRequest, completionFunc func(resp *kafkaprotocol.OffsetCommitResponse) error) error {
	var resp kafkaprotocol.OffsetCommitResponse
	topicNames := make([]string, len(req.Topics))
	partitionIDs := make([][]int32, len(req.Topics))
	offsets := make([][]int64, len(req.Topics))
	resp.Topics = make([]kafkaprotocol.OffsetCommitResponseOffsetCommitResponseTopic, len(req.Topics))
	for i, topicInfo := range req.Topics {
		resp.Topics[i].Partitions = make([]kafkaprotocol.OffsetCommitResponseOffsetCommitResponsePartition, len(topicInfo.Partitions))
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
	errorCodes := c.s.groupCoordinator.OffsetCommit(*req.GroupId, *req.MemberId,
		int(req.GenerationIdOrMemberEpoch), topicNames, partitionIDs, offsets)
	for i, errs := range errorCodes {
		for j, errCode := range errs {
			resp.Topics[i].Partitions[j].ErrorCode = errCode
		}
	}
	return completionFunc(&resp)
}

func (c *connection) OffsetFetchRequestErrorResponse(errorCode int16, _ string, req *kafkaprotocol.OffsetFetchRequest) *kafkaprotocol.OffsetFetchResponse {
	var resp kafkaprotocol.OffsetFetchResponse
	if len(req.Groups) > 0 {
		resp.Groups = make([]kafkaprotocol.OffsetFetchResponseOffsetFetchResponseGroup, len(req.Groups))
		for i, g := range req.Groups {
			resp.Groups[i].GroupId = g.GroupId
			resp.Groups[i].ErrorCode = errorCode
		}
	} else {
		resp.ErrorCode = errorCode
	}
	return &resp
}

func (c *connection) HandleOffsetFetchRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.OffsetFetchRequest, completionFunc func(resp *kafkaprotocol.OffsetFetchResponse) error) error {
	var resp kafkaprotocol.OffsetFetchResponse
	topicNames := make([]string, len(req.Topics))
	partitionIDs := make([][]int32, len(req.Topics))
	for i, topicInfo := range req.Topics {
		topicNames[i] = *topicInfo.Name
		partitionIDs[i] = topicInfo.PartitionIndexes
	}
	offsets, errorCodes, topLevelErrorCode := c.s.groupCoordinator.OffsetFetch(*req.GroupId, topicNames, partitionIDs)
	resp.Topics = make([]kafkaprotocol.OffsetFetchResponseOffsetFetchResponseTopic, len(req.Topics))
	for i, topicName := range topicNames {
		resp.Topics[i].Name = &topicName
		resp.Topics[i].Partitions = make([]kafkaprotocol.OffsetFetchResponseOffsetFetchResponsePartition, len(partitionIDs[i]))
		partitions := partitionIDs[i]
		for j, partitionID := range partitions {
			errorCode := errorCodes[i][j]
			if topLevelErrorCode != kafkaprotocol.ErrorCodeNone {
				errorCode = topLevelErrorCode
			}
			resp.Topics[i].Partitions[j].PartitionIndex = partitionID
			if errorCode == kafkaprotocol.ErrorCodeNone {
				resp.Topics[i].Partitions[j].CommittedOffset = offsets[i][j]
			} else {
				resp.Topics[i].Partitions[j].ErrorCode = errorCode
			}
		}
	}
	return completionFunc(&resp)
}

func (c *connection) ListOffsetsRequestErrorResponse(errorCode int16, _ string, req *kafkaprotocol.ListOffsetsRequest) *kafkaprotocol.ListOffsetsResponse {
	var resp kafkaprotocol.ListOffsetsResponse
	resp.Topics = make([]kafkaprotocol.ListOffsetsResponseListOffsetsTopicResponse, len(req.Topics))
	for i, topic := range req.Topics {
		resp.Topics[i].Name = topic.Name
		partitionResponses := make([]kafkaprotocol.ListOffsetsResponseListOffsetsPartitionResponse, len(topic.Partitions))
		resp.Topics[i].Partitions = partitionResponses
		for j, partitionData := range topic.Partitions {
			partitionResponses[j].PartitionIndex = partitionData.PartitionIndex
			partitionResponses[j].ErrorCode = errorCode
		}
	}
	return &resp
}

func (c *connection) HandleListOffsetsRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.ListOffsetsRequest, completionFunc func(resp *kafkaprotocol.ListOffsetsResponse) error) error {
	var resp kafkaprotocol.ListOffsetsResponse
	topicNames := make([]string, len(req.Topics))
	type partitionOffset struct {
		partitionID  int32
		timestamp    int64
		resOffset    int64
		resTimestamp int64
		errorCode    int16
	}
	partitionOffsets := make([][]partitionOffset, len(req.Topics))
	resp.Topics = make([]kafkaprotocol.ListOffsetsResponseListOffsetsTopicResponse, len(req.Topics))
	for i, topicInfo := range req.Topics {
		resp.Topics[i].Partitions = make([]kafkaprotocol.ListOffsetsResponseListOffsetsPartitionResponse, len(topicInfo.Partitions))
		resp.Topics[i].Name = topicInfo.Name
		topicNames[i] = *topicInfo.Name
		partitionOffsets[i] = make([]partitionOffset, len(topicInfo.Partitions))
		for j, partitionInfo := range topicInfo.Partitions {
			partitionOffsets[i][j].partitionID = partitionInfo.PartitionIndex
			partitionOffsets[i][j].timestamp = partitionInfo.Timestamp
		}
	}
	for i, topicName := range topicNames {
		topicInfo, topicExists := c.s.metadataProvider.GetTopicInfo(topicName)
		for j, partitionOff := range partitionOffsets[i] {
			resp.Topics[i].Partitions[j].PartitionIndex = partitionOff.partitionID
			if !topicExists {
				resp.Topics[i].Partitions[j].ErrorCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
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
					resp.Topics[i].Partitions[j].ErrorCode = kafkaprotocol.ErrorCodeUnknownServerError
					continue
				}
			} else {
				resOffset, resTimestamp, ok = topicInfo.ConsumerInfoProvider.OffsetByTimestamp(types.NewTimestamp(timestamp), int(partitionOff.partitionID))
			}
			if !ok {
				resp.Topics[i].Partitions[j].ErrorCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
			} else {
				resp.Topics[i].Partitions[j].Offset = resOffset
				resp.Topics[i].Partitions[j].Timestamp = resTimestamp
			}
		}
	}
	return completionFunc(&resp)
}

func (c *connection) FindCoordinatorRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.FindCoordinatorRequest) *kafkaprotocol.FindCoordinatorResponse {
	var resp kafkaprotocol.FindCoordinatorResponse
	resp.ErrorCode = errorCode
	if errorMsg != "" {
		resp.ErrorMessage = &errorMsg
	}
	return &resp
}

func (c *connection) HandleFindCoordinatorRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.FindCoordinatorRequest, completionFunc func(resp *kafkaprotocol.FindCoordinatorResponse) error) error {
	nodeID := c.s.groupCoordinator.FindCoordinator(*req.Key)
	address := c.s.cfg.KafkaServerListenerConfig.Addresses[nodeID]
	if len(c.s.cfg.KafkaServerListenerConfig.AdvertisedAddresses) > 0 {
		address = c.s.cfg.KafkaServerListenerConfig.AdvertisedAddresses[nodeID]
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
	var resp kafkaprotocol.FindCoordinatorResponse
	resp.NodeId = int32(nodeID)
	resp.Host = &host
	resp.Port = int32(port)
	return completionFunc(&resp)
}

func (c *connection) JoinGroupRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.JoinGroupRequest) *kafkaprotocol.JoinGroupResponse {
	var resp kafkaprotocol.JoinGroupResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (c *connection) HandleJoinGroupRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.JoinGroupRequest, completionFunc func(resp *kafkaprotocol.JoinGroupResponse) error) error {
	infos := make([]ProtocolInfo, len(req.Protocols))
	for i, protoInfo := range req.Protocols {
		infos[i] = ProtocolInfo{
			Name:     *protoInfo.Name,
			Metadata: common.ByteSliceCopy(protoInfo.Metadata),
		}
	}
	rebalanceTimeout := 5 * time.Minute
	sessionTimeout := time.Duration(req.SessionTimeoutMs) * time.Millisecond
	c.s.groupCoordinator.JoinGroup(hdr.RequestApiVersion, *req.GroupId, *hdr.ClientId, *req.MemberId, *req.ProtocolType, infos, sessionTimeout, rebalanceTimeout, func(result JoinResult) {
		var resp kafkaprotocol.JoinGroupResponse
		resp.ErrorCode = int16(result.ErrorCode)
		if resp.ErrorCode == kafkaprotocol.ErrorCodeNone {
			resp.GenerationId = int32(result.GenerationID)
			resp.ProtocolName = &result.ProtocolName
			resp.Leader = &result.LeaderMemberID
			resp.MemberId = &result.MemberID
			resp.Members = make([]kafkaprotocol.JoinGroupResponseJoinGroupResponseMember, len(result.Members))
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

func (c *connection) HeartbeatRequestErrorResponse(errorCode int16, _ string, _ *kafkaprotocol.HeartbeatRequest) *kafkaprotocol.HeartbeatResponse {
	var resp kafkaprotocol.HeartbeatResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (c *connection) HandleHeartbeatRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.HeartbeatRequest, completionFunc func(resp *kafkaprotocol.HeartbeatResponse) error) error {
	errorCode := c.s.groupCoordinator.HeartbeatGroup(*req.GroupId, *req.MemberId, int(req.GenerationId))
	var resp kafkaprotocol.HeartbeatResponse
	resp.ErrorCode = int16(errorCode)
	return completionFunc(&resp)
}

func (c *connection) LeaveGroupRequestErrorResponse(errorCode int16, _ string, _ *kafkaprotocol.LeaveGroupRequest) *kafkaprotocol.LeaveGroupResponse {
	var resp kafkaprotocol.LeaveGroupResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (c *connection) HandleLeaveGroupRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.LeaveGroupRequest, completionFunc func(resp *kafkaprotocol.LeaveGroupResponse) error) error {
	leaveInfos := []MemberLeaveInfo{{MemberID: *req.MemberId}}
	errorCode := c.s.groupCoordinator.LeaveGroup(*req.GroupId, leaveInfos)
	var resp kafkaprotocol.LeaveGroupResponse
	resp.ErrorCode = errorCode
	return completionFunc(&resp)
}

func (c *connection) SyncGroupRequestErrorResponse(errorCode int16, _ string, _ *kafkaprotocol.SyncGroupRequest) *kafkaprotocol.SyncGroupResponse {
	var resp kafkaprotocol.SyncGroupResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (c *connection) HandleSyncGroupRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.SyncGroupRequest, completionFunc func(resp *kafkaprotocol.SyncGroupResponse) error) error {
	assignments := make([]AssignmentInfo, len(req.Assignments))
	for i, assignment := range req.Assignments {
		mem := *assignment.MemberId
		assignments[i] = AssignmentInfo{
			MemberID:   mem,
			Assignment: assignment.Assignment,
		}
	}
	c.s.groupCoordinator.SyncGroup(*req.GroupId, *req.MemberId, int(req.GenerationId), assignments, func(errorCode int, assignment []byte) {
		var resp kafkaprotocol.SyncGroupResponse
		resp.ErrorCode = int16(errorCode)
		if resp.ErrorCode == kafkaprotocol.ErrorCodeNone {
			resp.Assignment = assignment
		}
		if err := completionFunc(&resp); err != nil {
			log.Errorf("failed to send sync group response %v", err)
		}
	})
	return nil
}

func (c *connection) ApiVersionsRequestErrorResponse(errorCode int16, _ string, req *kafkaprotocol.ApiVersionsRequest) *kafkaprotocol.ApiVersionsResponse {
	var resp kafkaprotocol.ApiVersionsResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (c *connection) HandleApiVersionsRequest(_ *kafkaprotocol.RequestHeader, _ *kafkaprotocol.ApiVersionsRequest, completionFunc func(resp *kafkaprotocol.ApiVersionsResponse) error) error {
	var resp kafkaprotocol.ApiVersionsResponse
	resp.ApiKeys = kafkaprotocol.SupportedAPIVersions
	return completionFunc(&resp)
}

func (c *connection) HandleInitProducerIdRequest(_ *kafkaprotocol.RequestHeader, _ *kafkaprotocol.InitProducerIdRequest, completionFunc func(resp *kafkaprotocol.InitProducerIdResponse) error) error {
	newPid, err := c.s.sequenceManager.GetNextID(c.s.cfg.SequencesObjectName, 1)
	if err != nil {
		panic(err)
	}
	var resp kafkaprotocol.InitProducerIdResponse
	resp.ProducerId = int64(newPid)
	return completionFunc(&resp)
}

func (c *connection) InitProducerIdRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.InitProducerIdRequest) *kafkaprotocol.InitProducerIdResponse {
	var resp kafkaprotocol.InitProducerIdResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (c *connection) SaslHandshakeRequestErrorResponse(errorCode int16, _ string, req *kafkaprotocol.SaslHandshakeRequest) *kafkaprotocol.SaslHandshakeResponse {
	var resp kafkaprotocol.SaslHandshakeResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (c *connection) HandleSaslHandshakeRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.SaslHandshakeRequest, completionFunc func(resp *kafkaprotocol.SaslHandshakeResponse) error) error {
	var resp kafkaprotocol.SaslHandshakeResponse
	conversation, ok, err := c.s.saslAuthManager.CreateConversation(*req.Mechanism)
	if err != nil {
		return err
	}
	if !ok {
		resp.ErrorCode = kafkaprotocol.ErrorCodeUnsupportedSaslMechanism
	} else {
		c.saslConversation = conversation
	}
	authType := c.s.saslAuthManager.ScramAuthType()
	resp.Mechanisms = []*string{&authType}
	return completionFunc(&resp)
}

func (c *connection) SaslAuthenticateRequestErrorResponse(errorCode int16, _ string, req *kafkaprotocol.SaslAuthenticateRequest) *kafkaprotocol.SaslAuthenticateResponse {
	var resp kafkaprotocol.SaslAuthenticateResponse
	resp.ErrorCode = errorCode
	return &resp
}

func (c *connection) HandleSaslAuthenticateRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.SaslAuthenticateRequest, completionFunc func(resp *kafkaprotocol.SaslAuthenticateResponse) error) error {
	var resp kafkaprotocol.SaslAuthenticateResponse
	conv := c.saslConversation
	if conv == nil {
		resp.ErrorCode = kafkaprotocol.ErrorCodeIllegalSaslState
		msg := "SaslAuthenticateRequest without a preceding SaslAuthenticateRequest"
		resp.ErrorMessage = &msg
	} else {
		saslRespBytes, complete, failed := conv.Process(req.AuthBytes)
		if failed {
			resp.ErrorCode = kafkaprotocol.ErrorCodeSaslAuthenticationFailed
		} else {
			resp.AuthBytes = saslRespBytes
			if complete {
				principal := conv.Principal()
				c.authContext.Principal = &principal
				c.authContext.Authenticated = true
			}
		}
	}
	return completionFunc(&resp)
}

func (c *connection) HandleAddOffsetsToTxnRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.AddOffsetsToTxnRequest, completionFunc func(resp *kafkaprotocol.AddOffsetsToTxnResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleAddPartitionsToTxnRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.AddPartitionsToTxnRequest, completionFunc func(resp *kafkaprotocol.AddPartitionsToTxnResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleTxnOffsetCommitRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.TxnOffsetCommitRequest, completionFunc func(resp *kafkaprotocol.TxnOffsetCommitResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleEndTxnRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.EndTxnRequest, completionFunc func(resp *kafkaprotocol.EndTxnResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandlePutUserCredentialsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.PutUserCredentialsRequest, completionFunc func(resp *kafkaprotocol.PutUserCredentialsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleDeleteUserRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.DeleteUserRequest, completionFunc func(resp *kafkaprotocol.DeleteUserResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleOffsetDeleteRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.OffsetDeleteRequest, completionFunc func(resp *kafkaprotocol.OffsetDeleteResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleListGroupsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.ListGroupsRequest, completionFunc func(resp *kafkaprotocol.ListGroupsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleDescribeGroupsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.DescribeGroupsRequest, completionFunc func(resp *kafkaprotocol.DescribeGroupsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleDeleteGroupsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.DeleteGroupsRequest, completionFunc func(resp *kafkaprotocol.DeleteGroupsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleCreatePartitionsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.CreatePartitionsRequest, completionFunc func(resp *kafkaprotocol.CreatePartitionsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleDescribeConfigsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.DescribeConfigsRequest, completionFunc func(resp *kafkaprotocol.DescribeConfigsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleAlterConfigsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.AlterConfigsRequest, completionFunc func(resp *kafkaprotocol.AlterConfigsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleDescribeClusterRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.DescribeClusterRequest, completionFunc func(resp *kafkaprotocol.DescribeClusterResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleCreateAclsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.CreateAclsRequest, completionFunc func(resp *kafkaprotocol.CreateAclsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleDeleteAclsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.DeleteAclsRequest, completionFunc func(resp *kafkaprotocol.DeleteAclsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (c *connection) HandleDescribeAclsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.DescribeAclsRequest, completionFunc func(resp *kafkaprotocol.DescribeAclsResponse) error) error {
	//TODO implement me
	panic("implement me")
}
