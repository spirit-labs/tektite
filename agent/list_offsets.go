package agent

import (
	"github.com/spirit-labs/tektite/acls"
	auth "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/offsets"
)

func (a *Agent) HandleListOffsetsRequest(authContext *auth.Context, req *kafkaprotocol.ListOffsetsRequest) *kafkaprotocol.ListOffsetsResponse {
	resp, err := a.handleListOffsetsRequest(authContext, req)
	if err != nil {
		// Send back unknown topic for unavailable error as client will retry
		errCode := kafkaencoding.ErrorCodeForError(err, kafkaprotocol.ErrorCodeUnknownTopicOrPartition)
		for i := 0; i < len(resp.Topics); i++ {
			for j := 0; j < len(resp.Topics[i].Partitions); j++ {
				resp.Topics[i].Partitions[j].ErrorCode = errCode
				resp.Topics[i].Partitions[j].Offset = 0
				resp.Topics[i].Partitions[j].Timestamp = 0

			}
		}
	}
	return resp
}

func (a *Agent) handleListOffsetsRequest(authContext *auth.Context, req *kafkaprotocol.ListOffsetsRequest) (*kafkaprotocol.ListOffsetsResponse, error) {
	client, err := a.controlClientCache.GetClient()
	if err != nil {
		return nil, err
	}
	var resp kafkaprotocol.ListOffsetsResponse
	resp.Topics = make([]kafkaprotocol.ListOffsetsResponseListOffsetsTopicResponse, len(req.Topics))
	for i, topicInfo := range req.Topics {
		resp.Topics[i].Partitions = make([]kafkaprotocol.ListOffsetsResponseListOffsetsPartitionResponse, len(topicInfo.Partitions))
		resp.Topics[i].Name = topicInfo.Name
		for j, partition := range topicInfo.Partitions {
			resp.Topics[i].Partitions[j].PartitionIndex = partition.PartitionIndex
		}
	}
	getOffsetRequests := make([]offsets.GetOffsetTopicInfo, 0, len(req.Topics))
	type respIndex struct {
		topicIndex int
		partIndex  int
	}
	var respIndexes []respIndex
	for i, topicInfo := range req.Topics {
		topicName := common.SafeDerefStringPtr(topicInfo.Name)
		info, exists, err := a.topicMetaCache.GetTopicInfo(topicName)
		if err != nil {
			return &resp, err
		}
		errCode := int16(kafkaprotocol.ErrorCodeNone)
		if !exists {
			log.Warnf("list_offsets: unknown topic: %s", topicName)
			errCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
		} else if authContext != nil {
			authorised, err := authContext.Authorize(acls.ResourceTypeTopic, topicName, acls.OperationDescribe)
			if err != nil {
				return &resp, err
			}
			if !authorised {
				errCode = kafkaprotocol.ErrorCodeTopicAuthorizationFailed
			}
		}
		var getOffsetTopicInfo offsets.GetOffsetTopicInfo
		for j, partInfo := range topicInfo.Partitions {
			if errCode != int16(kafkaprotocol.ErrorCodeNone) {
				resp.Topics[i].Partitions[j].ErrorCode = errCode
			} else if partInfo.Timestamp == -2 || partInfo.Timestamp == -4 {
				// earliest - just return zero - this should be ok as if first offset is > 0 then iterator will
				// just skip to that on fetch
				resp.Topics[i].Partitions[j].Offset = 0
			} else if partInfo.Timestamp == -1 {
				// latest
				getOffsetTopicInfo.PartitionIDs = append(getOffsetTopicInfo.PartitionIDs, int(partInfo.PartitionIndex))
				respIndexes = append(respIndexes, respIndex{
					topicIndex: i,
					partIndex:  j,
				})
			} else {
				// by timestamp
				// TODO currently not supported
				return &resp, &kafkaencoding.KafkaError{
					ErrorCode: kafkaprotocol.ErrorCodeInvalidRequest,
					ErrorMsg:  "list offsets by timestamp currently not supported",
				}
			}
		}
		if exists {
			getOffsetTopicInfo.TopicID = info.ID
			getOffsetRequests = append(getOffsetRequests, getOffsetTopicInfo)
		}
	}
	// Actually get the offsets
	offs, err := client.GetOffsetInfos(getOffsetRequests)
	if err != nil {
		return &resp, err
	}
	// fill in the results
	k := 0
	for _, topicOff := range offs {
		for _, partOff := range topicOff.PartitionInfos {
			respInd := respIndexes[k]
			resp.Topics[respInd.topicIndex].Partitions[respInd.partIndex].Offset = partOff.Offset
			k++
		}
	}
	return &resp, nil
}
