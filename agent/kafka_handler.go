package agent

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/kafkaserver2"
	"github.com/spirit-labs/tektite/topicmeta"
)

var validTopicChars = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

const maxNameLength = 249

func (a *Agent) newKafkaHandler(ctx kafkaserver2.ConnectionContext) kafkaprotocol.RequestHandler {
	return &kafkaHandler{
		agent: a,
		ctx:   ctx,
	}
}

type kafkaHandler struct {
	agent *Agent
	ctx   kafkaserver2.ConnectionContext
}

func extractErrorCode(err error) common.ErrCode {
	var tektiteErr common.TektiteError
	if errors.As(err, &tektiteErr) {
		return tektiteErr.Code
	}
	return kafkaprotocol.ErrorCodeUnknownServerError
}

func isInvalidTopicName(name string) error {
	// Check if the name is empty
	if len(name) == 0 {
		return errors.New("the empty string is not allowed")
	}
	// Check if the name is "." or ".."
	if name == "." {
		return errors.New("'.' is not allowed")
	}
	if name == ".." {
		return errors.New("'..' is not allowed")
	}
	// Check the length of the name
	if len(name) > maxNameLength {
		return fmt.Errorf("the length of '%s' is longer than the max allowed length %d", name, maxNameLength)
	}
	// Check if the name contains only valid characters
	if !validTopicChars.MatchString(name) {
		return fmt.Errorf("'%s' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'", name)
	}
	return nil
}

func (k *kafkaHandler) validateAndCreateTopic(topicName string, topic kafkaprotocol.CreateTopicsRequestCreatableTopic, retentionTime time.Duration) (int16, string) {
	err := isInvalidTopicName(topicName)
	if err != nil {
		return int16(kafkaprotocol.ErrorCodeInvalidTopicException), fmt.Sprintf("Invalid topic: %s (Reason: %s)", topicName, err.Error())
	}

	acl, err := k.agent.controlClientCache.GetClient()
	if err != nil {
		return kafkaprotocol.ErrorCodeCoordinatorNotAvailable, err.Error()
	}

	topicInfo := topicmeta.TopicInfo{
		Name:           topicName,
		PartitionCount: int(topic.NumPartitions),
		RetentionTime:  retentionTime,
	}

	err = acl.CreateTopic(topicInfo)
	if err != nil {
		if extractErrorCode(err) == common.TopicAlreadyExists {
			return kafkaprotocol.ErrorCodeTopicAlreadyExists, err.Error()
		}
		return kafkaprotocol.ErrorCodeInvalidTopicException, err.Error()
	}

	return 0, ""
}

func isValidRetentionTime(retentionMs int) bool {
	return retentionMs > 0 || retentionMs == -1
}

func (k *kafkaHandler) parseRetentionConfig(topic kafkaprotocol.CreateTopicsRequestCreatableTopic, topicName string) (time.Duration, []kafkaprotocol.CreateTopicsResponseCreatableTopicConfigs, int16, string) {
	retentionTime := k.agent.cfg.DefaultDefaultTopicRetentionTime
	respConfigs := make([]kafkaprotocol.CreateTopicsResponseCreatableTopicConfigs, len(topic.Configs))
	errCode := int16(0)
	var errMsg string

	for cidx, config := range topic.Configs {
		if common.SafeDerefStringPtr(config.Name) == "retention.ms" {
			retentionMs, err := strconv.Atoi(common.SafeDerefStringPtr(config.Value))
			if err == nil && isValidRetentionTime(retentionMs) {
				retentionTime = time.Duration(retentionMs) * time.Millisecond
			} else {
				errCode = int16(kafkaprotocol.ErrorCodeUnknownServerError)
				errMsg = fmt.Sprintf("Invalid retention time for topic: %s", topicName)
			}
		}
		respConfigs[cidx] = kafkaprotocol.CreateTopicsResponseCreatableTopicConfigs{
			Name:  config.Name,
			Value: config.Value,
		}
	}
	return retentionTime, respConfigs, errCode, errMsg
}

func (k *kafkaHandler) HandleCreateTopicsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.CreateTopicsRequest, completionFunc func(resp *kafkaprotocol.CreateTopicsResponse) error) error {
	resp := &kafkaprotocol.CreateTopicsResponse{
		ThrottleTimeMs: 0,
		Topics:         make([]kafkaprotocol.CreateTopicsResponseCreatableTopicResult, len(req.Topics)),
	}

	for tidx, topic := range req.Topics {
		derefTopicName := common.SafeDerefStringPtr(topic.Name)
		retentionTime, respConfigs, errCode, errMsg := k.parseRetentionConfig(topic, derefTopicName)

		if errCode == 0 { // No error from config parsing
			errCode, errMsg = k.validateAndCreateTopic(derefTopicName, topic, retentionTime)
		}

		resp.Topics[tidx] = kafkaprotocol.CreateTopicsResponseCreatableTopicResult{
			Name:          topic.Name,
			ErrorCode:     errCode,
			ErrorMessage:  &errMsg,
			NumPartitions: topic.NumPartitions,
			Configs:       respConfigs,
		}
	}

	return completionFunc(resp)
}

func (k *kafkaHandler) HandleDeleteTopicsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.DeleteTopicsRequest, completionFunc func(resp *kafkaprotocol.DeleteTopicsResponse) error) error {
	resp := &kafkaprotocol.DeleteTopicsResponse{
		ThrottleTimeMs: 0,
		Responses:      make([]kafkaprotocol.DeleteTopicsResponseDeletableTopicResult, len(req.TopicNames)),
	}
	for tidx, topicName := range req.TopicNames {
		var errMsg string
		errCode := int16(0)
		acl, err := k.agent.controlClientCache.GetClient()
		if err != nil {
			errMsg = err.Error()
			errCode = kafkaprotocol.ErrorCodeCoordinatorNotAvailable
		} else {
			err = acl.DeleteTopic(common.SafeDerefStringPtr(topicName))
			if err != nil {
				errMsg = err.Error()
				if extractErrorCode(err) == common.TopicDoesNotExist {
					errCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
				} else {
					errCode = kafkaprotocol.ErrorCodeInvalidTopicException
				}
			}
		}
		resp.Responses[tidx] = kafkaprotocol.DeleteTopicsResponseDeletableTopicResult{
			Name:         topicName,
			ErrorCode:    errCode,
			ErrorMessage: &errMsg,
		}
	}
	return completionFunc(resp)
}

func (k *kafkaHandler) HandleProduceRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.ProduceRequest,
	completionFunc func(resp *kafkaprotocol.ProduceResponse) error) error {
	return k.agent.tablePusher.HandleProduceRequest(req, completionFunc)
}

func (k *kafkaHandler) HandleFetchRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.FetchRequest,
	completionFunc func(resp *kafkaprotocol.FetchResponse) error) error {
	return k.agent.batchFetcher.HandleFetchRequest(req, completionFunc)
}

func (k *kafkaHandler) HandleListOffsetsRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.ListOffsetsRequest,
	completionFunc func(resp *kafkaprotocol.ListOffsetsResponse) error) error {
	return completionFunc(k.agent.HandleListOffsetsRequest(req))
}

func (k *kafkaHandler) HandleMetadataRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.MetadataRequest,
	completionFunc func(resp *kafkaprotocol.MetadataResponse) error) error {
	resp, err := k.agent.HandleMetadataRequest(hdr, req)
	if err != nil {
		return err
	}
	return completionFunc(resp)
}

func (k *kafkaHandler) HandleOffsetCommitRequest(_ *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.OffsetCommitRequest, completionFunc func(resp *kafkaprotocol.OffsetCommitResponse) error) error {
	resp, err := k.agent.groupCoordinator.OffsetCommit(req)
	if err != nil {
		return err
	}
	return completionFunc(resp)
}

func (k *kafkaHandler) HandleOffsetFetchRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.OffsetFetchRequest,
	completionFunc func(resp *kafkaprotocol.OffsetFetchResponse) error) error {
	resp, err := k.agent.groupCoordinator.OffsetFetch(req)
	if err != nil {
		return err
	}
	return completionFunc(resp)
}

func (k *kafkaHandler) HandleFindCoordinatorRequest(_ *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.FindCoordinatorRequest,
	completionFunc func(resp *kafkaprotocol.FindCoordinatorResponse) error) error {
	return k.agent.groupCoordinator.HandleFindCoordinatorRequest(req, completionFunc)
}

func (k *kafkaHandler) HandleJoinGroupRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.JoinGroupRequest,
	completionFunc func(resp *kafkaprotocol.JoinGroupResponse) error) error {
	return k.agent.groupCoordinator.HandleJoinGroupRequest(hdr, req, completionFunc)
}

func (k *kafkaHandler) HandleHeartbeatRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.HeartbeatRequest,
	completionFunc func(resp *kafkaprotocol.HeartbeatResponse) error) error {
	return k.agent.groupCoordinator.HandleHeartbeatRequest(req, completionFunc)
}

func (k *kafkaHandler) HandleLeaveGroupRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.LeaveGroupRequest,
	completionFunc func(resp *kafkaprotocol.LeaveGroupResponse) error) error {
	return k.agent.groupCoordinator.HandleLeaveGroupRequest(req, completionFunc)
}

func (k *kafkaHandler) HandleSyncGroupRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.SyncGroupRequest,
	completionFunc func(resp *kafkaprotocol.SyncGroupResponse) error) error {
	return k.agent.groupCoordinator.HandleSyncGroupRequest(req, completionFunc)
}

func (k *kafkaHandler) HandleApiVersionsRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.ApiVersionsRequest,
	completionFunc func(resp *kafkaprotocol.ApiVersionsResponse) error) error {
	var resp kafkaprotocol.ApiVersionsResponse
	resp.ApiKeys = kafkaprotocol.SupportedAPIVersions
	return completionFunc(&resp)
}

func (k *kafkaHandler) HandleInitProducerIdRequest(_ *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.InitProducerIdRequest, completionFunc func(resp *kafkaprotocol.InitProducerIdResponse) error) error {
	return completionFunc(k.agent.txCoordinator.HandleInitProducerID(req))
}

func (k *kafkaHandler) HandleAddOffsetsToTxnRequest(_ *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.AddOffsetsToTxnRequest, completionFunc func(resp *kafkaprotocol.AddOffsetsToTxnResponse) error) error {
	return completionFunc(k.agent.txCoordinator.HandleAddOffsetsToTxn(req))
}

func (k *kafkaHandler) HandleAddPartitionsToTxnRequest(_ *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.AddPartitionsToTxnRequest, completionFunc func(resp *kafkaprotocol.AddPartitionsToTxnResponse) error) error {
	return completionFunc(k.agent.txCoordinator.HandleAddPartitionsToTxn(req))
}

func (k *kafkaHandler) HandleTxnOffsetCommitRequest(_ *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.TxnOffsetCommitRequest, completionFunc func(resp *kafkaprotocol.TxnOffsetCommitResponse) error) error {
	resp, err := k.agent.groupCoordinator.OffsetCommitTransactional(req)
	if err != nil {
		return err
	}
	return completionFunc(resp)
}

func (k *kafkaHandler) HandleEndTxnRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.EndTxnRequest,
	completionFunc func(resp *kafkaprotocol.EndTxnResponse) error) error {
	return completionFunc(k.agent.txCoordinator.HandleEndTxn(req))
}

func (k *kafkaHandler) HandleSaslAuthenticateRequest(_ *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.SaslAuthenticateRequest,
	completionFunc func(resp *kafkaprotocol.SaslAuthenticateResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleSaslHandshakeRequest(_ *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.SaslHandshakeRequest,
	completionFunc func(resp *kafkaprotocol.SaslHandshakeResponse) error) error {
	//TODO implement me
	panic("implement me")
}
