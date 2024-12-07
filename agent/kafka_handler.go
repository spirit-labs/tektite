package agent

import (
	"fmt"
	"github.com/pkg/errors"
	auth "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/kafkaserver2"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/topicmeta"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var validTopicChars = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

const maxNameLength = 249

func (a *Agent) newKafkaHandler(ctx kafkaserver2.ConnectionContext) kafkaprotocol.RequestHandler {
	return &kafkaHandler{
		agent:       a,
		authContext: ctx.AuthContext(),
		clientHost:  ctx.ClientHost(),
	}
}

type kafkaHandler struct {
	agent            *Agent
	saslConversation auth.SaslConversation
	authContext      *auth.Context
	clientHost       string
}

func extractErrorCode(err error) common.ErrCode {
	var tektiteErr common.TektiteError
	if errors.As(err, &tektiteErr) {
		return tektiteErr.Code
	}
	return kafkaprotocol.ErrorCodeUnknownServerError
}

func (k *kafkaHandler) HandleProduceRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.ProduceRequest,
	completionFunc func(resp *kafkaprotocol.ProduceResponse) error) error {
	return k.agent.tablePusher.HandleProduceRequest(req, completionFunc)
}

func (k *kafkaHandler) HandleFetchRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.FetchRequest,
	completionFunc func(resp *kafkaprotocol.FetchResponse) error) error {
	return k.agent.batchFetcher.HandleFetchRequest(hdr.RequestApiVersion, req, completionFunc)
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
	return k.agent.groupCoordinator.HandleJoinGroupRequest(k.clientHost, hdr, req, completionFunc)
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

func (k *kafkaHandler) HandleApiVersionsRequest(_ *kafkaprotocol.RequestHeader, _ *kafkaprotocol.ApiVersionsRequest,
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
	var resp kafkaprotocol.SaslAuthenticateResponse
	conv := k.saslConversation
	if conv == nil {
		resp.ErrorCode = kafkaprotocol.ErrorCodeIllegalSaslState
		msg := "SaslAuthenticateRequest without a preceding SaslAuthenticateRequest"
		resp.ErrorMessage = &msg
	} else {
		reqBytes := req.AuthBytes
		sc, isSCram := conv.(*auth.ScramConversation)
		if isSCram && k.agent.cfg.AddJunkOnScramNonce && sc.Step() == 1 {
			log.Warnf("Testing: Adding Junk to SCRAM nonce")
			reqBytes = addJunkToScramNonce(reqBytes)
		}
		saslRespBytes, complete, failed := conv.Process(reqBytes)
		if failed {
			resp.ErrorCode = kafkaprotocol.ErrorCodeSaslAuthenticationFailed
		} else {
			resp.AuthBytes = saslRespBytes
			if complete {
				principal := conv.Principal()
				k.authContext.Principal = principal
				k.authContext.Authenticated = true
			}
		}
	}
	return completionFunc(&resp)
}

func addJunkToScramNonce(reqBytes []byte) []byte {
	// Used in testing only. We add some junk to the nonce
	sRequest := string(reqBytes)
	fields := strings.Split(sRequest, ",")
	var newRequest strings.Builder
	for i, field := range fields {
		if i == 1 {
			nonce := strings.TrimPrefix(field, "r=")
			newRequest.WriteString("r=" + nonce + "-some-junk")
		} else {
			newRequest.WriteString(field)
		}
		if i != len(fields)-1 {
			newRequest.WriteRune(',')
		}
	}
	return []byte(newRequest.String())
}

func (k *kafkaHandler) HandleSaslHandshakeRequest(_ *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.SaslHandshakeRequest,
	completionFunc func(resp *kafkaprotocol.SaslHandshakeResponse) error) error {
	var resp kafkaprotocol.SaslHandshakeResponse
	conversation, ok, err := k.agent.saslAuthManager.CreateConversation(*req.Mechanism)
	if err != nil {
		return err
	}
	if !ok {
		resp.ErrorCode = kafkaprotocol.ErrorCodeUnsupportedSaslMechanism
	} else {
		k.saslConversation = conversation
	}
	resp.Mechanisms = []*string{&plain, &sha512}
	return completionFunc(&resp)
}

var plain = auth.AuthenticationSaslPlain
var sha512 = auth.AuthenticationSaslScramSha512

func (k *kafkaHandler) HandlePutUserCredentialsRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.PutUserCredentialsRequest, completionFunc func(resp *kafkaprotocol.PutUserCredentialsResponse) error) error {
	// TODO only allow admin??
	var resp kafkaprotocol.PutUserCredentialsResponse
	cl, err := k.agent.controlClientCache.GetClient()
	setErrorForPutUserResponse(err, &resp)
	if err == nil {
		username := common.SafeDerefStringPtr(req.Username)
		salt := common.SafeDerefStringPtr(req.Salt)
		err = cl.PutUserCredentials(username, req.StoredKey, req.ServerKey, salt, int(req.Iters))
		setErrorForPutUserResponse(err, &resp)
	}
	return completionFunc(&resp)
}

func setErrorForPutUserResponse(err error, resp *kafkaprotocol.PutUserCredentialsResponse) {
	if err != nil {
		resp.ErrorMessage = common.StrPtr(err.Error())
		if common.IsUnavailableError(err) {
			resp.ErrorCode = kafkaprotocol.ErrorCodeCoordinatorNotAvailable
		} else {
			resp.ErrorCode = kafkaprotocol.ErrorCodeUnknownServerError
		}
	}
}

func (k *kafkaHandler) HandleDeleteUserRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.DeleteUserRequest,
	completionFunc func(resp *kafkaprotocol.DeleteUserResponse) error) error {
	// TODO only allow admin??
	var resp kafkaprotocol.DeleteUserResponse
	cl, err := k.agent.controlClientCache.GetClient()
	setErrorForDeleteUserResponse(err, &resp)
	if err == nil {
		username := common.SafeDerefStringPtr(req.Username)
		err = cl.DeleteUserCredentials(username)
		setErrorForDeleteUserResponse(err, &resp)
	}
	return completionFunc(&resp)
}

func setErrorForDeleteUserResponse(err error, resp *kafkaprotocol.DeleteUserResponse) {
	if err != nil {
		resp.ErrorMessage = common.StrPtr(err.Error())
		if common.IsUnavailableError(err) {
			resp.ErrorCode = kafkaprotocol.ErrorCodeCoordinatorNotAvailable
		} else if common.IsTektiteErrorWithCode(err, common.NoSuchUser) {
			resp.ErrorCode = kafkaprotocol.ErrorCodeNoSuchUser
			resp.ErrorMessage = common.StrPtr(err.Error())
		} else {
			resp.ErrorCode = kafkaprotocol.ErrorCodeUnknownServerError
		}
	}
}

func (k *kafkaHandler) HandleOffsetDeleteRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.OffsetDeleteRequest, completionFunc func(resp *kafkaprotocol.OffsetDeleteResponse) error) error {
	resp, err := k.agent.groupCoordinator.OffsetDelete(req)
	if err != nil {
		return err
	}
	return completionFunc(resp)
}

func (k *kafkaHandler) HandleListGroupsRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.ListGroupsRequest, completionFunc func(resp *kafkaprotocol.ListGroupsResponse) error) error {
	resp, err := k.agent.groupCoordinator.ListGroups(req)
	if err != nil {
		return err
	}
	return completionFunc(resp)
}

func (k *kafkaHandler) HandleDescribeGroupsRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.DescribeGroupsRequest, completionFunc func(resp *kafkaprotocol.DescribeGroupsResponse) error) error {
	resp, err := k.agent.groupCoordinator.DescribeGroups(req)
	if err != nil {
		return err
	}
	return completionFunc(resp)
}

func (k *kafkaHandler) HandleDeleteGroupsRequest(_ *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.DeleteGroupsRequest, completionFunc func(resp *kafkaprotocol.DeleteGroupsResponse) error) error {
	resp, err := k.agent.groupCoordinator.DeleteGroups(req)
	if err != nil {
		return err
	}
	return completionFunc(resp)
}

func (k *kafkaHandler) HandleCreatePartitionsRequest(_ *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.CreatePartitionsRequest, completionFunc func(resp *kafkaprotocol.CreatePartitionsResponse) error) error {
	resp := kafkaprotocol.CreatePartitionsResponse{
		Results: make([]kafkaprotocol.CreatePartitionsResponseCreatePartitionsTopicResult, len(req.Topics)),
	}
	for i := 0; i < len(req.Topics); i++ {
		resp.Results[i].Name = req.Topics[i].Name
	}
	for i, topic := range req.Topics {
		cl, err := k.agent.controlClientCache.GetClient()
		if err != nil {
			errCode, errMsg := getErrorCodeAndMessageForCreatePartitionsResponse(err)
			resp.Results[i].ErrorCode = errCode
			resp.Results[i].ErrorMessage = common.StrPtr(errMsg)
			continue
		}
		topicName := common.SafeDerefStringPtr(topic.Name)
		if req.ValidateOnly {
			info, _, exists, err := cl.GetTopicInfo(topicName)
			if err != nil {
				errCode, errMsg := getErrorCodeAndMessageForCreatePartitionsResponse(err)
				resp.Results[i].ErrorCode = errCode
				resp.Results[i].ErrorMessage = common.StrPtr(errMsg)
			} else {
				if !exists {
					resp.Results[i].ErrorCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
					resp.Results[i].ErrorMessage = common.StrPtr(fmt.Sprintf("unknown topic: %s", topicName))
				} else {
					if int(topic.Count) < info.PartitionCount {
						resp.Results[i].ErrorCode = kafkaprotocol.ErrorCodeInvalidPartitions
						resp.Results[i].ErrorMessage = common.StrPtr("cannot reduce partition count")
					}
				}
			}
		} else {
			if err := cl.CreateOrUpdateTopic(topicmeta.TopicInfo{
				Name:           common.SafeDerefStringPtr(topic.Name),
				PartitionCount: int(topic.Count),
			}, false); err != nil {
				errCode, errMsg := getErrorCodeAndMessageForCreatePartitionsResponse(err)
				resp.Results[i].ErrorCode = errCode
				resp.Results[i].ErrorMessage = common.StrPtr(errMsg)
			}
		}
	}
	return completionFunc(&resp)
}

func getErrorCodeAndMessageForCreatePartitionsResponse(err error) (int16, string) {
	errMsg := err.Error()
	var errCode int16
	if common.IsUnavailableError(err) {
		errCode = kafkaprotocol.ErrorCodeCoordinatorNotAvailable
	} else if common.IsTektiteErrorWithCode(err, common.TopicDoesNotExist) {
		errCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
	} else if common.IsTektiteErrorWithCode(err, common.PartitionOutOfRange) {
		errCode = kafkaprotocol.ErrorCodeInvalidPartitions
	} else if common.IsTektiteErrorWithCode(err, common.InvalidPartitionCount) {
		errCode = kafkaprotocol.ErrorCodeInvalidPartitions
	} else {
		errCode = kafkaprotocol.ErrorCodeUnknownServerError
	}
	return errCode, errMsg
}

func (k *kafkaHandler) HandleCreateTopicsRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.CreateTopicsRequest,
	completionFunc func(resp *kafkaprotocol.CreateTopicsResponse) error) error {
	resp := &kafkaprotocol.CreateTopicsResponse{
		ThrottleTimeMs: 0,
		Topics:         make([]kafkaprotocol.CreateTopicsResponseCreatableTopicResult, len(req.Topics)),
	}
	for i, topic := range req.Topics {
		topicName := common.SafeDerefStringPtr(topic.Name)
		retentionTime, respConfigs, errCode, errMsg := k.parseRetentionConfig(topic, topicName)
		if errCode == kafkaprotocol.ErrorCodeNone { // No error from config parsing
			errCode, errMsg = k.validateAndCreateTopic(topicName, topic, retentionTime)
		}
		res := kafkaprotocol.CreateTopicsResponseCreatableTopicResult{
			Name:          topic.Name,
			ErrorCode:     errCode,
			NumPartitions: topic.NumPartitions,
			Configs:       respConfigs,
		}
		if errCode != kafkaprotocol.ErrorCodeNone {
			res.ErrorMessage = &errMsg
		}
		resp.Topics[i] = res
	}
	return completionFunc(resp)
}

func (k *kafkaHandler) HandleDeleteTopicsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.DeleteTopicsRequest, completionFunc func(resp *kafkaprotocol.DeleteTopicsResponse) error) error {
	resp := &kafkaprotocol.DeleteTopicsResponse{
		ThrottleTimeMs: 0,
		Responses:      make([]kafkaprotocol.DeleteTopicsResponseDeletableTopicResult, len(req.TopicNames)),
	}
	for i, topicName := range req.TopicNames {
		var errMsg string
		errCode := kafkaprotocol.ErrorCodeNone
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
		res := kafkaprotocol.DeleteTopicsResponseDeletableTopicResult{
			Name:      topicName,
			ErrorCode: int16(errCode),
		}
		if errCode != kafkaprotocol.ErrorCodeNone {
			res.ErrorMessage = &errMsg
		}
		resp.Responses[i] = res
	}
	return completionFunc(resp)
}

func checkTopicNameValid(name string) error {
	if len(name) == 0 {
		return errors.New("topic name cannot be an empty string")
	}
	if name == "." {
		return errors.New("'.' is not allowed for topic name")
	}
	if name == ".." {
		return errors.New("'..' is not allowed for topic name")
	}
	if len(name) > maxNameLength {
		return fmt.Errorf("the length of '%s' is longer than the max allowed length %d", name, maxNameLength)
	}
	if !validTopicChars.MatchString(name) {
		return fmt.Errorf("'%s' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'", name)
	}
	return nil
}

func (k *kafkaHandler) validateAndCreateTopic(topicName string, topic kafkaprotocol.CreateTopicsRequestCreatableTopic,
	retentionTime time.Duration) (int16, string) {
	err := checkTopicNameValid(topicName)
	if err != nil {
		return int16(kafkaprotocol.ErrorCodeInvalidTopicException),
			fmt.Sprintf("invalid topic: %s (reason: %s)", topicName, err.Error())
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
	err = acl.CreateOrUpdateTopic(topicInfo, true)
	if err != nil {
		if extractErrorCode(err) == common.TopicAlreadyExists {
			return kafkaprotocol.ErrorCodeTopicAlreadyExists, err.Error()
		}
		return kafkaprotocol.ErrorCodeInvalidTopicException, err.Error()
	}
	return int16(kafkaprotocol.ErrorCodeNone), ""
}

func isValidRetentionTime(retentionMs int) bool {
	return retentionMs > 0 || retentionMs == -1
}

func (k *kafkaHandler) parseRetentionConfig(topic kafkaprotocol.CreateTopicsRequestCreatableTopic,
	topicName string) (time.Duration, []kafkaprotocol.CreateTopicsResponseCreatableTopicConfigs, int16, string) {
	retentionTime := k.agent.cfg.DefaultTopicRetentionTime
	respConfigs := make([]kafkaprotocol.CreateTopicsResponseCreatableTopicConfigs, len(topic.Configs))
	errCode := int16(kafkaprotocol.ErrorCodeNone)
	var errMsg string
	for i, config := range topic.Configs {
		if common.SafeDerefStringPtr(config.Name) == "retention.ms" {
			retentionMs, err := strconv.Atoi(common.SafeDerefStringPtr(config.Value))
			if err != nil {
				errCode = kafkaprotocol.ErrorCodeInvalidTopicException
				errMsg = fmt.Sprintf("Invalid retention time for topic: %s", topicName)
			} else {
				if isValidRetentionTime(retentionMs) {
					retentionTime = time.Duration(retentionMs) * time.Millisecond
				} else {
					errCode = int16(kafkaprotocol.ErrorCodeInvalidTopicException)
					errMsg = fmt.Sprintf("Invalid retention time for topic: %s", topicName)
				}
			}
		}
		respConfigs[i] = kafkaprotocol.CreateTopicsResponseCreatableTopicConfigs{
			Name:  config.Name,
			Value: config.Value,
		}
	}
	return retentionTime, respConfigs, errCode, errMsg
}

func (k *kafkaHandler) HandleDescribeConfigsRequest(hdr *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.DescribeConfigsRequest, completionFunc func(resp *kafkaprotocol.DescribeConfigsResponse) error) error {
	resp := &kafkaprotocol.DescribeConfigsResponse{
		Results: make([]kafkaprotocol.DescribeConfigsResponseDescribeConfigsResult, len(req.Resources)),
	}
	// We currently do not return any configs via the api
	for i, resource := range req.Resources {
		resp.Results[i].ResourceType = resource.ResourceType
		resp.Results[i].ResourceName = resource.ResourceName
		resp.Results[i].Configs = []kafkaprotocol.DescribeConfigsResponseDescribeConfigsResourceResult{}
	}
	return completionFunc(resp)
}

func (k *kafkaHandler) HandleAlterConfigsRequest(hdr *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.AlterConfigsRequest, completionFunc func(resp *kafkaprotocol.AlterConfigsResponse) error) error {
	resp := &kafkaprotocol.AlterConfigsResponse{
		Responses: make([]kafkaprotocol.AlterConfigsResponseAlterConfigsResourceResponse, len(req.Resources)),
	}
	// We currently do not return any configs via the api
	for i, resource := range req.Resources {
		resp.Responses[i].ResourceType = resource.ResourceType
		resp.Responses[i].ResourceName = resource.ResourceName
		resp.Responses[i].ErrorCode = kafkaprotocol.ErrorCodeInvalidConfig
		resp.Responses[i].ErrorMessage = common.StrPtr("changing config not supported")
	}
	return completionFunc(resp)
}

func (k *kafkaHandler) HandleDescribeClusterRequest(_ *kafkaprotocol.RequestHeader,
	_ *kafkaprotocol.DescribeClusterRequest, completionFunc func(resp *kafkaprotocol.DescribeClusterResponse) error) error {
	return k.agent.HandleDescribeClusterRequest(completionFunc)
}
