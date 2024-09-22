package agent

import (
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/kafkaserver2"
)

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

func (k *kafkaHandler) HandleProduceRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.ProduceRequest, completionFunc func(resp *kafkaprotocol.ProduceResponse) error) error {
	return k.agent.tablePusher.HandleProduceRequest(req, completionFunc)
}

func (k *kafkaHandler) ProduceRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.ProduceRequest) *kafkaprotocol.ProduceResponse {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleFetchRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.FetchRequest, completionFunc func(resp *kafkaprotocol.FetchResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) FetchRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.FetchRequest) *kafkaprotocol.FetchResponse {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleListOffsetsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.ListOffsetsRequest, completionFunc func(resp *kafkaprotocol.ListOffsetsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) ListOffsetsRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.ListOffsetsRequest) *kafkaprotocol.ListOffsetsResponse {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleMetadataRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.MetadataRequest, completionFunc func(resp *kafkaprotocol.MetadataResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) MetadataRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.MetadataRequest) *kafkaprotocol.MetadataResponse {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleOffsetCommitRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.OffsetCommitRequest, completionFunc func(resp *kafkaprotocol.OffsetCommitResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) OffsetCommitRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.OffsetCommitRequest) *kafkaprotocol.OffsetCommitResponse {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleOffsetFetchRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.OffsetFetchRequest, completionFunc func(resp *kafkaprotocol.OffsetFetchResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) OffsetFetchRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.OffsetFetchRequest) *kafkaprotocol.OffsetFetchResponse {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleFindCoordinatorRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.FindCoordinatorRequest, completionFunc func(resp *kafkaprotocol.FindCoordinatorResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) FindCoordinatorRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.FindCoordinatorRequest) *kafkaprotocol.FindCoordinatorResponse {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleJoinGroupRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.JoinGroupRequest, completionFunc func(resp *kafkaprotocol.JoinGroupResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) JoinGroupRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.JoinGroupRequest) *kafkaprotocol.JoinGroupResponse {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleHeartbeatRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.HeartbeatRequest, completionFunc func(resp *kafkaprotocol.HeartbeatResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HeartbeatRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.HeartbeatRequest) *kafkaprotocol.HeartbeatResponse {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleLeaveGroupRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.LeaveGroupRequest, completionFunc func(resp *kafkaprotocol.LeaveGroupResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) LeaveGroupRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.LeaveGroupRequest) *kafkaprotocol.LeaveGroupResponse {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleSyncGroupRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.SyncGroupRequest, completionFunc func(resp *kafkaprotocol.SyncGroupResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) SyncGroupRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.SyncGroupRequest) *kafkaprotocol.SyncGroupResponse {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleApiVersionsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.ApiVersionsRequest, completionFunc func(resp *kafkaprotocol.ApiVersionsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) ApiVersionsRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.ApiVersionsRequest) *kafkaprotocol.ApiVersionsResponse {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleInitProducerIdRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.InitProducerIdRequest, completionFunc func(resp *kafkaprotocol.InitProducerIdResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) InitProducerIdRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.InitProducerIdRequest) *kafkaprotocol.InitProducerIdResponse {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleSaslAuthenticateRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.SaslAuthenticateRequest, completionFunc func(resp *kafkaprotocol.SaslAuthenticateResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) SaslAuthenticateRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.SaslAuthenticateRequest) *kafkaprotocol.SaslAuthenticateResponse {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) HandleSaslHandshakeRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.SaslHandshakeRequest, completionFunc func(resp *kafkaprotocol.SaslHandshakeResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (k *kafkaHandler) SaslHandshakeRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.SaslHandshakeRequest) *kafkaprotocol.SaslHandshakeResponse {
	//TODO implement me
	panic("implement me")
}
