package kafkaserver2

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"net"
	"sync"
	"testing"
)

func init() {
	common.EnableTestPorts()
}

/*
The tests here are simple smoke-style tests which check whether an API request can be handled - they do not check
all the versions of the requests/responses. A deeper protocol test is done in kafkaprotocol/protocol_test.go
*/

func TestProduce(t *testing.T) {
	requestHeader := kafkaprotocol.RequestHeader{
		RequestApiKey:     kafkaprotocol.APIKeyProduce,
		RequestApiVersion: 3,
		CorrelationId:     23,
		ClientId:          common.StrPtr("some-client-id"),
	}
	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							[]byte("foo"),
						},
					},
				},
			},
		},
	}
	resp := kafkaprotocol.ProduceResponse{
		Responses: []kafkaprotocol.ProduceResponseTopicProduceResponse{
			{
				Name: common.StrPtr("topic1"),
				PartitionResponses: []kafkaprotocol.ProduceResponsePartitionProduceResponse{
					{
						Index:      12,
						BaseOffset: 12323,
					},
				},
			},
		},
		ThrottleTimeMs: 123123,
	}

	connHandlers := &testConnHandlers{response: &resp}
	address, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	kafkaServer := NewKafkaServer(address, conf.TLSConfig{}, "", connHandlers.createHandler)
	err = kafkaServer.Start()
	require.NoError(t, err)

	conn, err := net.Dial("tcp", address)
	require.NoError(t, err)

	buff := requestHeader.Write(1, nil, nil)
	buff = req.Write(3, buff, nil)

	_, err = conn.Write(createLengthPrefixedMessage(buff))
	require.NoError(t, err)

	testutils.WaitUntil(t, func() (bool, error) {
		hndlrs := connHandlers.getHandlers()
		if len(hndlrs) != 1 {
			return false, nil
		}
		hndlr := hndlrs[0]
		r, _ := hndlr.getReceivedRequest()
		return r != nil, nil
	})

	handlers := connHandlers.getHandlers()
	require.Equal(t, 1, len(handlers))
	receivedHdr, receivedReq := handlers[0].getReceivedRequest()

	require.Equal(t, &requestHeader, receivedHdr)
	require.Equal(t, &req, receivedReq)
}

type testConnHandlers struct {
	lock     sync.Mutex
	handlers []*testKafkaHandler
	response any
}

func (s *testConnHandlers) createHandler(ctx ConnectionContext) kafkaprotocol.RequestHandler {
	s.lock.Lock()
	defer s.lock.Unlock()
	handler := &testKafkaHandler{
		ctx:      ctx,
		response: s.response,
	}
	s.handlers = append(s.handlers, handler)
	return handler
}

func (s *testConnHandlers) getHandlers() []*testKafkaHandler {
	s.lock.Lock()
	defer s.lock.Unlock()
	copied := make([]*testKafkaHandler, len(s.handlers))
	copy(copied, s.handlers)
	return copied
}

func createLengthPrefixedMessage(body []byte) []byte {
	var msg []byte
	msg = binary.BigEndian.AppendUint32(msg, uint32(len(body)))
	msg = append(msg, body...)
	return msg
}

type testKafkaHandler struct {
	lock            sync.Mutex
	ctx             ConnectionContext
	receivedHeader  *kafkaprotocol.RequestHeader
	receivedRequest any
	response        any
	called          bool
}

func (t *testKafkaHandler) getReceivedRequest() (hdr *kafkaprotocol.RequestHeader, req any) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.receivedHeader, t.receivedRequest
}

func (t *testKafkaHandler) HandleProduceRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.ProduceRequest, completionFunc func(resp *kafkaprotocol.ProduceResponse) error) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.called {
		panic("called more than once")
	}
	t.receivedHeader = hdr
	t.receivedRequest = req
	t.called = true
	return completionFunc(t.response.(*kafkaprotocol.ProduceResponse))
}

func (t *testKafkaHandler) ProduceRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.ProduceRequest) *kafkaprotocol.ProduceResponse {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HandleFetchRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.FetchRequest, completionFunc func(resp *kafkaprotocol.FetchResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) FetchRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.FetchRequest) *kafkaprotocol.FetchResponse {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HandleListOffsetsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.ListOffsetsRequest, completionFunc func(resp *kafkaprotocol.ListOffsetsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) ListOffsetsRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.ListOffsetsRequest) *kafkaprotocol.ListOffsetsResponse {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HandleMetadataRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.MetadataRequest, completionFunc func(resp *kafkaprotocol.MetadataResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) MetadataRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.MetadataRequest) *kafkaprotocol.MetadataResponse {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HandleOffsetCommitRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.OffsetCommitRequest, completionFunc func(resp *kafkaprotocol.OffsetCommitResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) OffsetCommitRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.OffsetCommitRequest) *kafkaprotocol.OffsetCommitResponse {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HandleOffsetFetchRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.OffsetFetchRequest, completionFunc func(resp *kafkaprotocol.OffsetFetchResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) OffsetFetchRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.OffsetFetchRequest) *kafkaprotocol.OffsetFetchResponse {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HandleFindCoordinatorRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.FindCoordinatorRequest, completionFunc func(resp *kafkaprotocol.FindCoordinatorResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) FindCoordinatorRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.FindCoordinatorRequest) *kafkaprotocol.FindCoordinatorResponse {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HandleJoinGroupRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.JoinGroupRequest, completionFunc func(resp *kafkaprotocol.JoinGroupResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) JoinGroupRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.JoinGroupRequest) *kafkaprotocol.JoinGroupResponse {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HandleHeartbeatRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.HeartbeatRequest, completionFunc func(resp *kafkaprotocol.HeartbeatResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HeartbeatRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.HeartbeatRequest) *kafkaprotocol.HeartbeatResponse {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HandleLeaveGroupRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.LeaveGroupRequest, completionFunc func(resp *kafkaprotocol.LeaveGroupResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) LeaveGroupRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.LeaveGroupRequest) *kafkaprotocol.LeaveGroupResponse {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HandleSyncGroupRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.SyncGroupRequest, completionFunc func(resp *kafkaprotocol.SyncGroupResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) SyncGroupRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.SyncGroupRequest) *kafkaprotocol.SyncGroupResponse {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HandleApiVersionsRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.ApiVersionsRequest, completionFunc func(resp *kafkaprotocol.ApiVersionsResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) ApiVersionsRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.ApiVersionsRequest) *kafkaprotocol.ApiVersionsResponse {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HandleInitProducerIdRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.InitProducerIdRequest, completionFunc func(resp *kafkaprotocol.InitProducerIdResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) InitProducerIdRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.InitProducerIdRequest) *kafkaprotocol.InitProducerIdResponse {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HandleSaslAuthenticateRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.SaslAuthenticateRequest, completionFunc func(resp *kafkaprotocol.SaslAuthenticateResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) SaslAuthenticateRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.SaslAuthenticateRequest) *kafkaprotocol.SaslAuthenticateResponse {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) HandleSaslHandshakeRequest(hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.SaslHandshakeRequest, completionFunc func(resp *kafkaprotocol.SaslHandshakeResponse) error) error {
	//TODO implement me
	panic("implement me")
}

func (t *testKafkaHandler) SaslHandshakeRequestErrorResponse(errorCode int16, errorMsg string, req *kafkaprotocol.SaslHandshakeRequest) *kafkaprotocol.SaslHandshakeResponse {
	//TODO implement me
	panic("implement me")
}
