package agent

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/sockserver"
	"sync"
)

type KafkaApiClient struct {
	sockClient *sockserver.SocketClient
	clientID string
}

func NewKafkaApiClient() (*KafkaApiClient, error) {
	return NewKafkaApiClientWithClientID("some-client-id")
}

func NewKafkaApiClientWithClientID(clientID string) (*KafkaApiClient, error) {
	sockClient, err := sockserver.NewSocketClient(nil)
	if err != nil {
		return nil, err
	}
	return &KafkaApiClient{
		sockClient: sockClient,
		clientID: clientID,
	}, nil
}

func (k *KafkaApiClient) NewConnection(address string) (*KafkaApiConnection, error) {
	kc := &KafkaApiConnection{
		respHandlers: make(map[int32]respHolder),
		cl: k,
	}
	socketConnection, err := k.sockClient.CreateConnection(address, kc.responseHandler)
	if err != nil {
		return nil, err
	}
	kc.sockConnection = socketConnection
	return kc, nil
}

type KafkaApiConnection struct {
	lock             sync.Mutex
	cl *KafkaApiClient
	correlationIDSeq int32
	respHandlers     map[int32]respHolder
	sockConnection   *sockserver.SocketConnection
}

type respHolder struct {
	resp              KafkaProtocolMessage
	respHeaderVersion int16
	ch                chan KafkaProtocolMessage
	apiVersion        int16
}

func (k *KafkaApiConnection) SendRequest(req KafkaProtocolRequest, apiKey int16, apiVersion int16,
	resp KafkaProtocolMessage) (KafkaProtocolMessage, error) {
	msg, ch := k.createRequestAndRegisterHandler(req, apiKey, apiVersion, resp)
	if err := k.sockConnection.SendMessage(msg); err != nil {
		return nil, err
	}
	return <-ch, nil
}

func (k *KafkaApiConnection) Close() error {
	return k.sockConnection.Close()
}

func (k *KafkaApiConnection) createRequestAndRegisterHandler(req KafkaProtocolRequest, apiKey int16, apiVersion int16,
	resp KafkaProtocolMessage) ([]byte, chan KafkaProtocolMessage) {
	k.lock.Lock()
	defer k.lock.Unlock()
	var hdr kafkaprotocol.RequestHeader
	hdr.CorrelationId = k.correlationIDSeq
	hdr.RequestApiKey = apiKey
	hdr.RequestApiVersion = apiVersion
	hdr.ClientId = common.StrPtr(k.cl.clientID)
	requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
	buff := hdr.Write(requestHeaderVersion, nil, nil)
	buff = req.Write(apiVersion, buff, nil)
	ch := make(chan KafkaProtocolMessage, 1)
	k.respHandlers[k.correlationIDSeq] = respHolder{
		resp:              resp,
		respHeaderVersion: responseHeaderVersion,
		ch:                ch,
		apiVersion:        apiVersion,
	}
	k.correlationIDSeq++
	return buff, ch
}

func (k *KafkaApiConnection) responseHandler(buff []byte) error {
	k.lock.Lock()
	defer k.lock.Unlock()
	correlationID := int32(binary.BigEndian.Uint32(buff))
	respHandler, ok := k.respHandlers[correlationID]
	if !ok {
		return errors.Errorf("response handler for correlation id %d not found", correlationID)
	}
	var hdr kafkaprotocol.ResponseHeader
	bytesRead, err := hdr.Read(respHandler.respHeaderVersion, buff)
	if err != nil {
		return err
	}
	if _, err := respHandler.resp.Read(respHandler.apiVersion, buff[bytesRead:]); err != nil {
		return err
	}
	delete(k.respHandlers, hdr.CorrelationId)
	respHandler.ch <- respHandler.resp
	return nil
}

type KafkaProtocolMessage interface {
	Read(version int16, buff []byte) (int, error)
	Write(version int16, buff []byte, tagSizes []int) []byte
}

type KafkaProtocolRequest interface {
	KafkaProtocolMessage
	HeaderVersions(version int16) (int16, int16)
}
