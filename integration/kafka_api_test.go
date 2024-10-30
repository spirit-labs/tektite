//go:build integration

package integration

import (
	"encoding/binary"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/server"
	"github.com/spirit-labs/tektite/auth"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/stretchr/testify/require"
	"io"
	"net"
	"testing"
	"time"
)

/*
We use this test mainly to check that error codes are propagated back correctly as it's hard to test for that using
the Kafka clients as often they catch the errors and retry
*/
func TestKafkaApi(t *testing.T) {
	clientTLSConfig := client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
	servers, tearDown := startKafkaServers(t, "")
	defer tearDown(t)
	cl, err := client.NewClient(servers[0].GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer cl.Close()
	serverAddress := servers[0].GetConfig().KafkaServerListenerConfig.Addresses[0]

	for _, tc := range apiTestCases {
		t.Run(tc.caseName, func(t *testing.T) {
			tc.f(t, serverAddress)
		})
	}
}

func startKafkaServers(t *testing.T, authType string) ([]*server.Server, func(t *testing.T)) {
	return startClusterWithConfigSetter(t, 3, nil, func(cfg *conf.Config) {
		cfg.KafkaServerEnabled = true
		var kafkaListenAddresses []string
		for i := 0; i < 3; i++ {
			address, err := common.AddressWithPort("localhost")
			require.NoError(t, err)
			kafkaListenAddresses = append(kafkaListenAddresses, address)
		}
		cfg.KafkaServerListenerConfig.Addresses = kafkaListenAddresses
		cfg.KafkaServerListenerConfig.AuthenticationType = authType
	})
}

type apiTestCase struct {
	caseName string
	f        func(t *testing.T, address string)
}

var apiTestCases = []apiTestCase{
	{caseName: "testApiVersions", f: testApiVersions},
	{caseName: "testProduceErrorUnknownTopic", f: testProduceErrorUnknownTopic},
	{caseName: "testFetchErrorUnknownTopic", f: testFetchErrorUnknownTopic},
	{caseName: "testSaslHandshakeRequest", f: testSaslHandshakeRequest},
	{caseName: "testSaslAuthenticateRequest", f: testSaslAuthenticateRequest},
}

func createConn(t *testing.T, address string) net.Conn {
	addr, err := net.ResolveTCPAddr("tcp", address)
	require.NoError(t, err)
	d := net.Dialer{Timeout: 1 * time.Second}
	conn, err := d.Dial("tcp", addr.String())
	require.NoError(t, err)
	tcpnc := conn.(*net.TCPConn)
	err = tcpnc.SetNoDelay(true)
	require.NoError(t, err)
	return conn
}

func testApiVersions(t *testing.T, address string) {
	conn := createConn(t, address)
	var req kafkaprotocol.ApiVersionsRequest
	req.ClientSoftwareVersion = stringPtr("1.23")
	req.ClientSoftwareName = stringPtr("software1")
	var resp kafkaprotocol.ApiVersionsResponse
	writeRequest(t, kafkaprotocol.APIKeyAPIVersions, 3, &req, &resp, conn)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.ErrorCode))
	require.Equal(t, kafkaprotocol.SupportedAPIVersions, resp.ApiKeys)
}

func testProduceErrorUnknownTopic(t *testing.T, address string) {
	conn := createConn(t, address)
	req := kafkaprotocol.ProduceRequest{
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: stringPtr("unknown_topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index:   1,
						Records: [][]byte{[]byte("abc")},
					},
					{
						Index:   3,
						Records: [][]byte{[]byte("abc")},
					},
				},
			},
			{
				Name: stringPtr("unknown_topic2"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index:   2,
						Records: [][]byte{[]byte("abc")},
					},
					{
						Index:   4,
						Records: [][]byte{[]byte("abc")},
					},
				},
			},
		},
	}
	var resp kafkaprotocol.ProduceResponse
	writeRequest(t, kafkaprotocol.APIKeyProduce, 3, &req, &resp, conn)
	require.Equal(t, 2, len(resp.Responses))
	require.Equal(t, 2, len(resp.Responses[0].PartitionResponses))
	require.Equal(t, 2, len(resp.Responses[1].PartitionResponses))
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[0].PartitionResponses[0].ErrorCode))
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[0].PartitionResponses[1].ErrorCode))
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[1].PartitionResponses[0].ErrorCode))
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[1].PartitionResponses[1].ErrorCode))
}

func testFetchErrorUnknownTopic(t *testing.T, address string) {
	conn := createConn(t, address)
	req := kafkaprotocol.FetchRequest{
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: stringPtr("topic1"),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:   1,
						FetchOffset: 1234,
					},
					{
						Partition:   3,
						FetchOffset: 2323,
					},
				},
			},
			{
				Topic: stringPtr("topic2"),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:   2,
						FetchOffset: 2234,
					},
					{
						Partition:   4,
						FetchOffset: 5323,
					},
				},
			},
		},
	}
	var resp kafkaprotocol.FetchResponse
	writeRequest(t, kafkaprotocol.APIKeyFetch, 4, &req, &resp, conn)
	require.Equal(t, 2, len(resp.Responses))
	require.Equal(t, 2, len(resp.Responses[0].Partitions))
	require.Equal(t, 2, len(resp.Responses[1].Partitions))
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[0].Partitions[0].ErrorCode))
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[0].Partitions[1].ErrorCode))
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[1].Partitions[0].ErrorCode))
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[1].Partitions[1].ErrorCode))
}

func testSaslHandshakeRequest(t *testing.T, address string) {
	conn := createConn(t, address)
	var req kafkaprotocol.SaslHandshakeRequest
	req.Mechanism = stringPtr("PLAIN")
	var resp kafkaprotocol.SaslHandshakeResponse
	writeRequest(t, kafkaprotocol.APIKeySaslHandshake, 1, &req, &resp, conn)
	require.Equal(t, 1, len(resp.Mechanisms))
	auth256 := auth.AuthenticationSaslScramSha256
	require.Equal(t, []*string{&auth256}, resp.Mechanisms)
}

// testSaslAuthenticateRequest simply validates that the handler is called, tests for actual sasl authentication are
// in kafka auth integration tests
func testSaslAuthenticateRequest(t *testing.T, address string) {
	conn := createConn(t, address)
	var req kafkaprotocol.SaslAuthenticateRequest
	req.AuthBytes = []byte("foo")
	var resp kafkaprotocol.SaslAuthenticateResponse
	writeRequest(t, kafkaprotocol.APIKeySaslAuthenticate, 1, &req, &resp, conn)
	require.Equal(t, kafkaprotocol.ErrorCodeIllegalSaslState, int(resp.ErrorCode))
}

func TestKafkaApiNotAuthenticated(t *testing.T) {
	clientTLSConfig := client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
	servers, tearDown := startKafkaServers(t, auth.AuthenticationSaslScramSha256)
	defer tearDown(t)
	cl, err := client.NewClient(servers[0].GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer cl.Close()
	serverAddress := servers[0].GetConfig().KafkaServerListenerConfig.Addresses[0]

	// Try and send a produce request
	conn := createConn(t, serverAddress)
	req := kafkaprotocol.ProduceRequest{
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: stringPtr("unknown_topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index:   1,
						Records: [][]byte{[]byte("abc")},
					},
					{
						Index:   3,
						Records: [][]byte{[]byte("abc")},
					},
				},
			},
			{
				Name: stringPtr("unknown_topic2"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index:   2,
						Records: [][]byte{[]byte("abc")},
					},
					{
						Index:   4,
						Records: [][]byte{[]byte("abc")},
					},
				},
			},
		},
	}
	var resp kafkaprotocol.ProduceResponse
	// Connection should be closed
	writeRequestWithError(t, kafkaprotocol.APIKeyProduce, 3, &req, &resp, conn, io.EOF)
}

func writeRequest(t *testing.T, apiKey int16, apiVersion int16, req request, resp serializable, conn net.Conn) *kafkaprotocol.ResponseHeader {
	return writeRequestWithError(t, apiKey, apiVersion, req, resp, conn, nil)
}

func writeRequestWithError(t *testing.T, apiKey int16, apiVersion int16, req request, resp serializable, conn net.Conn,
	expectedErr error) *kafkaprotocol.ResponseHeader {
	var reqHdr kafkaprotocol.RequestHeader
	reqHdr.RequestApiKey = apiKey
	reqHdr.RequestApiVersion = apiVersion
	reqHdr.CorrelationId = 333
	requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
	size, hdrTagSizes := reqHdr.CalcSize(requestHeaderVersion, nil)
	reqSize, reqTagSizes := req.CalcSize(apiVersion, nil)
	size += reqSize
	buff := make([]byte, 0, size+4)
	buff = binary.BigEndian.AppendUint32(buff, uint32(size))
	buff = reqHdr.Write(requestHeaderVersion, buff, hdrTagSizes)
	buff = req.Write(apiVersion, buff, reqTagSizes)
	_, err := conn.Write(buff)
	require.NoError(t, err)
	respSizeBuff := make([]byte, 4)
	_, err = io.ReadAtLeast(conn, respSizeBuff, 4)
	if err != nil {
		require.True(t, errors.Is(err, expectedErr))
		return nil
	} else {
		require.NoError(t, err)
	}
	respMsgSize := binary.BigEndian.Uint32(respSizeBuff)
	msgBuff := make([]byte, respMsgSize)
	_, err = io.ReadAtLeast(conn, msgBuff, int(respMsgSize))
	require.NoError(t, err)
	var respHdr kafkaprotocol.ResponseHeader
	offset, err := respHdr.Read(responseHeaderVersion, msgBuff)
	require.NoError(t, err)
	require.Equal(t, 333, int(respHdr.CorrelationId))
	_, err = resp.Read(reqHdr.RequestApiVersion, msgBuff[offset:])
	require.NoError(t, err)
	return &respHdr
}

type request interface {
	HeaderVersions(version int16) (int16, int16)
	serializable
}

type serializable interface {
	Read(version int16, buff []byte) (int, error)
	Write(version int16, buff []byte, tagSizes []int) []byte
	CalcSize(version int16, tagSizes []int) (int, []int)
}

func stringPtr(s string) *string {
	return &s
}

func randomUUID() []byte {
	u, err := uuid.New().MarshalBinary()
	if err != nil {
		panic(err)
	}
	if len(u) != 16 {
		panic("invalid uuid")
	}
	return u
}
