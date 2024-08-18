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
	"github.com/spirit-labs/tektite/kafkaserver/protocol"
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
	{caseName: "testApiVersionsUnsupportedVersion", f: testApiVersionsUnsupportedVersion},
	{caseName: "testProduceErrorUnknownTopic", f: testProduceErrorUnknownTopic},
	{caseName: "testProduceErrorUnsupportedVersion", f: testProduceErrorUnsupportedVersion},
	{caseName: "testFetchErrorUnknownTopic", f: testFetchErrorUnknownTopic},
	{caseName: "testFetchErrorUnsupportedVersion", f: testFetchErrorUnsupportedVersion},
	{caseName: "testSaslHandshakeRequest", f: testSaslHandshakeRequest},
	{caseName: "testSaslHandshakeErrorUnsupportedVersion", f: testSaslHandshakeErrorUnsupportedVersion},
	{caseName: "testSaslAuthenticateRequest", f: testSaslAuthenticateRequest},
	{caseName: "testSaslAuthenticateUnsupportedVersion", f: testSaslAuthenticateUnsupportedVersion},
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
	var req protocol.ApiVersionsRequest
	req.ClientSoftwareVersion = stringPtr("1.23")
	req.ClientSoftwareName = stringPtr("software1")
	var resp protocol.ApiVersionsResponse
	writeRequest(t, protocol.APIKeyAPIVersions, 3, &req, &resp, conn)
	require.Equal(t, protocol.ErrorCodeNone, int(resp.ErrorCode))
	require.Equal(t, protocol.SupportedAPIVersions, resp.ApiKeys)
}

func testApiVersionsUnsupportedVersion(t *testing.T, address string) {
	conn := createConn(t, address)
	var req protocol.ApiVersionsRequest
	req.ClientSoftwareVersion = stringPtr("1.23")
	req.ClientSoftwareName = stringPtr("software1")
	var resp protocol.ApiVersionsResponse
	writeRequest(t, protocol.APIKeyAPIVersions, 100, &req, &resp, conn)
	require.Equal(t, protocol.ErrorCodeUnsupportedVersion, int(resp.ErrorCode))
	require.Equal(t, 0, len(resp.ApiKeys))
}

func testProduceErrorUnknownTopic(t *testing.T, address string) {
	conn := createConn(t, address)
	req := protocol.ProduceRequest{
		TopicData: []protocol.ProduceRequestTopicProduceData{
			{
				Name: stringPtr("unknown_topic1"),
				PartitionData: []protocol.ProduceRequestPartitionProduceData{
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
				PartitionData: []protocol.ProduceRequestPartitionProduceData{
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
	var resp protocol.ProduceResponse
	writeRequest(t, protocol.APIKeyProduce, 3, &req, &resp, conn)
	require.Equal(t, 2, len(resp.Responses))
	require.Equal(t, 2, len(resp.Responses[0].PartitionResponses))
	require.Equal(t, 2, len(resp.Responses[1].PartitionResponses))
	require.Equal(t, protocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[0].PartitionResponses[0].ErrorCode))
	require.Equal(t, protocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[0].PartitionResponses[1].ErrorCode))
	require.Equal(t, protocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[1].PartitionResponses[0].ErrorCode))
	require.Equal(t, protocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[1].PartitionResponses[1].ErrorCode))
}

func testProduceErrorUnsupportedVersion(t *testing.T, address string) {
	conn := createConn(t, address)
	req := protocol.ProduceRequest{
		TopicData: []protocol.ProduceRequestTopicProduceData{
			{
				Name: stringPtr("unknown_topic1"),
				PartitionData: []protocol.ProduceRequestPartitionProduceData{
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
				PartitionData: []protocol.ProduceRequestPartitionProduceData{
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
	var resp protocol.ProduceResponse
	writeRequest(t, protocol.APIKeyProduce, 100, &req, &resp, conn)
	require.Equal(t, 2, len(resp.Responses))
	require.Equal(t, 2, len(resp.Responses[0].PartitionResponses))
	require.Equal(t, 2, len(resp.Responses[1].PartitionResponses))
	require.Equal(t, protocol.ErrorCodeUnsupportedVersion, int(resp.Responses[0].PartitionResponses[0].ErrorCode))
	require.Equal(t, protocol.ErrorCodeUnsupportedVersion, int(resp.Responses[0].PartitionResponses[1].ErrorCode))
	require.Equal(t, protocol.ErrorCodeUnsupportedVersion, int(resp.Responses[1].PartitionResponses[0].ErrorCode))
	require.Equal(t, protocol.ErrorCodeUnsupportedVersion, int(resp.Responses[1].PartitionResponses[1].ErrorCode))
}

func testFetchErrorUnknownTopic(t *testing.T, address string) {
	conn := createConn(t, address)
	req := protocol.FetchRequest{
		Topics: []protocol.FetchRequestFetchTopic{
			{
				Topic: stringPtr("topic1"),
				Partitions: []protocol.FetchRequestFetchPartition{
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
				Partitions: []protocol.FetchRequestFetchPartition{
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
	var resp protocol.FetchResponse
	writeRequest(t, protocol.APIKeyFetch, 4, &req, &resp, conn)
	require.Equal(t, 2, len(resp.Responses))
	require.Equal(t, 2, len(resp.Responses[0].Partitions))
	require.Equal(t, 2, len(resp.Responses[1].Partitions))
	require.Equal(t, protocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[0].Partitions[0].ErrorCode))
	require.Equal(t, protocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[0].Partitions[1].ErrorCode))
	require.Equal(t, protocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[1].Partitions[0].ErrorCode))
	require.Equal(t, protocol.ErrorCodeUnknownTopicOrPartition, int(resp.Responses[1].Partitions[1].ErrorCode))
}

func testFetchErrorUnsupportedVersion(t *testing.T, address string) {
	conn := createConn(t, address)
	req := protocol.FetchRequest{
		ReplicaState: protocol.FetchRequestReplicaState{
			ReplicaId:    1001,
			ReplicaEpoch: 34,
		},
		MaxWaitMs:      250,
		MinBytes:       65536,
		MaxBytes:       4746464,
		IsolationLevel: 3,
		SessionId:      45464,
		SessionEpoch:   456,
		ClusterId:      stringPtr("someclusterid"),
		Topics: []protocol.FetchRequestFetchTopic{
			{
				TopicId: randomUUID(),
				Partitions: []protocol.FetchRequestFetchPartition{
					{Partition: 1, FetchOffset: 230000, PartitionMaxBytes: 4453453, LogStartOffset: 120000, CurrentLeaderEpoch: 12, LastFetchedEpoch: 13, ReplicaDirectoryId: randomUUID()},
					{Partition: 5, FetchOffset: 576484, PartitionMaxBytes: 575757, LogStartOffset: 36358888, CurrentLeaderEpoch: 43, LastFetchedEpoch: 67, ReplicaDirectoryId: randomUUID()},
				},
			},
			{
				TopicId: randomUUID(),
				Partitions: []protocol.FetchRequestFetchPartition{
					{Partition: 7, FetchOffset: 266000, PartitionMaxBytes: 723663, LogStartOffset: 4363636, CurrentLeaderEpoch: 1, LastFetchedEpoch: 3, ReplicaDirectoryId: randomUUID()},
					{Partition: 9, FetchOffset: 5978484, PartitionMaxBytes: 56595, LogStartOffset: 4743545, CurrentLeaderEpoch: 67, LastFetchedEpoch: 5, ReplicaDirectoryId: randomUUID()},
				},
			},
		},
		ForgottenTopicsData: []protocol.FetchRequestForgottenTopic{
			{TopicId: randomUUID(), Partitions: []int32{34, 456, 6, 7}},
			{TopicId: randomUUID(), Partitions: []int32{34, 456, 6, 7}},
		},
		RackId: stringPtr("rack1"),
	}
	var resp protocol.FetchResponse
	writeRequest(t, protocol.APIKeyFetch, 100, &req, &resp, conn)
	require.Equal(t, 2, len(resp.Responses))
	require.Equal(t, 2, len(resp.Responses[0].Partitions))
	require.Equal(t, 2, len(resp.Responses[1].Partitions))
	require.Equal(t, protocol.ErrorCodeUnsupportedVersion, int(resp.Responses[0].Partitions[0].ErrorCode))
	require.Equal(t, protocol.ErrorCodeUnsupportedVersion, int(resp.Responses[0].Partitions[1].ErrorCode))
	require.Equal(t, protocol.ErrorCodeUnsupportedVersion, int(resp.Responses[1].Partitions[0].ErrorCode))
	require.Equal(t, protocol.ErrorCodeUnsupportedVersion, int(resp.Responses[1].Partitions[1].ErrorCode))
}

func testSaslHandshakeRequest(t *testing.T, address string) {
	conn := createConn(t, address)
	var req protocol.SaslHandshakeRequest
	req.Mechanism = stringPtr("PLAIN")
	var resp protocol.SaslHandshakeResponse
	writeRequest(t, protocol.APIKeySaslHandshake, 1, &req, &resp, conn)
	require.Equal(t, 1, len(resp.Mechanisms))
	auth256 := auth.AuthenticationSaslScramSha256
	require.Equal(t, []*string{&auth256}, resp.Mechanisms)
}

func testSaslHandshakeErrorUnsupportedVersion(t *testing.T, address string) {
	conn := createConn(t, address)
	var req protocol.SaslHandshakeRequest
	req.Mechanism = stringPtr("PLAIN")
	var resp protocol.SaslHandshakeResponse
	writeRequest(t, protocol.APIKeySaslHandshake, 100, &req, &resp, conn)
	require.Equal(t, protocol.ErrorCodeUnsupportedVersion, int(resp.ErrorCode))
}

// testSaslAuthenticateRequest simply validates that the handler is called, tests for actual sasl authentication are
// in kafka auth integration tests
func testSaslAuthenticateRequest(t *testing.T, address string) {
	conn := createConn(t, address)
	var req protocol.SaslAuthenticateRequest
	req.AuthBytes = []byte("foo")
	var resp protocol.SaslAuthenticateResponse
	writeRequest(t, protocol.APIKeySaslAuthenticate, 1, &req, &resp, conn)
	require.Equal(t, protocol.ErrorCodeIllegalSaslState, int(resp.ErrorCode))
}

func testSaslAuthenticateUnsupportedVersion(t *testing.T, address string) {
	conn := createConn(t, address)
	var req protocol.SaslAuthenticateRequest
	req.AuthBytes = []byte("foo")
	var resp protocol.SaslAuthenticateResponse
	writeRequest(t, protocol.APIKeySaslAuthenticate, 100, &req, &resp, conn)
	require.Equal(t, protocol.ErrorCodeUnsupportedVersion, int(resp.ErrorCode))
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
	req := protocol.ProduceRequest{
		TopicData: []protocol.ProduceRequestTopicProduceData{
			{
				Name: stringPtr("unknown_topic1"),
				PartitionData: []protocol.ProduceRequestPartitionProduceData{
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
				PartitionData: []protocol.ProduceRequestPartitionProduceData{
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
	var resp protocol.ProduceResponse
	// Connection should be closed
	writeRequestWithError(t, protocol.APIKeyProduce, 3, &req, &resp, conn, io.EOF)
}

func writeRequest(t *testing.T, apiKey int16, apiVersion int16, req request, resp serializable, conn net.Conn) *protocol.ResponseHeader {
	return writeRequestWithError(t, apiKey, apiVersion, req, resp, conn, nil)
}

func writeRequestWithError(t *testing.T, apiKey int16, apiVersion int16, req request, resp serializable, conn net.Conn,
	expectedErr error) *protocol.ResponseHeader {
	var reqHdr protocol.RequestHeader
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
	var respHdr protocol.ResponseHeader
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
