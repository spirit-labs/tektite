//go:build integration

package integration

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
	"time"
)

// MetadataRequest represents a Kafka Metadata request
type MetadataRequest struct {
	Topics []MetadataRequestTopic
}

// MetadataRequestTopic represents a topic in the Metadata request
type MetadataRequestTopic struct {
	Name *string
}

// MetadataResponse represents a Kafka Metadata response
type MetadataResponse struct {
	ThrottleTimeMs int32
	Brokers        []Broker
	ClusterID      *string
	ControllerID   int32
	Topics         []TopicMetadata
}

// Broker represents a broker in the Metadata response
type Broker struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   *string
}

// TopicMetadata represents topic metadata in the Metadata response
type TopicMetadata struct {
	ErrorCode  int16
	Name       string
	IsInternal bool
	Partitions []PartitionMetadata
}

// PartitionMetadata represents partition metadata in the Metadata response
type PartitionMetadata struct {
	ErrorCode      int16
	PartitionIndex int32
	LeaderID       int32
	LeaderEpoch    int32
	ReplicaNodes   []int32
	IsrNodes       []int32
}

func TestBrokerSendsAdvertisedAddress(t *testing.T) {
	t.Skip()

	tcs := []struct {
		name                string
		advertisedAddresses []string
	}{
		{
			name:                "Addresses are set and AdvertisedAddresses are not set",
			advertisedAddresses: nil,
		},
		{
			name:                "Addresses are set and AdvertisedAddresses are set",
			advertisedAddresses: []string{"advertisedAddress1:8080", "advertisedAddress2:8080", "advertisedAddress3:8080"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			clientTLSConfig := client.TLSConfig{
				TrustedCertsPath: serverCertPath,
			}
			servers, tearDown := startClusterWithConfigSetter(t, 3, nil, func(cfg *conf.Config) {
				cfg.KafkaServerEnabled = true
				var kafkaListenAddresses []string
				for i := 0; i < 3; i++ {
					address, err := common.AddressWithPort("localhost")
					require.NoError(t, err)
					kafkaListenAddresses = append(kafkaListenAddresses, address)

				}
				cfg.KafkaServerListenerConfig.Addresses = kafkaListenAddresses
				if tc.advertisedAddresses != nil {
					cfg.KafkaServerListenerConfig.AdvertisedAddresses = tc.advertisedAddresses
				}
			})
			defer tearDown(t)
			client, err := client.NewClient(servers[0].GetConfig().HttpApiAddresses[0], clientTLSConfig)
			require.NoError(t, err)
			defer client.Close()

			err = client.ExecuteStatement(`topic1 :=  (kafka in partitions=1) -> (store stream)`)
			require.NoError(t, err)

			serverAddress := servers[0].GetConfig().KafkaServerListenerConfig.Addresses[0]
			wantAddresses := servers[0].GetConfig().KafkaServerListenerConfig.Addresses
			if tc.advertisedAddresses != nil {
				wantAddresses = tc.advertisedAddresses
			}

			err = executeMetadataRequest(t, serverAddress, wantAddresses)
			if err != nil {
				log.Errorf("failed to execute client actions %v", err)
			}
			require.NoError(t, err)
		})
	}
}

func executeMetadataRequest(t *testing.T, serverAddress string, desiredAddresses []string) error {
	conn, err := net.DialTimeout("tcp", serverAddress, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}
	defer conn.Close()

	// Create a Metadata request
	request := MetadataRequest{
		Topics: []MetadataRequestTopic{},
	}

	// Serialize the request
	requestData := serializeMetadataRequest(request)

	// Send the request
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(len(requestData)))
	_, err = conn.Write(buf)
	_, err = conn.Write(requestData)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	// Read the response size (first 4 bytes)
	var size int32
	err = binary.Read(conn, binary.BigEndian, &size)
	if err != nil {
		return fmt.Errorf("failed to read response size: %w", err)
	}

	// Read the response data
	responseData := make([]byte, size)
	_, err = conn.Read(responseData)
	if err != nil {
		return fmt.Errorf("failed to read response data: %w", err)
	}
	var correlationId = int32(binary.BigEndian.Uint32(responseData[0:4]))
	t.Logf("correlationId: %d\n", correlationId)
	// Deserialize the response
	response := deserializeMetadataResponse(responseData[4:])
	for _, broker := range response.Brokers {
		require.Contains(t, desiredAddresses, fmt.Sprintf("%s:%d", broker.Host, broker.Port))
	}
	t.Logf("Metadata Response: %+v\n", response)
	return nil
}

func serializeMetadataRequest(req MetadataRequest) []byte {
	buf := new(bytes.Buffer)

	// API Key for Metadata request
	binary.Write(buf, binary.BigEndian, int16(3))
	// API Version
	binary.Write(buf, binary.BigEndian, int16(3))
	// Correlation ID
	binary.Write(buf, binary.BigEndian, int32(1))
	// Client ID (Nullable String)
	writeNullableString(buf, "go-client")

	// Number of topics
	writeCompactArrayLength(buf, len(req.Topics))
	for _, topic := range req.Topics {
		// Topic name (String)
		writeString(buf, topic.Name)
	}

	return buf.Bytes()
}

func deserializeMetadataResponse(data []byte) MetadataResponse {
	buf := bytes.NewReader(data)
	var resp MetadataResponse

	// ThrottleTimeMs
	binary.Read(buf, binary.BigEndian, &resp.ThrottleTimeMs)

	// Brokers
	brokerCount := readArrayLength(buf)
	resp.Brokers = make([]Broker, brokerCount)
	for i := range resp.Brokers {
		var broker Broker
		binary.Read(buf, binary.BigEndian, &broker.NodeID)
		broker.Host = readString(buf)
		binary.Read(buf, binary.BigEndian, &broker.Port)
		broker.Rack = readNullableString(buf)
		resp.Brokers[i] = broker
	}

	// ClusterID
	resp.ClusterID = readNullableString(buf)
	// ControllerID
	binary.Read(buf, binary.BigEndian, &resp.ControllerID)

	// Topics
	topicCount := readArrayLength(buf)
	resp.Topics = make([]TopicMetadata, topicCount)
	for i := range resp.Topics {
		var topic TopicMetadata
		binary.Read(buf, binary.BigEndian, &topic.ErrorCode)
		topic.Name = readString(buf)
		binary.Read(buf, binary.BigEndian, &topic.IsInternal)

		// Partitions
		partitionCount := readArrayLength(buf)
		topic.Partitions = make([]PartitionMetadata, partitionCount)
		for j := range topic.Partitions {
			var partition PartitionMetadata
			binary.Read(buf, binary.BigEndian, &partition.ErrorCode)
			binary.Read(buf, binary.BigEndian, &partition.PartitionIndex)
			binary.Read(buf, binary.BigEndian, &partition.LeaderID)
			partition.ReplicaNodes = readInt32Array(buf)
			partition.IsrNodes = readInt32Array(buf)
			topic.Partitions[j] = partition
		}

		resp.Topics[i] = topic
	}

	return resp
}

func writeNullableString(buf *bytes.Buffer, str string) {
	length := len(str)
	binary.Write(buf, binary.BigEndian, uint16(length))
	buf.WriteString(str)
}

func writeString(buf *bytes.Buffer, str *string) {
	if str == nil {
		binary.Write(buf, binary.BigEndian, uint16(0))
	} else {
		length := len(*str)
		binary.Write(buf, binary.BigEndian, uint16(length))
		buf.WriteString(*str)
	}
}

func writeCompactArrayLength(buf *bytes.Buffer, length int) {
	binary.Write(buf, binary.BigEndian, uint32(length))
}

func readString(buf *bytes.Reader) string {
	var length uint16
	binary.Read(buf, binary.BigEndian, &length)
	strBytes := make([]byte, length)
	buf.Read(strBytes)
	return string(strBytes)
}

func readNullableString(buf *bytes.Reader) *string {
	var length uint16
	binary.Read(buf, binary.BigEndian, &length)
	if int16(length) == -1 {
		return nil
	}
	strBytes := make([]byte, length)
	buf.Read(strBytes)
	str := string(strBytes)
	return &str
}

func readArrayLength(buf *bytes.Reader) int32 {
	var length uint32
	binary.Read(buf, binary.BigEndian, &length)
	return int32(length)
}

func readInt32Array(buf *bytes.Reader) []int32 {
	length := readArrayLength(buf)
	array := make([]int32, length)
	for i := range array {
		var n uint32
		binary.Read(buf, binary.BigEndian, &n)
		array[i] = int32(n)
	}
	return array
}
