package protocol

import (
	"crypto/rand"
	"github.com/google/uuid"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestRequestHeader(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &RequestHeader{RequestApiKey: 20, RequestApiVersion: 3, CorrelationId: 12345}},
		{version: 1, obj: &RequestHeader{RequestApiKey: 20, RequestApiVersion: 3, CorrelationId: 12345, ClientId: stringPtr("someclient")}},
		{version: 2, obj: &RequestHeader{RequestApiKey: 20, RequestApiVersion: 3, CorrelationId: 12345, ClientId: stringPtr("someclient")}},
	}
	testReadWriteCases(t, testCases)
}

func TestResponseHeader(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &ResponseHeader{CorrelationId: 12345}},
		{version: 1, obj: &ResponseHeader{CorrelationId: 12345}},
	}
	testReadWriteCases(t, testCases)
}

func TestApiVersionsRequest(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &ApiVersionsRequest{}},
		{version: 3, obj: &ApiVersionsRequest{
			ClientSoftwareName:    stringPtr("software123"),
			ClientSoftwareVersion: stringPtr("version5343"),
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestApiVersionsResponse(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &ApiVersionsResponse{
			ApiKeys: SupportedAPIVersions,
		}},
		{version: 1, obj: &ApiVersionsResponse{
			ApiKeys:        SupportedAPIVersions,
			ThrottleTimeMs: 550,
		}},
		{version: 3, obj: &ApiVersionsResponse{
			ApiKeys:           SupportedAPIVersions,
			SupportedFeatures: []ApiVersionsResponseSupportedFeatureKey{},
			FinalizedFeatures: []ApiVersionsResponseFinalizedFeatureKey{},
		}},
		{version: 3, obj: &ApiVersionsResponse{
			ApiKeys:        SupportedAPIVersions,
			ThrottleTimeMs: 550,
			SupportedFeatures: []ApiVersionsResponseSupportedFeatureKey{
				{Name: stringPtr("feature1"), MinVersion: 2, MaxVersion: 5},
				{Name: stringPtr("feature2"), MinVersion: 3, MaxVersion: 7},
				{Name: stringPtr("feature3"), MinVersion: 1, MaxVersion: -1},
			},
			FinalizedFeaturesEpoch: 12345,
			FinalizedFeatures: []ApiVersionsResponseFinalizedFeatureKey{
				{Name: stringPtr("feature1"), MinVersionLevel: 1, MaxVersionLevel: 4},
				{Name: stringPtr("feature2"), MinVersionLevel: 0, MaxVersionLevel: 7},
				{Name: stringPtr("feature3"), MinVersionLevel: 2, MaxVersionLevel: -1},
			},
			ZkMigrationReady: true,
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestFetchRequest(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &FetchRequest{
			ReplicaId: 1001,
			MaxWaitMs: 250,
			MinBytes:  65536,
			Topics: []FetchRequestFetchTopic{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 1, FetchOffset: 230000, PartitionMaxBytes: 4453453},
						{Partition: 5, FetchOffset: 576484, PartitionMaxBytes: 575757},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 7, FetchOffset: 266000, PartitionMaxBytes: 723663},
						{Partition: 9, FetchOffset: 5978484, PartitionMaxBytes: 56595},
					},
				},
			},
		}},
		{version: 3, obj: &FetchRequest{
			ReplicaId: 1001,
			MaxWaitMs: 250,
			MinBytes:  65536,
			MaxBytes:  4746464,
			Topics: []FetchRequestFetchTopic{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 1, FetchOffset: 230000, PartitionMaxBytes: 4453453},
						{Partition: 5, FetchOffset: 576484, PartitionMaxBytes: 575757},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 7, FetchOffset: 266000, PartitionMaxBytes: 723663},
						{Partition: 9, FetchOffset: 5978484, PartitionMaxBytes: 56595},
					},
				},
			},
		}},
		{version: 4, obj: &FetchRequest{
			ReplicaId:      1001,
			MaxWaitMs:      250,
			MinBytes:       65536,
			MaxBytes:       4746464,
			IsolationLevel: 3,
			Topics: []FetchRequestFetchTopic{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 1, FetchOffset: 230000, PartitionMaxBytes: 4453453},
						{Partition: 5, FetchOffset: 576484, PartitionMaxBytes: 575757},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 7, FetchOffset: 266000, PartitionMaxBytes: 723663},
						{Partition: 9, FetchOffset: 5978484, PartitionMaxBytes: 56595},
					},
				},
			},
		}},
		{version: 5, obj: &FetchRequest{
			ReplicaId:      1001,
			MaxWaitMs:      250,
			MinBytes:       65536,
			MaxBytes:       4746464,
			IsolationLevel: 3,
			Topics: []FetchRequestFetchTopic{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 1, FetchOffset: 230000, PartitionMaxBytes: 4453453, LogStartOffset: 120000},
						{Partition: 5, FetchOffset: 576484, PartitionMaxBytes: 575757, LogStartOffset: 36358888},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 7, FetchOffset: 266000, PartitionMaxBytes: 723663, LogStartOffset: 4363636},
						{Partition: 9, FetchOffset: 5978484, PartitionMaxBytes: 56595, LogStartOffset: 4743545},
					},
				},
			},
		}},
		{version: 7, obj: &FetchRequest{
			ReplicaId:      1001,
			MaxWaitMs:      250,
			MinBytes:       65536,
			MaxBytes:       4746464,
			IsolationLevel: 3,
			SessionId:      45464,
			SessionEpoch:   456,
			Topics: []FetchRequestFetchTopic{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 1, FetchOffset: 230000, PartitionMaxBytes: 4453453, LogStartOffset: 120000},
						{Partition: 5, FetchOffset: 576484, PartitionMaxBytes: 575757, LogStartOffset: 36358888},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 7, FetchOffset: 266000, PartitionMaxBytes: 723663, LogStartOffset: 4363636},
						{Partition: 9, FetchOffset: 5978484, PartitionMaxBytes: 56595, LogStartOffset: 4743545},
					},
				},
			},
			ForgottenTopicsData: []FetchRequestForgottenTopic{
				{Topic: stringPtr("topic3"), Partitions: []int32{34, 456, 6, 7}},
				{Topic: stringPtr("topic4"), Partitions: []int32{34, 456, 6, 7}},
			},
		}},
		{version: 9, obj: &FetchRequest{
			ReplicaId:      1001,
			MaxWaitMs:      250,
			MinBytes:       65536,
			MaxBytes:       4746464,
			IsolationLevel: 3,
			SessionId:      45464,
			SessionEpoch:   456,
			Topics: []FetchRequestFetchTopic{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 1, FetchOffset: 230000, PartitionMaxBytes: 4453453, LogStartOffset: 120000, CurrentLeaderEpoch: 12},
						{Partition: 5, FetchOffset: 576484, PartitionMaxBytes: 575757, LogStartOffset: 36358888, CurrentLeaderEpoch: 43},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 7, FetchOffset: 266000, PartitionMaxBytes: 723663, LogStartOffset: 4363636, CurrentLeaderEpoch: 1},
						{Partition: 9, FetchOffset: 5978484, PartitionMaxBytes: 56595, LogStartOffset: 4743545, CurrentLeaderEpoch: 67},
					},
				},
			},
			ForgottenTopicsData: []FetchRequestForgottenTopic{
				{Topic: stringPtr("topic3"), Partitions: []int32{34, 456, 6, 7}},
				{Topic: stringPtr("topic4"), Partitions: []int32{34, 456, 6, 7}},
			},
		}},
		{version: 11, obj: &FetchRequest{
			ReplicaId:      1001,
			MaxWaitMs:      250,
			MinBytes:       65536,
			MaxBytes:       4746464,
			IsolationLevel: 3,
			SessionId:      45464,
			SessionEpoch:   456,
			Topics: []FetchRequestFetchTopic{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 1, FetchOffset: 230000, PartitionMaxBytes: 4453453, LogStartOffset: 120000, CurrentLeaderEpoch: 12},
						{Partition: 5, FetchOffset: 576484, PartitionMaxBytes: 575757, LogStartOffset: 36358888, CurrentLeaderEpoch: 43},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 7, FetchOffset: 266000, PartitionMaxBytes: 723663, LogStartOffset: 4363636, CurrentLeaderEpoch: 1},
						{Partition: 9, FetchOffset: 5978484, PartitionMaxBytes: 56595, LogStartOffset: 4743545, CurrentLeaderEpoch: 67},
					},
				},
			},
			ForgottenTopicsData: []FetchRequestForgottenTopic{
				{Topic: stringPtr("topic3"), Partitions: []int32{34, 456, 6, 7}},
				{Topic: stringPtr("topic4"), Partitions: []int32{34, 456, 6, 7}},
			},
			RackId: stringPtr("rack1"),
		}},
		{version: 12, obj: &FetchRequest{
			ReplicaId:      1001,
			MaxWaitMs:      250,
			MinBytes:       65536,
			MaxBytes:       4746464,
			IsolationLevel: 3,
			SessionId:      45464,
			SessionEpoch:   456,
			Topics: []FetchRequestFetchTopic{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 1, FetchOffset: 230000, PartitionMaxBytes: 4453453, LogStartOffset: 120000, CurrentLeaderEpoch: 12, LastFetchedEpoch: 13},
						{Partition: 5, FetchOffset: 576484, PartitionMaxBytes: 575757, LogStartOffset: 36358888, CurrentLeaderEpoch: 43, LastFetchedEpoch: 67},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 7, FetchOffset: 266000, PartitionMaxBytes: 723663, LogStartOffset: 4363636, CurrentLeaderEpoch: 1, LastFetchedEpoch: 3},
						{Partition: 9, FetchOffset: 5978484, PartitionMaxBytes: 56595, LogStartOffset: 4743545, CurrentLeaderEpoch: 67, LastFetchedEpoch: 5},
					},
				},
			},
			ForgottenTopicsData: []FetchRequestForgottenTopic{
				{Topic: stringPtr("topic3"), Partitions: []int32{34, 456, 6, 7}},
				{Topic: stringPtr("topic4"), Partitions: []int32{34, 456, 6, 7}},
			},
			RackId: stringPtr("rack1"),
		}},
		{version: 12, obj: &FetchRequest{
			ReplicaId:      1001,
			MaxWaitMs:      250,
			MinBytes:       65536,
			MaxBytes:       4746464,
			IsolationLevel: 3,
			SessionId:      45464,
			SessionEpoch:   456,
			ClusterId:      stringPtr("someclusterid"),
			Topics: []FetchRequestFetchTopic{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 1, FetchOffset: 230000, PartitionMaxBytes: 4453453, LogStartOffset: 120000, CurrentLeaderEpoch: 12, LastFetchedEpoch: 13},
						{Partition: 5, FetchOffset: 576484, PartitionMaxBytes: 575757, LogStartOffset: 36358888, CurrentLeaderEpoch: 43, LastFetchedEpoch: 67},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 7, FetchOffset: 266000, PartitionMaxBytes: 723663, LogStartOffset: 4363636, CurrentLeaderEpoch: 1, LastFetchedEpoch: 3},
						{Partition: 9, FetchOffset: 5978484, PartitionMaxBytes: 56595, LogStartOffset: 4743545, CurrentLeaderEpoch: 67, LastFetchedEpoch: 5},
					},
				},
			},
			ForgottenTopicsData: []FetchRequestForgottenTopic{
				{Topic: stringPtr("topic3"), Partitions: []int32{34, 456, 6, 7}},
				{Topic: stringPtr("topic4"), Partitions: []int32{34, 456, 6, 7}},
			},
			RackId: stringPtr("rack1"),
		}},
		{version: 13, obj: &FetchRequest{
			ReplicaId:      1001,
			MaxWaitMs:      250,
			MinBytes:       65536,
			MaxBytes:       4746464,
			IsolationLevel: 3,
			SessionId:      45464,
			SessionEpoch:   456,
			ClusterId:      stringPtr("someclusterid"),
			Topics: []FetchRequestFetchTopic{
				{
					TopicId: randomUUID(),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 1, FetchOffset: 230000, PartitionMaxBytes: 4453453, LogStartOffset: 120000, CurrentLeaderEpoch: 12, LastFetchedEpoch: 13},
						{Partition: 5, FetchOffset: 576484, PartitionMaxBytes: 575757, LogStartOffset: 36358888, CurrentLeaderEpoch: 43, LastFetchedEpoch: 67},
					},
				},
				{
					TopicId: randomUUID(),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 7, FetchOffset: 266000, PartitionMaxBytes: 723663, LogStartOffset: 4363636, CurrentLeaderEpoch: 1, LastFetchedEpoch: 3},
						{Partition: 9, FetchOffset: 5978484, PartitionMaxBytes: 56595, LogStartOffset: 4743545, CurrentLeaderEpoch: 67, LastFetchedEpoch: 5},
					},
				},
			},
			ForgottenTopicsData: []FetchRequestForgottenTopic{
				{TopicId: randomUUID(), Partitions: []int32{34, 456, 6, 7}},
				{TopicId: randomUUID(), Partitions: []int32{34, 456, 6, 7}},
			},
			RackId: stringPtr("rack1"),
		}},
		{version: 15, obj: &FetchRequest{
			ReplicaState: FetchRequestReplicaState{
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
			Topics: []FetchRequestFetchTopic{
				{
					TopicId: randomUUID(),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 1, FetchOffset: 230000, PartitionMaxBytes: 4453453, LogStartOffset: 120000, CurrentLeaderEpoch: 12, LastFetchedEpoch: 13},
						{Partition: 5, FetchOffset: 576484, PartitionMaxBytes: 575757, LogStartOffset: 36358888, CurrentLeaderEpoch: 43, LastFetchedEpoch: 67},
					},
				},
				{
					TopicId: randomUUID(),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 7, FetchOffset: 266000, PartitionMaxBytes: 723663, LogStartOffset: 4363636, CurrentLeaderEpoch: 1, LastFetchedEpoch: 3},
						{Partition: 9, FetchOffset: 5978484, PartitionMaxBytes: 56595, LogStartOffset: 4743545, CurrentLeaderEpoch: 67, LastFetchedEpoch: 5},
					},
				},
			},
			ForgottenTopicsData: []FetchRequestForgottenTopic{
				{TopicId: randomUUID(), Partitions: []int32{34, 456, 6, 7}},
				{TopicId: randomUUID(), Partitions: []int32{34, 456, 6, 7}},
			},
			RackId: stringPtr("rack1"),
		}},
		{version: 17, obj: &FetchRequest{
			ReplicaState: FetchRequestReplicaState{
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
			Topics: []FetchRequestFetchTopic{
				{
					TopicId: randomUUID(),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 1, FetchOffset: 230000, PartitionMaxBytes: 4453453, LogStartOffset: 120000, CurrentLeaderEpoch: 12, LastFetchedEpoch: 13, ReplicaDirectoryId: randomUUID()},
						{Partition: 5, FetchOffset: 576484, PartitionMaxBytes: 575757, LogStartOffset: 36358888, CurrentLeaderEpoch: 43, LastFetchedEpoch: 67, ReplicaDirectoryId: randomUUID()},
					},
				},
				{
					TopicId: randomUUID(),
					Partitions: []FetchRequestFetchPartition{
						{Partition: 7, FetchOffset: 266000, PartitionMaxBytes: 723663, LogStartOffset: 4363636, CurrentLeaderEpoch: 1, LastFetchedEpoch: 3, ReplicaDirectoryId: randomUUID()},
						{Partition: 9, FetchOffset: 5978484, PartitionMaxBytes: 56595, LogStartOffset: 4743545, CurrentLeaderEpoch: 67, LastFetchedEpoch: 5, ReplicaDirectoryId: randomUUID()},
					},
				},
			},
			ForgottenTopicsData: []FetchRequestForgottenTopic{
				{TopicId: randomUUID(), Partitions: []int32{34, 456, 6, 7}},
				{TopicId: randomUUID(), Partitions: []int32{34, 456, 6, 7}},
			},
			RackId: stringPtr("rack1"),
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestFetchResponse(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &FetchResponse{
			Responses: []FetchResponseFetchableTopicResponse{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex: 1,
							HighWatermark:  12345,
							Records:        randomFetchRecords(1000),
						},
						{
							PartitionIndex: 3,
							HighWatermark:  32234,
							Records:        randomFetchRecords(1000),
						},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex: 4,
							HighWatermark:  3434,
							Records:        randomFetchRecords(1000),
						},
						{
							PartitionIndex: 9,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
					},
				},
			},
		}},
		{version: 1, obj: &FetchResponse{
			ThrottleTimeMs: 250,
			Responses: []FetchResponseFetchableTopicResponse{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex: 1,
							HighWatermark:  12345,
							Records:        randomFetchRecords(1000),
						},
						{
							PartitionIndex: 3,
							HighWatermark:  32234,
							Records:        randomFetchRecords(1000),
						},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex: 4,
							HighWatermark:  3434,
							Records:        randomFetchRecords(1000),
						},
						{
							PartitionIndex: 9,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
					},
				},
			},
		}},
		{version: 4, obj: &FetchResponse{
			ThrottleTimeMs: 250,
			Responses: []FetchResponseFetchableTopicResponse{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex:   1,
							HighWatermark:    12345,
							LastStableOffset: 12300,
							Records:          randomFetchRecords(1000),
							AbortedTransactions: []FetchResponseAbortedTransaction{
								{
									ProducerId:  123,
									FirstOffset: 12000,
								},
								{
									ProducerId:  100,
									FirstOffset: 11000,
								},
							},
						},
						{
							PartitionIndex:   3,
							HighWatermark:    32234,
							LastStableOffset: 32000,
							Records:          randomFetchRecords(1000),
						},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex:   4,
							HighWatermark:    3434,
							LastStableOffset: 3000,
							Records:          randomFetchRecords(1000),
							AbortedTransactions: []FetchResponseAbortedTransaction{
								{
									ProducerId:  777,
									FirstOffset: 14000,
								},
								{
									ProducerId:  333,
									FirstOffset: 10000,
								},
							},
						},
						{
							PartitionIndex: 9,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
					},
				},
			},
		}},
		{version: 5, obj: &FetchResponse{
			ThrottleTimeMs: 250,
			Responses: []FetchResponseFetchableTopicResponse{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex:   1,
							HighWatermark:    12345,
							LastStableOffset: 12300,
							LogStartOffset:   12000,
							Records:          randomFetchRecords(1000),
							AbortedTransactions: []FetchResponseAbortedTransaction{
								{
									ProducerId:  123,
									FirstOffset: 12000,
								},
								{
									ProducerId:  100,
									FirstOffset: 11000,
								},
							},
						},
						{
							PartitionIndex:   3,
							HighWatermark:    32234,
							LastStableOffset: 32000,
							LogStartOffset:   31000,
							Records:          randomFetchRecords(1000),
						},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex:   4,
							HighWatermark:    3434,
							LastStableOffset: 3000,
							LogStartOffset:   2500,
							Records:          randomFetchRecords(1000),
							AbortedTransactions: []FetchResponseAbortedTransaction{
								{
									ProducerId:  777,
									FirstOffset: 14000,
								},
								{
									ProducerId:  333,
									FirstOffset: 10000,
								},
							},
						},
						{
							PartitionIndex: 9,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
					},
				},
			},
		}},
		{version: 7, obj: &FetchResponse{
			ThrottleTimeMs: 250,
			ErrorCode:      ErrorCodeUnknownServerError,
			Responses:      []FetchResponseFetchableTopicResponse{},
		}},
		{version: 7, obj: &FetchResponse{
			ThrottleTimeMs: 250,
			SessionId:      777,
			Responses: []FetchResponseFetchableTopicResponse{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex:   1,
							HighWatermark:    12345,
							LastStableOffset: 12300,
							LogStartOffset:   12000,
							Records:          randomFetchRecords(1000),
							AbortedTransactions: []FetchResponseAbortedTransaction{
								{
									ProducerId:  123,
									FirstOffset: 12000,
								},
								{
									ProducerId:  100,
									FirstOffset: 11000,
								},
							},
						},
						{
							PartitionIndex:   3,
							HighWatermark:    32234,
							LastStableOffset: 32000,
							LogStartOffset:   31000,
							Records:          randomFetchRecords(1000),
						},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex:   4,
							HighWatermark:    3434,
							LastStableOffset: 3000,
							LogStartOffset:   2500,
							Records:          randomFetchRecords(1000),
							AbortedTransactions: []FetchResponseAbortedTransaction{
								{
									ProducerId:  777,
									FirstOffset: 14000,
								},
								{
									ProducerId:  333,
									FirstOffset: 10000,
								},
							},
						},
						{
							PartitionIndex: 9,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
					},
				},
			},
		}},
		{version: 12, obj: &FetchResponse{
			ThrottleTimeMs: 250,
			SessionId:      777,
			Responses: []FetchResponseFetchableTopicResponse{
				{
					Topic: stringPtr("topic1"),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex:   1,
							HighWatermark:    12345,
							LastStableOffset: 12300,
							LogStartOffset:   12000,
							Records:          randomFetchRecords(1000),
							AbortedTransactions: []FetchResponseAbortedTransaction{
								{
									ProducerId:  123,
									FirstOffset: 12000,
								},
								{
									ProducerId:  100,
									FirstOffset: 11000,
								},
							},
							DivergingEpoch: FetchResponseEpochEndOffset{
								Epoch:     32142,
								EndOffset: 34234234,
							},
							CurrentLeader: FetchResponseLeaderIdAndEpoch{
								LeaderId:    2,
								LeaderEpoch: 234,
							},
							SnapshotId: FetchResponseSnapshotId{
								EndOffset: 3212123,
								Epoch:     12,
							},
						},
						{
							PartitionIndex:   3,
							HighWatermark:    32234,
							LastStableOffset: 32000,
							LogStartOffset:   31000,
							Records:          randomFetchRecords(1000),
							CurrentLeader: FetchResponseLeaderIdAndEpoch{
								LeaderId:    2,
								LeaderEpoch: 235,
							},
							SnapshotId: FetchResponseSnapshotId{
								EndOffset: 234234,
								Epoch:     13,
							},
						},
					},
				},
				{
					Topic: stringPtr("topic2"),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex:   4,
							HighWatermark:    3434,
							LastStableOffset: 3000,
							LogStartOffset:   2500,
							Records:          randomFetchRecords(1000),
							AbortedTransactions: []FetchResponseAbortedTransaction{
								{
									ProducerId:  777,
									FirstOffset: 14000,
								},
								{
									ProducerId:  333,
									FirstOffset: 10000,
								},
							},
							DivergingEpoch: FetchResponseEpochEndOffset{
								Epoch:     343434,
								EndOffset: 5467456,
							},
							CurrentLeader: FetchResponseLeaderIdAndEpoch{
								LeaderId:    3,
								LeaderEpoch: 236,
							},
						},
						{
							PartitionIndex: 9,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
					},
				},
			},
		}},
		{version: 13, obj: &FetchResponse{
			ThrottleTimeMs: 250,
			SessionId:      777,
			Responses: []FetchResponseFetchableTopicResponse{
				{
					TopicId: randomUUID(),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex:   1,
							HighWatermark:    12345,
							LastStableOffset: 12300,
							LogStartOffset:   12000,
							Records:          randomFetchRecords(1000),
							AbortedTransactions: []FetchResponseAbortedTransaction{
								{
									ProducerId:  123,
									FirstOffset: 12000,
								},
								{
									ProducerId:  100,
									FirstOffset: 11000,
								},
							},
							DivergingEpoch: FetchResponseEpochEndOffset{
								Epoch:     32142,
								EndOffset: 34234234,
							},
							CurrentLeader: FetchResponseLeaderIdAndEpoch{
								LeaderId:    2,
								LeaderEpoch: 234,
							},
							SnapshotId: FetchResponseSnapshotId{
								EndOffset: 3212123,
								Epoch:     12,
							},
						},
						{
							PartitionIndex:   3,
							HighWatermark:    32234,
							LastStableOffset: 32000,
							LogStartOffset:   31000,
							Records:          randomFetchRecords(1000),
							CurrentLeader: FetchResponseLeaderIdAndEpoch{
								LeaderId:    2,
								LeaderEpoch: 235,
							},
							SnapshotId: FetchResponseSnapshotId{
								EndOffset: 234234,
								Epoch:     13,
							},
						},
					},
				},
				{
					TopicId: randomUUID(),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex:   4,
							HighWatermark:    3434,
							LastStableOffset: 3000,
							LogStartOffset:   2500,
							Records:          randomFetchRecords(1000),
							AbortedTransactions: []FetchResponseAbortedTransaction{
								{
									ProducerId:  777,
									FirstOffset: 14000,
								},
								{
									ProducerId:  333,
									FirstOffset: 10000,
								},
							},
							DivergingEpoch: FetchResponseEpochEndOffset{
								Epoch:     343434,
								EndOffset: 5467456,
							},
							CurrentLeader: FetchResponseLeaderIdAndEpoch{
								LeaderId:    3,
								LeaderEpoch: 236,
							},
						},
						{
							PartitionIndex: 9,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
					},
				},
			},
		}},
		{version: 16, obj: &FetchResponse{
			ThrottleTimeMs: 250,
			SessionId:      777,
			Responses: []FetchResponseFetchableTopicResponse{
				{
					TopicId: randomUUID(),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex:   1,
							HighWatermark:    12345,
							LastStableOffset: 12300,
							LogStartOffset:   12000,
							Records:          randomFetchRecords(1000),
							AbortedTransactions: []FetchResponseAbortedTransaction{
								{
									ProducerId:  123,
									FirstOffset: 12000,
								},
								{
									ProducerId:  100,
									FirstOffset: 11000,
								},
							},
							DivergingEpoch: FetchResponseEpochEndOffset{
								Epoch:     32142,
								EndOffset: 34234234,
							},
							CurrentLeader: FetchResponseLeaderIdAndEpoch{
								LeaderId:    2,
								LeaderEpoch: 234,
							},
							SnapshotId: FetchResponseSnapshotId{
								EndOffset: 3212123,
								Epoch:     12,
							},
						},
						{
							PartitionIndex:   3,
							HighWatermark:    32234,
							LastStableOffset: 32000,
							LogStartOffset:   31000,
							Records:          randomFetchRecords(1000),
							CurrentLeader: FetchResponseLeaderIdAndEpoch{
								LeaderId:    2,
								LeaderEpoch: 235,
							},
							SnapshotId: FetchResponseSnapshotId{
								EndOffset: 234234,
								Epoch:     13,
							},
						},
					},
				},
				{
					TopicId: randomUUID(),
					Partitions: []FetchResponsePartitionData{
						{
							PartitionIndex:   4,
							HighWatermark:    3434,
							LastStableOffset: 3000,
							LogStartOffset:   2500,
							Records:          randomFetchRecords(1000),
							AbortedTransactions: []FetchResponseAbortedTransaction{
								{
									ProducerId:  777,
									FirstOffset: 14000,
								},
								{
									ProducerId:  333,
									FirstOffset: 10000,
								},
							},
							DivergingEpoch: FetchResponseEpochEndOffset{
								Epoch:     343434,
								EndOffset: 5467456,
							},
							CurrentLeader: FetchResponseLeaderIdAndEpoch{
								LeaderId:    3,
								LeaderEpoch: 236,
							},
						},
						{
							PartitionIndex: 9,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
					},
				},
			},
			NodeEndpoints: []FetchResponseNodeEndpoint{
				{
					NodeId: 0,
					Host:   stringPtr("node1"),
					Port:   12345,
					Rack:   stringPtr("rack1"),
				},
				{
					NodeId: 1,
					Host:   stringPtr("node2"),
					Port:   12346,
					Rack:   stringPtr("rack2"),
				},
				{
					NodeId: 3,
					Host:   stringPtr("node3"),
					Port:   12347,
					Rack:   stringPtr("rack3"),
				},
			},
		}},
	}
	testReadWriteCasesWithPrecompareFunc(t, testCases, func(tc *readWriteCase) {
		// There's a special case with FetchResponse - we can write multiple buffers for records, but we read as a single
		// buffer, so we need to combine buffers before comparison
		resp := tc.obj.(*FetchResponse)
		for i := 0; i < len(resp.Responses); i++ {
			for j := 0; j < len(resp.Responses[i].Partitions); j++ {
				if resp.Responses[i].Partitions[j].Records != nil {
					var totBuff []byte
					for _, buff := range resp.Responses[i].Partitions[j].Records {
						totBuff = append(totBuff, buff...)
					}
					resp.Responses[i].Partitions[j].Records = [][]byte{totBuff}
				}
			}
		}
	})
}

func TestFindCoordinatorRequest(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &FindCoordinatorRequest{
			Key: stringPtr("key1"),
		}},
		{version: 1, obj: &FindCoordinatorRequest{
			Key:     stringPtr("key1"),
			KeyType: 23,
		}},
		{version: 3, obj: &FindCoordinatorRequest{
			Key:     stringPtr("key1"),
			KeyType: 23,
		}},
		{version: 4, obj: &FindCoordinatorRequest{
			KeyType: 23,
			CoordinatorKeys: []*string{
				stringPtr("key2"),
				stringPtr("key3"),
			},
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestFindCoordinatorResponse(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &FindCoordinatorResponse{
			NodeId: 1,
			Host:   stringPtr("node1"),
			Port:   12345,
		}},
		{version: 0, obj: &FindCoordinatorResponse{
			ErrorCode: ErrorCodeLeaderNotAvailable,
			NodeId:    1,
			Host:      stringPtr("node1"),
			Port:      12345,
		}},
		{version: 1, obj: &FindCoordinatorResponse{
			NodeId:         1,
			Host:           stringPtr("node1"),
			Port:           12345,
			ThrottleTimeMs: 250,
		}},
		{version: 1, obj: &FindCoordinatorResponse{
			ErrorCode:    ErrorCodeLeaderNotAvailable,
			ErrorMessage: stringPtr("blah"),
			NodeId:       1,
			Host:         stringPtr("node1"),
			Port:         12345,
		}},
		{version: 3, obj: &FindCoordinatorResponse{
			NodeId:         1,
			Host:           stringPtr("node1"),
			Port:           12345,
			ThrottleTimeMs: 250,
		}},
		{version: 4, obj: &FindCoordinatorResponse{
			ThrottleTimeMs: 250,
			Coordinators: []FindCoordinatorResponseCoordinator{
				{
					Key:    stringPtr("key1"),
					NodeId: 1,
					Host:   stringPtr("host1"),
					Port:   12345,
				},
				{
					ErrorCode:    ErrorCodeLeaderNotAvailable,
					ErrorMessage: stringPtr("blah"),
					Key:          stringPtr("key2"),
					NodeId:       2,
					Host:         stringPtr("node2"),
					Port:         12346,
				},
			},
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestHeartbeatRequest(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &HeartbeatRequest{
			GroupId:      stringPtr("group1"),
			GenerationId: 123,
			MemberId:     stringPtr("member1"),
		}},
		{version: 3, obj: &HeartbeatRequest{
			GroupId:         stringPtr("group1"),
			GenerationId:    123,
			MemberId:        stringPtr("member1"),
			GroupInstanceId: stringPtr("instance1"),
		}},
		{version: 3, obj: &HeartbeatRequest{
			GroupId:      stringPtr("group1"),
			GenerationId: 123,
			MemberId:     stringPtr("member1"),
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestHeartbeatResponse(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &HeartbeatResponse{
			ErrorCode: ErrorCodeUnknownServerError,
		}},
		{version: 0, obj: &HeartbeatResponse{}},
		{version: 1, obj: &HeartbeatResponse{
			ThrottleTimeMs: 300,
		}},
		{version: 4, obj: &HeartbeatResponse{
			ThrottleTimeMs: 300,
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestJoinGroupRequest(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &JoinGroupRequest{
			GroupId:          stringPtr("group1"),
			SessionTimeoutMs: 12000,
			MemberId:         stringPtr("member1"),
			ProtocolType:     stringPtr("protocoltype1"),
			Protocols: []JoinGroupRequestJoinGroupRequestProtocol{
				{
					Name:     stringPtr("protocol1"),
					Metadata: randomBytes(100),
				},
				{
					Name:     stringPtr("protocol2"),
					Metadata: randomBytes(100),
				},
			},
		}},
		{version: 5, obj: &JoinGroupRequest{
			GroupId:          stringPtr("group1"),
			GroupInstanceId:  stringPtr("instance1"),
			SessionTimeoutMs: 12000,
			MemberId:         stringPtr("member1"),
			ProtocolType:     stringPtr("protocoltype1"),
			Protocols: []JoinGroupRequestJoinGroupRequestProtocol{
				{
					Name:     stringPtr("protocol1"),
					Metadata: randomBytes(100),
				},
				{
					Name:     stringPtr("protocol2"),
					Metadata: randomBytes(100),
				},
			},
		}},
		{version: 5, obj: &JoinGroupRequest{
			GroupId:          stringPtr("group1"),
			SessionTimeoutMs: 12000,
			MemberId:         stringPtr("member1"),
			ProtocolType:     stringPtr("protocoltype1"),
			Protocols: []JoinGroupRequestJoinGroupRequestProtocol{
				{
					Name:     stringPtr("protocol1"),
					Metadata: randomBytes(100),
				},
				{
					Name:     stringPtr("protocol2"),
					Metadata: randomBytes(100),
				},
			},
		}},
		{version: 6, obj: &JoinGroupRequest{
			GroupId:          stringPtr("group1"),
			GroupInstanceId:  stringPtr("instance1"),
			SessionTimeoutMs: 12000,
			MemberId:         stringPtr("member1"),
			ProtocolType:     stringPtr("protocoltype1"),
			Protocols: []JoinGroupRequestJoinGroupRequestProtocol{
				{
					Name:     stringPtr("protocol1"),
					Metadata: randomBytes(100),
				},
				{
					Name:     stringPtr("protocol2"),
					Metadata: randomBytes(100),
				},
			},
		}},
		{version: 8, obj: &JoinGroupRequest{
			GroupId:          stringPtr("group1"),
			GroupInstanceId:  stringPtr("instance1"),
			SessionTimeoutMs: 12000,
			MemberId:         stringPtr("member1"),
			ProtocolType:     stringPtr("protocoltype1"),
			Protocols: []JoinGroupRequestJoinGroupRequestProtocol{
				{
					Name:     stringPtr("protocol1"),
					Metadata: randomBytes(100),
				},
				{
					Name:     stringPtr("protocol2"),
					Metadata: randomBytes(100),
				},
			},
			Reason: stringPtr("ufos have landed"),
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestJoinGroupResponse(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &JoinGroupResponse{
			GenerationId: 12,
			ProtocolName: stringPtr("protocol1"),
			Leader:       stringPtr("leader1"),
			MemberId:     stringPtr("member1"),
			Members: []JoinGroupResponseJoinGroupResponseMember{
				{
					MemberId: stringPtr("member2"),
					Metadata: randomBytes(100),
				},
				{
					MemberId: stringPtr("member3"),
					Metadata: randomBytes(100),
				},
			},
		}},
		{version: 0, obj: &JoinGroupResponse{
			GenerationId: 12,
			ProtocolName: stringPtr("protocol1"),
			Leader:       stringPtr("leader1"),
			MemberId:     stringPtr("member1"),
			ErrorCode:    ErrorCodeUnknownServerError,
			Members:      []JoinGroupResponseJoinGroupResponseMember{},
		}},
		{version: 2, obj: &JoinGroupResponse{
			GenerationId: 12,
			ProtocolName: stringPtr("protocol1"),
			Leader:       stringPtr("leader1"),
			MemberId:     stringPtr("member1"),
			Members: []JoinGroupResponseJoinGroupResponseMember{
				{
					MemberId: stringPtr("member2"),
					Metadata: randomBytes(100),
				},
				{
					MemberId: stringPtr("member3"),
					Metadata: randomBytes(100),
				},
			},
			ThrottleTimeMs: 333,
		}},
		{version: 5, obj: &JoinGroupResponse{
			GenerationId: 12,
			ProtocolName: stringPtr("protocol1"),
			Leader:       stringPtr("leader1"),
			MemberId:     stringPtr("member1"),
			Members: []JoinGroupResponseJoinGroupResponseMember{
				{
					MemberId:        stringPtr("member2"),
					Metadata:        randomBytes(100),
					GroupInstanceId: stringPtr("instance1"),
				},
				{
					MemberId:        stringPtr("member3"),
					Metadata:        randomBytes(100),
					GroupInstanceId: stringPtr("instance2"),
				},
			},
			ThrottleTimeMs: 333,
		}},
		{version: 6, obj: &JoinGroupResponse{
			GenerationId: 12,
			ProtocolName: stringPtr("protocol1"),
			Leader:       stringPtr("leader1"),
			MemberId:     stringPtr("member1"),
			Members: []JoinGroupResponseJoinGroupResponseMember{
				{
					MemberId:        stringPtr("member2"),
					Metadata:        randomBytes(100),
					GroupInstanceId: stringPtr("instance1"),
				},
				{
					MemberId:        stringPtr("member3"),
					Metadata:        randomBytes(100),
					GroupInstanceId: stringPtr("instance2"),
				},
			},
			ThrottleTimeMs: 333,
		}},
		{version: 7, obj: &JoinGroupResponse{
			GenerationId: 12,
			ProtocolName: stringPtr("protocol1"),
			ProtocolType: stringPtr("protocoltype1"),
			Leader:       stringPtr("leader1"),
			MemberId:     stringPtr("member1"),
			Members: []JoinGroupResponseJoinGroupResponseMember{
				{
					MemberId:        stringPtr("member2"),
					Metadata:        randomBytes(100),
					GroupInstanceId: stringPtr("instance1"),
				},
				{
					MemberId:        stringPtr("member3"),
					Metadata:        randomBytes(100),
					GroupInstanceId: stringPtr("instance2"),
				},
			},
			ThrottleTimeMs: 333,
		}},
		{version: 7, obj: &JoinGroupResponse{
			GenerationId: 12,
			ProtocolName: stringPtr("protocol1"),
			Leader:       stringPtr("leader1"),
			MemberId:     stringPtr("member1"),
			Members: []JoinGroupResponseJoinGroupResponseMember{
				{
					MemberId:        stringPtr("member2"),
					Metadata:        randomBytes(100),
					GroupInstanceId: stringPtr("instance1"),
				},
				{
					MemberId:        stringPtr("member3"),
					Metadata:        randomBytes(100),
					GroupInstanceId: stringPtr("instance2"),
				},
			},
			ThrottleTimeMs: 333,
		}},
		{version: 9, obj: &JoinGroupResponse{
			GenerationId: 12,
			ProtocolName: stringPtr("protocol1"),
			Leader:       stringPtr("leader1"),
			MemberId:     stringPtr("member1"),
			Members: []JoinGroupResponseJoinGroupResponseMember{
				{
					MemberId:        stringPtr("member2"),
					Metadata:        randomBytes(100),
					GroupInstanceId: stringPtr("instance1"),
				},
				{
					MemberId:        stringPtr("member3"),
					Metadata:        randomBytes(100),
					GroupInstanceId: stringPtr("instance2"),
				},
			},
			ThrottleTimeMs: 333,
			SkipAssignment: true,
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestLeaveGroupRequest(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &LeaveGroupRequest{
			GroupId:  stringPtr("group1"),
			MemberId: stringPtr("member1"),
		}},
		{version: 3, obj: &LeaveGroupRequest{
			GroupId: stringPtr("group1"),
			Members: []LeaveGroupRequestMemberIdentity{
				{
					MemberId:        stringPtr("member2"),
					GroupInstanceId: stringPtr("instance2"),
				},
				{
					MemberId:        stringPtr("member3"),
					GroupInstanceId: stringPtr("instance2"),
				},
			},
		}},
		{version: 4, obj: &LeaveGroupRequest{
			GroupId: stringPtr("group1"),
			Members: []LeaveGroupRequestMemberIdentity{
				{
					MemberId:        stringPtr("member2"),
					GroupInstanceId: stringPtr("instance2"),
				},
				{
					MemberId:        stringPtr("member3"),
					GroupInstanceId: stringPtr("instance2"),
				},
			},
		}},
		{version: 5, obj: &LeaveGroupRequest{
			GroupId: stringPtr("group1"),
			Members: []LeaveGroupRequestMemberIdentity{
				{
					MemberId:        stringPtr("member2"),
					GroupInstanceId: stringPtr("instance2"),
					Reason:          stringPtr("dog ate homework"),
				},
				{
					MemberId:        stringPtr("member3"),
					GroupInstanceId: stringPtr("instance2"),
					Reason:          stringPtr("kippers"),
				},
			},
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestLeaveGroupResponse(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &LeaveGroupResponse{}},
		{version: 0, obj: &LeaveGroupResponse{
			ErrorCode: ErrorCodeUnknownMemberID,
		}},
		{version: 1, obj: &LeaveGroupResponse{
			ThrottleTimeMs: 777,
		}},
		{version: 3, obj: &LeaveGroupResponse{
			ThrottleTimeMs: 777,
			Members: []LeaveGroupResponseMemberResponse{
				{
					MemberId:        stringPtr("member1"),
					GroupInstanceId: stringPtr("instance1"),
				},
				{
					MemberId:        stringPtr("member2"),
					GroupInstanceId: stringPtr("instance2"),
				},
			},
		}},
		{version: 3, obj: &LeaveGroupResponse{
			ThrottleTimeMs: 777,
			Members: []LeaveGroupResponseMemberResponse{
				{
					MemberId: stringPtr("member1"),
				},
				{
					MemberId: stringPtr("member2"),
				},
			},
		}},
		{version: 3, obj: &LeaveGroupResponse{
			ThrottleTimeMs: 777,
			Members: []LeaveGroupResponseMemberResponse{
				{
					MemberId:  stringPtr("member1"),
					ErrorCode: ErrorCodeUnknownMemberID,
				},
				{
					MemberId:        stringPtr("member2"),
					GroupInstanceId: stringPtr("instance2"),
					ErrorCode:       ErrorCodeUnknownMemberID,
				},
			},
		}},
		{version: 4, obj: &LeaveGroupResponse{
			ThrottleTimeMs: 777,
			Members: []LeaveGroupResponseMemberResponse{
				{
					MemberId:        stringPtr("member1"),
					GroupInstanceId: stringPtr("instance1"),
				},
				{
					MemberId:        stringPtr("member2"),
					GroupInstanceId: stringPtr("instance2"),
				},
			},
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestListOffsetsRequest(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &ListOffsetsRequest{
			ReplicaId: 123,
			Topics: []ListOffsetsRequestListOffsetsTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []ListOffsetsRequestListOffsetsPartition{
						{
							PartitionIndex: 1,
							Timestamp:      123456,
							MaxNumOffsets:  12,
						},
						{
							PartitionIndex: 3,
							Timestamp:      356353,
							MaxNumOffsets:  23,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []ListOffsetsRequestListOffsetsPartition{
						{
							PartitionIndex: 2,
							Timestamp:      345345,
							MaxNumOffsets:  56,
						},
						{
							PartitionIndex: 5,
							Timestamp:      23323,
							MaxNumOffsets:  3,
						},
					},
				},
			},
		}},
		{version: 1, obj: &ListOffsetsRequest{
			ReplicaId: 123,
			Topics: []ListOffsetsRequestListOffsetsTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []ListOffsetsRequestListOffsetsPartition{
						{
							PartitionIndex: 1,
							Timestamp:      123456,
						},
						{
							PartitionIndex: 3,
							Timestamp:      356353,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []ListOffsetsRequestListOffsetsPartition{
						{
							PartitionIndex: 2,
							Timestamp:      345345,
						},
						{
							PartitionIndex: 5,
							Timestamp:      23323,
						},
					},
				},
			},
		}},
		{version: 2, obj: &ListOffsetsRequest{
			ReplicaId:      123,
			IsolationLevel: 2,
			Topics: []ListOffsetsRequestListOffsetsTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []ListOffsetsRequestListOffsetsPartition{
						{
							PartitionIndex: 1,
							Timestamp:      123456,
						},
						{
							PartitionIndex: 3,
							Timestamp:      356353,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []ListOffsetsRequestListOffsetsPartition{
						{
							PartitionIndex: 2,
							Timestamp:      345345,
						},
						{
							PartitionIndex: 5,
							Timestamp:      23323,
						},
					},
				},
			},
		}},
		{version: 4, obj: &ListOffsetsRequest{
			ReplicaId:      123,
			IsolationLevel: 2,
			Topics: []ListOffsetsRequestListOffsetsTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []ListOffsetsRequestListOffsetsPartition{
						{
							PartitionIndex:     1,
							Timestamp:          123456,
							CurrentLeaderEpoch: 12,
						},
						{
							PartitionIndex:     3,
							Timestamp:          356353,
							CurrentLeaderEpoch: 4,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []ListOffsetsRequestListOffsetsPartition{
						{
							PartitionIndex:     2,
							Timestamp:          345345,
							CurrentLeaderEpoch: 44,
						},
						{
							PartitionIndex:     5,
							Timestamp:          23323,
							CurrentLeaderEpoch: 6,
						},
					},
				},
			},
		}},
		{version: 6, obj: &ListOffsetsRequest{
			ReplicaId:      123,
			IsolationLevel: 2,
			Topics: []ListOffsetsRequestListOffsetsTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []ListOffsetsRequestListOffsetsPartition{
						{
							PartitionIndex:     1,
							Timestamp:          123456,
							CurrentLeaderEpoch: 12,
						},
						{
							PartitionIndex:     3,
							Timestamp:          356353,
							CurrentLeaderEpoch: 4,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []ListOffsetsRequestListOffsetsPartition{
						{
							PartitionIndex:     2,
							Timestamp:          345345,
							CurrentLeaderEpoch: 44,
						},
						{
							PartitionIndex:     5,
							Timestamp:          23323,
							CurrentLeaderEpoch: 6,
						},
					},
				},
			},
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestListOffsetsResponse(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &ListOffsetsResponse{
			Topics: []ListOffsetsResponseListOffsetsTopicResponse{
				{
					Name: stringPtr("topic1"),
					Partitions: []ListOffsetsResponseListOffsetsPartitionResponse{
						{
							PartitionIndex:  1,
							OldStyleOffsets: []int64{34, 544557, 21123123},
						},
						{
							PartitionIndex:  2,
							OldStyleOffsets: []int64{345345, 345345345, 4564564564},
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []ListOffsetsResponseListOffsetsPartitionResponse{
						{
							PartitionIndex:  3,
							OldStyleOffsets: []int64{34534345, 43345, 56765},
						},
						{
							PartitionIndex:  7,
							OldStyleOffsets: []int64{8978, 3456346, 435346},
						},
					},
				},
			},
		}},
		{version: 0, obj: &ListOffsetsResponse{
			Topics: []ListOffsetsResponseListOffsetsTopicResponse{
				{
					Name: stringPtr("topic1"),
					Partitions: []ListOffsetsResponseListOffsetsPartitionResponse{
						{
							PartitionIndex:  1,
							ErrorCode:       ErrorCodeUnknownTopicOrPartition,
							OldStyleOffsets: []int64{},
						},
						{
							PartitionIndex:  2,
							ErrorCode:       ErrorCodeUnknownTopicOrPartition,
							OldStyleOffsets: []int64{},
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []ListOffsetsResponseListOffsetsPartitionResponse{
						{
							PartitionIndex:  3,
							ErrorCode:       ErrorCodeUnknownTopicOrPartition,
							OldStyleOffsets: []int64{},
						},
						{
							PartitionIndex:  7,
							ErrorCode:       ErrorCodeUnknownTopicOrPartition,
							OldStyleOffsets: []int64{},
						},
					},
				},
			},
		}},
		{version: 1, obj: &ListOffsetsResponse{
			Topics: []ListOffsetsResponseListOffsetsTopicResponse{
				{
					Name: stringPtr("topic1"),
					Partitions: []ListOffsetsResponseListOffsetsPartitionResponse{
						{
							PartitionIndex: 1,
							Timestamp:      34234234,
							Offset:         234234,
						},
						{
							PartitionIndex: 2,
							Timestamp:      456456,
							Offset:         2178334,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []ListOffsetsResponseListOffsetsPartitionResponse{
						{
							PartitionIndex: 3,
							Timestamp:      8237234,
							Offset:         238472387432,
						},
						{
							PartitionIndex: 7,
							Timestamp:      45363456,
							Offset:         34534345,
						},
					},
				},
			},
		}},
		{version: 2, obj: &ListOffsetsResponse{
			ThrottleTimeMs: 333,
			Topics: []ListOffsetsResponseListOffsetsTopicResponse{
				{
					Name: stringPtr("topic1"),
					Partitions: []ListOffsetsResponseListOffsetsPartitionResponse{
						{
							PartitionIndex: 1,
							Timestamp:      34234234,
							Offset:         234234,
						},
						{
							PartitionIndex: 2,
							Timestamp:      456456,
							Offset:         2178334,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []ListOffsetsResponseListOffsetsPartitionResponse{
						{
							PartitionIndex: 3,
							Timestamp:      8237234,
							Offset:         238472387432,
						},
						{
							PartitionIndex: 7,
							Timestamp:      45363456,
							Offset:         34534345,
						},
					},
				},
			},
		}},
		{version: 4, obj: &ListOffsetsResponse{
			ThrottleTimeMs: 333,
			Topics: []ListOffsetsResponseListOffsetsTopicResponse{
				{
					Name: stringPtr("topic1"),
					Partitions: []ListOffsetsResponseListOffsetsPartitionResponse{
						{
							PartitionIndex: 1,
							Timestamp:      34234234,
							Offset:         234234,
							LeaderEpoch:    12,
						},
						{
							PartitionIndex: 2,
							Timestamp:      456456,
							Offset:         2178334,
							LeaderEpoch:    65,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []ListOffsetsResponseListOffsetsPartitionResponse{
						{
							PartitionIndex: 3,
							Timestamp:      8237234,
							Offset:         238472387432,
							LeaderEpoch:    3,
						},
						{
							PartitionIndex: 7,
							Timestamp:      45363456,
							Offset:         34534345,
							LeaderEpoch:    76,
						},
					},
				},
			},
		}},
		{version: 6, obj: &ListOffsetsResponse{
			ThrottleTimeMs: 333,
			Topics: []ListOffsetsResponseListOffsetsTopicResponse{
				{
					Name: stringPtr("topic1"),
					Partitions: []ListOffsetsResponseListOffsetsPartitionResponse{
						{
							PartitionIndex: 1,
							Timestamp:      34234234,
							Offset:         234234,
							LeaderEpoch:    12,
						},
						{
							PartitionIndex: 2,
							Timestamp:      456456,
							Offset:         2178334,
							LeaderEpoch:    65,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []ListOffsetsResponseListOffsetsPartitionResponse{
						{
							PartitionIndex: 3,
							Timestamp:      8237234,
							Offset:         238472387432,
							LeaderEpoch:    3,
						},
						{
							PartitionIndex: 7,
							Timestamp:      45363456,
							Offset:         34534345,
							LeaderEpoch:    76,
						},
					},
				},
			},
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestMetadataRequest(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &MetadataRequest{
			Topics: []MetadataRequestMetadataRequestTopic{
				{
					Name: stringPtr("topic1"),
				},
			},
		}},
		{version: 0, obj: &MetadataRequest{
			Topics: []MetadataRequestMetadataRequestTopic{},
		}},
		{version: 4, obj: &MetadataRequest{
			AllowAutoTopicCreation: true,
			Topics: []MetadataRequestMetadataRequestTopic{
				{
					Name: stringPtr("topic1"),
				},
			},
		}},
		{version: 8, obj: &MetadataRequest{
			AllowAutoTopicCreation: true,
			Topics: []MetadataRequestMetadataRequestTopic{
				{
					Name: stringPtr("topic1"),
				},
			},
			IncludeClusterAuthorizedOperations: true,
			IncludeTopicAuthorizedOperations:   true,
		}},
		{version: 9, obj: &MetadataRequest{
			AllowAutoTopicCreation: true,
			Topics: []MetadataRequestMetadataRequestTopic{
				{
					Name: stringPtr("topic1"),
				},
			},
			IncludeClusterAuthorizedOperations: true,
			IncludeTopicAuthorizedOperations:   true,
		}},
		{version: 10, obj: &MetadataRequest{
			AllowAutoTopicCreation: true,
			Topics: []MetadataRequestMetadataRequestTopic{
				{
					Name:    stringPtr("topic1"),
					TopicId: randomUUID(),
				},
			},
			IncludeClusterAuthorizedOperations: true,
			IncludeTopicAuthorizedOperations:   true,
		}},
		{version: 11, obj: &MetadataRequest{
			AllowAutoTopicCreation: true,
			Topics: []MetadataRequestMetadataRequestTopic{
				{
					Name:    stringPtr("topic1"),
					TopicId: randomUUID(),
				},
			},
			IncludeTopicAuthorizedOperations: true,
		}},
		{version: 12, obj: &MetadataRequest{
			AllowAutoTopicCreation: true,
			Topics: []MetadataRequestMetadataRequestTopic{
				{
					TopicId: randomUUID(),
				},
			},
			IncludeTopicAuthorizedOperations: true,
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestMetadataResponse(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &MetadataResponse{
			Brokers: []MetadataResponseMetadataResponseBroker{
				{
					NodeId: 1,
					Host:   stringPtr("host1"),
					Port:   12345,
				},
				{
					NodeId: 2,
					Host:   stringPtr("host2"),
					Port:   12346,
				},
				{
					NodeId: 3,
					Host:   stringPtr("host3"),
					Port:   12347,
				},
			},
			Topics: []MetadataResponseMetadataResponseTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex: 1,
							LeaderId:       2,
							ReplicaNodes:   []int32{3, 4, 1},
							IsrNodes:       []int32{4, 1},
						},
						{
							PartitionIndex: 3,
							LeaderId:       1,
							ReplicaNodes:   []int32{1, 3, 2},
							IsrNodes:       []int32{2, 3},
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex: 7,
							LeaderId:       0,
							ReplicaNodes:   []int32{4, 2, 1},
							IsrNodes:       []int32{2, 1},
						},
						{
							PartitionIndex: 9,
							LeaderId:       3,
							ReplicaNodes:   []int32{3, 2, 1},
							IsrNodes:       []int32{2, 3},
						},
					},
				},
			},
		}},
		{version: 0, obj: &MetadataResponse{
			Brokers: []MetadataResponseMetadataResponseBroker{
				{
					NodeId: 1,
					Host:   stringPtr("host1"),
					Port:   12345,
				},
				{
					NodeId: 2,
					Host:   stringPtr("host2"),
					Port:   12346,
				},
				{
					NodeId: 3,
					Host:   stringPtr("host3"),
					Port:   12347,
				},
			},
			Topics: []MetadataResponseMetadataResponseTopic{
				{
					ErrorCode:  ErrorCodeUnknownTopicOrPartition,
					Name:       stringPtr("topic1"),
					Partitions: []MetadataResponseMetadataResponsePartition{},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex: 7,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
							ReplicaNodes:   []int32{},
							IsrNodes:       []int32{},
						},
						{
							PartitionIndex: 9,
							LeaderId:       3,
							ReplicaNodes:   []int32{3, 2, 1},
							IsrNodes:       []int32{2, 3},
						},
					},
				},
			},
		}},
		{version: 1, obj: &MetadataResponse{
			Brokers: []MetadataResponseMetadataResponseBroker{
				{
					NodeId: 1,
					Host:   stringPtr("host1"),
					Port:   12345,
					Rack:   stringPtr("rack1"),
				},
				{
					NodeId: 2,
					Host:   stringPtr("host2"),
					Port:   12346,
					Rack:   stringPtr("rack2"),
				},
				{
					NodeId: 3,
					Host:   stringPtr("host3"),
					Port:   12347,
					Rack:   stringPtr("rack3"),
				},
			},
			ControllerId: 23,
			Topics: []MetadataResponseMetadataResponseTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex: 1,
							LeaderId:       2,
							ReplicaNodes:   []int32{3, 4, 1},
							IsrNodes:       []int32{4, 1},
						},
						{
							PartitionIndex: 3,
							LeaderId:       1,
							ReplicaNodes:   []int32{1, 3, 2},
							IsrNodes:       []int32{2, 3},
						},
					},
					IsInternal: true,
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex: 7,
							LeaderId:       0,
							ReplicaNodes:   []int32{4, 2, 1},
							IsrNodes:       []int32{2, 1},
						},
						{
							PartitionIndex: 9,
							LeaderId:       3,
							ReplicaNodes:   []int32{3, 2, 1},
							IsrNodes:       []int32{2, 3},
						},
					},
				},
			},
		}},
		{version: 2, obj: &MetadataResponse{
			Brokers: []MetadataResponseMetadataResponseBroker{
				{
					NodeId: 1,
					Host:   stringPtr("host1"),
					Port:   12345,
					Rack:   stringPtr("rack1"),
				},
				{
					NodeId: 2,
					Host:   stringPtr("host2"),
					Port:   12346,
					Rack:   stringPtr("rack2"),
				},
				{
					NodeId: 3,
					Host:   stringPtr("host3"),
					Port:   12347,
					Rack:   stringPtr("rack3"),
				},
			},
			ControllerId: 23,
			ClusterId:    stringPtr("cluster1"),
			Topics: []MetadataResponseMetadataResponseTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex: 1,
							LeaderId:       2,
							ReplicaNodes:   []int32{3, 4, 1},
							IsrNodes:       []int32{4, 1},
						},
						{
							PartitionIndex: 3,
							LeaderId:       1,
							ReplicaNodes:   []int32{1, 3, 2},
							IsrNodes:       []int32{2, 3},
						},
					},
					IsInternal: true,
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex: 7,
							LeaderId:       0,
							ReplicaNodes:   []int32{4, 2, 1},
							IsrNodes:       []int32{2, 1},
						},
						{
							PartitionIndex: 9,
							LeaderId:       3,
							ReplicaNodes:   []int32{3, 2, 1},
							IsrNodes:       []int32{2, 3},
						},
					},
				},
			},
		}},
		{version: 3, obj: &MetadataResponse{
			ThrottleTimeMs: 333,
			Brokers: []MetadataResponseMetadataResponseBroker{
				{
					NodeId: 1,
					Host:   stringPtr("host1"),
					Port:   12345,
					Rack:   stringPtr("rack1"),
				},
				{
					NodeId: 2,
					Host:   stringPtr("host2"),
					Port:   12346,
					Rack:   stringPtr("rack2"),
				},
				{
					NodeId: 3,
					Host:   stringPtr("host3"),
					Port:   12347,
					Rack:   stringPtr("rack3"),
				},
			},
			ControllerId: 23,
			ClusterId:    stringPtr("cluster1"),
			Topics: []MetadataResponseMetadataResponseTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex: 1,
							LeaderId:       2,
							ReplicaNodes:   []int32{3, 4, 1},
							IsrNodes:       []int32{4, 1},
						},
						{
							PartitionIndex: 3,
							LeaderId:       1,
							ReplicaNodes:   []int32{1, 3, 2},
							IsrNodes:       []int32{2, 3},
						},
					},
					IsInternal: true,
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex: 7,
							LeaderId:       0,
							ReplicaNodes:   []int32{4, 2, 1},
							IsrNodes:       []int32{2, 1},
						},
						{
							PartitionIndex: 9,
							LeaderId:       3,
							ReplicaNodes:   []int32{3, 2, 1},
							IsrNodes:       []int32{2, 3},
						},
					},
				},
			},
		}},
		{version: 5, obj: &MetadataResponse{
			ThrottleTimeMs: 333,
			Brokers: []MetadataResponseMetadataResponseBroker{
				{
					NodeId: 1,
					Host:   stringPtr("host1"),
					Port:   12345,
					Rack:   stringPtr("rack1"),
				},
				{
					NodeId: 2,
					Host:   stringPtr("host2"),
					Port:   12346,
					Rack:   stringPtr("rack2"),
				},
				{
					NodeId: 3,
					Host:   stringPtr("host3"),
					Port:   12347,
					Rack:   stringPtr("rack3"),
				},
			},
			ControllerId: 23,
			ClusterId:    stringPtr("cluster1"),
			Topics: []MetadataResponseMetadataResponseTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex:  1,
							LeaderId:        2,
							ReplicaNodes:    []int32{3, 4, 1},
							IsrNodes:        []int32{4, 1},
							OfflineReplicas: []int32{1},
						},
						{
							PartitionIndex:  3,
							LeaderId:        1,
							ReplicaNodes:    []int32{1, 3, 2},
							IsrNodes:        []int32{2, 3},
							OfflineReplicas: []int32{0, 2},
						},
					},
					IsInternal: true,
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex:  7,
							LeaderId:        0,
							ReplicaNodes:    []int32{4, 2, 1},
							IsrNodes:        []int32{2, 1},
							OfflineReplicas: []int32{1, 2},
						},
						{
							PartitionIndex:  9,
							LeaderId:        3,
							ReplicaNodes:    []int32{3, 2, 1},
							IsrNodes:        []int32{2, 3},
							OfflineReplicas: []int32{1, 2, 3},
						},
					},
				},
			},
		}},
		{version: 7, obj: &MetadataResponse{
			ThrottleTimeMs: 333,
			Brokers: []MetadataResponseMetadataResponseBroker{
				{
					NodeId: 1,
					Host:   stringPtr("host1"),
					Port:   12345,
					Rack:   stringPtr("rack1"),
				},
				{
					NodeId: 2,
					Host:   stringPtr("host2"),
					Port:   12346,
					Rack:   stringPtr("rack2"),
				},
				{
					NodeId: 3,
					Host:   stringPtr("host3"),
					Port:   12347,
					Rack:   stringPtr("rack3"),
				},
			},
			ControllerId: 23,
			ClusterId:    stringPtr("cluster1"),
			Topics: []MetadataResponseMetadataResponseTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex:  1,
							LeaderId:        2,
							ReplicaNodes:    []int32{3, 4, 1},
							IsrNodes:        []int32{4, 1},
							OfflineReplicas: []int32{1},
							LeaderEpoch:     12,
						},
						{
							PartitionIndex:  3,
							LeaderId:        1,
							ReplicaNodes:    []int32{1, 3, 2},
							IsrNodes:        []int32{2, 3},
							OfflineReplicas: []int32{0, 2},
							LeaderEpoch:     1,
						},
					},
					IsInternal: true,
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex:  7,
							LeaderId:        0,
							ReplicaNodes:    []int32{4, 2, 1},
							IsrNodes:        []int32{2, 1},
							OfflineReplicas: []int32{1, 2},
							LeaderEpoch:     65,
						},
						{
							PartitionIndex:  9,
							LeaderId:        3,
							ReplicaNodes:    []int32{3, 2, 1},
							IsrNodes:        []int32{2, 3},
							OfflineReplicas: []int32{1, 2, 3},
							LeaderEpoch:     32,
						},
					},
				},
			},
		}},
		{version: 9, obj: &MetadataResponse{
			ThrottleTimeMs: 333,
			Brokers: []MetadataResponseMetadataResponseBroker{
				{
					NodeId: 1,
					Host:   stringPtr("host1"),
					Port:   12345,
					Rack:   stringPtr("rack1"),
				},
				{
					NodeId: 2,
					Host:   stringPtr("host2"),
					Port:   12346,
					Rack:   stringPtr("rack2"),
				},
				{
					NodeId: 3,
					Host:   stringPtr("host3"),
					Port:   12347,
					Rack:   stringPtr("rack3"),
				},
			},
			ControllerId: 23,
			ClusterId:    stringPtr("cluster1"),
			Topics: []MetadataResponseMetadataResponseTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex:  1,
							LeaderId:        2,
							ReplicaNodes:    []int32{3, 4, 1},
							IsrNodes:        []int32{4, 1},
							OfflineReplicas: []int32{1},
							LeaderEpoch:     12,
						},
						{
							PartitionIndex:  3,
							LeaderId:        1,
							ReplicaNodes:    []int32{1, 3, 2},
							IsrNodes:        []int32{2, 3},
							OfflineReplicas: []int32{0, 2},
							LeaderEpoch:     1,
						},
					},
					IsInternal: true,
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex:  7,
							LeaderId:        0,
							ReplicaNodes:    []int32{4, 2, 1},
							IsrNodes:        []int32{2, 1},
							OfflineReplicas: []int32{1, 2},
							LeaderEpoch:     65,
						},
						{
							PartitionIndex:  9,
							LeaderId:        3,
							ReplicaNodes:    []int32{3, 2, 1},
							IsrNodes:        []int32{2, 3},
							OfflineReplicas: []int32{1, 2, 3},
							LeaderEpoch:     32,
						},
					},
				},
			},
		}},
		{version: 10, obj: &MetadataResponse{
			ThrottleTimeMs: 333,
			Brokers: []MetadataResponseMetadataResponseBroker{
				{
					NodeId: 1,
					Host:   stringPtr("host1"),
					Port:   12345,
					Rack:   stringPtr("rack1"),
				},
				{
					NodeId: 2,
					Host:   stringPtr("host2"),
					Port:   12346,
					Rack:   stringPtr("rack2"),
				},
				{
					NodeId: 3,
					Host:   stringPtr("host3"),
					Port:   12347,
					Rack:   stringPtr("rack3"),
				},
			},
			ControllerId: 23,
			ClusterId:    stringPtr("cluster1"),
			Topics: []MetadataResponseMetadataResponseTopic{
				{
					Name:    stringPtr("topic1"),
					TopicId: randomUUID(),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex:  1,
							LeaderId:        2,
							ReplicaNodes:    []int32{3, 4, 1},
							IsrNodes:        []int32{4, 1},
							OfflineReplicas: []int32{1},
							LeaderEpoch:     12,
						},
						{
							PartitionIndex:  3,
							LeaderId:        1,
							ReplicaNodes:    []int32{1, 3, 2},
							IsrNodes:        []int32{2, 3},
							OfflineReplicas: []int32{0, 2},
							LeaderEpoch:     1,
						},
					},
					IsInternal: true,
				},
				{
					Name:    stringPtr("topic2"),
					TopicId: randomUUID(),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex:  7,
							LeaderId:        0,
							ReplicaNodes:    []int32{4, 2, 1},
							IsrNodes:        []int32{2, 1},
							OfflineReplicas: []int32{1, 2},
							LeaderEpoch:     65,
						},
						{
							PartitionIndex:  9,
							LeaderId:        3,
							ReplicaNodes:    []int32{3, 2, 1},
							IsrNodes:        []int32{2, 3},
							OfflineReplicas: []int32{1, 2, 3},
							LeaderEpoch:     32,
						},
					},
				},
			},
		}},
		{version: 12, obj: &MetadataResponse{
			ThrottleTimeMs: 333,
			Brokers: []MetadataResponseMetadataResponseBroker{
				{
					NodeId: 1,
					Host:   stringPtr("host1"),
					Port:   12345,
					Rack:   stringPtr("rack1"),
				},
				{
					NodeId: 2,
					Host:   stringPtr("host2"),
					Port:   12346,
					Rack:   stringPtr("rack2"),
				},
				{
					NodeId: 3,
					Host:   stringPtr("host3"),
					Port:   12347,
					Rack:   stringPtr("rack3"),
				},
			},
			ControllerId: 23,
			ClusterId:    stringPtr("cluster1"),
			Topics: []MetadataResponseMetadataResponseTopic{
				{
					TopicId: randomUUID(),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex:  1,
							LeaderId:        2,
							ReplicaNodes:    []int32{3, 4, 1},
							IsrNodes:        []int32{4, 1},
							OfflineReplicas: []int32{1},
							LeaderEpoch:     12,
						},
						{
							PartitionIndex:  3,
							LeaderId:        1,
							ReplicaNodes:    []int32{1, 3, 2},
							IsrNodes:        []int32{2, 3},
							OfflineReplicas: []int32{0, 2},
							LeaderEpoch:     1,
						},
					},
					IsInternal: true,
				},
				{
					TopicId: randomUUID(),
					Partitions: []MetadataResponseMetadataResponsePartition{
						{
							PartitionIndex:  7,
							LeaderId:        0,
							ReplicaNodes:    []int32{4, 2, 1},
							IsrNodes:        []int32{2, 1},
							OfflineReplicas: []int32{1, 2},
							LeaderEpoch:     65,
						},
						{
							PartitionIndex:  9,
							LeaderId:        3,
							ReplicaNodes:    []int32{3, 2, 1},
							IsrNodes:        []int32{2, 3},
							OfflineReplicas: []int32{1, 2, 3},
							LeaderEpoch:     32,
						},
					},
				},
			},
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestOffsetCommitRequest(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &OffsetCommitRequest{
			GroupId: stringPtr("group1"),
			Topics: []OffsetCommitRequestOffsetCommitRequestTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:    1,
							CommittedOffset:   12345676,
							CommittedMetadata: stringPtr("foobar"),
						},
						{
							PartitionIndex:  3,
							CommittedOffset: 2323432,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:    7,
							CommittedOffset:   234234,
							CommittedMetadata: stringPtr("dassadas"),
						},
						{
							PartitionIndex:  9,
							CommittedOffset: 234234,
						},
					},
				},
			},
		}},
		{version: 1, obj: &OffsetCommitRequest{
			GroupId:                   stringPtr("group1"),
			GenerationIdOrMemberEpoch: 1234,
			MemberId:                  stringPtr("member1"),
			Topics: []OffsetCommitRequestOffsetCommitRequestTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:    1,
							CommittedOffset:   12345676,
							CommittedMetadata: stringPtr("foobar"),
						},
						{
							PartitionIndex:  3,
							CommittedOffset: 2323432,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:    7,
							CommittedOffset:   234234,
							CommittedMetadata: stringPtr("dassadas"),
						},
						{
							PartitionIndex:  9,
							CommittedOffset: 234234,
						},
					},
				},
			},
		}},
		{version: 2, obj: &OffsetCommitRequest{
			GroupId:                   stringPtr("group1"),
			GenerationIdOrMemberEpoch: 1234,
			MemberId:                  stringPtr("member1"),
			RetentionTimeMs:           12323,
			Topics: []OffsetCommitRequestOffsetCommitRequestTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:    1,
							CommittedOffset:   12345676,
							CommittedMetadata: stringPtr("foobar"),
						},
						{
							PartitionIndex:  3,
							CommittedOffset: 2323432,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:    7,
							CommittedOffset:   234234,
							CommittedMetadata: stringPtr("dassadas"),
						},
						{
							PartitionIndex:  9,
							CommittedOffset: 234234,
						},
					},
				},
			},
		}},
		{version: 5, obj: &OffsetCommitRequest{
			GroupId:                   stringPtr("group1"),
			GenerationIdOrMemberEpoch: 1234,
			MemberId:                  stringPtr("member1"),
			Topics: []OffsetCommitRequestOffsetCommitRequestTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:    1,
							CommittedOffset:   12345676,
							CommittedMetadata: stringPtr("foobar"),
						},
						{
							PartitionIndex:  3,
							CommittedOffset: 2323432,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:    7,
							CommittedOffset:   234234,
							CommittedMetadata: stringPtr("dassadas"),
						},
						{
							PartitionIndex:  9,
							CommittedOffset: 234234,
						},
					},
				},
			},
		}},
		{version: 6, obj: &OffsetCommitRequest{
			GroupId:                   stringPtr("group1"),
			GenerationIdOrMemberEpoch: 1234,
			MemberId:                  stringPtr("member1"),
			Topics: []OffsetCommitRequestOffsetCommitRequestTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:       1,
							CommittedOffset:      12345676,
							CommittedMetadata:    stringPtr("foobar"),
							CommittedLeaderEpoch: 12,
						},
						{
							PartitionIndex:       3,
							CommittedOffset:      2323432,
							CommittedLeaderEpoch: 2,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:       7,
							CommittedOffset:      234234,
							CommittedMetadata:    stringPtr("dassadas"),
							CommittedLeaderEpoch: 43,
						},
						{
							PartitionIndex:       9,
							CommittedOffset:      234234,
							CommittedLeaderEpoch: 3,
						},
					},
				},
			},
		}},
		{version: 7, obj: &OffsetCommitRequest{
			GroupId:                   stringPtr("group1"),
			GenerationIdOrMemberEpoch: 1234,
			MemberId:                  stringPtr("member1"),
			GroupInstanceId:           stringPtr("groupinstance1"),
			Topics: []OffsetCommitRequestOffsetCommitRequestTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:       1,
							CommittedOffset:      12345676,
							CommittedMetadata:    stringPtr("foobar"),
							CommittedLeaderEpoch: 12,
						},
						{
							PartitionIndex:       3,
							CommittedOffset:      2323432,
							CommittedLeaderEpoch: 2,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:       7,
							CommittedOffset:      234234,
							CommittedMetadata:    stringPtr("dassadas"),
							CommittedLeaderEpoch: 43,
						},
						{
							PartitionIndex:       9,
							CommittedOffset:      234234,
							CommittedLeaderEpoch: 3,
						},
					},
				},
			},
		}},
		{version: 7, obj: &OffsetCommitRequest{
			GroupId:                   stringPtr("group1"),
			GenerationIdOrMemberEpoch: 1234,
			MemberId:                  stringPtr("member1"),
			Topics: []OffsetCommitRequestOffsetCommitRequestTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:       1,
							CommittedOffset:      12345676,
							CommittedMetadata:    stringPtr("foobar"),
							CommittedLeaderEpoch: 12,
						},
						{
							PartitionIndex:       3,
							CommittedOffset:      2323432,
							CommittedLeaderEpoch: 2,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:       7,
							CommittedOffset:      234234,
							CommittedMetadata:    stringPtr("dassadas"),
							CommittedLeaderEpoch: 43,
						},
						{
							PartitionIndex:       9,
							CommittedOffset:      234234,
							CommittedLeaderEpoch: 3,
						},
					},
				},
			},
		}},
		{version: 8, obj: &OffsetCommitRequest{
			GroupId:                   stringPtr("group1"),
			GenerationIdOrMemberEpoch: 1234,
			MemberId:                  stringPtr("member1"),
			GroupInstanceId:           stringPtr("groupinstance1"),
			Topics: []OffsetCommitRequestOffsetCommitRequestTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:       1,
							CommittedOffset:      12345676,
							CommittedMetadata:    stringPtr("foobar"),
							CommittedLeaderEpoch: 12,
						},
						{
							PartitionIndex:       3,
							CommittedOffset:      2323432,
							CommittedLeaderEpoch: 2,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetCommitRequestOffsetCommitRequestPartition{
						{
							PartitionIndex:       7,
							CommittedOffset:      234234,
							CommittedMetadata:    stringPtr("dassadas"),
							CommittedLeaderEpoch: 43,
						},
						{
							PartitionIndex:       9,
							CommittedOffset:      234234,
							CommittedLeaderEpoch: 3,
						},
					},
				},
			},
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestOffsetCommitResponse(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &OffsetCommitResponse{
			Topics: []OffsetCommitResponseOffsetCommitResponseTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetCommitResponseOffsetCommitResponsePartition{
						{
							PartitionIndex: 1,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
						{
							PartitionIndex: 7,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetCommitResponseOffsetCommitResponsePartition{
						{
							PartitionIndex: 9,
						},
						{
							PartitionIndex: 3,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
					},
				},
			},
		}},
		{version: 3, obj: &OffsetCommitResponse{
			ThrottleTimeMs: 333,
			Topics: []OffsetCommitResponseOffsetCommitResponseTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetCommitResponseOffsetCommitResponsePartition{
						{
							PartitionIndex: 1,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
						{
							PartitionIndex: 7,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetCommitResponseOffsetCommitResponsePartition{
						{
							PartitionIndex: 9,
						},
						{
							PartitionIndex: 3,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
					},
				},
			},
		}},
		{version: 8, obj: &OffsetCommitResponse{
			ThrottleTimeMs: 333,
			Topics: []OffsetCommitResponseOffsetCommitResponseTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetCommitResponseOffsetCommitResponsePartition{
						{
							PartitionIndex: 1,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
						{
							PartitionIndex: 7,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetCommitResponseOffsetCommitResponsePartition{
						{
							PartitionIndex: 9,
						},
						{
							PartitionIndex: 3,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
					},
				},
			},
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestOffsetFetchRequest(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &OffsetFetchRequest{
			GroupId: stringPtr("group1"),
			Topics: []OffsetFetchRequestOffsetFetchRequestTopic{
				{
					Name:             stringPtr("topic1"),
					PartitionIndexes: []int32{1, 3, 7},
				},
				{
					Name:             stringPtr("topic2"),
					PartitionIndexes: []int32{2, 6, 10},
				},
			},
		}},
		{version: 2, obj: &OffsetFetchRequest{
			GroupId: stringPtr("group1"),
		}},
		{version: 6, obj: &OffsetFetchRequest{
			GroupId: stringPtr("group1"),
			Topics: []OffsetFetchRequestOffsetFetchRequestTopic{
				{
					Name:             stringPtr("topic1"),
					PartitionIndexes: []int32{1, 3, 7},
				},
				{
					Name:             stringPtr("topic2"),
					PartitionIndexes: []int32{2, 6, 10},
				},
			},
		}},
		{version: 7, obj: &OffsetFetchRequest{
			GroupId: stringPtr("group1"),
			Topics: []OffsetFetchRequestOffsetFetchRequestTopic{
				{
					Name:             stringPtr("topic1"),
					PartitionIndexes: []int32{1, 3, 7},
				},
				{
					Name:             stringPtr("topic2"),
					PartitionIndexes: []int32{2, 6, 10},
				},
			},
			RequireStable: true,
		}},
		{version: 8, obj: &OffsetFetchRequest{
			Groups: []OffsetFetchRequestOffsetFetchRequestGroup{
				{
					GroupId: stringPtr("group1"),
					Topics: []OffsetFetchRequestOffsetFetchRequestTopics{
						{
							Name:             stringPtr("topic1"),
							PartitionIndexes: []int32{1, 3, 7},
						},
						{
							Name:             stringPtr("topic2"),
							PartitionIndexes: []int32{2, 6, 10},
						},
					},
				},
				{
					GroupId: stringPtr("group2"),
					Topics: []OffsetFetchRequestOffsetFetchRequestTopics{
						{
							Name:             stringPtr("topic3"),
							PartitionIndexes: []int32{3, 5, 7},
						},
						{
							Name:             stringPtr("topic4"),
							PartitionIndexes: []int32{6, 6, 11},
						},
					},
				},
			},
			RequireStable: true,
		}},
		{version: 8, obj: &OffsetFetchRequest{
			Groups: []OffsetFetchRequestOffsetFetchRequestGroup{
				{
					GroupId: stringPtr("group1"),
					Topics: []OffsetFetchRequestOffsetFetchRequestTopics{
						{
							Name:             stringPtr("topic1"),
							PartitionIndexes: []int32{1, 3, 7},
						},
						{
							Name:             stringPtr("topic2"),
							PartitionIndexes: []int32{2, 6, 10},
						},
					},
				},
				{
					GroupId: stringPtr("group2"),
					Topics: []OffsetFetchRequestOffsetFetchRequestTopics{
						{
							Name:             stringPtr("topic3"),
							PartitionIndexes: []int32{3, 5, 7},
						},
						{
							Name:             stringPtr("topic4"),
							PartitionIndexes: []int32{6, 6, 11},
						},
					},
				},
			},
			RequireStable: true,
		}},
		{version: 9, obj: &OffsetFetchRequest{
			Groups: []OffsetFetchRequestOffsetFetchRequestGroup{
				{
					GroupId: stringPtr("group1"),
					Topics: []OffsetFetchRequestOffsetFetchRequestTopics{
						{
							Name:             stringPtr("topic1"),
							PartitionIndexes: []int32{1, 3, 7},
						},
						{
							Name:             stringPtr("topic2"),
							PartitionIndexes: []int32{2, 6, 10},
						},
					},
					MemberId:    stringPtr("member1"),
					MemberEpoch: 23,
				},
				{
					GroupId: stringPtr("group2"),
					Topics: []OffsetFetchRequestOffsetFetchRequestTopics{
						{
							Name:             stringPtr("topic3"),
							PartitionIndexes: []int32{3, 5, 7},
						},
						{
							Name:             stringPtr("topic4"),
							PartitionIndexes: []int32{6, 6, 11},
						},
					},
					MemberId:    stringPtr("member2"),
					MemberEpoch: 45,
				},
			},
			RequireStable: true,
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestOffsetFetchResponse(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &OffsetFetchResponse{
			Topics: []OffsetFetchResponseOffsetFetchResponseTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetFetchResponseOffsetFetchResponsePartition{
						{
							PartitionIndex:  1,
							CommittedOffset: 12333,
						},
						{
							PartitionIndex:  3,
							CommittedOffset: 345345,
							Metadata:        stringPtr("wfewefwefwe"),
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetFetchResponseOffsetFetchResponsePartition{
						{
							PartitionIndex: 7,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
						{
							PartitionIndex:  9,
							CommittedOffset: 567567,
							Metadata:        stringPtr("thertheh"),
						},
					},
				},
			},
		}},
		{version: 2, obj: &OffsetFetchResponse{
			ErrorCode: ErrorCodeUnknownTopicOrPartition,
			Topics:    []OffsetFetchResponseOffsetFetchResponseTopic{},
		}},
		{version: 3, obj: &OffsetFetchResponse{
			ThrottleTimeMs: 333,
			Topics: []OffsetFetchResponseOffsetFetchResponseTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetFetchResponseOffsetFetchResponsePartition{
						{
							PartitionIndex:  1,
							CommittedOffset: 12333,
						},
						{
							PartitionIndex:  3,
							CommittedOffset: 345345,
							Metadata:        stringPtr("wfewefwefwe"),
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetFetchResponseOffsetFetchResponsePartition{
						{
							PartitionIndex: 7,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
						{
							PartitionIndex:  9,
							CommittedOffset: 567567,
							Metadata:        stringPtr("thertheh"),
						},
					},
				},
			},
		}},
		{version: 5, obj: &OffsetFetchResponse{
			ThrottleTimeMs: 333,
			Topics: []OffsetFetchResponseOffsetFetchResponseTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetFetchResponseOffsetFetchResponsePartition{
						{
							PartitionIndex:       1,
							CommittedOffset:      12333,
							CommittedLeaderEpoch: 12,
						},
						{
							PartitionIndex:       3,
							CommittedOffset:      345345,
							Metadata:             stringPtr("wfewefwefwe"),
							CommittedLeaderEpoch: 54,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetFetchResponseOffsetFetchResponsePartition{
						{
							PartitionIndex: 7,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
						{
							PartitionIndex:       9,
							CommittedOffset:      567567,
							Metadata:             stringPtr("thertheh"),
							CommittedLeaderEpoch: 2,
						},
					},
				},
			},
		}},
		{version: 6, obj: &OffsetFetchResponse{
			ThrottleTimeMs: 333,
			Topics: []OffsetFetchResponseOffsetFetchResponseTopic{
				{
					Name: stringPtr("topic1"),
					Partitions: []OffsetFetchResponseOffsetFetchResponsePartition{
						{
							PartitionIndex:       1,
							CommittedOffset:      12333,
							CommittedLeaderEpoch: 12,
						},
						{
							PartitionIndex:       3,
							CommittedOffset:      345345,
							Metadata:             stringPtr("wfewefwefwe"),
							CommittedLeaderEpoch: 54,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					Partitions: []OffsetFetchResponseOffsetFetchResponsePartition{
						{
							PartitionIndex: 7,
							ErrorCode:      ErrorCodeUnknownTopicOrPartition,
						},
						{
							PartitionIndex:       9,
							CommittedOffset:      567567,
							Metadata:             stringPtr("thertheh"),
							CommittedLeaderEpoch: 2,
						},
					},
				},
			},
		}},
		{version: 8, obj: &OffsetFetchResponse{
			ThrottleTimeMs: 333,
			Groups: []OffsetFetchResponseOffsetFetchResponseGroup{
				{
					GroupId: stringPtr("group1"),
					Topics: []OffsetFetchResponseOffsetFetchResponseTopics{
						{
							Name: stringPtr("topic1"),
							Partitions: []OffsetFetchResponseOffsetFetchResponsePartitions{
								{
									PartitionIndex:       1,
									CommittedOffset:      12333,
									CommittedLeaderEpoch: 12,
								},
								{
									PartitionIndex:       3,
									CommittedOffset:      345345,
									Metadata:             stringPtr("wfewefwefwe"),
									CommittedLeaderEpoch: 54,
								},
							},
						},
						{
							Name: stringPtr("topic2"),
							Partitions: []OffsetFetchResponseOffsetFetchResponsePartitions{
								{
									PartitionIndex: 7,
									ErrorCode:      ErrorCodeUnknownTopicOrPartition,
								},
								{
									PartitionIndex:       9,
									CommittedOffset:      567567,
									Metadata:             stringPtr("thertheh"),
									CommittedLeaderEpoch: 2,
								},
							},
						},
					},
				},
				{
					GroupId: stringPtr("group2"),
					Topics: []OffsetFetchResponseOffsetFetchResponseTopics{
						{
							Name: stringPtr("topic3"),
							Partitions: []OffsetFetchResponseOffsetFetchResponsePartitions{
								{
									PartitionIndex:       7,
									CommittedOffset:      324234,
									CommittedLeaderEpoch: 12,
								},
								{
									PartitionIndex:       8,
									CommittedOffset:      234234,
									Metadata:             stringPtr("wefewfewf"),
									CommittedLeaderEpoch: 32,
								},
							},
						},
					},
				},
			},
		}},
		{version: 8, obj: &OffsetFetchResponse{
			ThrottleTimeMs: 333,
			Groups: []OffsetFetchResponseOffsetFetchResponseGroup{
				{
					GroupId:   stringPtr("group1"),
					ErrorCode: ErrorCodeUnknownServerError,
					Topics:    []OffsetFetchResponseOffsetFetchResponseTopics{},
				},
			},
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestProduceRequest(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &ProduceRequest{
			Acks:      -1,
			TimeoutMs: 777,
			TopicData: []ProduceRequestTopicProduceData{
				{
					Name: stringPtr("topic1"),
					PartitionData: []ProduceRequestPartitionProduceData{
						{
							Index:   1,
							Records: randomProduceRecords(100),
						},
						{
							Index:   3,
							Records: randomProduceRecords(100),
						},
						{
							Index:   7,
							Records: randomProduceRecords(100),
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					PartitionData: []ProduceRequestPartitionProduceData{
						{
							Index:   2,
							Records: randomProduceRecords(100),
						},
						{
							Index:   5,
							Records: randomProduceRecords(100),
						},
						{
							Index:   9,
							Records: randomProduceRecords(100),
						},
					},
				},
			},
		}},
		{version: 3, obj: &ProduceRequest{
			TransactionalId: stringPtr("transaction1"),
			Acks:            -1,
			TimeoutMs:       777,
			TopicData: []ProduceRequestTopicProduceData{
				{
					Name: stringPtr("topic1"),
					PartitionData: []ProduceRequestPartitionProduceData{
						{
							Index:   1,
							Records: randomProduceRecords(100),
						},
						{
							Index:   3,
							Records: randomProduceRecords(100),
						},
						{
							Index:   7,
							Records: randomProduceRecords(100),
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					PartitionData: []ProduceRequestPartitionProduceData{
						{
							Index:   2,
							Records: randomProduceRecords(100),
						},
						{
							Index:   5,
							Records: randomProduceRecords(100),
						},
						{
							Index:   9,
							Records: randomProduceRecords(100),
						},
					},
				},
			},
		}},
		{version: 9, obj: &ProduceRequest{
			TransactionalId: stringPtr("transaction1"),
			Acks:            -1,
			TimeoutMs:       777,
			TopicData: []ProduceRequestTopicProduceData{
				{
					Name: stringPtr("topic1"),
					PartitionData: []ProduceRequestPartitionProduceData{
						{
							Index:   1,
							Records: randomProduceRecords(100),
						},
						{
							Index:   3,
							Records: randomProduceRecords(100),
						},
						{
							Index:   7,
							Records: randomProduceRecords(100),
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					PartitionData: []ProduceRequestPartitionProduceData{
						{
							Index:   2,
							Records: randomProduceRecords(100),
						},
						{
							Index:   5,
							Records: randomProduceRecords(100),
						},
						{
							Index:   9,
							Records: randomProduceRecords(100),
						},
					},
				},
			},
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestProduceResponse(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &ProduceResponse{
			Responses: []ProduceResponseTopicProduceResponse{
				{
					Name: stringPtr("topic1"),
					PartitionResponses: []ProduceResponsePartitionProduceResponse{
						{
							Index:      1,
							BaseOffset: 1245142,
						},
						{
							Index:      3,
							BaseOffset: 23234,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					PartitionResponses: []ProduceResponsePartitionProduceResponse{
						{
							Index:      2,
							BaseOffset: 1245142,
						},
						{
							Index:      4,
							BaseOffset: 34545,
						},
					},
				},
			},
		}},
		{version: 1, obj: &ProduceResponse{
			ThrottleTimeMs: 444,
			Responses: []ProduceResponseTopicProduceResponse{
				{
					Name: stringPtr("topic1"),
					PartitionResponses: []ProduceResponsePartitionProduceResponse{
						{
							Index:      1,
							BaseOffset: 1245142,
						},
						{
							Index:      3,
							BaseOffset: 23234,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					PartitionResponses: []ProduceResponsePartitionProduceResponse{
						{
							Index:      2,
							BaseOffset: 1245142,
						},
						{
							Index:      4,
							BaseOffset: 34545,
						},
					},
				},
			},
		}},
		{version: 5, obj: &ProduceResponse{
			ThrottleTimeMs: 444,
			Responses: []ProduceResponseTopicProduceResponse{
				{
					Name: stringPtr("topic1"),
					PartitionResponses: []ProduceResponsePartitionProduceResponse{
						{
							Index:          1,
							BaseOffset:     1245142,
							LogStartOffset: 123123,
						},
						{
							Index:          3,
							BaseOffset:     23234,
							LogStartOffset: 2131,
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					PartitionResponses: []ProduceResponsePartitionProduceResponse{
						{
							Index:          2,
							BaseOffset:     1245142,
							LogStartOffset: 32323,
						},
						{
							Index:          4,
							BaseOffset:     34545,
							LogStartOffset: 12323,
						},
					},
				},
			},
		}},
		{version: 8, obj: &ProduceResponse{
			ThrottleTimeMs: 444,
			Responses: []ProduceResponseTopicProduceResponse{
				{
					Name: stringPtr("topic1"),
					PartitionResponses: []ProduceResponsePartitionProduceResponse{
						{
							Index:          1,
							BaseOffset:     1245142,
							LogStartOffset: 123123,
							ErrorCode:      ErrorCodeUnknownServerError,
							RecordErrors: []ProduceResponseBatchIndexAndErrorMessage{
								{
									BatchIndex:             213123,
									BatchIndexErrorMessage: stringPtr("foo"),
								},
							},
						},
						{
							Index:          3,
							BaseOffset:     23234,
							LogStartOffset: 2131,
							RecordErrors: []ProduceResponseBatchIndexAndErrorMessage{
								{
									BatchIndex: 234234,
								},
							},
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					PartitionResponses: []ProduceResponsePartitionProduceResponse{
						{

							Index:          2,
							BaseOffset:     1245142,
							LogStartOffset: 32323,
							RecordErrors:   []ProduceResponseBatchIndexAndErrorMessage{},
						},
						{
							Index:          4,
							BaseOffset:     34545,
							LogStartOffset: 12323,
							ErrorCode:      ErrorCodeUnknownServerError,
							RecordErrors: []ProduceResponseBatchIndexAndErrorMessage{
								{
									BatchIndex:             2355,
									BatchIndexErrorMessage: stringPtr("bar"),
								},
							},
						},
					},
				},
			},
		}},
		{version: 9, obj: &ProduceResponse{
			ThrottleTimeMs: 444,
			Responses: []ProduceResponseTopicProduceResponse{
				{
					Name: stringPtr("topic1"),
					PartitionResponses: []ProduceResponsePartitionProduceResponse{
						{
							Index:          1,
							BaseOffset:     1245142,
							LogStartOffset: 123123,
							ErrorCode:      ErrorCodeUnknownServerError,
							RecordErrors: []ProduceResponseBatchIndexAndErrorMessage{
								{
									BatchIndex:             213123,
									BatchIndexErrorMessage: stringPtr("foo"),
								},
							},
						},
						{
							Index:          3,
							BaseOffset:     23234,
							LogStartOffset: 2131,
							RecordErrors: []ProduceResponseBatchIndexAndErrorMessage{
								{
									BatchIndex: 234234,
								},
							},
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					PartitionResponses: []ProduceResponsePartitionProduceResponse{
						{

							Index:          2,
							BaseOffset:     1245142,
							LogStartOffset: 32323,
							RecordErrors:   []ProduceResponseBatchIndexAndErrorMessage{},
						},
						{
							Index:          4,
							BaseOffset:     34545,
							LogStartOffset: 12323,
							ErrorCode:      ErrorCodeUnknownServerError,
							RecordErrors: []ProduceResponseBatchIndexAndErrorMessage{
								{
									BatchIndex:             2355,
									BatchIndexErrorMessage: stringPtr("bar"),
								},
							},
						},
					},
				},
			},
		}},
		{version: 10, obj: &ProduceResponse{
			ThrottleTimeMs: 444,
			Responses: []ProduceResponseTopicProduceResponse{
				{
					Name: stringPtr("topic1"),
					PartitionResponses: []ProduceResponsePartitionProduceResponse{
						{
							Index:          1,
							BaseOffset:     1245142,
							LogStartOffset: 123123,
							ErrorCode:      ErrorCodeUnknownServerError,
							RecordErrors: []ProduceResponseBatchIndexAndErrorMessage{
								{
									BatchIndex:             213123,
									BatchIndexErrorMessage: stringPtr("foo"),
								},
							},
							CurrentLeader: ProduceResponseLeaderIdAndEpoch{
								LeaderId:    2,
								LeaderEpoch: 23,
							},
						},
						{
							Index:          3,
							BaseOffset:     23234,
							LogStartOffset: 2131,
							RecordErrors: []ProduceResponseBatchIndexAndErrorMessage{
								{
									BatchIndex: 234234,
								},
							},
							CurrentLeader: ProduceResponseLeaderIdAndEpoch{
								LeaderId:    1,
								LeaderEpoch: 33,
							},
						},
					},
				},
				{
					Name: stringPtr("topic2"),
					PartitionResponses: []ProduceResponsePartitionProduceResponse{
						{

							Index:          2,
							BaseOffset:     1245142,
							LogStartOffset: 32323,
							RecordErrors:   []ProduceResponseBatchIndexAndErrorMessage{},
							CurrentLeader: ProduceResponseLeaderIdAndEpoch{
								LeaderId:    1,
								LeaderEpoch: 2,
							},
						},
						{
							Index:          4,
							BaseOffset:     34545,
							LogStartOffset: 12323,
							ErrorCode:      ErrorCodeUnknownServerError,
							RecordErrors: []ProduceResponseBatchIndexAndErrorMessage{
								{
									BatchIndex:             2355,
									BatchIndexErrorMessage: stringPtr("bar"),
								},
							},
							CurrentLeader: ProduceResponseLeaderIdAndEpoch{
								LeaderId:    3,
								LeaderEpoch: 13,
							},
						},
					},
				},
			},
			NodeEndpoints: []ProduceResponseNodeEndpoint{
				{
					NodeId: 1,
					Host:   stringPtr("host1"),
					Port:   12345,
					Rack:   stringPtr("rack1"),
				},
				{
					NodeId: 0,
					Host:   stringPtr("host2"),
					Port:   12346,
				},
				{
					NodeId: 2,
					Host:   stringPtr("host2"),
					Port:   12347,
					Rack:   stringPtr("rack3"),
				},
			},
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestSyncGroupRequest(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &SyncGroupRequest{
			GroupId:      stringPtr("group1"),
			GenerationId: 23,
			MemberId:     stringPtr("member1"),
			Assignments: []SyncGroupRequestSyncGroupRequestAssignment{
				{
					MemberId:   stringPtr("member2"),
					Assignment: randomBytes(100),
				},
				{
					MemberId:   stringPtr("member3"),
					Assignment: randomBytes(100),
				},
				{
					MemberId:   stringPtr("member3"),
					Assignment: randomBytes(100),
				},
			},
		}},
		{version: 3, obj: &SyncGroupRequest{
			GroupId:         stringPtr("group1"),
			GroupInstanceId: stringPtr("instance1"),
			GenerationId:    23,
			MemberId:        stringPtr("member1"),
			Assignments: []SyncGroupRequestSyncGroupRequestAssignment{
				{
					MemberId:   stringPtr("member2"),
					Assignment: randomBytes(100),
				},
				{
					MemberId:   stringPtr("member3"),
					Assignment: randomBytes(100),
				},
				{
					MemberId:   stringPtr("member3"),
					Assignment: randomBytes(100),
				},
			},
		}},
		{version: 3, obj: &SyncGroupRequest{
			GroupId:      stringPtr("group1"),
			GenerationId: 23,
			MemberId:     stringPtr("member1"),
			Assignments: []SyncGroupRequestSyncGroupRequestAssignment{
				{
					MemberId:   stringPtr("member2"),
					Assignment: randomBytes(100),
				},
				{
					MemberId:   stringPtr("member3"),
					Assignment: randomBytes(100),
				},
				{
					MemberId:   stringPtr("member3"),
					Assignment: randomBytes(100),
				},
			},
		}},
		{version: 4, obj: &SyncGroupRequest{
			GroupId:         stringPtr("group1"),
			GroupInstanceId: stringPtr("instance1"),
			GenerationId:    23,
			MemberId:        stringPtr("member1"),
			Assignments: []SyncGroupRequestSyncGroupRequestAssignment{
				{
					MemberId:   stringPtr("member2"),
					Assignment: randomBytes(100),
				},
				{
					MemberId:   stringPtr("member3"),
					Assignment: randomBytes(100),
				},
				{
					MemberId:   stringPtr("member3"),
					Assignment: randomBytes(100),
				},
			},
		}},
		{version: 5, obj: &SyncGroupRequest{
			GroupId:         stringPtr("group1"),
			GroupInstanceId: stringPtr("instance1"),
			GenerationId:    23,
			MemberId:        stringPtr("member1"),
			ProtocolType:    stringPtr("protocoltype1"),
			ProtocolName:    stringPtr("protocolname1"),
			Assignments: []SyncGroupRequestSyncGroupRequestAssignment{
				{
					MemberId:   stringPtr("member2"),
					Assignment: randomBytes(100),
				},
				{
					MemberId:   stringPtr("member3"),
					Assignment: randomBytes(100),
				},
				{
					MemberId:   stringPtr("member3"),
					Assignment: randomBytes(100),
				},
			},
		}},
		{version: 5, obj: &SyncGroupRequest{
			GroupId:         stringPtr("group1"),
			GroupInstanceId: stringPtr("instance1"),
			GenerationId:    23,
			MemberId:        stringPtr("member1"),
			Assignments: []SyncGroupRequestSyncGroupRequestAssignment{
				{
					MemberId:   stringPtr("member2"),
					Assignment: randomBytes(100),
				},
				{
					MemberId:   stringPtr("member3"),
					Assignment: randomBytes(100),
				},
				{
					MemberId:   stringPtr("member3"),
					Assignment: randomBytes(100),
				},
			},
		}},
	}
	testReadWriteCases(t, testCases)
}

func TestSyncGroupResponse(t *testing.T) {
	testCases := []readWriteCase{
		{version: 0, obj: &SyncGroupResponse{
			ErrorCode:  ErrorCodeUnknownServerError,
			Assignment: []byte{},
		}},
		{version: 0, obj: &SyncGroupResponse{
			Assignment: randomBytes(100),
		}},
		{version: 1, obj: &SyncGroupResponse{
			ThrottleTimeMs: 222,
			Assignment:     randomBytes(100),
		}},
		{version: 4, obj: &SyncGroupResponse{
			ThrottleTimeMs: 222,
			Assignment:     randomBytes(100),
		}},
		{version: 5, obj: &SyncGroupResponse{
			ThrottleTimeMs: 222,
			Assignment:     randomBytes(100),
			ProtocolType:   stringPtr("protocoltype1"),
			ProtocolName:   stringPtr("protocolname1"),
		}},
		{version: 5, obj: &SyncGroupResponse{
			ThrottleTimeMs: 222,
			Assignment:     randomBytes(100),
		}},
	}
	testReadWriteCases(t, testCases)
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

func randomProduceRecords(n int) [][]byte {
	return [][]byte{randomBytes(n)}
}

func randomFetchRecords(n int) [][]byte {
	// For fetch we can combine from multiple []byte
	return [][]byte{randomBytes(n), randomBytes(n), randomBytes(n)}
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}

func testReadWriteCasesWithPrecompareFunc(t *testing.T, testCases []readWriteCase, preCompareFunc func(tc *readWriteCase)) {
	for _, tc := range testCases {
		t.Run(reflect.TypeOf(tc).String(), func(t *testing.T) {
			testReadWriteStructWithPrecompareFunc(t, tc, preCompareFunc)
		})
	}
}

func testReadWriteCases(t *testing.T, testCases []readWriteCase) {
	testReadWriteCasesWithPrecompareFunc(t, testCases, nil)
}

func stringPtr(s string) *string {
	return &s
}

type serializable interface {
	Read(version int16, buff []byte) (int, error)
	Write(version int16, buff []byte, tagSizes []int) []byte
	CalcSize(version int16, tagSizes []int) (int, []int)
}

type readWriteCase struct {
	version int16
	obj     serializable
}

func testReadWriteStructWithPrecompareFunc(t *testing.T, tc readWriteCase, preCompareFunc func(tc *readWriteCase)) {
	initialData := "sausages"
	buff := []byte(initialData) // buff already contains some data

	calcedSize, tagSizes := tc.obj.CalcSize(tc.version, nil)

	sizeStart := len(buff)
	buff = tc.obj.Write(tc.version, buff, tagSizes)
	actualSize := len(buff) - sizeStart

	log.Infof("obj %s version %d calced size %d", t.Name(), tc.version, actualSize)

	// make sure calc size matches actual serialized size
	require.Equal(t, calcedSize, actualSize)

	// create new instance and deserialize
	originalType := reflect.TypeOf(tc.obj).Elem() // Get the underlying type of the original instance
	obj2 := reflect.New(originalType).Interface().(serializable)

	off, err := obj2.Read(tc.version, buff[len(initialData):])
	require.NoError(t, err)
	require.Equal(t, calcedSize, off)

	if preCompareFunc != nil {
		preCompareFunc(&tc)
	}

	require.Equal(t, tc.obj, obj2)
}

func BenchmarkWriteFetchRequest(b *testing.B) {
	fr := &FetchRequest{
		ReplicaState: FetchRequestReplicaState{
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
		Topics: []FetchRequestFetchTopic{
			{
				TopicId: randomUUID(),
				Partitions: []FetchRequestFetchPartition{
					{Partition: 1, FetchOffset: 230000, PartitionMaxBytes: 4453453, LogStartOffset: 120000, CurrentLeaderEpoch: 12, LastFetchedEpoch: 13, ReplicaDirectoryId: randomUUID()},
					{Partition: 5, FetchOffset: 576484, PartitionMaxBytes: 575757, LogStartOffset: 36358888, CurrentLeaderEpoch: 43, LastFetchedEpoch: 67, ReplicaDirectoryId: randomUUID()},
				},
			},
			{
				TopicId: randomUUID(),
				Partitions: []FetchRequestFetchPartition{
					{Partition: 7, FetchOffset: 266000, PartitionMaxBytes: 723663, LogStartOffset: 4363636, CurrentLeaderEpoch: 1, LastFetchedEpoch: 3, ReplicaDirectoryId: randomUUID()},
					{Partition: 9, FetchOffset: 5978484, PartitionMaxBytes: 56595, LogStartOffset: 4743545, CurrentLeaderEpoch: 67, LastFetchedEpoch: 5, ReplicaDirectoryId: randomUUID()},
				},
			},
		},
		ForgottenTopicsData: []FetchRequestForgottenTopic{
			{TopicId: randomUUID(), Partitions: []int32{34, 456, 6, 7}},
			{TopicId: randomUUID(), Partitions: []int32{34, 456, 6, 7}},
		},
		RackId: stringPtr("rack1"),
	}
	b.ResetTimer()
	var buff []byte
	for i := 0; i < b.N; i++ {
		size, tagSizes := fr.CalcSize(17, nil)
		buff = make([]byte, size)
		buff = fr.Write(17, buff, tagSizes)
	}
}
