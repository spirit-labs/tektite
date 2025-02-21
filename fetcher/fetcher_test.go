package fetcher

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/acls"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"math"
	"sort"
	"sync"
	"time"

	"testing"
)

const (
	databucketName       = "test-bucket"
	defaultTopicName     = "default-test-topic"
	defaultTopicID       = 1001
	defaultPartitionID   = 23
	defaultNumPartitions = 100
	defaultMaxBytes      = math.MaxInt32
)

func TestFetcherSinglePartitionNoWaitFetchBeforeFirstOffsetFromZero(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 100, 10000, 10000, 1, 1, topicProvider, controlClient, objStore)
	resp := sendFetchDefault(t, 0, 5*time.Second, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	verifyDefaultResponse(t, resp, batches)
}

func TestFetcherSinglePartitionNoWaitFetchBeforeFirstOffsetFromNonZero(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 100, 10000, 10000, 1, 1, topicProvider, controlClient, objStore)
	resp := sendFetchDefault(t, 50, 5*time.Second, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	verifyDefaultResponse(t, resp, batches)
}

func TestFetcherSinglePartitionNoWaitFetchAtFirstOffset(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 100, 10000, 10000, 1, 1, topicProvider, controlClient, objStore)
	resp := sendFetchDefault(t, 100, 5*time.Second, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	verifyDefaultResponse(t, resp, batches)
}

func TestFetcherSinglePartitionNoWaitMultipleTablesAndBatches(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 100, 10000, 10000, 100, 10, topicProvider, controlClient, objStore)
	resp := sendFetchDefault(t, 0, 5*time.Second, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	verifyDefaultResponse(t, resp, batches)
}

func TestFetcherSinglePartitionNoWaitFetchAfterLastOffset(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	setupDataDefault(t, 100, 10000, 10000, 1, 1, topicProvider, controlClient, objStore)
	resp := sendFetchDefault(t, 10000, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	// Should receive no batches
	verifyDefaultResponse(t, resp, nil)
}

func TestFetcherSinglePartitionNoWaitFetchMuchAfterLastOffset(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	setupDataDefault(t, 100, 10000, 10000, 1, 1, topicProvider, controlClient, objStore)
	resp := sendFetchDefault(t, 20000, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	// Should receive no batches
	verifyDefaultResponse(t, resp, nil)
}

func TestFetcherSinglePartitionNoWaitFetchAfterBeginningOfFirstBatch(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	setupDataDefault(t, 100, 10000, 10000, 1, 1, topicProvider, controlClient, objStore)
	resp := sendFetchDefault(t, 101, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	// If fetchOffset is greater than first offset in batch we do NOT return the batch - this is ok
	// as the consumer wil only fetch with a fetchOffset equal to last offset + 1 so this will align with the start of
	// next batch.
	verifyDefaultResponse(t, resp, nil)
}

func TestFetcherSinglePartitionNoWaitFetch(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 0, 9999, 10000, 10, 2, topicProvider, controlClient, objStore)
	resp := sendFetchDefault(t, 4000, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	// We are fetching from offset 4000 out of offsets 0-9999, so we should not fetch the first 4 batches (0-3999)
	verifyDefaultResponse(t, resp, batches[4:])
}

func TestFetcherSinglePartitionNoWaitFetchAfterLastReadable(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	setupDataDefault(t, 100, 10000, 99, 1, 1, topicProvider, controlClient, objStore)
	resp := sendFetchDefault(t, 100, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	// Should receive no batches
	verifyDefaultResponse(t, resp, nil)
}

func TestFetcherSinglePartitionNoWaitFetchWellAfterLastReadable(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	setupDataDefault(t, 100, 10000, 0, 1, 1, topicProvider, controlClient, objStore)
	resp := sendFetchDefault(t, 100, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	// Should receive no batches
	verifyDefaultResponse(t, resp, nil)
}

func TestFetcherSinglePartitionNoWaitFetchAlignedWithLastReadable(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 100, 10000, 10000, 1, 1, topicProvider, controlClient, objStore)
	resp := sendFetchDefault(t, 100, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	// Should receive the batch
	verifyDefaultResponse(t, resp, batches)
}

func TestFetcherSinglePartitionNoWaitFetchMultipleBatchesSomeAfterLastReadable(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 0, 9999, 3999, 10, 2, topicProvider, controlClient, objStore)
	resp := sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	// batches 0-999, 1000-1999, 2000-2999 and 3000-3999 should be returned
	// Note that lastReadableOffset always aligns with the last offset in a batch.
	verifyDefaultResponse(t, resp, batches[:4])
}

func TestFetcherSinglePartitionNoWaitFetchMultipleBatchesSomeAfterLastReadableAndAfterFirstBatch(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 0, 9999, 3999, 10, 2, topicProvider, controlClient, objStore)
	resp := sendFetchDefault(t, 2000, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	// batches 2000-2999 and 3000-3999 should be returned
	// Note that lastReadableOffset always aligns with the last offset in a batch.
	verifyDefaultResponse(t, resp, batches[2:4])
}

func TestFetcherSingleTopicMultiplePartitionsFetchAll(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	batches1, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 23, 1000, 9999, 9999, 10, 2, topicProvider, controlClient, objStore)
	batches2, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 24, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batches3, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 25, 7000, 29999, 29999, 10, 2, topicProvider, controlClient, objStore)

	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  0,
		MaxBytes:  defaultMaxBytes,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(defaultTopicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         23,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         24,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         25,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
		},
	}
	resp := sendFetch(t, &req, fetcher)

	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 23, batches1)
	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 24, batches2)
	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 25, batches3)
}

func TestFetcherSingleTopicMultiplePartitionsFetchPartial(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	batches1, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 23, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	setupForPartition(t, defaultTopicID, defaultTopicName, 24, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batches3, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 25, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  0,
		MaxBytes:  defaultMaxBytes,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(defaultTopicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         23,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         24,
						FetchOffset:       130000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         25,
						FetchOffset:       11000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
		},
	}
	resp := sendFetch(t, &req, fetcher)

	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 23, batches1[2:])
	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 24, nil)
	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 25, batches3[4:])
}

func TestFetcherMultipleTopicsMultiplePartitionsFetchAll(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	topicIdA := 1001
	topicNameA := "topic-a"
	batchesA1, _ := setupForPartition(t, topicIdA, topicNameA, 23, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batchesA2, _ := setupForPartition(t, topicIdA, topicNameA, 24, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batchesA3, _ := setupForPartition(t, topicIdA, topicNameA, 25, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	topicIdB := 1002
	topicNameB := "topic-b"
	batchesB1, _ := setupForPartition(t, topicIdB, topicNameB, 33, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batchesB2, _ := setupForPartition(t, topicIdB, topicNameB, 34, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batchesB3, _ := setupForPartition(t, topicIdB, topicNameB, 35, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	topicIdC := 1003
	topicNameC := "topic-c"
	batchesC1, _ := setupForPartition(t, topicIdC, topicNameC, 43, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batchesC2, _ := setupForPartition(t, topicIdC, topicNameC, 44, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batchesC3, _ := setupForPartition(t, topicIdC, topicNameC, 45, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  0,
		MaxBytes:  defaultMaxBytes,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(topicNameA),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         23,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         24,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         25,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
			{
				Topic: common.StrPtr(topicNameB),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         33,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         34,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         35,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
			{
				Topic: common.StrPtr(topicNameC),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         43,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         44,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         45,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
		},
	}
	resp := sendFetch(t, &req, fetcher)

	verifyPartitionRecordsInResponse(t, resp, topicNameA, 23, batchesA1)
	verifyPartitionRecordsInResponse(t, resp, topicNameA, 24, batchesA2)
	verifyPartitionRecordsInResponse(t, resp, topicNameA, 25, batchesA3)

	verifyPartitionRecordsInResponse(t, resp, topicNameB, 33, batchesB1)
	verifyPartitionRecordsInResponse(t, resp, topicNameB, 34, batchesB2)
	verifyPartitionRecordsInResponse(t, resp, topicNameB, 35, batchesB3)

	verifyPartitionRecordsInResponse(t, resp, topicNameC, 43, batchesC1)
	verifyPartitionRecordsInResponse(t, resp, topicNameC, 44, batchesC2)
	verifyPartitionRecordsInResponse(t, resp, topicNameC, 45, batchesC3)
}

func TestFetcherMultipleTopicsMultiplePartitionsFetchAllMixtureSuccessAndFailure(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	topicIdA := 1001
	topicNameA := "topic-a"
	batchesA1, _ := setupForPartition(t, topicIdA, topicNameA, 23, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	setupForPartition(t, topicIdA, topicNameA, 24, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batchesA3, _ := setupForPartition(t, topicIdA, topicNameA, 25, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	topicIdB := 1002
	topicNameB := "topic-b"
	setupForPartition(t, topicIdB, topicNameB, 33, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	setupForPartition(t, topicIdB, topicNameB, 34, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	setupForPartition(t, topicIdB, topicNameB, 35, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	topicIdC := 1003
	topicNameC := "topic-c"
	setupForPartition(t, topicIdC, topicNameC, 43, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batchesC2, _ := setupForPartition(t, topicIdC, topicNameC, 44, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	setupForPartition(t, topicIdC, topicNameC, 45, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	unknownTopic := "nosuchtopic"

	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  0,
		MaxBytes:  defaultMaxBytes,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(topicNameA),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         23,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         888,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         25,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
			{
				Topic: common.StrPtr(unknownTopic),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         33,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         34,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         35,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
			{
				Topic: common.StrPtr(topicNameC),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         777,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         44,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         666,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
		},
	}
	resp := sendFetch(t, &req, fetcher)

	verifyPartitionRecordsInResponse(t, resp, topicNameA, 23, batchesA1)
	verifyPartitionErrorInResponse(t, resp, topicNameA, 888, kafkaprotocol.ErrorCodeUnknownTopicOrPartition)
	verifyPartitionRecordsInResponse(t, resp, topicNameA, 25, batchesA3)

	verifyPartitionErrorInResponse(t, resp, unknownTopic, 33, kafkaprotocol.ErrorCodeUnknownTopicOrPartition)
	verifyPartitionErrorInResponse(t, resp, unknownTopic, 34, kafkaprotocol.ErrorCodeUnknownTopicOrPartition)
	verifyPartitionErrorInResponse(t, resp, unknownTopic, 35, kafkaprotocol.ErrorCodeUnknownTopicOrPartition)

	verifyPartitionErrorInResponse(t, resp, topicNameC, 777, kafkaprotocol.ErrorCodeUnknownTopicOrPartition)
	verifyPartitionRecordsInResponse(t, resp, topicNameC, 44, batchesC2)
	verifyPartitionErrorInResponse(t, resp, topicNameC, 666, kafkaprotocol.ErrorCodeUnknownTopicOrPartition)
}

func TestFetcherMultipleTopicsMultiplePartitionsFetchPartial(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	topicIdA := 1001
	topicNameA := "topic-a"
	batchesA1, _ := setupForPartition(t, topicIdA, topicNameA, 23, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	setupForPartition(t, topicIdA, topicNameA, 24, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batchesA3, _ := setupForPartition(t, topicIdA, topicNameA, 25, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	topicIdB := 1002
	topicNameB := "topic-b"
	batchesB1, _ := setupForPartition(t, topicIdB, topicNameB, 33, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batchesB2, _ := setupForPartition(t, topicIdB, topicNameB, 34, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batchesB3, _ := setupForPartition(t, topicIdB, topicNameB, 35, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	topicIdC := 1003
	topicNameC := "topic-c"
	batchesC1, _ := setupForPartition(t, topicIdC, topicNameC, 43, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batchesC2, _ := setupForPartition(t, topicIdC, topicNameC, 44, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batchesC3, _ := setupForPartition(t, topicIdC, topicNameC, 45, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  0,
		MaxBytes:  defaultMaxBytes,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(topicNameA),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         23,
						FetchOffset:       2000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         24,
						FetchOffset:       13000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         25,
						FetchOffset:       9000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
			{
				Topic: common.StrPtr(topicNameB),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         33,
						FetchOffset:       4000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         34,
						FetchOffset:       0,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         35,
						FetchOffset:       14000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
			{
				Topic: common.StrPtr(topicNameC),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         43,
						FetchOffset:       2000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         44,
						FetchOffset:       5000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         45,
						FetchOffset:       6000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
		},
	}
	resp := sendFetch(t, &req, fetcher)

	verifyPartitionRecordsInResponse(t, resp, topicNameA, 23, batchesA1[1:])
	verifyPartitionRecordsInResponse(t, resp, topicNameA, 24, nil)
	verifyPartitionRecordsInResponse(t, resp, topicNameA, 25, batchesA3[2:])

	verifyPartitionRecordsInResponse(t, resp, topicNameB, 33, batchesB1[3:])
	verifyPartitionRecordsInResponse(t, resp, topicNameB, 34, batchesB2)
	verifyPartitionRecordsInResponse(t, resp, topicNameB, 35, batchesB3[7:])

	verifyPartitionRecordsInResponse(t, resp, topicNameC, 43, batchesC1[1:])
	verifyPartitionRecordsInResponse(t, resp, topicNameC, 44, batchesC2[2:])
	verifyPartitionRecordsInResponse(t, resp, topicNameC, 45, batchesC3)
}

func TestFetcherSinglePartitionNoWaitPartitionMaxSizeExactlyFirstBatchSize(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 0, 9999, 9999, 10, 2, topicProvider, controlClient, objStore)
	partitionMaxBytes := len(batches[0])
	resp := sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, partitionMaxBytes, fetcher)
	// Only first batch should be fetched
	verifyDefaultResponse(t, resp, batches[:1])
}

func TestFetcherSinglePartitionNoWaitPartitionMaxSizeSlightlyLessThanFirstBatchSize(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 0, 9999, 9999, 10, 2, topicProvider, controlClient, objStore)
	partitionMaxBytes := len(batches[0]) - 1
	resp := sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, partitionMaxBytes, fetcher)
	// Only first batch should be fetched - as we always return one batch if available even if it exceeds max size
	verifyDefaultResponse(t, resp, batches[:1])
}

func TestFetcherSinglePartitionNoWaitPartitionMaxSizeExactlyFirstTwoBatchSizes(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 0, 9999, 9999, 10, 2, topicProvider, controlClient, objStore)
	partitionMaxBytes := len(batches[0]) + len(batches[1])
	resp := sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, partitionMaxBytes, fetcher)
	// First two batches should be fetched
	verifyDefaultResponse(t, resp, batches[:2])
}

func TestFetcherSinglePartitionNoWaitPartitionMaxSizeSlightlyMoreThanTwoBatchSizes(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 0, 9999, 9999, 10, 2, topicProvider, controlClient, objStore)
	partitionMaxBytes := len(batches[0]) + len(batches[1]) + 1
	resp := sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, partitionMaxBytes, fetcher)
	// First two batches should be fetched
	verifyDefaultResponse(t, resp, batches[:2])
}

func TestFetcherSingleTopicMultiplePartitionsNoWaitPartitionMax(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	batches1, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 23, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batches2, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 24, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batches3, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 25, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  0,
		MaxBytes:  defaultMaxBytes,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(defaultTopicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         23,
						FetchOffset:       1000,
						PartitionMaxBytes: int32(len(batches1[0])),
					},
					{
						Partition:         24,
						FetchOffset:       3000,
						PartitionMaxBytes: int32(len(batches2[0]) + len(batches2[1]) + len(batches2[2])),
					},
					{
						Partition:         25,
						FetchOffset:       7000,
						PartitionMaxBytes: int32(len(batches3[0]) + len(batches3[1]) + len(batches3[2]) + len(batches3[3]) + 1),
					},
				},
			},
		},
	}
	resp := sendFetch(t, &req, fetcher)

	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 23, batches1[:1])
	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 24, batches2[:3])
	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 25, batches3[:4])
}

func TestFetcherSinglePartitionNoWaitRequestMaxSizeExceeded(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 0, 9999, 9999, 10, 2, topicProvider, controlClient, objStore)
	maxBytes := len(batches[0]) + len(batches[1]) + 1
	resp := sendFetchDefault(t, 0, 0, 0, maxBytes, defaultMaxBytes, fetcher)
	// First two batches should be fetched
	verifyDefaultResponse(t, resp, batches[:2])
}

func TestFetcherSingleTopicMultiplePartitionsNoWaitRequestMaxSizeExceededV3(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	batches1, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 23, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	setupForPartition(t, defaultTopicID, defaultTopicName, 24, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	setupForPartition(t, defaultTopicID, defaultTopicName, 25, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	maxBytes := len(common.RemoveValueMetadata(batches1[0])) * 5

	// The order in which partitions are fetched is not deterministic so we just validate total max size
	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  0,
		MaxBytes:  int32(maxBytes),
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(defaultTopicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         23,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         24,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         25,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
		},
	}
	resp := sendFetchWithVersion(t, &req, fetcher, 3)

	totSize := 0
	for _, topicResp := range resp.Responses {
		for _, partResp := range topicResp.Partitions {
			require.Equal(t, kafkaprotocol.ErrorCodeNone, int(partResp.ErrorCode))
			totSize += len(partResp.Records)
		}
	}
	// Should be exactly equal as our max size is in multiples of batches
	require.Equal(t, totSize, maxBytes)
}

func TestFetcherSingleTopicMultiplePartitionsNoWaitRequestMaxSizeExceededV2(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	batches1, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 23, 1000, 100999,
		100999, 100, 2, topicProvider, controlClient, objStore)

	// version < 3 of fetch doesn't support max bytes so it defaults
	expectedBytes := 0
	for _, batch := range batches1 {
		lb := len(common.RemoveValueMetadata(batch))
		if expectedBytes+lb > defaultFetchMaxBytes {
			break
		}
		expectedBytes += lb
	}

	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  0,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(defaultTopicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         23,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
		},
	}
	resp := sendFetchWithVersion(t, &req, fetcher, 2)

	totSize := 0
	for _, topicResp := range resp.Responses {
		for _, partResp := range topicResp.Partitions {
			require.Equal(t, kafkaprotocol.ErrorCodeNone, int(partResp.ErrorCode))
			totSize += len(partResp.Records)
		}
	}
	require.Equal(t, expectedBytes, totSize)
}

func TestFetcherSinglePartitionNoWaitAndMinBytesExactlyReached(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 0, 9999, 9999, 10, 2, topicProvider, controlClient, objStore)
	totBatchSizes := 0
	for _, batch := range batches {
		totBatchSizes += len(common.RemoveValueMetadata(batch))
	}
	resp := sendFetchDefault(t, 0, 0, totBatchSizes, defaultMaxBytes, defaultMaxBytes, fetcher)
	// All batches should be fetched
	verifyDefaultResponse(t, resp, batches)
}

func TestFetcherSinglePartitionNoWaitAndMinBytesNotReached(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 0, 9999, 9999, 10, 2, topicProvider, controlClient, objStore)
	totBatchSizes := 0
	for _, batch := range batches {
		totBatchSizes += len(batch)
	}
	resp := sendFetchDefault(t, 0, 0, totBatchSizes+1, defaultMaxBytes, defaultMaxBytes, fetcher)
	// No batches should be fetched
	verifyDefaultResponse(t, resp, nil)
}

func TestFetcherSinglePartitionMoreThanMinBytesReached(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 0, 9999, 9999, 10, 2, topicProvider, controlClient, objStore)
	totBatchSizes := 0
	for _, batch := range batches {
		totBatchSizes += len(common.RemoveValueMetadata(batch))
	}
	resp := sendFetchDefault(t, 0, 0, totBatchSizes-1, defaultMaxBytes, defaultMaxBytes, fetcher)
	// All batches should be fetched
	verifyDefaultResponse(t, resp, batches)
}

func TestFetcherSinglePartitionMinBytesNotReachedMultiplePartitions(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	batches1, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 23, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batches2, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 24, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batches3, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 25, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	totSize := 0
	for _, batch := range batches1 {
		totSize += len(batch)
	}
	for _, batch := range batches2 {
		totSize += len(batch)
	}
	for _, batch := range batches3 {
		totSize += len(batch)
	}

	// The order in which partitions are fetched is not deterministic so we just validate total max size
	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  int32(totSize + 1),
		MaxBytes:  defaultMaxBytes,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(defaultTopicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         23,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         24,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         25,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
		},
	}
	resp := sendFetch(t, &req, fetcher)

	// No batches should be returned
	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 23, nil)
	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 24, nil)
	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 25, nil)
}

func TestFetcherSinglePartitionMinBytesReachedExactlyMultiplePartitions(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	batches1, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 23, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batches2, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 24, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batches3, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 25, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	totSize := 0
	for _, batch := range batches1 {
		totSize += len(common.RemoveValueMetadata(batch))
	}
	for _, batch := range batches2 {
		totSize += len(common.RemoveValueMetadata(batch))
	}
	for _, batch := range batches3 {
		totSize += len(common.RemoveValueMetadata(batch))
	}

	// The order in which partitions are fetched is not deterministic so we just validate total max size
	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  int32(totSize),
		MaxBytes:  defaultMaxBytes,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(defaultTopicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         23,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         24,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         25,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
		},
	}
	resp := sendFetch(t, &req, fetcher)

	// No batches should be returned
	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 23, batches1)
	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 24, batches2)
	verifyPartitionRecordsInResponse(t, resp, defaultTopicName, 25, batches3)
}

func TestFetcherMinBytesNotReachedMultipleTopicsMultiplePartitions(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	var allBatches [][]byte

	topicIdA := 1001
	topicNameA := "topic-a"
	batchesA1, _ := setupForPartition(t, topicIdA, topicNameA, 23, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batchesA2, _ := setupForPartition(t, topicIdA, topicNameA, 24, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batchesA3, _ := setupForPartition(t, topicIdA, topicNameA, 25, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)
	allBatches = append(allBatches, batchesA1...)
	allBatches = append(allBatches, batchesA2...)
	allBatches = append(allBatches, batchesA3...)

	topicIdB := 1002
	topicNameB := "topic-b"
	batchesB1, _ := setupForPartition(t, topicIdB, topicNameB, 33, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batchesB2, _ := setupForPartition(t, topicIdB, topicNameB, 34, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batchesB3, _ := setupForPartition(t, topicIdB, topicNameB, 35, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)
	allBatches = append(allBatches, batchesB1...)
	allBatches = append(allBatches, batchesB2...)
	allBatches = append(allBatches, batchesB3...)

	topicIdC := 1003
	topicNameC := "topic-c"
	batchesC1, _ := setupForPartition(t, topicIdC, topicNameC, 43, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batchesC2, _ := setupForPartition(t, topicIdC, topicNameC, 44, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batchesC3, _ := setupForPartition(t, topicIdC, topicNameC, 45, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)
	allBatches = append(allBatches, batchesC1...)
	allBatches = append(allBatches, batchesC2...)
	allBatches = append(allBatches, batchesC3...)

	totSize := 0
	for _, batch := range allBatches {
		totSize += len(batch)
	}

	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  int32(totSize + 1),
		MaxBytes:  defaultMaxBytes,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(topicNameA),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         23,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         24,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         25,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
			{
				Topic: common.StrPtr(topicNameB),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         33,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         34,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         35,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
			{
				Topic: common.StrPtr(topicNameC),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         43,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         44,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         45,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
		},
	}
	resp := sendFetch(t, &req, fetcher)

	verifyPartitionRecordsInResponse(t, resp, topicNameA, 23, nil)
	verifyPartitionRecordsInResponse(t, resp, topicNameA, 24, nil)
	verifyPartitionRecordsInResponse(t, resp, topicNameA, 25, nil)

	verifyPartitionRecordsInResponse(t, resp, topicNameB, 33, nil)
	verifyPartitionRecordsInResponse(t, resp, topicNameB, 34, nil)
	verifyPartitionRecordsInResponse(t, resp, topicNameB, 35, nil)

	verifyPartitionRecordsInResponse(t, resp, topicNameC, 43, nil)
	verifyPartitionRecordsInResponse(t, resp, topicNameC, 44, nil)
	verifyPartitionRecordsInResponse(t, resp, topicNameC, 45, nil)
}

func TestFetcherMinBytesExactlyReachedMultipleTopicsMultiplePartitions(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	var allBatches [][]byte

	topicIdA := 1001
	topicNameA := "topic-a"
	batchesA1, _ := setupForPartition(t, topicIdA, topicNameA, 23, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batchesA2, _ := setupForPartition(t, topicIdA, topicNameA, 24, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batchesA3, _ := setupForPartition(t, topicIdA, topicNameA, 25, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)
	allBatches = append(allBatches, batchesA1...)
	allBatches = append(allBatches, batchesA2...)
	allBatches = append(allBatches, batchesA3...)

	topicIdB := 1002
	topicNameB := "topic-b"
	batchesB1, _ := setupForPartition(t, topicIdB, topicNameB, 33, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batchesB2, _ := setupForPartition(t, topicIdB, topicNameB, 34, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batchesB3, _ := setupForPartition(t, topicIdB, topicNameB, 35, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)
	allBatches = append(allBatches, batchesB1...)
	allBatches = append(allBatches, batchesB2...)
	allBatches = append(allBatches, batchesB3...)

	topicIdC := 1003
	topicNameC := "topic-c"
	batchesC1, _ := setupForPartition(t, topicIdC, topicNameC, 43, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	batchesC2, _ := setupForPartition(t, topicIdC, topicNameC, 44, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	batchesC3, _ := setupForPartition(t, topicIdC, topicNameC, 45, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)
	allBatches = append(allBatches, batchesC1...)
	allBatches = append(allBatches, batchesC2...)
	allBatches = append(allBatches, batchesC3...)

	totSize := 0
	for _, batch := range allBatches {
		totSize += len(common.RemoveValueMetadata(batch))
	}

	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  int32(totSize),
		MaxBytes:  defaultMaxBytes,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(topicNameA),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         23,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         24,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         25,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
			{
				Topic: common.StrPtr(topicNameB),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         33,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         34,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         35,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
			{
				Topic: common.StrPtr(topicNameC),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         43,
						FetchOffset:       1000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         44,
						FetchOffset:       3000,
						PartitionMaxBytes: defaultMaxBytes,
					},
					{
						Partition:         45,
						FetchOffset:       7000,
						PartitionMaxBytes: defaultMaxBytes,
					},
				},
			},
		},
	}
	resp := sendFetch(t, &req, fetcher)

	verifyPartitionRecordsInResponse(t, resp, topicNameA, 23, batchesA1)
	verifyPartitionRecordsInResponse(t, resp, topicNameA, 24, batchesA2)
	verifyPartitionRecordsInResponse(t, resp, topicNameA, 25, batchesA3)

	verifyPartitionRecordsInResponse(t, resp, topicNameB, 33, batchesB1)
	verifyPartitionRecordsInResponse(t, resp, topicNameB, 34, batchesB2)
	verifyPartitionRecordsInResponse(t, resp, topicNameB, 35, batchesB3)

	verifyPartitionRecordsInResponse(t, resp, topicNameC, 43, batchesC1)
	verifyPartitionRecordsInResponse(t, resp, topicNameC, 44, batchesC2)
	verifyPartitionRecordsInResponse(t, resp, topicNameC, 45, batchesC3)
}

func TestFetcherSinglePartitionMinBytesNotReachedTimeout(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 0, 9999, 9999, 10, 2, topicProvider, controlClient, objStore)
	totBatchSizes := 0
	for _, batch := range batches {
		totBatchSizes += len(batch)
	}
	timeout := 100 * time.Millisecond
	start := time.Now()
	resp := sendFetchDefault(t, 0, timeout, totBatchSizes+1, defaultMaxBytes, defaultMaxBytes, fetcher)
	dur := time.Since(start)
	require.GreaterOrEqual(t, dur, timeout)
	// At timeout, whatever data there is, is returned even if less than min bytes
	verifyDefaultResponse(t, resp, batches)
}

func TestFetcherControllerUnavailabilitySinglePartition(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	batches, _ := setupDataDefault(t, 0, 9999, 9999, 1, 1, topicProvider, controlClient, objStore)

	controlClient.setUnavailable()

	resp := sendFetchDefault(t, 0, 0, len(batches[0]), defaultMaxBytes, defaultMaxBytes, fetcher)
	require.Equal(t, 1, len(resp.Responses))
	topicResp := resp.Responses[0]
	require.Equal(t, 1, len(topicResp.Partitions))
	partResp := topicResp.Partitions[0]
	require.Equal(t, kafkaprotocol.ErrorCodeLeaderNotAvailable, int(partResp.ErrorCode))
}

func TestFetcherControllerUnavailabilityMultiplePartitions(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	setupDataDefault(t, 0, 9999, 9999, 1, 1, topicProvider, controlClient, objStore)

	controlClient.setUnavailable()

	req := kafkaprotocol.FetchRequest{
		MinBytes: 0,
		MaxBytes: int32(defaultMaxBytes),
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(defaultTopicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         int32(0),
						FetchOffset:       0,
						PartitionMaxBytes: int32(defaultMaxBytes),
					},
					{
						Partition:         int32(1),
						FetchOffset:       0,
						PartitionMaxBytes: int32(defaultMaxBytes),
					},
				},
			},
		},
	}
	resp := sendFetch(t, &req, fetcher)
	require.Equal(t, 1, len(resp.Responses))
	topicResp := resp.Responses[0]
	require.Equal(t, 2, len(topicResp.Partitions))
	require.Equal(t, kafkaprotocol.ErrorCodeLeaderNotAvailable, int(topicResp.Partitions[0].ErrorCode))
	require.Equal(t, kafkaprotocol.ErrorCodeLeaderNotAvailable, int(topicResp.Partitions[1].ErrorCode))
}

func TestFetcherControllerUnexpectedErrorSinglePartition(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	batches, _ := setupDataDefault(t, 0, 9999, 9999, 1, 1, topicProvider, controlClient, objStore)

	controlClient.setFailWithUnexpectedErr()

	resp := sendFetchDefault(t, 0, 0, len(batches[0]), defaultMaxBytes, defaultMaxBytes, fetcher)
	require.Equal(t, 1, len(resp.Responses))
	topicResp := resp.Responses[0]
	require.Equal(t, 1, len(topicResp.Partitions))
	partResp := topicResp.Partitions[0]
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownServerError, int(partResp.ErrorCode))
}

func TestFetcherErrorUnknownTopic(t *testing.T) {
	fetcher, _, _, _ := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	req := kafkaprotocol.FetchRequest{
		MinBytes: 0,
		MaxBytes: int32(defaultMaxBytes),
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr("nosuchtopic"),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         int32(defaultPartitionID),
						FetchOffset:       0,
						PartitionMaxBytes: int32(defaultMaxBytes),
					},
				},
			},
		},
	}
	resp := sendFetch(t, &req, fetcher)
	require.Equal(t, 1, len(resp.Responses))
	topicResp := resp.Responses[0]
	require.Equal(t, 1, len(topicResp.Partitions))
	partResp := topicResp.Partitions[0]
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownTopicOrPartition, int(partResp.ErrorCode))
}

func TestFetcherErrorUnknownPartition(t *testing.T) {
	testFetcherErrorUnknownPartition(t, 777)
	testFetcherErrorUnknownPartition(t, -1)
}

func testFetcherErrorUnknownPartition(t *testing.T, partitionID int) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	setupDataDefault(t, 100, 10000, 10000, 1, 1, topicProvider, controlClient, objStore)

	req := kafkaprotocol.FetchRequest{
		MinBytes: 0,
		MaxBytes: int32(defaultMaxBytes),
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(defaultTopicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         int32(partitionID), // invalid partition
						FetchOffset:       0,
						PartitionMaxBytes: int32(defaultMaxBytes),
					},
				},
			},
		},
	}
	resp := sendFetch(t, &req, fetcher)
	require.Equal(t, 1, len(resp.Responses))
	topicResp := resp.Responses[0]
	require.Equal(t, 1, len(topicResp.Partitions))
	partResp := topicResp.Partitions[0]
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownTopicOrPartition, int(partResp.ErrorCode))
}

func setupFetcher(t *testing.T) (*BatchFetcher, *testTopicProvider, *testControlClient, objstore.Client) {
	objStore := dev.NewInMemStore(0)
	infoProvider := &testTopicProvider{infos: map[string]topicmeta.TopicInfo{}}
	partHashes, err := parthash.NewPartitionHashes(0)
	require.NoError(t, err)
	getter := &testTableGetter{
		bucketName: databucketName,
		objStore:   objStore,
	}
	controlClient := newTestControlClient()
	controlFactory := func() (control.Client, error) {
		return controlClient, nil
	}
	controlClientCache := control.NewClientCache(10, controlFactory)
	cfg := NewConf()
	cfg.DataBucketName = databucketName
	fetcher, err := NewBatchFetcher(objStore, infoProvider, partHashes, controlClientCache, getter.getSSTable, cfg)
	require.NoError(t, err)
	err = fetcher.Start()
	require.NoError(t, err)
	err = fetcher.MembershipChanged(0, cluster.MembershipState{
		LeaderVersion:  1,
		ClusterVersion: 1,
	})
	require.NoError(t, err)
	return fetcher, infoProvider, controlClient, objStore
}

func setupTable(t *testing.T, batchInfos []partitionBatchInfo, objStore objstore.Client, bucketName string) (sst.SSTableID, [][]byte) {
	partHashes, err := parthash.NewPartitionHashes(len(batchInfos))
	require.NoError(t, err)
	var kvs []common.KV
	var batches [][]byte
	for _, info := range batchInfos {
		prefix, err := partHashes.GetPartitionHash(info.topicID, info.partitionID)
		require.NoError(t, err)
		key := make([]byte, 0, 24)
		key = append(key, prefix...)
		key = append(key, common.EntryTypeTopicData)
		key = encoding.KeyEncodeInt(key, info.offsetStart)
		key = encoding.EncodeVersion(key, 0)
		batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(int(info.offsetStart), info.numRecords)
		batch = common.AppendValueMetadata(batch, int64(info.topicID), int64(info.partitionID))
		batches = append(batches, batch)
		kvs = append(kvs, common.KV{
			Key:   key,
			Value: batch,
		})
	}
	sort.SliceStable(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
	})
	iter := common.NewKvSliceIterator(kvs)
	table, _, _, _, _, err := sst.BuildSSTable(common.DataFormatV1, 0, 0, iter)
	require.NoError(t, err)
	tableID := sst.CreateSSTableId()
	tableData := table.Serialize()
	err = objStore.Put(context.Background(), bucketName, tableID, tableData)
	require.NoError(t, err)
	return []byte(tableID), batches
}

type partitionBatchInfo struct {
	topicID     int
	partitionID int
	offsetStart int64
	numRecords  int
}

func setupDataDefault(t *testing.T, firstOffset int, lastOffset int, lastReadableOffset int, numBatches int,
	numTables int, topicProvider *testTopicProvider, controlClient *testControlClient, objStore objstore.Client) ([][]byte, []sst.SSTableID) {
	return setupForPartition(t, defaultTopicID, defaultTopicName, defaultPartitionID, firstOffset, lastOffset, lastReadableOffset, numBatches, numTables, topicProvider, controlClient, objStore)
}

func setupForPartition(t *testing.T, topicID int, topicName string, partitionID int, firstOffset int, lastOffset int, lastReadableOffset int, numBatches int,
	numTables int, topicProvider *testTopicProvider, controlClient *testControlClient, objStore objstore.Client) ([][]byte, []sst.SSTableID) {
	topicProvider.infos[topicName] = topicmeta.TopicInfo{
		ID:             topicID,
		Name:           topicName,
		PartitionCount: defaultNumPartitions,
	}
	totBatches, tabIDs, queryRes := setupBatchesForPartition(t, topicID, partitionID, firstOffset, lastOffset, numBatches, numTables, objStore)
	controlClient.queryRes = append(controlClient.queryRes, queryRes...)
	controlClient.setLastReadableOffset(topicID, partitionID, int64(lastReadableOffset))
	return totBatches, tabIDs
}

func setupBatchesForPartition(t *testing.T, topicID int, partitionID int, firstOffset int, lastOffset int, numBatches int,
	numTables int, objStore objstore.Client) ([][]byte, []sst.SSTableID, lsm.OverlappingTables) {
	numRecords := lastOffset - firstOffset + 1
	numRecordsPerBatch := numRecords / numBatches
	offset := firstOffset
	var batchInfos []partitionBatchInfo
	for i := 0; i < numBatches; i++ {
		batchInfos = append(batchInfos, partitionBatchInfo{
			topicID:     topicID,
			partitionID: partitionID,
			offsetStart: int64(offset),
			numRecords:  numRecordsPerBatch,
		})
		offset += numRecordsPerBatch
	}
	if len(batchInfos) != numBatches {
		panic("num records must be divisible by num batches")
	}
	if numBatches%numTables != 0 {
		panic("num batches must be divisible by num tables")
	}
	var tabIDs []sst.SSTableID
	var ids lsm.OverlappingTables
	var totBatches [][]byte
	numBatchesPerTable := numBatches / numTables
	var tableInfos []partitionBatchInfo
	for i, batchInfo := range batchInfos {
		tableInfos = append(tableInfos, batchInfo)
		if (i+1)%numBatchesPerTable == 0 {
			tableID, batches := setupTable(t, tableInfos, objStore, databucketName)
			tabIDs = append(tabIDs, tableID)
			ids = append(ids, lsm.NonOverlappingTables{{ID: tableID}})
			totBatches = append(totBatches, batches...)
			tableInfos = nil
		}
	}
	require.Equal(t, numBatches, len(totBatches))
	return totBatches, tabIDs, ids
}

func sendFetchDefault(t *testing.T, fetchOffset int, maxWait time.Duration, minBytes int, maxBytes int, partitionMaxBytes int, fetcher *BatchFetcher) *kafkaprotocol.FetchResponse {
	return sendFetchRequest(t, defaultTopicName, defaultPartitionID, fetchOffset, maxWait, minBytes, maxBytes, partitionMaxBytes, fetcher)
}

func sendFetchRequest(t *testing.T, topicName string, partitionID int, fetchOffset int, maxWait time.Duration, minBytes int, maxBytes int, partitionMaxBytes int, fetcher *BatchFetcher) *kafkaprotocol.FetchResponse {
	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: int32(maxWait.Milliseconds()),
		MinBytes:  int32(minBytes),
		MaxBytes:  int32(maxBytes),
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(topicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         int32(partitionID),
						FetchOffset:       int64(fetchOffset),
						PartitionMaxBytes: int32(partitionMaxBytes),
					},
				},
			},
		},
	}
	return sendFetch(t, &req, fetcher)
}

func sendFetch(t *testing.T, req *kafkaprotocol.FetchRequest, fetcher *BatchFetcher) *kafkaprotocol.FetchResponse {
	return sendFetchWithVersion(t, req, fetcher, 3)
}

func sendFetchWithVersion(t *testing.T, req *kafkaprotocol.FetchRequest, fetcher *BatchFetcher, apiVersion int16) *kafkaprotocol.FetchResponse {
	ch := make(chan *kafkaprotocol.FetchResponse, 1)
	err := fetcher.HandleFetchRequest(nil, apiVersion, req, func(resp *kafkaprotocol.FetchResponse) error {
		ch <- resp
		return nil
	})
	require.NoError(t, err)
	return <-ch
}

func stopFetcher(t *testing.T, fetcher *BatchFetcher) {
	err := fetcher.Stop()
	require.NoError(t, err)
}

func verifyDefaultResponse(t *testing.T, resp *kafkaprotocol.FetchResponse, batches [][]byte) {
	verifySinglePartitionResponse(t, resp, defaultTopicName, defaultPartitionID, batches)
}

func verifySinglePartitionResponse(t *testing.T, resp *kafkaprotocol.FetchResponse, topicName string, partitionID int, batches [][]byte) {
	require.Equal(t, 1, len(resp.Responses))
	topicResp := resp.Responses[0]
	require.Equal(t, topicName, *topicResp.Topic)
	require.Equal(t, 1, len(topicResp.Partitions))
	partitionResp := topicResp.Partitions[0]
	require.Equal(t, partitionID, int(partitionResp.PartitionIndex))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(partitionResp.ErrorCode))
	if len(batches) == 0 {
		require.Equal(t, 0, len(partitionResp.Records))
	} else {
		verifyBatchesSame(t, batches, partitionResp.Records)
	}
}

func verifyBatchesSame(t *testing.T, expected [][]byte, batches []byte) {
	extracted := extractBatches(batches)
	require.Equal(t, len(expected), len(extracted))
	for i, exp := range expected {
		require.Equal(t, common.RemoveValueMetadata(exp), extracted[i])
	}
}

func extractBatches(buff []byte) [][]byte {
	if len(buff) == 0 {
		return nil
	}
	// Multiple record batches are concatenated together
	var batches [][]byte
	for {
		batchLen := binary.BigEndian.Uint32(buff[8:])
		batch := buff[:int(batchLen)+12] // 12: First two fields are not included in size
		batches = append(batches, batch)
		if int(batchLen)+12 == len(buff) {
			break
		}
		buff = buff[int(batchLen)+12:]
	}
	return batches
}

func verifyPartitionRecordsInResponse(t *testing.T, resp *kafkaprotocol.FetchResponse, topicName string, partitionID int, batches [][]byte) {
	var tResp *kafkaprotocol.FetchResponseFetchableTopicResponse
	for _, topicResp := range resp.Responses {
		if *topicResp.Topic == topicName {
			tResp = &topicResp
		}
	}
	require.NotNil(t, tResp)
	partResps := map[int]*kafkaprotocol.FetchResponsePartitionData{}
	for _, partResp := range tResp.Partitions {
		partResps[int(partResp.PartitionIndex)] = &partResp
	}
	partResp, ok := partResps[partitionID]
	require.True(t, ok)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(partResp.ErrorCode))
	verifyBatchesSame(t, batches, partResp.Records)
}

func verifyPartitionErrorInResponse(t *testing.T, resp *kafkaprotocol.FetchResponse, topicName string, partitionID int, errCode int) {
	var tResp *kafkaprotocol.FetchResponseFetchableTopicResponse
	for _, topicResp := range resp.Responses {
		if *topicResp.Topic == topicName {
			tResp = &topicResp
		}
	}
	require.NotNil(t, tResp)
	partResps := map[int]*kafkaprotocol.FetchResponsePartitionData{}
	for _, partResp := range tResp.Partitions {
		partResps[int(partResp.PartitionIndex)] = &partResp
	}
	partResp, ok := partResps[partitionID]
	require.True(t, ok)
	require.Equal(t, errCode, int(partResp.ErrorCode))
}

func newTestControlClient() *testControlClient {
	return &testControlClient{
		lastReadableOffsets: map[int]map[int]int64{},
	}
}

type testControlClient struct {
	lock                sync.Mutex
	queryRes            lsm.OverlappingTables
	lastReadableOffsets map[int]map[int]int64
	unavailable         bool
	unexpectedErr       bool
	memberID            int32
	resetSequence       int64
}

func (t *testControlClient) clearQueryRes() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.queryRes = nil
}

func (t *testControlClient) setUnavailable() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.unavailable = true
}

func (t *testControlClient) setFailWithUnexpectedErr() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.unexpectedErr = true
}

func (t *testControlClient) RegisterTableListener(topicID int, partitionID int, memberID int32,
	resetSequence int64) (int64, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.unavailable {
		return 0, common.NewTektiteErrorf(common.Unavailable, "controller is unavailable")
	}
	if t.unexpectedErr {
		return 0, errors.New("unexpected error")
	}
	partMap, ok := t.lastReadableOffsets[topicID]
	if !ok {
		return 0, errors.Errorf("unknown topic: %d", topicID)
	}
	off, ok := partMap[partitionID]
	if !ok {
		return 0, errors.Errorf("unknown partition: %d", partitionID)
	}
	t.memberID = memberID
	t.resetSequence = resetSequence
	return off, nil
}

func (t *testControlClient) QueryTablesForPartition(topicID int, partitionID int, keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, int64, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.unavailable {
		return nil, 0, common.NewTektiteErrorf(common.Unavailable, "controller is unavailable")
	}
	if t.unexpectedErr {
		return nil, 0, errors.New("unexpected error")
	}
	partOffs, ok := t.lastReadableOffsets[topicID]
	if !ok {
		return nil, 0, common.NewTektiteErrorf(common.Unavailable, "unknown topic")
	}
	partOff, ok := partOffs[partitionID]
	if !ok {
		return nil, 0, common.NewTektiteErrorf(common.Unavailable, "unknown partition")
	}
	return t.queryRes, partOff, nil
}

func (t *testControlClient) QueryTablesInRange(_ []byte, _ []byte) (lsm.OverlappingTables, error) {
	panic("should not be called")
}

func (t *testControlClient) getMemberIDAndResetSequence() (int32, int64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.memberID, t.resetSequence
}

func (t *testControlClient) setLastReadableOffset(topicID int, partitionID int, offset int64) {
	partMap, ok := t.lastReadableOffsets[topicID]
	if !ok {
		partMap = map[int]int64{}
		t.lastReadableOffsets[topicID] = partMap
	}
	partMap[partitionID] = offset
}

func (t *testControlClient) PrePush(infos []offsets.GenerateOffsetTopicInfo, epochInfos []control.EpochInfo) ([]offsets.OffsetTopicInfo, int64, []bool, error) {
	panic("should not be called")
}

func (t *testControlClient) ApplyLsmChanges(regBatch lsm.RegistrationBatch) error {
	panic("should not be called")

}

func (t *testControlClient) RegisterL0Table(sequence int64, regEntry lsm.RegistrationEntry) error {
	panic("should not be called")
}

func (t *testControlClient) PollForJob() (lsm.CompactionJob, error) {
	panic("should not be called")
}

func (t *testControlClient) GetOffsetInfos(infos []offsets.GetOffsetTopicInfo) ([]offsets.OffsetTopicInfo, error) {
	panic("should not be called")
}

func (t *testControlClient) GetTopicInfo(topicName string) (topicmeta.TopicInfo, int, bool, error) {
	panic("should not be called")
}

func (t *testControlClient) GetTopicInfoByID(topicID int) (topicmeta.TopicInfo, bool, error) {
	panic("should not be called")
}

func (t *testControlClient) GetAllTopicInfos() ([]topicmeta.TopicInfo, error) {
	panic("should not be called")
}

func (t *testControlClient) CreateOrUpdateTopic(topicInfo topicmeta.TopicInfo, create bool) error {
	panic("should not be called")
}

func (t *testControlClient) DeleteTopic(topicName string) error {
	panic("should not be called")
}

func (t *testControlClient) GetCoordinatorInfo(key string) (memberID int32, address string, groupEpoch int, err error) {
	panic("should not be called")
}

func (t *testControlClient) GenerateSequence(sequenceName string) (int64, error) {
	panic("should not be called")
}

func (t *testControlClient) PutUserCredentials(username string, storedKey []byte, serverKey []byte, salt string, iters int) error {
	panic("should not be called")
}

func (t *testControlClient) DeleteUserCredentials(username string) error {
	panic("should not be called")
}

func (t *testControlClient) Authorise(principal string, resourceType acls.ResourceType, resourceName string, operation acls.Operation) (bool, error) {
	panic("should not be called")
}

func (t *testControlClient) CreateAcls(aclEntries []acls.AclEntry) error {
	panic("should not be called")
}

func (t *testControlClient) ListAcls(resourceType acls.ResourceType, resourceNameFilter string, patternTypeFilter acls.ResourcePatternType, principal string, host string, operation acls.Operation, permission acls.Permission) ([]acls.AclEntry, error) {
	panic("should not be called")
}

func (t *testControlClient) DeleteAcls(resourceType acls.ResourceType, resourceNameFilter string, patternTypeFilter acls.ResourcePatternType, principal string, host string, operation acls.Operation, permission acls.Permission) error {
	panic("should not be called")
}

func (t *testControlClient) Close() error {
	return nil
}

type testTableGetter struct {
	bucketName string
	objStore   objstore.Client
}

func (t *testTableGetter) getSSTable(id sst.SSTableID) (*sst.SSTable, error) {
	buff, err := t.objStore.Get(context.Background(), t.bucketName, string(id))
	if err != nil {
		return nil, err
	}
	if len(buff) == 0 {
		return nil, errors.New("table not found")
	}
	var table sst.SSTable
	table.Deserialize(buff, 0)
	return &table, nil
}

type testTopicProvider struct {
	infos map[string]topicmeta.TopicInfo
}

func (t *testTopicProvider) GetTopicInfo(topicName string) (topicmeta.TopicInfo, bool, error) {
	info, ok := t.infos[topicName]
	if !ok {
		return topicmeta.TopicInfo{}, false, nil
	}
	return info, true, nil
}
