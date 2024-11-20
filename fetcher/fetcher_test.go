package fetcher

import (
	"bytes"
	"context"
	"github.com/pkg/errors"
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
	"sync/atomic"
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

func TestFetcherSingleTopicMultiplePartitionsNoWaitRequestMaxSizeExceeded(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	batches1, _ := setupForPartition(t, defaultTopicID, defaultTopicName, 23, 1000, 10999, 10999, 10, 2, topicProvider, controlClient, objStore)
	setupForPartition(t, defaultTopicID, defaultTopicName, 24, 3000, 12999, 12999, 10, 2, topicProvider, controlClient, objStore)
	setupForPartition(t, defaultTopicID, defaultTopicName, 25, 7000, 16999, 16999, 10, 2, topicProvider, controlClient, objStore)

	maxBytes := len(batches1[0]) * 5

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
	resp := sendFetch(t, &req, fetcher)

	totSize := 0
	for _, topicResp := range resp.Responses {
		for _, partResp := range topicResp.Partitions {
			require.Equal(t, kafkaprotocol.ErrorCodeNone, int(partResp.ErrorCode))
			for _, rec := range partResp.Records {
				totSize += len(rec)
			}
		}
	}
	// Should be exactly equal as our max size is in multiples of batches
	require.Equal(t, totSize, maxBytes)
}

func TestFetcherSinglePartitionNoWaitAndMinBytesExactlyReached(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 0, 9999, 9999, 10, 2, topicProvider, controlClient, objStore)
	totBatchSizes := 0
	for _, batch := range batches {
		totBatchSizes += len(batch)
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
		totBatchSizes += len(batch)
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
		totSize += len(batch)
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
	// No batches should be fetched
	verifyDefaultResponse(t, resp, nil)
}

func TestFetcherMultipleRequestsFetchFromCacheAfterFirstRequest(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	topicProvider.infos[defaultTopicName] = topicmeta.TopicInfo{
		ID:             defaultTopicID,
		Name:           defaultTopicName,
		PartitionCount: defaultNumPartitions,
	}
	controlClient.setLastReadableOffset(defaultTopicID, defaultPartitionID, -1)

	// initialise partition tables
	resp := sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)

	verifyDefaultResponse(t, resp, nil)

	batches, tabIDs, queryRes := setupBatchesForPartition(t, defaultTopicID, defaultPartitionID, 0, 9999, 10, 10, objStore)
	controlClient.queryRes = queryRes
	controlClient.setLastReadableOffset(defaultTopicID, defaultPartitionID, 9999)

	totBatchSizes := 0
	for _, batch := range batches {
		totBatchSizes += len(batch)
	}

	var seq int64
	// send notifications - should result in entries added
	// we add the first two tables
	err := fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
		LeaderVersion: 1,
		TableIDs:      tabIDs[0:1],
		Sequence:      seq,
		Infos: []offsets.OffsetTopicInfo{
			{
				TopicID: defaultTopicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: defaultPartitionID,
						Offset:      999,
					},
				},
			},
		},
	})
	require.NoError(t, err)
	seq++
	err = fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
		LeaderVersion: 1,
		TableIDs:      tabIDs[1:2],
		Sequence:      seq,
		Infos: []offsets.OffsetTopicInfo{
			{
				TopicID: defaultTopicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: defaultPartitionID,
						Offset:      1999,
					},
				},
			},
		},
	})
	require.NoError(t, err)
	seq++

	// reset ids from testControllerClient so won't fetch from controller
	controlClient.queryRes = nil

	// should fetch from recent tables
	resp = sendFetchDefault(t, 1000, 0, 0, len(batches[1]), defaultMaxBytes, fetcher)
	verifyDefaultResponse(t, resp, batches[1:2])

	// Now add the rest to the cache
	tabsToAdd := tabIDs[2:]
	lros := []int64{2999, 3999, 4999, 5999, 6999, 7999, 8999, 9999}
	for i, tabID := range tabsToAdd {
		err = fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
			LeaderVersion: 1,
			TableIDs:      []sst.SSTableID{tabID},
			Sequence:      seq,
			Infos: []offsets.OffsetTopicInfo{
				{
					TopicID: defaultTopicID,
					PartitionInfos: []offsets.OffsetPartitionInfo{
						{
							PartitionID: defaultPartitionID,
							Offset:      lros[i],
						},
					},
				},
			},
		})
		require.NoError(t, err)
		seq++
	}
	// And get them one by one in different requests
	offset := 2000
	for i := 2; i < len(batches); i++ {
		resp = sendFetchDefault(t, offset, 0, 0, len(batches[i]), defaultMaxBytes, fetcher)
		verifyDefaultResponse(t, resp, batches[i:i+1])
		offset += 1000
	}

	// Try and get another - shouldn't get anything
	resp = sendFetchDefault(t, offset, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	verifyDefaultResponse(t, resp, nil)
}

func TestFetcherRequestNotEnoughBytesAndNotificationAddsSufficientData(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches, _ := setupDataDefault(t, 0, 999, 999, 1, 1, topicProvider, controlClient, objStore)

	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: 1000000,
		MinBytes:  int32(len(batches[0]) + 1), // just too much
		MaxBytes:  int32(defaultMaxBytes),
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(defaultTopicName),
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
	var completionCalled atomic.Bool
	resCh := make(chan *kafkaprotocol.FetchResponse, 1)
	err := fetcher.HandleFetchRequest(&req, func(resp *kafkaprotocol.FetchResponse) error {
		completionCalled.Store(true)
		resCh <- resp
		return nil
	})
	require.NoError(t, err)

	// Should wait, not complete yet
	time.Sleep(100 * time.Millisecond)
	require.False(t, completionCalled.Load())

	batches2, tabIDs2 := setupDataDefault(t, 1000, 1999, 1999, 1, 1, topicProvider, controlClient, objStore)

	// reset ids on controller client so no remote query can be served
	controlClient.queryRes = nil

	// send notification - should result in entry added to recent tables
	// we add the first two tables
	err = fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
		LeaderVersion: 1,
		TableIDs:      tabIDs2[0:1],
		Infos: []offsets.OffsetTopicInfo{
			{
				TopicID: defaultTopicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: defaultPartitionID,
						Offset:      1999,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Now response should complete, and both batches should be received
	resp := <-resCh

	totBatches := append(batches, batches2[0])
	verifyDefaultResponse(t, resp, totBatches)
}

func TestFetcherRequestNotEnoughBytesAndNotificationsDontAddSufficientData(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	batches1, _ := setupDataDefault(t, 0, 999, 999, 1, 1, topicProvider, controlClient, objStore)

	req := kafkaprotocol.FetchRequest{
		MaxWaitMs: 1000000,
		// MinBytes is such that won't reach minBytes until all batches are added to cache
		MinBytes: int32(3*len(batches1[0]) + 1),
		MaxBytes: int32(defaultMaxBytes),
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(defaultTopicName),
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

	var completionCalled atomic.Bool
	resCh := make(chan *kafkaprotocol.FetchResponse, 1)
	err := fetcher.HandleFetchRequest(&req, func(resp *kafkaprotocol.FetchResponse) error {
		completionCalled.Store(true)
		resCh <- resp
		return nil
	})
	require.NoError(t, err)

	// Should wait, not complete yet
	time.Sleep(100 * time.Millisecond)
	require.False(t, completionCalled.Load())

	batches2, tabIds2 := setupDataDefault(t, 1000, 1999, 1999, 1, 1, topicProvider, controlClient, objStore)
	batches3, tabIds3 := setupDataDefault(t, 2000, 2999, 2999, 1, 1, topicProvider, controlClient, objStore)
	batches4, tabIds4 := setupDataDefault(t, 3000, 3999, 3999, 1, 1, topicProvider, controlClient, objStore)

	// reset ids on controller client so no remote query can be served
	controlClient.queryRes = nil

	var seq int64

	// send notification - with second batch - not enough data so shouldn't complete yet
	err = fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
		LeaderVersion: 1,
		Sequence:      seq,
		TableIDs:      tabIds2[0:1],
		Infos: []offsets.OffsetTopicInfo{
			{
				TopicID: defaultTopicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: defaultPartitionID,
						Offset:      1999,
					},
				},
			},
		},
	})
	require.NoError(t, err)
	seq++

	// Should wait, not complete yet
	require.False(t, completionCalled.Load())

	// send notification - with third batch - not enough data so shouldn't complete yet
	err = fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
		LeaderVersion: 1,
		Sequence:      seq,
		TableIDs:      tabIds3[0:1],
		Infos: []offsets.OffsetTopicInfo{
			{
				TopicID: defaultTopicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: defaultPartitionID,
						Offset:      2999,
					},
				},
			},
		},
	})
	require.NoError(t, err)
	seq++

	// Should wait, not complete yet
	require.False(t, completionCalled.Load())

	// send notification - with fourth batch - should now complete
	err = fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
		LeaderVersion: 1,
		Sequence:      seq,
		TableIDs:      tabIds4[0:1],
		Infos: []offsets.OffsetTopicInfo{
			{
				TopicID: defaultTopicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: defaultPartitionID,
						Offset:      3999,
					},
				},
			},
		},
	})
	require.NoError(t, err)
	seq++

	// Now response should complete, and all batches should be received
	resp := <-resCh

	totBatches := append(batches1, append(batches2, append(batches3, batches4...)...)...)
	verifyDefaultResponse(t, resp, totBatches)
}

func TestFetcherHistoricConsumer(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)
	// Setup a bunch of batches
	batches, tabIDs := setupDataDefault(t, 0, 7999, 7999, 8, 8, topicProvider, controlClient, objStore)

	// Send a fetch past the last offset to initialise the partition tables
	sendFetchDefault(t, 8000, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)

	// Add two more tables
	batches2, tabIDs2, _ := setupBatchesForPartition(t, defaultTopicID, defaultPartitionID, 8000, 9999, 2, 2, objStore)
	batches = append(batches, batches2...)
	tabIDs = append(tabIDs, tabIDs2...)

	// Add just the last two to the cache - this simulates the case where we have older batches in storage but only
	// newer ones cached
	err := fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
		LeaderVersion: 1,
		Sequence:      0,
		TableIDs:      tabIDs[8:9],
		Infos: []offsets.OffsetTopicInfo{
			{
				TopicID: defaultTopicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: defaultPartitionID,
						Offset:      8999,
					},
				},
			},
		},
	})
	require.NoError(t, err)
	err = fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
		LeaderVersion: 1,
		Sequence:      1,
		TableIDs:      tabIDs[9:10],
		Infos: []offsets.OffsetTopicInfo{
			{
				TopicID: defaultTopicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: defaultPartitionID,
						Offset:      9999,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Now fetch in batches of 10 - first 9 requests should go to the controller, last one should be served from the cache
	offset := 0
	for i := 0; i < 10; i++ {
		if i == 9 {
			// last one must be served from cache so we set ids to nil to prevent any request to controller succeeding
			controlClient.queryRes = nil
		}
		resp := sendFetchDefault(t, offset, 0, 0, len(batches[i]), defaultMaxBytes, fetcher)
		verifyDefaultResponse(t, resp, batches[i:i+1])
		offset += 1000
	}
}

func TestFetcherExceedMaxCachedTables(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	// no data yet
	topicProvider.infos[defaultTopicName] = topicmeta.TopicInfo{
		ID:             defaultTopicID,
		Name:           defaultTopicName,
		PartitionCount: defaultNumPartitions,
	}
	controlClient.setLastReadableOffset(defaultTopicID, defaultPartitionID, -1)

	// Send a fetch past the last offset to initialise the partition tables
	sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)

	// Setup a bunch of batches
	numBatches := DefaultMaxCachedTablesPerPartition * 2
	lastOff := numBatches*1000 - 1
	batches, tabIDs := setupDataDefault(t, 0, lastOff, lastOff, numBatches, numBatches, topicProvider, controlClient, objStore)

	// send notifications
	var seq int64
	for i := 0; i < numBatches; i++ {
		err := fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
			LeaderVersion: 1,
			Sequence:      seq,
			TableIDs:      tabIDs[i : i+1],
			Infos: []offsets.OffsetTopicInfo{
				{
					TopicID: defaultTopicID,
					PartitionInfos: []offsets.OffsetPartitionInfo{
						{
							PartitionID: defaultPartitionID,
							Offset:      int64((i+1)*1000 - 1),
						},
					},
				},
			},
		})
		require.NoError(t, err)
		seq++
	}

	// Should cause max cached entries to be exceeded and only half the table ids will be cached

	offset := 0
	for i := 0; i < numBatches; i++ {
		if i >= numBatches/2 {
			// second half will be served from cache so we set ids to nil to prevent any request to controller succeeding
			controlClient.queryRes = nil
		}
		resp := sendFetchDefault(t, offset, 0, 0, len(batches[i]), defaultMaxBytes, fetcher)
		verifyDefaultResponse(t, resp, batches[i:i+1])
		offset += 1000
	}
}

func TestFetcherResetSequence(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	// no data yet
	topicProvider.infos[defaultTopicName] = topicmeta.TopicInfo{
		ID:             defaultTopicID,
		Name:           defaultTopicName,
		PartitionCount: defaultNumPartitions,
	}
	controlClient.setLastReadableOffset(defaultTopicID, defaultPartitionID, -1)

	// Send a fetch past the last offset to initialise the partition tables
	sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)

	// Setup a bunch of batches
	numBatches := 5
	lastOff := numBatches*1000 - 1
	batches, tabIDs := setupDataDefault(t, 0, lastOff, lastOff, numBatches, numBatches, topicProvider, controlClient, objStore)

	// send initial notifications
	var seq int64
	for i := 0; i < numBatches; i++ {
		err := fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
			LeaderVersion: 1,
			Sequence:      seq,
			TableIDs:      tabIDs[i : i+1],
			Infos: []offsets.OffsetTopicInfo{
				{
					TopicID: defaultTopicID,
					PartitionInfos: []offsets.OffsetPartitionInfo{
						{
							PartitionID: defaultPartitionID,
							Offset:      int64((i+1)*1000 - 1),
						},
					},
				},
			},
		})
		require.NoError(t, err)
		seq++
	}

	// fetch data - so far so good
	offset := 0
	for i := 0; i < numBatches; i++ {
		resp := sendFetchDefault(t, offset, 0, 0, len(batches[i]), defaultMaxBytes, fetcher)
		verifyDefaultResponse(t, resp, batches[i:i+1])
		offset += 1000
	}
	require.Equal(t, numBatches-1, fetcher.recentTables.getLastReceivedSequence())
	require.Equal(t, 0, int(atomic.LoadInt64(&fetcher.resetSequence)))
	memberID, resetSequence := controlClient.getMemberIDAndResetSequence()
	require.Equal(t, fetcher.memberID, memberID)
	require.Equal(t, 0, int(resetSequence))

	// now send notification out of sequence - should cause partition states to be invalidated
	seq++
	err := fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
		LeaderVersion: 1,
		Sequence:      seq,
		TableIDs:      []sst.SSTableID{tabIDs[numBatches-1]},
		Infos: []offsets.OffsetTopicInfo{
			{
				TopicID: defaultTopicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: defaultPartitionID,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	partitionMap := fetcher.recentTables.getPartitionMap(defaultTopicID)
	partitionTables := fetcher.recentTables.getPartitionTables(partitionMap, defaultPartitionID)
	require.False(t, partitionTables.isInitialised())

	require.Equal(t, -1, fetcher.recentTables.getLastReceivedSequence())
	require.Equal(t, 1, int(atomic.LoadInt64(&fetcher.resetSequence)))

	// Register again - this should cause controller to be called again
	sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)

	memberID, resetSequence = controlClient.getMemberIDAndResetSequence()
	require.Equal(t, fetcher.memberID, memberID)
	require.Equal(t, 1, int(resetSequence))

	// setup more batches
	batches, tabIDs = setupDataDefault(t, 5000, 5000+lastOff, 5000+lastOff, numBatches, numBatches, topicProvider, controlClient, objStore)

	// prevent going to controller
	controlClient.clearQueryRes()

	// The controller should now reset it's sequence to zero, so we should be able to receive further notifications
	seq = 0

	for i := 0; i < numBatches; i++ {
		err := fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
			LeaderVersion: 1,
			Sequence:      seq,
			TableIDs:      tabIDs[i : i+1],
			Infos: []offsets.OffsetTopicInfo{
				{
					TopicID: defaultTopicID,
					PartitionInfos: []offsets.OffsetPartitionInfo{
						{
							PartitionID: defaultPartitionID,
							Offset:      5000 + int64((i+1)*1000-1),
						},
					},
				},
			},
		})
		require.NoError(t, err)
		seq++
	}

	for i := 0; i < numBatches; i++ {
		resp := sendFetchDefault(t, offset, 0, 0, len(batches[i]), defaultMaxBytes, fetcher)
		verifyDefaultResponse(t, resp, batches[i:i+1])
		offset += 1000
	}
}

func TestFetcherInvalidateOnLeaderChange(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	memberID := int32(23)
	fetcher.memberID = memberID

	// no data yet
	topicProvider.infos[defaultTopicName] = topicmeta.TopicInfo{
		ID:             defaultTopicID,
		Name:           defaultTopicName,
		PartitionCount: defaultNumPartitions,
	}
	controlClient.setLastReadableOffset(defaultTopicID, defaultPartitionID, -1)

	// Send a fetch past the last offset to initialise the partition tables
	sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)

	numBatches := 5
	lastOff := numBatches*1000 - 1
	batches, tabIDs := setupDataDefault(t, 0, lastOff, lastOff, numBatches, numBatches, topicProvider, controlClient, objStore)

	// send initial notifications
	var seq int64
	for i := 0; i < numBatches; i++ {
		err := fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
			LeaderVersion: 1,
			Sequence:      seq,
			TableIDs:      tabIDs[i : i+1],
			Infos: []offsets.OffsetTopicInfo{
				{
					TopicID: defaultTopicID,
					PartitionInfos: []offsets.OffsetPartitionInfo{
						{
							PartitionID: defaultPartitionID,
							Offset:      int64((i+1)*1000 - 1),
						},
					},
				},
			},
		})
		require.NoError(t, err)
		seq++
	}

	// fetch data - so far so good
	offset := 0
	for i := 0; i < numBatches; i++ {
		resp := sendFetchDefault(t, offset, 0, 0, len(batches[i]), defaultMaxBytes, fetcher)
		verifyDefaultResponse(t, resp, batches[i:i+1])
		offset += 1000
	}
	require.Equal(t, numBatches-1, fetcher.recentTables.getLastReceivedSequence())
	require.Equal(t, 0, int(atomic.LoadInt64(&fetcher.resetSequence)))
	memberID, resetSequence := controlClient.getMemberIDAndResetSequence()
	require.Equal(t, fetcher.memberID, memberID)
	require.Equal(t, 0, int(resetSequence))

	// now bump leader version
	err := fetcher.MembershipChanged(0, cluster.MembershipState{
		LeaderVersion: 2,
	})
	require.NoError(t, err)

	// The controller should now reset it's sequence to zero, so we should be able to receive further notifications
	seq = 0

	// register again
	sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)

	// setup more batches
	batches, tabIDs = setupDataDefault(t, 5000, 5000+lastOff, 5000+lastOff, numBatches, numBatches, topicProvider, controlClient, objStore)

	for i := 0; i < numBatches; i++ {
		err := fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
			LeaderVersion: 2,
			Sequence:      seq,
			TableIDs:      tabIDs[i : i+1],
			Infos: []offsets.OffsetTopicInfo{
				{
					TopicID: defaultTopicID,
					PartitionInfos: []offsets.OffsetPartitionInfo{
						{
							PartitionID: defaultPartitionID,
							Offset:      5000 + int64((i+1)*1000-1),
						},
					},
				},
			},
		})
		require.NoError(t, err)
		seq++
	}

	// prevent going to controller
	controlClient.clearQueryRes()

	for i := 0; i < numBatches; i++ {
		resp := sendFetchDefault(t, offset, 0, 0, len(batches[i]), defaultMaxBytes, fetcher)
		verifyDefaultResponse(t, resp, batches[i:i+1])
		offset += 1000
	}
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

func TestFetcherNotificationWithInvalidLeaderVersion(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	topicProvider.infos[defaultTopicName] = topicmeta.TopicInfo{
		ID:             defaultTopicID,
		Name:           defaultTopicName,
		PartitionCount: defaultNumPartitions,
	}
	controlClient.setLastReadableOffset(defaultTopicID, defaultPartitionID, -1)

	// initialise partition tables
	resp := sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)

	verifyDefaultResponse(t, resp, nil)

	// Add two tables
	batches, tabIDs, queryRes := setupBatchesForPartition(t, defaultTopicID, defaultPartitionID, 0, 1999, 2, 2, objStore)
	controlClient.queryRes = queryRes
	controlClient.setLastReadableOffset(defaultTopicID, defaultPartitionID, 1999)

	var seq int64
	// send notification for first table
	err := fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
		LeaderVersion: 1,
		TableIDs:      tabIDs[0:1],
		Sequence:      seq,
		Infos: []offsets.OffsetTopicInfo{
			{
				TopicID: defaultTopicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: defaultPartitionID,
						Offset:      999,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Next notification should be ignored as wrong leader version
	seq++
	err = fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
		LeaderVersion: 0,
		TableIDs:      tabIDs[1:2],
		Sequence:      seq,
		Infos: []offsets.OffsetTopicInfo{
			{
				TopicID: defaultTopicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: defaultPartitionID,
						Offset:      1999,
					},
				},
			},
		},
	})
	require.NoError(t, err)
	seq++

	// reset ids from testControllerClient so won't fetch from controller
	controlClient.queryRes = nil

	// Should only get first batch
	resp = sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	verifyDefaultResponse(t, resp, batches[0:1])
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

func TestFetcherTableNotificationWithMultipleTables(t *testing.T) {
	fetcher, topicProvider, controlClient, objStore := setupFetcher(t)
	defer stopFetcher(t, fetcher)

	topicProvider.infos[defaultTopicName] = topicmeta.TopicInfo{
		ID:             defaultTopicID,
		Name:           defaultTopicName,
		PartitionCount: defaultNumPartitions,
	}
	controlClient.setLastReadableOffset(defaultTopicID, defaultPartitionID, -1)

	// Initialise
	resp := sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	verifyDefaultResponse(t, resp, nil)

	batches, tabIds := setupDataDefault(t, 0, 9999, 9999, 10, 10, topicProvider, controlClient, objStore)

	// send a notification with all the tables
	err := fetcher.recentTables.handleTableRegisteredNotification(&control.TablesRegisteredNotification{
		LeaderVersion: 1,
		TableIDs:      tabIds,
		Infos: []offsets.OffsetTopicInfo{
			{
				TopicID: defaultTopicID,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: defaultPartitionID,
						Offset:      9999,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// should get everything
	resp = sendFetchDefault(t, 0, 0, 0, defaultMaxBytes, defaultMaxBytes, fetcher)
	verifyDefaultResponse(t, resp, batches)
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
	ch := make(chan *kafkaprotocol.FetchResponse, 1)
	err := fetcher.HandleFetchRequest(req, func(resp *kafkaprotocol.FetchResponse) error {
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
	require.Equal(t, len(batches), len(partitionResp.Records))
	require.Equal(t, batches, partitionResp.Records)
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
	require.Equal(t, len(batches), len(partResp.Records))
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

func (t *testControlClient) QueryTablesInRange(_ []byte, _ []byte) (lsm.OverlappingTables, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.unavailable {
		return nil, common.NewTektiteErrorf(common.Unavailable, "controller is unavailable")
	}
	if t.unexpectedErr {
		return nil, errors.New("unexpected error")
	}
	return t.queryRes, nil
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

func (t *testControlClient) GetAllTopicInfos() ([]topicmeta.TopicInfo, error) {
	panic("should not be called")
}

func (t *testControlClient) CreateTopic(topicInfo topicmeta.TopicInfo) error {
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
