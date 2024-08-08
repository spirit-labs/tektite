package kafkaserver

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/spirit-labs/tektite/asl/conf"
	encoding2 "github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/tppm"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestFetchOneBatchFromCache(t *testing.T) {

	procMgr := tppm.NewTestProcessorManager()

	fetcher := newFetcher(procMgr, &testStreamMgr{}, conf.DefaultKafkaFetchCacheMaxSizeBytes)
	topicInfo := newTopicInfo("topic1", 10, 1000)
	startProcessors(topicInfo, procMgr)

	batch1 := createBatch(10)
	partitionFetcher, err := fetcher.GetPartitionFetcher(topicInfo, 0)
	require.NoError(t, err)
	partitionFetcher.AddBatch(0, 99, batch1)

	res := execFetch(topicInfo, 0, 0, 1000,
		1, 1000, fetcher)

	require.NoError(t, res.err)
	require.Equal(t, 1, len(res.batches))
	require.Equal(t, batch1, res.batches[0])

	// offset overlapping with batch
	res = execFetch(topicInfo, 0, 50, 1000,
		1, 1000, fetcher)

	require.NoError(t, res.err)
	require.Equal(t, 1, len(res.batches))
	require.Equal(t, batch1, res.batches[0])
}

func TestFetchFromCacheMultipleBatchesFromOffset(t *testing.T) {
	procMgr := tppm.NewTestProcessorManager()

	fetcher := newFetcher(procMgr, &testStreamMgr{}, conf.DefaultKafkaFetchCacheMaxSizeBytes)
	topicInfo := newTopicInfo("topic1", 10, 1000)
	startProcessors(topicInfo, procMgr)

	partitionFetcher, err := fetcher.GetPartitionFetcher(topicInfo, 0)
	require.NoError(t, err)

	batch1 := createBatch(10)
	partitionFetcher.AddBatch(50, 99, batch1)
	batch2 := createBatch(15)
	partitionFetcher.AddBatch(100, 199, batch2)
	batch3 := createBatch(20)
	partitionFetcher.AddBatch(200, 299, batch3)
	batch4 := createBatch(25)
	partitionFetcher.AddBatch(300, 399, batch4)

	res := execFetch(topicInfo, 0, 400, 0,
		1, 1000, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 0, len(res.batches))

	res = execFetch(topicInfo, 0, 250, 0,
		1, 1000, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 2, len(res.batches))
	require.Equal(t, batch3, res.batches[0])
	require.Equal(t, batch4, res.batches[1])

	res = execFetch(topicInfo, 0, 200, 0,
		1, 1000, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 2, len(res.batches))
	require.Equal(t, batch3, res.batches[0])
	require.Equal(t, batch4, res.batches[1])

	res = execFetch(topicInfo, 0, 150, 0,
		1, 1000, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 3, len(res.batches))
	require.Equal(t, batch2, res.batches[0])
	require.Equal(t, batch3, res.batches[1])
	require.Equal(t, batch4, res.batches[2])

	res = execFetch(topicInfo, 0, 100, 0,
		1, 1000, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 3, len(res.batches))
	require.Equal(t, batch2, res.batches[0])
	require.Equal(t, batch3, res.batches[1])
	require.Equal(t, batch4, res.batches[2])

	res = execFetch(topicInfo, 0, 50, 0,
		1, 1000, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 4, len(res.batches))
	require.Equal(t, batch1, res.batches[0])
	require.Equal(t, batch2, res.batches[1])
	require.Equal(t, batch3, res.batches[2])
	require.Equal(t, batch4, res.batches[3])
}

func TestFetchBatchAddedWhileWaiting(t *testing.T) {
	procMgr := tppm.NewTestProcessorManager()

	fetcher := newFetcher(procMgr, &testStreamMgr{}, conf.DefaultKafkaFetchCacheMaxSizeBytes)
	topicInfo := newTopicInfo("topic1", 10, 1000)
	startProcessors(topicInfo, procMgr)

	partitionFetcher, err := fetcher.GetPartitionFetcher(topicInfo, 0)
	require.NoError(t, err)
	batch1 := createBatch(10)
	partitionFetcher.AddBatch(0, 99, batch1)
	batch2 := createBatch(10)

	ch := make(chan fetchResult, 1)
	w := partitionFetcher.Fetch(100, 1,
		1000, 5*time.Second, func(batches [][]byte, hwm int64, err error) {
			ch <- fetchResult{batches, hwm, err}
		})
	require.NotNil(t, w)
	w.schedule()

	partitionFetcher.AddBatch(100, 199, batch2)

	res := <-ch
	require.NoError(t, res.err)
	require.Equal(t, 1, len(res.batches))
	require.Equal(t, batch2, res.batches[0])
}

func TestFetchWaitForBatchTimeout(t *testing.T) {
	procMgr := tppm.NewTestProcessorManager()

	fetcher := newFetcher(procMgr, &testStreamMgr{}, conf.DefaultKafkaFetchCacheMaxSizeBytes)
	topicInfo := newTopicInfo("topic1", 10, 1000)
	startProcessors(topicInfo, procMgr)

	partitionFetcher, err := fetcher.GetPartitionFetcher(topicInfo, 0)
	require.NoError(t, err)
	batch1 := createBatch(10)
	partitionFetcher.AddBatch(0, 99, batch1)

	maxWait := 250 * time.Millisecond
	start := time.Now()

	ch := make(chan fetchResult, 1)
	w := partitionFetcher.Fetch(100, 1,
		1000, maxWait, func(batches [][]byte, hwm int64, err error) {
			ch <- fetchResult{batches, hwm, err}
		})
	require.NotNil(t, w)
	w.schedule()
	res := <-ch

	require.NoError(t, res.err)
	require.Equal(t, 0, len(res.batches))
	dur := time.Now().Sub(start)
	require.GreaterOrEqual(t, dur, maxWait)
}

func TestFetchMinBytes(t *testing.T) {
	procMgr := tppm.NewTestProcessorManager()

	fetcher := newFetcher(procMgr, &testStreamMgr{}, conf.DefaultKafkaFetchCacheMaxSizeBytes)
	topicInfo := newTopicInfo("topic1", 10, 1000)
	startProcessors(topicInfo, procMgr)

	partitionFetcher, err := fetcher.GetPartitionFetcher(topicInfo, 0)
	require.NoError(t, err)

	batch1 := createBatch(10)
	partitionFetcher.AddBatch(0, 99, batch1)
	batch2 := createBatch(15)
	partitionFetcher.AddBatch(100, 199, batch2)
	batch3 := createBatch(20)
	partitionFetcher.AddBatch(200, 299, batch3)

	res := execFetch(topicInfo, 0, 0, 0,
		46, 1000, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 0, len(res.batches))

	batch4 := createBatch(10)
	partitionFetcher.AddBatch(300, 399, batch4)

	res = execFetch(topicInfo, 0, 0, 0,
		31, 1000, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 4, len(res.batches))
	require.Equal(t, batch1, res.batches[0])
	require.Equal(t, batch2, res.batches[1])
	require.Equal(t, batch3, res.batches[2])
	require.Equal(t, batch4, res.batches[3])
}

func TestFetchMinBytesWhileWaitingTimeout(t *testing.T) {
	procMgr := tppm.NewTestProcessorManager()

	fetcher := newFetcher(procMgr, &testStreamMgr{}, conf.DefaultKafkaFetchCacheMaxSizeBytes)
	topicInfo := newTopicInfo("topic1", 10, 1000)
	startProcessors(topicInfo, procMgr)

	partitionFetcher, err := fetcher.GetPartitionFetcher(topicInfo, 0)
	require.NoError(t, err)

	batch1 := createBatch(10)
	batch2 := createBatch(15)
	batch3 := createBatch(20)

	partitionFetcher.AddBatch(0, 99, batch1)
	partitionFetcher.AddBatch(100, 199, batch2)
	partitionFetcher.AddBatch(200, 299, batch3)

	// insufficient bytes so should timeout
	start := time.Now()
	maxWait := 250 * time.Millisecond
	ch := make(chan fetchResult, 1)
	w := partitionFetcher.Fetch(0, 500,
		1000, maxWait, func(batches [][]byte, hwm int64, err error) {
			ch <- fetchResult{batches, hwm, err}
		})
	require.NotNil(t, w)
	w.schedule()

	res := <-ch
	require.NoError(t, res.err)
	require.Equal(t, 3, len(res.batches))
	require.Equal(t, batch1, res.batches[0])
	require.Equal(t, batch2, res.batches[1])
	require.Equal(t, batch3, res.batches[2])
	dur := time.Now().Sub(start)
	require.GreaterOrEqual(t, dur, maxWait)
}

func TestFetchMinBytesWhileWaitingNoTimeout(t *testing.T) {
	procMgr := tppm.NewTestProcessorManager()

	fetcher := newFetcher(procMgr, &testStreamMgr{}, conf.DefaultKafkaFetchCacheMaxSizeBytes)
	topicInfo := newTopicInfo("topic1", 10, 1000)
	startProcessors(topicInfo, procMgr)

	partitionFetcher, err := fetcher.GetPartitionFetcher(topicInfo, 0)
	require.NoError(t, err)

	batch1 := createBatch(10)
	batch2 := createBatch(15)
	batch3 := createBatch(20)

	partitionFetcher.AddBatch(0, 99, batch1)

	start := time.Now()
	maxWait := 250 * time.Millisecond

	ch := make(chan fetchResult, 1)
	w := partitionFetcher.Fetch(0, 45,
		1000, maxWait, func(batches [][]byte, hwm int64, err error) {
			ch <- fetchResult{batches, hwm, err}
		})
	require.NotNil(t, w)
	w.schedule()

	partitionFetcher.AddBatch(100, 199, batch2)
	partitionFetcher.AddBatch(200, 299, batch3)

	res := <-ch

	require.NoError(t, res.err)
	require.Equal(t, 3, len(res.batches))
	require.Equal(t, batch1, res.batches[0])
	require.Equal(t, batch2, res.batches[1])
	require.Equal(t, batch3, res.batches[2])
	dur := time.Now().Sub(start)
	require.Less(t, dur, maxWait)
}

func TestFetchFromCacheMaxBytes(t *testing.T) {
	procMgr := tppm.NewTestProcessorManager()

	fetcher := newFetcher(procMgr, &testStreamMgr{}, conf.DefaultKafkaFetchCacheMaxSizeBytes)
	topicInfo := newTopicInfo("topic1", 10, 1000)
	startProcessors(topicInfo, procMgr)

	partitionFetcher, err := fetcher.GetPartitionFetcher(topicInfo, 0)
	require.NoError(t, err)

	batch1 := createBatch(10)
	partitionFetcher.AddBatch(0, 99, batch1)
	batch2 := createBatch(15)
	partitionFetcher.AddBatch(100, 199, batch2)
	batch3 := createBatch(20)
	partitionFetcher.AddBatch(200, 299, batch3)
	batch4 := createBatch(15)
	partitionFetcher.AddBatch(300, 399, batch4)

	// First batch always returned even if greater than max bytes
	res := execFetch(topicInfo, 0, 0, 0,
		1, 5, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 1, len(res.batches))
	require.Equal(t, batch1, res.batches[0])

	res = execFetch(topicInfo, 0, 0, 0,
		1, 10, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 1, len(res.batches))
	require.Equal(t, batch1, res.batches[0])

	res = execFetch(topicInfo, 0, 0, 0,
		1, 25, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 2, len(res.batches))
	require.Equal(t, batch1, res.batches[0])
	require.Equal(t, batch2, res.batches[1])

	res = execFetch(topicInfo, 0, 0, 0,
		1, 50, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 3, len(res.batches))
	require.Equal(t, batch1, res.batches[0])
	require.Equal(t, batch2, res.batches[1])
	require.Equal(t, batch3, res.batches[2])

	res = execFetch(topicInfo, 0, 105, 0,
		1, 37, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 2, len(res.batches))
	require.Equal(t, batch2, res.batches[0])
	require.Equal(t, batch3, res.batches[1])
}

func TestFetchFromStore(t *testing.T) {
	procMgr := tppm.NewTestProcessorManager()

	slabID := 1000
	fetcher := newFetcher(procMgr, &testStreamMgr{}, conf.DefaultKafkaFetchCacheMaxSizeBytes)
	topicInfo := newTopicInfo("topic1", 10, slabID)
	startProcessors(topicInfo, procMgr)

	partitionFetcher, err := fetcher.GetPartitionFetcher(topicInfo, 0)
	require.NoError(t, err)

	numRows := 100

	insertRowsInStore(t, procMgr.GetStore(), topicInfo.ConsumerInfoProvider.PartitionScheme().MappingID, slabID, 0, numRows, 0, 0)

	batch1 := createBatch(10)
	partitionFetcher.AddBatch(1000, 1099, batch1)

	res := execFetch(topicInfo, 0, 0, 0,
		1, 1000000, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 1, len(res.batches))

	kvs, _, _ := decodeBatch(res.batches[0])
	require.Equal(t, numRows, len(kvs))
	for i, kv := range kvs {
		require.Equal(t, fmt.Sprintf("key-%05d", i), string(kv.Key))
		require.Equal(t, fmt.Sprintf("val-%05d", i), string(kv.Value))
	}
}

func TestFetchFromStoreMaxBytes(t *testing.T) {
	procMgr := tppm.NewTestProcessorManager()

	slabID := 1000
	fetcher := newFetcher(procMgr, &testStreamMgr{}, conf.DefaultKafkaFetchCacheMaxSizeBytes)
	topicInfo := newTopicInfo("topic1", 10, slabID)
	startProcessors(topicInfo, procMgr)

	partitionFetcher, err := fetcher.GetPartitionFetcher(topicInfo, 0)
	require.NoError(t, err)

	numRows := 100

	insertRowsInStore(t, procMgr.GetStore(), topicInfo.ConsumerInfoProvider.PartitionScheme().MappingID, slabID, 0, numRows, 0, 0)

	batch1 := createBatch(10)
	partitionFetcher.AddBatch(1000, 1099, batch1)

	maxBytes := 1000
	res := execFetch(topicInfo, 0, 0, 0,
		1, maxBytes, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 1, len(res.batches))
	require.LessOrEqual(t, len(res.batches[0]), maxBytes)
}

func TestFetchFromStoreOneRecordReturnedEvenIfExceedsMaxBytes(t *testing.T) {
	procMgr := tppm.NewTestProcessorManager()

	slabID := 1000
	fetcher := newFetcher(procMgr, &testStreamMgr{}, conf.DefaultKafkaFetchCacheMaxSizeBytes)
	topicInfo := newTopicInfo("topic1", 10, slabID)
	startProcessors(topicInfo, procMgr)

	partitionFetcher, err := fetcher.GetPartitionFetcher(topicInfo, 0)
	require.NoError(t, err)

	insertRowsInStore(t, procMgr.GetStore(), topicInfo.ConsumerInfoProvider.PartitionScheme().MappingID, slabID, 0, 10, 0, 2000)

	batch1 := createBatch(10)
	partitionFetcher.AddBatch(1000, 1099, batch1)

	maxBytes := 1000
	res := execFetch(topicInfo, 0, 0, 0,
		1, maxBytes, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 1, len(res.batches))
	require.Greater(t, len(res.batches[0]), maxBytes)

	kvs, _, _ := decodeBatch(res.batches[0])
	require.Equal(t, 1, len(kvs))
	require.Equal(t, "key-00000", string(kvs[0].Key))
	require.True(t, strings.HasPrefix(string(kvs[0].Value), "val-00000"))
}

func TestFetchEvictBatches(t *testing.T) {
	procMgr := tppm.NewTestProcessorManager()

	slabID := 1000
	fetcher := newFetcher(procMgr, &testStreamMgr{}, conf.DefaultKafkaFetchCacheMaxSizeBytes)
	topicInfo := newTopicInfo("topic1", 10, slabID)
	startProcessors(topicInfo, procMgr)

	partitionFetcher, err := fetcher.GetPartitionFetcher(topicInfo, 0)
	require.NoError(t, err)

	batch1 := createBatch(10)
	partitionFetcher.AddBatch(0, 99, batch1)
	batch2 := createBatch(15)
	partitionFetcher.AddBatch(100, 199, batch2)
	batch3 := createBatch(20)
	partitionFetcher.AddBatch(200, 299, batch3)
	batch4 := createBatch(15)
	partitionFetcher.AddBatch(300, 399, batch4)

	res := execFetch(topicInfo, 0, 0, 0, 1, 5, fetcher)
	require.NoError(t, res.err)
	require.Equal(t, 1, len(res.batches))
	require.Equal(t, batch1, res.batches[0])

	partitionFetcher.evictEntry()
	partitionFetcher.evictEntry()

	require.Equal(t, 4, len(partitionFetcher.cachedBatches()))

	// Add another batch, will cause first two to be evicted
	batch5 := createBatch(15)
	partitionFetcher.AddBatch(400, 499, batch5)

	cached := partitionFetcher.cachedBatches()
	require.Equal(t, 3, len(cached))
	require.Equal(t, batch3, cached[0].recordBatch)
	require.Equal(t, batch4, cached[1].recordBatch)
	require.Equal(t, batch5, cached[2].recordBatch)

	partitionFetcher.evictEntry()

	// Call to Fetch should also cause an eviction
	res = execFetch(topicInfo, 0, 0, 0, 1, 5, fetcher)
	require.NoError(t, res.err)

	cached = partitionFetcher.cachedBatches()
	require.Equal(t, 2, len(cached))
	require.Equal(t, batch4, cached[0].recordBatch)
	require.Equal(t, batch5, cached[1].recordBatch)

	partitionFetcher.evictEntry()
	// And also call to checkEvictBatches()
	partitionFetcher.checkEvictBatches()

	cached = partitionFetcher.cachedBatches()
	require.Equal(t, 1, len(cached))
	require.Equal(t, batch5, cached[0].recordBatch)
}

func insertRowsInStore(t *testing.T, st tppm.Store, mappingID string, slabID int, partitionID int, numRows int, startOffset int,
	paddingBytes int) {
	offset := startOffset
	mb := mem.NewBatch()
	for i := 0; i < numRows; i++ {
		partitionHash := proc.CalcPartitionHash(mappingID, uint64(partitionID))
		key := encoding2.EncodeEntryPrefix(partitionHash, uint64(slabID), 41)
		key = append(key, 1) // not null
		key = encoding2.KeyEncodeInt(key, int64(offset))
		key = encoding2.EncodeVersion(key, 0)
		messageKey := fmt.Sprintf("key-%05d", i)
		var messageValue string
		if paddingBytes == 0 {
			messageValue = fmt.Sprintf("val-%05d", i)
		} else {
			padding := make([]byte, paddingBytes)
			for i := 0; i < paddingBytes; i++ {
				padding[i] = 'a'
			}
			messageValue = fmt.Sprintf("val-%05d-%s", i, string(padding))
		}
		var val []byte
		val = append(val, 1)
		val = encoding2.AppendUint64ToBufferLE(val, uint64(i)) // event_time
		val = append(val, 1)
		val = encoding2.AppendBytesToBufferLE(val, []byte(messageKey))
		val = append(val, 1)
		val = encoding2.AppendBytesToBufferLE(val, []byte{}) // headers
		val = append(val, 1)
		val = encoding2.AppendBytesToBufferLE(val, []byte(messageValue))

		mb.AddEntry(common.KV{
			Key:   key,
			Value: val,
		})
		offset++
	}
	err := st.Write(mb)
	require.NoError(t, err)
}

func createBatch(size int) []byte {
	batch := make([]byte, size)
	_, err := rand.Read(batch)
	if err != nil {
		panic(err)
	}
	return batch
}

type fetchResult struct {
	batches [][]byte
	hwm     int64
	err     error
}

func execFetch(topicInfo *TopicInfo, partitionID int32, fetchOffset int64, maxWait time.Duration, minBytes int,
	maxBytes int, fetcher *fetcher) fetchResult {
	ch := make(chan fetchResult, 1)
	partitionFetcher, err := fetcher.GetPartitionFetcher(topicInfo, partitionID)
	if err != nil {
		panic(err)
	}
	partitionFetcher.Fetch(fetchOffset, minBytes,
		maxBytes, maxWait, func(batches [][]byte, hwm int64, err error) {
			ch <- fetchResult{batches, hwm, err}
		})
	return <-ch
}

func newTopicInfo(topicName string, partitions int, slabID int) *TopicInfo {
	var pis []PartitionInfo
	for i := 0; i < partitions; i++ {
		pis = append(pis, PartitionInfo{
			ID: i,
		})
	}
	ps := opers.NewPartitionScheme("_default_", partitions, true, conf.DefaultProcessorCount)
	return &TopicInfo{
		Name:                 topicName,
		ConsumeEnabled:       true,
		ConsumerInfoProvider: &testConsumerInfoProvider{slabID: slabID, partitionScheme: &ps},
		Partitions:           pis,
		CanCache:             true,
	}
}

func decodeBatch(bytes []byte) (kvs []common.KV, baseOffset int64, baseTimeStamp int64) {
	baseOffset = int64(binary.BigEndian.Uint64(bytes))
	baseTimeStamp = int64(binary.BigEndian.Uint64(bytes[27:]))
	off := 57
	numRecords := int(binary.BigEndian.Uint32(bytes[off:]))
	off += 4
	for i := 0; i < numRecords; i++ {
		recordLength, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		recordStart := off
		off++ // skip past attributes
		_, bytesRead = binary.Varint(bytes[off:])
		off += bytesRead
		_, bytesRead = binary.Varint(bytes[off:])
		off += bytesRead
		keyLength, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		ikl := int(keyLength)
		key := bytes[off : off+ikl]
		off += ikl
		valueLength, bytesRead := binary.Varint(bytes[off:])
		off += bytesRead
		ivl := int(valueLength)
		value := bytes[off : off+ivl]
		kvs = append(kvs, common.KV{
			Key:   key,
			Value: value,
		})
		off = recordStart + int(recordLength)
	}
	return kvs, baseOffset, baseTimeStamp
}

type testStreamMgr struct {
}

func (t testStreamMgr) RegisterSystemSlab(slabName string, persistorReceiverID int, deleterReceiverID int, slabID int, schema *opers.OperatorSchema, keyCols []string) error {
	return nil
}

func (t testStreamMgr) RegisterChangeListener(listener func(streamName string, deployed bool)) {
}

func startProcessors(topicInfo *TopicInfo, procMgr *tppm.TestProcessorManager) {
	for i := 0; i < topicInfo.ConsumerInfoProvider.PartitionScheme().Partitions; i++ {
		procID := topicInfo.ConsumerInfoProvider.PartitionScheme().PartitionProcessorMapping[i]
		procMgr.AddActiveProcessor(procID)
	}
}
