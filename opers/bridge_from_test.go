package opers

import (
	"fmt"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/kafka"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/tppm"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
)

func TestBridgeFromIngestAndConvertToEventBatch(t *testing.T) {

	receiverID := 23
	topicName := "test_topic"

	kafkaProps := map[string]string{}

	numMessages := 10
	var msgs []*kafka.Message
	for i := 0; i < numMessages; i++ {
		msg := createKafkaMessage(0, i, fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i), int64(1000+i))
		msg.PartInfo.Offset = int64(i)
		msgs = append(msgs, msg)
	}

	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	var ingestedMessageCount uint64

	procMgr := tppm.NewTestProcessorManager()
	ingestEnabled := atomic.Bool{}
	ingestEnabled.Store(true)
	var lastFlushedVersion int64 = -1
	oper, err := NewBridgeFromOperator(receiverID, defaultPollTimeout, defaultMaxMessages, topicName,
		10, kafkaProps, msgClientFact{}.createTestMessageClient, false,
		procMgr, 1001, cfg, &ingestedMessageCount, "fk", &ingestEnabled, &lastFlushedVersion)
	require.NoError(t, err)

	processor := &capturingProcessor{}
	err = oper.IngestMessages(processor, msgs)
	require.NoError(t, err)

	batch := processor.processBatch.EvBatch

	for i := 0; i < numMessages; i++ {
		offset := batch.GetIntColumn(0).Get(i)
		require.Equal(t, int64(i), offset)
		ts := batch.GetTimestampColumn(1).Get(i)
		require.Equal(t, int64(1000+i), ts.Val)
		key := batch.GetBytesColumn(2).Get(i)
		require.Equal(t, fmt.Sprintf("key%d", i), string(key))
		require.Equal(t, []byte{0}, batch.GetBytesColumn(3).Get(i))
		val := batch.GetBytesColumn(4).Get(i)
		require.Equal(t, fmt.Sprintf("val%d", i), string(val))
	}
}

type capturingProcessor struct {
	processBatch *proc.ProcessBatch
}

func (cp *capturingProcessor) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (cp *capturingProcessor) GetWithMaxVersion(key []byte, maxVersion uint64) ([]byte, error) {
	return nil, nil
}

func (cp *capturingProcessor) NewIterator(keyStart []byte, keyEnd []byte, highestVersion uint64, preserveTombstones bool) (iteration.Iterator, error) {
	return nil, nil
}

func (cp *capturingProcessor) LoadLastProcessedReplBatchSeq(int) (int64, error) {
	return 0, nil
}

func (cp *capturingProcessor) WriteCache() *proc.WriteCache {
	return nil
}

func (cp *capturingProcessor) CloseVersion(int, []int) {
}

func (cp *capturingProcessor) ProcessBatch(*proc.ProcessBatch, func(error)) {
}

func (cp *capturingProcessor) ReprocessBatch(*proc.ProcessBatch, func(error)) {
}

func (cp *capturingProcessor) SetLeader() {
}

func (cp *capturingProcessor) SetReplicator(proc.Replicator) {
}

func (cp *capturingProcessor) GetReplicator() proc.Replicator {
	return nil
}

func (cp *capturingProcessor) SubmitAction(func() error) bool {
	return false
}

func (cp *capturingProcessor) IsStopped() bool {
	return false
}

func (cp *capturingProcessor) SetBarriersInjected() {
}

func (cp *capturingProcessor) SetNotIdleNotifier(func()) {
}

func (cp *capturingProcessor) IsIdle(int) bool {
	return false
}

func (cp *capturingProcessor) SetVersionCompleteHandler(proc.VersionCompleteHandler) {
}

func (cp *capturingProcessor) InvalidateCachedReceiverInfo() {
}

func (cp *capturingProcessor) IsLeader() bool {
	return true
}

func (cp *capturingProcessor) Pause() {
}

func (cp *capturingProcessor) Unpause() {
}

func (cp *capturingProcessor) Stop() {
}

func (cp *capturingProcessor) IngestBatch(processBatch *proc.ProcessBatch, completionFunc func(error)) {
	cp.processBatch = processBatch
	completionFunc(nil)
}

func (cp *capturingProcessor) IngestBatchSync(processBatch *proc.ProcessBatch) error {
	ch := make(chan error, 1)
	cp.IngestBatch(processBatch, func(err error) {
		ch <- err
	})
	return <-ch
}

func (cp *capturingProcessor) RunOnLoop() error {
	return nil
}

func (cp *capturingProcessor) CheckInProcessorLoop() {
}

func (cp *capturingProcessor) ID() int {
	return 0
}

func TestLoadStoreLastOffset(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	var ingestedMessageCount uint64
	procMgr := tppm.NewTestProcessorManager()
	ingestEnabled := atomic.Bool{}
	ingestEnabled.Store(true)
	var lastFlushedVersion int64 = -1
	oper, err := NewBridgeFromOperator(1001, defaultPollTimeout, defaultMaxMessages, "test_topic",
		10, map[string]string{}, msgClientFact{}.createTestMessageClient, false,
		procMgr, 1001, cfg, &ingestedMessageCount, "fk", &ingestEnabled, &lastFlushedVersion)
	require.NoError(t, err)

	st := tppm.NewTestStore()
	p := &testProcessor{id: 23, st: st}

	off, err := oper.loadLastOffset(9, 1000, p)
	require.NoError(t, err)
	require.Equal(t, int64(-1), off)

	ec := &testExecCtx{partitionID: 9, processor: p, version: 500}

	oper.storeLastOffset(9, 1000, ec)
	batch := mem.NewBatch()
	batch.AddEntry(ec.entries[0])

	err = st.Write(batch)
	require.NoError(t, err)

	off, err = oper.loadLastOffset(9, 500, p)
	require.NoError(t, err)
	require.Equal(t, int64(1000), off)

	off, err = oper.loadLastOffset(9, 499, p)
	require.NoError(t, err)
	require.Equal(t, int64(-1), off)
}
