//go:build !main

package tppm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/iteration"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/proc"
	"github.com/timandy/routine"
	"math"
	"sync"
	"sync/atomic"
)

type Store interface {
	Write(batch *mem.Batch) error
	NewIterator(keyStart []byte, keyEnd []byte, maxVersion int64, preserveTombStones bool) (iteration.Iterator, error)
	GetWithMaxVersion(key []byte, maxVersion uint64) ([]byte, error)
}

type TestStore struct {
	lock sync.Mutex
	data *treemap.Map
}

func NewTestStore() *TestStore {
	return &TestStore{
		data: treemap.NewWithStringComparator(),
	}
}

func (t *TestStore) Write(batch *mem.Batch) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	batch.Range(func(key []byte, value []byte) bool {
		t.data.Put(string(key), value)
		return true
	})
	return nil
}

func (t *TestStore) GetWithMaxVersion(key []byte, maxVersion uint64) ([]byte, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	iter := t.data.Iterator()
	for iter.Next() {
		dataKey := []byte(iter.Key().(string))
		value := iter.Value().([]byte)
		dataKeyNoVersion := dataKey[:len(dataKey)-8]
		if bytes.Equal(key, dataKeyNoVersion) {
			ver := math.MaxUint64 - binary.BigEndian.Uint64(dataKey[len(dataKey)-8:])
			if ver <= maxVersion {
				return value, nil
			}
		}
	}
	return nil, nil
}

func (t *TestStore) NewIterator(keyStart []byte, keyEnd []byte, maxVersion int64, preserveTombStones bool) (iteration.Iterator, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	pref := keyStart[:24]
	var entries []common.KV
	iter := t.data.Iterator()
	for iter.Next() {
		key := []byte(iter.Key().(string))
		if !bytes.Equal(key[:24], pref) {
			continue
		}
		keyNoVersion := key[:len(key)-8]
		if bytes.Compare(keyNoVersion, keyStart) >= 0 && bytes.Compare(keyNoVersion, keyEnd) < 0 {
			value := iter.Value().([]byte)
			entries = append(entries, common.KV{
				Key:   key,
				Value: value,
			})
		}
	}
	si := iteration.NewStaticIterator(entries)
	return iteration.NewMergingIterator([]iteration.Iterator{si}, preserveTombStones, uint64(maxVersion))
}

func NewTestProcessorManager() *TestProcessorManager {
	return &TestProcessorManager{
		listeners:        map[string]proc.ProcessorListener{},
		activeProcessors: map[int]*TestProcessor{},
		st:               NewTestStore(),
		writeVersion:     123,
	}
}

type TestProcessorManager struct {
	lock             sync.Mutex
	listeners        map[string]proc.ProcessorListener
	activeProcessors map[int]*TestProcessor
	batchHandler     proc.BatchHandler
	st               Store
	writeVersion     uint64
}

func (t *TestProcessorManager) NodeForPartition(partitionID int, mappingID string, partitionCount int) int {
	return 0
}

func (t *TestProcessorManager) GetStore() Store {
	return t.st
}

func (t *TestProcessorManager) SetWriteVersion(version uint64) {
	atomic.StoreUint64(&t.writeVersion, version)
}

func (t *TestProcessorManager) SetBatchHandler(handler proc.BatchHandler) {
	t.batchHandler = handler
}

func (t *TestProcessorManager) AddActiveProcessor(id int) {
	t.lock.Lock()
	defer t.lock.Unlock()
	_, ok := t.activeProcessors[id]
	if ok {
		return
	}
	processor := newTestProcessor(id, t)
	go processor.eventLoop()
	t.activeProcessors[id] = processor
	t.sendProcessorChanged(processor, true)
}

func (t *TestProcessorManager) RemoveActiveProcessor(id int) {
	t.lock.Lock()
	defer t.lock.Unlock()
	processor, ok := t.activeProcessors[id]
	if !ok {
		panic("cannot find processor")
	}
	processor.close()
	delete(t.activeProcessors, id)
	t.sendProcessorChanged(processor, false)
}

func (t *TestProcessorManager) GetProcessor(id int) proc.Processor {
	t.lock.Lock()
	defer t.lock.Unlock()
	processor, ok := t.activeProcessors[id]
	if !ok {
		return nil
	}
	return processor
}

func (t *TestProcessorManager) sendProcessorChanged(processor proc.Processor, added bool) {
	for _, listener := range t.listeners {
		listener(processor, added, false)
	}
}

func (t *TestProcessorManager) AddFlushedCallback() {

}

func (t *TestProcessorManager) RegisterListener(id string, listener proc.ProcessorListener) []proc.Processor {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.listeners[id] = listener
	var active []proc.Processor
	for _, processor := range t.activeProcessors {
		active = append(active, processor)
	}
	return active
}

func (t *TestProcessorManager) UnregisterListener(id string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.listeners, id)
}

func (t *TestProcessorManager) Close() {
	t.lock.Lock()
	defer t.lock.Unlock()
	for _, processor := range t.activeProcessors {
		processor.close()
	}
}

func (t *TestProcessorManager) BeforeReceiverChange() error {
	return nil
}

func (t *TestProcessorManager) AfterReceiverChange() {
}

func (t *TestProcessorManager) ForwardBatch(batch *proc.ProcessBatch, replicate bool, completionFunc func(error)) {
}

type TestProcessor struct {
	lock       sync.Mutex
	closed     bool
	id         int
	ingestCh   chan ingestBatchHolder
	goID       int64
	tpm        *TestProcessorManager
	stopWg     sync.WaitGroup
	version    *uint64
	storeCache *proc.WriteCache
}

func (t *TestProcessor) Get(key []byte) ([]byte, error) {
	return t.tpm.st.GetWithMaxVersion(key, math.MaxUint64)
}

func (t *TestProcessor) GetWithMaxVersion(key []byte, maxVersion uint64) ([]byte, error) {
	return t.tpm.st.GetWithMaxVersion(key, maxVersion)
}

func (t *TestProcessor) NewIterator(keyStart []byte, keyEnd []byte, highestVersion uint64, preserveTombstones bool) (iteration.Iterator, error) {
	return t.tpm.st.NewIterator(keyStart, keyEnd, int64(highestVersion), preserveTombstones)
}

func (t *TestProcessor) LoadLastProcessedReplBatchSeq(int) (int64, error) {
	return 0, nil
}

func (t *TestProcessor) WriteCache() *proc.WriteCache {
	return t.storeCache
}

func (t *TestProcessor) CloseVersion(int, []int) {
}

func (t *TestProcessor) ProcessBatch(*proc.ProcessBatch, func(error)) {
}

func (t *TestProcessor) ReprocessBatch(*proc.ProcessBatch, func(error)) {
}

func (t *TestProcessor) SetLeader() {
}

func (t *TestProcessor) SetReplicator(proc.Replicator) {
}

func (t *TestProcessor) GetReplicator() proc.Replicator {
	return nil
}

func (t *TestProcessor) SubmitAction(func() error) bool {
	return false
}

func (t *TestProcessor) IsStopped() bool {
	return false
}

func (t *TestProcessor) SetBarriersInjected() {
}

func (t *TestProcessor) SetVersionCompleteHandler(proc.VersionCompleteHandler) {
}

func (t *TestProcessor) InvalidateCachedReceiverInfo() {
}

func (t *TestProcessor) IsLeader() bool {
	return true
}

func (t *TestProcessor) Pause() {
}

func (t *TestProcessor) Unpause() {
}

func (t *TestProcessor) Stop() {
}

func (t *TestProcessor) IsIdle(int) bool {
	return false
}

func (t *TestProcessor) SetNotIdleNotifier(func()) {
}

func newTestProcessor(id int, tpm *TestProcessorManager) *TestProcessor {
	p := &TestProcessor{
		id:         id,
		ingestCh:   make(chan ingestBatchHolder, 10),
		version:    &tpm.writeVersion,
		tpm:        tpm,
		storeCache: proc.NewWriteCache(tpm.st, math.MaxInt64, -1),
	}
	p.stopWg.Add(1)
	return p
}

type ingestBatchHolder struct {
	pb *proc.ProcessBatch
	cf func(error)
}

func (t *TestProcessor) ID() int {
	return t.id
}

func (t *TestProcessor) IngestBatchSync(processBatch *proc.ProcessBatch) error {
	ch := make(chan error, 1)
	t.IngestBatch(processBatch, func(err error) {
		ch <- err
	})
	return <-ch
}

func (t *TestProcessor) IngestBatch(processBatch *proc.ProcessBatch, cf func(error)) {
	t.lock.Lock()
	defer func() {
		t.lock.Unlock()
	}()
	if t.closed {
		cf(errors.NewTektiteErrorf(errors.Unavailable, "processor is closed"))
		return
	}
	t.ingestCh <- ingestBatchHolder{
		pb: processBatch,
		cf: cf,
	}
}

func (t *TestProcessor) CheckInProcessorLoop() {
	if routine.Goid() != t.goID {
		panic("not on processor loop")
	}
}

func (t *TestProcessor) eventLoop() {
	defer t.stopWg.Done()
	t.goID = routine.Goid()
	for holder := range t.ingestCh {
		t.handleBatchHolder(holder)
	}
}

func (t *TestProcessor) handleBatchHolder(holder ingestBatchHolder) {
	ver := int(atomic.LoadUint64(t.version))
	holder.pb.Version = ver
	ok, mb, forwardBatches, err := t.tpm.batchHandler.HandleProcessBatch(t, holder.pb, false)
	if err != nil {
		holder.cf(err)
		return
	}
	if !ok {
		log.Error("batch handler failed to find receiver")
		return
	}
	if mb != nil {
		if err := t.tpm.st.Write(mb); err != nil {
			log.Errorf("failed to write batch %v\ncreation stack %s", err)
		}
	}
	if forwardBatches != nil {
		for _, pb := range forwardBatches {
			processor, ok := t.tpm.activeProcessors[pb.ProcessorID]
			if !ok {
				panic(fmt.Sprintf("no processor for partition %d", pb.PartitionID))
			}
			if err := processor.IngestBatchSync(pb); err != nil {
				panic(err)
			}
		}
	}
	holder.cf(nil)
	if err := t.WriteCache().MaybeWriteToStore(); err != nil {
		log.Errorf("failed to write to store %v", err)
	}
}

func (t *TestProcessor) close() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.closed = true
	close(t.ingestCh)
	t.stopWg.Wait()
}

func NewTestNodePartitionProvider(nodePartitions map[int][]int) *TestNodePartitionProvider {
	partitionNodeMap := map[int]int{}
	for nodeID, parts := range nodePartitions {
		for _, partID := range parts {
			partitionNodeMap[partID] = nodeID
		}
	}
	return &TestNodePartitionProvider{
		nodePartitions:   nodePartitions,
		partitionNodeMap: partitionNodeMap,
	}
}

type TestNodePartitionProvider struct {
	nodePartitions   map[int][]int
	partitionNodeMap map[int]int
}

func (t *TestNodePartitionProvider) NodeForPartition(partitionID int, _ string, _ int) int {
	return t.partitionNodeMap[partitionID]
}

func (t *TestNodePartitionProvider) NodePartitions(string, int) (map[int][]int, error) {
	return t.nodePartitions, nil
}

type TestClustVersionProvider struct {
	ClustVersion int
}

func (tcvp *TestClustVersionProvider) ClusterVersion() int {
	return tcvp.ClustVersion
}

func (tcvp *TestClustVersionProvider) IsReadyAsOfVersion(int) bool {
	return true
}
