//go:build !main

package tppm

import (
	"fmt"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/proc"
	"github.com/timandy/routine"
	"math"
	"sync"
	"sync/atomic"
)

type store interface {
	Write(batch *mem.Batch) error
}

type TestProcessorManager struct {
	lock             sync.Mutex
	listeners        map[string]proc.ProcessorListener
	activeProcessors map[int]*TestProcessor
	batchHandler     proc.BatchHandler
	st               store
	writeVersion     uint64
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
		panic("already a processor")
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

func (t *TestProcessorManager) GetProcessor(id int) *TestProcessor {
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

func NewTestProcessorManager(st store) *TestProcessorManager {
	return &TestProcessorManager{
		listeners:        map[string]proc.ProcessorListener{},
		activeProcessors: map[int]*TestProcessor{},
		st:               st,
		writeVersion:     123,
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
