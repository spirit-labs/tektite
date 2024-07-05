package proc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/vmgr"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCreateProcessor(t *testing.T) {
	id := 1234
	proc := createProcessor(t, id, &testForwarder{}, &testBatchHandler{}, &testReceiverInfoProvider{})
	require.Equal(t, id, proc.ID())
}

func TestProcessBatchNotLeader(t *testing.T) {
	id := 1234
	proc := createProcessor(t, id, &testForwarder{}, &testBatchHandler{}, &testReceiverInfoProvider{})
	batch := NewProcessBatch(id, nil, 1, 1, -1)
	ch := make(chan error, 1)
	proc.IngestBatch(batch, func(err error) {
		ch <- err
	})
	err := <-ch
	require.Error(t, err)
	require.Equal(t, "processor is not leader", err.Error())
}

func TestProcessBatch(t *testing.T) {

	memBatch := mem.NewBatch()
	memBatch.AddEntry(common.KV{
		Key:   encoding.EncodeVersion([]byte("key1"), 0),
		Value: []byte("val1"),
	})

	processorID := 1234

	batchHandler := &testBatchHandler{
		memBatch: memBatch,
	}

	proc := createProcessor(t, processorID, &testForwarder{}, batchHandler, &testReceiverInfoProvider{})
	proc.SetLeader()

	proc.CloseVersion(0, nil)

	batch := NewProcessBatch(processorID, nil, 1, 1, -1)
	ch := make(chan error, 1)
	proc.IngestBatch(batch, func(err error) {
		ch <- err
	})
	err := <-ch
	require.NoError(t, err)

	require.Equal(t, 1, len(batchHandler.receivedBatches))
	require.Equal(t, batch, batchHandler.receivedBatches[0].processBatch)
	require.Equal(t, proc, batchHandler.receivedBatches[0].processor)

	v, err := proc.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("val1"), v)
}

func TestActionsAfterStopNotRun(t *testing.T) {

	batchHandler := &testBatchHandler{}
	receiverInfoProvider := &testReceiverInfoProvider{}

	proc := createProcessor(t, 1234, &testForwarder{}, batchHandler, receiverInfoProvider)

	proc.SetLeader()

	proc.Stop()

	// Actions submitted after stop won't be run
	var called atomic.Bool
	proc.SubmitAction(func() error {
		called.Store(true)
		return nil
	})
	time.Sleep(10 * time.Millisecond)
	require.False(t, called.Load())
}

func TestForwardBatches(t *testing.T) {

	memBatch := mem.NewBatch()
	memBatch.AddEntry(common.KV{
		Key:   encoding.EncodeVersion([]byte("key1"), 0),
		Value: []byte("val1"),
	})

	processorID := 10

	var forwardBatches []*ProcessBatch
	numParts := 10
	numBatchesPerPart := 5
	for i := 0; i < numParts; i++ {
		for j := 0; j < numBatchesPerPart; j++ {
			forwardBatch := NewProcessBatch(processorID, nil, 1, i, processorID)
			forwardBatches = append(forwardBatches, forwardBatch)
		}
	}

	batchHandler := &testBatchHandler{
		memBatch:         memBatch,
		forwardedBatches: forwardBatches,
	}

	forwarder := &testForwarder{}

	proc := createProcessor(t, processorID, forwarder, batchHandler, &testReceiverInfoProvider{})

	proc.SetLeader()

	proc.CloseVersion(0, nil)

	batch := NewProcessBatch(processorID, nil, 1, 1, -1)
	ch := make(chan error, 1)
	proc.IngestBatch(batch, func(err error) {
		ch <- err
	})
	err := <-ch
	require.NoError(t, err)

	require.Equal(t, 1, len(batchHandler.receivedBatches))
	require.Equal(t, batch, batchHandler.receivedBatches[0].processBatch)
	require.Equal(t, proc, batchHandler.receivedBatches[0].processor)

	require.Equal(t, forwardBatches, batchHandler.forwardedBatches)

	v, err := proc.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("val1"), v)
}

type vcHandler struct {
	lock              sync.Mutex
	completingVersion int
	completedVersion  int
	completions       int
}

func newVcHandler() vcHandler {
	return vcHandler{
		completedVersion:  -1,
		completingVersion: -1,
	}
}

func (v *vcHandler) versionComplete(version int, requiredCompletions int, _ int, _ bool, cf func(error)) {
	v.lock.Lock()
	defer v.lock.Unlock()
	if version <= v.completedVersion {
		panic(fmt.Sprintf("version %d already complete - completed version %d", version, v.completingVersion))
	}
	if version < v.completingVersion {
		// Ignore - completion came in for older version - this can happen when newer barriers are injected when
		// previous version hasn't been completed yet
		return
	}
	if version > v.completingVersion {
		// Higher version supersedes lower version
		v.completingVersion = version
		v.completions = 0
	}
	v.completions++
	if v.completions == requiredCompletions {
		v.completedVersion = version
		v.completions = 0
	}
	go func() {
		cf(nil)
	}()
}

func (v *vcHandler) getCompletedVersion() int {
	v.lock.Lock()
	defer v.lock.Unlock()
	return v.completedVersion
}

func (v *vcHandler) waitForVersionToComplete(t *testing.T, version int) {
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		return v.getCompletedVersion() == version, nil
	}, 5*time.Second, 1*time.Millisecond)
	require.True(t, ok)
	require.NoError(t, err)
}

func TestBarrierWithSameVersionAfterVersionComplete(t *testing.T) {

	processorID := 1234

	batchHandler := newForwardingBatchHandler()
	batchHandler.requiredCompletions = 1

	proc := createProcessor(t, processorID, &testForwarder{}, batchHandler, batchHandler)

	vHandler := newVcHandler()
	proc.SetVersionCompleteHandler(vHandler.versionComplete)

	proc.SetLeader()
	receiverID := 1000

	ch := make(chan error, 1)
	closeVersion(proc, []int{receiverID}, 100, func(err error) {
		ch <- err
	})
	err := <-ch
	require.NoError(t, err)

	require.Equal(t, 100, vHandler.getCompletedVersion())

	// Inject same version again - should be ignored
	ch = make(chan error, 1)
	closeVersion(proc, []int{receiverID}, 100, func(err error) {
		ch <- err
	})
	err = <-ch
	require.NoError(t, err)
}

func TestBarrierWithOlderVersionAfterVersionComplete(t *testing.T) {

	processorID := 1234

	batchHandler := newForwardingBatchHandler()
	batchHandler.requiredCompletions = 1

	proc := createProcessor(t, processorID, &testForwarder{}, batchHandler, batchHandler)

	vHandler := newVcHandler()
	proc.SetVersionCompleteHandler(vHandler.versionComplete)

	proc.SetLeader()

	receiverID := 1000

	ch := make(chan error, 1)
	closeVersion(proc, []int{receiverID}, 100, func(err error) {
		ch <- err
	})
	err := <-ch
	require.NoError(t, err)

	require.Equal(t, 100, vHandler.getCompletedVersion())

	ch = make(chan error, 1)
	injectBatch(proc, receiverID, func(err error) {
		ch <- err
	})
	err = <-ch
	require.NoError(t, err)

	require.Equal(t, 101, batchHandler.receivedBatches[receiverID][0].Version)

	// Inject barrier with older version - should be ignored
	ch = make(chan error, 1)
	closeVersion(proc, []int{receiverID}, 99, func(err error) {
		ch <- err
	})
	err = <-ch
	require.NoError(t, err)

	require.Equal(t, 100, vHandler.getCompletedVersion())

	ch = make(chan error, 1)
	injectBatch(proc, receiverID, func(err error) {
		ch <- err
	})
	err = <-ch
	require.NoError(t, err)

	require.Equal(t, 101, batchHandler.receivedBatches[receiverID][1].Version)
}

func TestForwardingMultipleReceiversAndProcessors(t *testing.T) {

	numProcessors := 10
	var processors []*processor

	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.ProcessorCount = numProcessors

	var batchHandlers []*forwardingBatchHandler
	batchForwarder := &testBatchForwarder{}

	vHandler := newVcHandler()

	receiver1 := 10
	receiver2 := 12
	receiver3 := 20
	receiver4 := 21
	receiver5 := 100

	receiver2Processors := []int{2, 5, 7}
	receiver3Processors := []int{0, 6, 9, 3}
	receiver4Processors := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	receiver1Processors := []int{3, 7, 5, 8, 9, 2, 1, 4}
	receiver5Processors := []int{0, 1, 3, 4, 6, 7, 9}

	injectableReceivers := map[int][]int{
		0: {receiver5},
		1: {receiver1, receiver5},
		2: {receiver1},
		3: {receiver1, receiver5},
		4: {receiver1, receiver5},
		5: {receiver1},
		6: {receiver5},
		7: {receiver1, receiver5},
		8: {receiver1},
		9: {receiver1, receiver5},
	}
	requiredCompletions := len(receiver5Processors) + len(receiver3Processors) + len(receiver4Processors)

	for i := 0; i < numProcessors; i++ {
		/*
				receiver1 -> receiver2 -> receiver3
									   |-> receiver4
			    receiver5

			    receiver1 and receiver5 are top level
			    receiver3 and receiver4 and receiver5 are terminal

				receiver 1 has 8 processors
				receiver 2 has 3 processors
				receiver 3 has 4 processors
				receiver 4 has 10 processors
			    receiver 5 has 7 processors
		*/
		batchHandler := newForwardingBatchHandler()
		batchHandler.forwardBatchToOneProcessor = true
		batchHandler.requiredCompletions = requiredCompletions
		// This tells us for each receiver, what receiver(s) it forwards to and the processors on the receiver(s) it
		// forwards to
		batchHandler.forwardingInfos = map[int][]*barrierForwardingInfo{
			receiver1: {{
				forwardReceiverID: receiver2,
				processorIDs:      receiver2Processors,
			}},
			receiver2: {{
				forwardReceiverID: receiver3,
				processorIDs:      receiver3Processors,
			}, {
				forwardReceiverID: receiver4,
				processorIDs:      receiver4Processors,
			}},
		}
		// These are the processor counts for the receivers that *forward to* the specified receiver
		batchHandler.forwardingProcessorCounts = map[int]int{
			receiver2: len(receiver1Processors),
			receiver3: len(receiver2Processors),
			receiver4: len(receiver2Processors),
		}
		batchHandlers = append(batchHandlers, batchHandler)

		proc := NewProcessor(i, cfg, &dummyStore{}, batchForwarder, batchHandler, batchHandler, dataKey(), nil).(*processor)

		proc.SetVersionCompleteHandler(vHandler.versionComplete)

		processors = append(processors, proc)
		proc.SetLeader()

	}
	batchForwarder.processors = processors

	numBatchesPerVersion := 10
	numVersions := 10

	ch := make(chan error, 1)
	cf := common.NewCountDownFuture((1+numVersions)*numProcessors+len(receiver1Processors)*numBatchesPerVersion*numVersions+
		len(receiver5Processors)*numBatchesPerVersion*numVersions, func(err error) {
		ch <- err
	})
	var receiver1SentBatches []*ProcessBatch
	var receiver5SentBatches []*ProcessBatch

	for i := 0; i < numVersions; i++ {
		version := 100 + i
		// We inject in the top level receivers - these are receiver1 and receiver5

		// Close the initial version
		if i == 0 {
			for procID := 0; procID < numProcessors; procID++ {
				recs := injectableReceivers[procID]
				proc := processors[procID]
				closeVersion(proc, recs, 99, cf.CountDown)
			}
			vHandler.waitForVersionToComplete(t, 99)
		}

		// receiver1
		for _, procID := range receiver1Processors {
			proc := processors[procID]
			for j := 0; j < numBatchesPerVersion; j++ {
				batch := injectBatch(proc, receiver1, cf.CountDown)
				receiver1SentBatches = append(receiver1SentBatches, batch)
			}
		}

		// receiver5

		for _, procID := range receiver5Processors {
			proc := processors[procID]
			for j := 0; j < numBatchesPerVersion; j++ {
				batch := injectBatch(proc, receiver5, cf.CountDown)
				receiver5SentBatches = append(receiver5SentBatches, batch)
			}
		}

		// Close the version
		for procID := 0; procID < numProcessors; procID++ {
			recs := injectableReceivers[procID]
			proc := processors[procID]
			closeVersion(proc, recs, version, cf.CountDown)
		}

		if true {
			// We will block waiting for version to complete before going to next version
			vHandler.waitForVersionToComplete(t, version)
		}
	}
	err := <-ch
	require.NoError(t, err)

	// Wait for last version to complete
	finalVersion := 100 + numVersions - 1
	vHandler.waitForVersionToComplete(t, finalVersion)

	verifyBatchesReceivedAtTerminalReceiver(t, receiver1SentBatches, receiver3, receiver3Processors, batchHandlers)
	verifyBatchesReceivedAtTerminalReceiver(t, receiver1SentBatches, receiver4, receiver4Processors, batchHandlers)
	verifyBatchesReceivedAtTerminalReceiver(t, receiver5SentBatches, receiver5, receiver5Processors, batchHandlers)
}

type dummyStore struct {
}

func (d *dummyStore) Start() {
}

func (d *dummyStore) Stop() {
	panic("implement me")
}

func (d *dummyStore) Get(key []byte) ([]byte, error) {
	panic("implement me")
}

func (d *dummyStore) Write(batch *mem.Batch) error {
	return nil
}

func (d *dummyStore) Flush(cb func(error)) error {
	panic("implement me")
}

func (d *dummyStore) MaybeReplaceMemtable() error {
	panic("implement me")
}

func (d *dummyStore) Clear() error {
	panic("implement me")
}

func (d *dummyStore) GetWithMaxVersion(key []byte, maxVersion uint64) ([]byte, error) {
	panic("implement me")
}

func (d *dummyStore) NewIterator(keyStart []byte, keyEnd []byte, highestVersion uint64, preserveTombstones bool) (iteration.Iterator, error) {
	panic("implement me")
}

func verifyBatchesReceivedAtTerminalReceiver(t *testing.T, sentBatches []*ProcessBatch, receiverID int, processorIDs []int,
	batchHandlers []*forwardingBatchHandler) {

	receiverProcessors := map[int]struct{}{}
	for _, procID := range processorIDs {
		receiverProcessors[procID] = struct{}{}
	}

	var receivedBatches []*ProcessBatch
	for procID, handler := range batchHandlers {
		_, isReceiverProcessor := receiverProcessors[procID]
		batches, ok := handler.receivedBatches[receiverID]
		if isReceiverProcessor {
			require.True(t, ok)
			receivedBatches = append(receivedBatches, batches...)
		} else {
			// Should be none received on non receiver processors
			require.False(t, ok)
		}
	}
	require.Equal(t, len(sentBatches), len(receivedBatches))
	receivedBatchesSet := map[*evbatch.Batch]struct{}{}
	for _, receivedBatch := range receivedBatches {
		receivedBatchesSet[receivedBatch.EvBatch] = struct{}{}
	}
	require.Equal(t, len(sentBatches), len(receivedBatchesSet))
	for _, sentBatch := range sentBatches {
		_, ok := receivedBatchesSet[sentBatch.EvBatch]
		require.True(t, ok)
	}
}

func TestBarrierNewerVersionOverridesVersionBeingCompleted(t *testing.T) {
	receiverID := 10
	forwardReceiverID := 11
	numProcessors := 10

	var processors []*processor

	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.ProcessorCount = numProcessors

	var batchHandlers []*forwardingBatchHandler
	batchForwarder := &testBatchForwarder{}

	vHandler := newVcHandler()

	var allProcessors []int
	for i := 0; i < numProcessors; i++ {
		allProcessors = append(allProcessors, i)
	}

	for i := 0; i < numProcessors; i++ {
		batchHandler := newForwardingBatchHandler()
		batchHandler.requiredCompletions = numProcessors
		batchHandler.forwardingInfos = map[int][]*barrierForwardingInfo{
			receiverID: {{
				forwardReceiverID: forwardReceiverID,
				processorIDs:      allProcessors,
			}},
			forwardReceiverID: nil,
		}
		batchHandler.forwardingProcessorCounts = map[int]int{
			forwardReceiverID: numProcessors,
		}
		batchHandlers = append(batchHandlers, batchHandler)

		proc := NewProcessor(i, cfg, &dummyStore{}, batchForwarder, batchHandler, batchHandler, dataKey(), nil).(*processor)

		proc.SetVersionCompleteHandler(vHandler.versionComplete)

		processors = append(processors, proc)
		proc.SetLeader()

	}
	batchForwarder.processors = processors

	for _, proc := range processors {
		// Inject initial barrier
		ch := make(chan error, 1)
		closeVersion(proc, []int{receiverID}, 100, func(err error) {
			ch <- err
		})
		err := <-ch
		require.NoError(t, err)
	}
	vHandler.waitForVersionToComplete(t, 100)

	for _, proc := range processors {
		// Inject a batch
		ch := make(chan error, 1)
		injectBatch(proc, receiverID, func(err error) {
			ch <- err
		})
		err := <-ch
		require.NoError(t, err)
	}

	// Now inject barrier at all but one processor
	// This should leave the barrier in a waiting but incomplete state, so the version shouldn't be completed
	for i, proc := range processors {
		if i != len(processors)-1 {
			ch := make(chan error, 1)
			closeVersion(proc, []int{receiverID}, 101, func(err error) {
				ch <- err
			})
			err := <-ch
			require.NoError(t, err)
		}
	}
	time.Sleep(10 * time.Millisecond) // a little time to forward
	require.Equal(t, 100, vHandler.getCompletedVersion())

	// Now inject barriers for all processors at higher version - this should cause barrier to unblock and be completed
	// at higher version
	for _, proc := range processors {
		ch := make(chan error, 1)
		closeVersion(proc, []int{receiverID}, 102, func(err error) {
			ch <- err
		})
		err := <-ch
		require.NoError(t, err)
	}

	vHandler.waitForVersionToComplete(t, 102)
}

func TestInvalidateCachedReceiverInfo(t *testing.T) {

	batchHandler := newForwardingBatchHandler()

	receiverID := 1000
	forwardingReceiverID := 1001

	batchHandler.forwardingProcessorCounts = map[int]int{
		forwardingReceiverID: 1,
	}
	batchHandler.forwardingInfos = map[int][]*barrierForwardingInfo{
		receiverID: {
			{forwardingReceiverID, []int{0}},
		},
	}
	batchHandler.requiredCompletions = 1

	forwarder := &testBatchForwarder{}

	proc := createProcessor(t, 0, forwarder, batchHandler, batchHandler)

	forwarder.processors = []*processor{proc}

	vhandler := newVcHandler()
	proc.SetVersionCompleteHandler(vhandler.versionComplete)

	proc.SetLeader()

	ch := make(chan error, 1)
	closeVersion(proc, []int{receiverID}, 100, func(err error) {
		ch <- err
	})
	err := <-ch
	require.NoError(t, err)

	vhandler.waitForVersionToComplete(t, 100)

	require.Equal(t, 1, int(atomic.LoadInt64(&batchHandler.getRequiredCompletionsCallCount)))
	require.Equal(t, 1, int(atomic.LoadInt64(&batchHandler.getForwardingProcessorCountCallCount)))

	// Now inject a barrier at version 101
	ch = make(chan error, 1)
	closeVersion(proc, []int{receiverID}, 101, func(err error) {
		ch <- err
	})
	err = <-ch
	require.NoError(t, err)

	vhandler.waitForVersionToComplete(t, 101)

	require.Equal(t, 1, int(atomic.LoadInt64(&batchHandler.getRequiredCompletionsCallCount)))
	require.Equal(t, 1, int(atomic.LoadInt64(&batchHandler.getForwardingProcessorCountCallCount)))

	proc.InvalidateCachedReceiverInfo()

	// Now inject a barrier at version 1012
	ch = make(chan error, 1)
	closeVersion(proc, []int{receiverID}, 102, func(err error) {
		ch <- err
	})
	err = <-ch
	require.NoError(t, err)

	vhandler.waitForVersionToComplete(t, 102)

	// Processor rc info was invalidated so these should have been called again
	require.Equal(t, 2, int(atomic.LoadInt64(&batchHandler.getRequiredCompletionsCallCount)))
	require.Equal(t, 2, int(atomic.LoadInt64(&batchHandler.getForwardingProcessorCountCallCount)))
}

func TestForwardFromSingleProcessor(t *testing.T) {

	// Barriers take a different route (no delaying) in the processor for this case, so we test it explicitly

	batchHandler := newForwardingBatchHandler()

	receiverID := 1000
	forwardingReceiverID := 1001

	batchHandler.forwardingProcessorCounts = map[int]int{
		forwardingReceiverID: 1,
	}
	batchHandler.forwardingInfos = map[int][]*barrierForwardingInfo{
		receiverID: {
			{forwardingReceiverID, []int{0}},
		},
	}
	batchHandler.requiredCompletions = 1

	forwarder := &testBatchForwarder{}

	proc := createProcessor(t, 0, forwarder, batchHandler, batchHandler)

	forwarder.processors = []*processor{proc}

	vhandler := newVcHandler()
	proc.SetVersionCompleteHandler(vhandler.versionComplete)

	proc.SetLeader()

	ch := make(chan error, 1)
	closeVersion(proc, []int{receiverID}, 100, func(err error) {
		ch <- err
	})
	err := <-ch
	require.NoError(t, err)

	vhandler.waitForVersionToComplete(t, 100)

	require.Equal(t, 1, int(atomic.LoadInt64(&batchHandler.getRequiredCompletionsCallCount)))
	require.Equal(t, 1, int(atomic.LoadInt64(&batchHandler.getForwardingProcessorCountCallCount)))

	// Now inject a barrier at version 101
	ch = make(chan error, 1)
	closeVersion(proc, []int{receiverID}, 101, func(err error) {
		ch <- err
	})
	err = <-ch
	require.NoError(t, err)

	vhandler.waitForVersionToComplete(t, 101)
}

func TestBarriersNoForwarding(t *testing.T) {

	processorID := 1234

	batchHandler := newForwardingBatchHandler()

	vHandler := newVcHandler()
	proc := createProcessor(t, processorID, &testForwarder{}, batchHandler, batchHandler)
	proc.SetVersionCompleteHandler(vHandler.versionComplete)

	proc.SetLeader()

	numReceivers := 10
	batchHandler.requiredCompletions = numReceivers

	numBatchesPerVersion := 10
	numVersions := 10
	ch := make(chan error, 1)
	cf := common.NewCountDownFuture(numVersions+1+numVersions*numReceivers*numBatchesPerVersion, func(err error) {
		ch <- err
	})
	sentBatches := map[int][]*ProcessBatch{}
	var receiverIDs []int
	for receiverID := common.UserReceiverIDBase; receiverID < common.UserReceiverIDBase+numReceivers; receiverID++ {
		receiverIDs = append(receiverIDs, receiverID)
	}
	// Close the first version
	closeVersion(proc, receiverIDs, 99, cf.CountDown)
	vHandler.waitForVersionToComplete(t, 99)
	for version := 100; version < 100+numVersions; version++ {
		for _, receiverID := range receiverIDs {
			for i := 0; i < numBatchesPerVersion; i++ {
				batch := injectBatch(proc, receiverID, cf.CountDown)
				sentBatches[receiverID] = append(sentBatches[receiverID], batch)
			}
		}
		closeVersion(proc, receiverIDs, version, cf.CountDown)
		vHandler.waitForVersionToComplete(t, version)
	}
	err := <-ch
	require.NoError(t, err)

	for _, receiverID := range receiverIDs {
		sent := sentBatches[receiverID]
		received := batchHandler.receivedBatches[receiverID]
		require.Equal(t, len(sent), len(received))
		require.Equal(t, numVersions*numBatchesPerVersion, len(received))
		i := 0
		for v := 0; v < numVersions; v++ {
			version := 100 + v
			for j := 0; j < numBatchesPerVersion; j++ {
				require.Equal(t, sent[i], received[i])
				require.Equal(t, version, received[i].Version)
				require.False(t, received[i].Barrier)
				i++
			}
		}
	}
	require.Equal(t, 100+numVersions-1, vHandler.getCompletedVersion())
}

func TestBarriersWithForwarding(t *testing.T) {

	receiverID := common.UserReceiverIDBase + 10
	forwardReceiverID := common.UserReceiverIDBase + 11
	numProcessors := 10
	numVersions := 10

	var processors []*processor

	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.ProcessorCount = numProcessors

	var batchHandlers []*forwardingBatchHandler
	batchForwarder := &testBatchForwarder{}

	finalVersion := 100 + numVersions - 1

	vHandler := newVcHandler()

	var allProcessors []int
	for i := 0; i < numProcessors; i++ {
		allProcessors = append(allProcessors, i)
	}

	for i := 0; i < numProcessors; i++ {
		batchHandler := newForwardingBatchHandler()
		batchHandler.requiredCompletions = numProcessors
		batchHandler.forwardingInfos = map[int][]*barrierForwardingInfo{
			receiverID: {{
				forwardReceiverID: forwardReceiverID,
				processorIDs:      allProcessors,
			}},
			forwardReceiverID: nil,
		}
		batchHandler.forwardingProcessorCounts = map[int]int{
			forwardReceiverID: numProcessors,
		}
		batchHandlers = append(batchHandlers, batchHandler)

		proc := NewProcessor(i, cfg, &dummyStore{}, batchForwarder, batchHandler, batchHandler, dataKey(), nil).(*processor)

		proc.SetVersionCompleteHandler(vHandler.versionComplete)

		processors = append(processors, proc)
		proc.SetLeader()

	}
	batchForwarder.processors = processors

	numBatchesPerVersion := 10

	ch := make(chan error, 1)
	cf := common.NewCountDownFuture(numProcessors*(1+numVersions*(1+numBatchesPerVersion)), func(err error) {
		ch <- err
	})
	sentBatches := map[int][]*ProcessBatch{}

	for i := 0; i < numVersions; i++ {
		version := 100 + i
		for _, proc := range processors {
			if i == 0 {
				// Inject an initial barrier to initialise the processor and set currentVersion
				closeVersion(proc, []int{receiverID}, version-1, cf.CountDown)
			}
			for j := 0; j < numBatchesPerVersion; j++ {
				batch := injectBatch(proc, receiverID, cf.CountDown)
				sentBatches[proc.id] = append(sentBatches[proc.id], batch)
			}
			// Close the version
			closeVersion(proc, []int{receiverID}, version, cf.CountDown)
		}
		// We will block waiting for version to complete before going to next version
		vHandler.waitForVersionToComplete(t, version)
	}
	err := <-ch
	require.NoError(t, err)

	for procIndex, proc := range processors {
		sent := sentBatches[proc.id]
		batchHandler := batchHandlers[procIndex]
		received := batchHandler.receivedBatches[receiverID]
		require.Equal(t, len(sent), len(received))
		require.Equal(t, numVersions*numBatchesPerVersion, len(received))
		i := 0
		for version := 100; version < 100+numVersions; version++ {
			for j := 0; j < numBatchesPerVersion; j++ {
				require.Equal(t, sent[i], received[i])
				require.Equal(t, version, received[i].Version)
				require.False(t, received[i].Barrier)
				i++
			}
		}
	}

	// Wait for last version to complete in all processors
	vHandler.waitForVersionToComplete(t, finalVersion)

	for i := 0; i < numProcessors; i++ {
		sent := sentBatches[i]
		handler := batchHandlers[i]
		receivedLocal := handler.receivedBatches[receiverID]
		require.Equal(t, sent, receivedLocal)

		// Every handler should have received batches from every other handler
		receivedRemote := handler.receivedBatches[forwardReceiverID]
		require.Equal(t, numProcessors*numBatchesPerVersion*numVersions, len(receivedRemote))

		receivedByForwardingProc := map[int][]*ProcessBatch{}
		for _, forwardedBatch := range receivedRemote {
			forwardedBatch.ForwardSequence = 0
			receivedByForwardingProc[forwardedBatch.ForwardingProcessorID] =
				append(receivedByForwardingProc[forwardedBatch.ForwardingProcessorID], forwardedBatch)

		}
		// Verify that the received batches are the ones that were forwarded
		for forwardingProcId, batches := range receivedByForwardingProc {
			forwardingHandler := batchHandlers[forwardingProcId]
			allForwardedBatches := forwardingHandler.forwardedBatches[forwardReceiverID]
			var forwardedBatches []*ProcessBatch
			for _, forwardedBatch := range allForwardedBatches {
				if forwardedBatch.ProcessorID == i {
					forwardedBatches = append(forwardedBatches, forwardedBatch)
				}
			}
			require.Equal(t, batches, forwardedBatches)

			// Check the versions of the forwarded batches
			pos := 0
			for v := 0; v < numVersions; v++ {
				version := 100 + v
				for i := 0; i < numBatchesPerVersion; i++ {
					batch := forwardedBatches[pos]
					require.Equal(t, version, batch.Version)
					pos++
				}
			}
		}
	}
}

func TestForwardAfterUnavailability(t *testing.T) {
	forwarder := &testBatchForwarder{}

	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.ProcessorCount = 2

	vHandler := newVcHandler()

	// Create first processor

	batchHandler1 := &forwardingBatchHandler{
		requiredCompletions: 2,
		receivedBatches:     map[int][]*ProcessBatch{},
		forwardedBatches:    map[int][]*ProcessBatch{},
		forwardingInfos: map[int][]*barrierForwardingInfo{
			1000: {{
				forwardReceiverID: 1001,
				processorIDs:      []int{0, 1},
			}},
		},
		forwardingProcessorCounts: map[int]int{
			1001: 2,
		},
	}

	proc1 := NewProcessor(1, cfg, &dummyStore{}, forwarder, batchHandler1, batchHandler1, dataKey(), nil).(*processor)
	proc1.SetVersionCompleteHandler(vHandler.versionComplete)
	proc1.SetLeader()

	// Create second processor
	batchHandler2 := &forwardingBatchHandler{
		requiredCompletions: 2,
		receivedBatches:     map[int][]*ProcessBatch{},
		forwardedBatches:    map[int][]*ProcessBatch{},
		forwardingInfos: map[int][]*barrierForwardingInfo{
			1000: {{
				forwardReceiverID: 1001,
				processorIDs:      []int{0, 1},
			}},
		},
		forwardingProcessorCounts: map[int]int{
			1001: 2,
		},
	}
	proc2 := NewProcessor(2, cfg, &dummyStore{}, forwarder, batchHandler2, batchHandler2, dataKey(), nil).(*processor)
	proc2.SetVersionCompleteHandler(vHandler.versionComplete)
	proc2.SetLeader()

	forwarder.processors = []*processor{proc1, proc2}

	// Send initial barriers
	proc1.CloseVersion(100, []int{1000})
	proc2.CloseVersion(100, []int{1000})

	// Send a batch
	batch := NewProcessBatch(1, nil, 1000, 1, -1)
	ch := make(chan error, 1)
	proc1.IngestBatch(batch, func(err error) {
		ch <- err
	})
	err := <-ch
	require.NoError(t, err)

	// Send barriers to complete version
	proc1.CloseVersion(101, []int{1000})
	proc2.CloseVersion(101, []int{1000})

	// Wait until version is completed on proc2 - this means batch and barrier must have been forwarded
	vHandler.waitForVersionToComplete(t, 101)

	// Tell forwarder to inject failures for a while
	forwarder.failFor(1 * time.Second)

	// Send some more batches
	ch = make(chan error, 1)
	cf := common.NewCountDownFuture(10, func(err error) {
		ch <- err
	})
	for i := 0; i < 10; i++ {
		batch := NewProcessBatch(1, nil, 1000, 1, -1)
		proc1.IngestBatch(batch, cf.CountDown)
		require.NoError(t, err)
	}
	err = <-ch
	require.NoError(t, err)

	// Send barriers to complete version
	proc1.CloseVersion(102, []int{1000})
	proc2.CloseVersion(102, []int{1000})

	// After failing for a while batches should get through and version should be completed
	vHandler.waitForVersionToComplete(t, 102)

	require.Less(t, 0, int(atomic.LoadInt64(&forwarder.failCount)))

	require.Equal(t, 11, len(batchHandler2.receivedBatches[1001]))
}

func TestLoadStoreReplBatchSeq(t *testing.T) {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	st := newTestStore()
	proc1 := NewProcessor(1, cfg, st, nil, nil, nil, dataKey(), nil).(*processor)
	proc1.SetLeader()

	seq, err := proc1.LoadLastProcessedReplBatchSeq(500)
	require.NoError(t, err)
	require.Equal(t, int64(-1), seq)

	batch := &ProcessBatch{Version: 500, ReplSeq: 23}
	mb := mem.NewBatch()
	proc1.persistReplBatchSeq(batch, mb)
	err = st.Write(mb)
	require.NoError(t, err)

	seq, err = proc1.LoadLastProcessedReplBatchSeq(500)
	require.NoError(t, err)
	require.Equal(t, int64(23), seq)

	seq, err = proc1.LoadLastProcessedReplBatchSeq(400)
	require.NoError(t, err)
	require.Equal(t, int64(-1), seq)

	proc2 := NewProcessor(2, cfg, st, nil, nil, nil, dataKey(), nil).(*processor)
	proc2.SetLeader()

	batch = &ProcessBatch{Version: 500, ReplSeq: 56}
	mb = mem.NewBatch()
	proc2.persistReplBatchSeq(batch, mb)
	err = st.Write(mb)
	require.NoError(t, err)

	seq, err = proc2.LoadLastProcessedReplBatchSeq(500)
	require.NoError(t, err)
	require.Equal(t, int64(56), seq)
}

type testBatchForwarder struct {
	processors []*processor
	failing    atomic.Bool
	failCount  int64
}

func (t *testBatchForwarder) ForwardBatch(batch *ProcessBatch, _ bool, completionFunc func(error)) {
	if t.failing.Load() {
		completionFunc(errors.NewTektiteErrorf(errors.Unavailable, "test inject failure"))
		atomic.AddInt64(&t.failCount, 1)
		return
	}
	t.processors[batch.ProcessorID].IngestBatch(batch, completionFunc)
}

func (t *testBatchForwarder) failFor(dur time.Duration) {
	t.failing.Store(true)
	time.AfterFunc(dur, func() {
		t.failing.Store(false)
	})
}

type forwardingBatchHandler struct {
	receivedBatches  map[int][]*ProcessBatch
	forwardedBatches map[int][]*ProcessBatch

	forwardingProcessorCounts map[int]int // map of receiver to number of processors which forward to it
	requiredCompletions       int

	getRequiredCompletionsCallCount      int64
	getForwardingProcessorCountCallCount int64

	forwardingInfos map[int][]*barrierForwardingInfo

	forwardBatchToOneProcessor bool

	injectableReceivers map[int][]int
}

type barrierForwardingInfo struct {
	forwardReceiverID int
	processorIDs      []int
}

func (g *forwardingBatchHandler) GetForwardingProcessorCount(receiverID int) (int, bool) {
	atomic.AddInt64(&g.getForwardingProcessorCountCallCount, 1)
	cnt, ok := g.forwardingProcessorCounts[receiverID]
	return cnt, ok
}

func (g *forwardingBatchHandler) GetInjectableReceivers(processorID int) []int {
	return g.injectableReceivers[processorID]
}

func (g *forwardingBatchHandler) GetRequiredCompletions() int {
	atomic.AddInt64(&g.getRequiredCompletionsCallCount, 1)
	return g.requiredCompletions
}

func (g *forwardingBatchHandler) GetTerminalReceiverCount() int {
	return 0
}

func newForwardingBatchHandler() *forwardingBatchHandler {
	return &forwardingBatchHandler{
		receivedBatches:     map[int][]*ProcessBatch{},
		forwardedBatches:    map[int][]*ProcessBatch{},
		injectableReceivers: map[int][]int{},
	}
}

func (g *forwardingBatchHandler) HandleProcessBatch(processor Processor, processBatch *ProcessBatch,
	_ bool) (bool, *mem.Batch, []*ProcessBatch, error) {
	if !processBatch.Barrier {
		g.receivedBatches[processBatch.ReceiverID] = append(g.receivedBatches[processBatch.ReceiverID], processBatch)
	}
	if g.forwardingInfos != nil {
		forwardInfos, ok := g.forwardingInfos[processBatch.ReceiverID]
		if ok {
			// Note a stream can forward to multiple receivers, e.g. in the case of child streams which have
			// partitions
			var fBatches []*ProcessBatch
			for _, info := range forwardInfos {
				if processBatch.Barrier {
					for _, forwardProcessorID := range info.processorIDs {
						fBatch := NewBarrierProcessBatch(forwardProcessorID, info.forwardReceiverID,
							processBatch.Version, processBatch.Watermark, processor.ID(), 0)
						fBatches = append(fBatches, fBatch)
					}
				} else {
					if g.forwardBatchToOneProcessor {
						// choose one to forward to randomly
						randIndex := rand.Intn(len(info.processorIDs))
						forwardProcessorID := info.processorIDs[randIndex]
						fBatch := NewProcessBatch(forwardProcessorID, processBatch.EvBatch, info.forwardReceiverID, 0, processor.ID())
						g.forwardedBatches[info.forwardReceiverID] = append(g.forwardedBatches[info.forwardReceiverID], fBatch)
						fBatches = append(fBatches, fBatch)
					} else {
						for _, forwardProcessorID := range info.processorIDs {
							fBatch := NewProcessBatch(forwardProcessorID, processBatch.EvBatch, info.forwardReceiverID, 0, processor.ID())
							g.forwardedBatches[info.forwardReceiverID] = append(g.forwardedBatches[info.forwardReceiverID], fBatch)
							fBatches = append(fBatches, fBatch)
						}
					}
				}
			}
			return true, mem.NewBatch(), fBatches, nil
		}
	}
	return true, mem.NewBatch(), nil, nil
}

func injectBatch(proc Processor, receiverID int, cf func(error)) *ProcessBatch {
	batch := NewProcessBatch(proc.ID(), &evbatch.Batch{}, receiverID, 0, -1)
	proc.IngestBatch(batch, cf)
	return batch
}

func closeVersion(proc Processor, receiverIDs []int, version int, cf func(error)) {
	proc.SubmitAction(func() error {
		err := proc.(*processor).closeVersion(version, receiverIDs)
		cf(err)
		return nil
	})
}

func createProcessor(t *testing.T, id int, batchForwarder BatchForwarder, batchHandler BatchHandler, receiverInfoProvider ReceiverInfoProvider) *processor {
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	pm := &ProcessorManager{}
	pm.cfg = cfg
	procStore := NewProcessorStore(pm, id)
	return NewProcessor(id, cfg, procStore, batchForwarder, batchHandler, receiverInfoProvider, dataKey(), nil).(*processor)
}

func dataKey() []byte {
	return make([]byte, 16)
}

type testForwarder struct {
	receivedBatches []*ProcessBatch
}

func (t *testForwarder) ForwardBatch(batch *ProcessBatch, _ bool, _ func(error)) {
	t.receivedBatches = append(t.receivedBatches, batch)
}

type testBatchHandler struct {
	receivedBatches  []batchInfo
	memBatch         *mem.Batch
	forwardedBatches []*ProcessBatch
	err              error
}

type batchInfo struct {
	processor    Processor
	processBatch *ProcessBatch
	currVersion  int
}

func (t *testBatchHandler) HandleProcessBatch(processor Processor, processBatch *ProcessBatch,
	_ bool) (bool, *mem.Batch, []*ProcessBatch, error) {
	if processBatch.Barrier {
		return false, nil, nil, nil
	}
	t.receivedBatches = append(t.receivedBatches, batchInfo{
		processor:    processor,
		processBatch: processBatch,
	})
	return true, t.memBatch, t.forwardedBatches, t.err
}

type testReceiverInfoProvider struct {
	lock                     sync.Mutex
	requiredCompletions      int
	injectableReceiverIDs    map[int][]int
	forwardingProcessorCount int
}

func (t *testReceiverInfoProvider) SetForwardingProcessorCount(count int) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.forwardingProcessorCount = count
}

func (t *testReceiverInfoProvider) GetForwardingProcessorCount(int) (int, bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.forwardingProcessorCount, true
}

func (t *testReceiverInfoProvider) SetInjectableReceivers(m map[int][]int) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.injectableReceiverIDs = m
}

func (t *testReceiverInfoProvider) GetInjectableReceivers(processorID int) []int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.injectableReceiverIDs[processorID]
}

func (t *testReceiverInfoProvider) SetRequiredCompletions(required int) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.requiredCompletions = required
}

func (t *testReceiverInfoProvider) GetRequiredCompletions() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.requiredCompletions
}

func (t *testReceiverInfoProvider) GetTerminalReceiverCount() int {
	return 0
}

func newTestStore() *testStore {
	return &testStore{data: treemap.NewWithStringComparator()}
}

type testStore struct {
	data *treemap.Map
}

func (t *testStore) Start() {
}

func (t *testStore) Stop() {
}

func (t *testStore) Get(key []byte) ([]byte, error) {
	return t.GetWithMaxVersion(key, math.MaxUint)
}

func (t *testStore) Write(batch *mem.Batch) error {
	batch.Range(func(key []byte, value []byte) bool {
		t.data.Put(string(key), value)
		return true
	})
	return nil
}
func (t *testStore) GetWithMaxVersion(key []byte, maxVersion uint64) ([]byte, error) {
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

func (t *testStore) NewIterator(keyStart []byte, keyEnd []byte, highestVersion uint64, preserveTombstones bool) (iteration.Iterator, error) {
	var entries []common.KV
	iter := t.data.Iterator()
	for iter.Next() {
		key := []byte(iter.Key().(string))
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
	return iteration.NewMergingIterator([]iteration.Iterator{si}, preserveTombstones, highestVersion)
}

type testProcessorManager struct {
}

func (t *testProcessorManager) RegisterStateHandler(stateHandler clustmgr.ClusterStateHandler) {
	panic("not implemented")
}

func (t *testProcessorManager) NodePartitions(mappingID string, partitionCount int) (map[int][]int, error) {
	panic("not implemented")
}

func (t *testProcessorManager) NodeForPartition(partitionID int, mappingID string, partitionCount int) int {
	panic("not implemented")
}

func (t *testProcessorManager) ForwardBatch(batch *ProcessBatch, replicate bool, completionFunc func(error)) {
	panic("not implemented")
}

func (t *testProcessorManager) Start() error {
	panic("not implemented")
}

func (t *testProcessorManager) Stop() error {
	panic("not implemented")
}

func (t *testProcessorManager) RegisterListener(listenerName string, listener ProcessorListener) []Processor {
	panic("not implemented")
}

func (t *testProcessorManager) UnregisterListener(listenerName string) {
	panic("not implemented")
}

func (t *testProcessorManager) SetClusterMessageHandlers(remotingServer remoting.Server, vbHandler *remoting.TeeBlockingClusterMessageHandler) {
	panic("not implemented")
}

func (t *testProcessorManager) MarkGroupAsValid(nodeID int, groupID int, joinedVersion int) (bool, error) {
	panic("not implemented")
}

func (t *testProcessorManager) SetVersionManagerClient(client vmgr.Client) {
	panic("not implemented")
}

func (t *testProcessorManager) HandleVersionBroadcast(currentVersion int, completedVersion int, flushedVersion int) {
	panic("not implemented")
}

func (t *testProcessorManager) GetGroupState(processorID int) (clustmgr.GroupState, bool) {
	panic("not implemented")
}

func (t *testProcessorManager) GetLeaderNode(processorID int) (int, error) {
	panic("not implemented")
}

func (t *testProcessorManager) GetProcessor(processorID int) Processor {
	panic("not implemented")
}

func (t *testProcessorManager) ClusterVersion() int {
	panic("not implemented")
}

func (t *testProcessorManager) IsReadyAsOfVersion(clusterVersion int) bool {
	panic("not implemented")
}

func (t *testProcessorManager) HandleClusterState(cs clustmgr.ClusterState) error {
	panic("not implemented")
}

func (t *testProcessorManager) AfterReceiverChange() {
	panic("not implemented")
}

func (t *testProcessorManager) GetCurrentVersion() int {
	panic("not implemented")
}

func (t *testProcessorManager) PrepareForShutdown() {
	panic("not implemented")
}

func (t *testProcessorManager) AcquiesceLevelManagerProcessor() error {
	panic("not implemented")
}

func (t *testProcessorManager) WaitForProcessingToComplete() {
	panic("not implemented")
}

func (t *testProcessorManager) Freeze() {
	panic("not implemented")
}

func (t *testProcessorManager) FailoverOccurred() bool {
	panic("not implemented")
}

func (t *testProcessorManager) VersionManagerClient() vmgr.Client {
	panic("not implemented")
}

func (t *testProcessorManager) EnsureReplicatorsReady() error {
	panic("not implemented")
}

func (t *testProcessorManager) SetLevelMgrProcessorInitialisedCallback(callback func() error) {
	panic("not implemented")
}

func (t *testProcessorManager) ClearUnflushedData() {
	panic("not implemented")
}

func (t *testProcessorManager) FlushAllProcessors(shutdown bool) error {
	panic("not implemented")
}

func (t *testProcessorManager) GetLastCompletedVersion() int64 {
	panic("not implemented")
}

func (t *testProcessorManager) GetLastFlushedVersion() int64 {
	panic("not implemented")
}
