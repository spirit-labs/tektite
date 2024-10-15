package fetcher

import (
	"github.com/spirit-labs/tektite/control"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/sst"
	"sync"
	"sync/atomic"
)

type PartitionRecentTables struct {
	bf                     *BatchFetcher
	lock                   sync.RWMutex
	topicMap               map[int]map[int]*PartitionTables
	maxEntriesPerPartition int
	lastReceivedSequence   int64
}

func CreatePartitionRecentTables(maxEntriesPerPartition int, bf *BatchFetcher) PartitionRecentTables {
	return PartitionRecentTables{
		bf:                     bf,
		topicMap:               make(map[int]map[int]*PartitionTables),
		maxEntriesPerPartition: maxEntriesPerPartition,
		lastReceivedSequence:   -1,
	}
}

type PartitionTables struct {
	lock                   sync.RWMutex
	maxEntriesPerPartition int
	entries                []RecentTableEntry
	listeners              []*PartitionFetchState
	initialised            bool
	validFromOffset        int64
}

func (p *PartitionRecentTables) getPartitionMap(topicID int) map[int]*PartitionTables {
	p.lock.RLock()
	defer p.lock.RUnlock()
	partitionMap, ok := p.topicMap[topicID]
	if !ok {
		partitionMap = p.createPartitionMap(topicID)
	}
	return partitionMap
}

func (p *PartitionRecentTables) getPartitionTables(partitionMap map[int]*PartitionTables, partitionID int) *PartitionTables {
	p.lock.RLock()
	defer p.lock.RUnlock()
	partitionTables, ok := partitionMap[partitionID]
	if !ok {
		partitionTables = p.createPartitionTables(partitionID, partitionMap)
	}
	return partitionTables
}

func (p *PartitionTables) initialise(queryFunc func() (int64, error)) (int64, bool, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.initialised {
		return 0, true, nil
	}
	// We execute the registration and get the initial lastReadableOffset via a function passed in here. The function is
	// executed with the lock held which handles a race where a new notification comes in very quickly and updates
	// partitionTables before we have fully initialised.
	lro, err := queryFunc()
	if err != nil {
		return 0, false, err
	}
	p.validFromOffset = lro + 1
	p.initialised = true
	return lro, false, nil
}

func (p *PartitionTables) maybeGetRecentTableIDs(fetchOffset int64) (tables []*sst.SSTableID, lastReadableOffset int64,
	initialised bool, isInCachedRange bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if !p.initialised {
		return nil, 0, false, false
	}
	if len(p.entries) == 0 {
		lastReadableOffset = p.validFromOffset - 1
	} else {
		lastReadableOffset = p.entries[len(p.entries)-1].LastReadableOffset
	}
	if fetchOffset < p.validFromOffset {
		// We are trying to fetch from an offset before the first offset we are caching tables from
		return nil, lastReadableOffset, true, false
	}
	// Screen out any ids which can't contain any data from >= fetchOffset
	var tabIDs []*sst.SSTableID
	for _, entry := range p.entries {
		if entry.LastReadableOffset < fetchOffset {
			continue
		}
		tabIDs = append(tabIDs, entry.TableID)
	}
	return tabIDs, lastReadableOffset, true, true
}

func (p *PartitionTables) isInitialised() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.initialised
}

func (p *PartitionTables) addEntry(tableID *sst.SSTableID, lastReadableOffset int64,
	fetchStates map[*FetchState]struct{}) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.initialised {
		panic("partition tables not initialised")
	}
	if len(p.entries) == p.maxEntriesPerPartition {
		// remove the first one before adding
		p.validFromOffset = p.entries[0].LastReadableOffset + 1
		p.entries = p.entries[1:]
	}
	p.entries = append(p.entries, RecentTableEntry{tableID, lastReadableOffset})
	// Call listeners
	for _, listener := range p.listeners {
		fs, err := listener.updateIterator(p.entries)
		if err != nil {
			return err
		}
		if fs != nil {
			fetchStates[fs] = struct{}{}
		}
	}
	return nil
}

func (p *PartitionTables) addListener(listener *PartitionFetchState) (*FetchState, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, entry := range p.listeners {
		if entry == listener {
			panic("listener already registered")
		}
	}
	p.listeners = append(p.listeners, listener)
	if len(p.entries) > 0 {
		// data might have been added between us executing the controller query and registering listeners in the case
		// that the agent was already registered for updates. so we need to update the partition fetch state's
		// iterator
		return listener.updateIterator(p.entries)
	}
	return nil, nil
}

func (p *PartitionTables) removeListener(listener *PartitionFetchState) {
	p.lock.Lock()
	defer p.lock.Unlock()
	var newListeners []*PartitionFetchState
	for _, entry := range p.listeners {
		if entry != listener {
			newListeners = append(newListeners, entry)
		}
	}
	p.listeners = newListeners
}

func (p *PartitionTables) invalidate() {
	p.lock.Lock()
	defer p.lock.Unlock()
	// Clear the cache entries
	p.entries = nil
	// Clear the listeners. Any waiting fetches will timeout and return
	p.listeners = nil
	// Set as initialised - this will cause next fetch to call to the controller with incremented reset sequence which
	// will cause it to invalidate all listeners for the agent
	p.initialised = false
	p.validFromOffset = -1
}

type RecentTableEntry struct {
	TableID            *sst.SSTableID
	LastReadableOffset int64
}

func (p *PartitionRecentTables) registerPartitionStates(partitionStates map[int]map[int]*PartitionFetchState) error {
	p.lock.RLock()
	defer p.lock.RUnlock()
	fetchStates := map[*FetchState]struct{}{}
	for topicID, partitionFetchStates := range partitionStates {
		partitionMap, ok := p.topicMap[topicID]
		if !ok {
			partitionMap = p.createPartitionMap(topicID)
		}
		for partitionID, partitionFetchState := range partitionFetchStates {
			partitionTables, ok := partitionMap[partitionID]
			if !ok {
				partitionTables = p.createPartitionTables(partitionID, partitionMap)
			}
			// data might have been added between us executing the controller query and registering listeners in the case
			// that the agent was already initialised. so we need to update the partition fetch state's
			// iterator and collect a unique set of fetch states so we can call read on them again
			fs, err := partitionTables.addListener(partitionFetchState)
			if err != nil {
				return err
			}
			if fs != nil {
				fetchStates[fs] = struct{}{}
			}
		}
	}
	for fs := range fetchStates {
		fs.readAsync()
	}
	return nil
}

func (p *PartitionRecentTables) createPartitionMap(topicID int) map[int]*PartitionTables {
	p.lock.RUnlock()
	p.lock.Lock()
	defer func() {
		p.lock.Unlock()
		p.lock.RLock()
	}()
	partitionMap, ok := p.topicMap[topicID]
	if ok {
		return partitionMap
	}
	partitionMap = map[int]*PartitionTables{}
	p.topicMap[topicID] = partitionMap
	return partitionMap
}

func (p *PartitionRecentTables) createPartitionTables(partitionID int, partitionMap map[int]*PartitionTables) *PartitionTables {
	p.lock.RUnlock()
	p.lock.Lock()
	defer func() {
		p.lock.Unlock()
		p.lock.RLock()
	}()
	partitionTables, ok := partitionMap[partitionID]
	if ok {
		return partitionTables
	}
	partitionTables = &PartitionTables{
		maxEntriesPerPartition: p.maxEntriesPerPartition,
	}
	partitionMap[partitionID] = partitionTables
	return partitionTables
}

func (p *PartitionRecentTables) handleTableRegisteredNotification(notification control.TableRegisteredNotification) error {
	// check sequence number
	if !atomic.CompareAndSwapInt64(&p.lastReceivedSequence, notification.Sequence-1, notification.Sequence) {
		// unexpected sequence number
		p.maybeInvalidateRecentTables()
		return nil
	}
	if notification.ID == nil {
		// Periodic empty notification - this just updates the sequence number and received time
		return nil
	}
	p.lock.RLock()
	defer p.lock.RUnlock()

	// Get unique set of fetch states to read
	fetchStates := map[*FetchState]struct{}{}
	for _, topicNotification := range notification.Infos {
		partitionMap, ok := p.topicMap[topicNotification.TopicID]
		if !ok {
			panic("cannot find topic map")
		}
		for _, partitionInfo := range topicNotification.PartitionInfos {
			partitionTables, ok := partitionMap[partitionInfo.PartitionID]
			if !ok {
				panic("cannot find partition tables")
			}
			if err := partitionTables.addEntry(&notification.ID, partitionInfo.LastReadableOffset, fetchStates); err != nil {
				return err
			}
		}
	}
	for fs := range fetchStates {
		fs.readAsync()
	}
	return nil
}

func (p *PartitionRecentTables) maybeInvalidateRecentTables() {
	p.lock.Lock()
	defer p.lock.Unlock()
	currSequence := atomic.LoadInt64(&p.lastReceivedSequence)
	if currSequence == -1 {
		// Ignore - we received another invalid sequence while we were waiting
		return
	}
	log.Warn("batch fetcher received out of sequence readable offsets notification. will invalidate")
	for _, partitionTables := range p.topicMap {
		for _, partition := range partitionTables {
			partition.invalidate()
		}
	}
	// Reset to -1, then next good sequence will be zero
	atomic.StoreInt64(&p.lastReceivedSequence, -1)
	// Increment reset sequence
	atomic.AddInt64(&p.bf.resetSequence, 1)
}
