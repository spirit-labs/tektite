package fetcher

import (
	"github.com/spirit-labs/tektite/sst"
	"sync"
)

type PartitionRecentTables struct {
	lock                   sync.RWMutex
	topicMap               map[int]map[int]*PartitionTables
	maxEntriesPerPartition int
}

func CreatePartitionRecentTables(maxEntriesPerPartition int) PartitionRecentTables {
	return PartitionRecentTables{
		topicMap:               make(map[int]map[int]*PartitionTables),
		maxEntriesPerPartition: maxEntriesPerPartition,
	}
}

type PartitionTables struct {
	lock                   sync.RWMutex
	maxEntriesPerPartition int
	entries                []RecentTableEntry
	listeners              []*PartitionFetchState
}

func (p *PartitionRecentTables) getPartitionTables(topicID int, partitionID int) *PartitionTables {
	p.lock.RLock()
	defer p.lock.RUnlock()
	partitionMap, ok := p.topicMap[topicID]
	if !ok {
		partitionMap = p.createPartitionMap(topicID)
	}
	partitionTables, ok := partitionMap[partitionID]
	if !ok {
		partitionTables = p.createPartitionTables(partitionID, partitionMap)
	}
	return partitionTables
}

func (p *PartitionTables) getCacheTables(fetchOffset int64) ([]*sst.SSTableID, int64) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if len(p.entries) == 0 {
		return nil, 0
	}
	// We find the rightmost table where the lastReadableOffset < fetchOffset
	// Then we know that any table to the left of that must contain only offsets < fetchOffset and we have all the
	// table ids we need for the iterator.
	// If we can't find any such table it means all tables lastReadableOffset must be >= fetchOffset and we don't have
	// enough tables cached to return the data
	pos := -1
	for i := len(p.entries) - 1; i >= 0; i-- {
		if p.entries[i].LastReadableOffset >= fetchOffset {
			continue
		}
		pos = i
		break
	}
	if pos == -1 {
		return nil, 0
	}
	tabIDs := make([]*sst.SSTableID, len(p.entries)-pos)
	for i := pos; i < len(p.entries); i++ {
		tabIDs[i-pos] = p.entries[i].TableID
	}
	return tabIDs, p.entries[len(p.entries)-1].LastReadableOffset
}

func (p *PartitionTables) addEntry(tableID *sst.SSTableID, lastReadableOffset int64, fetchStates map[*FetchState]struct{}) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.entries = append(p.entries, RecentTableEntry{tableID, lastReadableOffset})
	if len(p.entries) == p.maxEntriesPerPartition {
		// remove the first one.
		p.entries = p.entries[1:]
	}
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

type RecentTableEntry struct {
	TableID            *sst.SSTableID
	LastReadableOffset int64
}

type TableRegisteredNotification struct {
	ID                       sst.SSTableID
	PartitionReadableOffsets map[int]map[int]int64
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
			// that the agent was already registered for updates. so we need to update the partition fetch state's
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

func (p *PartitionRecentTables) unregisterPartitionStates(partitionStates map[int]map[int]*PartitionFetchState) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	for topicID, partitionFetchStates := range partitionStates {
		partitionMap, ok := p.topicMap[topicID]
		if !ok {
			continue
		}
		for partitionID, partitionFetchState := range partitionFetchStates {
			partitionTables, ok := partitionMap[partitionID]
			if !ok {
				continue
			}
			partitionTables.removeListener(partitionFetchState)
		}
	}
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

func (p *PartitionRecentTables) handleTableRegisteredNotification(notif TableRegisteredNotification) error {
	p.lock.RLock()
	defer p.lock.RUnlock()
	// Get unique set of fetch states to read
	fetchStates := map[*FetchState]struct{}{}
	for topicID, topicNotif := range notif.PartitionReadableOffsets {
		partitionMap, ok := p.topicMap[topicID]
		if !ok {
			// Even though we register write before waiting, there is a race with the notification coming in after we
			// called to fetch on the controller, and a new table gets registered really quickly, so we need to create
			// here if necessary
			partitionMap = p.createPartitionMap(topicID)
		}
		for partitionID, offset := range topicNotif {
			partitionTables, ok := partitionMap[partitionID]
			if !ok {
				partitionTables = p.createPartitionTables(partitionID, partitionMap)
			}
			if err := partitionTables.addEntry(&notif.ID, offset, fetchStates); err != nil {
				return err
			}
		}
	}
	for fs := range fetchStates {
		fs.readAsync()
	}
	return nil
}
