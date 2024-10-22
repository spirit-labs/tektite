package fetcher

import (
	"fmt"
	"github.com/spirit-labs/tektite/cluster"
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
	lastReceivedSequence   int
	currentLeaderVersion   int
	// We use a separate lock to protect received sequence and leader version so not to contend with fetching table ids
	sequenceLock sync.Mutex
}

func CreatePartitionRecentTables(maxEntriesPerPartition int, bf *BatchFetcher) PartitionRecentTables {
	return PartitionRecentTables{
		bf:                     bf,
		topicMap:               make(map[int]map[int]*PartitionTables),
		maxEntriesPerPartition: maxEntriesPerPartition,
		lastReceivedSequence:   -1,
		currentLeaderVersion:   -1,
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

func (p *PartitionRecentTables) membershipChanged(membership cluster.MembershipState) {
	p.sequenceLock.Lock()
	defer p.sequenceLock.Unlock()
	if p.currentLeaderVersion != -1 && p.currentLeaderVersion != membership.LeaderVersion {
		// If the leader changed them we invalidate any cached table ids
		p.maybeInvalidateRecentTables()
	}
	p.currentLeaderVersion = membership.LeaderVersion
}

func (p *PartitionRecentTables) getLastReceivedSequence() int {
	p.sequenceLock.Lock()
	defer p.sequenceLock.Unlock()
	return p.lastReceivedSequence
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

func (p *PartitionTables) addTableIDs(tableIDs []sst.SSTableID, lastReadableOffset int64,
	fetchStates map[*FetchState]struct{}) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.initialised {
		panic("partition tables not initialised")
	}
	for _, tabID := range tableIDs {
		p.entries = append(p.entries, RecentTableEntry{&tabID, lastReadableOffset})
	}
	extra := len(p.entries) - p.maxEntriesPerPartition
	if extra > 0 {
		p.validFromOffset = p.entries[extra-1].LastReadableOffset + 1
		p.entries = p.entries[extra:]
	}
	// Gather listeners
	for _, pfs := range p.listeners {
		fetchStates[pfs.fs] = struct{}{}
	}
	return nil
}

func (p *PartitionTables) addListener(listener *PartitionFetchState) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.listeners = append(p.listeners, listener)
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

func (p *PartitionRecentTables) handleTableRegisteredNotification(notification *control.TablesRegisteredNotification) error {
	p.sequenceLock.Lock()
	defer p.sequenceLock.Unlock()
	if notification.LeaderVersion != p.currentLeaderVersion {
		log.Warnf("received table registered notification from invalid leader version expected %d actual %d",
			p.currentLeaderVersion, notification.LeaderVersion)
		return nil
	}
	if int(notification.Sequence) != p.lastReceivedSequence+1 {
		// unexpected sequence number
		log.Warn("batch fetcher received out of sequence readable offsets notification. will invalidate")
		p.maybeInvalidateRecentTables()
		return nil
	}
	p.lastReceivedSequence = int(notification.Sequence)
	if notification.TableIDs == nil {
		// Periodic empty notification - this just updates the sequence number - we send them so they will highlight
		// any previous gap in sequence and cause an invalidation
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
				panic(fmt.Sprintf("cannot find partition tables for partition %d", partitionInfo.PartitionID))
			}
			if err := partitionTables.addTableIDs(notification.TableIDs, partitionInfo.Offset, fetchStates); err != nil {
				return err
			}
		}
	}
	for fs, _ := range fetchStates {
		fs.readAsync()
	}
	return nil
}

func (p *PartitionRecentTables) maybeInvalidateRecentTables() {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, partitionTables := range p.topicMap {
		for _, partition := range partitionTables {
			partition.invalidate()
		}
	}
	// Reset to -1, then next good sequence will be zero
	p.lastReceivedSequence = -1
	// Increment reset sequence
	atomic.AddInt64(&p.bf.resetSequence, 1)
}
