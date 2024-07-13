package levels

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	iteration2 "github.com/spirit-labs/tektite/iteration"
	log "github.com/spirit-labs/tektite/logger"
	"math"
	"time"
)

// RemoveExpiredEntriesIterator filters out any keys which have expired due to retention time being exceeded
type RemoveExpiredEntriesIterator struct {
	iter              iteration2.Iterator
	sstCreationTime   uint64
	now               uint64
	retentionProvider RetentionProvider
}

type RetentionProvider interface {
	GetSlabRetention(slabID int) (time.Duration, error)
}

func NewRemoveExpiredEntriesIterator(iter iteration2.Iterator, sstCreationTime uint64, now uint64,
	retentionProvider RetentionProvider) *RemoveExpiredEntriesIterator {
	return &RemoveExpiredEntriesIterator{
		iter:              iter,
		sstCreationTime:   sstCreationTime,
		now:               now,
		retentionProvider: retentionProvider,
	}
}

func (r *RemoveExpiredEntriesIterator) Next() (bool, common.KV, error) {
	for {
		valid, curr, err := r.iter.Next()
		if err != nil || !valid {
			return false, curr, err
		}
		expired, err := r.isExpired(curr.Key)
		if err != nil {
			return false, common.KV{}, err
		}
		if !expired {
			return true, curr, nil
		}
		if log.DebugEnabled {
			log.Debugf("RemoveExpiredEntriesIterator removed key %v (%s) value %v (%s)", curr.Key, string(curr.Key),
				curr.Value, string(curr.Value))
		}
	}
}

func (r *RemoveExpiredEntriesIterator) Current() common.KV {
	return r.iter.Current()
}

func (r *RemoveExpiredEntriesIterator) Close() {
}

func (r *RemoveExpiredEntriesIterator) isExpired(key []byte) (bool, error) {
	slabID := int(binary.BigEndian.Uint64(key[16:]))
	retention, err := r.retentionProvider.GetSlabRetention(slabID)
	if err != nil {
		return false, err
	}
	if retention == 0 {
		return false, nil
	}
	expired := r.sstCreationTime+uint64(retention.Milliseconds()) <= r.now
	return expired, nil
}

// RemoveDeadVersionsIterator filters out any dead version ranges
type RemoveDeadVersionsIterator struct {
	iter              iteration2.Iterator
	deadVersionRanges []VersionRange
}

func NewRemoveDeadVersionsIterator(iter iteration2.Iterator, deadVersionRanges []VersionRange) *RemoveDeadVersionsIterator {
	return &RemoveDeadVersionsIterator{
		iter:              iter,
		deadVersionRanges: deadVersionRanges,
	}
}

func (r *RemoveDeadVersionsIterator) Next() (bool, common.KV, error) {
	for {
		valid, curr, err := r.iter.Next()
		if err != nil || !valid {
			return valid, curr, err
		}
		dead := r.hasDeadVersion(curr.Key)
		if !dead {
			return true, curr, nil
		}
		if log.DebugEnabled {
			log.Debugf("RemoveDeadVersionsIterator removed key %v (%s) value %v (%s)", curr.Key, string(curr.Key),
				curr.Value, string(curr.Value))
		}
	}
}

func (r *RemoveDeadVersionsIterator) Current() common.KV {
	return r.iter.Current()
}

func (r *RemoveDeadVersionsIterator) Close() {
}

func (r *RemoveDeadVersionsIterator) hasDeadVersion(key []byte) bool {
	ver := math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-8:])
	for _, versionRange := range r.deadVersionRanges {
		if ver >= versionRange.VersionStart && ver <= versionRange.VersionEnd {
			return true
		}
	}
	return false
}
