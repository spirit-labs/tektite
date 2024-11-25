package lsm

import (
	"encoding/binary"
	"github.com/pkg/errors"
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
		expired, err := r.isExpired(curr.Value)
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
	r.iter.Close()
}

func (r *RemoveExpiredEntriesIterator) isExpired(value []byte) (bool, error) {
	if len(value) < 2 {
		// tombstone or marker
		return false, nil
	}
	valueMetaData := common.ReadValueMetadata(value)
	if len(valueMetaData) == 0 {
		return false, nil
	}
	topicID := valueMetaData[0]
	retention, err := r.retentionProvider.GetSlabRetention(int(topicID))
	if err != nil {
		return false, err
	}
	if retention == 0 {
		return false, errors.Errorf("invalid zero retention for topic %v", topicID)
	}
	if retention == -1 {
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
			return false, curr, err
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
	r.iter.Close()
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
