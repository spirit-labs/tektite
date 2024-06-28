// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

func (r *RemoveExpiredEntriesIterator) Current() common.KV {
	return r.iter.Current()
}

func (r *RemoveExpiredEntriesIterator) Next() error {
	return r.iter.Next()
}

func (r *RemoveExpiredEntriesIterator) IsValid() (bool, error) {
	for {
		valid, err := r.iter.IsValid()
		if err != nil {
			return false, err
		}
		if !valid {
			return false, nil
		}
		curr := r.iter.Current()
		expired, err := r.isExpired(curr.Key)
		if err != nil {
			return false, err
		}
		if !expired {
			return true, nil
		}
		if log.DebugEnabled {
			log.Debugf("RemoveExpiredEntriesIterator removed key %v (%s) value %v (%s)", curr.Key, string(curr.Key),
				curr.Value, string(curr.Value))
		}
		err = r.iter.Next()
		if err != nil {
			return false, err
		}
	}
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

func (r *RemoveDeadVersionsIterator) Current() common.KV {
	return r.iter.Current()
}

func (r *RemoveDeadVersionsIterator) Next() error {
	return r.iter.Next()
}

func (r *RemoveDeadVersionsIterator) IsValid() (bool, error) {
	for {
		valid, err := r.iter.IsValid()
		if err != nil {
			return false, err
		}
		if !valid {
			return false, nil
		}
		curr := r.iter.Current()
		dead := r.hasDeadVersion(curr.Key)
		if !dead {
			return true, nil
		}
		if log.DebugEnabled {
			log.Debugf("RemoveDeadVersionsIterator removed key %v (%s) value %v (%s)", curr.Key, string(curr.Key),
				curr.Value, string(curr.Value))
		}
		err = r.iter.Next()
		if err != nil {
			return false, err
		}
	}
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
