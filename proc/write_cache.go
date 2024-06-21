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

package proc

import (
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/mem"
)

// WriteCache caches writes for a processor and is flushed when a version is complete on a processor. This improves
// performance when a key is updated multiple times in the same version. This can happen with aggregations and the
// table operator.
type WriteCache struct {
	store        storage
	batch        *mem.Batch
	maxSizeBytes int64
	processorID  int
}

type storage interface {
	Write(batch *mem.Batch) error
}

func NewWriteCache(store storage, maxSizeBytes int64, processorID int) *WriteCache {
	return &WriteCache{
		store:        store,
		batch:        mem.NewBatchWithMaxSize(maxSizeBytes),
		maxSizeBytes: maxSizeBytes,
		processorID:  processorID,
	}
}

func (w *WriteCache) Put(kv common.KV) {
	ok := w.batch.AddEntry(kv)
	if !ok {
		// Adding to batch would make it exceed maxSize so we write the batch, then replace it then add it in the new
		// batch
		if err := w.writeToStore(); err != nil {
			panic(err)
		}
		if ok := w.batch.AddEntry(kv); !ok {
			panic("cannot add entry")
		}
	}
}

func (w *WriteCache) Get(key []byte) ([]byte, bool) {
	return w.batch.Get(key)
}

func (w *WriteCache) MaybeWriteToStore() error {
	if w.batch.Len() > 0 {
		return w.writeToStore()
	}
	return nil
}

func (w *WriteCache) writeToStore() error {
	if err := w.store.Write(w.batch); err != nil {
		return err
	}
	w.batch = mem.NewBatchWithMaxSize(w.maxSizeBytes)
	return nil
}

func (w *WriteCache) Clear() {
	log.Debugf("clearing write cache to store for processor %d", w.processorID)
	w.batch = mem.NewBatchWithMaxSize(w.maxSizeBytes)
}
