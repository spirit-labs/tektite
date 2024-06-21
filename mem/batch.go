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

package mem

import (
	"github.com/spirit-labs/tektite/arenaskl"
	"github.com/spirit-labs/tektite/common"
	"math"
)

func NewBatch() *Batch {
	return NewBatchWithMaxSize(math.MaxInt64)
}

func NewBatchWithMaxSize(maxSize int64) *Batch {
	return &Batch{
		maxSize: maxSize,
		m:       NewLinkedKVMap(),
	}
}

type Batch struct {
	maxSize       int64
	memTableBytes int64
	m             *LinkedKVMap
}

func (b *Batch) AddEntry(kv common.KV) bool {
	diff := arenaskl.MaxEntrySize(int64(len(kv.Key)), int64(len(kv.Value)))
	if b.memTableBytes+diff > b.maxSize {
		return false
	}
	b.m.Put(kv)
	b.memTableBytes += diff
	return true
}

func (b *Batch) Get(key []byte) ([]byte, bool) {
	return b.m.Get(key)
}

func (b *Batch) Len() int {
	return b.m.Len()
}

func (b *Batch) MemTableBytes() int64 {
	return b.memTableBytes
}

func (b *Batch) Range(f func(key []byte, value []byte) bool) {
	b.m.Range(f)
}
