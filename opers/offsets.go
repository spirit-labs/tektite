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

package opers

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
)

func loadOffset(partitionHash []byte, slabID int, store store) (int64, error) {
	key := encoding.EncodeEntryPrefix(partitionHash, uint64(slabID), 24)
	value, err := store.Get(key)
	if err != nil {
		return 0, err
	}
	if value == nil {
		return 0, nil
	}
	seq, _ := encoding.ReadUint64FromBufferLE(value, 0)
	return int64(seq), nil
}

func storeOffset(execCtx StreamExecContext, offset int64, partitionHash []byte, slabID int, version int) {
	key := encoding.EncodeEntryPrefix(partitionHash, uint64(slabID), 32)
	key = encoding.EncodeVersion(key, uint64(version))
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(offset))
	execCtx.StoreEntry(common.KV{
		Key:   key,
		Value: value,
	}, false)
}
