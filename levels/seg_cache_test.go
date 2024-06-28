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
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAddLoadDeletePreFlush(t *testing.T) {
	segCache := newSegmentCache(1000)
	segMap := map[string]*segment{}
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("segment-%d", i)
		seg := &segment{}
		segMap[id] = seg
		segCache.put(id, seg)
	}
	for id, seg := range segMap {
		segReceived := segCache.get(id)
		require.NotNil(t, segReceived)
		require.Equal(t, seg, segReceived)
	}
	for id := range segMap {
		segCache.delete(id)
	}
	for id := range segMap {
		segReceived := segCache.get(id)
		require.Nil(t, segReceived)
	}
}

func TestAddLoadDeletePreFlushZeroSizeCache(t *testing.T) {
	segCache := newSegmentCache(0)
	segMap := map[string]*segment{}
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("segment-%d", i)
		seg := &segment{}
		segMap[id] = seg
		segCache.put(id, seg)
	}
	for id, seg := range segMap {
		segReceived := segCache.get(id)
		require.NotNil(t, segReceived)
		require.Equal(t, seg, segReceived)
	}
	for id := range segMap {
		segCache.delete(id)
	}
	for id := range segMap {
		segReceived := segCache.get(id)
		require.Nil(t, segReceived)
	}
}

func TestAddLoadDeletePostFlush(t *testing.T) {
	segCache := newSegmentCache(1000)
	segMap := map[string]*segment{}
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("segment-%d", i)
		seg := &segment{}
		segMap[id] = seg
		segCache.put(id, seg)
	}
	segCache.flush()
	for id, seg := range segMap {
		segReceived := segCache.get(id)
		require.NotNil(t, segReceived)
		require.Equal(t, seg, segReceived)
	}
	for id := range segMap {
		segCache.delete(id)
	}
	for id := range segMap {
		segReceived := segCache.get(id)
		require.Nil(t, segReceived)
	}
}

func TestNoEvictionPreFlush(t *testing.T) {
	segCache := newSegmentCache(5)
	segMap := map[string]*segment{}
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("segment-%d", i)
		seg := &segment{}
		segMap[id] = seg
		segCache.put(id, seg)
	}
	for id, seg := range segMap {
		segReceived := segCache.get(id)
		require.NotNil(t, segReceived)
		require.Equal(t, seg, segReceived)
	}
}

func TestEvictionPostFlush(t *testing.T) {
	segCache := newSegmentCache(5)
	segMap := map[string]*segment{}
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("segment-%d", i)
		seg := &segment{}
		segMap[id] = seg
		segCache.put(id, seg)
	}
	segCache.flush()
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("segment-%d", i)
		segReceived := segCache.get(id)
		if i < 5 {
			require.Nil(t, segReceived)
		} else {
			require.NotNil(t, segReceived)
		}
	}
}
