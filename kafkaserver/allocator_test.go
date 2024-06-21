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

package kafkaserver

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestAllocator(t *testing.T) {
	maxSize := 1000000
	allocator := newDefaultAllocator(maxSize)
	i := 0
	as := &allocateState{}
	totalAllocates := 0
	for totalAllocates < 2*maxSize {
		size := rand.Intn(10000)
		as.totSize += size
		ii := i
		buff, err := allocator.Allocate(size, func() {
			// Make sure callback is not called unless totSize > maxSize
			require.Greater(t, as.totSize, maxSize)
			// Make sure callbacks are called in correct order
			require.Equal(t, as.expectedCallback, ii)
			as.expectedCallback++
			as.totSize -= size
		})
		require.NoError(t, err)
		require.Equal(t, size, len(buff))
		i++
		totalAllocates += size
	}
}

type allocateState struct {
	expectedCallback int
	totSize          int
}
