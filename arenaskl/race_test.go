/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
 * Modifications copyright (C) 2020 Andy Kimball and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package arenaskl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNodeArenaEnd tests allocating a node at the boundary of an arena. In Go
// 1.14 when the race detector is running, Go will also perform some pointer
// alignment checks. It will detect alignment issues where a node's memory would
// straddle the arena boundary, with unused regions of the node struct dipping
// into unallocated memory. This test is only run when the race build tag is
// provided.
func TestNodeArenaEnd(t *testing.T) {
	// Rather than hardcode an arena size at just the right size, try
	// allocating using successively larger arena sizes until we allocate
	// successfully. The prior attempt will have exercised the right code
	// path.
	for i := uint32(1); i < 256; i++ {
		a := NewArena(i)
		_, err := newNode(a, 1)
		if err == nil {
			// We reached an arena size big enough to allocate a node. If
			// there's an issue at the boundary, the race detector would have
			// found it by now.
			t.Log(i)
			break
		}
		require.Equal(t, ErrArenaFull, err)
	}
}
