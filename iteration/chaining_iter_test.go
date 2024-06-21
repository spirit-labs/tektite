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

package iteration

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestChainingIterator(t *testing.T) {
	iter1 := createIter(1, 1, 3, 3, 7, 7, 8, 8, 9, 9)
	iter2 := createIter(12, 12, 13, 13, 17, 17)
	iter3 := createIter(18, 18, 20, 20, 21, 21, 25, 25, 26, 26)
	ci := NewChainingIterator([]Iterator{iter1, iter2, iter3})
	expectEntriesChaining(t, ci, 1, 1, 3, 3, 7, 7, 8, 8, 9, 9, 12, 12, 13, 13, 17, 17, 18, 18, 20, 20, 21, 21, 25, 25, 26, 26)
}

func expectEntriesChaining(t *testing.T, iter Iterator, expected ...int) {
	t.Helper()
	for i := 0; i < len(expected); i++ {
		expKey := expected[i]
		i++
		expVal := expected[i]
		requireIterValid(t, iter, true)
		curr := iter.Current()
		ekey := fmt.Sprintf("key-%010d", expKey)
		key := string(curr.Key[:len(curr.Key)-8])
		require.Equal(t, ekey, key)
		evalue := fmt.Sprintf("value-%010d", expVal)
		require.Equal(t, evalue, string(curr.Value))
		err := iter.Next()
		require.NoError(t, err)
	}
	requireIterValid(t, iter, false)
}
