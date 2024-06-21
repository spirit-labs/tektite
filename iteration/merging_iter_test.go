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
	"github.com/spirit-labs/tektite/encoding"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestMergingIteratorNoDups(t *testing.T) {
	iter1 := createIter(2, 2, 5, 5, 9, 9, 11, 11, 12, 12)
	iter2 := createIter(0, 0, 3, 3, 7, 7, 10, 10, 13, 13)
	iter3 := createIter(4, 4, 6, 6, 8, 8, 14, 14, 18, 18)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 0)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13, 14, 14, 18, 18)
}

func TestMergingIteratorDupKeys(t *testing.T) {
	iter1 := createIter(2, 20, 5, 50, 9, 90, 11, 110, 12, 120)
	iter2 := createIter(0, 0, 3, 300, 5, 500, 13, 1300, 14, 1400)
	iter3 := createIter(4, 4000, 5, 5000, 8, 8000, 14, 14000, 18, 18000)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 0)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 2, 20, 3, 300, 4, 4000, 5, 50, 8, 8000, 9, 90, 11, 110, 12, 120, 13, 1300, 14, 1400, 18, 18000)
}

func TestMergingIteratorTombstonesDoNotPreserve(t *testing.T) {
	iter1 := createIter(2, -1, 5, -1, 9, 90, 11, -1, 12, 120)
	iter2 := createIter(0, 0, 3, 300, 5, 500, 13, 1300, 14, -1)
	iter3 := createIter(4, -1, 5, 5000, 8, 8000, 14, 14000, 18, 18000)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 0)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 3, 300, 8, 8000, 9, 90, 12, 120, 13, 1300, 18, 18000)
}

func TestMergingIteratorTombstonesPreserve(t *testing.T) {
	iter1 := createIter(2, -1, 5, -1, 9, 90, 11, -1, 12, 120)
	iter2 := createIter(0, 0, 3, 300, 5, 500, 13, 1300, 14, -1)
	iter3 := createIter(4, -1, 5, 5000, 8, 8000, 14, 14000, 18, 18000)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, true, 0)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 2, -1, 3, 300, 4, -1, 5, -1, 8, 8000, 9, 90, 11, -1, 12, 120, 13, 1300, 14, -1, 18, 18000)
}

func TestMergingIteratorPutThenTombstonesLater(t *testing.T) {
	// Tests the case where there's a put, then a delete, then a put in iterators
	iter1 := createIter(2, 20, 5, 50, 9, 90, 11, 110, 12, 120)
	iter2 := createIter(0, 0, 4, -1, 5, 500, 9, -1, 13, 1300, 14, -1)
	iter3 := createIter(4, 4000, 5, 5000, 8, 8000, 9, 99, 14, 14000, 18, 18000)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 0)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 2, 20, 5, 50, 8, 8000, 9, 90, 11, 110, 12, 120, 13, 1300, 18, 18000)
}

func TestMergingIteratorTombstonesAfterNonTombstoneEntry(t *testing.T) {
	// Tests the case where there is a non tombstone before a tombstone in the iterators (9, 90 before 4, -1)
	iter1 := createIter(9, 90)
	iter2 := createIter(4, -1, 5, 500)
	iter3 := createIter(4, 4000, 5, 5000, 8, 8000)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 0)
	require.NoError(t, err)
	expectEntries(t, mi, 5, 500, 8, 8000, 9, 90)
}

func TestMergingIteratorOneIterator(t *testing.T) {
	iter1 := createIter(2, 20, 5, 50, 9, 90, 11, 110, 12, 120)
	iters := []Iterator{iter1}
	mi, err := NewMergingIterator(iters, false, 0)
	require.NoError(t, err)
	expectEntries(t, mi, 2, 20, 5, 50, 9, 90, 11, 110, 12, 120)
}

func TestMergingIteratorTwoIteratorsOneSmall(t *testing.T) {
	iter1 := createIter(1, 1, 9, 9)
	iter2 := createIter(2, 20, 5, 50, 9, 90, 11, 110, 12, 120)
	iters := []Iterator{iter1, iter2}
	mi, err := NewMergingIterator(iters, false, 0)
	require.NoError(t, err)
	expectEntries(t, mi, 1, 1, 2, 20, 5, 50, 9, 9, 11, 110, 12, 120)
}

func TestMergingIteratorValidReturnChanges(t *testing.T) {
	// We need to support iterators changing the result of IsValid() from false to true on
	// subsequent calls. E.g. this can happen with MemTableIterator as new data arrives between the two calls
	iter1 := createIter(5, 50)
	iter2 := createIter(2, 20)
	iter3 := createIter(3, 30)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 0)
	require.NoError(t, err)

	requireIterValid(t, mi, true)
	expectEntry(t, mi, 2, 20)
	err = mi.Next()
	require.NoError(t, err)
	requireIterValid(t, mi, true)
	expectEntry(t, mi, 3, 30)

	// Force iter1 to return not valid
	iter1.SetValidOverride(false)

	err = mi.Next()
	require.NoError(t, err)
	requireIterValid(t, mi, false)

	// Make it valid again
	iter1.UnsetValidOverride()
	requireIterValid(t, mi, true)
	expectEntry(t, mi, 5, 50)

	err = mi.Next()
	require.NoError(t, err)
	requireIterValid(t, mi, false)
}

func TestMergingIteratorValidReturnChangesWithTombstone(t *testing.T) {
	// We need to support iterators changing the rersult of IsValid() from false to true on
	// subsequent calls. E.g. this can happen with MemTableIterator as new data arrives between the two calls
	// Here iterator changes from not valid to valid and reveals a tombstone which deletes an entry from a later iterator
	iter1 := createIter(4, -1)
	iter2 := createIter(2, 20)
	iter3 := createIter(3, 30, 4, 40)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 0)
	require.NoError(t, err)

	requireIterValid(t, mi, true)
	expectEntry(t, mi, 2, 20)
	err = mi.Next()
	require.NoError(t, err)
	requireIterValid(t, mi, true)
	expectEntry(t, mi, 3, 30)

	// Force iter1 to return not valid
	iter1.SetValidOverride(false)

	err = mi.Next()
	require.NoError(t, err)
	requireIterValid(t, mi, true)

	// Make it valid again
	iter1.UnsetValidOverride()
	requireIterValid(t, mi, false)
}

func TestMergingIteratorIsValidCurrDoesntAdvanceCursor(t *testing.T) {
	// Make sure that calling IsValid() or Curr() multiple times doesn't advance the cursor
	iter1 := createIter(2, 2)
	iter2 := createIter(0, 0)
	iter3 := createIter(4, 4)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 0)
	require.NoError(t, err)

	requireIterValid(t, mi, true)
	requireIterValid(t, mi, true)
	expectEntry(t, mi, 0, 0)
	expectEntry(t, mi, 0, 0)
	err = mi.Next()
	require.NoError(t, err)

	requireIterValid(t, mi, true)
	requireIterValid(t, mi, true)
	expectEntry(t, mi, 2, 2)
	expectEntry(t, mi, 2, 2)
	err = mi.Next()
	require.NoError(t, err)

	requireIterValid(t, mi, true)
	requireIterValid(t, mi, true)
	expectEntry(t, mi, 4, 4)
	expectEntry(t, mi, 4, 4)
	err = mi.Next()
	require.NoError(t, err)

	requireIterValid(t, mi, false)
	requireIterValid(t, mi, false)
}

func TestMergingIteratorWithVersionAllSameVersionEqualHighestVersion(t *testing.T) {
	iter1 := createIterWithVersions(0, 0, 7, 1, 10, 7, 2, 20, 7)
	iter2 := createIterWithVersions(0, 1, 7, 1, 11, 7, 3, 30, 7)
	iter3 := createIterWithVersions(2, 21, 7, 4, 40, 7, 5, 50, 7)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 7)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 1, 10, 2, 20, 3, 30, 4, 40, 5, 50)
}

func TestMergingIteratorWithVersionAllSameVersionLessThanHighestVersion(t *testing.T) {
	iter1 := createIterWithVersions(0, 0, 7, 1, 10, 7, 2, 20, 7)
	iter2 := createIterWithVersions(0, 1, 7, 1, 11, 7, 3, 30, 7)
	iter3 := createIterWithVersions(2, 21, 7, 4, 40, 7, 5, 50, 7)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 8)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 1, 10, 2, 20, 3, 30, 4, 40, 5, 50)
}

func TestMergingIteratorWithVersionAllSameVersionLowerThanHighestVersion(t *testing.T) {
	iter1 := createIterWithVersions(0, 0, 7, 1, 10, 7, 2, 20, 7)
	iter2 := createIterWithVersions(0, 1, 7, 1, 11, 7, 3, 30, 7)
	iter3 := createIterWithVersions(2, 21, 7, 4, 40, 7, 5, 50, 7)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 6)
	require.NoError(t, err)
	requireIterValid(t, mi, false)
}

func TestMergingIteratorWithVersionScreenOutDups(t *testing.T) {
	iter1 := createIterWithVersions(0, 0, 7, 1, 10, 8, 2, 20, 8, 5, 50, 7)
	iter2 := createIterWithVersions(0, 1, 7, 1, 11, 7, 3, 30, 7)
	iter3 := createIterWithVersions(2, 21, 7, 4, 40, 7, 5, 51, 8)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 7)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 1, 11, 2, 21, 3, 30, 4, 40, 5, 50)
}

func TestMergingIteratorWithVersionTombstones(t *testing.T) {
	iter1 := createIterWithVersions(0, 0, 7, 1, -1, 7, 2, -1, 7, 5, 50, 7)
	iter2 := createIterWithVersions(0, -1, 7, 1, 11, 7, 3, 30, 7)
	iter3 := createIterWithVersions(2, 21, 7, 4, 40, 7, 5, -1, 7)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 7)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 3, 30, 4, 40, 5, 50)
}

func TestMergingIteratorWithVersionTombstonesScreenOut1(t *testing.T) {
	iter1 := createIterWithVersions(0, 0, 7, 1, -1, 7, 2, -1, 7, 5, 50, 7)
	iter2 := createIterWithVersions(0, -1, 8, 1, 11, 8, 3, 30, 7)
	iter3 := createIterWithVersions(2, 21, 8, 4, 40, 7, 5, -1, 8)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 7)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 3, 30, 4, 40, 5, 50)
}

func TestMergingIteratorWithVersionTombstonesScreenOut2(t *testing.T) {
	iter1 := createIterWithVersions(0, 0, 8, 1, -1, 8, 2, -1, 8, 5, 50, 8)
	iter2 := createIterWithVersions(0, -1, 7, 1, 11, 7, 3, 30, 8)
	iter3 := createIterWithVersions(2, 21, 7, 4, 40, 8, 5, -1, 7)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 7)
	require.NoError(t, err)
	expectEntries(t, mi, 1, 11, 2, 21)
}

func TestMergingIteratorWithVersion(t *testing.T) {
	iter1 := createIterWithVersions(2, 2, 1, 3, 3, 2)
	iter2 := createIterWithVersions(0, 0, 1, 4, 4, 2)
	iter3 := createIterWithVersions(4, 4, 1, 5, 5, 3)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 1)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 2, 2, 4, 4)
}

func TestMergingIteratorSameKeysDifferentVersions1(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 1, 1, 11, 1, 2, 12, 3)
	iter2 := createIterWithVersions(0, 20, 3, 1, 21, 2, 2, 22, 2)
	iter3 := createIterWithVersions(0, 30, 2, 1, 31, 3, 2, 32, 1)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 3)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 20, 1, 31, 2, 12)
}

func TestMergingIteratorSameKeysDifferentVersions2(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 1, 1, 11, 1, 2, 12, 3)
	iter2 := createIterWithVersions(0, 20, 3, 1, 21, 2, 2, 22, 3)
	iter3 := createIterWithVersions(0, 30, 3, 1, 31, 2, 2, 32, 1)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 3)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 20, 1, 21, 2, 12)
}

func TestMergingIteratorSameKeysDifferentVersionsHighestVersion(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 1, 1, 11, 1, 2, 12, 3)
	iter2 := createIterWithVersions(0, 20, 3, 1, 21, 2, 2, 22, 2)
	iter3 := createIterWithVersions(0, 30, 2, 1, 31, 3, 2, 32, 1)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 2)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 30, 1, 21, 2, 22)
}

func TestMergingIteratorSameKeysSameVersions(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 1, 1, 11, 2, 2, 12, 3)
	iter2 := createIterWithVersions(0, 20, 1, 1, 21, 2, 2, 22, 3)
	iter3 := createIterWithVersions(0, 30, 1, 1, 31, 2, 2, 32, 3)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 3)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 10, 1, 11, 2, 12)
}

func TestMergingIteratorSameKeysSameVersionsHighestVersion(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 1, 1, 11, 2, 2, 12, 3)
	iter2 := createIterWithVersions(0, 20, 1, 1, 21, 2, 2, 22, 3)
	iter3 := createIterWithVersions(0, 30, 1, 1, 31, 2, 2, 32, 3)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false, 2)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 10, 1, 11)
}

func TestMergingIteratorSameKeysDifferentVersionsSingleIterator(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 3, 0, 11, 2, 0, 12, 1)
	iters := []Iterator{iter1}
	mi, err := NewMergingIterator(iters, false, 3)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 10)
}

func TestMergingIteratorSameKeysSameVersionsSingleIterator(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 1, 0, 11, 1, 0, 12, 1)
	iters := []Iterator{iter1}
	mi, err := NewMergingIterator(iters, false, 3)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 10)
}

func TestCompactionMergingIteratorDontCompactIfNonCompactable(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 3, 1, 11, 4, 2, 12, 7, 3, 13, 11)
	iter2 := createIterWithVersions(0, 20, 1, 1, 21, 6, 2, 22, 8, 3, 23, 10)
	iter3 := createIterWithVersions(0, 30, 2, 1, 31, 5, 2, 32, 9, 3, 33, 12)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewCompactionMergingIterator(iters, false, 7)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 10, 1, 21, 2, 32, 2, 22, 2, 12, 3, 33, 3, 13, 3, 23)
}

func TestCompactionMergingIteratorCompactWhenSameKeySameVersionEveIfNonCompactable(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 3, 1, 11, 4, 2, 12, 7, 3, 13, 10)
	iter2 := createIterWithVersions(0, 20, 1, 1, 21, 6, 2, 22, 8, 3, 23, 10)
	iter3 := createIterWithVersions(0, 30, 2, 1, 31, 5, 2, 32, 8, 3, 33, 12)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewCompactionMergingIterator(iters, false, 7)
	require.NoError(t, err)
	// When key and version is same, the one in the leftmost iterator is chosen
	expectEntries(t, mi, 0, 10, 1, 21, 2, 22, 2, 12, 3, 33, 3, 13)
}

func TestCompactionMergingIteratorAllCompactable(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 3, 1, 11, 4, 2, 12, 7, 3, 13, 11)
	iter2 := createIterWithVersions(0, 20, 1, 1, 21, 6, 2, 22, 8, 3, 23, 10)
	iter3 := createIterWithVersions(0, 30, 2, 1, 31, 5, 2, 32, 9, 3, 33, 12)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewCompactionMergingIterator(iters, false, 100)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 10, 1, 21, 2, 32, 3, 33)
}

func TestCompactionMergingIteratorSkipPastCompactableEntriesSameKey1(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 20, 0, 11, 19, 0, 12, 17, 0, 13, 12)
	iters := []Iterator{iter1}
	mi, err := NewCompactionMergingIterator(iters, false, 30)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 10)
}

func TestCompactionMergingIteratorSkipPastCompactableEntriesSameKey2(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 20, 0, 11, 19, 0, 12, 17, 0, 13, 12)
	iters := []Iterator{iter1}
	mi, err := NewCompactionMergingIterator(iters, false, 20)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 10, 0, 11, 0, 12, 0, 13)
}

func TestCompactionMergingIteratorSkipPastCompactableEntriesSameKey3(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 20, 0, 11, 19, 0, 12, 17, 0, 13, 12)
	iters := []Iterator{iter1}
	mi, err := NewCompactionMergingIterator(iters, false, 18)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 10, 0, 11, 0, 12, 0, 13)
}

func TestCompactionMergingIteratorSkipPastCompactableEntriesSameKey4(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 20, 0, 11, 19, 0, 12, 17, 0, 13, 12,
		1, 10, 20, 1, 11, 19, 1, 12, 17, 1, 13, 12)
	iters := []Iterator{iter1}
	mi, err := NewCompactionMergingIterator(iters, false, 18)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 10, 0, 11, 0, 12, 0, 13, 1, 10, 1, 11, 1, 12, 1, 13)
}

func TestCompactionMergingIteratorWithTombstonesNotPreserved(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 3, 1, 11, 4, 2, 12, 7, 3, 13, 11)
	iter2 := createIterWithVersions(0, 20, 1, 1, 21, 6, 2, -1, 8, 3, -1, 10)
	iter3 := createIterWithVersions(0, 30, 2, 1, 31, 5, 2, 32, 9, 3, 33, 12)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewCompactionMergingIterator(iters, false, 7)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 10, 1, 21, 2, 32, 2, 12, 3, 33, 3, 13)
}

func TestCompactionMergingIteratorWithTombstonesPreserved(t *testing.T) {
	iter1 := createIterWithVersions(0, 10, 3, 1, 11, 4, 2, 12, 7, 3, 13, 11)
	iter2 := createIterWithVersions(0, 20, 1, 1, 21, 6, 2, -1, 8, 3, -1, 10)
	iter3 := createIterWithVersions(0, 30, 2, 1, 31, 5, 2, 32, 9, 3, 33, 12)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewCompactionMergingIterator(iters, true, 7)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 10, 1, 21, 2, 32, 2, -1, 2, 12, 3, 33, 3, 13, 3, -1)
}

func TestMergingIteratorPrefixTombstoneDoNotPreserve(t *testing.T) {
	iter1 := &StaticIterator{}
	iter1.AddKVAsStringWithVersion("a/", "val1", 1)
	iter1.AddKVAsStringWithVersion("a/b", "val2", 1)
	iter1.AddKVAsStringWithVersion("a/c", "val3", 1)
	iter1.AddKVAsStringWithVersion("a/c/a", "val4", 1)
	iter1.AddKVAsStringWithVersion("a/c/a/b", "val5", 1)
	iter1.AddKVAsStringWithVersion("a/d/a", "val7", 1)
	iter1.AddKVAsStringWithVersion("a/d/a/b", "val8", 1)

	iter2 := &StaticIterator{}
	iter2.AddKVAsStringWithVersion("a/c", "", math.MaxUint64)
	iter2.AddKVAsStringWithVersion("a/d/0", "x", math.MaxUint64)

	iters := []Iterator{iter2, iter1}
	mi, err := NewMergingIterator(iters, false, 2)
	require.NoError(t, err)

	requireEntry(t, mi, "a/", "val1", false)
	requireEntry(t, mi, "a/b", "val2", false)
	requireEntry(t, mi, "a/d/a", "val7", false)
	requireEntry(t, mi, "a/d/a/b", "val8", false)
}

func TestMergingIteratorPrefixTombstonePreserveTombstones(t *testing.T) {
	iter1 := &StaticIterator{}
	iter1.AddKVAsStringWithVersion("a/", "val1", 1)
	iter1.AddKVAsStringWithVersion("a/b", "val2", 1)
	iter1.AddKVAsStringWithVersion("a/c", "val3", 1)
	iter1.AddKVAsStringWithVersion("a/c/a", "val4", 1)
	iter1.AddKVAsStringWithVersion("a/c/a/b", "val5", 1)
	iter1.AddKVAsStringWithVersion("a/c/a/b/c", "val6", 1)
	iter1.AddKVAsStringWithVersion("a/d/a", "val8", 1)
	iter1.AddKVAsStringWithVersion("a/d/a/b", "val9", 1)

	iter2 := &StaticIterator{}
	iter2.AddKVAsStringWithVersion("a/c", "", math.MaxUint64)
	iter2.AddKVAsStringWithVersion("a/d/0", "x", math.MaxUint64)

	iters := []Iterator{iter2, iter1}
	mi, err := NewMergingIterator(iters, true, 1)
	require.NoError(t, err)

	requireEntry(t, mi, "a/", "val1", false)
	requireEntry(t, mi, "a/b", "val2", false)

	requireEntry(t, mi, "a/c", "", false)
	requireEntry(t, mi, "a/d/0", "x", false)

	requireEntry(t, mi, "a/d/a", "val8", false)
	requireEntry(t, mi, "a/d/a/b", "val9", true)
}

func requireEntry(t *testing.T, iter Iterator, expectedKey string, expectedVal string, last bool) {
	requireIterValid(t, iter, true)
	curr := iter.Current()
	require.Equal(t, expectedKey, string(curr.Key[:len(curr.Key)-8]))
	require.Equal(t, expectedVal, string(curr.Value))
	if !last {
		require.NoError(t, iter.Next())
	}
}

func expectEntry(t *testing.T, iter Iterator, expKey int, expVal int) {
	t.Helper()
	curr := iter.Current()
	curr.Key = curr.Key[:len(curr.Key)-8] // Strip version
	ekey := fmt.Sprintf("key-%010d", expKey)
	require.Equal(t, ekey, string(curr.Key))
	evalue := fmt.Sprintf("value-%010d", expVal)
	require.Equal(t, evalue, string(curr.Value))
}

func expectEntries(t *testing.T, iter Iterator, expected ...int) {
	t.Helper()
	for i := 0; i < len(expected); i++ {
		expKey := expected[i]
		i++
		expVal := expected[i]
		requireIterValid(t, iter, true)
		curr := iter.Current()
		log.Debugf("got key:%s val:%s", string(curr.Key), string(curr.Value))
		curr.Key = curr.Key[:len(curr.Key)-8] // strip version
		ekey := fmt.Sprintf("key-%010d", expKey)
		require.Equal(t, ekey, string(curr.Key))
		if expVal != -1 {
			evalue := fmt.Sprintf("value-%010d", expVal)
			require.Equal(t, evalue, string(curr.Value))
		} else {
			require.Equal(t, 0, len(curr.Value))
		}
		err := iter.Next()
		require.NoError(t, err)
	}
	requireIterValid(t, iter, false)
}

func createIterWithVersions(vals ...int) *StaticIterator {
	return createIter0(true, vals...)
}

func createIter(vals ...int) *StaticIterator {
	return createIter0(false, vals...)
}

func createIter0(versions bool, vals ...int) *StaticIterator {
	gi := &StaticIterator{}
	for i := 0; i < len(vals); i++ {
		k := vals[i]
		i++
		v := vals[i]
		version := 0
		if versions {
			i++
			version = vals[i]
		}
		key := fmt.Sprintf("key-%010d", k)
		key = string(encoding.EncodeVersion([]byte(key), uint64(version)))
		if v == -1 {
			gi.AddKVAsString(key, "")
		} else {
			value := fmt.Sprintf("value-%010d", v)
			gi.AddKVAsString(key, value)
		}
	}
	return gi
}

func requireIterValid(t *testing.T, iter Iterator, valid bool) {
	t.Helper()
	v, err := iter.IsValid()
	require.NoError(t, err)
	require.Equal(t, valid, v)
}
