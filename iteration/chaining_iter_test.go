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
