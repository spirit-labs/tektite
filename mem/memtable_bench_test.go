package mem

import (
	"fmt"
	"github.com/spirit-labs/tektite/arenaskl"
	"github.com/spirit-labs/tektite/common"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func BenchmarkMemTableWrites(b *testing.B) {
	numEntries := 1000
	batch := NewBatch()
	for i := 0; i < numEntries; i++ {
		k := rand.Intn(100000)
		key := []byte(fmt.Sprintf("prefix/key%010d", k))
		val := []byte(fmt.Sprintf("val%010d", k))
		batch.AddEntry(common.KV{
			Key:   key,
			Value: val,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		memTable := NewMemtable(arenaskl.NewArena(1024*1024), 0, 1024*1024)
		ok, err := memTable.Write(batch)
		require.NoError(b, err)
		require.True(b, ok)
	}
}
