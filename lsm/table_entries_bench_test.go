package lsm

import (
	log "github.com/spirit-labs/tektite/logger"
	"testing"
)

func BenchmarkSerializeManyTableEntries(b *testing.B) {

	numEntries := 1000000

	levEntry := levelEntry{}
	for i := 0; i < numEntries; i++ {
		levEntry.InsertAt(i, createTabEntry(i))
	}

	b.ResetTimer()

	buff := make([]byte, 0, 16*1024*1024)

	for i := 0; i < b.N; i++ {
		buff = levEntry.Serialize(buff)
		if i == 0 {
			log.Infof("buff len %d", len(buff))
		}
		var le2 levelEntry
		le2.Deserialize(buff, 0)
		buff = buff[:0]
	}
}
