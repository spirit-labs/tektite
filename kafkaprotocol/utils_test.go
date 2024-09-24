package kafkaprotocol

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestSizeofUvarint(t *testing.T) {
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	var buff []byte
	for i := 0; i < 1000000; i++ {
		num := int(r.Uint32())
		size := sizeofUvarint(num)
		buff = binary.AppendUvarint(buff, uint64(num))
		require.Equal(t, size, len(buff))
		buff = buff[:0]
	}
}

func BenchmarkSizeofUvarint(b *testing.B) {
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	num := int(r.Uint32())
	b.ResetTimer()
	tot := 0
	for i := 0; i < b.N; i++ {
		tot += sizeofUvarint(num)
	}
	b.StopTimer()
	fmt.Print(tot)
}
