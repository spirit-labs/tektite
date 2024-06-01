package types

import (
	"fmt"
	"testing"
)

func BenchmarkDecimal(b *testing.B) {
	var l int64
	dec := Dec(12345, 0, 2)
	for i := 0; i < b.N; i++ {
		i := dec.ToInt64()
		l += i
	}

	b.StopTimer()
	fmt.Println(l)
}
