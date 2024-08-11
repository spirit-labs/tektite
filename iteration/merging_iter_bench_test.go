package iteration

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func BenchmarkMergingIterator(b *testing.B) {
	numEntries := 1000
	lengths := []int{2, 3, 4, 5}

	for _, numIters := range lengths {
		iters := make([]Iterator, numIters)
		for i := 0; i < numIters; i++ {
			iters[i] = &StaticIterator{}
		}
		var expectedKeys [][]byte
		var expectedVals [][]byte
		for i := 0; i < numEntries; i++ {
			skey := fmt.Sprintf("someprefix/key-%010d", i)
			sval := fmt.Sprintf("someprefix/val-%010d", i)
			expectedKeys = append(expectedKeys, []byte(skey))
			expectedVals = append(expectedVals, []byte(sval))
			r := rand.Intn(numIters)
			sIter := iters[r].(*StaticIterator) //nolint:forcetypeassert
			sIter.AddKVAsStringWithVersion(skey, sval, 0)
		}

		b.Run(fmt.Sprintf("MerginIterator-%d", numIters), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for j := 0; j < numIters; j++ {
					iters[j].(*StaticIterator).pos = 0
				}

				mi, err := NewMergingIterator(iters, false, 0)
				require.NoError(b, err)

				for j := 0; j < numEntries; j++ {
					valid, curr, err := mi.Next()
					if err != nil {
						panic(err)
					}
					if !valid {
						panic("not valid")
					}
					expectedKey := expectedKeys[j]
					expectedVal := expectedVals[j]
					if !bytes.Equal(expectedKey, curr.Key[:len(curr.Key)-8]) {
						panic("key not equal")
					}
					if !bytes.Equal(expectedVal, curr.Value) {
						panic("key not equal")
					}
				}
			}
		})
	}
}
