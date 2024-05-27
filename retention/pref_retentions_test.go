package retention

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDoesPrefixApplyToRange(t *testing.T) {

	// prefix is shorter than table keys
	testDoesPrefixApplyToRange(t, "db001/tab001/", "db001/tab001/key003", "db001/tab001/key005", true)
	testDoesPrefixApplyToRange(t, "db001/tab001/", "db001/tab000/key001", "db001/tab001/key005", true)
	testDoesPrefixApplyToRange(t, "db001/tab001/", "db001/tab000/key001", "db001/tab002/key005", true)

	testDoesPrefixApplyToRange(t, "db001/tab001/", "db001/tab002/key003", "db001/tab002/key005", false)
	testDoesPrefixApplyToRange(t, "db001/tab001/", "db001/tab002/key003", "db001/tab003/key005", false)

	// prefix is longer than table keys
	testDoesPrefixApplyToRange(t, "db001/tab001/", "db001/", "db010/", true)
	testDoesPrefixApplyToRange(t, "db001/tab001/", "db000/", "db010/", true)
	testDoesPrefixApplyToRange(t, "db001/tab001/", "db002/", "db010/", false)
	testDoesPrefixApplyToRange(t, "db001/tab001/", "db000/", "db000/", false)

	testDoesPrefixApplyToRange(t, "db001/tab001/", "db001/tab001/key003", "", true)
	testDoesPrefixApplyToRange(t, "db001/tab001/", "db001/tab002/key003", "", false)

	testDoesPrefixApplyToRange(t, "db001/tab002/", "", "db001/tab001/key003", false)
	testDoesPrefixApplyToRange(t, "db001/tab002/", "", "db001/tab002/key003", true)

	testDoesPrefixApplyToRange(t, "db001/tab002/", "", "", true)
}

func testDoesPrefixApplyToRange(t *testing.T, prefix string, rangeStart string, rangeEnd string, applies bool) {
	var rs, re []byte
	if rangeStart == "" {
		rs = nil
	} else {
		rs = []byte(rangeStart)
	}
	if rangeEnd == "" {
		re = nil
	} else {
		re = []byte(rangeEnd)
	}
	require.Equal(t, applies, DoesPrefixApplyToTable([]byte(prefix), rs, re))
}

func TestSerializeDeserializePrefixRetentions(t *testing.T) {
	var prefRets []PrefixRetention
	for i := 0; i < 10; i++ {
		prefix := []byte(fmt.Sprintf("prefix-%d", i))
		prefRets = append(prefRets, PrefixRetention{
			Prefix:    prefix,
			Retention: uint64(i - 1), // Make sure we get one -ve number
		})
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = SerializePrefixRetentions(buff, prefRets)

	dpAfter, _ := DeserializePrefixRetentions(buff, 3)
	require.Equal(t, prefRets, dpAfter)
}
