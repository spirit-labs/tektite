package consistent

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"testing"
)

func TestGetNoMembers(t *testing.T) {
	consist := NewConsistentHash(100)
	_, ok := consist.Get([]byte("foo"))
	require.False(t, ok)
}

func TestAddRemoveNodes(t *testing.T) {
	consist := NewConsistentHash(100)
	active := map[string]struct{}{}
	numMembers := 10
	// first add
	for i := 0; i < numMembers; i++ {
		member := uuid.New().String()
		active[member] = struct{}{}
		consist.Add(member)
		for i := 0; i < 1000; i++ {
			m, ok := consist.Get([]byte(uuid.New().String()))
			require.True(t, ok)
			// Make sure it's from the active set
			_, exists := active[m]
			require.True(t, exists)
		}
	}
	// then remove
	for member := range active {
		consist.Remove(member)
		delete(active, member)
		if len(active) == 0 {
			_, ok := consist.Get([]byte(uuid.New().String()))
			require.False(t, ok)
		} else {
			for i := 0; i < 1000; i++ {
				m, ok := consist.Get([]byte(uuid.New().String()))
				require.True(t, ok)
				// Make sure it's from the active set
				_, exists := active[m]
				require.True(t, exists)
			}
		}
	}
}

// TestConsistDistribution - test the distribution of the consistent hash
func TestConsistDistribution(t *testing.T) {

	numKeys := 100000
	var keys []string
	for i := 0; i < numKeys; i++ {
		keys = append(keys, fmt.Sprintf("key-%09d", i))
	}

	consist := NewConsistentHash(250)

	numMembers := 10
	for i := 0; i < numMembers; i++ {
		consist.Add(fmt.Sprintf("member-%05d", i))
	}

	keyMemberAssignment := map[string]string{}

	memberCounts := map[string]int{}
	for _, key := range keys {
		member, ok := consist.Get([]byte(key))
		require.True(t, ok)
		keyMemberAssignment[key] = member
		memberCounts[member]++
	}

	minMemberCount := math.MaxInt
	maxMemberCount := 0
	for _, count := range memberCounts {
		if count < minMemberCount {
			minMemberCount = count
		}
		if count > maxMemberCount {
			maxMemberCount = count
		}
	}
	diff := maxMemberCount - minMemberCount

	avgKeysPerMember := float64(numKeys) / float64(numMembers)

	perCentDiff := 100 * float64(diff) / avgKeysPerMember

	require.LessOrEqual(t, perCentDiff, 20.0)

	// add a new member
	consist.Add(fmt.Sprintf("member-%05d", numMembers))

	keyMemberAssignment2 := map[string]string{}
	for _, key := range keys {
		member, ok := consist.Get([]byte(key))
		require.True(t, ok)
		keyMemberAssignment2[key] = member
	}
	numChanged := 0
	for key, member := range keyMemberAssignment {
		newMember, ok := keyMemberAssignment2[key]
		require.True(t, ok)
		if member != newMember {
			numChanged++
		}
	}

	percentChanged := 100 * float64(numChanged) / float64(numKeys)

	require.LessOrEqual(t, percentChanged, 10.0)
}

func BenchmarkConsistGet(b *testing.B) {

	consist := NewConsistentHash(10)

	numMembers := 10
	for i := 0; i < numMembers; i++ {
		consist.Add(uuid.New().String())
	}

	numKeys := 10000
	var keys [][]byte
	for i := 0; i < numKeys; i++ {
		keys = append(keys, []byte(uuid.New().String()))
	}

	start := rand.Int()

	b.ResetTimer()

	l := 0
	for i := 0; i < b.N; i++ {
		key := keys[(start+i)%numKeys]
		member, ok := consist.Get(key)
		if !ok {
			panic("member not found")
		}
		l += len(member)
	}

	b.StopTimer()

	fmt.Println(l)
}
