package consistent

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

/*
HashRing is an implementation of consistent hashing using virtual nodes.
The hash ring contains members which can be added or removed dynamically. The method Get returns a member for a given
key - it will always return the same member as long as it's in the ring.
With consistent hashing, as members are added or removed only a fraction of the mappings of key->member change.
Typically this should be approximately 1/N where N is number of members. Contrast this with a naive member selection
which hashes the key and then uses a modulus to select a member. In that method almost every key->member mapping changes
when a member is added or removed.
For each member we maintain multiple virtual nodes by generating keys, hashing them and laying them out in sorted order
in a slice. To choose a member for a key, we hash the key, then we find the smallest key in the ring smaller than the
given key, using binary search. We then lookup which member that was generated for and return that.
We create multiple virtual nodes for each member rather than just hashing the member once, as this provides a more
uniform distribution. Increasing the virtualFactor increase uniformity at the expense of memory usage and processing time
in adding/removing members.
The HashRing currently uses SHA-256 hashing to hash keys and virtual nodes.
*/
type HashRing struct {
	lock          sync.RWMutex
	virtualFactor int
	hashMemberMap map[uint64]string
	memberHashes  map[string][]uint64
	sorted        []uint64
}

func NewConsistentHash(virtualFactor int) *HashRing {
	return &HashRing{
		virtualFactor: virtualFactor,
		hashMemberMap: map[uint64]string{},
		memberHashes:  map[string][]uint64{},
	}
}

func (c *HashRing) Add(member string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	hashes := make([]uint64, c.virtualFactor)
	for i := 0; i < c.virtualFactor; i++ {
		key := []byte(fmt.Sprintf("%s-%d", member, i))
		h := hash(key)
		hashes[i] = h
		_, exists := c.hashMemberMap[h]
		if exists {
			// should never happen with sha-256
			panic("hash collision")
		}
		c.hashMemberMap[h] = member
	}
	c.memberHashes[member] = hashes
	c.createSortedKeys()
}

func (c *HashRing) Remove(member string) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	hashes, ok := c.memberHashes[member]
	if !ok {
		return false
	}
	for _, h := range hashes {
		delete(c.hashMemberMap, h)
	}
	c.createSortedKeys()
	return true
}

func (c *HashRing) createSortedKeys() {
	all := make([]uint64, 0, len(c.hashMemberMap))
	for h := range c.hashMemberMap {
		all = append(all, h)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i] < all[j]
	})
	c.sorted = all
}

func (c *HashRing) Get(key []byte) (string, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if len(c.sorted) == 0 {
		return "", false
	}
	h := hash(key)
	pos := sort.Search(len(c.sorted), func(x int) bool {
		return c.sorted[x] > h
	})
	if pos >= len(c.sorted) {
		pos = 0
	}
	v := c.sorted[pos]
	member := c.hashMemberMap[v]
	return member, true
}

func hash(key []byte) uint64 {
	h := sha256.New()
	if _, err := h.Write(key); err != nil {
		panic(err)
	}
	bytes := h.Sum(nil)
	return binary.BigEndian.Uint64(bytes)
}
