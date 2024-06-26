package iteration

import (
	"github.com/spirit-labs/tektite/common"
)

type ChainingIterator struct {
	iterators []Iterator
	pos       int
}

func NewChainingIterator(its []Iterator) *ChainingIterator {
	return &ChainingIterator{iterators: its}
}

func (c *ChainingIterator) Next() (bool, common.KV, error) {
	for ; c.pos < len(c.iterators); c.pos++ {
		valid, kv, err := c.iterators[c.pos].Next()
		if err == nil && !valid {
			continue
		}
		return valid, kv, err
	}
	return false, common.KV{}, nil
}

func (c *ChainingIterator) Close() {
}
