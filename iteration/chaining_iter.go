package iteration

import (
	"github.com/spirit-labs/tektite/common"
)

type ChainingIterator struct {
	iterators []Iterator
	pos       int
	head      common.KV
}

func NewChainingIterator(its []Iterator) *ChainingIterator {
	return &ChainingIterator{iterators: its}
}

func (c *ChainingIterator) Next() (bool, common.KV, error) {
	for ; c.pos < len(c.iterators); c.pos++ {
		valid, kv, err := c.iterators[c.pos].Next()
		if err != nil {
			return false, common.KV{}, err
		}
		if !valid {
			continue
		}
		c.head = kv
		return true, kv, err
	}
	c.head = common.KV{}
	return false, c.head, nil
}

func (c *ChainingIterator) Current() common.KV {
	return c.head
}

func (c *ChainingIterator) Close() {
}
