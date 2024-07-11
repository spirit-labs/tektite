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

func (c *ChainingIterator) Current() common.KV {
	return c.iterators[c.pos].Current()
}

func (c *ChainingIterator) Next() error {
	if err := c.iterators[c.pos].Next(); err != nil {
		return err
	}
	valid, err := c.iterators[c.pos].IsValid()
	if err != nil {
		return err
	}
	if valid {
		return nil
	}
	c.pos++
	return nil
}

func (c *ChainingIterator) IsValid() (bool, error) {
	if c.pos >= len(c.iterators) {
		return false, nil
	}
	return c.iterators[c.pos].IsValid()
}

func (c *ChainingIterator) Close() {
}
