// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
