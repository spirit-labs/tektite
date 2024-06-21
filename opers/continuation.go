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

package opers

import (
	"github.com/spirit-labs/tektite/evbatch"
	"sync"
)

type ContinuationOperator struct {
	BaseOperator
	schema *OperatorSchema
}

func (c *ContinuationOperator) HandleStreamBatch(batch *evbatch.Batch, execCtx StreamExecContext) (*evbatch.Batch, error) {
	return nil, c.sendBatchDownStream(batch, execCtx)
}

func (c *ContinuationOperator) HandleQueryBatch(*evbatch.Batch, QueryExecContext) (*evbatch.Batch, error) {
	panic("not supported")
}

func (c *ContinuationOperator) HandleBarrier(execCtx StreamExecContext) error {
	return c.BaseOperator.HandleBarrier(execCtx)
}

func (c *ContinuationOperator) InSchema() *OperatorSchema {
	return c.schema
}

func (c *ContinuationOperator) OutSchema() *OperatorSchema {
	return c.schema
}

func (c *ContinuationOperator) Setup(StreamManagerCtx) error {
	return nil
}

func (c *ContinuationOperator) Teardown(StreamManagerCtx, *sync.RWMutex) {
}
