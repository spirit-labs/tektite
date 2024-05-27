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
