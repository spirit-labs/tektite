package iteration

import "github.com/spirit-labs/tektite/common"

type EmptyIterator struct {
}

func (e EmptyIterator) Next() (bool, common.KV, error) {
	return false, common.KV{}, nil
}

func (e EmptyIterator) Current() common.KV {
	return common.KV{}
}

func (e EmptyIterator) Close() {
}

