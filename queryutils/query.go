package queryutils

import (
	"github.com/spirit-labs/tektite/iteration"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/sst"
	"math"
)

type Querier interface {
	QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error)
}

func CreateIteratorForKeyRange(keyStart []byte, keyEnd []byte, querier Querier, tableGetter sst.TableGetter) (iteration.Iterator, error) {
	ids, err := querier.QueryTablesInRange(keyStart, keyEnd)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return iteration.EmptyIterator{}, nil
	}
	var iters []iteration.Iterator
	for _, nonOverLapIDs := range ids {
		if len(nonOverLapIDs) == 1 {
			info := nonOverLapIDs[0]
			iter, err := sst.NewLazySSTableIterator(info.ID, tableGetter, keyStart, keyEnd)
			if err != nil {
				return nil, err
			}
			iters = append(iters, iter)
		} else {
			itersInChain := make([]iteration.Iterator, len(nonOverLapIDs))
			for j, nonOverlapID := range nonOverLapIDs {
				iter, err := sst.NewLazySSTableIterator(nonOverlapID.ID, tableGetter, keyStart, keyEnd)
				if err != nil {
					return nil, err
				}
				itersInChain[j] = iter
			}
			iters = append(iters, iteration.NewChainingIterator(itersInChain))
		}
	}
	return iteration.NewMergingIterator(iters, false, math.MaxUint64)
}
