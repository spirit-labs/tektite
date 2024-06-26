package iteration

import (
	"github.com/spirit-labs/tektite/common"
)

type Iterator interface {
	Next() (bool, common.KV, error)
	Close()
}
