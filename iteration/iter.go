package iteration

import "github.com/spirit-labs/tektite/common"

type Iterator interface {
	Current() common.KV
	Next() error
	IsValid() (bool, error)
	Close()
}
