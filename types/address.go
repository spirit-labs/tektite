package types

import "golang.org/x/exp/constraints"

type TekTiteOrdered interface {
	constraints.Ordered | bool
}

func AddressOf[T TekTiteOrdered](x T) *T {
	return &x
}
