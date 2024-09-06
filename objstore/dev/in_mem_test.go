package dev

import (
	"github.com/spirit-labs/tektite/objstore"
	"testing"
)

func TestInMemStore(t *testing.T) {
	inMem := NewInMemStore(0)
	objstore.TestApi(t, inMem)
}
