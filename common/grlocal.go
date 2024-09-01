package common

import (
	"github.com/timandy/routine"
	"sync"
)

// GRLocal is similar to a thread local found in other languages. It allows a value to be cached on
// a per go-routine basis. Assumes that Gets happen much more frequently than sets as set holds a write lock which will
// contend with any gets
type GRLocal struct {
	lock   sync.RWMutex
	values map[int64]any
}

func NewGRLocal() GRLocal {
	return GRLocal{
		values: map[int64]any{},
	}
}

func (g *GRLocal) Get() (any, bool) {
	g.lock.RLock()
	defer g.lock.RUnlock()
	v, ok := g.values[routine.Goid()]
	return v, ok
}

func (g *GRLocal) Set(value any) {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.values[routine.Goid()] = value
}

func (g *GRLocal) Delete() {
	g.lock.Lock()
	defer g.lock.Unlock()
	delete(g.values, routine.Goid())
}
