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
