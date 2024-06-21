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

package proc

import (
	log "github.com/spirit-labs/tektite/logger"
	"testing"
)

func BenchmarkCompletion(b *testing.B) {
	var o obj
	tot := 0
	for i := 0; i < b.N; i++ {
		o.cFunc1(i, func(i int) {
			tot += i
		})
	}
	b.StopTimer()
	log.Debugf("tot:%d", tot)
}

func BenchmarkDirect(b *testing.B) {
	var o obj
	tot := 0
	for i := 0; i < b.N; i++ {
		tot += o.dFunc1(i)
	}
	b.StopTimer()
	log.Debugf("tot:%d", tot)
}

type obj struct {
}

func (o *obj) dFunc1(i int) int {
	return i + 1
}

func (o *obj) dFunc2(i int) int {
	return i + 1
}

func (o *obj) dFunc3(i int) int {
	return i + 1
}

func (o *obj) dFunc4(i int) int {
	return i
}

func (o *obj) cFunc1(i int, complFunc func(int)) {
	o.cFunc2(i, func(i int) {
		complFunc(i + 1)
	})
}

func (o *obj) cFunc2(i int, complFunc func(int)) {
	o.cFunc3(i, func(i int) {
		complFunc(i + 1)
	})
}

func (o *obj) cFunc3(i int, complFunc func(int)) {
	o.cFunc4(i, func(i int) {
		complFunc(i + 1)
	})
}

func (o *obj) cFunc4(i int, complFunc func(int)) {
	complFunc(i)
}

func (o *obj) cFunc5() {

}
