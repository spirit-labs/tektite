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

package types

import (
	"fmt"
	"testing"
)

func BenchmarkDecimal(b *testing.B) {
	var l int64
	dec := Dec(12345, 0, 2)
	for i := 0; i < b.N; i++ {
		i := dec.ToInt64()
		l += i
	}

	b.StopTimer()
	fmt.Println(l)
}
