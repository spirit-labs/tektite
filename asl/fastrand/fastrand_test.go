/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 * Modifications copyright (C) 2020 Andy Kimball and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fastrand

import (
	"math/rand"
	"testing"
)

// Results with go1.13.12 on a Mac with a 2.3 GHz 8-Core Intel Core i9 processor:
//
// $ go test -cpu 1,2,4,8,16,32 -bench=. -count=1 | benchstat /dev/stdin
//
//   name            time/op
//   FastRand        2.25ns ± 2%
//   FastRand-2      1.14ns ± 3%
//   FastRand-4      0.59ns ± 4%
//   FastRand-8      0.35ns ± 6%
//   FastRand-16     0.33ns ± 4%
//   FastRand-32     0.32ns ± 2%
//   DefaultRand     13.3ns ± 3%
//   DefaultRand-2   19.2ns ± 3%
//   DefaultRand-4   41.5ns ± 1%
//   DefaultRand-8   56.7ns ± 1%
//   DefaultRand-16  66.6ns ±12%
//   DefaultRand-32  65.8ns ± 6%
//

func BenchmarkFastRand(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Uint32()
		}
	})
}

func BenchmarkDefaultRand(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rand.Uint32()
		}
	})
}
