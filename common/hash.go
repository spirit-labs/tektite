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
	"crypto/sha256"
	"hash/fnv"
)

func CalcPartition(hash uint32, numPartitions int) uint32 {
	return hash % uint32(numPartitions)
}

type HashFunc func([]byte) uint32

func HashSha256(key []byte) uint32 {
	h := sha256.New()
	if _, err := h.Write(key); err != nil {
		panic(err)
	}
	res := h.Sum(nil)
	h2 := fnv.New32()
	if _, err := h2.Write(res); err != nil {
		panic(err)
	}
	return h2.Sum32()
}

func HashFnv(key []byte) uint32 {
	h := fnv.New32()
	if _, err := h.Write(key); err != nil {
		panic(err)
	}
	return h.Sum32()
}

func DefaultHash(key []byte) uint32 {
	return KafkaCompatibleMurmur2Hash(key)
}
