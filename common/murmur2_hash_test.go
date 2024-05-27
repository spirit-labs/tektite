package common

import "testing"

/*
Adapted from  https://github.com/movio/go-kafka

License header is included below:
*/

/*
MIT License

Copyright (c) 2016 Movio

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

var tests = []struct {
	key          []byte
	expectedHash uint32
	expected2    uint32
	expected3    uint32
	expected100  uint32
	expected999  uint32
}{
	{[]byte(""), 275646681, 1, 0, 81, 603},
	{[]byte("⌘"), 39915425, 1, 2, 25, 380},
	{[]byte("oh no"), 939168436, 0, 1, 36, 544},
	{[]byte("c03a3475-3ed6-4ed1-8ae5-1c432da43e73"), 376769867, 1, 2, 67, 14},
}

func Murmur2Partition(key []byte, numPartitions int) uint32 {
	hash := KafkaCompatibleMurmur2Hash(key)
	return CalcPartition(hash, numPartitions)
}

func TestHashing(t *testing.T) {
	for _, i := range tests {
		if v := KafkaCompatibleMurmur2Hash(i.key); v != i.expectedHash {
			t.Errorf("Positive, expected: %v, got: %v\n", i.expectedHash, v)
		}
		if v := Murmur2Partition(i.key, 2); v != i.expected2 {
			t.Errorf("2 patitions, expected: %v, got: %v\n", v, i.expected2)
		}
		if v := Murmur2Partition(i.key, 3); v != i.expected3 {
			t.Errorf("3 patitions, expected: %v, got: %v\n", v, i.expected3)
		}
		if v := Murmur2Partition(i.key, 100); v != i.expected100 {
			t.Errorf("100 patitions, expected: %v, got: %v\n", v, i.expected100)
		}
		if v := Murmur2Partition(i.key, 999); v != i.expected999 {
			t.Errorf("999 patitions, expected: %v, got: %v\n", v, i.expected999)
		}
	}
}
