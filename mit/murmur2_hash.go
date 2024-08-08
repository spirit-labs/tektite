package mit

/*
Adapted from the movio implementation of the murmur2 hash in golang which mimics the behaviour of the Kafka default Java
partition hash function - used in Apache Samza. https://github.com/movio/go-kafka

That in turn was adapted from the original implementation by David Irvine https://github.com/aviddiviner/go-murmur

Both license headers are included below:
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

/*
MIT License

Copyright (c) 2015 David Irvine

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

func KafkaCompatibleMurmur2Hash(data []byte) uint32 {
	var h int32
	const (
		M = 0x5bd1e995
		R = 24
		// From https://github.com/apache/kafka/blob/0.10.1/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L342
		seed = int32(-1756908916)
	)

	var k int32

	h = seed ^ int32(len(data))

	// Mix 4 bytes at a time into the hash
	for l := len(data); l >= 4; l -= 4 {
		k = int32(data[0]) | int32(data[1])<<8 | int32(data[2])<<16 | int32(data[3])<<24
		k *= M
		k ^= int32(uint32(k) >> R) // To match Kafka Impl
		k *= M
		h *= M
		h ^= k
		data = data[4:]
	}

	// Handle the last few bytes of the input array
	switch len(data) {
	case 3:
		h ^= int32(data[2]) << 16
		fallthrough
	case 2:
		h ^= int32(data[1]) << 8
		fallthrough
	case 1:
		h ^= int32(data[0])
		h *= M
	}

	// Do a few final mixes of the hash to ensure the last few bytes are well incorporated
	h ^= int32(uint32(h) >> 13)
	h *= M
	h ^= int32(uint32(h) >> 15)

	// See https://github.com/apache/kafka/blob/0.10.1/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L728
	return uint32(h & 0x7fffffff)
}
