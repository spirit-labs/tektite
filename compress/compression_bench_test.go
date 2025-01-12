package compress

import "testing"

func BenchmarkCompressGzip(b *testing.B) {
	benchmarkCompress(b, CompressionTypeGzip)
}

func BenchmarkCompressSnappy(b *testing.B) {
	benchmarkCompress(b, CompressionTypeSnappy)
}

func BenchmarkCompressLz4(b *testing.B) {
	benchmarkCompress(b, CompressionTypeLz4)
}

func BenchmarkCompressZstd(b *testing.B) {
	benchmarkCompress(b, CompressionTypeZstd)
}

func benchmarkCompress(b *testing.B, compressionType CompressionType) {
	buff := randomBytes(100000)
	for i := 0; i < b.N; i++ {
		compressed, err := Compress(compressionType, nil, buff)
		if err != nil {
			panic(err)
		}
		decompressed, err := Decompress(compressionType, compressed)
		if err != nil {
			panic(err)
		}
		if len(decompressed) != len(buff) {
			panic("wrong compressed size")
		}
	}
}
