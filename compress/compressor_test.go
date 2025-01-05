package compress

import (
	"crypto/rand"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompressionGzip(t *testing.T) {
	testCompressor(t, CompressionTypeGzip)
}

func TestCompressionSnappy(t *testing.T) {
	testCompressor(t, CompressionTypeSnappy)
}

func TestCompressionLz4(t *testing.T) {
	testCompressor(t, CompressionTypeLz4)
}

func TestCompressionZstd(t *testing.T) {
	testCompressor(t, CompressionTypeZstd)
}

func testCompressor(t *testing.T, compressionType CompressionType) {
	testCompressorWithInitialBytes(t, compressionType, 0)
	testCompressorWithInitialBytes(t, compressionType, 100)
}

func testCompressorWithInitialBytes(t *testing.T, compressionType CompressionType, numInitialBytes int) {
	data := randomBytes(10000)
	var initialBytes []byte
	if numInitialBytes > 0 {
		initialBytes = randomBytes(numInitialBytes)
	}
	compressed, err := Compress(compressionType, initialBytes, data)
	require.NoError(t, err)
	if numInitialBytes > 0 {
		require.Equal(t, initialBytes, compressed[:len(initialBytes)])
	}
	decompressed, err := Decompress(compressionType, compressed[len(initialBytes):])
	require.NoError(t, err)
	require.Equal(t, data, decompressed)
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}
