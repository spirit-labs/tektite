package compress

import (
	"bytes"
	"compress/gzip"
	"github.com/DataDog/zstd"
	"github.com/klauspost/compress/s2"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
	"io"
)

type CompressionType byte

const (
	CompressionTypeNone    CompressionType = 0
	CompressionTypeGzip    CompressionType = 1
	CompressionTypeSnappy  CompressionType = 2
	CompressionTypeLz4     CompressionType = 3
	CompressionTypeZstd    CompressionType = 4
	CompressionTypeUnknown CompressionType = 255
)

func FromString(str string) CompressionType {
	switch str {
	case "none":
		return CompressionTypeNone
	case "gzip":
		return CompressionTypeGzip
	case "snappy":
		return CompressionTypeSnappy
	case "lz4":
		return CompressionTypeLz4
	case "zstd":
		return CompressionTypeZstd
	default:
		return CompressionTypeUnknown
	}
}

func (t CompressionType) String() string {
	switch t {
	case CompressionTypeNone:
		return "none"
	case CompressionTypeGzip:
		return "gzip"
	case CompressionTypeSnappy:
		return "snappy"
	case CompressionTypeLz4:
		return "lz4"
	case CompressionTypeZstd:
		return "zstd"
	case CompressionTypeUnknown:
		return "unknown"
	default:
		panic("unknown compression type")
	}
}

func Compress(compressionType CompressionType, buff []byte, data []byte) ([]byte, error) {
	var w io.WriteCloser
	var buf *bytes.Buffer
	switch compressionType {
	case CompressionTypeGzip:
		buf = bytes.NewBuffer(buff)
		w = gzip.NewWriter(buf)
		if _, err := w.Write(data); err != nil {
			return nil, err
		}
	case CompressionTypeSnappy:
		compressed := s2.EncodeSnappy(nil, data)
		if buff == nil {
			return compressed, nil
		}
		return append(buff, compressed...), nil
	case CompressionTypeLz4:
		buf = bytes.NewBuffer(buff)
		w = lz4.NewWriter(buf)
		if _, err := w.Write(data); err != nil {
			return nil, err
		}
	case CompressionTypeZstd:
		compressed, err := zstd.Compress(nil, data)
		if err != nil {
			return nil, err
		}
		if buff == nil {
			return compressed, nil
		}
		return append(buff, compressed...), nil
	default:
		return nil, errors.Errorf("unexpected compression type: %d", compressionType)
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decompress(compressionType CompressionType, data []byte) ([]byte, error) {
	var r io.Reader
	switch compressionType {
	case CompressionTypeGzip:
		var err error
		r, err = gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
	case CompressionTypeSnappy:
		return s2.Decode(nil, data)
	case CompressionTypeLz4:
		r = lz4.NewReader(bytes.NewReader(data))
	case CompressionTypeZstd:
		return zstd.Decompress(nil, data)
	default:
		return nil, errors.Errorf("unexpected compression type: %d", compressionType)
	}
	buf := new(bytes.Buffer)
	_, err := io.Copy(buf, r)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
