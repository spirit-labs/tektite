package objstore

import (
	"context"
	"time"
)

type Client interface {
	Get(ctx context.Context, bucket string, key string) ([]byte, error)
	Put(ctx context.Context, bucket string, key string, value []byte) error
	PutIfNotExists(ctx context.Context, bucket string, key string, value []byte) (bool, error)
	Delete(ctx context.Context, bucket string, key string) error
	DeleteAll(ctx context.Context, bucket string, keys []string) error
	ListObjectsWithPrefix(ctx context.Context, bucket string, prefix string, maxKeys int) ([]ObjectInfo, error)
	Start() error
	Stop() error
}

type ObjectInfo struct {
	Key          string
	LastModified time.Time
}

const DefaultCallTimeout = 5 * time.Second

// Convenience methods that apply a timeout to the Client operations

func GetWithTimeout(client Client, bucket string, key string, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.Get(ctx, bucket, key)
}

func PutWithTimeout(client Client, bucket string, key string, value []byte, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.Put(ctx, bucket, key, value)
}

func PutIfNotExistsWithTimeout(client Client, bucket string, key string, value []byte, timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.PutIfNotExists(ctx, bucket, key, value)
}

func DeleteWithTimeout(client Client, bucket string, key string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.Delete(ctx, bucket, key)
}

func DeleteAllWithTimeout(client Client, bucket string, keys []string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.DeleteAll(ctx, bucket, keys)
}

func ListObjectsWithPrefixWithTimeout(client Client, bucket string, prefix string, maxKeys int, timeout time.Duration) ([]ObjectInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.ListObjectsWithPrefix(ctx, bucket, prefix, maxKeys)
}