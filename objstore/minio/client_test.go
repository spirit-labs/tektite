package minio

import (
	"context"
	"fmt"
	miniolib "github.com/minio/minio-go/v7"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"testing"
)

func TestMinioObjStore(t *testing.T) {

	ctx := context.Background()

	minioContainer, err := minio.Run(ctx, "minio/minio:RELEASE.2024-08-26T15-33-07Z", minio.WithUsername("minioadmin"), minio.WithPassword("minioadmin"))
	require.NoError(t, err)

	// Clean up the container
	defer func() {
		err := minioContainer.Terminate(ctx)
		require.NoError(t, err)
	}()

	cfg := Conf{}

	ip, err := minioContainer.Host(ctx)
	require.NoError(t, err)

	port, err := minioContainer.MappedPort(ctx, "9000")
	require.NoError(t, err)

	cfg.Endpoint = fmt.Sprintf("%s:%d", ip, port.Int())

	cfg.Username = "minioadmin"
	cfg.Password = "minioadmin"

	client := NewMinioClient(cfg)
	err = client.Start()
	require.NoError(t, err)

	// Make the buckets needed by the tests
	for i := 0; i < objstore.NumTestBuckets; i++ {
		bucket := fmt.Sprintf("%s%.2d", objstore.TestBucketPrefix, i)
		err = client.client.MakeBucket(ctx, bucket, miniolib.MakeBucketOptions{})
		require.NoError(t, err)
	}

	objstore.TestApi(t, client)
}
