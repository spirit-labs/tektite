package minio

import (
	"context"
	"fmt"
	"os"
	"testing"

	miniolib "github.com/minio/minio-go/v7"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

func TestMinioObjStore(t *testing.T) {
	// Currently, Podman doesn't support Ryuk
	// Raised an issue on testcontainers-go
	// https://github.com/testcontainers/testcontainers-go/issues/2781
	// ToDo: Remove the below line once the above issue is resolved
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	ctx := context.Background()
	minioContainer, err := minio.Run(ctx, "minio/minio:RELEASE.2024-08-26T15-33-07Z", minio.WithUsername("minioadmin"), minio.WithPassword("minioadmin"))
	require.NoError(t, err)

	// Clean up the container
	defer func() {
		err := minioContainer.Terminate(ctx)
		require.NoError(t, err)
	}()

	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	ip, err := minioContainer.Host(ctx)
	require.NoError(t, err)

	port, err := minioContainer.MappedPort(ctx, "9000")
	require.NoError(t, err)

	cfg.MinioEndpoint = fmt.Sprintf("%s:%d", ip, port.Int())

	cfg.MinioUsername = "minioadmin"
	cfg.MinioPassword = "minioadmin"

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
