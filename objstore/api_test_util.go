package objstore

import (
	"context"
	"fmt"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestApi(t *testing.T, client Client) {

	testCases := []testCase{
		{testName: "testPutGet", test: testPutGet},
		{testName: "testPutOverwrite", test: testPutOverwrite},
		{testName: "testDelete", test: testDelete},
		{testName: "testDeleteAll", test: testDeleteAll},
		{testName: "testPutIfNotExists", test: testPutIfNotExists},
		{testName: "testListAllObjects", test: testListAllObjects},
		{testName: "testListSomeObjects", test: testListSomeObjects},
		{testName: "testListObjectsNilPrefix", test: testListObjectsEmptyPrefix},
		{testName: "testListMoreThan1000Objects", test: testListMoreThan1000Objects},
		{testName: "testMultipleBuckets", test: testMultipleBuckets},
	}

	for _, tc := range testCases {
		// Need to empty buckets before each test
		for i := 0; i < NumTestBuckets; i++ {
			bucket := fmt.Sprintf("%s%.2d", TestBucketPrefix, i)
			emptyBucket(t, bucket, client)
		}

		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, client)
		})
	}
}

func emptyBucket(t *testing.T, bucket string, client Client) {
	infos, err := client.ListObjectsWithPrefix(context.Background(), bucket, "", -1)
	require.NoError(t, err)
	if len(infos) > 0 {
		keys := make([]string, len(infos))
		for i, info := range infos {
			keys[i] = info.Key
		}
		err = client.DeleteAll(context.Background(), bucket, keys)
		require.NoError(t, err)
	}
}

const (
	DefaultBucket    = "test-bucket-01"
	TestBucketPrefix = "test-bucket-"
	NumTestBuckets   = 3
)

type testCase struct {
	testName string
	test     func(t *testing.T, client Client)
}

func testPutGet(t *testing.T, client Client) {
	ctx := context.Background()

	vb, err := client.Get(ctx, DefaultBucket, "key1")
	require.NoError(t, err)
	require.Nil(t, vb)

	vb, err = client.Get(ctx, DefaultBucket, "key2")
	require.NoError(t, err)
	require.Nil(t, vb)

	vb, err = client.Get(ctx, DefaultBucket, "key3")
	require.NoError(t, err)
	require.Nil(t, vb)

	err = client.Put(ctx, DefaultBucket, "key1", []byte("val1"))
	require.NoError(t, err)

	vb, err = client.Get(ctx, DefaultBucket, "key1")
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val1", string(vb))

	err = client.Put(ctx, DefaultBucket, "key2", []byte("val2"))
	require.NoError(t, err)

	vb, err = client.Get(ctx, DefaultBucket, "key2")
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val2", string(vb))

	vb, err = client.Get(ctx, DefaultBucket, "key3")
	require.NoError(t, err)
	require.Nil(t, vb)
}

func testPutOverwrite(t *testing.T, client Client) {
	ctx := context.Background()

	err := client.Put(ctx, DefaultBucket, "key1", []byte("val1"))
	require.NoError(t, err)

	vb, err := client.Get(ctx, DefaultBucket, "key1")
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val1", string(vb))

	err = client.Put(ctx, DefaultBucket, "key1", []byte("val2"))
	require.NoError(t, err)

	vb, err = client.Get(ctx, DefaultBucket, "key1")
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val2", string(vb))
}

func testDelete(t *testing.T, client Client) {
	ctx := context.Background()

	err := client.Put(ctx, DefaultBucket, "key1", []byte("val1"))
	require.NoError(t, err)

	vb, err := client.Get(ctx, DefaultBucket, "key1")
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val1", string(vb))

	err = client.Delete(ctx, DefaultBucket, "key1")
	require.NoError(t, err)

	vb, err = client.Get(ctx, DefaultBucket, "key1")
	require.NoError(t, err)
	require.Nil(t, vb)
}

func testDeleteAll(t *testing.T, client Client) {
	ctx := context.Background()

	numKeys := 10
	var keys []string
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%d", i)
		err := client.Put(ctx, DefaultBucket, key, []byte(fmt.Sprintf("val%d", i)))
		keys = append(keys, key)
		require.NoError(t, err)
	}

	// delete some of them

	err := client.DeleteAll(ctx, DefaultBucket, keys[2:7])
	require.NoError(t, err)

	for i, key := range keys {
		v, err := client.Get(ctx, DefaultBucket, key)
		require.NoError(t, err)
		if i >= 2 && i < 7 {
			require.Nil(t, v) // should be deleted
		} else {
			require.Equal(t, []byte(fmt.Sprintf("val%d", i)), v)
		}
	}
}

func testPutIfNotExists(t *testing.T, client Client) {
	ctx := context.Background()

	err := client.Put(ctx, DefaultBucket, "key1", []byte("val1"))
	require.NoError(t, err)

	ok, err := client.PutIfNotExists(ctx, DefaultBucket, "key1", []byte("val2"))
	require.NoError(t, err)
	require.False(t, ok)

	vb, err := client.Get(ctx, DefaultBucket, "key1")
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val1", string(vb))

	ok, err = client.PutIfNotExists(ctx, DefaultBucket, "key2", []byte("val2"))
	require.NoError(t, err)
	require.True(t, ok)

	vb, err = client.Get(ctx, DefaultBucket, "key2")
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val2", string(vb))
}

func testListAllObjects(t *testing.T, client Client) {
	ctx := context.Background()

	numPrefixes := 10
	numKeys := 10
	var keys []string
	for i := 0; i < numPrefixes; i++ {
		for j := 0; j < numKeys; j++ {
			key := fmt.Sprintf("prefix-%.10d-%.10d", i, j)
			keys = append(keys, key)
		}
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	nowMs := time.Now().UnixMilli()
	for _, key := range keys {
		err := client.Put(ctx, DefaultBucket, key, []byte(key))
		require.NoError(t, err)
	}
	// note: last-modified in S3 only has millisecond precision
	afterMs := time.Now().UnixMilli()
	for i := 0; i < numPrefixes; i++ {
		prefix := fmt.Sprintf("prefix-%.10d", i)
		infos, err := client.ListObjectsWithPrefix(ctx, DefaultBucket, prefix, -1)
		require.NoError(t, err)
		require.Equal(t, numKeys, len(infos))
		for j, info := range infos {
			expectedKey := fmt.Sprintf("%s-%.10d", prefix, j)
			require.Equal(t, expectedKey, info.Key)
			log.Infof("lm:%d", info.LastModified.Nanosecond())
			require.GreaterOrEqual(t, info.LastModified.UnixMilli(), nowMs)
			require.LessOrEqual(t, info.LastModified.UnixMilli(), afterMs)
		}
	}
}

func testListSomeObjects(t *testing.T, client Client) {
	ctx := context.Background()

	numKeys := 2500
	var keys []string
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%.10d", i)
		keys = append(keys, key)
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	for _, key := range keys {
		err := client.Put(ctx, DefaultBucket, key, []byte(key))
		require.NoError(t, err)
	}

	listAndVerifyObjects(t, client, 1)
	listAndVerifyObjects(t, client, 10)
	listAndVerifyObjects(t, client, 1000)
	listAndVerifyObjects(t, client, 1001)
	listAndVerifyObjects(t, client, 1500)
	listAndVerifyObjects(t, client, 2000)
	listAndVerifyObjects(t, client, 2500)
}

func listAndVerifyObjects(t *testing.T, client Client, numKeys int) {
	infos, err := client.ListObjectsWithPrefix(context.Background(), DefaultBucket, "", numKeys)
	require.NoError(t, err)
	require.Equal(t, numKeys, len(infos))
	for i, info := range infos {
		expectedKey := fmt.Sprintf("key-%.10d", i)
		require.Equal(t, expectedKey, info.Key)
	}
}

func testListObjectsEmptyPrefix(t *testing.T, client Client) {
	ctx := context.Background()

	numPrefixes := 10
	numKeys := 10
	var keys []string
	for i := 0; i < numPrefixes; i++ {
		for j := 0; j < numKeys; j++ {
			key := fmt.Sprintf("prefix-%.10d-%.10d", i, j)
			keys = append(keys, key)
		}
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	nowMs := time.Now().UnixMilli()
	for _, key := range keys {
		err := client.Put(ctx, DefaultBucket, key, []byte(key))
		require.NoError(t, err)
	}
	// note: last-modified in S3 only has millisecond precision
	afterMs := time.Now().UnixMilli()
	infos, err := client.ListObjectsWithPrefix(ctx, DefaultBucket, "", -1)
	require.NoError(t, err)
	require.Equal(t, numKeys*numPrefixes, len(infos))
	for i := 0; i < numPrefixes; i++ {
		for j := 0; j < numKeys; j++ {
			expectedKey := fmt.Sprintf("prefix-%.10d-%.10d", i, j)
			k := i*numKeys + j
			require.Equal(t, expectedKey, infos[k].Key)
			require.GreaterOrEqual(t, infos[k].LastModified.UnixMilli(), nowMs)
			require.LessOrEqual(t, infos[k].LastModified.UnixMilli(), afterMs)
		}
	}
}

// Make sure return more than 1000 - S3 uses pagination and limits max 1000 usually - the client should handle
// calling the S3 API more than once
func testListMoreThan1000Objects(t *testing.T, client Client) {
	ctx := context.Background()
	numObjects := 2555
	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("key-%.10d", i)
		err := client.Put(ctx, DefaultBucket, key, []byte(key))
		require.NoError(t, err)
	}
	infos, err := client.ListObjectsWithPrefix(ctx, DefaultBucket, "", -1)
	require.NoError(t, err)
	require.Equal(t, numObjects, len(infos))
	for i, info := range infos {
		expectedKey := fmt.Sprintf("key-%.10d", i)
		require.Equal(t, expectedKey, info.Key)
	}
}

func testMultipleBuckets(t *testing.T, client Client) {
	ctx := context.Background()
	numKeys := 10
	for i := 0; i < NumTestBuckets; i++ {
		bucket := fmt.Sprintf("%s%.2d", TestBucketPrefix, i)
		for j := 0; j < numKeys; j++ {
			key := fmt.Sprintf("key-%.2d", j)
			err := client.Put(ctx, bucket, key, []byte(fmt.Sprintf("%s-value1", bucket)))
			require.NoError(t, err)
		}
	}
	for i := 0; i < NumTestBuckets; i++ {
		bucket := fmt.Sprintf("%s%.2d", TestBucketPrefix, i)
		for j := 0; j < numKeys; j++ {
			key := fmt.Sprintf("key-%.2d", j)
			val, err := client.Get(ctx, bucket, key)
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprintf("%s-value1", bucket)), val)
		}
	}
}
