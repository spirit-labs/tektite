package minio

//
// Needs minio running
//
//func TestMinio(t *testing.T) {
//
//	cfg := &conf.Config{}
//	cfg.ApplyDefaults()
//
//	cfg.MinioEndpoint = "127.0.0.1:9000"
//	cfg.MinioAccessKey = "tYTKoueu7NyentYPe3OF"
//	cfg.MinioSecretKey = "DxMe9mGt5OEeUNvqv3euXMcOx7mmLui6g9q4CMjB"
//	cfg.MinioBucketName = "tektite-dev"
//
//	client := NewMinioClient(cfg)
//	err := client.Start()
//	require.NoError(t, err)
//
//	vb, err := client.Get([]byte("key10"))
//	require.NoError(t, err)
//	require.Nil(t, vb)
//
//	err = client.Put([]byte("key1"), []byte("val1"))
//	require.NoError(t, err)
//
//	vb, err = client.Get([]byte("key1"))
//	require.NoError(t, err)
//	require.NotNil(t, vb)
//	require.Equal(t, "val1", string(vb))
//
//	err = client.Put([]byte("key2"), []byte("val2"))
//	require.NoError(t, err)
//
//	vb, err = client.Get([]byte("key2"))
//	require.NoError(t, err)
//	require.NotNil(t, vb)
//	require.Equal(t, "val2", string(vb))
//
//	err = client.Delete([]byte("key1"))
//	require.NoError(t, err)
//	vb, err = client.Get([]byte("key1"))
//	require.NoError(t, err)
//	require.Nil(t, vb)
//
//	vb, err = client.Get([]byte("key2"))
//	require.NoError(t, err)
//	require.NotNil(t, vb)
//	require.Equal(t, "val2", string(vb))
//
//	err = client.Delete([]byte("key2"))
//	require.NoError(t, err)
//	vb, err = client.Get([]byte("key2"))
//	require.NoError(t, err)
//	require.Nil(t, vb)
//}
