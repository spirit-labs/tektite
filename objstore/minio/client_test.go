// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
