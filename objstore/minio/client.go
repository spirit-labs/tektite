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

import (
	"bytes"
	"context"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/errors"
	"io"
)

func NewMinioClient(cfg *conf.Config) *Client {
	return &Client{
		cfg: cfg,
	}
}

type Client struct {
	cfg    *conf.Config
	client *minio.Client
}

func (m *Client) Get(key []byte) ([]byte, error) {
	objName := string(key)
	obj, err := m.client.GetObject(context.Background(), m.cfg.MinioBucketName, objName, minio.GetObjectOptions{})
	if err != nil {
		return nil, maybeConvertError(err)
	}
	//goland:noinspection GoUnhandledErrorResult
	defer obj.Close()
	buff, err := io.ReadAll(obj)
	if err != nil {
		var merr minio.ErrorResponse
		if errors.As(err, &merr) {
			if merr.StatusCode == 404 {
				// does not exist
				return nil, nil
			}
		}
		return nil, maybeConvertError(err)
	}
	return buff, nil
}

func (m *Client) Put(key []byte, value []byte) error {
	buff := bytes.NewBuffer(value)
	objName := string(key)
	_, err := m.client.PutObject(context.Background(), m.cfg.MinioBucketName, objName, buff, int64(len(value)),
		minio.PutObjectOptions{})
	return maybeConvertError(err)
}

func (m *Client) Delete(key []byte) error {
	objName := string(key)
	return maybeConvertError(m.client.RemoveObject(context.Background(), m.cfg.MinioBucketName, objName, minio.RemoveObjectOptions{}))
}

func (m *Client) Start() error {
	client, err := minio.New(m.cfg.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(m.cfg.MinioAccessKey, m.cfg.MinioSecretKey, ""),
		Secure: m.cfg.MinioSecure,
	})
	if err != nil {
		return err
	}
	m.client = client
	return nil
}

func (m *Client) Stop() error {
	m.client = nil
	return nil
}

func maybeConvertError(err error) error {
	if err == nil {
		return err
	}
	return errors.NewTektiteErrorf(errors.Unavailable, err.Error())
}
