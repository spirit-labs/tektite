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
	obj, err := m.client.GetObject(context.Background(), *m.cfg.MinioBucketName, objName, minio.GetObjectOptions{})
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
	_, err := m.client.PutObject(context.Background(), *m.cfg.MinioBucketName, objName, buff, int64(len(value)),
		minio.PutObjectOptions{})
	return maybeConvertError(err)
}

func (m *Client) Delete(key []byte) error {
	objName := string(key)
	return maybeConvertError(m.client.RemoveObject(context.Background(), *m.cfg.MinioBucketName, objName, minio.RemoveObjectOptions{}))
}

func (m *Client) Start() error {
	client, err := minio.New(*m.cfg.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(*m.cfg.MinioAccessKey, *m.cfg.MinioSecretKey, ""),
		Secure: *m.cfg.MinioSecure,
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
