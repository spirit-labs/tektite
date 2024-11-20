package minio

import (
	"bytes"
	"context"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/objstore"
	"io"
	"math"
)

func NewMinioClient(cfg Conf) *Client {
	return &Client{
		cfg: cfg,
	}
}

type Client struct {
	cfg    Conf
	client *minio.Client
}

func (m *Client) Get(ctx context.Context, bucket string, key string) ([]byte, error) {
	obj, err := m.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, maybeConvertError(err)
	}
	//goland:noinspection GoUnhandledErrorResult
	defer obj.Close()
	buff, err := io.ReadAll(obj)
	if err != nil {
		var merr minio.ErrorResponse
		if errwrap.As(err, &merr) {
			if merr.StatusCode == 404 {
				// does not exist
				return nil, nil
			}
		}
		return nil, maybeConvertError(err)
	}
	return buff, nil
}

func (m *Client) Put(ctx context.Context, bucket string, key string, value []byte) error {
	buff := bytes.NewBuffer(value)
	_, err := m.client.PutObject(ctx, bucket, key, buff, int64(len(value)),
		minio.PutObjectOptions{})
	return maybeConvertError(err)
}

func (m *Client) PutIfNotExists(ctx context.Context, bucket string, key string, value []byte) (bool, error) {
	buff := bytes.NewBuffer(value)
	opts := minio.PutObjectOptions{}
	opts.SetMatchETagExcept("*")
	_, err := m.client.PutObject(ctx, bucket, key, buff, int64(len(value)), opts)
	if err != nil {
		var errResponse minio.ErrorResponse
		if errors.As(err, &errResponse) {
			if errResponse.StatusCode == 412 {
				// Pre-condition failed - this means key already exists
				return false, nil
			}
		}
		return false, maybeConvertError(err)
	}
	return true, nil
}

func (m *Client) Delete(ctx context.Context, bucket string, key string) error {
	return maybeConvertError(m.client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{}))
}

func (m *Client) DeleteAll(ctx context.Context, bucket string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	opts := minio.RemoveObjectsOptions{}
	// must be a blocking channel
	ch := make(chan minio.ObjectInfo)
	errCh := m.client.RemoveObjects(ctx, bucket, ch, opts)
	// Minio client has a weird API here forcing us to spawn GRs, and add messages to channel after calling RemoveObjects
	// to avoid losing any messages
	go func() {
		for _, key := range keys {
			ch <- minio.ObjectInfo{
				Key: key,
			}
		}
		close(ch)
	}()
	for err := range errCh {
		if err.Err != nil {
			return err.Err
		}
	}
	return nil
}

func (m *Client) ListObjectsWithPrefix(ctx context.Context, bucket string, prefix string, maxKeys int) ([]objstore.ObjectInfo, error) {
	var opts minio.ListObjectsOptions
	opts.Prefix = string(prefix)

	if maxKeys != -1 && maxKeys < 1000 {
		// The minio MaxKeys option just determines the max number of keys in each batch (it does the batching itself)
		// not the total number of keys
		opts.MaxKeys = maxKeys
	}

	ch := m.client.ListObjects(ctx, bucket, opts)

	if maxKeys == -1 {
		maxKeys = math.MaxInt
	}

	var infos []objstore.ObjectInfo
	for info := range ch {
		if info.Err != nil {
			return nil, maybeConvertError(info.Err)
		}
		infos = append(infos, objstore.ObjectInfo{
			Key:          info.Key,
			LastModified: info.LastModified,
		})
		// Sadly minio client does not let us request a certain number of keys - it returns them all, so we
		// need to bail out here if there are too many
		if len(infos) == maxKeys {
			ctx.Done()
			break
		}
	}
	return infos, nil
}


func (m *Client) Start() error {
	client, err := minio.New(m.cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(m.cfg.Username, m.cfg.Password, ""),
		Secure: m.cfg.Secure,
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
	return common.NewTektiteErrorf(common.Unavailable, err.Error())
}

type Conf struct {
	Endpoint string
	Username string
	Password string
	Secure bool
}
