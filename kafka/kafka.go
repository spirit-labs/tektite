package kafka

import (
	"github.com/spirit-labs/tektite/evbatch"
	"time"
)

type ClientFactory func(topicName string, props map[string]string) (MessageClient, error)

type MessageClient interface {
	NewMessageProvider(partitions []int, startOffsets []int64) (MessageProvider, error)
	NewMessageProducer(partitionID int, connectTimeout time.Duration, sendTimeout time.Duration) (MessageProducer, error)
}

type MessageProvider interface {
	GetMessage(pollTimeout time.Duration) (*Message, error)
	Start() error
	Stop() error
}

type MessageProducer interface {
	SendBatch(batch *evbatch.Batch) error
	Stop() error
	Start() error
}

type Message struct {
	PartInfo  PartInfo
	TimeStamp time.Time
	Key       []byte
	Value     []byte
	Headers   []MessageHeader
}

type MessageHeader struct {
	Key   string
	Value []byte
}

type PartInfo struct {
	PartitionID int32
	Offset      int64
}
