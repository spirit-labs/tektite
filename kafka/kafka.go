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
