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

package opers

import (
	"errors"
	"github.com/spirit-labs/tektite/kafka"
	"sync/atomic"
	"time"
)

type msgClientFact struct {
	msgs                [][]*kafka.Message
	numFailuresToCreate int
}

func (m msgClientFact) createTestMessageClient(topicName string, props map[string]string) (kafka.MessageClient, error) {
	return newTestMessageClient(topicName, props, m.msgs, m.numFailuresToCreate), nil
}

func newTestMessageClient(topicName string, props map[string]string, msgs [][]*kafka.Message,
	numFailuresToCreate int) *testMessageClient {
	return &testMessageClient{
		topicName:           topicName,
		props:               props,
		msgs:                msgs,
		numFailuresToCreate: numFailuresToCreate,
	}
}

type testMessageClient struct {
	topicName           string
	props               map[string]string
	msgs                [][]*kafka.Message
	numFailuresToCreate int
	failureCount        int64
}

type testMessageProvider struct {
	topicName  string
	properties map[string]string
	msgs       [][]*kafka.Message
	pos        int
}

func (t *testMessageProvider) GetMessage(pollTimeout time.Duration) (*kafka.Message, error) {
	for i := 0; i < len(t.msgs); i++ {
		msg := t.getMessage()
		if msg != nil {
			return msg, nil
		}
	}
	time.Sleep(pollTimeout)
	return nil, nil
}

func (t *testMessageProvider) getMessage() *kafka.Message {
	var msg *kafka.Message
	part := t.msgs[t.pos]
	if len(part) != 0 {
		msg = part[0]
		t.msgs[t.pos] = t.msgs[t.pos][1:]
	}
	t.pos++
	if t.pos == len(t.msgs) {
		t.pos = 0
	}
	return msg
}

func (t *testMessageProvider) Stop() error {
	return nil
}

func (t *testMessageProvider) Start() error {
	return nil
}

func (t *testMessageProvider) Close() error {
	return nil
}

func (t *testMessageClient) NewMessageProvider(partitionIDs []int, _ []int64) (kafka.MessageProvider, error) {
	if t.numFailuresToCreate > 0 && atomic.AddInt64(&t.failureCount, 1) <= int64(t.numFailuresToCreate) {
		return nil, errors.New("forcing test connect error")
	}
	var partitions [][]*kafka.Message
	for _, part := range partitionIDs {
		partitions = append(partitions, t.msgs[part])
	}
	return &testMessageProvider{
		msgs:       partitions,
		topicName:  t.topicName,
		properties: t.props,
	}, nil
}

func (t *testMessageClient) NewMessageProducer(_ int, _ time.Duration, _ time.Duration) (kafka.MessageProducer, error) {
	return nil, nil
}
