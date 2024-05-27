package fake

import (
	"fmt"
	"github.com/spirit-labs/tektite/kafka"
	"github.com/spirit-labs/tektite/testutils"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	log "github.com/spirit-labs/tektite/logger"
	"github.com/stretchr/testify/require"
)

func TestCreateDeleteTopic(t *testing.T) {

	fk := &Kafka{}

	t1, err := fk.CreateTopic("topic1", 10)
	require.NoError(t, err)
	require.Equal(t, "topic1", t1.Name)
	require.Equal(t, 10, len(t1.partitions))

	names := fk.GetTopicNames()
	require.Equal(t, 1, len(names))
	require.Equal(t, "topic1", names[0])

	t2, err := fk.CreateTopic("topic2", 20)
	require.NoError(t, err)
	require.Equal(t, "topic2", t2.Name)
	require.Equal(t, 20, len(t2.partitions))

	names = fk.GetTopicNames()
	require.Equal(t, 2, len(names))
	m := map[string]struct{}{}
	for _, name := range names {
		m[name] = struct{}{}
	}
	_, ok := m["topic2"]
	require.True(t, ok)
	_, ok = m["topic1"]
	require.True(t, ok)

	err = fk.DeleteTopic("topic1")
	require.NoError(t, err)
	names = fk.GetTopicNames()
	require.Equal(t, 1, len(names))
	require.Equal(t, "topic2", names[0])

	err = fk.DeleteTopic("topic2")
	require.NoError(t, err)
	names = fk.GetTopicNames()
	require.Equal(t, 0, len(names))
}

func TestIngestConsumeOneSubscriber(t *testing.T) {
	fk := &Kafka{}
	parts := 10
	topic, err := fk.CreateTopic("topic1", parts)
	require.NoError(t, err)
	var partIds []int
	var offsets []int64
	for i := 0; i < parts; i++ {
		partIds = append(partIds, i)
		offsets = append(offsets, 0)
	}

	numMessages := 1000
	sentMsgs := sendMessages(t, fk, numMessages, topic.Name)

	sub, err := topic.CreateSubscriber(partIds, offsets)
	require.NoError(t, err)

	receivedMsgs := map[string]*kafka.Message{}
	for i := 0; i < numMessages; i++ {
		msg, err := sub.GetMessage(5 * time.Second)
		require.NoError(t, err)
		require.NotNil(t, msg)
		receivedMsgs[string(msg.Key)] = msg
	}

	for _, msg := range sentMsgs {
		rec, ok := receivedMsgs[string(msg.Key)]
		require.True(t, ok)
		require.Equal(t, msg, rec)
	}
}

func TestIngestConsumeTwoSubscribersOneGroup(t *testing.T) {
	fk := &Kafka{}
	parts := 1000
	var partIds []int
	var offsets []int64
	for i := 0; i < parts; i++ {
		partIds = append(partIds, i)
		offsets = append(offsets, 0)
	}

	topic, err := fk.CreateTopic("topic1", parts)
	require.NoError(t, err)
	var numMessages int64 = 10
	var msgCounter int64
	consumer1 := newConsumer(topic, &msgCounter, numMessages, partIds[:parts/2], offsets[:parts/2])
	consumer2 := newConsumer(topic, &msgCounter, numMessages, partIds[parts/2:], offsets[parts/2:])
	consumer1.start()
	consumer2.start()

	sentMsgs := sendMessages(t, fk, int(numMessages), topic.Name)

	testutils.WaitUntil(t, func() (bool, error) {
		return atomic.LoadInt64(&msgCounter) == numMessages, nil
	})

	recvMsgs := make(map[string][]byte)
	for _, msg := range consumer1.getMessages() {
		recvMsgs[string(msg.Key)] = msg.Value
	}
	for _, msg := range consumer2.getMessages() {
		recvMsgs[string(msg.Key)] = msg.Value
	}

	for _, sentMsg := range sentMsgs {
		_, ok := recvMsgs[string(sentMsg.Key)]
		require.True(t, ok, fmt.Sprintf("did not receive msg %s", string(sentMsg.Key)))
	}
}

func newConsumer(topic *Topic, msgCounter *int64, maxMessages int64, partIds []int, offsets []int64) *consumer {
	return &consumer{
		topic:       topic,
		msgCounter:  msgCounter,
		maxMessages: maxMessages,
		partIDs:     partIds,
		offsets:     offsets,
	}
}

type consumer struct {
	lock        sync.Mutex
	topic       *Topic
	msgCounter  *int64
	msgs        []*kafka.Message
	maxMessages int64
	partIDs     []int
	offsets     []int64
}

func (c *consumer) start() {
	go func() {
		err := c.runLoop()
		if err != nil {
			log.Fatal(err)
		}
	}()
}

func (c *consumer) runLoop() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	subscriber, err := c.topic.CreateSubscriber(c.partIDs, c.offsets)
	if err != nil {
		return err
	}
	for {
		msg, err := subscriber.GetMessage(10 * time.Millisecond)
		if err != nil {
			return err
		}
		if msg != nil {
			c.msgs = append(c.msgs, msg)
			atomic.AddInt64(c.msgCounter, 1)
		}
		if atomic.LoadInt64(c.msgCounter) == c.maxMessages {
			return subscriber.Unsubscribe()
		}
	}
}

func (c *consumer) getMessages() []*kafka.Message {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.msgs
}

func sendMessages(t *testing.T, fk *Kafka, numMessages int, topicName string) []*kafka.Message {
	t.Helper()
	var sentMsgs []*kafka.Message
	topic, ok := fk.GetTopic(topicName)
	require.True(t, ok)
	for i := 0; i < numMessages; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		var headers []kafka.MessageHeader
		for j := 0; j < 10; j++ {
			headerKey := fmt.Sprintf("header-key-%d", i)
			headerValue := []byte(fmt.Sprintf("header-value-%d", i))
			headers = append(headers, kafka.MessageHeader{
				Key:   headerKey,
				Value: headerValue,
			})
		}
		msg := &kafka.Message{
			Key:     key,
			Value:   value,
			Headers: headers,
		}
		err := topic.Push(msg)
		require.NoError(t, err)
		sentMsgs = append(sentMsgs, msg)
	}
	return sentMsgs
}
