package fake

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/kafka"
	"sync"
	"time"

	"github.com/spirit-labs/tektite/errors"

	log "github.com/spirit-labs/tektite/logger"

	"github.com/spirit-labs/tektite/common"
)

type Kafka struct {
	topicLock sync.Mutex
	topics    sync.Map
}

func (f *Kafka) CreateTopic(name string, partitions int) (*Topic, error) {
	f.topicLock.Lock()
	defer f.topicLock.Unlock()
	if _, ok := f.getTopic(name); ok {
		return nil, errors.Errorf("topic with name %s already exists", name)
	}
	parts := make([]*Partition, partitions)
	for i := 0; i < partitions; i++ {
		parts[i] = &Partition{
			id: int32(i),
		}
	}
	topic := &Topic{
		Name:       name,
		partitions: parts,
	}
	f.topics.Store(name, topic)
	return topic, nil
}

func (f *Kafka) GetTopic(name string) (*Topic, bool) {
	return f.getTopic(name)
}

func (f *Kafka) DeleteTopic(name string) error {
	f.topicLock.Lock()
	defer f.topicLock.Unlock()
	topic, ok := f.getTopic(name)
	if !ok {
		return errors.Errorf("no such topic %s", name)
	}
	topic.close()
	f.topics.Delete(name)
	return nil
}

func (f *Kafka) GetTopicNames() []string {
	f.topicLock.Lock()
	defer f.topicLock.Unlock()
	var names []string
	f.topics.Range(func(key, _ interface{}) bool {
		names = append(names, key.(string))
		return true
	})
	return names
}

func (f *Kafka) getTopic(name string) (*Topic, bool) {
	t, ok := f.topics.Load(name)
	if !ok {
		return nil, false
	}
	return t.(*Topic), true
}

func hash(key []byte) (uint32, error) {
	return common.DefaultHash(key), nil
}

type Topic struct {
	Name       string
	lock       sync.RWMutex
	groups     sync.Map
	partitions []*Partition
}

type Partition struct {
	lock     sync.Mutex
	id       int32
	messages []*kafka.Message
}

type MessageQueue chan *kafka.Message

func (p *Partition) push(message *kafka.Message) {
	p.lock.Lock()
	defer p.lock.Unlock()
	message.PartInfo = kafka.PartInfo{
		PartitionID: p.id,
		Offset:      int64(len(p.messages)),
	}
	p.messages = append(p.messages, message)
}

func (t *Topic) Push(message *kafka.Message) error {
	part, err := t.calcPartition(message)
	if err != nil {
		return errors.WithStack(err)
	}
	t.partitions[part].push(message)
	return nil
}

func (t *Topic) calcPartition(message *kafka.Message) (int, error) {
	h, err := hash(message.Key)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	partID := int(h % uint32(len(t.partitions)))
	return partID, nil
}

func (t *Topic) CreateSubscriber(partitionIDs []int, startOffsets []int64) (*Subscriber, error) {

	log.Debugf("creating fake kafka subscriber for partitions %v, offsets %v", partitionIDs, startOffsets)

	offsetsMap := map[int32]int64{}
	var partitions []*Partition
	for i, partitionID := range partitionIDs {
		var offset int64
		if startOffsets[i] == -1 {
			// start at beginning
			offset = 0
		} else {
			offset = startOffsets[i]
		}
		offsetsMap[int32(partitionID)] = offset
		partitions = append(partitions, t.partitions[partitionID])
	}
	subscriber := &Subscriber{
		topic:       t,
		nextOffsets: offsetsMap,
		partitions:  partitions,
	}
	return subscriber, nil
}

func (t *Topic) close() {
}

type Subscriber struct {
	topic       *Topic
	partitions  []*Partition
	stopped     common.AtomicBool
	msgBuffer   []*kafka.Message
	nextOffsets map[int32]int64
}

func (c *Subscriber) GetMessage(pollTimeout time.Duration) (*kafka.Message, error) {
	if c.stopped.Get() {
		panic("subscriber is stopped")
	}
	start := time.Now()
	for time.Since(start) < pollTimeout {

		if len(c.msgBuffer) == 0 {
			for _, part := range c.partitions {
				offset, ok := c.nextOffsets[part.id]
				if !ok {
					offset = 0
				}
				part.lock.Lock()
				if len(part.messages) > int(offset) {
					msg := part.messages[offset]
					c.nextOffsets[part.id] = offset + 1
					c.msgBuffer = append(c.msgBuffer, msg)
				}
				part.lock.Unlock()
			}
		}

		if len(c.msgBuffer) != 0 {
			msg := c.msgBuffer[0]
			c.msgBuffer = c.msgBuffer[1:]
			return msg, nil
		}
		time.Sleep(1 * time.Millisecond)
	}
	return nil, nil
}

func (c *Subscriber) Unsubscribe() error {
	c.stopped.Set(true)
	return nil
}

func NewFakeMessageClientFactory(fk *Kafka) kafka.ClientFactory {
	return func(topicName string, props map[string]string) (kafka.MessageClient, error) {
		return &MessageProviderFactory{
			fk:        fk,
			topicName: topicName,
			props:     props,
		}, nil
	}
}

type MessageProviderFactory struct {
	fk        *Kafka
	topicName string
	props     map[string]string
}

func (fmpf *MessageProviderFactory) NewMessageProvider(partitions []int, offsets []int64) (kafka.MessageProvider, error) {
	topic, ok := fmpf.fk.GetTopic(fmpf.topicName)
	if !ok {
		return nil, errors.Errorf("no such topic %s", fmpf.topicName)
	}
	return &MessageProvider{
		topic:        topic,
		partitionIDs: partitions,
		offsets:      offsets,
	}, nil
}

type MessageProvider struct {
	subscriber   *Subscriber
	topic        *Topic
	partitionIDs []int
	offsets      []int64
	lock         sync.Mutex
}

func (f *MessageProvider) SetMaxRate() {
}

func (f *MessageProvider) GetMessage(pollTimeout time.Duration) (*kafka.Message, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.subscriber == nil {
		// This is ok, we must start the message consumer before the we start the message provider
		// so there is a window where the subscriber is not set.
		return nil, nil
	}
	return f.subscriber.GetMessage(pollTimeout)
}

func (f *MessageProvider) Start() error {
	f.lock.Lock()
	defer f.lock.Unlock()
	subscriber, err := f.topic.CreateSubscriber(f.partitionIDs, f.offsets)
	if err != nil {
		return errors.WithStack(err)
	}
	f.subscriber = subscriber
	return nil
}

func (f *MessageProvider) Stop() error {
	f.subscriber.stopped.Set(true)
	return nil
}

func (f *MessageProvider) Close() error {
	return f.subscriber.Unsubscribe()
}

func (fmpf *MessageProviderFactory) NewMessageProducer(partitionID int, _ time.Duration, _ time.Duration) (kafka.MessageProducer, error) {
	return &MessageProducer{
		fk:          fmpf.fk,
		topicName:   fmpf.topicName,
		partitionID: partitionID,
	}, nil
}

type MessageProducer struct {
	fk          *Kafka
	topicName   string
	partitionID int
}

func (f *MessageProducer) SendBatch(batch *evbatch.Batch) error {
	topic, ok := f.fk.GetTopic(f.topicName)
	if !ok {
		return errors.Errorf("no such topic %s", f.topicName)
	}
	msgs, err := createMessages(batch)
	if err != nil {
		return err
	}
	for _, msg := range msgs {
		if err := topic.Push(&msg); err != nil {
			return err
		}
	}
	return nil
}

func (f *MessageProducer) Stop() error {
	return nil
}

func (f *MessageProducer) Start() error {
	return nil
}

func createMessages(batch *evbatch.Batch) ([]kafka.Message, error) {
	etCol := batch.GetTimestampColumn(1)
	keyCol := batch.GetBytesColumn(2)
	hdrsCol := batch.GetBytesColumn(3)
	valCol := batch.GetBytesColumn(4)
	rc := batch.RowCount
	msgs := make([]kafka.Message, rc)
	for i := 0; i < rc; i++ {
		et := etCol.Get(i)
		var key []byte
		if !keyCol.IsNull(i) {
			key = keyCol.Get(i)
		}
		var headers []kafka.MessageHeader
		if !hdrsCol.IsNull(i) {
			headerBytes := hdrsCol.Get(i)
			var err error
			headers, err = decodeHeaders(headerBytes)
			if err != nil {
				return nil, err
			}
		}
		val := valCol.Get(i)
		ts := time.UnixMilli(et.Val).UTC()
		msg := &msgs[i]
		msg.TimeStamp = ts
		msg.Key = key
		msg.Value = val
		msg.Headers = headers
	}
	return msgs, nil
}

func decodeHeaders(kafkaHeaders []byte) ([]kafka.MessageHeader, error) {
	numHeaders, off := binary.Varint(kafkaHeaders)
	if off <= 0 {
		return nil, errors.Errorf("failed to decode uvarint from kafka headers: %d", off)
	}
	in := int(numHeaders)
	headers := make([]kafka.MessageHeader, in)
	for i := 0; i < in; i++ {
		headerNameLen, n := binary.Varint(kafkaHeaders[off:])
		if n <= 0 {
			return nil, errors.Errorf("failed to decode uvarint from kafka headers: %d", n)
		}
		off += n
		hNameOff := off
		iHNameLen := int(headerNameLen)
		off += iHNameLen
		headerValLen, n := binary.Varint(kafkaHeaders[off:])
		if n <= 0 {
			return nil, errors.Errorf("failed to decode uvarint from kafka headers: %d", n)
		}
		off += n
		hValOff := off
		iHValLen := int(headerValLen)
		off += iHValLen
		headerName := kafkaHeaders[hNameOff : hNameOff+iHNameLen]
		headerVal := kafkaHeaders[hValOff : hValOff+iHValLen]
		headers[i].Key = string(headerName)
		if len(headerVal) > 0 {
			headers[i].Value = headerVal
		}
	}
	return headers, nil
}
