//go:build !segmentio
// +build !segmentio

package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	segment "github.com/segmentio/kafka-go"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	log "github.com/spirit-labs/tektite/logger"
	"strings"
	"sync"
	"time"
)

func NewMessageProviderFactory(topicName string, props map[string]string) (MessageClient, error) {
	var earliest bool
	sAutoOffsetReset, ok := props["auto.offset.reset"]
	if !ok {
		// default to latest
		earliest = false
	} else {
		if sAutoOffsetReset == "earliest" {
			earliest = true
		} else if sAutoOffsetReset == "latest" {
			earliest = false
		} else {
			return nil, common.NewTektiteErrorf(common.InvalidConfiguration, "invalid value for auto.offset.reset: %s - must be one of 'earliest' or 'latest'", sAutoOffsetReset)
		}
	}
	return &DefaultMessageProviderFactory{
		topicName: topicName,
		props:     props,
		earliest:  earliest,
	}, nil
}

type DefaultMessageProviderFactory struct {
	topicName string
	props     map[string]string
	earliest  bool
}

func (dmpf *DefaultMessageProviderFactory) NewMessageProvider(partitions []int, offsets []int64) (MessageProvider, error) {
	kmp := &DefaultMessageProvider{}
	kmp.krpf = dmpf
	kmp.topicName = dmpf.topicName
	// generate a group id - we don't use consumer re-balancing as we assign partitions to consumers explicitly
	// however Kafka still requires group.id to be set
	kmp.groupID = fmt.Sprintf("tektite-%s-%s", dmpf.topicName, uuid.New().String())
	topicPartitions := make([]kafka.TopicPartition, len(partitions))

	for i, partitionID := range partitions {
		var offset kafka.Offset
		if offsets[i] == -1 {
			// -1 represents we haven't yet consumed any messages
			if dmpf.earliest {
				// start at the first message in the partition
				offset = kafka.OffsetBeginning
			} else {
				// start at end
				offset = kafka.OffsetEnd
			}
		} else {
			// carry on where we left off
			offset = kafka.Offset(offsets[i])
		}
		topicPartitions[i] = kafka.TopicPartition{
			Topic:     &dmpf.topicName,
			Partition: int32(partitionID),
			Offset:    offset,
		}
	}
	kmp.partitions = topicPartitions
	return kmp, nil
}

func (dmpf *DefaultMessageProviderFactory) NewMessageProducer(partitionID int, connectTimeout time.Duration,
	sendTimeout time.Duration) (MessageProducer, error) {
	mp := &DefaultMessageProducer{}
	mp.dmpf = dmpf
	mp.topicName = dmpf.topicName
	mp.partitionID = partitionID
	mp.connectTimeout = connectTimeout
	mp.sendTimeout = sendTimeout
	return mp, nil
}

/*
DefaultMessageProducer
We use the segmentio Kafka client as the Confluent client sadly doesn't return errors when target Kafka is unavailable -
instead it retries to send forever. This makes it very hard to use for us where we need to block on send until the
messages are delivered, but we also need to error immediately if Kafka not available so we can backoff and retry after
a delay.
*/
type DefaultMessageProducer struct {
	dmpf             *DefaultMessageProviderFactory
	topicName        string
	partitionID      int
	conn             *segment.Conn
	bootstrapServers []string
	acks             int
	bootstrapPos     int
	lock             sync.RWMutex
	connectTimeout   time.Duration
	sendTimeout      time.Duration
}

func (dmp *DefaultMessageProducer) Stop() error {
	dmp.lock.Lock()
	defer dmp.lock.Unlock()
	if dmp.conn != nil {
		return dmp.conn.Close()
	}
	return nil
}

func (dmp *DefaultMessageProducer) Start() error {
	bs, ok := dmp.dmpf.props["bootstrap.servers"]
	if !ok {
		return common.NewStatementError("cannot start message producer - bootstrap.servers must be specified")
	}
	split := strings.Split(bs, ",")
	for _, s := range split {
		dmp.bootstrapServers = append(dmp.bootstrapServers, strings.Trim(s, " "))
	}
	sacks, ok := dmp.dmpf.props["acks"]
	acks := -1
	if ok {
		switch sacks {
		case "1":
			acks = 1
		case "0":
			acks = 0
		case "all":
			acks = -1
		default:
			return common.NewStatementError("invalid value for acks")
		}
	}
	dmp.acks = acks
	return nil
}

func (dmp *DefaultMessageProducer) SendBatch(batch *evbatch.Batch) error {
	dmp.lock.RLock()
	defer dmp.lock.RUnlock()
	if dmp.conn == nil {
		conn, err := dmp.createConnection()
		if err != nil {
			return err
		}
		dmp.conn = conn
	}
	msgs, err := dmp.createMessages(batch)
	if err != nil {
		return err
	}
	if err := dmp.conn.SetWriteDeadline(time.Now().Add(dmp.sendTimeout)); err != nil {
		return err
	}
	_, err = dmp.conn.WriteMessages(msgs...)
	if err != nil {
		// We close connection on error, next attempt will try with next bootstrap server
		if err := dmp.conn.Close(); err != nil {
			// Ignore
		}
		dmp.conn = nil
	}
	return err
}

func (dmp *DefaultMessageProducer) createMessages(batch *evbatch.Batch) ([]segment.Message, error) {
	etCol := batch.GetTimestampColumn(1)
	keyCol := batch.GetBytesColumn(2)
	hdrsCol := batch.GetBytesColumn(3)
	valCol := batch.GetBytesColumn(4)
	rc := batch.RowCount
	msgs := make([]segment.Message, rc)
	for i := 0; i < rc; i++ {
		et := etCol.Get(i)
		var key []byte
		if !keyCol.IsNull(i) {
			key = keyCol.Get(i)
		}
		var headers []segment.Header
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
		msg.Time = ts
		msg.Key = key
		msg.Value = val
		msg.Headers = headers
	}
	return msgs, nil
}

func decodeHeaders(kafkaHeaders []byte) ([]segment.Header, error) {
	numHeaders, off := binary.Varint(kafkaHeaders)
	if off <= 0 {
		return nil, errwrap.Errorf("failed to decode uvarint from kafka headers: %d", off)
	}
	in := int(numHeaders)
	headers := make([]segment.Header, in)
	for i := 0; i < in; i++ {
		headerNameLen, n := binary.Varint(kafkaHeaders[off:])
		if n <= 0 {
			return nil, errwrap.Errorf("failed to decode uvarint from kafka headers: %d", n)
		}
		off += n
		hNameOff := off
		iHNameLen := int(headerNameLen)
		off += iHNameLen
		headerValLen, n := binary.Varint(kafkaHeaders[off:])
		if n <= 0 {
			return nil, errwrap.Errorf("failed to decode uvarint from kafka headers: %d", n)
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

func (dmp *DefaultMessageProducer) createConnection() (*segment.Conn, error) {
	startPos := dmp.bootstrapPos
	for {
		address := dmp.bootstrapServers[dmp.bootstrapPos]
		dmp.bootstrapPos++
		if dmp.bootstrapPos == len(dmp.bootstrapServers) {
			dmp.bootstrapPos = 0
		}
		ctx, cancel := context.WithTimeout(context.Background(), dmp.connectTimeout)
		conn, err := segment.DialLeader(ctx, "tcp", address, dmp.topicName, dmp.partitionID)
		//goland:noinspection ALL
		defer cancel()
		if err == nil {
			if err := conn.SetRequiredAcks(dmp.acks); err != nil {
				return nil, err
			}
			return conn, nil
		}
		log.Warnf("failed to connect to kafka server %s - %v", address, err)
		if dmp.bootstrapPos == startPos {
			return nil, errwrap.Errorf("unable to connection to any of the kafka bootstrap servers: %v", dmp.bootstrapServers)
		}
	}
}

var _ MessageProducer = &DefaultMessageProducer{}

type DefaultMessageProvider struct {
	lock       sync.Mutex
	consumer   *kafka.Consumer
	topicName  string
	groupID    string
	partitions []kafka.TopicPartition
	krpf       *DefaultMessageProviderFactory
}

var _ MessageProvider = &DefaultMessageProvider{}

func (dmp *DefaultMessageProvider) GetMessage(pollTimeout time.Duration) (*Message, error) {
	dmp.lock.Lock()
	defer dmp.lock.Unlock()
	if dmp.consumer == nil {
		return nil, nil
	}

	ev := dmp.consumer.Poll(int(pollTimeout.Milliseconds()))
	if ev == nil {
		return nil, nil
	}
	switch e := ev.(type) {
	case *kafka.Message:
		msg := e
		headers := make([]MessageHeader, len(msg.Headers))
		for i, hdr := range msg.Headers {
			headers[i] = MessageHeader{
				Key:   hdr.Key,
				Value: hdr.Value,
			}
		}
		m := &Message{
			PartInfo: PartInfo{
				PartitionID: msg.TopicPartition.Partition,
				Offset:      int64(msg.TopicPartition.Offset),
			},
			TimeStamp: msg.Timestamp,
			Key:       msg.Key,
			Value:     msg.Value,
			Headers:   headers,
		}
		return m, nil
	case kafka.Error:
		return nil, e
	default:
		return nil, errwrap.Errorf("unexpected result from poll %+v", e)
	}
}

func (dmp *DefaultMessageProvider) Stop() error {
	dmp.lock.Lock()
	defer dmp.lock.Unlock()
	err := dmp.consumer.Close()
	dmp.consumer = nil
	return errwrap.WithStack(err)
}

func (dmp *DefaultMessageProvider) Start() error {
	dmp.lock.Lock()
	defer dmp.lock.Unlock()
	cm := &kafka.ConfigMap{
		"auto.offset.reset":    "earliest",
		"enable.auto.commit":   false,
		"session.timeout.ms":   30 * 1000,
		"max.poll.interval.ms": 60 * 1000,
		"group.id":             dmp.groupID,
	}
	_, ok := dmp.krpf.props["bootstrap.servers"]
	if !ok {
		return common.NewStatementError("cannot start message provider - bootstrap.servers must be specified")
	}
	for k, v := range dmp.krpf.props {
		if err := cm.SetKey(k, v); err != nil {
			return errwrap.WithStack(err)
		}
	}
	consumer, err := kafka.NewConsumer(cm)
	if err != nil {
		return errwrap.WithStack(err)
	}
	if err := consumer.Assign(dmp.partitions); err != nil {
		return errwrap.WithStack(err)
	}
	dmp.consumer = consumer
	return nil
}
