package integ

import (
	"fmt"
	kafkago "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/compress"
	"github.com/spirit-labs/tektite/kafka"
	log "github.com/spirit-labs/tektite/logger"
	"sync"
	"time"
)

type KafkaGoProducer struct {
	producer *kafkago.Producer
}

func NewKafkaGoProducer(address string, tlsEnabled bool, serverCertFile string, clientCertFile string,
	clientPrivateKeyFile string, compressionType compress.CompressionType, az string) (Producer, error) {
	cm := kafkago.ConfigMap{
		"partitioner":        "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers":  address,
		"acks":               "all",
		"enable.idempotence": "true",
		"compression.type":   compressionType.String(),
		"linger.ms":          10,
		"client.id":          fmt.Sprintf("tek_az=%s", az),
		//"debug":              "all",
	}
	if tlsEnabled {
		cm = configureForTls(cm, serverCertFile, clientCertFile, clientPrivateKeyFile)
	}
	producer, err := kafkago.NewProducer(&cm)
	if err != nil {
		return nil, err
	}
	return &KafkaGoProducer{producer}, nil
}

func (k *KafkaGoProducer) Produce(topicProduces ...TopicProduce) error {
	var msgs []*kafkago.Message
	for _, topicProduce := range topicProduces {
		for _, km := range topicProduce.Messages {
			msg := &kafkago.Message{
				TopicPartition: kafkago.TopicPartition{Topic: common.StrPtr(topicProduce.TopicName), Partition: kafkago.PartitionAny},
				Key:            km.Key,
				Value:          km.Value,
				Timestamp:      km.TimeStamp,
			}
			for _, hdr := range km.Headers {
				msg.Headers = append(msg.Headers, kafkago.Header{Key: hdr.Key, Value: hdr.Value})
			}
			msgs = append(msgs, msg)
		}
	}
	deliveryChan := make(chan kafkago.Event, len(msgs))
	for _, msg := range msgs {
		err := k.producer.Produce(msg, deliveryChan)
		if err != nil {
			return err
		}
	}
	for i := 0; i < len(msgs); i++ {
		e := <-deliveryChan
		m := e.(*kafkago.Message)
		if m.TopicPartition.Error != nil {
			return m.TopicPartition.Error
		}
	}
	return nil
}

func (k *KafkaGoProducer) Close() error {
	k.producer.Close()
	return nil
}

type KafkaGoConsumer struct {
	lock        sync.Mutex
	consumer    *kafkago.Consumer
	uncommitted map[int32]kafkago.TopicPartition
}

func NewKafkaGoConsumer(address string, groupID string, tlsEnabled bool, serverCertFile string, clientCertFile string,
	clientPrivateKeyFile string, az string) (Consumer, error) {
	cm := kafkago.ConfigMap{
		"bootstrap.servers":               address,
		"group.id":                        groupID,
		"auto.offset.reset":               "earliest",
		"enable.auto.commit":              false,
		"enable.auto.offset.store":        false,
		"client.id":                       fmt.Sprintf("tek_az=%s", az),
		"go.application.rebalance.enable": true,
		//"debug": "all",
	}
	if tlsEnabled {
		cm = configureForTls(cm, serverCertFile, clientCertFile, clientPrivateKeyFile)
	}
	consumer, err := kafkago.NewConsumer(&cm)
	if err != nil {
		return nil, err
	}
	return &KafkaGoConsumer{consumer: consumer, uncommitted: map[int32]kafkago.TopicPartition{}}, nil
}

func (k *KafkaGoConsumer) String() string {
	return k.consumer.String()
}

func (k *KafkaGoConsumer) Subscribe(topicName string) error {
	return k.consumer.Subscribe(topicName, k.rebalanceCallback)
}

func (k *KafkaGoConsumer) FetchAll() ([]*kafka.Message, error) {
	return nil, nil
}

func (k *KafkaGoConsumer) Fetch(timeout time.Duration) (*kafka.Message, error) {
	k.lock.Lock()
	defer k.lock.Unlock()
	msg, err := k.consumer.ReadMessage(timeout)
	if err != nil {
		if err.(kafkago.Error).Code() == kafkago.ErrTimedOut {
			// not an error
			return nil, nil
		}
		return nil, err
	}
	km := &kafka.Message{
		PartInfo: kafka.PartInfo{
			PartitionID: msg.TopicPartition.Partition,
			Offset:      int64(msg.TopicPartition.Offset),
		},
		TimeStamp: msg.Timestamp,
		Key:       msg.Key,
		Value:     msg.Value,
	}
	for _, hdr := range msg.Headers {
		km.Headers = append(km.Headers, kafka.MessageHeader{
			Key:   hdr.Key,
			Value: hdr.Value,
		})
	}
	if msg.TopicPartition.Error != nil {
		return nil, msg.TopicPartition.Error
	}
	topicPart := msg.TopicPartition
	topicPart.Offset++
	k.uncommitted[msg.TopicPartition.Partition] = topicPart
	return km, nil
}

func (k *KafkaGoConsumer) Commit() error {
	k.lock.Lock()
	defer k.lock.Unlock()
	return k.commit()
}

func (k *KafkaGoConsumer) commit() error {
	if len(k.uncommitted) == 0 {
		return nil
	}
	for _, topicPart := range k.uncommitted {
		_, err := k.consumer.StoreOffsets([]kafkago.TopicPartition{topicPart})
		if err != nil {
			return err
		}
	}
	_, err := k.consumer.Commit()
	k.uncommitted = map[int32]kafkago.TopicPartition{}
	return err
}

func (k *KafkaGoConsumer) Close() error {
	k.lock.Lock()
	defer k.lock.Unlock()
	return k.consumer.Close()
}

func (k *KafkaGoConsumer) rebalanceCallback(c *kafkago.Consumer, ev kafkago.Event) error {
	switch ev.(type) {
	case kafkago.RevokedPartitions:
		if err := k.commit(); err != nil {
			log.Errorf("failed to commit during rebalance: %v", err)
		}
	}
	return nil
}

func configureForTls(cm kafkago.ConfigMap, serverCertFile string, clientCertFile string, clientPrivateKeyFile string) kafkago.ConfigMap {
	cm["security.protocol"] = "ssl"
	cm["ssl.ca.location"] = serverCertFile
	if clientCertFile != "" {
		cm["ssl.certificate.location"] = clientCertFile
	}
	if clientPrivateKeyFile != "" {
		cm["ssl.key.location"] = clientPrivateKeyFile
	}
	return cm
}
